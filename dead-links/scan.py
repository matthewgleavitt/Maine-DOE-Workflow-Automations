"""
Maine DOE Dead Link Scanner
Runs via GitHub Actions on a schedule.
- Fetches all published pages via JSON:API
- Extracts every <a href> and <img src>
- Checks each DOE URL via HEAD request (no CORS restrictions server-side)
- Groups results by page author (node uid)
- Sends email notifications to authors via SendGrid
- Saves results JSON for the browser triage tool
"""

import requests
import json
import os
import sys
import time
import hashlib
from datetime import datetime
from html.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, unquote

# ─── Config ──────────────────────────────────────────────────────────────────
BASE_URL = "https://www.maine.gov/doe"
JSONAPI_URL = f"{BASE_URL}/jsonapi/node/multi_column_page"
PAGE_LIMIT = 50
CHECK_TIMEOUT = 15
MAX_WORKERS = 10
RESULTS_FILE = "dead-links/scan-results.json"

# MailerSend
MAILERSEND_API_KEY = os.environ.get("MAILERSEND_API_KEY", "")
FROM_EMAIL = os.environ.get("FROM_EMAIL", "matthew.g.leavitt@maine.gov")
FROM_NAME = os.environ.get("FROM_NAME", "Maine DOE Web Team")
SEND_EMAILS = os.environ.get("SEND_EMAILS", "false").lower() == "true"

# Optional: skip external checks for faster runs
CHECK_EXTERNAL = os.environ.get("CHECK_EXTERNAL", "true").lower() == "true"


# ─── HTML Link Extractor ─────────────────────────────────────────────────────
class LinkExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []  # (url, anchor_text, tag_type)
        self._current_anchor_text = None

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "a" and "href" in attrs_dict:
            href = attrs_dict["href"]
            if href and not href.startswith(("#", "mailto:", "tel:", "javascript:")):
                self._current_anchor_text = ""
                self.links.append([href, "", "link"])
        elif tag == "img" and "src" in attrs_dict:
            src = attrs_dict["src"]
            if src and "/sites/maine.gov.doe/files/" in src:
                alt = attrs_dict.get("alt", src.split("/")[-1])
                self.links.append([src, f"img: {alt}", "image"])

    def handle_data(self, data):
        if self._current_anchor_text is not None:
            self._current_anchor_text += data

    def handle_endtag(self, tag):
        if tag == "a" and self._current_anchor_text is not None:
            if self.links:
                self.links[-1][1] = self._current_anchor_text.strip()[:150]
            self._current_anchor_text = None


def extract_links(html):
    parser = LinkExtractor()
    try:
        parser.feed(html)
    except Exception:
        pass
    return parser.links


def resolve_url(href, page_url):
    """Resolve relative URLs to absolute."""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return f"https://www.maine.gov{href}"
    return urljoin(page_url, href)


def is_doe(url):
    """Links within maine.gov/doe/ — our pages and files."""
    lower = url.lower()
    return "maine.gov/doe/" in lower or "maine.gov/doe" == lower.rstrip("/")


def is_other_maine(url):
    """Links to other maine.gov departments (not DOE) — we can't control these."""
    lower = url.lower()
    return ("maine.gov" in lower) and not is_doe(url)


def is_internal(url):
    """Any maine.gov link."""
    return "maine.gov" in url.lower()


def is_file(url):
    return "/sites/maine.gov.doe/files/" in url or "/sites/default/files/" in url


# ─── Phase 1: Fetch all pages ────────────────────────────────────────────────
def fetch_all_pages():
    print("Phase 1: Fetching all published pages...")
    pages = []
    offset = 0

    while True:
        url = (
            f"{JSONAPI_URL}?filter[status]=1"
            f"&include=uid"
            f"&fields[node--multi_column_page]=body,title,path,drupal_internal__nid"
            f"&fields[user--user]=mail,name"
            f"&page[limit]={PAGE_LIMIT}&page[offset]={offset}"
        )
        try:
            resp = requests.get(url, timeout=30, headers={"Accept": "application/vnd.api+json"})
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  Error fetching page batch at offset {offset}: {e}")
            break

        if not data.get("data"):
            break

        # Build user lookup from included
        users = {}
        for inc in data.get("included", []):
            if inc.get("type") == "user--user":
                if inc.get("type") == "user--user" and "attributes" in inc:
                    users[inc["id"]] = inc["attributes"].get("mail") or inc["attributes"].get("name") or ""

        for node in data["data"]:
            attrs = node["attributes"]
            body = attrs.get("body", {})
            body_html = body.get("value", "") if body else ""
            path_obj = attrs.get("path", {})
            alias = path_obj.get("alias", "") if path_obj else ""
            nid = attrs.get("drupal_internal__nid")
            title = attrs.get("title", "")

            page_url = f"{BASE_URL}{alias}" if alias else f"{BASE_URL}/node/{nid}"

            # Get author from uid relationship
            uid_data = node.get("relationships", {}).get("uid", {}).get("data", {})
            uid_id = uid_data.get("id", "") if uid_data else ""
            author_email = users.get(uid_id, "")

            pages.append({
                "nid": nid,
                "title": title,
                "url": page_url,
                "author": author_email,
                "body": body_html,
            })

        print(f"  Loaded {len(pages)} pages...")
        if not data.get("links", {}).get("next"):
            break
        offset += PAGE_LIMIT
        time.sleep(0.1)

    print(f"  Total: {len(pages)} published pages")
    return pages


# ─── Phase 2: Extract all links ──────────────────────────────────────────────
def extract_all_links(pages):
    print("Phase 2: Extracting links from page bodies...")
    link_map = {}  # url -> [refs]

    for page in pages:
        if not page["body"]:
            continue
        links = extract_links(page["body"])
        for href, anchor, link_type in links:
            full_url = resolve_url(href, page["url"])
            if full_url not in link_map:
                link_map[full_url] = []
            link_map[full_url].append({
                "page_url": page["url"],
                "page_title": page["title"],
                "anchor": anchor,
                "nid": page["nid"],
                "author": page["author"],
                "link_type": link_type,
            })

    total_refs = sum(len(v) for v in link_map.values())
    print(f"  Found {len(link_map)} unique URLs ({total_refs} total references)")
    return link_map


# ─── Phase 3: Check URLs ─────────────────────────────────────────────────────
def check_url(url):
    """Check a single URL. Returns (url, status, error)."""
    try:
        resp = requests.head(
            url, timeout=CHECK_TIMEOUT, allow_redirects=True,
            headers={"User-Agent": "Maine-DOE-Link-Checker/1.0"}
        )
        if resp.status_code < 400:
            return (url, resp.status_code, None)
        # Try GET as fallback (some servers reject HEAD)
        if resp.status_code in (403, 405, 406):
            resp2 = requests.get(
                url, timeout=CHECK_TIMEOUT, allow_redirects=True,
                headers={"User-Agent": "Maine-DOE-Link-Checker/1.0"},
                stream=True
            )
            resp2.close()
            if resp2.status_code < 400:
                return (url, resp2.status_code, None)
            return (url, resp2.status_code, f"{resp2.status_code} {resp2.reason}")
        return (url, resp.status_code, f"{resp.status_code} {resp.reason}")
    except requests.exceptions.Timeout:
        return (url, 0, "timeout")
    except requests.exceptions.ConnectionError as e:
        return (url, 0, "connection_error")
    except requests.exceptions.SSLError as e:
        return (url, 0, "ssl_error")
    except Exception as e:
        return (url, 0, str(e)[:100])


def check_all_urls(link_map):
    urls_to_check = list(link_map.keys())
    if not CHECK_EXTERNAL:
        internal_count = len([u for u in urls_to_check if is_internal(u)])
        external_count = len(urls_to_check) - internal_count
        urls_to_check = [u for u in urls_to_check if is_internal(u)]
        print(f"Phase 3: Checking {len(urls_to_check)} internal URLs (skipping {external_count} external)...")
    else:
        print(f"Phase 3: Checking {len(urls_to_check)} URLs...")

    results = {}
    checked = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(check_url, url): url for url in urls_to_check}
        for future in as_completed(futures):
            url, status, error = future.result()
            if error:
                results[url] = {"status": status, "error": error}
            checked += 1
            if checked % 100 == 0:
                print(f"  Checked {checked}/{len(urls_to_check)} ({len(results)} broken so far)")

    print(f"  Done: {len(results)} broken links found out of {len(urls_to_check)} checked")
    return results


# ─── Phase 4: Build results ──────────────────────────────────────────────────
def categorize(url, error):
    if is_file(url) and is_doe(url):
        return "MISSING_FILE"
    if is_doe(url):
        return "INTERNAL_404"
    if is_other_maine(url):
        return "OTHER_MAINE_GOV"
    return "EXTERNAL_DEAD"


def build_results(link_map, check_results):
    print("Phase 4: Building results...")
    dead_links = []

    for url, result in check_results.items():
        refs = link_map.get(url, [])
        cat = categorize(url, result["error"])
        for ref in refs:
            dead_links.append({
                "category": cat,
                "page_url": ref["page_url"],
                "page_title": ref["page_title"],
                "anchor": ref["anchor"],
                "broken_url": url,
                "error": result["error"],
                "status": result["status"],
                "nid": ref["nid"],
                "author": ref["author"],
                "link_type": ref["link_type"],
            })

    dead_links.sort(key=lambda x: (
        ["MISSING_FILE", "INTERNAL_404", "OTHER_MAINE_GOV", "EXTERNAL_DEAD"].index(x["category"]),
        x["page_url"]
    ))

    # Stats
    by_cat = {}
    for d in dead_links:
        by_cat[d["category"]] = by_cat.get(d["category"], 0) + 1

    unique_pages = len(set(d["page_url"] for d in dead_links))
    unique_authors = len(set(d["author"] for d in dead_links if d["author"]))

    meta = {
        "scan_date": datetime.now().isoformat(),
        "pages_scanned": len(link_map),
        "urls_checked": len(check_results) + sum(1 for u in link_map if u not in check_results),
        "broken_found": len(dead_links),
        "affected_pages": unique_pages,
        "affected_authors": unique_authors,
        "by_category": by_cat,
    }

    actionable_count = sum(1 for d in dead_links if d["category"] != "OTHER_MAINE_GOV")
    other_count = sum(1 for d in dead_links if d["category"] == "OTHER_MAINE_GOV")
    print(f"  {actionable_count} actionable + {other_count} other-maine.gov across {unique_pages} pages, {unique_authors} authors")
    for cat, count in by_cat.items():
        print(f"    {cat}: {count}")

    return dead_links, meta


# ─── Phase 5: Save results ───────────────────────────────────────────────────
def save_results(dead_links, meta):
    output = {"meta": meta, "results": dead_links}

    with open(RESULTS_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to {RESULTS_FILE} ({os.path.getsize(RESULTS_FILE) // 1024} KB)")

    # Also save a compact version for the browser triage tool
    compact = {}
    for d in dead_links:
        page = d["page_url"]
        if page not in compact:
            compact[page] = {"p": page, "nid": d["nid"], "author": d["author"], "l": []}
        compact[page]["l"].append({
            "c": d["category"],
            "a": d["anchor"],
            "u": d["broken_url"],
            "e": d["error"],
        })
    compact_list = sorted(compact.values(), key=lambda x: -len(x["l"]))

    with open("dead-links/scan-results-compact.json", "w") as f:
        json.dump({"meta": meta, "pages": compact_list}, f, separators=(",", ":"))
    print(f"Compact results saved to dead-links/scan-results-compact.json")

    return output


# ─── Phase 6: Email authors ──────────────────────────────────────────────────
def send_author_emails(dead_links, meta):
    if not SEND_EMAILS:
        print("\nEmail sending disabled (set SEND_EMAILS=true to enable)")
        return
    if not MAILERSEND_API_KEY:
        print("\nNo MAILERSEND_API_KEY set — skipping emails")
        return

    # Group by author
    by_author = {}
    for d in dead_links:
        if d["category"] == "OTHER_MAINE_GOV":
            continue
        email = d["author"]
        if not email or "@" not in email:
            continue
        if email not in by_author:
            by_author[email] = []
        by_author[email].append(d)

    print(f"\nPhase 6: Sending emails to {len(by_author)} authors...")

    scan_date = datetime.fromisoformat(meta["scan_date"]).strftime("%B %d, %Y")

    for author_email, links in by_author.items():
        # Group by page
        pages = {}
        for link in links:
            pu = link["page_url"]
            if pu not in pages:
                pages[pu] = {"title": link["page_title"], "links": []}
            pages[pu]["links"].append(link)

        total = len(links)
        page_count = len(pages)

        # Build HTML email
        rows = ""
        for page_url, page_data in sorted(pages.items()):
            rows += f'<tr style="background:#f8f9fa"><td colspan="3" style="padding:10px 12px;font-weight:600;border-bottom:2px solid #182b3c">'
            rows += f'<a href="{page_url}" style="color:#182b3c">{page_data["title"]}</a></td></tr>\n'
            for link in page_data["links"]:
                cat_label = {
                    "MISSING_FILE": "Missing File",
                    "INTERNAL_404": "Internal 404",
                    "OTHER_MAINE_GOV": "Other Maine.gov",
                    "EXTERNAL_DEAD": "External Dead",
                }.get(link["category"], link["category"])
                rows += f'<tr>'
                rows += f'<td style="padding:6px 12px;border-bottom:1px solid #eee">{link["anchor"]}</td>'
                rows += f'<td style="padding:6px 12px;border-bottom:1px solid #eee;font-size:12px;color:#666;word-break:break-all">{link["broken_url"]}</td>'
                rows += f'<td style="padding:6px 12px;border-bottom:1px solid #eee;font-size:12px">{cat_label}</td>'
                rows += f'</tr>\n'

        html_body = f"""
        <div style="font-family:Calibri,sans-serif;max-width:700px;margin:0 auto">
            <div style="background:#182b3c;padding:20px 24px;border-radius:8px 8px 0 0">
                <h2 style="color:#fff;margin:0;font-size:20px">Broken Links Found on Your Pages</h2>
                <p style="color:#8ab4d4;margin:6px 0 0;font-size:14px">Monthly scan — {scan_date}</p>
            </div>
            <div style="padding:20px 24px;background:#fff;border:1px solid #e9ecef">
                <p>Hi,</p>
                <p>Our monthly link scan found <strong>{total} broken link{'' if total == 1 else 's'}</strong> across
                <strong>{page_count} page{'' if page_count == 1 else 's'}</strong> that you author on maine.gov/doe.</p>
                <p>These are links pointing to files, pages, or external sites that are no longer available.
                Visitors clicking them are hitting dead ends.</p>

                <p><strong>What to do:</strong></p>
                <ul>
                    <li>If the resource has moved, update the link to the new URL</li>
                    <li>If the resource no longer exists, remove the link (and surrounding text if needed)</li>
                    <li>Submit your page for approval after making changes</li>
                </ul>

                <table style="width:100%;border-collapse:collapse;margin:16px 0">
                    <thead>
                        <tr style="background:#182b3c;color:#fff">
                            <th style="padding:8px 12px;text-align:left">Link Text</th>
                            <th style="padding:8px 12px;text-align:left">Broken URL</th>
                            <th style="padding:8px 12px;text-align:left">Type</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows}
                    </tbody>
                </table>

                <p style="color:#666;font-size:13px">If you have questions or need help, reply to this email or contact
                the Web Team. This is an automated monthly scan.</p>
            </div>
            <div style="background:#f8f9fa;padding:12px 24px;border-radius:0 0 8px 8px;border:1px solid #e9ecef;border-top:none">
                <p style="margin:0;font-size:12px;color:#999">Maine Department of Education · Website & Technology ·
                <a href="https://www.maine.gov/doe" style="color:#42c3f7">maine.gov/doe</a></p>
            </div>
        </div>
        """

        # Send via MailerSend
        ms_data = {
            "from": {"email": FROM_EMAIL, "name": FROM_NAME},
            "to": [{"email": author_email}],
            "subject": f"Action Needed: {total} Broken Link{'s' if total != 1 else ''} on Your DOE Pages",
            "html": html_body,
        }

        try:
            resp = requests.post(
                "https://api.mailersend.com/v1/email",
                json=ms_data,
                headers={
                    "Authorization": f"Bearer {MAILERSEND_API_KEY}",
                    "Content-Type": "application/json",
                },
                timeout=15,
            )
            if resp.status_code in (200, 201, 202):
                print(f"  ✓ Sent to {author_email} ({total} links, {page_count} pages)")
            else:
                print(f"  ✗ Failed for {author_email}: {resp.status_code} {resp.text[:100]}")
        except Exception as e:
            print(f"  ✗ Error sending to {author_email}: {e}")

        time.sleep(0.5)  # Rate limit




# ─── Phase 5b: Generate HTML report page ─────────────────────────────────────
def generate_report(dead_links, meta):
    print("Generating HTML report...")
    scan_date = datetime.fromisoformat(meta["scan_date"]).strftime("%B %d, %Y")
    by_cat = meta.get("by_category", {})

    # Separate actionable (DOE) from informational (other maine.gov)
    actionable = [d for d in dead_links if d["category"] != "OTHER_MAINE_GOV"]
    other_maine = [d for d in dead_links if d["category"] == "OTHER_MAINE_GOV"]
    total = len(actionable)
    pages_affected = len(set(d["page_url"] for d in actionable)) if actionable else 0

    # Group by page (actionable only)
    pages = {}
    for d in actionable:
        pu = d["page_url"]
        if pu not in pages:
            pages[pu] = {"title": d["page_title"], "author": d["author"], "nid": d["nid"], "links": []}
        pages[pu]["links"].append(d)
    sorted_pages = sorted(pages.items(), key=lambda x: -len(x[1]["links"]))

    # Build table rows
    rows = ""
    for page_url, page_data in sorted_pages:
        short = page_url.replace("https://www.maine.gov/doe/", "/doe/")
        author = page_data["author"] or "—"
        for i, link in enumerate(page_data["links"]):
            cat_colors = {"MISSING_FILE": "#e74c3c", "INTERNAL_404": "#e67e22", "OTHER_MAINE_GOV": "#3498db", "EXTERNAL_DEAD": "#f1c40f"}
            cat_labels = {"MISSING_FILE": "Missing File", "INTERNAL_404": "Internal 404", "OTHER_MAINE_GOV": "Other Maine.gov", "EXTERNAL_DEAD": "External Dead"}
            color = cat_colors.get(link["category"], "#999")
            label = cat_labels.get(link["category"], link["category"])
            page_cell = f'<td style="padding:8px 12px;border-bottom:1px solid rgba(109,139,166,0.1)"><a href="{page_url}" target="_blank" style="color:#42c3f7;text-decoration:underline">{short}</a></td>' if i == 0 else '<td style="padding:8px 12px;border-bottom:1px solid rgba(109,139,166,0.1)"></td>'
            author_cell = f'<td style="padding:8px 12px;border-bottom:1px solid rgba(109,139,166,0.1);font-size:12px;color:#8ab4d4">{author}</td>' if i == 0 else '<td style="padding:8px 12px;border-bottom:1px solid rgba(109,139,166,0.1)"></td>'
            rows += f"""<tr>
              {page_cell}
              {author_cell}
              <td style="padding:8px 12px;border-bottom:1px solid rgba(109,139,166,0.1)">{link["anchor"]}</td>
              <td style="padding:8px 12px;border-bottom:1px solid rgba(109,139,166,0.1);font-size:11px;color:#6d8ba6;word-break:break-all">{link["broken_url"]}</td>
              <td style="padding:8px 12px;border-bottom:1px solid rgba(109,139,166,0.1)"><span style="color:{color};font-weight:600;font-size:11px">{label}</span></td>
              <td style="padding:8px 12px;border-bottom:1px solid rgba(109,139,166,0.1);font-size:11px;color:#8ab4d4">{link["error"]}</td>
            </tr>"""

    # Status banner
    if total == 0:
        status_html = """
        <div style="text-align:center;padding:60px 24px">
          <div style="font-size:64px;margin-bottom:16px">\u2705</div>
          <h2 style="color:#2ecc71;font-size:28px;margin:0 0 8px">All Clear!</h2>
          <p style="color:#8ab4d4;font-size:16px">No broken links found across the entire site.</p>
        </div>"""
    else:
        cat_stats = ""
        cat_icons = {"MISSING_FILE": "\U0001f534", "INTERNAL_404": "\U0001f7e0", "OTHER_MAINE_GOV": "\U0001f535", "EXTERNAL_DEAD": "\U0001f7e1"}
        cat_labels = {"MISSING_FILE": "Missing Files", "INTERNAL_404": "Internal 404s", "OTHER_MAINE_GOV": "Other Maine.gov", "EXTERNAL_DEAD": "External Dead"}
        for cat in ["MISSING_FILE", "INTERNAL_404", "EXTERNAL_DEAD"]:
            count = by_cat.get(cat, 0)
            if count:
                cat_stats += f'<div style="background:rgba(66,195,247,0.08);border:1px solid rgba(66,195,247,0.2);border-radius:8px;padding:6px 14px;font-size:13px"><strong style="color:#42c3f7">{count}</strong> {cat_icons.get(cat, "")} {cat_labels.get(cat, cat)}</div>'

        status_html = f"""
        <div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:16px">
          <div style="background:rgba(66,195,247,0.08);border:1px solid rgba(66,195,247,0.2);border-radius:8px;padding:6px 14px;font-size:13px"><strong style="color:#42c3f7">{total}</strong> broken links</div>
          <div style="background:rgba(66,195,247,0.08);border:1px solid rgba(66,195,247,0.2);border-radius:8px;padding:6px 14px;font-size:13px"><strong style="color:#42c3f7">{pages_affected}</strong> pages affected</div>
          {cat_stats}
        </div>
        <div style="overflow-x:auto">
        <table style="width:100%;border-collapse:collapse;font-size:13px">
          <thead><tr style="background:#1b3a54">
            <th style="padding:10px 12px;text-align:left;color:#42c3f7;position:sticky;top:0;background:#1b3a54">Page</th>
            <th style="padding:10px 12px;text-align:left;color:#42c3f7;position:sticky;top:0;background:#1b3a54">Author</th>
            <th style="padding:10px 12px;text-align:left;color:#42c3f7;position:sticky;top:0;background:#1b3a54">Link Text</th>
            <th style="padding:10px 12px;text-align:left;color:#42c3f7;position:sticky;top:0;background:#1b3a54">Broken URL</th>
            <th style="padding:10px 12px;text-align:left;color:#42c3f7;position:sticky;top:0;background:#1b3a54">Type</th>
            <th style="padding:10px 12px;text-align:left;color:#42c3f7;position:sticky;top:0;background:#1b3a54">Error</th>
          </tr></thead>
          <tbody>{rows}</tbody>
        </table>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Dead Link Report — Maine DOE</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ background: #0d1b2a; color: #e0e6ed; font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; }}
  a {{ color: #42c3f7; }}
</style>
</head>
<body>
<div style="background:linear-gradient(135deg,#182b3c 0%,#1b3a54 100%);padding:24px;border-bottom:3px solid #42c3f7">
  <h1 style="font-size:24px;color:#fff;margin:0">Dead Link Report</h1>
  <p style="font-size:14px;color:#8ab4d4;margin:6px 0 0">
    Maine DOE &middot; maine.gov/doe &middot; Scanned {scan_date} &middot;
    {meta["pages_scanned"]} pages checked &middot; {meta["urls_checked"]} URLs verified
  </p>
</div>
<div style="padding:24px">
  {status_html}
</div>
<div style="padding:0 24px 24px;font-size:12px;color:#3a5068">
  {f'<p style="color:#6d8ba6;margin-bottom:8px">Also found {len(other_maine)} broken link(s) to other maine.gov departments (e.g., /sos/, /dhhs/, /labor/). These are outside DOE control and not shown above.</p>' if other_maine else ''}
  Generated automatically by the Maine DOE Dead Link Scanner.
  Contact the Web Team with questions.
</div>
</body>
</html>"""

    with open("dead-links/index.html", "w") as f:
        f.write(html)
    print(f"  Report saved to dead-links/index.html")

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    start = time.time()
    print(f"═══ Maine DOE Dead Link Scanner ═══")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"External checks: {'enabled' if CHECK_EXTERNAL else 'disabled'}")
    print(f"Email sending: {'enabled' if SEND_EMAILS else 'disabled'}")
    print()

    pages = fetch_all_pages()
    if not pages:
        print("No pages found — aborting")
        sys.exit(1)

    link_map = extract_all_links(pages)
    check_results = check_all_urls(link_map)
    dead_links, meta = build_results(link_map, check_results)
    save_results(dead_links, meta)
    generate_report(dead_links, meta)
    send_author_emails(dead_links, meta)

    elapsed = time.time() - start
    print(f"\n═══ Complete in {elapsed:.0f}s ═══")


if __name__ == "__main__":
    main()
