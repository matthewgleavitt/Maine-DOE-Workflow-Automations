"""
Maine DOE Dead Link Scanner
Version: 2.0 — 2026-04-06 9:30 PM ET
Runs via GitHub Actions on a schedule.
- Fetches all published pages via JSON:API
- Extracts every <a href> and <img src>
- Checks each DOE URL via HEAD request (no CORS restrictions server-side)
- Groups results by page author (node uid)
- Sends email notifications to authors via MailerSend
- Saves results JSON for the browser triage tool
"""

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
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

# Test mode: sends ALL emails to ADMIN_EMAIL instead of real authors
TEST_MODE = os.environ.get("TEST_MODE", "false").lower() == "true"

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
    # Load author map from repo (built by the browser-based author-mapper tool)
    author_map = {}
    author_map_path = os.path.join(os.path.dirname(__file__), "author-map.json")
    if os.path.exists(author_map_path):
        with open(author_map_path) as f:
            author_map = json.load(f)
        print(f"  Loaded author-map.json ({len(author_map)} entries)")
    else:
        print("  No author-map.json found — will try API for authors")

    pages = []
    users = {}  # Accumulate across all batches
    offset = 0

    while True:
        url = (
            f"{JSONAPI_URL}?filter[status]=1"
            f"&include=uid"
            f"&fields[node--multi_column_page]=body,title,path,drupal_internal__nid,field_page_owner_email,created"
            f"&fields[user--user]=mail,name,display_name"
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

        # Build user lookup from included (accumulates across batches)
        for inc in data.get("included", []):
            if inc.get("type") == "user--user" and "attributes" in inc:
                attrs = inc["attributes"]
                email = attrs.get("mail") or attrs.get("display_name") or attrs.get("name") or ""
                if email:
                    users[inc["id"]] = email

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
            uid_data = node.get("relationships", {}).get("uid", {}).get("data")
            uid_id = uid_data.get("id", "") if uid_data else ""
            # Source 1: field_page_owner_email (public custom field on the node)
            author_email = attrs.get("field_page_owner_email") or ""
            # Source 2: node author from uid relationship
            if not author_email:
                author_email = users.get(uid_id, "")
            if not author_email and uid_id:
                for inc in data.get("included", []):
                    if inc.get("id") == uid_id and "attributes" in inc:
                        author_email = inc["attributes"].get("mail") or inc["attributes"].get("display_name") or inc["attributes"].get("name") or ""
                        break
            # Source 3: author-map.json fallback (from browser-based mapper)
            if not author_email and str(nid) in author_map:
                author_email = author_map[str(nid)]

            pages.append({
                "nid": nid,
                "title": title,
                "url": page_url,
                "author": author_email,
                "body": body_html,
                "created": attrs.get("created", ""),
            })

        print(f"  Loaded {len(pages)} pages...")
        if not data.get("links", {}).get("next"):
            break
        offset += PAGE_LIMIT
        time.sleep(0.1)

    authors_found = sum(1 for p in pages if p["author"])
    print(f"  Total: {len(pages)} published pages, {authors_found} with authors ({len(users)} unique API users, {len(author_map)} from author-map.json)")
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
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Only these HTTP status codes mean the link is DEFINITELY dead
CONFIRMED_DEAD_CODES = {404, 410, 451, 500, 502, 503, 521, 522, 523}


def check_url(url):
    """Check a single URL with multi-step verification to minimize false positives.
    Returns (url, status, error) where error=None means link is OK."""
    import time as _time

    # Step 1: HEAD request
    try:
        resp = requests.head(url, timeout=CHECK_TIMEOUT, allow_redirects=True, headers=BROWSER_HEADERS)
        if resp.status_code < 400:
            return (url, resp.status_code, None)
        if resp.status_code in CONFIRMED_DEAD_CODES:
            # Confirmed dead via HEAD — but verify with GET since some servers reject HEAD
            pass
        # Fall through to GET for any 4xx/5xx
    except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        pass  # Fall through to GET
    except Exception:
        pass

    # Step 2: GET request (many servers block HEAD but allow GET)
    try:
        resp = requests.get(url, timeout=CHECK_TIMEOUT, allow_redirects=True, headers=BROWSER_HEADERS, stream=True)
        resp.close()
        if resp.status_code < 400:
            return (url, resp.status_code, None)
        if resp.status_code in CONFIRMED_DEAD_CODES:
            return (url, resp.status_code, f"{resp.status_code} {resp.reason}")
        # 403, 401, 406, 429, etc. = site is alive but blocking us
        return (url, resp.status_code, "blocked")
    except requests.exceptions.SSLError:
        pass  # Try without SSL verification
    except requests.exceptions.ConnectionError:
        pass  # Retry after delay
    except requests.exceptions.Timeout:
        pass  # Retry after delay
    except Exception as e:
        return (url, 0, str(e)[:100])

    # Step 3: Retry GET after short delay (handles rate limiting)
    _time.sleep(2)
    try:
        resp = requests.get(url, timeout=CHECK_TIMEOUT + 5, allow_redirects=True, headers=BROWSER_HEADERS, stream=True, verify=False)
        resp.close()
        if resp.status_code < 400:
            return (url, resp.status_code, None)
        if resp.status_code in CONFIRMED_DEAD_CODES:
            return (url, resp.status_code, f"{resp.status_code} {resp.reason}")
        return (url, resp.status_code, "blocked")
    except requests.exceptions.ConnectionError:
        return (url, 0, "connection_error")
    except requests.exceptions.Timeout:
        return (url, 0, "timeout")
    except Exception as e:
        return (url, 0, str(e)[:100])


def check_all_urls(link_map, allowlist=None):
    urls_to_check = list(link_map.keys())
    if allowlist:
        before = len(urls_to_check)
        urls_to_check = [u for u in urls_to_check if u not in allowlist]
        skipped = before - len(urls_to_check)
        if skipped:
            print(f"  Skipped {skipped} allowlisted URLs")
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
    # Connection errors and access blocks from external sites are often false positives
    # (sites blocking cloud IPs/bot detection, not actually dead)
    # Anything that isn't a confirmed HTTP error = site might be fine but blocking us
    if error in ("connection_error", "ssl_error", "timeout", "blocked"):
        return "EXTERNAL_UNVERIFIABLE"
    return "EXTERNAL_DEAD"


def build_results(link_map, check_results, page_count):
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
        ["MISSING_FILE", "INTERNAL_404", "EXTERNAL_DEAD", "EXTERNAL_UNVERIFIABLE", "OTHER_MAINE_GOV"].index(x["category"]),
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
        "pages_scanned": page_count,
        "urls_checked": len(check_results),
        "broken_found": len(dead_links),
        "affected_pages": unique_pages,
        "affected_authors": unique_authors,
        "by_category": by_cat,
    }

    actionable_count = sum(1 for d in dead_links if d["category"] not in ("OTHER_MAINE_GOV", "EXTERNAL_UNVERIFIABLE"))
    other_count = sum(1 for d in dead_links if d["category"] == "OTHER_MAINE_GOV")
    unverif_count = sum(1 for d in dead_links if d["category"] == "EXTERNAL_UNVERIFIABLE")
    print(f"  {actionable_count} actionable + {unverif_count} unverifiable + {other_count} other-maine.gov across {unique_pages} pages, {unique_authors} authors")
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

    # Also save as .js for CORS-free loading via <script> tag
    with open("dead-links/scan-results.js", "w") as f:
        f.write("window.SCAN_DATA = ")
        json.dump({"meta": meta, "pages": compact_list}, f, separators=(",", ":"))
        f.write(";")
    print(f"JS version saved to dead-links/scan-results.js")

    return output


# ─── Phase 6: Email authors ──────────────────────────────────────────────────
def send_author_emails(dead_links, meta):
    if not SEND_EMAILS:
        print("\nEmail sending disabled (set SEND_EMAILS=true to enable)")
        return
    if not MAILERSEND_API_KEY:
        print("\nNo MAILERSEND_API_KEY set — skipping emails")
        return

    # Group by author (actionable only)
    by_author = {}
    unverifiable_by_author = {}
    for d in dead_links:
        email = d.get("author", "")
        if not email or "@" not in email:
            continue
        if d["category"] == "EXTERNAL_UNVERIFIABLE":
            unverifiable_by_author[email] = unverifiable_by_author.get(email, 0) + 1
            continue
        if d["category"] == "OTHER_MAINE_GOV":
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
                    "EXTERNAL_DEAD": "External Dead",
                    "EXTERNAL_UNVERIFIABLE": "Unverifiable",
                    "OTHER_MAINE_GOV": "Other Maine.gov",
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
                <p style="color:#42c3f7;margin:6px 0 0;font-size:14px">Monthly scan — {scan_date}</p>
            </div>
            <div style="padding:20px 24px;background:#fff;border:1px solid #e9ecef">
                <p>Hi,</p>
                <p>Our monthly link scan found <strong>{total} broken link{'' if total == 1 else 's'}</strong> across
                <strong>{page_count} page{'' if page_count == 1 else 's'}</strong> that you author on maine.gov/doe.</p>
                <p>These are links pointing to files, pages, or external sites that are no longer available.
                Visitors clicking them are hitting dead ends. Please address these within the next <strong>week</strong>.</p>

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
                <p style="margin:0;font-size:12px;color:#666">If you are not the appropriate owner for this page, please forward this email to <a href="mailto:matthew.g.leavitt@maine.gov" style="color:#42c3f7">matthew.g.leavitt@maine.gov</a> so it can be reassigned.</p>
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






# ─── Phase 5c: Orphan File Detection ─────────────────────────────────────────
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "matthew.g.leavitt@maine.gov")

def find_orphan_files(pages):
    """Find managed files not referenced in any page body."""
    print("\nPhase 5c: Checking for orphan files...")

    # Step 1: Build set of all file URLs referenced in page bodies
    referenced_files = set()
    for page in pages:
        body = page.get("body", "")
        if not body:
            continue
        parser = LinkExtractor()
        try:
            parser.feed(body)
        except Exception:
            pass
        for href, anchor, link_type in parser.links:
            url = href
            if url.startswith("/"):
                url = "https://www.maine.gov" + url
            if "/sites/maine.gov.doe/files/" in url:
                # Normalize: decode and lowercase for comparison
                try:
                    url = requests.utils.unquote(url).lower()
                except Exception:
                    url = url.lower()
                referenced_files.add(url)

    print(f"  Found {len(referenced_files)} unique file URLs in page bodies")

    # Step 2: Fetch all managed files from JSON:API
    print("  Fetching all file entities...")
    all_files = []
    offset = 0
    while True:
        url = (
            f"{BASE_URL}/jsonapi/file/file"
            f"?fields[file--file]=filename,uri,drupal_internal__fid,filesize,created"
            f"&page[limit]={PAGE_LIMIT}&page[offset]={offset}"
        )
        try:
            resp = requests.get(url, timeout=30, headers={"Accept": "application/vnd.api+json"})
            if not resp.ok:
                print(f"  Error fetching files at offset {offset}: {resp.status_code}")
                break
            data = resp.json()
        except Exception as e:
            print(f"  Error: {e}")
            break

        if not data.get("data"):
            break

        for f in data["data"]:
            attrs = f.get("attributes", {})
            uri = attrs.get("uri", {})
            file_url = uri.get("url", "") if isinstance(uri, dict) else ""
            if not file_url:
                continue
            # Only check managed files in the DOE files directory
            if "/sites/maine.gov.doe/files/" not in file_url:
                continue
            fid = attrs.get("drupal_internal__fid")
            filename = attrs.get("filename", "")
            filesize = attrs.get("filesize", 0)
            created = attrs.get("created", "")

            all_files.append({
                "fid": fid,
                "filename": filename,
                "url": file_url,
                "filesize": filesize,
                "created": created,
            })

        if not data.get("links", {}).get("next"):
            break
        offset += PAGE_LIMIT
        if offset % 500 == 0:
            print(f"  Loaded {len(all_files)} files...")
        time.sleep(0.1)

    print(f"  Total: {len(all_files)} managed files in CMS")

    # Step 3: Find orphans — files not referenced in any body
    orphans = []
    for f in all_files:
        # Normalize file URL for comparison
        full_url = "https://www.maine.gov" + f["url"] if f["url"].startswith("/") else f["url"]
        try:
            normalized = requests.utils.unquote(full_url).lower()
        except Exception:
            normalized = full_url.lower()
        if normalized not in referenced_files:
            orphans.append(f)

    # Sort by filesize descending (biggest waste first)
    orphans.sort(key=lambda x: -(x.get("filesize") or 0))

    total_size = sum(f.get("filesize") or 0 for f in orphans)
    print(f"  Found {len(orphans)} orphan files ({total_size // 1024 // 1024} MB)")

    return orphans


def save_orphan_results(orphans):
    """Save orphan file results to JSON."""
    output = {
        "scan_date": datetime.now().isoformat(),
        "orphan_count": len(orphans),
        "total_size_bytes": sum(f.get("filesize") or 0 for f in orphans),
        "files": orphans
    }
    orphan_path = os.path.join(os.path.dirname(__file__) or ".", "orphan-files.json")
    with open(orphan_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"  Orphan results saved to {orphan_path}")
    return output



# ─── Phase 5d: Maine DOE Content Audits ───────────────────────────────────────
def check_content_audits(pages):
    """Check which pages are due for content audit this month."""
    print("\nPhase 5d: Checking content audit schedule...")
    
    schedule_path = os.path.join(os.path.dirname(__file__), "audit-schedule.json")
    if not os.path.exists(schedule_path):
        print("  No audit-schedule.json found — creating empty schedule")
        schedule_data = {"schedule": {}, "summary": {}}
    else:
        with open(schedule_path) as f:
            schedule_data = json.load(f)
    
    schedule = schedule_data.get("schedule", {})
    current_month = datetime.now().month
    month_names = ["", "January","February","March","April","May","June",
                   "July","August","September","October","November","December"]
    
    print(f"  Loaded schedule ({len(schedule)} pages)")
    
    # ─── Auto-sync: detect new pages, removed pages, owner changes ───
    schedule_changed = False
    page_nids = set()
    
    for page in pages:
        nid_str = str(page["nid"])
        page_nids.add(nid_str)
        owner = page.get("author", "")
        
        if nid_str not in schedule:
            # NEW PAGE: assign to its creation month (audited annually on that month)
            created = page.get("created", "")
            if created:
                try:
                    creation_month = int(created[5:7])  # ISO format: 2026-04-07T...
                except (ValueError, IndexError):
                    creation_month = current_month
            else:
                creation_month = current_month
            schedule[nid_str] = {
                "month": creation_month,
                "month_name": month_names[creation_month],
                "owner": owner,
            }
            schedule_changed = True
            print(f"    + New page NID {nid_str}: '{page['title'][:50]}' → {month_names[creation_month]} (created {created[:10]})")
        
        elif owner and schedule[nid_str].get("owner", "") != owner:
            # OWNER CHANGED: update schedule
            old_owner = schedule[nid_str].get("owner", "(none)")
            schedule[nid_str]["owner"] = owner
            schedule_changed = True
            print(f"    ~ Owner changed NID {nid_str}: {old_owner} → {owner}")
    
    # REMOVED PAGES: pages in schedule but no longer published
    removed = [nid for nid in list(schedule.keys()) if nid not in page_nids]
    if removed:
        for nid in removed:
            del schedule[nid]
        schedule_changed = True
        print(f"    - Removed {len(removed)} unpublished page(s) from schedule")
    
    # Save if changed
    if schedule_changed:
        schedule_data["schedule"] = schedule
        schedule_data["last_synced"] = datetime.now().isoformat()
        with open(schedule_path, "w") as f:
            json.dump(schedule_data, f, indent=2)
        print(f"  Schedule updated and saved ({len(schedule)} pages)")
    else:
        print(f"  Schedule unchanged")
    
    print(f"  Current month: {month_names[current_month]} (month {current_month})")
    
    # Find pages due this month
    due_pages = []
    for page in pages:
        nid_str = str(page["nid"])
        if nid_str in schedule and schedule[nid_str].get("month") == current_month:
            due_pages.append({
                "nid": page["nid"],
                "title": page["title"],
                "url": page["url"],
                "owner": schedule[nid_str].get("owner", page.get("author", "")),
            })
    
    print(f"  {len(due_pages)} pages due for audit this month")
    
    # Send audit reminder emails
    if SEND_EMAILS and MAILERSEND_API_KEY and due_pages:
        # Group by owner
        by_owner = {}
        for p in due_pages:
            owner = p["owner"]
            if not owner or "@" not in owner:
                continue
            if owner not in by_owner:
                by_owner[owner] = []
            by_owner[owner].append(p)
        
        print(f"  Sending audit reminders to {len(by_owner)} authors...")
        scan_month = month_names[current_month]
        
        for owner_email, owner_pages in by_owner.items():
            rows = ""
            for p in owner_pages:
                rows += f'<tr><td style="padding:8px 12px;border-bottom:1px solid #eee"><a href="{p["url"]}" style="color:#182b3c">{p["title"]}</a></td><td style="padding:8px 12px;border-bottom:1px solid #eee;font-size:12px;color:#666">{p["url"].replace("https://www.maine.gov/doe/", "/doe/")}</td></tr>'
            
            html_body = f"""
            <div style="font-family:Calibri,sans-serif;max-width:700px;margin:0 auto">
                <div style="background:#182b3c;padding:20px 24px;border-radius:8px 8px 0 0">
                    <h2 style="color:#fff;margin:0;font-size:20px">Maine DOE Content Audit</h2>
                    <p style="color:#42c3f7;margin:6px 0 0;font-size:14px">{scan_month} audit cycle</p>
                </div>
                <div style="padding:20px 24px;background:#fff;border:1px solid #e9ecef">
                    <p>Hi,</p>
                    <p>The following <strong>{len(owner_pages)} page(s)</strong> are scheduled for their annual content audit.
                    Please review each page within the next <strong>two weeks</strong> to ensure the content is accurate, up-to-date, and all links are working.</p>
                    
                    <p><strong>What to check:</strong></p>
                    <ul>
                        <li><strong>Is the content still accurate and relevant?</strong><br>
                        <span style="color:#555;font-size:13px">Read through all content on the page carefully. Verify that facts, figures, and descriptions are current. Check that any references to dates, school years, or deadlines still make sense. Update any language that no longer reflects current policy or practice.</span></li>

                        <li style="margin-top:10px"><strong>Are all links working?</strong><br>
                        <span style="color:#555;font-size:13px">Check every link on the page to ensure it leads to a valid destination. Even if you receive broken link reports, some external links change without notice and cannot be verified automatically. You can use a tool like <a href="https://chromewebstore.google.com/detail/check-my-links/aajoalonednamcpodaeocebfgldhcpbe" style="color:#42c3f7">Check My Links</a> (while logged out of Drupal) to scan the page. External links should be manually verified by clicking through.</span></li>

                        <li style="margin-top:10px"><strong>Do any files or resources need replacing?</strong><br>
                        <span style="color:#555;font-size:13px">Review linked PDFs, spreadsheets, and other documents to confirm they are still accurate and up to date. Replace any outdated versions with current files.</span></li>

                        <li style="margin-top:10px"><strong>Should any content be removed or consolidated?</strong><br>
                        <span style="color:#555;font-size:13px">To reduce clutter on the website, only include resources and information that provide clear value to visitors. Consider whether items can be combined, whether outdated announcements should be removed, or whether rarely accessed materials should be archived offline. If you feel an entire page is no longer necessary — for example, if the information could live on another page or is no longer relevant — reach out to <a href="mailto:matthew.g.leavitt@maine.gov" style="color:#42c3f7">matthew.g.leavitt@maine.gov</a> and we can consolidate or remove it.</span></li>
                    </ul>
                    
                    <p><strong>After reviewing:</strong></p>
                    <ul>
                        <li>Use the <strong>Revision log</strong> on the side of the edit page to share what updates were made (generally speaking)</li>
                        <li>If no changes are needed, simply enter <em>"This page was fully reviewed"</em> in the Revision log</li>
                        <li>Submit the page for approval</li>
                    </ul>

                    <table style="width:100%;border-collapse:collapse;margin:16px 0">
                        <thead>
                            <tr style="background:#182b3c;color:#fff">
                                <th style="padding:8px 12px;text-align:left">Page Title</th>
                                <th style="padding:8px 12px;text-align:left">Path</th>
                            </tr>
                        </thead>
                        <tbody>{rows}</tbody>
                    </table>
                    
                    <p style="color:#666;font-size:13px">Page(s) will be scheduled for audit again next year.</p>
                </div>
                <div style="background:#f8f9fa;padding:12px 24px;border-radius:0 0 8px 8px;border:1px solid #e9ecef;border-top:none">
                    <p style="margin:0;font-size:12px;color:#666">If you are not the appropriate owner for this page, please forward this email to <a href="mailto:matthew.g.leavitt@maine.gov" style="color:#42c3f7">matthew.g.leavitt@maine.gov</a> so it can be reassigned.</p>
                </div>
            </div>"""
            
            try:
                resp = requests.post(
                    "https://api.mailersend.com/v1/email",
                    json={
                        "from": {"email": FROM_EMAIL, "name": FROM_NAME},
                        "to": [{"email": ADMIN_EMAIL if TEST_MODE else owner_email}],
                        "subject": f"{'[TEST → ' + owner_email + '] ' if TEST_MODE else ''}Action Needed: {len(owner_pages)} Page(s) Due for Content Audit — {scan_month}",
                        "html": html_body,
                    },
                    headers={"Authorization": f"Bearer {MAILERSEND_API_KEY}", "Content-Type": "application/json"},
                    timeout=15,
                )
                if resp.status_code in (200, 201, 202):
                    print(f"    ✓ {owner_email} ({len(owner_pages)} pages)")
                else:
                    print(f"    ✗ {owner_email}: {resp.status_code}")
            except Exception as e:
                print(f"    ✗ {owner_email}: {e}")
            
            time.sleep(0.5)
    
    return due_pages

# ─── Phase 5b: Generate HTML report page ─────────────────────────────────────
def generate_report(dead_links, meta, orphans=None, audit_pages=None):
    print("Generating HTML report...")
    scan_date = datetime.fromisoformat(meta["scan_date"]).strftime("%B %d, %Y")
    by_cat = meta.get("by_category", {})

    # Separate actionable (DOE) from informational (other maine.gov)
    actionable = [d for d in dead_links if d["category"] not in ("OTHER_MAINE_GOV", "EXTERNAL_UNVERIFIABLE")]
    other_maine = [d for d in dead_links if d["category"] == "OTHER_MAINE_GOV"]
    unverifiable = [d for d in dead_links if d["category"] == "EXTERNAL_UNVERIFIABLE"]
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
            cat_colors = {"MISSING_FILE": "#e74c3c", "INTERNAL_404": "#e67e22", "EXTERNAL_DEAD": "#f1c40f", "EXTERNAL_UNVERIFIABLE": "#95a5a6", "OTHER_MAINE_GOV": "#3498db"}
            cat_labels = {"MISSING_FILE": "Missing File", "INTERNAL_404": "Internal 404", "EXTERNAL_DEAD": "External Dead", "EXTERNAL_UNVERIFIABLE": "Unverifiable", "OTHER_MAINE_GOV": "Other Maine.gov"}
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
        cat_icons = {"MISSING_FILE": "\U0001f534", "INTERNAL_404": "\U0001f7e0", "EXTERNAL_DEAD": "\U0001f7e1", "EXTERNAL_UNVERIFIABLE": "\u26AA"}
        cat_labels = {"MISSING_FILE": "Missing Files", "INTERNAL_404": "Internal 404s", "EXTERNAL_DEAD": "External Dead", "EXTERNAL_UNVERIFIABLE": "Unverifiable"}
        for cat in ["MISSING_FILE", "INTERNAL_404", "EXTERNAL_DEAD", "EXTERNAL_UNVERIFIABLE"]:
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

    # Build audit section HTML
    if audit_pages:
        month_names = ["","January","February","March","April","May","June",
                       "July","August","September","October","November","December"]
        current_month = datetime.now().month
        audit_by_owner = {}
        for p in audit_pages:
            o = p.get("owner", "(no owner)")
            if o not in audit_by_owner:
                audit_by_owner[o] = []
            audit_by_owner[o].append(p)
        
        audit_rows = ""
        for owner, pages_list in sorted(audit_by_owner.items()):
            for i, p in enumerate(pages_list):
                owner_cell = f'<td style="padding:6px 12px;border-bottom:1px solid rgba(109,139,166,0.1);font-weight:600">{owner.replace("@maine.gov","")}</td>' if i == 0 else '<td style="padding:6px 12px;border-bottom:1px solid rgba(109,139,166,0.1)"></td>'
                audit_rows += f"""<tr>
                  {owner_cell}
                  <td style="padding:6px 12px;border-bottom:1px solid rgba(109,139,166,0.1)"><a href="{p['url']}" target="_blank" style="color:#42c3f7;text-decoration:underline">{p['title']}</a></td>
                </tr>"""
        
        audit_html = f"""
        <div style="margin-top:24px;background:rgba(27,58,84,0.4);border:1px solid rgba(109,139,166,0.15);border-radius:10px;padding:20px">
          <h2 style="font-size:18px;color:#fff;margin:0 0 8px">📋 Content Audit — {month_names[current_month]}</h2>
          <p style="font-size:13px;color:#8ab4d4;margin-bottom:12px">{len(audit_pages)} pages due for annual review this month across {len(audit_by_owner)} authors</p>
          <div style="overflow-x:auto">
          <table style="width:100%;border-collapse:collapse;font-size:13px">
            <thead><tr style="background:#1b3a54">
              <th style="padding:8px 12px;text-align:left;color:#42c3f7">Author</th>
              <th style="padding:8px 12px;text-align:left;color:#42c3f7">Page</th>
            </tr></thead>
            <tbody>{audit_rows}</tbody>
          </table>
          </div>
        </div>"""
    else:
        audit_html = ""

    # Build orphan section HTML
    if orphans:
        orphan_total_mb = sum(f.get("filesize") or 0 for f in orphans) / 1024 / 1024
        orphan_rows = ""
        for of in orphans[:50]:  # Show top 50 by size
            size_kb = (of.get("filesize") or 0) / 1024
            size_str = f"{size_kb:.0f} KB" if size_kb < 1024 else f"{size_kb/1024:.1f} MB"
            orphan_rows += f"""<tr>
              <td style="padding:6px 12px;border-bottom:1px solid rgba(109,139,166,0.1)">{of.get("fid","")}</td>
              <td style="padding:6px 12px;border-bottom:1px solid rgba(109,139,166,0.1)">{of.get("filename","")}</td>
              <td style="padding:6px 12px;border-bottom:1px solid rgba(109,139,166,0.1)">{size_str}</td>
              <td style="padding:6px 12px;border-bottom:1px solid rgba(109,139,166,0.1);font-size:11px;color:#6d8ba6">{of.get("created","")[:10]}</td>
            </tr>"""
        orphan_html = f"""
        <div style="margin-top:24px;background:rgba(27,58,84,0.4);border:1px solid rgba(109,139,166,0.15);border-radius:10px;padding:20px">
          <h2 style="font-size:18px;color:#fff;margin:0 0 8px">📁 Orphan Files</h2>
          <p style="font-size:13px;color:#8ab4d4;margin-bottom:12px">{len(orphans)} files not referenced in any page body ({orphan_total_mb:.1f} MB total)</p>
          <div style="overflow-x:auto">
          <table style="width:100%;border-collapse:collapse;font-size:13px">
            <thead><tr style="background:#1b3a54">
              <th style="padding:8px 12px;text-align:left;color:#42c3f7">FID</th>
              <th style="padding:8px 12px;text-align:left;color:#42c3f7">Filename</th>
              <th style="padding:8px 12px;text-align:left;color:#42c3f7">Size</th>
              <th style="padding:8px 12px;text-align:left;color:#42c3f7">Uploaded</th>
            </tr></thead>
            <tbody>{orphan_rows}</tbody>
          </table>
          </div>
          {"<p style='font-size:12px;color:#6d8ba6;margin-top:8px'>Showing top 50 by size. Full list in orphan-files.json.</p>" if len(orphans) > 50 else ""}
          <p style="font-size:12px;color:#6d8ba6;margin-top:8px">Use the Orphan File Finder tool on maine.gov/doe to review and delete these files.</p>
        </div>"""
    else:
        orphan_html = ""

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
    {meta["pages_scanned"]} pages scanned &middot; {meta["urls_checked"]} URLs checked
  </p>
</div>
<div style="padding:24px">
  {status_html}
</div>
<div style="padding:0 24px 24px">
  {audit_html}
  {orphan_html}
</div>
<div style="padding:0 24px 24px;font-size:12px;color:#3a5068">
  {f'<p style="color:#6d8ba6;margin-bottom:8px">Also excluded from this report: {len(unverifiable)} unverifiable external link(s) (sites that block automated checks) and {len(other_maine)} broken link(s) to other maine.gov departments outside DOE.</p>' if (other_maine or unverifiable) else ''}
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
    VERSION = "2.0 — 2026-04-06 9:30 PM ET"
    print(f"═══ Maine DOE Dead Link Scanner v{VERSION} ═══")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"External checks: {'enabled' if CHECK_EXTERNAL else 'disabled'}")
    print(f"Email sending: {'enabled' if SEND_EMAILS else 'disabled'}")
    if TEST_MODE:
        print(f"⚠ TEST MODE: All emails will go to {ADMIN_EMAIL} instead of real authors")
    print()

    # Load verified-alive allowlist (URLs to skip checking)
    allowlist = set()
    allowlist_path = os.path.join(os.path.dirname(__file__), "verified-alive.json")
    if os.path.exists(allowlist_path):
        with open(allowlist_path) as f:
            al_data = json.load(f)
            allowlist = set(al_data.get("urls", []))
        print(f"  Loaded verified-alive.json ({len(allowlist)} allowlisted URLs)")
    else:
        print("  No verified-alive.json found")

    pages = fetch_all_pages()
    if not pages:
        print("No pages found — aborting")
        sys.exit(1)

    link_map = extract_all_links(pages)
    check_results = check_all_urls(link_map, allowlist)
    dead_links, meta = build_results(link_map, check_results, len(pages))
    save_results(dead_links, meta)
    orphans = find_orphan_files(pages)
    save_orphan_results(orphans)
    audit_pages = check_content_audits(pages)
    generate_report(dead_links, meta, orphans, audit_pages)
    send_author_emails(dead_links, meta)

    # Send orphan notification to admin only
    if SEND_EMAILS and MAILERSEND_API_KEY and orphans:
        print(f"\nSending orphan file notification to {ADMIN_EMAIL}...")
        orphan_total_mb = sum(f.get("filesize") or 0 for f in orphans) / 1024 / 1024
        orphan_rows = ""
        for of in orphans[:30]:
            size_kb = (of.get("filesize") or 0) / 1024
            size_str = f"{size_kb:.0f} KB" if size_kb < 1024 else f"{size_kb/1024:.1f} MB"
            orphan_rows += f'<tr><td style="padding:6px 12px;border-bottom:1px solid #eee">{of.get("filename","")}</td><td style="padding:6px 12px;border-bottom:1px solid #eee">{size_str}</td><td style="padding:6px 12px;border-bottom:1px solid #eee;font-size:12px;color:#666">{of.get("created","")[:10]}</td></tr>'

        orphan_email = f"""
        <div style="font-family:Calibri,sans-serif;max-width:700px;margin:0 auto">
            <div style="background:#182b3c;padding:20px 24px;border-radius:8px 8px 0 0">
                <h2 style="color:#fff;margin:0">📁 Orphan Files Detected</h2>
                <p style="color:#42c3f7;margin:6px 0 0;font-size:14px">Monthly scan — {datetime.now().strftime("%B %d, %Y")}</p>
            </div>
            <div style="padding:20px 24px;background:#fff;border:1px solid #e9ecef">
                <p>{len(orphans)} managed files ({orphan_total_mb:.1f} MB) are not referenced in any page body.
                These may be safe to delete.</p>
                <table style="width:100%;border-collapse:collapse;margin:16px 0">
                    <thead><tr style="background:#182b3c;color:#fff">
                        <th style="padding:8px 12px;text-align:left">Filename</th>
                        <th style="padding:8px 12px;text-align:left">Size</th>
                        <th style="padding:8px 12px;text-align:left">Uploaded</th>
                    </tr></thead>
                    <tbody>{orphan_rows}</tbody>
                </table>
                {"<p style=\'font-size:13px;color:#666\'>Showing top 30. Full list in orphan-files.json on GitHub.</p>" if len(orphans) > 30 else ""}
                <p style="font-size:13px;color:#666">Review and delete using the Orphan File Finder tool.</p>
            </div>
        </div>"""

        try:
            resp = requests.post(
                "https://api.mailersend.com/v1/email",
                json={
                    "from": {"email": FROM_EMAIL, "name": FROM_NAME},
                    "to": [{"email": ADMIN_EMAIL}],
                    "subject": f"📁 {len(orphans)} Orphan Files Detected on maine.gov/doe",
                    "html": orphan_email,
                },
                headers={"Authorization": f"Bearer {MAILERSEND_API_KEY}", "Content-Type": "application/json"},
                timeout=15,
            )
            if resp.status_code in (200, 201, 202):
                print(f"  ✓ Orphan notification sent to {ADMIN_EMAIL}")
            else:
                print(f"  ✗ Failed: {resp.status_code} {resp.text[:100]}")
        except Exception as e:
            print(f"  ✗ Error: {e}")

    elapsed = time.time() - start
    print(f"\n═══ Complete in {elapsed:.0f}s ═══")


if __name__ == "__main__":
    main()
