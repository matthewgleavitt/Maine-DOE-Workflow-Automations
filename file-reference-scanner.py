"""
File Reference Scanner for Maine DOE
Scans all Drupal pages, extracts file references from body HTML,
builds a file→pages map and writes to Google Sheet.
Replaces the slow Apps Script scanBatch approach.

Runs via GitHub Actions weekly.
"""

import os
import re
import json
import sys
import time
import requests
from urllib.parse import unquote
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── Config ──
SA_KEY = json.loads(os.environ.get('GOOGLE_SA_KEY', '{}'))
SHEET_ID = os.environ.get('CACHE_SHEET_ID', '')
DRUPAL_BASE = 'https://www.maine.gov/doe/jsonapi'
FILE_PATTERN = re.compile(r'/sites/maine\.gov\.doe/files/[^"\'<>\s\)]+', re.IGNORECASE)

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_info(SA_KEY, scopes=SCOPES)
    return build('sheets', 'v4', credentials=creds)


def fetch_all_pages():
    """Fetch all multi_column_page nodes with body content."""
    pages = []
    url = (
        f"{DRUPAL_BASE}/node/multi_column_page"
        f"?fields[node--multi_column_page]=title,path,body,field_page_owner_email,drupal_internal__nid"
        f"&page[limit]=50&sort=-changed"
    )
    page_num = 0

    while url and page_num < 100:
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code != 200:
                print(f"  API error {resp.status_code} at page {page_num}")
                break
            data = resp.json()

            for node in data.get('data', []):
                attrs = node.get('attributes', {})
                body_html = ''
                if attrs.get('body') and attrs['body'].get('value'):
                    body_html = attrs['body']['value']

                path_alias = ''
                if attrs.get('path') and attrs['path'].get('alias'):
                    path_alias = attrs['path']['alias']

                email = attrs.get('field_page_owner_email', '') or ''
                owner = ''
                if '@' in email:
                    parts = email.split('@')[0].split('.')
                    owner = ' '.join(p.capitalize() for p in parts)

                pages.append({
                    'nid': attrs.get('drupal_internal__nid', ''),
                    'title': attrs.get('title', 'Untitled'),
                    'path': f'/doe{path_alias}' if path_alias else '',
                    'owner': owner,
                    'owner_email': email,
                    'body': body_html,
                })

            # Next page
            next_link = data.get('links', {}).get('next', {})
            url = next_link.get('href') if isinstance(next_link, dict) else None
            if url:
                url = url.replace('http:', 'https:')
            page_num += 1

            if page_num % 5 == 0:
                print(f"  Fetched {len(pages)} pages ({page_num} API calls)...")

        except Exception as e:
            print(f"  Error at page {page_num}: {e}")
            break

    print(f"  Total pages fetched: {len(pages)}")
    return pages


def extract_file_references(pages):
    """Parse body HTML of each page to find file references."""
    file_map = {}  # file_path → [{title, path, owner}]
    total_refs = 0

    for page in pages:
        body = page['body']
        if not body:
            continue

        # Find all file paths in body HTML
        matches = FILE_PATTERN.findall(body)

        # Deduplicate per page
        seen = set()
        for match in matches:
            # Normalize: decode URL encoding, lowercase for matching
            decoded = unquote(match).strip()
            # Remove query strings and anchors
            clean = decoded.split('?')[0].split('#')[0]

            if clean in seen:
                continue
            seen.add(clean)

            # Store with /doe prefix for consistency
            file_path = f'/doe{clean}' if not clean.startswith('/doe') else clean

            if file_path not in file_map:
                file_map[file_path] = []

            file_map[file_path].append({
                't': page['title'],
                'p': page['path'],
                'o': page['owner'],
            })
            total_refs += 1

    print(f"  Found {len(file_map)} unique files referenced across {total_refs} total references")
    return file_map


def get_filename_from_path(path):
    """Extract filename from a file path."""
    parts = path.rstrip('/').split('/')
    return unquote(parts[-1]) if parts else ''


def write_to_sheet(sheets, file_map):
    """Write file reference data to Google Sheet FilePages tab."""
    # Clear existing data
    try:
        sheets.spreadsheets().values().clear(
            spreadsheetId=SHEET_ID,
            range='FilePages!A:D'
        ).execute()
    except Exception:
        pass

    # Build rows
    rows = [['file_path', 'file_name', 'pages_json', 'scanned_at']]
    scan_time = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

    for file_path, page_refs in file_map.items():
        filename = get_filename_from_path(file_path)
        # Store the URI portion (without /doe prefix) for consistency with existing data
        uri = file_path.replace('/doe', '', 1) if file_path.startswith('/doe') else file_path
        rows.append([
            uri,
            filename,
            json.dumps(page_refs),
            scan_time
        ])

    # Write in chunks (Sheets API limit is 10MB per request)
    CHUNK_SIZE = 2000
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i:i + CHUNK_SIZE]
        start_row = i + 1
        end_row = start_row + len(chunk) - 1
        range_str = f'FilePages!A{start_row}:D{end_row}'

        sheets.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=range_str,
            valueInputOption='RAW',
            body={'values': chunk}
        ).execute()

    print(f"  Wrote {len(rows) - 1} file references to Sheet")
    return len(rows) - 1


def update_scan_properties(sheets, file_count):
    """Update ScanMeta tab with last scan info."""
    try:
        # Clear and write ScanMeta
        try:
            sheets.spreadsheets().values().clear(
                spreadsheetId=SHEET_ID,
                range='ScanMeta!A:B'
            ).execute()
        except Exception:
            pass

        scan_time = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        sheets.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range='ScanMeta!A1:B2',
            valueInputOption='RAW',
            body={'values': [
                ['last_scan', 'total_files'],
                [scan_time, file_count]
            ]}
        ).execute()
    except Exception as e:
        print(f"  ScanMeta update failed: {e}")


def main():
    print("=" * 50)
    print("File Reference Scanner")
    print("=" * 50)

    print("\n1. Fetching all pages from Drupal...")
    pages = fetch_all_pages()

    if not pages:
        print("No pages found. Exiting.")
        sys.exit(1)

    print("\n2. Extracting file references from page bodies...")
    file_map = extract_file_references(pages)

    print("\n3. Writing results to Google Sheet...")
    sheets = get_sheets_service()
    file_count = write_to_sheet(sheets, file_map)

    print("\n4. Updating scan metadata...")
    update_scan_properties(sheets, file_count)

    print(f"\nDone! {file_count} files mapped across {len(pages)} pages.")


if __name__ == '__main__':
    main()
