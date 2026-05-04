"""
YouTube Auto-Uploader for Maine DOE
Checks Google Drive folder for new videos, matches to Sheet submissions,
uploads to YouTube as Private, moves to Processed folder.
Runs via GitHub Actions every 10 minutes.
"""

import os
import sys
import json
import time
import tempfile
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ── Config from environment ──
SA_KEY = json.loads(os.environ.get('GOOGLE_SA_KEY', '{}'))
YOUTUBE_CLIENT_ID = os.environ.get('YOUTUBE_CLIENT_ID', '')
YOUTUBE_CLIENT_SECRET = os.environ.get('YOUTUBE_CLIENT_SECRET', '')
YOUTUBE_REFRESH_TOKEN = os.environ.get('YOUTUBE_REFRESH_TOKEN', '')
SHEET_ID = os.environ.get('CACHE_SHEET_ID', '')
TEAMS_WEBHOOK = os.environ.get('TEAMS_WEBHOOK', '')
DRIVE_FOLDER_ID = '1i5Hp9HSxyya3sgVA4HSMmMosIYRCLlv1'

VIDEO_EXTENSIONS = {'.mp4', '.mov', '.avi', '.wmv', '.flv', '.mkv', '.webm', '.m4v', '.mpg', '.mpeg', '.3gp'}

SCOPES_DRIVE = ['https://www.googleapis.com/auth/drive']
SCOPES_SHEETS = ['https://www.googleapis.com/auth/spreadsheets']


def get_drive_service():
    """Build Google Drive service using service account."""
    creds = service_account.Credentials.from_service_account_info(SA_KEY, scopes=SCOPES_DRIVE)
    return build('drive', 'v3', credentials=creds)


def get_sheets_service():
    """Build Google Sheets service using service account."""
    creds = service_account.Credentials.from_service_account_info(SA_KEY, scopes=SCOPES_SHEETS)
    return build('sheets', 'v4', credentials=creds)


def get_youtube_service():
    """Build YouTube service using OAuth2 refresh token."""
    # Exchange refresh token for access token
    resp = requests.post('https://oauth2.googleapis.com/token', data={
        'client_id': YOUTUBE_CLIENT_ID,
        'client_secret': YOUTUBE_CLIENT_SECRET,
        'refresh_token': YOUTUBE_REFRESH_TOKEN,
        'grant_type': 'refresh_token'
    })
    if resp.status_code != 200:
        print(f"Failed to refresh YouTube token: {resp.status_code} {resp.text}")
        sys.exit(1)

    access_token = resp.json()['access_token']

    from google.oauth2.credentials import Credentials
    creds = Credentials(
        token=access_token,
        refresh_token=YOUTUBE_REFRESH_TOKEN,
        token_uri='https://oauth2.googleapis.com/token',
        client_id=YOUTUBE_CLIENT_ID,
        client_secret=YOUTUBE_CLIENT_SECRET
    )
    return build('youtube', 'v3', credentials=creds)


def get_pending_videos(drive):
    """List video files in the upload folder (not in Processed subfolder)."""
    # Find the Processed folder ID
    processed_query = f"'{DRIVE_FOLDER_ID}' in parents and name = 'Processed' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    processed_results = drive.files().list(q=processed_query, fields='files(id)').execute()
    processed_id = processed_results['files'][0]['id'] if processed_results['files'] else None

    # List files in upload folder
    query = f"'{DRIVE_FOLDER_ID}' in parents and trashed = false and mimeType != 'application/vnd.google-apps.folder'"
    results = drive.files().list(q=query, fields='files(id, name, size, mimeType)').execute()
    files = results.get('files', [])

    videos = []
    for f in files:
        name = f['name']
        ext = os.path.splitext(name)[1].lower()
        if ext in VIDEO_EXTENSIONS:
            videos.append(f)

    return videos, processed_id


def match_to_submission(filename, sheets):
    """Match a video filename to a YouTube submission in the Sheet."""
    try:
        result = sheets.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range='YouTubeSubmissions!A:O'
        ).execute()
        rows = result.get('values', [])
    except Exception as e:
        print(f"Sheet read error: {e}")
        return {}

    clean_name = os.path.splitext(filename)[0].lower().strip()

    # Search from bottom (newest) up
    for i in range(len(rows) - 1, 0, -1):
        row = rows[i]
        if len(row) < 13:
            continue

        row_title = str(row[4]).lower().strip() if len(row) > 4 else ''
        row_filename = str(row[12]).lower().strip() if len(row) > 12 else ''

        # Exact filename match
        if row_filename and (row_filename == clean_name or
                            row_filename == filename.lower() or
                            clean_name.startswith(row_filename) or
                            row_filename.startswith(clean_name)):
            return {
                'title': str(row[4]) if len(row) > 4 else '',
                'description': str(row[5]) if len(row) > 5 else '',
                'requestor': str(row[1]) if len(row) > 1 else '',
                'email': str(row[2]) if len(row) > 2 else '',
                'privacy': str(row[8]) if len(row) > 8 else 'private',
                'row_index': i + 1  # 1-indexed for Sheets API
            }

        # Fuzzy match on title
        if row_title and (clean_name in row_title or row_title in clean_name):
            return {
                'title': str(row[4]) if len(row) > 4 else '',
                'description': str(row[5]) if len(row) > 5 else '',
                'requestor': str(row[1]) if len(row) > 1 else '',
                'email': str(row[2]) if len(row) > 2 else '',
                'privacy': str(row[8]) if len(row) > 8 else 'private',
                'row_index': i + 1
            }

    return {}


def upload_to_youtube(youtube, filepath, title, description):
    """Upload a video file to YouTube as Private."""
    body = {
        'snippet': {
            'title': title,
            'description': description,
            'categoryId': '27'  # Education
        },
        'status': {
            'privacyStatus': 'private',
            'selfDeclaredMadeForKids': False
        }
    }

    media = MediaFileUpload(
        filepath,
        resumable=True,
        chunksize=25 * 1024 * 1024  # 25MB chunks
    )

    request = youtube.videos().insert(
        part='snippet,status',
        body=body,
        media_body=media
    )

    print(f"  Uploading to YouTube...")
    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            pct = int(status.progress() * 100)
            print(f"  Progress: {pct}%")

    video_id = response['id']
    print(f"  Upload complete: https://www.youtube.com/watch?v={video_id}")
    return video_id


def update_sheet_status(sheets, row_index, video_id):
    """Update the submission status and add YouTube URL."""
    try:
        # Update status column (N = column 14, 0-indexed = 13)
        sheets.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f'YouTubeSubmissions!N{row_index}',
            valueInputOption='RAW',
            body={'values': [['Uploaded']]}
        ).execute()

        # Add YouTube URL to column O
        sheets.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f'YouTubeSubmissions!O{row_index}',
            valueInputOption='RAW',
            body={'values': [[f'https://youtu.be/{video_id}']]}
        ).execute()
        print(f"  Sheet updated: row {row_index}")
    except Exception as e:
        print(f"  Sheet update failed: {e}")


def move_to_processed(drive, file_id, processed_folder_id):
    """Move file from upload folder to Processed subfolder."""
    drive.files().update(
        fileId=file_id,
        addParents=processed_folder_id,
        removeParents=DRIVE_FOLDER_ID,
        fields='id, parents'
    ).execute()


def notify_teams(title, video_id, requestor=''):
    """Send Teams webhook notification."""
    if not TEAMS_WEBHOOK:
        return
    try:
        message = (
            f"🎬 **Auto-uploaded to YouTube**\n\n"
            f"**Title:** {title}\n"
            f"**Video:** [View on YouTube](https://www.youtube.com/watch?v={video_id})\n"
            f"**Status:** Private (review in YouTube Studio)\n"
            f"{f'**Requested by:** {requestor}' if requestor else ''}\n\n"
            f"_Review metadata and set to Public/Unlisted when ready._"
        )
        requests.post(TEAMS_WEBHOOK, json={
            'type': 'message',
            'attachments': [{
                'contentType': 'application/vnd.microsoft.card.adaptive',
                'content': {
                    'type': 'AdaptiveCard',
                    'version': '1.2',
                    'body': [{'type': 'TextBlock', 'text': message, 'wrap': True}]
                }
            }]
        })
    except Exception as e:
        print(f"  Teams notification failed: {e}")


def main():
    print("YouTube Auto-Uploader starting...")

    # Build services
    drive = get_drive_service()
    sheets = get_sheets_service()
    youtube = get_youtube_service()

    # Get pending videos
    videos, processed_id = get_pending_videos(drive)

    if not videos:
        print("No new videos found.")
        return

    if not processed_id:
        # Create Processed folder
        folder_meta = {
            'name': 'Processed',
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [DRIVE_FOLDER_ID]
        }
        folder = drive.files().create(body=folder_meta, fields='id').execute()
        processed_id = folder['id']
        print("Created Processed folder")

    print(f"Found {len(videos)} video(s) to process")

    for video in videos:
        name = video['name']
        file_id = video['id']
        size_mb = int(video.get('size', 0)) / 1048576

        print(f"\nProcessing: {name} ({size_mb:.1f} MB)")

        # Match to Sheet submission
        meta = match_to_submission(name, sheets)
        title = meta.get('title') or os.path.splitext(name)[0]
        description = meta.get('description') or 'Uploaded via Maine DOE Communications Portal'
        requestor = meta.get('requestor', '')

        print(f"  Title: {title}")
        print(f"  Matched to submission: {'Yes (row ' + str(meta.get('row_index', '?')) + ')' if meta else 'No — using filename as title'}")

        # Download from Drive to temp file
        with tempfile.NamedTemporaryFile(suffix=os.path.splitext(name)[1], delete=False) as tmp:
            tmp_path = tmp.name
            print(f"  Downloading from Drive...")
            request = drive.files().get_media(fileId=file_id)
            from googleapiclient.http import MediaIoBaseDownload
            import io
            fh = io.FileIO(tmp_path, 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    print(f"  Download: {int(status.progress() * 100)}%")
            fh.close()

        try:
            # Upload to YouTube
            video_id = upload_to_youtube(youtube, tmp_path, title, description)

            # Move to Processed
            move_to_processed(drive, file_id, processed_id)
            print(f"  Moved to Processed folder")

            # Update Sheet
            if meta.get('row_index'):
                update_sheet_status(sheets, meta['row_index'], video_id)

            # Notify Teams
            notify_teams(title, video_id, requestor)

        except Exception as e:
            print(f"  UPLOAD FAILED: {e}")
        finally:
            # Clean up temp file
            try:
                os.unlink(tmp_path)
            except:
                pass

    print("\nDone!")


if __name__ == '__main__':
    main()
