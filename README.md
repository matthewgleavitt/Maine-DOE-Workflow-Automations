# Maine DOE Dead Link Scanner

Automated monthly scan of all published pages on maine.gov/doe for broken links, with email notifications to page authors.

## What It Does

1. **Fetches** all published `multi_column_page` nodes via JSON:API (~900+ pages)
2. **Extracts** every `<a href>` and `<img src>` from each page body
3. **Checks** each URL via HEAD request (internal + external, no CORS restrictions)
4. **Categorizes** broken links: Missing Files, Internal 404s, External Dead
5. **Emails** each page author with a list of broken links on their pages
6. **Saves** results as JSON for the browser-based triage tool

## Setup

### 1. Create the GitHub repo

Create a new private repo (e.g., `doe-dead-link-scanner`) and push these files:

```
doe-dead-link-scanner/
├── .github/workflows/scan.yml
├── scan.py
└── README.md
```

### 2. Add SendGrid API key

In your repo: **Settings → Secrets and variables → Actions → New repository secret**

| Name | Value |
|------|-------|
| `SENDGRID_API_KEY` | Your SendGrid API key |

Optional repo variables (Settings → Variables):

| Name | Default | Description |
|------|---------|-------------|
| `FROM_EMAIL` | matthew.g.leavitt@maine.gov | Sender email address |
| `FROM_NAME` | Maine DOE Web Team | Sender display name |

### 3. Run it

- **Manual run:** Go to Actions tab → "Dead Link Scanner" → "Run workflow"
  - Choose whether to check external links and/or send emails
  - First run: set `send_emails` to `false` to preview results before emailing anyone
- **Scheduled:** Runs automatically on the 1st of every month at 6am ET

### 4. View results

After each run:
- **`scan-results.json`** — Full detailed results (committed to repo)
- **`scan-results-compact.json`** — Compact format for the browser triage tool
- **Artifacts** — Downloadable from the Actions tab (retained 90 days)

## Using with the Browser Triage Tool

The `scan-results-compact.json` file can be loaded by the browser-based Dead Link Triage tool deployed on maine.gov/doe. Copy the JSON to your FTP tools folder and the tool will pick it up.

## Configuration

Environment variables (set in workflow or repo settings):

| Variable | Default | Description |
|----------|---------|-------------|
| `CHECK_EXTERNAL` | `true` | Check external links (set `false` for faster internal-only scans) |
| `SEND_EMAILS` | `false` | Send email notifications to authors |
| `SENDGRID_API_KEY` | — | Required for email sending |
| `FROM_EMAIL` | matthew.g.leavitt@maine.gov | Sender email |
| `FROM_NAME` | Maine DOE Web Team | Sender name |

## Scan Duration

- Internal only: ~2-5 minutes
- Internal + external: ~10-20 minutes (depending on external site response times)

GitHub Actions provides 2,000 free minutes/month for private repos, so monthly scans are well within limits.
