# Healthcare Regulatory & Policy Monitor

An automated weekly briefing that tracks 30+ healthcare regulatory and policy sources — Federal Register notices, congressional bills, FDA actions, CMS updates, OIG reports, court cases, and industry news — and publishes two audience-tailored editions every Monday with zero manual effort.

**Live site → [kas1172-dotcom.github.io/Healthcare_WeeklyNewsletter_Kapil](https://kas1172-dotcom.github.io/Healthcare_WeeklyNewsletter_Kapil/)**

---

## How it works

```
pipeline/scrape.py → raw_articles.json → pipeline/generate.py → newsletter_data.json (+ newsletter_archive.json) → index.html
```

1. **Scrape** — fetches government APIs and RSS feeds in parallel, extracts the real publication date for each item (marking any it can't resolve as *date unknown* rather than faking it), stamps a discovery date, deduplicates, and saves to `raw_articles.json`
2. **Classify** — tags each article with a category, urgency tier, a 0–100 importance score, audience-specific relevance scores, a specific "so what / now what" analysis, calibrated uncertainty, and — for policy items — a balanced multi-stakeholder read (who benefits, who bears the cost, the case each side makes)
3. **Synthesize** — generates an editor's note and theme of the week for each edition
4. **Archive & diff** — appends the run to `newsletter_archive.json` (a rolling history, capped at the last 26 runs ≈ 6 months), then computes a *"what's new since last run"* digest for each edition
5. **Publish** — commits `newsletter_data.json` and `newsletter_archive.json`; GitHub Pages serves the static frontend

The GitHub Actions workflow runs automatically every Monday (13:00 UTC = 8am EST / 9am EDT). You can also trigger it manually from the Actions tab.

---

## Editions

| Edition | Audience |
|---|---|
| **Consulting** | Healthcare strategy consultants, compliance officers, and executives |
| **Policy** | Policy professionals, legislative staff, lobbyists, and government affairs leads |

Both editions draw from the same source pool but are classified and written for their respective audiences.

---

## Setup

### 1. Fork or clone the repo

### 2. Add repository secrets

Go to **Settings → Secrets and variables → Actions** and add:

| Secret | Description |
|---|---|
| `ANTHROPIC_API_KEY` | NLP API key for article classification and synthesis |
| `CONGRESS_API_KEY` | [congress.gov API key](https://api.congress.gov/sign-up/) for bill tracking |

### 3. Enable GitHub Pages

Go to **Settings → Pages** and set the source to **Deploy from a branch → main → / (root)**.

### 4. That's it

The workflow runs automatically every Monday. Trigger the first run manually from **Actions → Weekly Newsletter → Run workflow** to populate the site.

---

## Running locally

```bash
# Install dependencies
pip install requests feedparser anthropic beautifulsoup4

# Step 1: scrape all sources
python pipeline/scrape.py

# Step 2: classify and generate newsletter
ANTHROPIC_API_KEY=your_key python pipeline/generate.py

# Step 3: validate output
python pipeline/validate.py
```

Open `index.html` directly in a browser to preview the result.

---

## Project structure

```
.
├── .github/
│   └── workflows/
│       └── weekly-newsletter.yml   # Scheduled GitHub Actions pipeline
├── pipeline/
│   ├── scrape.py                   # Fetches all sources → raw_articles.json
│   ├── generate.py                 # Classifies + synthesizes → newsletter_data.json
│   └── validate.py                 # Validates both JSON outputs
├── index.html                      # Main dashboard frontend (GitHub Pages)
├── how-it-works.html               # Pipeline documentation page
├── newsletter_data.json            # Current edition data (auto-updated weekly)
└── newsletter_archive.json         # Rolling run history → powers "what's new"
```

---

## Sources

**Government APIs**
- Federal Register (proposed & final rules)
- Congress.gov (bills and legislative activity)
- Congressional Research Service reports
- FDA device clearances and drug approvals
- CMS newsroom
- OIG enforcement actions and reports
- PACER court cases

**RSS / Web**
- FDA press releases and MedWatch
- CMS blog and innovations
- HHS news
- Becker's Hospital Review
- Healthcare Dive
- Health Affairs
- Modern Healthcare
- STAT News
- Kaiser Health News
- American Hospital Association
- AHIP, HFMA, MGMA
- The Capitol Forum, Bloomberg Law Health, Politico Pulse

---

## Customization

| What | Where |
|---|---|
| Add/remove sources | `pipeline/scrape.py` → `fetch_all()` |
| Change categories | `pipeline/generate.py` → `CONSULTING_CATEGORIES` / `POLICY_CATEGORIES` |
| Adjust lookback window | `python pipeline/scrape.py --days-back 14` |
| Change run schedule | `.github/workflows/weekly-newsletter.yml` → `cron:` |
| Disable auto-push | `python pipeline/generate.py --no-push` |
| Archive retention (runs kept) | `pipeline/generate.py` → `ARCHIVE_MAX_RUNS` (or env var) |
| Max articles classified per edition | `pipeline/generate.py` → `MAX_ARTICLES_PER_EDITION` (or env var) |
