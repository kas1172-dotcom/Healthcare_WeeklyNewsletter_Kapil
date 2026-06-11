#!/usr/bin/env python3
"""
Healthcare Regulatory & Policy Monitor — Step 2: Newsletter Generator
=======================================================
Reads raw_articles.json, classifies and synthesizes articles,
generates two newsletter editions, pushes to GitHub.

SETUP:
    pip3 install anthropic

CONFIGURE:
    Set ANTHROPIC_API_KEY and optionally GITHUB_TOKEN/GITHUB_REPO using environment variables.

RUN:
    python3 generate.py

REQUIRES:
    raw_articles.json (from scrape.py)
"""

import argparse
import base64
import json
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import requests
from anthropic import Anthropic

# ── CONFIG ───────────────────────────────────────────────────────────────────
CLASSIFY_MODEL   = os.environ.get("ANTHROPIC_CLASSIFY_MODEL", "claude-haiku-4-5-20251001")
EDITORIAL_MODEL  = os.environ.get("ANTHROPIC_MODEL",          "claude-sonnet-4-6")
GITHUB_FILE_PATH = "newsletter_data.json"
ARCHIVE_FILE     = "newsletter_archive.json"

# Retain at most this many weekly runs in the archive run-list.
# Older run records are pruned; their articles remain in the article dict for de-dup.
# 26 runs ≈ 6 months. Override with env var ARCHIVE_MAX_RUNS.
ARCHIVE_MAX_RUNS = int(os.environ.get("ARCHIVE_MAX_RUNS", "26"))

# Articles from prior runs are carried forward ("persisted") into the current newsletter
# if they meet urgency/deadline criteria and are within this many days of first_seen.
# Deadline items (future comment_deadline or effective_date) persist regardless of window.
PERSIST_DAYS = int(os.environ.get("PERSIST_DAYS", "30"))

# Max articles classified per edition. Set well above typical weekly volume (~170 total)
# so it effectively never truncates — the frontend triages via tiers/drawers, so we
# classify everything rather than silently dropping items before they can be archived.
MAX_ARTICLES_PER_EDITION = int(os.environ.get("MAX_ARTICLES_PER_EDITION", "150"))
# How many regulatory items may spill into the policy pool to round it out.
POLICY_SPILLOVER_FROM_REG = 30

# Classification throughput — tuned to stay under the API output-tokens-per-MINUTE
# rate limit (default org tier is 10k/min). This is a weekly background job, so we
# favor reliability over speed: run batches serially and pace them so sustained
# output stays under the limit. ~8 articles/batch emits ~2–2.5k output tokens, so a
# batch every ~16s ≈ 9–9.5k tokens/min — comfortably under 10k.
# Raise the tier on the Anthropic console to go faster; lower the interval if so.
CLASSIFY_BATCH        = int(os.environ.get("CLASSIFY_BATCH", "8"))
CLASSIFY_MAX_TOKENS   = int(os.environ.get("CLASSIFY_MAX_TOKENS", "4000"))
CLASSIFY_WORKERS      = int(os.environ.get("CLASSIFY_WORKERS", "1"))
CLASSIFY_MIN_INTERVAL = float(os.environ.get("CLASSIFY_MIN_INTERVAL", "22"))  # seconds between API calls — raised from 16s; new fields add ~1k output tokens/batch, pushing past 10k/min at 16s

# Proactive pacing shared across (any) workers so we don't burn a retry on every
# batch. The 429 backoff in process_batch is the safety net on top of this.
_rate_lock = threading.Lock()
_last_call_ts = [0.0]

def _throttle():
    with _rate_lock:
        wait = CLASSIFY_MIN_INTERVAL - (time.monotonic() - _last_call_ts[0])
        if wait > 0:
            time.sleep(wait)
        _last_call_ts[0] = time.monotonic()

# ── CATEGORIES ────────────────────────────────────────────────────────────────
CONSULTING_CATEGORIES = [
    "Compliance & Enforcement",
    "Medicare & Medicaid Policy",
    "Medical Coding, Billing & Revenue Cycle",
    "HIPAA, Privacy & Cybersecurity",
    "Life Sciences, Pharma & Medical Devices",
    "AI, Digital Health & Health IT",
    "Telehealth & Virtual Care",
    "Payer, Market Access & Value-Based Care",
    "Behavioral Health & Substance Use",
    "Hospital & Post-Acute Care Operations",
    "Healthcare M&A, Antitrust & Finance",
]

POLICY_CATEGORIES = [
    "Federal Legislation & Congressional Activity",
    "Medicare & Medicaid Payment Reform",
    "Value-Based Care & APM Policy",
    "Health Equity, SDOH & Access",
    "Drug Pricing & Pharmaceutical Policy",
    "Healthcare Workforce & Labor Policy",
    "Budget, Appropriations & Federal Spending",
    "Research, Evidence & Think Tank Reports",
    "Regulatory & Administrative Actions",
    "State Policy & Medicaid Innovation",
]

CAT_GUIDANCE = """
Category definitions — pick the MOST specific match:

CONSULTING CATEGORIES:
- Compliance & Enforcement: OIG audits, fraud alerts, FCA settlements, AKS/Stark, exclusions, DOJ healthcare fraud, CIAs
- Medicare & Medicaid Policy: CMS coverage rules, payment rates, MA/Part D, Medicaid waivers/SPAs, benefit design, eligibility
- Medical Coding, Billing & Revenue Cycle: ICD-10/CPT/HCPCS updates, RAC/MAC audits, claim denials, E&M coding, RCM
- HIPAA, Privacy & Cybersecurity: OCR enforcement, breach notifications, data privacy, cybersecurity incidents
- Life Sciences, Pharma & Medical Devices: FDA drug/device approvals, 510(k)/PMA, clinical trials, biosimilars, oncology, recalls, device classifications
- AI, Digital Health & Health IT: AI/ML in healthcare, SaMD, ONC interoperability, EHR, information blocking, digital therapeutics
- Telehealth & Virtual Care: Telehealth reimbursement, DEA prescribing, state licensure, audio-only policy, RPM
- Payer, Market Access & Value-Based Care: Network adequacy, prior auth, Star ratings, HEDIS, formulary, payer-provider disputes
- Behavioral Health & Substance Use: Mental health parity, 42 CFR Part 2, MAT/buprenorphine, SAMHSA, crisis services, 988
- Hospital & Post-Acute Care Operations: Price transparency, SNF/LTACH/IRF, EMTALA, accreditation, staffing, CoPs, observation status
- Healthcare M&A, Antitrust & Finance: FTC/DOJ merger reviews, PE in healthcare, CON laws — NOT for clinical or payment content

POLICY CATEGORIES:
- Federal Legislation & Congressional Activity: Bills, markups, hearings, floor votes, CBO scoring
- Medicare & Medicaid Payment Reform: IPPS/OPPS/PFS rules, bundled payments, CMMI models, payment updates
- Value-Based Care & APM Policy: ACOs, MSSP, CMMI, MIPS/APM incentives, quality measurement
- Health Equity, SDOH & Access: Disparities, SDOH screening, Section 1557, underserved populations, rural health
- Drug Pricing & Pharmaceutical Policy: IRA negotiations, 340B, PBM reform, Part D redesign, international reference pricing
- Healthcare Workforce & Labor Policy: Staffing ratios, NLRB, clinician shortage, immigration/visa policy
- Budget, Appropriations & Federal Spending: Federal health spending, MedPAC recommendations, CBO scores, appropriations
- Research, Evidence & Think Tank Reports: KFF, Commonwealth Fund, RAND, Health Affairs, Brookings, GAO, academic findings
- Regulatory & Administrative Actions: Routine PRA notices, advisory committee meetings — LAST RESORT only
- State Policy & Medicaid Innovation: State Medicaid waivers, state legislation, NASHP, state insurance reform

Mark as DISCARD if clearly not healthcare: mining equipment, archival records, financial benefits unrelated to health, energy infrastructure, aviation, fisheries, military housing.
"""

NOISE_SIGNALS = [
    "aviation", "airworthiness", "fisheries", "bluefin tuna", "pipeline",
    "foreign-trade zone", "antidumping", "hydropower", "coast guard",
    "marine mammal", "endangered species", "natural gas", "ferc",
    "faa airworthiness", "phmsa", "mining equipment", "hoisting equipment",
    "archival", "military personnel records", "government life insurance",
    "specially adapted housing", "firearms", "nuclear reactor",
]

def _is_noise(a):
    """Hard-drop only items whose content contains definitively non-healthcare noise signals.
    DISCARD-tier items are kept — the frontend collapses them into a hidden drawer."""
    text = " ".join([
        a.get("headline") or "",
        a.get("title") or "",
        a.get("summary") or "",
        a.get("snippet") or "",
    ]).lower()
    return any(sig in text for sig in NOISE_SIGNALS)


def _fallback_article(a, categories):
    """Build a VALID article record for an item whose batch failed classification
    (rate limit, parse error, etc.). All required string fields are non-empty so the
    run still passes validation and publishes; the item is tiered as 'discard' with a
    low score so the frontend tucks it into the collapsed drawer rather than the main
    ranked view — visible and honest, but not pretending to be analyzed."""
    title   = a.get("title") or "Untitled item"
    snippet = a.get("snippet") or ""
    return {
        **a,
        "category":             categories[-1],
        "urgency":              "discard",
        "importance_score":     5,
        "consulting_relevance": 0,
        "policy_relevance":     0,
        "headline":             title,
        "summary":              (snippet[:400] if snippet else title),
        "so_what":              "Automated analysis was unavailable for this item this run (classification did not complete).",
        "now_what":             "Review the linked source directly.",
        "implication":          "Review the linked source directly.",
        "stakeholder_balance":  None,
        "confidence_note":      "Not analyzed this run — classification unavailable.",
        "is_comment_period":    bool(a.get("comment_deadline")),
    }


def _ensure_article_fields(a, categories):
    """Guarantee every field the validator requires is present and well-formed, so a
    single item where the model omitted or malformed a field can't abort the whole
    run at validation. Good values are left untouched; only missing/empty/invalid
    ones are backfilled."""
    out = dict(a)

    def _str_ok(v):
        return isinstance(v, str) and v.strip()

    title = out.get("title") or out.get("headline") or "Untitled item"

    if not _str_ok(out.get("category")):
        out["category"] = categories[-1]
    if out.get("urgency") not in ("urgent", "important", "routine", "discard"):
        out["urgency"] = "routine"

    out["headline"] = out["headline"] if _str_ok(out.get("headline")) else title
    out["summary"]  = out["summary"]  if _str_ok(out.get("summary"))  else (out.get("snippet") or title)
    out["so_what"]  = out["so_what"]  if _str_ok(out.get("so_what"))  else "Analysis not provided for this item."
    out["now_what"] = out["now_what"] if _str_ok(out.get("now_what")) else "Review the linked source directly."
    out["implication"] = out["implication"] if _str_ok(out.get("implication")) else out["now_what"]

    for f in ("importance_score", "consulting_relevance", "policy_relevance"):
        v = out.get(f)
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not (0 <= v <= 100):
            out[f] = 30

    if not isinstance(out.get("is_comment_period"), bool):
        out["is_comment_period"] = bool(out.get("comment_deadline"))

    if not isinstance(out.get("stakeholder_balance"), dict):
        out["stakeholder_balance"] = None
    out.setdefault("confidence_note", None)
    out.setdefault("persisted", False)  # preserve True on carried-over articles

    if not _str_ok(out.get("regulatory_stage")):
        out["regulatory_stage"] = "Administrative Action"
    if not isinstance(out.get("client_types"), list) or not out["client_types"]:
        out["client_types"] = []
    if "effective_date" not in out:
        out["effective_date"] = None
    if not _str_ok(out.get("watch_for")):
        out["watch_for"] = None

    return out

# ── HELPERS ───────────────────────────────────────────────────────────────────
def parse_json_response(raw_text):
    text = (raw_text or "").strip()
    if "```" in text:
        parts = text.split("```")
        cleaned = []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if part.lower().startswith("json"):
                part = part.split("\n", 1)[1] if "\n" in part else ""
            cleaned.append(part)
        text = "\n".join(cleaned).strip() or text

    # Decode the FIRST complete JSON value and ignore any trailing prose the model
    # may append (raw_decode stops at the end of the first value), which is what
    # caused intermittent "Extra data" failures.
    start = next((i for i, ch in enumerate(text) if ch in "[{"), None)
    if start is not None:
        try:
            obj, _ = json.JSONDecoder().raw_decode(text[start:])
            return obj
        except json.JSONDecodeError:
            pass

    match = re.search(r"(\[.*\]|\{.*\})", text, re.S)
    if match:
        text = match.group(1)
    return json.loads(text)


def validate_article_output(article, idx=None):
    errors = []
    if not isinstance(article, dict):
        errors.append("article is not an object")
        return errors

    string_required = ["category", "urgency", "headline", "summary", "so_what", "now_what", "implication"]
    for field in string_required:
        if field not in article:
            errors.append(f"missing {field}")
        elif not isinstance(article[field], str) or not article[field].strip():
            errors.append(f"{field} must be a non-empty string")

    numeric_required = ["importance_score", "consulting_relevance", "policy_relevance"]
    for field in numeric_required:
        if field not in article:
            errors.append(f"missing {field}")
        else:
            val = article[field]
            if not isinstance(val, (int, float)) or not (0 <= val <= 100):
                errors.append(f"{field} must be a number 0–100, got {val!r}")

    if "is_comment_period" not in article:
        errors.append("missing is_comment_period")
    elif not isinstance(article["is_comment_period"], bool):
        errors.append("is_comment_period must be boolean")

    urgency = article.get("urgency")
    if urgency not in ("routine", "important", "urgent", "discard"):
        errors.append(f"invalid urgency: {urgency!r}")

    # stakeholder_balance must be dict or null — not validated deeply
    sb = article.get("stakeholder_balance")
    if sb is not None and not isinstance(sb, dict):
        errors.append("stakeholder_balance must be a dict or null")

    return errors


def validate_newsletter_data(data):
    if not isinstance(data, dict):
        raise ValueError("newsletter data must be a JSON object")
    for key in ("generated_at", "week_of", "consulting", "policy"):
        if key not in data:
            raise ValueError(f"missing top-level key: {key}")
    for edition in ("consulting", "policy"):
        sub = data[edition]
        if not isinstance(sub, dict):
            raise ValueError(f"{edition} must be an object")
        for field in ("subject_line", "theme_of_week", "editors_note", "categories", "articles"):
            if field not in sub:
                raise ValueError(f"{edition} missing {field}")
        # whats_new is optional (absent on first run)
        if not isinstance(sub["categories"], list):
            raise ValueError(f"{edition}.categories must be a list")
        if not isinstance(sub["articles"], list):
            raise ValueError(f"{edition}.articles must be a list")
        for idx, article in enumerate(sub["articles"]):
            errors = validate_article_output(article, idx)
            if errors:
                raise ValueError(f"{edition} article {idx} errors: {errors}")


# ── ARCHIVE ───────────────────────────────────────────────────────────────────

def load_archive(path=ARCHIVE_FILE):
    """Load the on-disk archive, or return a fresh empty structure on first run."""
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            # Migrate: older archives may lack the 'runs' key
            data.setdefault("runs", [])
            data.setdefault("articles", {})
            return data
        except Exception as e:
            print(f"  ⚠ Could not load archive ({e}) — starting fresh")
    return {"last_updated": None, "runs": [], "articles": {}}


def update_archive(archive, output, run_date):
    """
    Upsert new articles into the archive and record this run's URL sets.
    Articles are stored by normalized URL (the dedup key).
    Runs older than ARCHIVE_MAX_RUNS are rolled off, and any article not referenced
    by a retained run is garbage-collected so the archive file stays bounded.
    """
    now = datetime.now(timezone.utc).isoformat()

    urgency_rank = {"urgent": 0, "important": 1, "routine": 2, "discard": 3}
    for edition in ("consulting", "policy"):
        for a in output.get(edition, {}).get("articles", []):
            url = a.get("url", "")
            if not url:
                continue
            if url not in archive["articles"]:
                archive["articles"][url] = {
                    "url": url,
                    "title": a.get("title", ""),
                    "snippet": a.get("snippet", ""),
                    "source_name": a.get("source_name", ""),
                    "source_url": a.get("source_url", ""),
                    "source_type": a.get("source_type", ""),
                    "published": a.get("published", ""),
                    "comment_deadline": a.get("comment_deadline") or None,
                    "effective_date": a.get("effective_date") or None,
                    "first_seen": now,
                    "last_seen": now,
                    "urgency": a.get("urgency", "discard"),
                    "editions": {},
                }
            rec = archive["articles"][url]
            rec["last_seen"] = now
            # Store full per-edition classification (overwritten each run so it stays fresh)
            rec.setdefault("editions", {})[edition] = {
                k: v for k, v in a.items() if k not in ("first_seen", "last_seen")
            }
            # Update top-level urgency to the best tier seen across editions
            new_urg = a.get("urgency", "discard")
            if urgency_rank.get(new_urg, 3) < urgency_rank.get(rec.get("urgency", "discard"), 3):
                rec["urgency"] = new_urg
            # Propagate deadline fields to top level so persistence logic can read them
            if a.get("comment_deadline"):
                rec["comment_deadline"] = a["comment_deadline"]
            if a.get("effective_date"):
                rec["effective_date"] = a["effective_date"]

    run_record = {
        "run_date":       run_date,
        "generated_at":   now,
        "consulting_urls": [a["url"] for a in output.get("consulting", {}).get("articles", []) if a.get("url")],
        "policy_urls":     [a["url"] for a in output.get("policy",     {}).get("articles", []) if a.get("url")],
    }
    archive["runs"].append(run_record)

    # Retention: keep only the last ARCHIVE_MAX_RUNS runs, then garbage-collect any
    # article no longer referenced by a retained run. This bounds the file size in
    # Git history. An item that reappears after rolling off is legitimately "new" again.
    if len(archive["runs"]) > ARCHIVE_MAX_RUNS:
        archive["runs"] = archive["runs"][-ARCHIVE_MAX_RUNS:]

    live_urls = set()
    for r in archive["runs"]:
        live_urls.update(r.get("consulting_urls", []))
        live_urls.update(r.get("policy_urls", []))
    before = len(archive["articles"])
    archive["articles"] = {u: a for u, a in archive["articles"].items() if u in live_urls}
    pruned = before - len(archive["articles"])
    if pruned:
        print(f"  ↺ Archive retention: pruned {pruned} article(s) older than {ARCHIVE_MAX_RUNS} runs")

    archive["last_updated"] = now
    return archive


def save_archive(archive, path=ARCHIVE_FILE):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(archive, f, indent=2, default=str)
    n_articles = len(archive["articles"])
    n_runs     = len(archive["runs"])
    print(f"  ✓ Archive: {n_articles} unique articles across {n_runs} runs → {path}")


def get_persisted_articles(archive, current_urls, edition, today_str):
    """Return articles from prior runs that qualify for carry-forward.

    Criteria (ALL must hold):
      a) urgency urgent/important  OR  has a future comment_deadline/effective_date
      b) first_seen within PERSIST_DAYS  OR  has a future deadline (deadline items
         persist indefinitely until the deadline passes)
      c) URL not already in the current scrape
    """
    from datetime import date as _date
    today = _date.fromisoformat(today_str[:10])
    cutoff = today - timedelta(days=PERSIST_DAYS)

    url_key = f"{edition}_urls"
    edition_urls: set = set()
    for r in archive.get("runs", []):
        edition_urls.update(r.get(url_key, []))

    urgency_rank = {"urgent": 0, "important": 1, "routine": 2, "discard": 3}
    persisted = []

    for url, a in archive.get("articles", {}).items():
        if url in current_urls:
            continue
        if url not in edition_urls:
            continue  # never appeared in this edition

        # Prefer edition-specific classification; fall back to top-level fields
        edition_data = a.get("editions", {}).get(edition) or a
        urgency = edition_data.get("urgency") or a.get("urgency", "discard")

        # Check for future deadline
        deadline = (
            edition_data.get("comment_deadline") or
            edition_data.get("effective_date") or
            a.get("comment_deadline") or
            a.get("effective_date")
        )
        has_future_deadline = False
        if deadline and str(deadline) not in ("", "None", "null", "TBD"):
            try:
                dl = _date.fromisoformat(str(deadline)[:10])
                has_future_deadline = dl >= today
            except ValueError:
                pass

        # Check age
        first_seen = (a.get("first_seen") or "")[:10]
        within_window = False
        if first_seen:
            try:
                within_window = _date.fromisoformat(first_seen) >= cutoff
            except ValueError:
                pass

        tier_ok = urgency_rank.get(urgency, 3) <= 1  # urgent or important
        if (tier_ok or has_future_deadline) and (within_window or has_future_deadline):
            article = dict(edition_data)
            # Ensure key metadata fields are present
            for key in ("url", "title", "published", "source_name", "source_url",
                        "comment_deadline", "effective_date"):
                if not article.get(key):
                    article[key] = a.get(key) or ""
            article["persisted"] = True
            article["first_seen"] = first_seen
            persisted.append(article)

    return persisted


def reconcile_shared_urgency(consulting_articles, policy_articles):
    """For articles in both editions, use the higher urgency tier in both.

    Prevents the same article being urgent in consulting and discard in policy
    (or vice versa), which looks inconsistent and buries high-signal items.
    """
    urgency_rank = {"urgent": 0, "important": 1, "routine": 2, "discard": 3}
    c_by_url = {a.get("url", ""): a for a in consulting_articles if a.get("url")}
    p_by_url = {a.get("url", ""): a for a in policy_articles   if a.get("url")}
    shared = set(c_by_url) & set(p_by_url)
    reconciled = 0
    for url in shared:
        c, p = c_by_url[url], p_by_url[url]
        cu, pu = c.get("urgency", "routine"), p.get("urgency", "routine")
        if urgency_rank.get(cu, 2) != urgency_rank.get(pu, 2):
            best = cu if urgency_rank.get(cu, 2) < urgency_rank.get(pu, 2) else pu
            c["urgency"] = best
            p["urgency"] = best
            reconciled += 1
    if reconciled:
        print(f"  ↺ Reconciled urgency for {reconciled} shared article(s)")
    return consulting_articles, policy_articles


# ── WHAT'S NEW ────────────────────────────────────────────────────────────────

def compute_whats_new(current_articles, archive, edition):
    """
    Diff this run's URLs against the immediately previous run for this edition.
    Returns a digest dict or None if there is no prior run to compare against.

    'New' means the URL appeared in this run but NOT in the previous run's URL set.
    We use the immediately prior run (not the full archive) so the window is always
    'since last Monday' — honest and predictable.
    """
    url_key = f"{edition}_urls"
    runs = archive.get("runs", [])

    # Find the most recent run that has a URL set for this edition.
    # The last entry in runs[] is the one we just appended, so the prior run is [-2].
    prior_runs = [r for r in runs if url_key in r]
    if len(prior_runs) < 2:
        # First run or only one run recorded — nothing to diff against.
        return None

    previous_run = prior_runs[-2]
    previous_urls = set(previous_run.get(url_key, []))
    previous_date = previous_run.get("run_date", "previous run")

    new_articles = [a for a in current_articles if a.get("url") and a["url"] not in previous_urls]

    if not new_articles:
        return {
            "count": 0, "urgent_count": 0, "important_count": 0,
            "previous_run_date": previous_date,
            "articles": [],
            "message": f"No new items since {previous_date}.",
        }

    # Sort by importance_score desc, then urgency
    urgency_order = {"urgent": 0, "important": 1, "routine": 2, "discard": 3}
    new_articles.sort(key=lambda a: (
        urgency_order.get(a.get("urgency", "routine"), 2),
        -(a.get("importance_score") or 0),
    ))

    urgent_count    = sum(1 for a in new_articles if a.get("urgency") == "urgent")
    important_count = sum(1 for a in new_articles if a.get("urgency") == "important")

    return {
        "count":           len(new_articles),
        "urgent_count":    urgent_count,
        "important_count": important_count,
        "previous_run_date": previous_date,
        "articles":        new_articles[:20],   # cap at 20 for the digest panel
        "message":         None,
    }


def require_env(name):
    value = os.environ.get(name)
    if not value:
        print(f"\n✗ Environment variable {name} is required.")
        sys.exit(1)
    return value


def parse_args():
    parser = argparse.ArgumentParser(description="Generate Healthcare Regulatory & Policy Monitor newsletter JSON from raw articles")
    parser.add_argument("-i", "--input", default="raw_articles.json", help="raw articles input JSON file")
    parser.add_argument("-o", "--output", default="newsletter_data.json", help="newsletter JSON output file")
    parser.add_argument("-m", "--model", default=EDITORIAL_MODEL, help="Anthropic model for editorial generation")
    parser.add_argument("--no-push", action="store_true", help="save newsletter locally without pushing to GitHub")
    return parser.parse_args()

# ── CLASSIFICATION ────────────────────────────────────────────────────────────
def classify_articles(client, articles, categories, audience_label, audience_desc, edition="consulting"):
    BATCH = CLASSIFY_BATCH
    total = len(articles)
    print(f"\n  Classifying {total} articles for {audience_label} audience...")

    batches   = [articles[i:i+BATCH] for i in range(0, total, BATCH)]
    n_batches = len(batches)

    # Policy edition gets stakeholder_balance; consulting gets null.
    if edition == "policy":
        stakeholder_schema = (
            '  "stakeholder_balance": {{\n'
            '    "who_benefits": "<who gains from this development>",\n'
            '    "who_bears_cost": "<who bears the burden or loses>",\n'
            '    "case_for": "<strongest argument in favor, fairly stated>",\n'
            '    "case_against": "<strongest argument against, fairly stated>"\n'
            '  }} or null if the item has no genuine political/stakeholder dimension,'
        )
        stakeholder_guidance = """
━━━ BIPARTISAN ANALYSIS (policy edition) ━━━
For items with a genuine policy/political dimension, populate stakeholder_balance.
Rules:
  • Present BOTH sides' strongest argument — not a strawman, not your view.
  • who_benefits and who_bears_cost name specific constituencies (providers, patients,
    insurers, taxpayers, rural hospitals, etc.) — not vague terms like "the public."
  • case_for / case_against: 1–2 sentences each, written so a knowledgeable advocate
    for that side would recognize it as their actual argument.
  • Do NOT signal which side is correct. Your job is calibration, not persuasion.
  • Set null for factual/administrative items (routine clearances, court scheduling
    notices, advisory committee meetings) that have no meaningful stakeholder split."""
    else:
        stakeholder_schema = '  "stakeholder_balance": null,'
        stakeholder_guidance = ""

    if edition == "policy":
        policy_rulemaking_rules = """

━━━ POLICY EDITION HARD RULES ━━━

1. Major federal rulemaking is NEVER discard in the policy edition. This includes:
   - Interim Final Rules, Proposed Rules (NPRMs), and Final Rules from CMS, FDA, or HHS
   - Presidential executive orders touching healthcare coverage, Medicare, or Medicaid
   If you would otherwise mark such an item as discard, upgrade it to routine (minimum).

2. News coverage OF a major rule inherits the rule's significance. A news article covering
   an IFR that would score 80+ is itself at minimum "important" — not "routine" or "discard."
   Coverage items may score 5–10 points lower than the primary rule document, but not more."""
    else:
        policy_rulemaking_rules = ""

    if edition == "consulting":
        so_what_desc = (
            "1 sentence, ≤25 words: name the specific client type affected "
            "(provider, payer, pharma, etc.), the concrete exposure (revenue/compliance/ops), "
            "and one non-obvious second-order consequence"
        )
        now_what_desc = (
            f"1 sentence: exact recommendation — advise [specific client type] to [concrete action] "
            f"within [30/60/90-day timeframe]"
        )
        watch_for_desc = (
            "1 sentence: the specific next event in this regulatory sequence — "
            "what rule/decision/deadline follows this one, when it's expected, and what it will signal"
        )
    else:
        so_what_desc = (
            "1–2 sentences: non-obvious policy consequence — the broader legislative or regulatory "
            "trend this fits, and the second-order effect on coverage, access, or spending"
        )
        now_what_desc = (
            f"1 sentence: what {audience_label}s should monitor or act on — name the specific "
            f"vote, comment window, stakeholder coalition, or policy vehicle"
        )
        watch_for_desc = (
            "1 sentence: the specific next legislative or regulatory development to monitor — "
            "floor vote, implementation deadline, state action, or stakeholder response expected next"
        )

    # Static system prompt — cached across all batches for this edition run.
    system_text = f"""You are a senior healthcare analyst producing structured intelligence for {audience_desc}.

Your output must help readers answer three questions fast:
  1. What happened and what changed from prior state?
  2. Why does it matter — non-obviously, with second-order effects?
  3. What should I do or watch for right now?

Classify each article using ONLY these exact category strings:
{chr(10).join(f'- {c}' for c in categories)}

{CAT_GUIDANCE}

Return ONLY a valid JSON array — no markdown, no preamble:
[{{
  "idx": <0-based index within this batch>,
  "category": "<exact category string or DISCARD>",
  "urgency": "urgent" | "important" | "routine" | "discard",
  "importance_score": <integer 0–100, see rubric>,
  "consulting_relevance": <integer 0–100, relevance to compliance officers and strategy consultants>,
  "policy_relevance": <integer 0–100, relevance to policy professionals and legislative staff>,
  "headline": "<rewritten headline, analyst voice, 10–15 words, specific — name the agency/rule/dollar amount>",
  "summary": "<2–3 sentences: what happened, what rule/law/action is involved, what changed from prior state>",
  "so_what": "<{so_what_desc}>",
  "now_what": "<{now_what_desc}>",
{stakeholder_schema}
  "confidence_note": "<calibrated hedge if uncertain — e.g. 'Enforcement trajectory unclear pending court ruling' or 'Comment period outcome uncertain given split committee' — use null when the analysis is high-confidence>",
  "is_comment_period": <true if a proposed rule is currently open for public comment, else false>,
  "regulatory_stage": "<one of: NPRM | Final Rule | Interim Final Rule | Guidance | Enforcement Action | Court Ruling | Legislation | Advisory Opinion | Administrative Action | Recall | Drug/Device Approval>",
  "effective_date": "<YYYY-MM-DD if specified in the document, 'TBD' if announced but unconfirmed, null if not applicable>",
  "client_types": ["<subset of: Hospital/Health System, Managed Care/Payer, Physician/Group Practice, Life Sciences/Pharma, Medical Device, Post-Acute/LTC, Behavioral Health, Digital Health, All Providers>"],
  "watch_for": "<{watch_for_desc}>"
}}]

━━━ IMPORTANCE SCORE RUBRIC (0–100) ━━━

Score the INTERSECTION of magnitude × breadth-of-impact, not just headline size.

  90–100  Landmark only:
          • Final rule reshaping national payment rates or coverage (IPPS, OPPS, PFS, MA rates)
          • Enforcement action >$50M or industry-wide consent decree
          • Legislation signed into law affecting Medicare/Medicaid/ACA
          • Supreme Court health ruling

  70–89   Significant — most {audience_label}s should act or closely monitor:
          • Proposed rule with major payment/coverage implications (comment period open)
          • Enforcement action $1M–$50M or notable FCA settlement with CIA
          • FDA approval of first-in-class drug or Class I recall of widely-used product
          • CBO score on major health legislation
          • Comment deadline within 14 days

  50–69   Notable — {audience_label}s should read in full:
          • Final guidance, interim rule, or significant policy clarification
          • OIG advisory opinion, fraud alert, or report with broad implications
          • FDA 510(k) clearance of a significant device category (not commodity)
          • Congressional hearing or committee markup on a health bill
          • Comment deadline 15–60 days out

  30–49   Routine but relevant:
          • Minor rule updates, technical corrections, PRA notices
          • Routine FDA clearances of commodity devices/drugs
          • Research or think tank report (reference value, no immediate action)
          • Standard advisory committee meeting notices
          • Comment deadline >60 days or no deadline

  0–29    Low signal or discard territory:
          • Non-health content that slipped keyword filters
          • Highly procedural notices with zero practical healthcare impact
          • Purely local scope with no national precedent

━━━ CALIBRATION ANCHOR ━━━

High bar (score 88, urgency "urgent"): CMS final 2025 IPPS rule — 2.9% base rate update, effective Oct 1.
  so_what: "2.9% trails market basket by ~0.4pp after productivity adjustment — continuing real-terms cuts that are accelerating consolidation among independent hospitals."
  now_what: "Model updated reimbursement rates for hospital clients before Oct 1 budget cycle; flag margin compression in Q3 board materials."

Low bar (score 18, urgency "routine"): Routine 510(k) clearance of a commodity blood pressure monitor.
  so_what: "Standard iterative clearance; no coverage or reimbursement change." now_what: "No action required."

━━━ QUALITY STANDARDS ━━━

so_what must be SPECIFIC and NON-OBVIOUS. Reject:
  ✗ "This is an important development for providers to monitor."
  ✗ "Healthcare organizations should be aware of this change."
Accept:
  ✓ "This extends a pattern of OIG scrutiny of orthopedics co-management arrangements
     — the third action in 18 months — suggesting the office is building toward a
     broader fraud alert or Special Fraud Alert in this space."
  ✓ "The effective date falls mid-fiscal-year for most health systems, creating a
     retroactive revenue impact that will show in Q3 earnings reports."

confidence_note: use it liberally. 'Prospects unclear given committee dynamics' and
'Insufficient information to assess enforcement trajectory' are professional, honest answers.
Reserve null for items where the facts clearly determine the analysis.{stakeholder_guidance}{policy_rulemaking_rules}

━━━ FIELD DEFINITIONS ━━━

regulatory_stage — pick the most precise label:
  NPRM = proposed rule open for comment
  Final Rule = published, binding rule
  Interim Final Rule = binding immediately, comment period open
  Guidance = sub-regulatory policy (FAQ, Dear Colleague, CMS memo)
  Enforcement Action = OIG/DOJ/FTC action, settlement, CIA
  Court Ruling = judicial decision affecting healthcare regulation
  Legislation = enacted law or active bill
  Advisory Opinion = OIG/FTC advisory, binding only to requester
  Recall = FDA product recall
  Drug/Device Approval = FDA approval, clearance, or authorization
  Administrative Action = everything else (committee meeting, PRA notice, personnel)

client_types — include ALL that are materially affected. Use exact strings from the list.
  If broadly applicable, use ["All Providers"] as one entry but still include more specific types.

effective_date — the date the rule/action TAKES EFFECT, not publication date.
  Most NPRMs: null (no effective date yet). Final Rules: check the document, usually 30-60 days after publication.

If two articles in this batch cover the exact same event (same rule, same enforcement action, same approval), emit only the higher-quality record. Use its idx, the more specific headline, and the higher scores. Omit the duplicate from your output entirely.
"""

    results_map = {}

    def process_batch(batch_idx, batch):
        batch_text = "\n\n".join(
            f"[{j}] TITLE: {a['title']}\n"
            f"SOURCE: {a['source_name']}\n"
            f"DATE: {a['published']}\n"
            f"SNIPPET: {a['snippet'][:300]}"
            for j, a in enumerate(batch)
        )
        MAX_ATTEMPTS = 4
        last_err = None
        for attempt in range(MAX_ATTEMPTS):
            try:
                _throttle()
                resp = client.messages.create(
                    model=CLASSIFY_MODEL,
                    max_tokens=CLASSIFY_MAX_TOKENS,
                    system=[{"type": "text", "text": system_text, "cache_control": {"type": "ephemeral"}}],
                    messages=[{"role": "user", "content": batch_text}],
                )
                parsed = parse_json_response(resp.content[0].text.strip())
                classified = []
                for item in parsed:
                    idx = item.get("idx", 0)
                    if 0 <= idx < len(batch):
                        merged = {**batch[idx], **item}
                        # Ensure implication mirrors now_what for frontend compat
                        if merged.get("now_what") and not merged.get("implication"):
                            merged["implication"] = merged["now_what"]
                        classified.append(merged)
                print(f"    ✓ Batch {batch_idx+1}/{n_batches} — {len(classified)} classified")
                return batch_idx, classified
            except Exception as e:
                last_err = e
                is_rate = "429" in str(e) or "rate_limit" in str(e).lower()
                if attempt < MAX_ATTEMPTS - 1:
                    # Rate-limit errors need a real cooldown (limit is per-minute);
                    # other errors get a short backoff.
                    wait = (25 if is_rate else 3) * (attempt + 1)
                    label = "rate-limited" if is_rate else "error"
                    print(f"    ⏳ Batch {batch_idx+1} {label}; retry {attempt+1}/{MAX_ATTEMPTS-1} in {wait}s")
                    time.sleep(wait)
        # All attempts failed — emit VALID fallback records so the run still publishes.
        print(f"    ✗ Batch {batch_idx+1} failed after {MAX_ATTEMPTS} attempts: {last_err}")
        return batch_idx, [_fallback_article(a, categories) for a in batch]

    with ThreadPoolExecutor(max_workers=CLASSIFY_WORKERS) as executor:
        futures = {executor.submit(process_batch, i, b): i for i, b in enumerate(batches)}
        for future in as_completed(futures):
            idx, items = future.result()
            results_map[idx] = items

    results = []
    for i in range(n_batches):
        results.extend(results_map.get(i, []))
    return results

def group_articles_by_topic(client, articles):
    """One extra API call per edition: stamps each article with topic_group (or None)."""
    candidates = [(i, a) for i, a in enumerate(articles)
                  if (a.get("urgency") or "routine") != "discard"]
    if len(candidates) < 2:
        for a in articles:
            a.setdefault("topic_group", None)
        return articles

    headline_list = "\n".join(
        f"[{orig_i}] {a.get('headline') or a.get('title', '')}"
        for orig_i, a in candidates
    )

    prompt = (
        "Group these healthcare news article headlines by shared underlying topic or story.\n\n"
        "Rules:\n"
        "- Assign a group name ONLY when 2+ articles cover the same story, rulemaking, drug, "
        "enforcement action, legislation, or event.\n"
        "- Group names: 3-7 words, title case "
        "(e.g. \"Medicaid Work Requirements Rule\", \"GLP-1 Coverage Policy\", \"CMS Prior Authorization Rule\").\n"
        "- Articles with no partner: topic_group = null.\n\n"
        "Return ONLY a JSON array, one object per article in input order:\n"
        "[{\"idx\": <number>, \"topic_group\": \"<name or null>\"}, ...]\n\n"
        f"Headlines:\n{headline_list}"
    )

    try:
        _throttle()
        resp = client.messages.create(
            model=CLASSIFY_MODEL,
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
        parsed = parse_json_response(resp.content[0].text.strip())
        groups = {item["idx"]: item.get("topic_group") or None for item in parsed if "idx" in item}
        for orig_i, a in candidates:
            a["topic_group"] = groups.get(orig_i)
    except Exception as e:
        print(f"  ⚠ Topic grouping failed ({e}) — skipping")

    for a in articles:
        a.setdefault("topic_group", None)
    return articles


def generate_category_notes(client, articles, edition, categories):
    """Single API call — generates a 2-sentence summary for every active category."""
    cat_groups = {}
    for a in articles:
        cat = a.get("category", "")
        if cat and cat.upper() != "DISCARD" and cat in categories:
            cat_groups.setdefault(cat, []).append(a)

    if not cat_groups:
        return {}

    sections = []
    for cat, arts in cat_groups.items():
        headlines = [a.get("headline") or a.get("title", "") for a in arts[:6]]
        sections.append(f"CATEGORY: {cat}\nHEADLINES: {' | '.join(h for h in headlines if h)}")

    audience_map = {
        "consulting": "compliance officers and healthcare executives",
        "policy":     "healthcare policy professionals and legislative staff",
    }
    prompt = (
        f"For each healthcare category below, write a 2-sentence summary of the key developments this week.\n"
        f"Be specific to the actual headlines — avoid generic filler. "
        f"Write for {audience_map.get(edition, 'healthcare professionals')}.\n\n"
        + "\n".join(sections)
        + '\n\nReturn a JSON object mapping the exact category name to its 2-sentence summary:\n'
          '{"Exact Category Name": "Summary text.", ...}'
    )

    try:
        _throttle()
        resp = client.messages.create(
            model=CLASSIFY_MODEL,
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
        notes = parse_json_response(resp.content[0].text.strip())
        count = len(notes) if isinstance(notes, dict) else 0
        print(f"    ✓ {count} category notes generated")
        return notes if isinstance(notes, dict) else {}
    except Exception as e:
        print(f"    ✗ Category notes error: {e}")
        return {}


def generate_editorial(client, articles, edition, week_of):
    print(f"  Generating {edition} editorial...")
    top = [
        f"[{a.get('category','')}] {a.get('headline', a.get('title',''))}"
        for a in articles[:25]
        if a.get("category", "").upper() != "DISCARD"
    ]
    audience_map = {
        "consulting": "healthcare strategy consultants, compliance officers, and healthcare executives",
        "policy":     "healthcare policy professionals, legislative staff, lobbyists, and government affairs leads",
    }
    prompt = f"""You are the editor of Healthcare Regulatory & Policy Monitor ({edition.title()} Edition), a premium weekly newsletter for {audience_map[edition]}.

Week of {week_of}. Top stories this week:
{chr(10).join(top[:20])}

Return a JSON object — no markdown:
{{
  "subject_line": "<compelling email subject line specific to this week, under 65 chars>",
  "theme_of_week": "<6-9 word theme capturing the dominant storyline this week>",
  "editors_note": "<3-4 sentences synthesizing the week's dominant theme and key tensions. Analyst voice. No 'this week we cover' phrasing.>",
  "what_to_watch": [
    "<forward-looking question or signal #1 — specific, tied to an actual item from this week's briefing, 1 sentence>",
    "<forward-looking question or signal #2>",
    "<forward-looking question or signal #3>",
    "<forward-looking question or signal #4>"
  ]
}}"""

    try:
        response = client.messages.create(
            model=EDITORIAL_MODEL,
            max_tokens=900,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text.strip()
        parsed = parse_json_response(text)
        if isinstance(parsed, dict):
            return parsed
        raise ValueError("editorial response was not a JSON object")
    except Exception as e:
        print(f"    ✗ Editorial error: {e}")
        return {
            "subject_line":   f"Healthcare Regulatory & Policy Monitor — {edition.title()} — {week_of}",
            "theme_of_week":  "Healthcare regulatory developments",
            "editors_note":   "This week's briefing covers the latest regulatory and policy developments.",
            "what_to_watch":  [],
        }

# ── GITHUB PUSH ───────────────────────────────────────────────────────────────
def save_json(data, filename):
    content_str = json.dumps(data, indent=2, default=str)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(content_str)
    print(f"  ✓ Saved {filename}")


def push_to_github(data, github_token, github_repo, output_file, no_push=False):
    if no_push:
        print("  ⚠ Skipping GitHub push (--no-push).")
        return

    if not github_token or not github_repo or "/" not in github_repo:
        print("  ⚠ GitHub not configured — skipping push.")
        print(f"    Upload {output_file} to your GitHub repo manually.")
        return

    content_str = json.dumps(data, indent=2, default=str)
    encoded = base64.b64encode(content_str.encode()).decode()
    api_url = f"https://api.github.com/repos/{github_repo}/contents/{os.path.basename(output_file)}"
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    sha = None
    try:
        r = requests.get(api_url, headers=headers, timeout=10)
        if r.status_code == 200:
            sha = r.json().get("sha")
    except Exception:
        pass

    payload = {
        "message": f"Newsletter update — {datetime.now().strftime('%Y-%m-%d')}",
        "content": encoded,
    }
    if sha:
        payload["sha"] = sha

    try:
        r = requests.put(api_url, headers=headers, json=payload, timeout=20)
        r.raise_for_status()
        print(f"  ✓ Pushed to github.com/{github_repo}")
        print(f"    Site updates in ~60 seconds.")
    except Exception as e:
        print(f"  ✗ GitHub push failed: {e}")
        print(f"    Upload {output_file} manually.")

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()
    anthropic_api_key = require_env("ANTHROPIC_API_KEY")
    github_token = os.environ.get("GITHUB_TOKEN")
    github_repo = os.environ.get("GITHUB_REPO") or os.environ.get("GITHUB_REPOSITORY")

    print("=" * 60)
    print("  Healthcare Regulatory & Policy Monitor — Step 2: Newsletter Generator")
    print("=" * 60)

    if not os.path.exists(args.input):
        print(f"\n✗ {args.input} not found. Run scrape.py first.")
        sys.exit(1)

    with open(args.input, "r", encoding="utf-8") as f:
        raw = json.load(f)

    articles = raw.get("articles", [])
    week_of = datetime.now().strftime("%B %d, %Y")
    run_date = datetime.now().strftime("%Y-%m-%d")
    scraped = raw.get("scraped_at", "")

    print(f"\n  Loaded {len(articles)} articles from {args.input}")
    if scraped:
        print(f"  Scraped at: {scraped[:19]}")

    global EDITORIAL_MODEL
    EDITORIAL_MODEL = args.model
    client = Anthropic(api_key=anthropic_api_key)

    # Load archive early — needed for persistence merge after classification
    print("\n[0/4] Loading archive...")
    archive = load_archive(ARCHIVE_FILE)

    reg_pool = [a for a in articles if a.get("source_type") in ("regulatory", "both")][:MAX_ARTICLES_PER_EDITION]
    pol_pool = [a for a in articles if a.get("source_type") in ("policy", "both")]
    pol_urls = {a.get("url") for a in pol_pool}
    # Exclude 510(k) clearances from policy spillover — they are guaranteed policy discards
    pol_pool += [
        a for a in reg_pool
        if a.get("url") not in pol_urls and a.get("doc_type") != "510(k)"
    ][:POLICY_SPILLOVER_FROM_REG]
    pol_pool = pol_pool[:MAX_ARTICLES_PER_EDITION]

    print(f"\n  Consulting pool: {len(reg_pool)} articles")
    print(f"  Policy pool:     {len(pol_pool)} articles")

    print("\n[1/4] Classifying...")
    consulting_raw = classify_articles(
        client, reg_pool, CONSULTING_CATEGORIES,
        "healthcare consultant",
        "healthcare strategy consultants, compliance officers, and healthcare executives",
        edition="consulting",
    )
    policy_raw = classify_articles(
        client, pol_pool, POLICY_CATEGORIES,
        "healthcare policy professional",
        "healthcare policy professionals, legislative staff, lobbyists, and government affairs leads",
        edition="policy",
    )

    # Keep ALL articles — urgency/importance_score handle triage in the frontend.
    # Only hard-drop items whose content is definitively non-healthcare (noise signals).
    urgency_key = lambda a: {"urgent": 0, "important": 1, "routine": 2, "discard": 3}.get(
        a.get("urgency", "routine"), 2
    )
    consulting_articles = sorted(
        [_ensure_article_fields(a, CONSULTING_CATEGORIES) for a in consulting_raw if not _is_noise(a)],
        key=urgency_key)
    policy_articles = sorted(
        [_ensure_article_fields(a, POLICY_CATEGORIES) for a in policy_raw if not _is_noise(a)],
        key=urgency_key)

    print(f"\n  After filter: {len(consulting_articles)} consulting, {len(policy_articles)} policy articles")

    # Reconcile urgency for articles that appear in both editions
    consulting_articles, policy_articles = reconcile_shared_urgency(consulting_articles, policy_articles)

    # Merge high-signal persisted articles from prior runs not in this scrape
    c_urls = {a.get("url", "") for a in consulting_articles}
    p_urls = {a.get("url", "") for a in policy_articles}
    c_persisted = get_persisted_articles(archive, c_urls, "consulting", run_date)
    p_persisted = get_persisted_articles(archive, p_urls, "policy", run_date)
    if c_persisted:
        print(f"  ↺ Carried forward {len(c_persisted)} persisted consulting article(s)")
        consulting_articles = sorted(
            consulting_articles + [_ensure_article_fields(a, CONSULTING_CATEGORIES) for a in c_persisted],
            key=urgency_key)
    if p_persisted:
        print(f"  ↺ Carried forward {len(p_persisted)} persisted policy article(s)")
        policy_articles = sorted(
            policy_articles + [_ensure_article_fields(a, POLICY_CATEGORIES) for a in p_persisted],
            key=urgency_key)

    print("\n[1.5/4] Grouping articles by topic...")
    consulting_articles = group_articles_by_topic(client, consulting_articles)
    policy_articles     = group_articles_by_topic(client, policy_articles)

    print("\n[2/4] Generating category notes...")
    consulting_notes = generate_category_notes(client, consulting_articles, "consulting", CONSULTING_CATEGORIES)
    policy_notes = generate_category_notes(client, policy_articles, "policy", POLICY_CATEGORIES)

    print("\n[3/4] Generating editorial...")
    consulting_ed = generate_editorial(client, consulting_articles, "consulting", week_of)
    policy_ed = generate_editorial(client, policy_articles, "policy", week_of)

    # ── Archive: already loaded at [0/4]; update with this run's results ──
    print("\n[3.5/4] Updating archive...")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "week_of": week_of,
        "consulting": {
            **consulting_ed,
            "categories": CONSULTING_CATEGORIES,
            "category_notes": consulting_notes,
            "articles": consulting_articles,
        },
        "policy": {
            **policy_ed,
            "categories": POLICY_CATEGORIES,
            "category_notes": policy_notes,
            "articles": policy_articles,
        },
    }

    # Append this run to the archive FIRST. compute_whats_new diffs against the
    # immediately-previous run (prior_runs[-2], since [-1] is now this run), so the
    # archive must already contain this run when we compute the digest.
    archive = update_archive(archive, output, run_date)

    consulting_new = compute_whats_new(consulting_articles, archive, "consulting")
    policy_new     = compute_whats_new(policy_articles,     archive, "policy")

    output["consulting"]["whats_new"] = consulting_new
    output["policy"]["whats_new"]     = policy_new

    if consulting_new:
        print(f"  ✓ Consulting what's new: {consulting_new['count']} new items "
              f"({consulting_new['urgent_count']} urgent) since {consulting_new['previous_run_date']}")
    else:
        print("  ✓ Consulting what's new: first run — no prior run to compare")

    if policy_new:
        print(f"  ✓ Policy what's new: {policy_new['count']} new items "
              f"({policy_new['urgent_count']} urgent) since {policy_new['previous_run_date']}")
    else:
        print("  ✓ Policy what's new: first run — no prior run to compare")

    # Validate BEFORE persisting the archive. The run was appended to the archive
    # in memory (so compute_whats_new could diff correctly), but if validation fails
    # we exit without writing it — a non-published run must not pollute the archive's
    # "what's new" baseline.
    try:
        validate_newsletter_data(output)
    except ValueError as exc:
        print(f"\n✗ Validation failed: {exc}")
        sys.exit(1)

    print("\n[4/4] Publishing...")
    save_archive(archive, ARCHIVE_FILE)
    save_json(output, args.output)
    push_to_github(output, github_token, github_repo, args.output, args.no_push)
    # Also push archive (it's small enough; keeps GitHub Pages host in sync)
    push_to_github(archive, github_token, github_repo, ARCHIVE_FILE, args.no_push)

    def _tier_summary(articles):
        tiers = {"urgent": 0, "important": 0, "routine": 0, "discard": 0}
        for a in articles:
            tiers[a.get("urgency", "routine")] = tiers.get(a.get("urgency", "routine"), 0) + 1
        scored = [a.get("importance_score", 0) for a in articles if isinstance(a.get("importance_score"), (int, float))]
        avg = round(sum(scored) / len(scored)) if scored else "n/a"
        return tiers, avg

    cat_counts_c = {}
    for a in consulting_articles:
        cat_counts_c[a.get("category", "?")] = cat_counts_c.get(a.get("category", "?"), 0) + 1
    cat_counts_p = {}
    for a in policy_articles:
        cat_counts_p[a.get("category", "?")] = cat_counts_p.get(a.get("category", "?"), 0) + 1

    c_tiers, c_avg = _tier_summary(consulting_articles)
    p_tiers, p_avg = _tier_summary(policy_articles)

    print("\n" + "=" * 60)
    print(f"  ✓ Consulting: {len(consulting_articles)} articles  "
          f"(urgent {c_tiers['urgent']} · important {c_tiers['important']} · "
          f"routine {c_tiers['routine']} · discard {c_tiers['discard']})  avg score {c_avg}")
    for cat, n in sorted(cat_counts_c.items(), key=lambda x: -x[1]):
        print(f"    {n:>3}  {cat}")
    print(f"\n  ✓ Policy: {len(policy_articles)} articles  "
          f"(urgent {p_tiers['urgent']} · important {p_tiers['important']} · "
          f"routine {p_tiers['routine']} · discard {p_tiers['discard']})  avg score {p_avg}")
    for cat, n in sorted(cat_counts_p.items(), key=lambda x: -x[1]):
        print(f"    {n:>3}  {cat}")
    print(f"\n  ✓ Week of: {week_of}")
    print("=" * 60)

if __name__ == "__main__":
    main()
