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
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from anthropic import Anthropic

# ── CONFIG ───────────────────────────────────────────────────────────────────
CLASSIFY_MODEL  = os.environ.get("ANTHROPIC_CLASSIFY_MODEL", "claude-haiku-4-5-20251001")
EDITORIAL_MODEL = os.environ.get("ANTHROPIC_MODEL",          "claude-sonnet-4-6")
GITHUB_FILE_PATH = "newsletter_data.json"

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

def is_health_relevant(a):
    if (a.get("category") or "").upper() == "DISCARD":
        return False
    text = " ".join([
        a.get("headline") or "",
        a.get("title") or "",
        a.get("summary") or "",
        a.get("snippet") or "",
    ]).lower()
    return not any(sig in text for sig in NOISE_SIGNALS)

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

    match = re.search(r"(\[.*\]|\{.*\})", text, re.S)
    if match:
        text = match.group(1)
    return json.loads(text)


def validate_article_output(article, idx=None):
    errors = []
    if not isinstance(article, dict):
        errors.append("article is not an object")
        return errors

    required = ["category", "urgency", "headline", "summary", "implication", "is_comment_period"]
    for field in required:
        if field not in article:
            errors.append(f"missing {field}")
            continue
        if field == "is_comment_period":
            if not isinstance(article[field], bool):
                errors.append("is_comment_period must be boolean")
        else:
            if not isinstance(article[field], str) or not article[field].strip():
                errors.append(f"{field} must be a non-empty string")

    urgency = article.get("urgency")
    if urgency not in ("routine", "important", "urgent"):
        errors.append(f"invalid urgency: {urgency}")
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
        if not isinstance(sub["categories"], list):
            raise ValueError(f"{edition}.categories must be a list")
        if not isinstance(sub["articles"], list):
            raise ValueError(f"{edition}.articles must be a list")
        for idx, article in enumerate(sub["articles"]):
            errors = validate_article_output(article, idx)
            if errors:
                raise ValueError(f"{edition} article {idx} errors: {errors}")


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
def classify_articles(client, articles, categories, audience_label, audience_desc):
    BATCH = 15
    total = len(articles)
    print(f"\n  Classifying {total} articles for {audience_label} audience...")

    batches   = [articles[i:i+BATCH] for i in range(0, total, BATCH)]
    n_batches = len(batches)

    # Static system prompt — cached across all batches for this edition run
    system_text = f"""You are a senior healthcare analyst writing for {audience_desc}.

Classify each article using ONLY these exact category strings:
{chr(10).join(f'- {c}' for c in categories)}

{CAT_GUIDANCE}

Return ONLY a valid JSON array — no markdown, no preamble:
[{{
  "idx": <0-based index within this batch>,
  "category": "<exact category string or DISCARD>",
  "urgency": "routine"|"important"|"urgent",
  "headline": "<rewritten headline, analyst-voice, 10-15 words, specific and informative>",
  "summary": "<2-3 sentences: what happened, regulatory/policy context, why it matters>",
  "implication": "<1 sentence: specific takeaway or action item for {audience_label}s>",
  "is_comment_period": <true if proposed rule open for public comment, else false>
}}]

Urgency: urgent=final rules effective immediately/major enforcement, important=proposed rules/significant guidance, routine=standard notices"""

    results_map = {}

    def process_batch(batch_idx, batch):
        batch_text = "\n\n".join(
            f"[{j}] TITLE: {a['title']}\n"
            f"SOURCE: {a['source_name']}\n"
            f"DATE: {a['published']}\n"
            f"SNIPPET: {a['snippet'][:300]}"
            for j, a in enumerate(batch)
        )
        for attempt in range(2):
            try:
                resp = client.messages.create(
                    model=CLASSIFY_MODEL,
                    max_tokens=3500,
                    system=[{"type": "text", "text": system_text, "cache_control": {"type": "ephemeral"}}],
                    messages=[{"role": "user", "content": batch_text}],
                )
                parsed = parse_json_response(resp.content[0].text.strip())
                classified = []
                for item in parsed:
                    idx = item.get("idx", 0)
                    if 0 <= idx < len(batch):
                        classified.append({**batch[idx], **item})
                print(f"    ✓ Batch {batch_idx+1}/{n_batches} — {len(classified)} classified")
                return batch_idx, classified
            except Exception as e:
                if attempt == 0:
                    print(f"    ✗ Batch {batch_idx+1} retry: {e}")
                    time.sleep(1)
                else:
                    print(f"    ✗ Batch {batch_idx+1} failed: {e}")
                    return batch_idx, [{
                        **a,
                        "category": categories[-1], "urgency": "routine",
                        "headline": a["title"], "summary": a["snippet"],
                        "implication": "", "is_comment_period": bool(a.get("comment_deadline")),
                    } for a in batch]
        return batch_idx, []

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(process_batch, i, b): i for i, b in enumerate(batches)}
        for future in as_completed(futures):
            idx, items = future.result()
            results_map[idx] = items

    results = []
    for i in range(n_batches):
        results.extend(results_map.get(i, []))
    return results

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
  "editors_note": "<4-5 sentences synthesizing the week — what the dominant theme is, what tensions exist, what readers should watch. Analyst voice. No 'this week we cover' phrasing.>"
}}"""

    try:
        response = client.messages.create(
            model=EDITORIAL_MODEL,
            max_tokens=700,
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
            "subject_line":  f"Healthcare Regulatory & Policy Monitor — {edition.title()} — {week_of}",
            "theme_of_week": "Healthcare regulatory developments",
            "editors_note":  "This week's briefing covers the latest regulatory and policy developments.",
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
    scraped = raw.get("scraped_at", "")

    print(f"\n  Loaded {len(articles)} articles from {args.input}")
    if scraped:
        print(f"  Scraped at: {scraped[:19]}")

    global EDITORIAL_MODEL
    EDITORIAL_MODEL = args.model
    client = Anthropic(api_key=anthropic_api_key)

    reg_pool = [a for a in articles if a.get("source_type") in ("regulatory", "both")][:60]
    pol_pool = [a for a in articles if a.get("source_type") in ("policy", "both")]
    pol_urls = {a.get("url") for a in pol_pool}
    pol_pool += [a for a in reg_pool if a.get("url") not in pol_urls][:20]
    pol_pool = pol_pool[:60]

    print(f"\n  Consulting pool: {len(reg_pool)} articles")
    print(f"  Policy pool:     {len(pol_pool)} articles")

    print("\n[1/4] Classifying...")
    consulting_raw = classify_articles(
        client, reg_pool, CONSULTING_CATEGORIES,
        "healthcare consultant",
        "healthcare strategy consultants, compliance officers, and healthcare executives"
    )
    policy_raw = classify_articles(
        client, pol_pool, POLICY_CATEGORIES,
        "healthcare policy professional",
        "healthcare policy professionals, legislative staff, lobbyists, and government affairs leads"
    )

    urgency_key = lambda a: {"urgent": 0, "important": 1, "routine": 2}.get(a.get("urgency", "routine"), 2)
    consulting_articles = sorted([a for a in consulting_raw if is_health_relevant(a)], key=urgency_key)
    policy_articles = sorted([a for a in policy_raw if is_health_relevant(a)], key=urgency_key)

    print(f"\n  After filter: {len(consulting_articles)} consulting, {len(policy_articles)} policy articles")

    print("\n[2/4] Generating category notes...")
    consulting_notes = generate_category_notes(client, consulting_articles, "consulting", CONSULTING_CATEGORIES)
    policy_notes = generate_category_notes(client, policy_articles, "policy", POLICY_CATEGORIES)

    print("\n[3/4] Generating editorial...")
    consulting_ed = generate_editorial(client, consulting_articles, "consulting", week_of)
    policy_ed = generate_editorial(client, policy_articles, "policy", week_of)

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

    try:
        validate_newsletter_data(output)
    except ValueError as exc:
        print(f"\n✗ Validation failed: {exc}")
        sys.exit(1)

    print("\n[4/4] Publishing...")
    save_json(output, args.output)
    push_to_github(output, github_token, github_repo, args.output, args.no_push)

    cat_counts_c = {}
    for a in consulting_articles:
        cat_counts_c[a.get("category", "?")] = cat_counts_c.get(a.get("category", "?"), 0) + 1
    cat_counts_p = {}
    for a in policy_articles:
        cat_counts_p[a.get("category", "?")] = cat_counts_p.get(a.get("category", "?"), 0) + 1

    print("\n" + "=" * 60)
    print(f"  ✓ Consulting: {len(consulting_articles)} articles")
    for cat, n in sorted(cat_counts_c.items(), key=lambda x: -x[1]):
        print(f"    {n:>3}  {cat}")
    print(f"\n  ✓ Policy: {len(policy_articles)} articles")
    for cat, n in sorted(cat_counts_p.items(), key=lambda x: -x[1]):
        print(f"    {n:>3}  {cat}")
    print(f"\n  ✓ Week of: {week_of}")
    print("=" * 60)

if __name__ == "__main__":
    main()
