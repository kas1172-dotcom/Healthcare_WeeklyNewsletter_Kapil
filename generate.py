#!/usr/bin/env python3
"""
HealthPulse Intelligence — Step 2: Newsletter Generator
=======================================================
Reads raw_articles.json, classifies with Claude Sonnet,
generates two newsletter editions, pushes to GitHub.

SETUP:
    pip3 install anthropic

CONFIGURE:
    Set ANTHROPIC_API_KEY and GITHUB_TOKEN/GITHUB_REPO below.

RUN:
    python3 generate.py

REQUIRES:
    raw_articles.json (from scrape.py)
"""

import os, json, re, base64, sys, time
import requests
from anthropic import Anthropic
from datetime import datetime, timezone

# ── CONFIG ────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "BJIUNcwTtjVr9Xko7Xpa82azZq7GzQ0cHq8_e5kJmabJvX1nbm7rdkgmC5lcGZao_KHg-VnDMH0kG2DzXjnw4A-0FQ55AAA")
GITHUB_TOKEN      = os.environ.get("GITHUB_TOKEN",      "YOUR_GITHUB_TOKEN_HERE")
GITHUB_REPO       = os.environ.get("GITHUB_REPO",       "YOUR_USERNAME/YOUR_REPO")
GITHUB_FILE_PATH  = "newsletter_data.json"

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

# ── CLASSIFICATION ────────────────────────────────────────────────────────────
def classify_articles(client, articles, categories, audience_label, audience_desc):
    print(f"\n  Classifying {len(articles)} articles for {audience_label} audience...")
    BATCH   = 8
    results = []

    for i in range(0, len(articles), BATCH):
        batch = articles[i:i+BATCH]
        batch_text = "\n\n".join(
            f"[{j}] TITLE: {a['title']}\n"
            f"SOURCE: {a['source_name']}\n"
            f"DATE: {a['published']}\n"
            f"SNIPPET: {a['snippet'][:300]}"
            for j, a in enumerate(batch)
        )

        prompt = f"""You are a senior healthcare analyst writing for {audience_desc}.

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

Urgency guide:
- urgent: final rules effective immediately, major enforcement actions, breaking policy shifts
- important: proposed rules, significant guidances, notable enforcement trends, major reports
- routine: routine notices, minor updates, standard publications

Articles:
{batch_text}"""

        try:
            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=3000,
                messages=[{"role": "user", "content": prompt}]
            )
            text = response.content[0].text.strip()
            if "```" in text:
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            parsed = json.loads(text.strip())
            for item in parsed:
                idx = item.get("idx", 0)
                if 0 <= idx < len(batch):
                    results.append({**batch[idx], **item})
            print(f"    ✓ Batch {i//BATCH+1}/{(len(articles)-1)//BATCH+1} — {len(parsed)} classified")
        except Exception as e:
            print(f"    ✗ Batch error: {e}")
            for j, a in enumerate(batch):
                results.append({
                    **a,
                    "category":          categories[-1],
                    "urgency":           "routine",
                    "headline":          a["title"],
                    "summary":           a["snippet"],
                    "implication":       "",
                    "is_comment_period": bool(a.get("comment_deadline")),
                })
        time.sleep(0.3)

    return results

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
    prompt = f"""You are the editor of HealthPulse {edition.title()}, a premium weekly newsletter for {audience_map[edition]}.

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
            model="claude-sonnet-4-6",
            max_tokens=700,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text.strip())
    except Exception as e:
        print(f"    ✗ Editorial error: {e}")
        return {
            "subject_line":  f"HealthPulse {edition.title()} — {week_of}",
            "theme_of_week": "Healthcare regulatory developments",
            "editors_note":  "This week's briefing covers the latest regulatory and policy developments.",
        }

# ── GITHUB PUSH ───────────────────────────────────────────────────────────────
def push_to_github(data):
    content_str = json.dumps(data, indent=2, default=str)
    with open("newsletter_data.json", "w", encoding="utf-8") as f:
        f.write(content_str)
    print("  ✓ Saved newsletter_data.json")

    if GITHUB_TOKEN == "YOUR_GITHUB_TOKEN_HERE" or "/" not in GITHUB_REPO:
        print("  ⚠ GitHub not configured — skipping push.")
        print("    Upload newsletter_data.json to your GitHub repo manually.")
        return

    encoded = base64.b64encode(content_str.encode()).decode()
    api_url  = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
    headers  = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    sha = None
    try:
        r = requests.get(api_url, headers=headers, timeout=10)
        if r.status_code == 200:
            sha = r.json().get("sha")
    except:
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
        print(f"  ✓ Pushed to github.com/{GITHUB_REPO}")
        print(f"    Site updates in ~60 seconds.")
    except Exception as e:
        print(f"  ✗ GitHub push failed: {e}")
        print(f"    Upload newsletter_data.json manually.")

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  HealthPulse — Step 2: Newsletter Generator")
    print("=" * 60)

    if ANTHROPIC_API_KEY == "YOUR_ANTHROPIC_KEY_HERE":
        print("\n✗ Set your ANTHROPIC_API_KEY in the CONFIG section.")
        sys.exit(1)

    # Load raw articles
    if not os.path.exists("raw_articles.json"):
        print("\n✗ raw_articles.json not found. Run scrape.py first.")
        sys.exit(1)

    with open("raw_articles.json", "r", encoding="utf-8") as f:
        raw = json.load(f)

    articles  = raw.get("articles", [])
    week_of   = datetime.now().strftime("%B %d, %Y")
    scraped   = raw.get("scraped_at", "")

    print(f"\n  Loaded {len(articles)} articles from raw_articles.json")
    if scraped:
        print(f"  Scraped at: {scraped[:19]}")

    client = Anthropic(api_key=ANTHROPIC_API_KEY)

    # Split by audience
    reg_pool = [a for a in articles if a["source_type"] in ("regulatory", "both")][:60]
    pol_pool = [a for a in articles if a["source_type"] in ("policy", "both")]
    # Add regulatory items to policy pool that aren't already there
    pol_urls = {a["url"] for a in pol_pool}
    pol_pool += [a for a in reg_pool if a["url"] not in pol_urls][:20]
    pol_pool  = pol_pool[:60]

    print(f"\n  Consulting pool: {len(reg_pool)} articles")
    print(f"  Policy pool:     {len(pol_pool)} articles")

    # Classify
    print("\n[1/3] Classifying...")
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

    # Filter noise and sort
    urgency_key = lambda a: {"urgent":0,"important":1,"routine":2}.get(a.get("urgency","routine"),2)
    consulting_articles = sorted([a for a in consulting_raw if is_health_relevant(a)], key=urgency_key)
    policy_articles     = sorted([a for a in policy_raw     if is_health_relevant(a)], key=urgency_key)

    print(f"\n  After filter: {len(consulting_articles)} consulting, {len(policy_articles)} policy articles")

    # Generate editorial
    print("\n[2/3] Generating editorial...")
    consulting_ed = generate_editorial(client, consulting_articles, "consulting", week_of)
    policy_ed     = generate_editorial(client, policy_articles,     "policy",     week_of)

    # Build output
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "week_of":      week_of,
        "consulting": {
            **consulting_ed,
            "categories": CONSULTING_CATEGORIES,
            "articles":   consulting_articles,
        },
        "policy": {
            **policy_ed,
            "categories": POLICY_CATEGORIES,
            "articles":   policy_articles,
        },
    }

    # Publish
    print("\n[3/3] Publishing...")
    push_to_github(output)

    # Summary
    cat_counts_c = {}
    for a in consulting_articles:
        cat_counts_c[a.get("category","?")] = cat_counts_c.get(a.get("category","?"),0)+1
    cat_counts_p = {}
    for a in policy_articles:
        cat_counts_p[a.get("category","?")] = cat_counts_p.get(a.get("category","?"),0)+1

    print("\n" + "="*60)
    print(f"  ✓ Consulting: {len(consulting_articles)} articles")
    for cat, n in sorted(cat_counts_c.items(), key=lambda x: -x[1]):
        print(f"    {n:>3}  {cat}")
    print(f"\n  ✓ Policy: {len(policy_articles)} articles")
    for cat, n in sorted(cat_counts_p.items(), key=lambda x: -x[1]):
        print(f"    {n:>3}  {cat}")
    print(f"\n  ✓ Week of: {week_of}")
    print("="*60)

if __name__ == "__main__":
    main()
