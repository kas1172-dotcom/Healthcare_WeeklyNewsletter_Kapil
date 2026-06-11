#!/usr/bin/env python3
"""
Healthcare Regulatory & Policy Monitor — Step 1: Scraper
===========================================
Fetches all sources and saves raw_articles.json.
No NLP API key required for scraping. Run this first.

SETUP:   pip3 install requests feedparser beautifulsoup4
RUN:     python3 scrape.py
OUTPUT:  raw_articles.json
"""

import os, json, re, sys, time
import argparse
import requests
import feedparser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ── CONFIG ────────────────────────────────────────────────────────────────────
DAYS_BACK        = 60
MAX_ITEMS        = 30   # max items per RSS feed
CONGRESS_API_KEY      = os.environ.get("CONGRESS_API_KEY", "DEMO_KEY")
# Optional: free token from https://www.courtlistener.com/help/api/rest/ improves court results
COURTLISTENER_TOKEN   = os.environ.get("COURTLISTENER_TOKEN", "")

# ── HTTP SESSION ──────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/rss+xml, application/xml, text/xml, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control":   "no-cache",
})
# Simplified retry strategy to avoid recursion issues
retry_strategy = Retry(
    total=2,
    backoff_factor=1.0,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET"],
)
SESSION.mount("https://", HTTPAdapter(max_retries=retry_strategy))
SESSION.mount("http://", HTTPAdapter(max_retries=retry_strategy))

# ── FILTERS ───────────────────────────────────────────────────────────────────
FR_AGENCIES = [
    "centers-for-medicare-medicaid-services",
    "food-and-drug-administration",
    "drug-enforcement-administration",
    "office-for-civil-rights-hhs",
    "office-of-the-national-coordinator-for-health-information-technology",
    "substance-abuse-and-mental-health-services-administration",
    "health-resources-and-services-administration",
    "national-institutes-of-health",
    "centers-for-disease-control-and-prevention",
    "agency-for-healthcare-research-and-quality",
]

NON_HEALTH_AGENCIES = [
    "transportation-department", "energy-department", "interior-department",
    "commerce-department", "environmental-protection-agency",
    "federal-communications-commission", "homeland-security-department",
    "national-foundation-on-the-arts", "government-ethics-office",
    "nuclear-regulatory-commission", "securities-and-exchange-commission",
    "pension-benefit-guaranty-corporation",
    "national-archives-and-records-administration",
]

VA_HEALTH_KW = [
    "health", "medical", "clinical", "mental health", "behavioral", "hospital",
    "pharmacy", "prosthetic", "rehabilitation", "caregiver", "suicide", "ptsd",
    "community care", "veterans health", "telehealth",
]

# Broad health keywords for RSS filtering
HEALTH_KW = [
    "health", "medicare", "medicaid", "hospital", "drug", "patient", "physician",
    "provider", "coverage", "clinical", "medical", "nursing", "telehealth",
    "behavioral", "mental", "opioid", "pharmaceutical", "insurance", "hhs", "cms",
    "fda", "hipaa", "prior authorization", "reimbursement", "billing", "coding",
    "payer", "beneficiary", "therapy", "hospice", "pharmacy", "vaccine",
    "public health", "controlled substance", "prescription", "ambulatory",
    "inpatient", "outpatient", "chip", "affordable care", "snf", "ltach",
    "home health", "dme", "340b", "part d", "part b", "accountable care",
    "value-based", "bundled payment", "oncology", "biosimilar", "device",
    "diagnostic", "false claims", "anti-kickback", "stark", "fraud",
]

# Expanded Congress keywords — catches bill titles that don't use "health"
CONGRESS_KW = [
    # Direct health terms
    "health", "medicare", "medicaid", "drug", "hospital", "patient", "coverage",
    "care", "pharma", "insurance", "behavioral", "mental", "opioid", "nursing",
    "telehealth", "physician", "provider", "payer", "clinical", "medical",
    "prescription", "vaccine", "disease", "cancer", "diabetes", "chronic",
    "affordable care", "chip", "fda", "cms", "hhs", "false claims",
    # Bill-title language that signals health legislation
    "denial", "prior auth", "network adequacy", "formulary", "deductible",
    "copay", "premium", "transparency", "surprise billing", "balance billing",
    "step therapy", "gold carding", "utilization review", "medical loss",
    "biosimilar", "340b", "rebate", "pharmacy benefit", "pbm",
    "nursing home", "long-term care", "home health", "hospice", "palliative",
    "maternal", "childbirth", "pregnancy", "reproductive", "abortion",
    "addiction", "recovery", "substance use", "alcohol", "tobacco",
    "obesity", "glp-1", "insulin", "diabetes",
    "disparities", "equity", "uninsured", "underinsured",
    "rural health", "critical access", "federally qualified",
    "community health", "workforce", "clinician", "nurse", "doctor",
    "hospital merger", "health system", "integrated delivery",
]

HEALTH_COMMITTEES = {
    "senate": {
        "SSHR": "Senate HELP",
        "SSFI": "Senate Finance",
    },
    "house": {
        "HSIF": "House Energy & Commerce",
        "HSWM": "House Ways & Means",
    },
}

# ── HELPERS ───────────────────────────────────────────────────────────────────
def strip_html(text):
    return re.sub(r"<[^>]+>", " ", text or "").strip()

def cutoff():
    return datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

def is_health(text):
    t = text.lower()
    return any(kw in t for kw in HEALTH_KW)

def make_item(title, url, published, source_name, source_url,
              snippet="", doc_type="", comment_deadline="", source_type="regulatory"):
    pub_str = (published or "").strip()[:10]
    # Never fall back to today — unknown is unknown.
    # discovery_date is injected by main() after all fetching completes.
    return {
        "title":            title,
        "url":              url,
        "published":        pub_str,
        "published_iso":    pub_str,
        "date_unknown":     not bool(pub_str),
        "source_name":      source_name,
        "source_url":       source_url,
        "snippet":          snippet[:600],
        "doc_type":         doc_type,
        "comment_deadline": comment_deadline,
        "source_type":      source_type,
    }

def normalize_url(url):
    if not url:
        return url
    try:
        parts = urlparse(url)
        query = sorted(parse_qsl(parts.query, keep_blank_values=True))
        normalized = urlunparse(parts._replace(query=urlencode(query, doseq=True)))
        return normalized
    except Exception:
        return url


# ── DATE HELPERS ──────────────────────────────────────────────────────────────

def _parse_date_text(text):
    """Parse a date from free text. Returns 'YYYY-MM-DD' or ''."""
    if not text:
        return ""
    # ISO format
    m = re.search(r'(\d{4}-\d{2}-\d{2})', text)
    if m:
        return m.group(1)
    # "Month DD, YYYY" or "Month DD YYYY"
    m = re.search(
        r'(January|February|March|April|May|June|July|August|September|October|November|December)'
        r'\s+(\d{1,2}),?\s+(\d{4})',
        text, re.I,
    )
    if m:
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%B %d %Y").strftime("%Y-%m-%d")
        except ValueError:
            pass
    # MM/DD/YYYY
    m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', text)
    if m:
        try:
            return datetime.strptime(f"{m.group(1)}/{m.group(2)}/{m.group(3)}", "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            pass
    return ""


def _date_from_url(url):
    """Extract a date from a URL path like /YYYY/MM/DD/ or /YYYY/MM/. Returns 'YYYY-MM-DD' or ''."""
    if not url:
        return ""
    m = re.search(r'/(\d{4})/(\d{2})(?:/(\d{2}))?', url)
    if m:
        year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3) or 1)
        try:
            dt = datetime(year, month, day)
            if datetime(2000, 1, 1) <= dt <= datetime.now():
                return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return ""


def _find_date_near_element(el):
    """
    Search for a publication date near a BeautifulSoup anchor element.
    Strategy (in order of reliability):
      1. URL path — OIG/CMS URLs embed YYYY/MM/DD
      2. <time datetime="..."> in ancestor subtrees (up to 3 levels up)
      3. Date-like text in direct children of ancestors
    Returns 'YYYY-MM-DD' or '' if nothing is found (caller sets date_unknown).
    """
    # 1. URL path is the most reliable signal for OIG (/fraud/enforcement/YYYY/MM/title/)
    href = el.get("href", "")
    d = _date_from_url(href)
    if d:
        return d

    # 2. Walk up to 3 ancestor levels
    node = el
    for _ in range(3):
        node = getattr(node, "parent", None)
        if node is None:
            break
        # Look for <time datetime="YYYY-MM-DD">
        for time_el in node.find_all("time", recursive=True):
            dt_attr = time_el.get("datetime", "")
            if re.match(r'\d{4}-\d{2}-\d{2}', dt_attr):
                return dt_attr[:10]
            d = _parse_date_text(time_el.get_text(strip=True))
            if d:
                return d
        # Date-like text in direct children
        for child in node.children:
            text = (child.get_text(strip=True) if hasattr(child, "get_text") else str(child).strip())
            if len(text) < 5 or len(text) > 80:
                continue
            d = _parse_date_text(text)
            if d:
                return d
    return ""


def get_url(url, timeout=15, **kwargs):
    try:
        return SESSION.get(url, timeout=timeout, **kwargs)
    except requests.RequestException:
        return None


def fetch_rss(name, urls_or_url, source_type, health_filter=False, date_filter=True):
    """Fetch RSS. urls_or_url can be a string or list of fallback URLs."""
    print(f"  {name}...")
    urls = [urls_or_url] if isinstance(urls_or_url, str) else urls_or_url
    cutoff_dt = cutoff()

    for url in urls:
        try:
            r = get_url(url, timeout=15)
            if r.status_code != 200:
                continue
            feed = feedparser.parse(r.content)
            if not feed.entries:
                continue
            feed_title = getattr(feed.feed, "title", name)
            feed_link  = getattr(feed.feed, "link",  "")
            items = []
            for entry in list(feed.entries)[:MAX_ITEMS]:
                pub = None
                for attr in ("published_parsed", "updated_parsed"):
                    val = getattr(entry, attr, None)
                    if val:
                        try:
                            pub = datetime(*val[:6], tzinfo=timezone.utc)
                            break
                        except Exception:
                            pass
                # Skip only if we have a real date and it's outside the lookback window.
                # Items with no parseable date are kept (date_unknown=True); they are NOT
                # silently passed through date filters in the frontend.
                if pub is not None and date_filter and pub < cutoff_dt:
                    continue
                pub_date_str = pub.strftime("%Y-%m-%d") if pub is not None else ""
                snippet = ""
                if hasattr(entry, "summary"):
                    snippet = strip_html(entry.summary)
                elif hasattr(entry, "content") and entry.content:
                    snippet = strip_html(entry.content[0].get("value", ""))
                title = getattr(entry, "title", "").strip()
                link  = getattr(entry, "link", "").strip()
                if not title or not link:
                    continue
                if health_filter and not is_health(title + " " + snippet):
                    continue
                items.append(make_item(
                    title, link, pub_date_str,
                    feed_title, feed_link, snippet, "", "", source_type
                ))
            if items or not date_filter:
                print(f"    ✓ {len(items)} items")
                return items
        except Exception:
            continue

    print(f"    ✗ No working URL found")
    return []

# ── GOVERNMENT API FETCHERS ───────────────────────────────────────────────────

def _fetch_fr_page(agency_qs, date_str, page=1):
    """Fetch one page of Federal Register results. Returns (results_list, total_pages)."""
    url = (
        f"https://www.federalregister.gov/api/v1/documents.json"
        f"?per_page=100&order=newest&page={page}&{agency_qs}"
        f"&conditions[publication_date][gte]={date_str}"
        f"&conditions[type][]=RULE&conditions[type][]=PRORULE&conditions[type][]=NOTICE"
        f"&fields[]=title&fields[]=abstract&fields[]=publication_date"
        f"&fields[]=html_url&fields[]=agencies&fields[]=type&fields[]=comments_close_on"
    )
    r = get_url(url, timeout=20)
    if r is None or r.status_code != 200:
        return [], 0
    data = r.json()
    total_pages = (data.get("total_pages") or 1)
    return data.get("results", []), total_pages


def fetch_federal_register(verbose=False):
    print("  Federal Register API...")
    cutoff_dt = cutoff()
    date_str  = cutoff_dt.strftime("%Y-%m-%d")
    agency_qs = "&".join(f"agency_slugs[]={a}" for a in FR_AGENCIES)

    # Fetch up to 3 pages to handle high-volume windows without hammering the API
    all_raw = []
    try:
        results, total_pages = _fetch_fr_page(agency_qs, date_str, page=1)
        all_raw.extend(results)
        if verbose:
            print(f"    [FR debug] page 1/{total_pages}: {len(results)} raw results")
        for page in range(2, min(total_pages + 1, 4)):  # pages 2 and 3 only
            results2, _ = _fetch_fr_page(agency_qs, date_str, page=page)
            all_raw.extend(results2)
            if verbose:
                print(f"    [FR debug] page {page}/{total_pages}: {len(results2)} raw results")
    except Exception as e:
        print(f"    ✗ {e}")
        return []

    if verbose:
        print(f"    [FR debug] total raw: {len(all_raw)}")

    items = []
    dropped_non_health_agency = 0
    dropped_va_keyword = 0
    dropped_health_kw = 0
    fr_agency_slugs = set(FR_AGENCIES)

    for d in all_raw:
        tl = (d.get("title") or "").lower()
        al = (d.get("abstract") or "").lower()
        ag = [a.get("slug", "") for a in (d.get("agencies") or [])]

        # Hard-skip explicitly non-health agencies (shouldn't appear but guard anyway)
        if any(na in ag for na in NON_HEALTH_AGENCIES):
            dropped_non_health_agency += 1
            continue

        # For VA documents, require at least one health keyword
        if "veterans-affairs-department" in ag:
            if not any(k in tl or k in al for k in VA_HEALTH_KW):
                dropped_va_keyword += 1
                continue

        # For documents from our explicitly listed health agencies (CMS, FDA, DEA, etc.)
        # skip the broad HEALTH_KW filter — we already know they're health agencies.
        # Only apply the keyword filter to documents from OTHER agencies that slipped
        # through the agency_slugs filter (e.g. multi-agency notices).
        is_explicit_health_agency = any(slug in fr_agency_slugs for slug in ag)
        if not is_explicit_health_agency:
            if not any(k in tl or k in al for k in HEALTH_KW):
                dropped_health_kw += 1
                continue

        agency_name = (d.get("agencies") or [{}])[0].get("name", "HHS")
        items.append(make_item(
            d.get("title", ""),
            d.get("html_url", ""),
            d.get("publication_date", ""),
            f"Federal Register — {agency_name}",
            "https://www.federalregister.gov",
            strip_html(d.get("abstract") or ""),
            d.get("type", ""),
            d.get("comments_close_on", ""),
            "regulatory"
        ))

    if verbose:
        print(f"    [FR debug] dropped: {dropped_non_health_agency} non-health agency, "
              f"{dropped_va_keyword} VA keyword miss, {dropped_health_kw} non-agency health kw miss")
    print(f"    ✓ {len(items)} documents (from {len(all_raw)} raw)")
    return items

def fetch_congress_bills():
    print("  Congress.gov — Bills (recent + key committees)...")
    items = []

    # Pull recent bills — no date filter, just take top 100 and keyword filter
    try:
        r = get_url(
            f"https://api.congress.gov/v3/bill?format=json&limit=100"
            f"&sort=updateDate+desc&api_key={CONGRESS_API_KEY}",
            timeout=15
        )
        r.raise_for_status()
        for b in r.json().get("bills", []):
            title = b.get("title","")
            if not any(k in title.lower() for k in CONGRESS_KW):
                continue
            cong = b.get("congress","")
            # Skip bills from before the 119th Congress (2025–present)
            try:
                if cong and int(str(cong)) < 119:
                    continue
            except (ValueError, TypeError):
                pass
            sponsors = b.get("sponsors") or []
            sponsor  = sponsors[0].get("fullName","N/A") if sponsors else "N/A"
            action   = (b.get("latestAction") or {}).get("text","")
            btype    = (b.get("type") or "").lower()
            num      = b.get("number","")
            pub_str  = (b.get("updateDate") or b.get("introducedDate") or "")[:10]
            items.append(make_item(
                title,
                f"https://www.congress.gov/bill/{cong}th-congress/{btype}-bill/{num}",
                pub_str,
                f"Congress.gov — {b.get('type','')}{num} ({cong}th)",
                "https://www.congress.gov",
                f"Sponsor: {sponsor}. {action}",
                "Bill", "", "policy"
            ))
    except Exception as e:
        print(f"    ✗ bills: {e}")

    # Also pull from key committee bill lists
    for chamber, committees in HEALTH_COMMITTEES.items():
        for code, name in committees.items():
            try:
                r = get_url(
                    f"https://api.congress.gov/v3/committee/{chamber}/{code}/bills"
                    f"?format=json&limit=20&api_key={CONGRESS_API_KEY}",
                    timeout=12
                )
                if r.status_code != 200:
                    continue
                for b in (r.json().get("bills") or [])[:10]:
                    title = b.get("title","")
                    btype = (b.get("type") or "").lower()
                    num   = b.get("number","")
                    cong  = b.get("congress","119")
                    url   = f"https://www.congress.gov/bill/{cong}th-congress/{btype}-bill/{num}"
                    # Skip if already captured
                    if any(i["url"] == url for i in items):
                        continue
                    items.append(make_item(
                        f"[{name}] {title}",
                        url,
                        "",   # committee bill list API doesn't expose a reliable date
                        f"Congress.gov — {name}",
                        "https://www.congress.gov",
                        f"Bill referred to or active in {name}.",
                        "Committee Bill", "", "policy"
                    ))
            except Exception:
                continue

    print(f"    ✓ {len(items)} health bills and committee items")
    return items

def fetch_bill_summaries():
    """Fetch recent bill summaries — richer text for health matching."""
    print("  Congress.gov — Bill Summaries...")
    try:
        r = get_url(
            f"https://api.congress.gov/v3/summaries?format=json&limit=50"
            f"&sort=updateDate+desc&api_key={CONGRESS_API_KEY}",
            timeout=15
        )
        r.raise_for_status()
        cutoff_dt = cutoff()
        items = []
        for s in r.json().get("summaries", []):
            bill    = s.get("bill") or {}
            title   = bill.get("title","")
            summary = strip_html(s.get("text",""))[:500]
            combined = title + " " + summary
            if not any(k in combined.lower() for k in CONGRESS_KW):
                continue
            cong    = bill.get("congress","")
            # Skip summaries from before the 119th Congress
            try:
                if cong and int(str(cong)) < 119:
                    continue
            except (ValueError, TypeError):
                pass
            pub_str = (s.get("updateDate") or s.get("actionDate") or "")[:10]
            btype   = (bill.get("type") or "").lower()
            num     = bill.get("number","")
            items.append(make_item(
                title,
                f"https://www.congress.gov/bill/{cong}th-congress/{btype}-bill/{num}",
                pub_str,
                f"Congress.gov — Summary {bill.get('type','')}{num}",
                "https://www.congress.gov",
                summary,
                "Bill Summary", "", "policy"
            ))
        print(f"    ✓ {len(items)} bill summaries")
        return items
    except Exception as e:
        print(f"    ✗ {e}")
        return []

def fetch_crs_reports():
    print("  Congress.gov — CRS Reports...")
    try:
        r = get_url(
            f"https://api.congress.gov/v3/crsreport?format=json&limit=20"
            f"&api_key={CONGRESS_API_KEY}",
            timeout=15
        )
        r.raise_for_status()
        data    = r.json()
        reports = (data.get("CRSReports") or data.get("reports") or
                   (data if isinstance(data, list) else []))
        cutoff_dt = cutoff()
        items = []
        for rpt in reports:
            title = rpt.get("title","")
            if not any(k in title.lower() for k in CONGRESS_KW):
                continue
            pub_raw = (rpt.get("updateDate") or rpt.get("publishDate") or "")[:10]
            try:
                if pub_raw and datetime.fromisoformat(pub_raw).replace(tzinfo=timezone.utc) < cutoff_dt:
                    continue
            except Exception:
                pass
            items.append(make_item(
                title,
                f"https://www.congress.gov/crsreport/{rpt.get('citation','')}",
                pub_raw,
                "Congressional Research Service",
                "https://www.congress.gov",
                rpt.get("summary",""),
                "CRS Report", "", "policy"
            ))
        print(f"    ✓ {len(items)} CRS health reports")
        return items
    except Exception as e:
        print(f"    ✗ {e}")
        return []

def fetch_fda_api():
    print("  openFDA — 510(k) clearances + drug recalls...")
    cutoff_dt = cutoff()
    date_str  = cutoff_dt.strftime("%Y%m%d")
    items = []
    try:
        r = get_url(
            f"https://api.fda.gov/device/510k.json"
            f"?limit=20&search=decision_date:[{date_str}+TO+99991231]"
            f"&sort=decision_date:desc",
            timeout=15
        )
        if r.status_code == 200:
            for d in r.json().get("results",[]):
                items.append(make_item(
                    f"FDA 510(k): {d.get('device_name','')} — {d.get('applicant','')}",
                    f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID={d.get('k_number','')}",
                    (d.get("decision_date") or "")[:10],
                    "FDA — 510(k) Clearances", "https://www.fda.gov",
                    f"{d.get('device_name','')} — {d.get('decision_description','')}. Code: {d.get('product_code','')}.",
                    "510(k)", "", "regulatory"
                ))
    except Exception as e:
        print(f"    ✗ 510k: {e}")
    try:
        r = get_url(
            f"https://api.fda.gov/drug/enforcement.json"
            f"?limit=10&search=report_date:[{date_str}+TO+99991231]"
            f"&sort=report_date:desc",
            timeout=15
        )
        if r.status_code == 200:
            for d in r.json().get("results",[]):
                # Each recall needs a unique URL — the generic recalls page collapses all
                # of them to one entry during dedup. Use the recall number as the key.
                recall_num = (d.get("recall_number") or "").replace("/", "-").replace(" ", "-")
                recall_url = (
                    f"https://www.accessdata.fda.gov/scripts/ires/?Product={recall_num}"
                    if recall_num
                    else "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"
                )
                items.append(make_item(
                    f"FDA Drug Recall: {d.get('product_description','')[:80]} — {d.get('recalling_firm','')}",
                    recall_url,
                    (d.get("report_date") or "")[:10],
                    "FDA — Drug Recalls", "https://www.fda.gov",
                    f"Reason: {d.get('reason_for_recall','')[:300]}. Class: {d.get('classification','')}.",
                    "Recall", "", "regulatory"
                ))
    except Exception as e:
        print(f"    ✗ recalls: {e}")
    print(f"    ✓ {len(items)} FDA items")
    return items

def fetch_oig_enforcement():
    print("  OIG — Enforcement Actions...")
    try:
        r = get_url("https://oig.hhs.gov/fraud/enforcement/", timeout=15)
        if not r or r.status_code != 200:
            print(f"    ✗ HTTP {getattr(r, 'status_code', 'no response')}")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        entries = []
        for a in soup.select('a[href*="/fraud/enforcement/"]'):
            title = a.get_text(strip=True)
            href = a.get("href", "")
            if not title or len(title) < 10 or not href:
                continue
            url = href if href.startswith("http") else "https://oig.hhs.gov" + href
            date_str = _find_date_near_element(a)
            entries.append((title, url, date_str))

        seen, items = set(), []
        for title, url, date_str in entries:
            if url in seen:
                continue
            seen.add(url)
            items.append(make_item(
                title, url, date_str,
                "HHS OIG — Enforcement", "https://oig.hhs.gov/fraud/enforcement/",
                f"OIG enforcement action: {title}", "Enforcement Action", "", "regulatory"
            ))
            if len(items) >= MAX_ITEMS:
                break
        dated = sum(1 for _, _, d in entries if d)
        unknown = len(entries) - dated
        print(f"    ✓ {len(items)} enforcement actions ({dated} dated, {unknown} date-unknown)")
        return items
    except Exception as e:
        print(f"    ✗ {e}")
        return []

def fetch_oig_reports():
    print("  OIG — Reports...")
    try:
        r = get_url("https://oig.hhs.gov/newsroom/whats-new/", timeout=15)
        if not r or r.status_code != 200:
            print(f"    ✗ HTTP {getattr(r, 'status_code', 'no response')}")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        entries = []
        for a in soup.select('a[href*="/reports/"], a[href*="/newsroom/"]'):
            title = a.get_text(strip=True)
            href = a.get("href", "")
            if not title or len(title) < 10 or not href:
                continue
            url = href if href.startswith("http") else "https://oig.hhs.gov" + href
            date_str = _find_date_near_element(a)
            entries.append((title, url, date_str))

        seen, items = set(), []
        for title, url, date_str in entries:
            if url in seen:
                continue
            seen.add(url)
            items.append(make_item(
                title, url, date_str,
                "HHS OIG — Reports", "https://oig.hhs.gov",
                f"OIG: {title}", "OIG Report", "", "regulatory"
            ))
            if len(items) >= MAX_ITEMS:
                break
        dated = sum(1 for _, _, d in entries if d)
        unknown = len(entries) - dated
        print(f"    ✓ {len(items)} OIG reports ({dated} dated, {unknown} date-unknown)")
        return items
    except Exception as e:
        print(f"    ✗ {e}")
        return []

def fetch_cms_newsroom():
    print("  CMS Newsroom...")
    # Try multiple CMS RSS URLs
    for url in [
        "https://www.cms.gov/newsroom/rss.xml",
        "https://www.cms.gov/about-cms/contact/newsroom/rss",
        "https://www.cms.gov/Outreach-and-Education/Outreach/CMSFeeds/CMSFeeds-items/CMSNewsroom.xml",
    ]:
        try:
            r = get_url(url, timeout=10)
            if r.status_code != 200: continue
            feed = feedparser.parse(r.content)
            if not feed.entries: continue
            cutoff_dt = cutoff()
            items = []
            for entry in list(feed.entries)[:MAX_ITEMS]:
                pub = None
                for attr in ("published_parsed","updated_parsed"):
                    val = getattr(entry, attr, None)
                    if val:
                        try: pub=datetime(*val[:6],tzinfo=timezone.utc); break
                        except: pass
                if pub is not None and pub < cutoff_dt: continue
                pub_date_str = pub.strftime("%Y-%m-%d") if pub is not None else ""
                title = getattr(entry,"title","").strip()
                link  = getattr(entry,"link","").strip()
                if not title or not link: continue
                items.append(make_item(
                    title, link, pub_date_str,
                    "CMS Newsroom", "https://www.cms.gov",
                    strip_html(getattr(entry,"summary","")),
                    "CMS News","","regulatory"
                ))
            if items:
                print(f"    ✓ {len(items)} CMS items")
                return items
        except Exception:
            continue
    # Fallback: scrape CMS press releases page
    try:
        r = get_url("https://www.cms.gov/newsroom", timeout=15)
        if r and r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            entries = []
            for a in soup.select('a[href*="/newsroom/press-releases/"]'):
                title = a.get_text(strip=True)
                href = a.get("href", "")
                if not title or len(title) < 15 or not href:
                    continue
                url = href if href.startswith("http") else "https://www.cms.gov" + href
                entries.append((title, url))

            seen, items = set(), []
            for title, url in entries:
                if url in seen:
                    continue
                seen.add(url)
                # CMS press-release URLs embed /YYYY/MM/ — extract the real date.
                date_str = _date_from_url(url)
                items.append(make_item(
                    title, url, date_str,
                    "CMS Newsroom", "https://www.cms.gov",
                    f"CMS press release: {title}",
                    "Press Release", "", "regulatory"
                ))
                if len(items) >= MAX_ITEMS:
                    break
            if items:
                print(f"    ✓ {len(items)} CMS items (scraped)")
                return items
    except Exception:
        pass
    print(f"    ✗ No CMS content retrieved")
    return []

def fetch_court_cases():
    print("  CourtListener — Federal Healthcare Opinions...")
    cutoff_dt = cutoff()
    date_str  = cutoff_dt.strftime("%Y-%m-%d")
    terms = [
        "medicare", "medicaid", "false claims act",
        "anti-kickback", "hipaa", "affordable care act",
        "prior authorization", "340b drug",
    ]
    seen, items = set(), []
    for term in terms:
        try:
            headers = {"Authorization": f"Token {COURTLISTENER_TOKEN}"} if COURTLISTENER_TOKEN else {}
            r = get_url(
                "https://www.courtlistener.com/api/rest/v4/search/",
                params={
                    "q": term, "filed_after": date_str,
                    "order_by": "dateFiled desc", "type": "o", "format": "json",
                },
                headers=headers,
                timeout=12
            )
            if r.status_code != 200: continue
            data = r.json()
            results = data.get("results") or []
            for op in results:
                # Handle both nested cluster and flat response formats
                cluster    = op.get("cluster") or {}
                case_name  = cluster.get("case_name") or op.get("case_name","")
                date_filed = (cluster.get("date_filed") or op.get("date_filed",""))[:10]
                court      = (cluster.get("court") or op.get("court_id","")).upper()
                abs_url    = op.get("absolute_url","")
                full_url   = f"https://www.courtlistener.com{abs_url}" if abs_url.startswith("/") else abs_url
                if not case_name or full_url in seen: continue
                seen.add(full_url)
                snippet = strip_html(op.get("plain_text","") or "")[:400] or f"Federal court opinion — {term}"
                items.append(make_item(
                    f"{case_name} ({court or 'Federal'})",
                    full_url or "https://www.courtlistener.com",
                    date_filed,
                    "CourtListener — Federal Courts",
                    "https://www.courtlistener.com",
                    snippet, "Court Opinion","","regulatory"
                ))
            time.sleep(0.4)
        except Exception:
            continue
    print(f"    ✓ {len(items)} court opinions")
    return items

# ── ALL SOURCES ───────────────────────────────────────────────────────────────

def fetch_all():
    all_items = []

    # ── Government APIs (parallel, max 4 concurrent) ──────────────────────────
    print("\n── Government APIs ──────────────────────────────────────")
    api_fetchers = [
        fetch_federal_register, fetch_congress_bills, fetch_bill_summaries,
        fetch_crs_reports, fetch_fda_api, fetch_oig_enforcement,
        fetch_oig_reports, fetch_cms_newsroom, fetch_court_cases,
    ]
    with ThreadPoolExecutor(max_workers=4) as exe:
        futures = [exe.submit(fn) for fn in api_fetchers]
        for f in as_completed(futures):
            try:
                all_items += f.result()
            except Exception:
                pass

    # ── RSS Feeds (parallel, max 12 concurrent) ───────────────────────────────
    print("\n── RSS Feeds ────────────────────────────────────────────")
    rss_tasks = [
        # (name, url_or_list, source_type, health_filter, date_filter)
        ("FDA Press Releases", "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/press-releases/rss.xml", "regulatory", False, True),
        ("FTC Health",         "https://www.ftc.gov/news-events/news/press-releases/rss",          "regulatory", True,  True),
        ("DOJ Press Releases", "https://www.justice.gov/news/rss",                                  "regulatory", True,  True),
        ("GAO Health Reports", "https://www.gao.gov/rss/reports.xml",                               "policy",     True,  True),
        ("CBO Publications",   "https://www.cbo.gov/rss/publications.xml",                          "policy",     True,  True),
        ("STAT News",          "https://www.statnews.com/feed/",                                     "policy",     False, True),
        ("Healthcare Dive",    "https://www.healthcaredive.com/feeds/news/",                         "both",       False, True),
        ("Becker's Hospital",  "https://www.beckershospitalreview.com/feed/",                        "both",       False, True),
        ("Becker's Payer",     "https://www.beckerspayer.com/feed/",                                 "regulatory", False, True),
        ("Fierce Healthcare",  "https://www.fiercehealthcare.com/rss/xml",                           "both",       False, True),
        ("Fierce Pharma",      "https://www.fiercepharma.com/rss/xml",                               "regulatory", False, True),
        ("Fierce Biotech",     "https://www.fiercebiotech.com/rss/xml",                              "regulatory", False, True),
        ("KFF Health News",    ["https://kffhealthnews.org/feed/", "http://kffhealthnews.org/feed/", "https://kffhealthnews.org/feed/rss/"], "policy", False, False),
        ("KFF — Medicare",     "https://www.kff.org/topic/medicare/feed/",                          "policy",     False, False),
        ("KFF — Medicaid",     "https://www.kff.org/topic/medicaid/feed/",                          "policy",     False, False),
        ("KFF — Health Costs", "https://www.kff.org/topic/health-costs/feed/",                      "policy",     False, False),
        ("KFF — ACA",          "https://www.kff.org/topic/affordable-care-act/feed/",               "policy",     False, False),
        ("Health Affairs",     ["https://www.healthaffairs.org/action/showFeed?type=etoc&feed=rss&jc=hlthaff", "https://www.healthaffairs.org/rss/", "https://www.healthaffairs.org/news/rss"], "policy", False, False),
        ("Commonwealth Fund",  ["https://www.commonwealthfund.org/rss.xml", "https://www.commonwealthfund.org/publications/rss", "https://www.commonwealthfund.org/feed"], "policy", False, False),
        ("Brookings Health",   ["https://www.brookings.edu/topics/health-care/feed/", "https://www.brookings.edu/topic/health-care/feed/"], "policy", False, False),
        ("NASHP",              "https://nashp.org/feed/",                                            "policy",     False, False),
        ("MedPAC",             "https://www.medpac.gov/blog/feed/",                                  "policy",     False, False),
        ("RAND Health",        "https://www.rand.org/content/rand/topics/health-care/jcr:content.feed", "policy", False, False),
        ("NIH News",           "https://www.nih.gov/news-releases/feed.xml",                         "both",       False, True),
        ("JD Supra Health",    "https://www.jdsupra.com/rss/Health_15.rss",                          "regulatory", False, False),
        ("Healthcare Finance", "https://www.healthcarefinancenews.com/rss.xml",                      "both",       False, True),
        ("MedCity News",       "https://medcitynews.com/feed/",                                      "both",       False, True),
    ]

    def _rss(task):
        name, url, src, hf, df = task
        return fetch_rss(name, url, src, health_filter=hf, date_filter=df)

    with ThreadPoolExecutor(max_workers=12) as exe:
        futures = [exe.submit(_rss, t) for t in rss_tasks]
        for f in as_completed(futures):
            try:
                all_items += f.result()
            except Exception:
                pass

    return all_items

# ── MAIN ──────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Fetch Healthcare Regulatory & Policy Monitor raw article data")
    parser.add_argument("-o", "--output", default="raw_articles.json", help="output JSON path")
    parser.add_argument("--days-back", type=int, default=DAYS_BACK, help="lookback window in days")
    parser.add_argument("--max-items", type=int, default=MAX_ITEMS, help="max items per feed")
    parser.add_argument("--test", action="store_true", help="run source connectivity diagnostics")
    return parser.parse_args()


def main():
    args = parse_args()
    global DAYS_BACK, MAX_ITEMS
    DAYS_BACK = args.days_back
    MAX_ITEMS = args.max_items

    print("=" * 60)
    print("  Healthcare Regulatory & Policy Monitor — Step 1: Scraper")
    print(f"  30+ sources, max {MAX_ITEMS} items per RSS feed")
    print(f"  Lookback: {DAYS_BACK} days")
    print("=" * 60)

    # Capture scrape timestamp before fetching so every item gets the same value.
    scraped_at = datetime.now(timezone.utc).isoformat()

    all_items = fetch_all()

    # Deduplicate by normalized URL
    seen, deduped = set(), []
    for a in all_items:
        url = normalize_url(a.get("url", ""))
        if url and url not in seen:
            seen.add(url)
            a["url"] = url
            deduped.append(a)

    # Inject discovery_date: when this pipeline run first saw each item.
    for a in deduped:
        a["discovery_date"] = scraped_at

    source_counts = {}
    date_unknown_by_source = {}
    for a in deduped:
        s = a["source_name"].split("—")[0].strip()
        source_counts[s] = source_counts.get(s, 0) + 1
        if a.get("date_unknown"):
            date_unknown_by_source[s] = date_unknown_by_source.get(s, 0) + 1

    # Warn if an entire source returns zero parseable dates — signals a page-layout change.
    zero_date_sources = [
        s for s, n in source_counts.items()
        if date_unknown_by_source.get(s, 0) == n and n > 0
    ]

    print(f"\n{'='*60}")
    print(f"  Total fetched: {len(all_items)}")
    print(f"  After dedup:   {len(deduped)}")
    print(f"  Regulatory:    {len([a for a in deduped if a['source_type'] in ('regulatory','both')])}")
    print(f"  Policy:        {len([a for a in deduped if a['source_type'] in ('policy','both')])}")
    total_unknown = sum(1 for a in deduped if a.get("date_unknown"))
    print(f"  Date-unknown:  {total_unknown} ({100*total_unknown//max(len(deduped),1)}%)")
    print("\n  By source:")
    for src, n in sorted(source_counts.items(), key=lambda x: -x[1]):
        if n > 0:
            unk = date_unknown_by_source.get(src, 0)
            flag = " ⚠ ALL dates unknown" if src in zero_date_sources else (f" ({unk} date-unknown)" if unk else "")
            print(f"    {src:<42} {n}{flag}")

    if zero_date_sources:
        print(f"\n  ⚠ WARNING: {len(zero_date_sources)} source(s) returned zero parseable dates:")
        for s in zero_date_sources:
            print(f"    • {s} — page layout may have changed, check selector")

    output = {
        "scraped_at": scraped_at,
        "days_back":  DAYS_BACK,
        "total":      len(deduped),
        "articles":   deduped,
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n  ✓ Saved {len(deduped)} articles to {args.output}")
    print(f"  Next: python3 generate.py -i {args.output}")
    print("=" * 60)

def test_sources():
    """Diagnostic mode: ping every source, print a pass/fail table."""
    print("=" * 64)
    print("  Healthcare Regulatory & Policy Monitor — Source Diagnostic")
    print("=" * 64)

    # (label, type, url-or-None-for-api)
    rss_checks = [
        ("FDA Press",          "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/press-releases/rss.xml"),
        ("FTC Health",         "https://www.ftc.gov/news-events/news/press-releases/rss"),
        ("DOJ Press",          "https://www.justice.gov/news/rss"),
        ("GAO Reports",        "https://www.gao.gov/rss/reports.xml"),
        ("CBO Publications",   "https://www.cbo.gov/rss/publications.xml"),
        ("SCOTUS Opinions",    "https://www.supremecourt.gov/rss/slipopinions.aspx"),
        ("STAT News",          "https://www.statnews.com/feed/"),
        ("Healthcare Dive",    "https://www.healthcaredive.com/feeds/news/"),
        ("Becker's Hospital",  "https://www.beckershospitalreview.com/feed/"),
        ("Becker's Payer",     "https://www.beckerspayer.com/feed/"),
        ("Fierce Healthcare",  "https://www.fiercehealthcare.com/rss/xml"),
        ("Fierce Pharma",      "https://www.fiercepharma.com/rss/xml"),
        ("Fierce Biotech",     "https://www.fiercebiotech.com/rss/xml"),
        ("KFF Health News",    "https://kffhealthnews.org/feed/"),
        ("KFF Medicare",       "https://www.kff.org/topic/medicare/feed/"),
        ("KFF Medicaid",       "https://www.kff.org/topic/medicaid/feed/"),
        ("Health Affairs",     "https://www.healthaffairs.org/action/showFeed?type=etoc&feed=rss&jc=hlthaff"),
        ("Commonwealth Fund",  "https://www.commonwealthfund.org/rss.xml"),
        ("Brookings Health",   "https://www.brookings.edu/topics/health-care/feed/"),
        ("NASHP",              "https://nashp.org/feed/"),
        ("MedPAC",             "https://www.medpac.gov/blog/feed/"),
        ("RAND Health",        "https://www.rand.org/topics/health-care/rss.xml"),
        ("NIH News",           "https://www.nih.gov/news-events/news-releases/feed"),
        ("JD Supra Health",    "https://www.jdsupra.com/topics/health-care-law/rss/"),
        ("Politico Pulse",     "https://www.politico.com/rss/politicopulse.xml"),
    ]

    print(f"\n  {'SOURCE':<24} {'STATUS':<14} ENTRIES")
    print("  " + "-" * 50)

    ok = warn = fail = 0
    for label, url in rss_checks:
        try:
            r = get_url(url, timeout=12)
            if r.status_code != 200:
                print(f"  {label:<24} HTTP {r.status_code:<9} —")
                fail += 1
                continue
            feed = feedparser.parse(r.content)
            n = len(feed.entries)
            if n > 0:
                print(f"  {label:<24} {'✓ OK':<14} {n}")
                ok += 1
            else:
                print(f"  {label:<24} {'⚠ EMPTY':<14} 0  (likely Cloudflare/JS)")
                warn += 1
        except Exception as e:
            print(f"  {label:<24} {'✗ ERROR':<14} {str(e)[:24]}")
            fail += 1

    # Federal Register filter diagnostic
    print("\n  Federal Register filter diagnostic:")
    try:
        from datetime import datetime as _dt, timedelta as _td, timezone as _tz
        date_str = (_dt.now(_tz.utc) - _td(days=DAYS_BACK)).strftime("%Y-%m-%d")
        agency_qs = "&".join(f"agency_slugs[]={a}" for a in FR_AGENCIES)
        results, total_pages = _fetch_fr_page(agency_qs, date_str, page=1)
        print(f"  {'FR page 1':<24} {len(results)} raw results  ({total_pages} total page(s) available)")
        if total_pages > 1:
            print(f"  {'':24} ⚠ {total_pages - 1} additional page(s) exist — run scrape.py normally to fetch them")
        fr_agency_slugs = set(FR_AGENCIES)
        dropped_non = dropped_va = dropped_kw = 0
        passed = 0
        for d in results:
            ag = [a.get("slug", "") for a in (d.get("agencies") or [])]
            tl = (d.get("title") or "").lower()
            al = (d.get("abstract") or "").lower()
            if any(na in ag for na in NON_HEALTH_AGENCIES):
                dropped_non += 1; continue
            if "veterans-affairs-department" in ag:
                if not any(k in tl or k in al for k in VA_HEALTH_KW):
                    dropped_va += 1; continue
            is_explicit = any(slug in fr_agency_slugs for slug in ag)
            if not is_explicit and not any(k in tl or k in al for k in HEALTH_KW):
                dropped_kw += 1; continue
            passed += 1
        print(f"  {'FR post-filter':<24} {passed} items pass")
        if dropped_non or dropped_va or dropped_kw:
            print(f"  {'FR dropped':<24} {dropped_non} non-health agency  "
                  f"{dropped_va} VA keyword miss  {dropped_kw} non-agency kw miss")
        ok += 1
    except Exception as e:
        print(f"  {'Federal Register':<24} ✗ {str(e)[:50]}")
        fail += 1

    # API checks
    print("\n  API sources:")
    api_checks = [
        ("Federal Register", "https://www.federalregister.gov/api/v1/documents.json?per_page=1"),
        ("Congress.gov",     f"https://api.congress.gov/v3/bill?format=json&limit=1&api_key={CONGRESS_API_KEY}"),
        ("openFDA 510k",     "https://api.fda.gov/device/510k.json?limit=1"),
        ("CourtListener",    "https://www.courtlistener.com/api/rest/v4/opinions/?q=medicare&page_size=1&format=json"),
    ]
    for label, url in api_checks:
        try:
            r = get_url(url, timeout=12)
            mark = "✓ OK" if r.status_code == 200 else f"HTTP {r.status_code}"
            print(f"  {label:<24} {mark}")
            if r.status_code == 200: ok += 1
            else: fail += 1
        except Exception as e:
            print(f"  {label:<24} ✗ {str(e)[:24]}")
            fail += 1

    # Scraper-based (HTML) checks — also verify date extraction works
    print("\n  Scraped pages (connectivity + date extraction):")
    scrape_checks = [
        ("OIG Enforcement", "https://oig.hhs.gov/fraud/enforcement/",   'a[href*="/fraud/enforcement/"]'),
        ("OIG What's New",  "https://oig.hhs.gov/newsroom/whats-new/",  'a[href*="/reports/"], a[href*="/newsroom/"]'),
        ("CMS Press",       "https://www.cms.gov/newsroom/press-releases", 'a[href*="/newsroom/press-releases/"]'),
    ]
    for label, url, selector in scrape_checks:
        try:
            r = get_url(url, timeout=12)
            if r.status_code != 200:
                print(f"  {label:<24} HTTP {r.status_code:<9} —")
                fail += 1
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            links = [a for a in soup.select(selector) if len(a.get_text(strip=True)) >= 10]
            sample = links[:10]
            dated = sum(1 for a in sample if _find_date_near_element(a))
            if dated == 0 and sample:
                mark = f"✓ OK ({len(links)} links)  ⚠ 0/{len(sample)} dates parseable — selector may need update"
                warn += 1
            elif sample:
                mark = f"✓ OK ({len(links)} links, {dated}/{len(sample)} dated in sample)"
                ok += 1
            else:
                mark = f"⚠ OK but 0 links matched selector"
                warn += 1
            print(f"  {label:<24} {mark}")
        except Exception as e:
            print(f"  {label:<24} ✗ {str(e)[:40]}")
            fail += 1

    print("\n  " + "-" * 50)
    print(f"  {ok} working   {warn} empty/warn   {fail} failing")
    print("=" * 64)


if __name__ == "__main__":
    if "--test" in sys.argv:
        test_sources()
    else:
        main()
