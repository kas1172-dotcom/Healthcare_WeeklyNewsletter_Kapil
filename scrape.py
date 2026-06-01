#!/usr/bin/env python3
"""
HealthPulse Intelligence — Step 1: Scraper
===========================================
Fetches all sources and saves raw_articles.json.
No Anthropic API key needed. Run this first.

SETUP:   pip3 install requests feedparser
RUN:     python3 scrape.py
OUTPUT:  raw_articles.json
"""

import os, json, re, sys, time
import requests
import feedparser
from datetime import datetime, timedelta, timezone

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
    pub_str = (published or "")[:10] or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return {
        "title":            title,
        "url":              url,
        "published":        pub_str,
        "published_iso":    pub_str,
        "source_name":      source_name,
        "source_url":       source_url,
        "snippet":          snippet[:600],
        "doc_type":         doc_type,
        "comment_deadline": comment_deadline,
        "source_type":      source_type,
    }

def try_rss_urls(name, urls, source_type, health_filter=False):
    """Try multiple RSS URL variants, return first that works."""
    for url in urls:
        try:
            r = SESSION.get(url, timeout=12)
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
                pub = pub or datetime.now(timezone.utc)
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
                    title, link, pub.strftime("%Y-%m-%d"),
                    feed_title, feed_link, snippet, "", "", source_type
                ))
            if items:
                return items, url
        except Exception:
            continue
    return [], None

def fetch_rss(name, urls_or_url, source_type, health_filter=False, date_filter=True):
    """Fetch RSS. urls_or_url can be a string or list of fallback URLs."""
    print(f"  {name}...")
    urls = [urls_or_url] if isinstance(urls_or_url, str) else urls_or_url
    cutoff_dt = cutoff()

    for url in urls:
        try:
            r = SESSION.get(url, timeout=15)
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
                pub = pub or datetime.now(timezone.utc)
                if date_filter and pub < cutoff_dt:
                    continue
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
                    title, link, pub.strftime("%Y-%m-%d"),
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

def fetch_federal_register():
    print("  Federal Register API...")
    cutoff_dt = cutoff()
    date_str  = cutoff_dt.strftime("%Y-%m-%d")
    agency_qs = "&".join(f"agency_slugs[]={a}" for a in FR_AGENCIES)
    url = (
        f"https://www.federalregister.gov/api/v1/documents.json"
        f"?per_page=100&order=newest&{agency_qs}"
        f"&conditions[publication_date][gte]={date_str}"
        f"&conditions[type][]=RULE&conditions[type][]=PRORULE&conditions[type][]=NOTICE"
        f"&fields[]=title&fields[]=abstract&fields[]=publication_date"
        f"&fields[]=html_url&fields[]=agencies&fields[]=type&fields[]=comments_close_on"
    )
    try:
        r = SESSION.get(url, timeout=20)
        r.raise_for_status()
        items = []
        for d in r.json().get("results", []):
            tl = (d.get("title") or "").lower()
            al = (d.get("abstract") or "").lower()
            ag = [a.get("slug","") for a in (d.get("agencies") or [])]
            if any(na in ag for na in NON_HEALTH_AGENCIES):
                continue
            if "veterans-affairs-department" in ag:
                if not any(k in tl or k in al for k in VA_HEALTH_KW):
                    continue
            if not any(k in tl or k in al for k in HEALTH_KW):
                continue
            agency_name = (d.get("agencies") or [{}])[0].get("name","HHS")
            items.append(make_item(
                d.get("title",""),
                d.get("html_url",""),
                d.get("publication_date",""),
                f"Federal Register — {agency_name}",
                "https://www.federalregister.gov",
                strip_html(d.get("abstract") or ""),
                d.get("type",""),
                d.get("comments_close_on",""),
                "regulatory"
            ))
        print(f"    ✓ {len(items)} documents")
        return items
    except Exception as e:
        print(f"    ✗ {e}")
        return []

def fetch_congress_bills():
    print("  Congress.gov — Bills (recent + key committees)...")
    items = []

    # Pull recent bills — no date filter, just take top 100 and keyword filter
    try:
        r = SESSION.get(
            f"https://api.congress.gov/v3/bill?format=json&limit=100"
            f"&sort=updateDate+desc&api_key={CONGRESS_API_KEY}",
            timeout=15
        )
        r.raise_for_status()
        for b in r.json().get("bills", []):
            title = b.get("title","")
            if not any(k in title.lower() for k in CONGRESS_KW):
                continue
            sponsors = b.get("sponsors") or []
            sponsor  = sponsors[0].get("fullName","N/A") if sponsors else "N/A"
            action   = (b.get("latestAction") or {}).get("text","")
            btype    = (b.get("type") or "").lower()
            cong     = b.get("congress","")
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
                r = SESSION.get(
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
                        datetime.now(timezone.utc).strftime("%Y-%m-%d"),
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
        r = SESSION.get(
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
            pub_str = (s.get("updateDate") or s.get("actionDate") or "")[:10]
            btype   = (bill.get("type") or "").lower()
            cong    = bill.get("congress","")
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
        r = SESSION.get(
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
        r = SESSION.get(
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
        r = SESSION.get(
            f"https://api.fda.gov/drug/enforcement.json"
            f"?limit=10&search=report_date:[{date_str}+TO+99991231]"
            f"&sort=report_date:desc",
            timeout=15
        )
        if r.status_code == 200:
            for d in r.json().get("results",[]):
                items.append(make_item(
                    f"FDA Drug Recall: {d.get('product_description','')[:80]} — {d.get('recalling_firm','')}",
                    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts",
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
        r = SESSION.get("https://oig.hhs.gov/fraud/enforcement/", timeout=15)
        if r.status_code != 200:
            print(f"    ✗ HTTP {r.status_code}")
            return []
        from html.parser import HTMLParser
        class P(HTMLParser):
            def __init__(self):
                super().__init__(); self.items=[]; self.cur={}; self.cap=False
            def handle_starttag(self, tag, attrs):
                attrs=dict(attrs); href=attrs.get("href","")
                if tag=="a" and "/fraud/enforcement/" in href and len(href)>20:
                    self.cur={"url":"https://oig.hhs.gov"+href if href.startswith("/") else href}
                    self.cap=True
            def handle_data(self, data):
                if self.cap and data.strip(): self.cur["title"]=data.strip(); self.cap=False
            def handle_endtag(self, tag):
                if tag=="a" and self.cur.get("title") and self.cur.get("url"):
                    self.items.append(self.cur.copy()); self.cur={}
        p=P(); p.feed(r.text)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        seen, items = set(), []
        for x in p.items[:MAX_ITEMS]:
            if x["url"] in seen or len(x.get("title",""))<10: continue
            seen.add(x["url"])
            items.append(make_item(
                x["title"], x["url"], today,
                "HHS OIG — Enforcement", "https://oig.hhs.gov/fraud/enforcement/",
                f"OIG enforcement action: {x['title']}", "Enforcement Action","","regulatory"
            ))
        print(f"    ✓ {len(items)} enforcement actions")
        return items
    except Exception as e:
        print(f"    ✗ {e}")
        return []

def fetch_oig_reports():
    print("  OIG — Reports...")
    try:
        r = SESSION.get("https://oig.hhs.gov/newsroom/whats-new/", timeout=15)
        if r.status_code != 200:
            print(f"    ✗ HTTP {r.status_code}")
            return []
        from html.parser import HTMLParser
        class P(HTMLParser):
            def __init__(self):
                super().__init__(); self.items=[]; self.cur={}; self.cap=False
            def handle_starttag(self, tag, attrs):
                attrs=dict(attrs); href=attrs.get("href","")
                if tag=="a" and ("/reports/" in href or "/newsroom/" in href):
                    self.cur={"url":"https://oig.hhs.gov"+href if href.startswith("/") else href}
                    self.cap=True
            def handle_data(self, data):
                if self.cap and data.strip() and len(data.strip())>10:
                    self.cur["title"]=data.strip(); self.cap=False
            def handle_endtag(self, tag):
                if tag=="a" and self.cur.get("title"):
                    self.items.append(self.cur.copy()); self.cur={}
        p=P(); p.feed(r.text)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        seen, items = set(), []
        for x in p.items[:MAX_ITEMS]:
            if x["url"] in seen or len(x.get("title",""))<10: continue
            seen.add(x["url"])
            items.append(make_item(
                x["title"], x["url"], today,
                "HHS OIG — Reports", "https://oig.hhs.gov",
                f"OIG: {x['title']}", "OIG Report","","regulatory"
            ))
        print(f"    ✓ {len(items)} OIG reports")
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
            r = SESSION.get(url, timeout=10)
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
                pub = pub or datetime.now(timezone.utc)
                if pub < cutoff_dt: continue
                title = getattr(entry,"title","").strip()
                link  = getattr(entry,"link","").strip()
                if not title or not link: continue
                items.append(make_item(
                    title, link, pub.strftime("%Y-%m-%d"),
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
        r = SESSION.get("https://www.cms.gov/newsroom", timeout=15)
        if r.status_code == 200:
            from html.parser import HTMLParser
            class P(HTMLParser):
                def __init__(self):
                    super().__init__(); self.items=[]; self.cur={}; self.cap=False
                def handle_starttag(self, tag, attrs):
                    attrs=dict(attrs); href=attrs.get("href","")
                    if tag=="a" and "/newsroom/press-releases/" in href and len(href)>30:
                        self.cur={"url":"https://www.cms.gov"+href if href.startswith("/") else href}
                        self.cap=True
                def handle_data(self, data):
                    if self.cap and data.strip() and len(data.strip())>15:
                        self.cur["title"]=data.strip(); self.cap=False
                def handle_endtag(self, tag):
                    if tag=="a" and self.cur.get("title"):
                        self.items.append(self.cur.copy()); self.cur={}
            p=P(); p.feed(r.text)
            today=datetime.now(timezone.utc).strftime("%Y-%m-%d")
            seen, items = set(), []
            for x in p.items[:MAX_ITEMS]:
                if x["url"] in seen or len(x.get("title",""))<10: continue
                seen.add(x["url"])
                items.append(make_item(
                    x["title"], x["url"], today,
                    "CMS Newsroom", "https://www.cms.gov",
                    f"CMS press release: {x['title']}",
                    "Press Release","","regulatory"
                ))
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
            r = SESSION.get(
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

    print("\n── Government APIs ──────────────────────────────────────")
    all_items += fetch_federal_register()
    all_items += fetch_congress_bills()
    all_items += fetch_bill_summaries()
    all_items += fetch_crs_reports()
    all_items += fetch_fda_api()
    all_items += fetch_oig_enforcement()
    all_items += fetch_oig_reports()
    all_items += fetch_cms_newsroom()
    all_items += fetch_court_cases()

    print("\n── Government RSS ───────────────────────────────────────")
    all_items += fetch_rss("FDA Press Releases",
        "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/press-releases/rss.xml",
        "regulatory")
    all_items += fetch_rss("FTC Health",
        "https://www.ftc.gov/news-events/news/press-releases/rss",
        "regulatory", health_filter=True)
    all_items += fetch_rss("DOJ Press Releases",
        "https://www.justice.gov/news/rss",
        "regulatory", health_filter=True)
    all_items += fetch_rss("GAO Health Reports",
        "https://www.gao.gov/rss/reports.xml",
        "policy", health_filter=True)
    all_items += fetch_rss("CBO Publications",
        "https://www.cbo.gov/rss/publications.xml",
        "policy", health_filter=True)

    print("\n── Industry & Trade RSS ─────────────────────────────────")
    all_items += fetch_rss("STAT News",
        "https://www.statnews.com/feed/",
        "policy")
    all_items += fetch_rss("Healthcare Dive",
        "https://www.healthcaredive.com/feeds/news/",
        "both")
    all_items += fetch_rss("Becker's Hospital",
        "https://www.beckershospitalreview.com/feed/",
        "both")
    all_items += fetch_rss("Becker's Payer",
        "https://www.beckerspayer.com/feed/",
        "regulatory")
    all_items += fetch_rss("Fierce Healthcare",
        "https://www.fiercehealthcare.com/rss/xml",
        "both")
    all_items += fetch_rss("Fierce Pharma",
        "https://www.fiercepharma.com/rss/xml",
        "regulatory")
    all_items += fetch_rss("Fierce Biotech",
        "https://www.fiercebiotech.com/rss/xml",
        "regulatory")

    print("\n── Policy & Research RSS ────────────────────────────────")
    # KFF — multiple URL variants, no date filter
    all_items += fetch_rss("KFF Health News",
        ["https://kffhealthnews.org/feed/",
         "http://kffhealthnews.org/feed/",
         "https://kffhealthnews.org/feed/rss/",],
        "policy", date_filter=False)
    all_items += fetch_rss("KFF — Medicare",
        "https://www.kff.org/topic/medicare/feed/",
        "policy", date_filter=False)
    all_items += fetch_rss("KFF — Medicaid",
        "https://www.kff.org/topic/medicaid/feed/",
        "policy", date_filter=False)
    all_items += fetch_rss("KFF — Health Costs",
        "https://www.kff.org/topic/health-costs/feed/",
        "policy", date_filter=False)
    all_items += fetch_rss("KFF — ACA",
        "https://www.kff.org/topic/affordable-care-act/feed/",
        "policy", date_filter=False)

    # Health Affairs — multiple variants
    all_items += fetch_rss("Health Affairs",
        ["https://www.healthaffairs.org/action/showFeed?type=etoc&feed=rss&jc=hlthaff",
         "https://www.healthaffairs.org/rss/",
         "https://www.healthaffairs.org/news/rss"],
        "policy", date_filter=False)

    # Commonwealth Fund — multiple variants
    all_items += fetch_rss("Commonwealth Fund",
        ["https://www.commonwealthfund.org/rss.xml",
         "https://www.commonwealthfund.org/publications/rss",
         "https://www.commonwealthfund.org/feed"],
        "policy", date_filter=False)

    all_items += fetch_rss("Brookings Health",
        ["https://www.brookings.edu/topics/health-care/feed/",
         "https://www.brookings.edu/topic/health-care/feed/"],
        "policy", date_filter=False)
    all_items += fetch_rss("NASHP",
        "https://nashp.org/feed/",
        "policy", date_filter=False)
    all_items += fetch_rss("MedPAC",
        "https://www.medpac.gov/blog/feed/",
        "policy", date_filter=False)
    all_items += fetch_rss("RAND Health",
        "https://www.rand.org/content/rand/topics/health-care/jcr:content.feed",
        "policy", date_filter=False)
    all_items += fetch_rss("NIH News",
        "https://www.nih.gov/news-releases/feed.xml",
        "both")
    all_items += fetch_rss("JD Supra Health",
        "https://www.jdsupra.com/rss/Health_15.rss",
        "regulatory", date_filter=False)
    all_items += fetch_rss("Healthcare Finance",
        "https://www.healthcarefinancenews.com/rss.xml",
        "both")
    all_items += fetch_rss("MedCity News",
        "https://medcitynews.com/feed/",
        "both")

    return all_items

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  HealthPulse — Step 1: Scraper")
    print(f"  30+ sources, max {MAX_ITEMS} items per RSS feed")
    print("=" * 60)

    all_items = fetch_all()

    # Deduplicate by URL
    seen, deduped = set(), []
    for a in all_items:
        if a["url"] and a["url"] not in seen:
            seen.add(a["url"])
            deduped.append(a)

    source_counts = {}
    for a in deduped:
        s = a["source_name"].split("—")[0].strip()
        source_counts[s] = source_counts.get(s, 0) + 1

    print(f"\n{'='*60}")
    print(f"  Total fetched: {len(all_items)}")
    print(f"  After dedup:   {len(deduped)}")
    print(f"  Regulatory:    {len([a for a in deduped if a['source_type'] in ('regulatory','both')])}")
    print(f"  Policy:        {len([a for a in deduped if a['source_type'] in ('policy','both')])}")
    print("\n  By source:")
    for src, n in sorted(source_counts.items(), key=lambda x: -x[1]):
        if n > 0:
            print(f"    {src:<42} {n}")

    output = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "days_back":  DAYS_BACK,
        "total":      len(deduped),
        "articles":   deduped,
    }
    with open("raw_articles.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n  ✓ Saved {len(deduped)} articles to raw_articles.json")
    print(f"  Next: python3 generate.py")
    print("=" * 60)

def test_sources():
    """Diagnostic mode: ping every source, print a pass/fail table."""
    print("=" * 64)
    print("  HealthPulse — Source Diagnostic")
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
            r = SESSION.get(url, timeout=12)
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
            r = SESSION.get(url, timeout=12)
            mark = "✓ OK" if r.status_code == 200 else f"HTTP {r.status_code}"
            print(f"  {label:<24} {mark}")
            if r.status_code == 200: ok += 1
            else: fail += 1
        except Exception as e:
            print(f"  {label:<24} ✗ {str(e)[:24]}")
            fail += 1

    # Scraper-based (HTML) checks
    print("\n  Scraped pages:")
    for label, url in [
        ("OIG Enforcement", "https://oig.hhs.gov/fraud/enforcement/"),
        ("OIG What's New",  "https://oig.hhs.gov/newsroom/whats-new/"),
        ("CMS Press",       "https://www.cms.gov/newsroom/press-releases"),
    ]:
        try:
            r = SESSION.get(url, timeout=12)
            mark = "✓ OK" if r.status_code == 200 else f"HTTP {r.status_code}"
            print(f"  {label:<24} {mark}")
            if r.status_code == 200: ok += 1
            else: fail += 1
        except Exception as e:
            print(f"  {label:<24} ✗ {str(e)[:24]}")
            fail += 1

    print("\n  " + "-" * 50)
    print(f"  {ok} working   {warn} empty   {fail} failing")
    print("=" * 64)


if __name__ == "__main__":
    if "--test" in sys.argv:
        test_sources()
    else:
        main()
