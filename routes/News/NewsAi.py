from fastapi import APIRouter, BackgroundTasks
from eventregistry import *
from config import NEWS_AI_API
from pathlib import Path
from typing import List
from datetime import datetime, timedelta
import json

router = APIRouter(prefix="/news-ai", tags=["NEWS"])

er = EventRegistry(apiKey=NEWS_AI_API, allowUseOfArchive=False)

# ─── Cache Config ──────────────────────────────────────────────
CACHE_DIR   = Path("Cache")
CACHE_FILE  = CACHE_DIR / "home_news.json"
CACHE_TTL   = timedelta(hours=2)
CACHE_DIR.mkdir(exist_ok=True)

# ─── Keys to Retain ────────────────────────────────────────────
KEEP_KEYS = {
    "dateTime", "dateTimePub", "title", "body",
    "source", "image"
}

# ─── Cache Helpers ─────────────────────────────────────────────
def _cache_valid() -> bool:
    if not CACHE_FILE.exists():
        return False
    try:
        payload = json.loads(CACHE_FILE.read_text())
        ts = datetime.fromisoformat(payload["last_updated"])
        return datetime.utcnow() - ts < CACHE_TTL
    except Exception:
        return False

def _read_cache() -> list[dict]:
    return json.loads(CACHE_FILE.read_text())["data"]

def _write_cache(data: list[dict]) -> None:
    CACHE_FILE.write_text(json.dumps({
        "last_updated": datetime.utcnow().isoformat(),
        "data":         data
    }, indent=2))

# ─── Article Formatter ─────────────────────────────────────────
def _filter_article_fields(article: dict) -> dict:
    return {k: article.get(k) for k in KEEP_KEYS if k in article}

# ─── Live Article Fetcher ──────────────────────────────────────
async def fetch_articles(query_terms: List[str]) -> list[dict]:
    india_uri = er.getLocationUri("India")

    q = QueryArticlesIter(
        keywords             = QueryItems.OR(query_terms),
        sourceLocationUri    = [india_uri],
        lang                 = ["eng", "hin"],
        ignoreSourceGroupUri = "paywall/paywalled_sources",
        startSourceRankPercentile = 0,
        endSourceRankPercentile   = 40,
        dataType             = ["news"],
    )

    ri = ReturnInfo(articleInfo=ArticleInfoFlags(
        concepts=True, sentiment=True, categories=True, location=True
    ))

    return [_filter_article_fields(art) for art in q.execQuery(
        er,
        sortBy     = "date",
        sortByAsc  = False,
        maxItems   = 100,
        returnInfo = ri,
    )]

budget_terms = [
    # Core budget keywords
    "union budget", "budget day", "budget 2026", "budget 2026-27",
    "interim budget", "finance bill", "appropriation bill",
    "budget speech", "budget highlights", "budget announcements",
    "budget live", "budget press conference",
    "finance ministry budget", "ministry of finance budget",
    "economic survey", "economic survey india",

    # People / institutions
    "finance minister", "nirmala sitharaman", "budget by finance minister",
    "rbi reaction budget", "sebi budget impact",

    # Macro / fiscal
    "fiscal deficit", "revenue deficit", "primary deficit",
    "gross fiscal deficit", "fiscal consolidation",
    "capex", "capital expenditure", "government capex",
    "infra spending", "budget allocation", "budget outlay",
    "subsidy", "fertilizer subsidy", "food subsidy", "fuel subsidy",
    "disinvestment", "privatization", "psu divestment",
    "borrowing plan", "g-sec", "bond yields", "government borrowing",

    # Tax (direct + indirect)
    "income tax budget", "income tax slab", "new tax regime", "old tax regime",
    "tax rebate", "standard deduction", "surcharge", "cess",
    "tds", "tcs", "tax compliance",
    "gst budget", "gst rate change", "gst council",
    "customs duty", "import duty", "excise duty",

    # Markets / trading impact
    "budget impact on stock market", "budget impact on nifty", "sensex budget reaction",
    "market rally budget", "market volatility budget",
    "capital gains tax", "ltcg", "stcg", "securities transaction tax", "stt",
    "dividend tax", "buyback tax",

    # Sectors commonly affected
    "infrastructure budget", "railways budget", "roads highways budget",
    "defence budget", "psu banks budget", "banking budget", "nbfc budget",
    "real estate budget", "housing budget", "affordable housing budget",
    "healthcare budget", "pharma budget", "education budget",
    "agriculture budget", "msme budget", "startup budget",
    "manufacturing budget", "make in india budget", "pli scheme budget",
    "renewable energy budget", "solar budget", "green energy budget",
    "electric vehicle budget", "ev subsidy budget",
    "semiconductor budget", "chip manufacturing budget",
    "telecom budget", "it services budget",

    # India-specific schemes & signals
    "pmay budget", "mgnrega budget", "nrega allocation",
    "msp budget", "farm credit budget", "rural spending budget",
    "inflation outlook budget", "growth projection budget",

    # Hindi keywords (helps if lang includes hin)
    "केंद्रीय बजट", "यूनियन बजट", "बजट भाषण", "बजट हाइलाइट्स",
    "आर्थिक सर्वेक्षण", "वित्त मंत्री", "आयकर", "इनकम टैक्स स्लैब",
    "नया टैक्स रेजीम", "जीएसटी", "राजकोषीय घाटा", "पूंजीगत व्यय",
    "डिसइन्वेस्टमेंट", "कैपेक्स", "कैपिटल गेन टैक्स"
]


# ─── FastAPI Endpoint ──────────────────────────────────────────
@router.get("/news/home", tags=["Articles"])
async def get_news_home(background_tasks: BackgroundTasks):
    if _cache_valid():
        return _read_cache()

    query_terms = [
        "nifty", "nifty 50", "nifty midcap", "nifty bank", "nifty fin service",
        "nifty it", "nifty pharma", "nifty pvt bank", "sensex", "indian financial",
        "income tax", "gst", "mca", "finance ministry", "micro economics",
        "macro economics", "global economy", "indian economy", "rbi",
        "equity", "foreign investment", "stock sector"
    ]

    articles = await fetch_articles(query_terms)

    background_tasks.add_task(_write_cache, articles)

    return articles