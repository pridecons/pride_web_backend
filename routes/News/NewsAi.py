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
    # Budget Day / speech / documents
    "Union Budget", "Budget 2026", "Budget 2026-27",
    "Budget Speech", "Budget Highlights", "Budget Live",
    "Finance Bill", "Budget Announcement", "Budget Press Conference",
    "Economic Survey",

    # Key people / institutions
    "Finance Minister", "Nirmala Sitharaman",
    "RBI reaction to budget", "SEBI budget impact",

    # Direct tax (most searched on budget day)
    "income tax slabs", "new tax regime", "old tax regime",
    "standard deduction", "tax rebate", "capital gains tax",
    "LTCG", "STCG", "STT",

    # Indirect tax / duties
    "GST changes", "customs duty", "import duty",

    # Fiscal / macro / borrowing (headline heavy)
    "fiscal deficit", "capital expenditure", "capex",
    "government borrowing", "bond yields",

    # Sector headlines (budget day favorites)
    "railway budget", "infrastructure spending", "defence budget",
    "affordable housing", "MSME support", "startup incentives",
    "agriculture allocation", "PLI scheme", "renewable energy",

    # Hindi (high-signal)
    "केंद्रीय बजट", "बजट भाषण", "बजट हाइलाइट्स",
    "आर्थिक सर्वेक्षण", "आयकर स्लैब", "नया टैक्स रेजीम",
    "कैपेक्स", "राजकोषीय घाटा"
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

    articles = await fetch_articles(budget_terms)

    background_tasks.add_task(_write_cache, articles)

    return articles