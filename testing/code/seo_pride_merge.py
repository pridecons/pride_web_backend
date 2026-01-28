# /home/pride/pride_web_backend/testing/code/seo_pride_merge.py
from __future__ import annotations

import argparse
import json
import random
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from db.connection import SessionLocal
from db.models import NseCmSecurity, SEOKeyword


PRIDECONS_LOGO = "https://pridecons.com/logo.png"
PRIDECONS_HOME = "https://pridecons.com"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------

@dataclass
class FetchCfg:
    timeout: int = 25
    retries: int = 3
    backoff_base: float = 1.35
    sleep_jitter: Tuple[float, float] = (0.2, 0.8)


def fetch_html(url: str, cfg: FetchCfg) -> str:
    last_err: Optional[str] = None
    for attempt in range(1, cfg.retries + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Connection": "close",
                },
                method="GET",
            )
            with urllib.request.urlopen(req, timeout=cfg.timeout) as resp:
                data = resp.read()
            return data.decode("utf-8", errors="ignore")
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read(2000).decode("utf-8", errors="ignore")
            except Exception:
                pass
            last_err = f"HTTP {e.code}: {body[:500]}"
        except Exception as e:
            last_err = str(e)

        # backoff
        sleep_s = (cfg.backoff_base**attempt) + random.uniform(*cfg.sleep_jitter)
        time.sleep(sleep_s)

    raise RuntimeError(f"Failed to fetch {url}. Last error: {last_err}")


# ---------------------------------------------------------------------
# HTML -> SEO extractors (BeautifulSoup)
# ---------------------------------------------------------------------

def _clean_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s or None


def _first_meta(
    soup: BeautifulSoup, *, name: Optional[str] = None, prop: Optional[str] = None
) -> Optional[str]:
    if name:
        tag = soup.find("meta", attrs={"name": name})
        if tag and tag.get("content"):
            return _clean_text(tag.get("content"))
    if prop:
        tag = soup.find("meta", attrs={"property": prop})
        if tag and tag.get("content"):
            return _clean_text(tag.get("content"))
    return None


def _canonical(soup: BeautifulSoup) -> Optional[str]:
    link = soup.find("link", rel=lambda v: v and "canonical" in v)
    if link and link.get("href"):
        return _clean_text(link.get("href"))
    return None


def _jsonld(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        txt = (tag.string or tag.get_text() or "").strip()
        if not txt:
            continue
        try:
            parsed = json.loads(txt)
            if isinstance(parsed, list):
                out.extend([x for x in parsed if isinstance(x, dict)])
            elif isinstance(parsed, dict):
                out.append(parsed)
        except Exception:
            continue
    return out


def extract_seo_from_html(html: str, *, source: str, url: str, parser: str) -> Dict[str, Any]:
    # parser="lxml" is much faster, fallback to "html.parser" if lxml not installed
    soup = BeautifulSoup(html, parser)

    title = _clean_text((soup.title.string if soup.title else None))
    description = _first_meta(soup, name="description")
    robots = _first_meta(soup, name="robots")
    keywords = _first_meta(soup, name="keywords")
    canonical = _canonical(soup)

    og_keys = [
        "og:locale",
        "og:type",
        "og:title",
        "og:description",
        "og:image",
        "og:url",
        "og:site_name",
    ]
    open_graph: Dict[str, Any] = {}
    for k in og_keys:
        v = _first_meta(soup, prop=k)
        if v:
            open_graph[k] = v

    tw_keys = [
        "twitter:card",
        "twitter:title",
        "twitter:description",
        "twitter:image",
        "twitter:site",
        "twitter:creator",
    ]
    twitter: Dict[str, Any] = {}
    for k in tw_keys:
        v = _first_meta(soup, name=k)
        if v:
            twitter[k] = v

    return {
        "source": source,
        "url": url,
        "title": title,
        "description": description,
        "robots": robots,
        "keywords": keywords,
        "canonical": canonical,
        "open_graph": open_graph,
        "twitter": twitter,
        "jsonld": _jsonld(soup),
    }


# ---------------------------------------------------------------------
# Groww URL resolver (better)
# ---------------------------------------------------------------------

def _slugify(name: str) -> str:
    s = "".join(ch.lower() if ch.isalnum() else "-" for ch in name.strip())
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


def _normalize_company_name(name: str) -> str:
    """
    Remove trailing legal suffixes so we don't generate '-limited-limited' / '-ltd-ltd'
    """
    s = re.sub(r"\s+", " ", (name or "")).strip()
    s = re.sub(r"\b(limited|ltd\.?)\b\.?\s*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\b(limited|ltd\.?)\b\.?\s*$", "", s, flags=re.IGNORECASE).strip()
    return s or name


def resolve_groww_urls(symbol: str, company_name_hint: Optional[str]) -> List[str]:
    tried: List[str] = []
    if company_name_hint and company_name_hint.strip():
        base_name = _normalize_company_name(company_name_hint)
        base = _slugify(base_name)
        tried.append(f"https://groww.in/stocks/{base}")
        tried.append(f"https://groww.in/stocks/{base}-ltd")
        tried.append(f"https://groww.in/stocks/{base}-limited")
    else:
        sym = symbol.strip().upper()
        tried.append(f"https://groww.in/stocks/{sym}")
        tried.append(f"https://groww.in/stocks/{sym.lower()}")

    out: List[str] = []
    seen = set()
    for u in tried:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


# ---------------------------------------------------------------------
# Merge logic
# ---------------------------------------------------------------------

def _pick(prefer: Optional[str], fallback: Optional[str]) -> Optional[str]:
    return prefer if (prefer is not None and str(prefer).strip() != "") else fallback


def _merge_dict(prefer: Dict[str, Any], fallback: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(fallback or {})
    for k, v in (prefer or {}).items():
        if v is not None and v != "":
            out[k] = v
    return out


def _merge_jsonld(prefer: List[Dict[str, Any]], fallback: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for item in (prefer or []) + (fallback or []):
        if not isinstance(item, dict):
            continue
        key = json.dumps(item, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def merge_seo(groww: Dict[str, Any], screener: Dict[str, Any], *, canonical_url: Optional[str]) -> Dict[str, Any]:
    return {
        "title": _pick(groww.get("title"), screener.get("title")),
        "description": _pick(groww.get("description"), screener.get("description")),
        "robots": _pick(groww.get("robots"), screener.get("robots")),
        "keywords": _pick(screener.get("keywords"), groww.get("keywords")),
        "canonical": canonical_url or _pick(groww.get("canonical"), screener.get("canonical")),
        "open_graph": _merge_dict(groww.get("open_graph") or {}, screener.get("open_graph") or {}),
        "twitter": _merge_dict(groww.get("twitter") or {}, screener.get("twitter") or {}),
        "jsonld": _merge_jsonld(groww.get("jsonld") or [], screener.get("jsonld") or []),
    }


# ---------------------------------------------------------------------
# PrideCons rewrite + keep only useful fields (FAQ allowed)
# ---------------------------------------------------------------------

def build_pride_url(symbol: str, stock_name: str) -> str:
    sym = symbol.strip().upper()
    slug = _slugify(stock_name)
    return f"https://pridecons.com/nse/stock/{sym}/{slug}"


def _rewrite_jsonld_urls(item: Dict[str, Any], canonical: str) -> Dict[str, Any]:
    x = dict(item)

    if isinstance(x.get("url"), str):
        x["url"] = canonical
    if isinstance(x.get("mainEntityOfPage"), str):
        x["mainEntityOfPage"] = canonical
    if isinstance(x.get("mainEntityOfPage"), dict):
        mep = dict(x["mainEntityOfPage"])
        if isinstance(mep.get("@id"), str):
            mep["@id"] = canonical
        x["mainEntityOfPage"] = mep

    pub = x.get("publisher")
    if isinstance(pub, dict):
        pub2 = dict(pub)
        pub2["name"] = "PrideCons"
        pub2["url"] = PRIDECONS_HOME
        logo = pub2.get("logo")
        if isinstance(logo, dict):
            logo2 = dict(logo)
            if "contentUrl" in logo2:
                logo2["contentUrl"] = PRIDECONS_LOGO
            if "url" in logo2:
                logo2["url"] = PRIDECONS_LOGO
            pub2["logo"] = logo2
        else:
            pub2["logo"] = {"@type": "ImageObject", "contentUrl": PRIDECONS_LOGO}
        x["publisher"] = pub2

    for k in ("image", "logo"):
        if k in x:
            if isinstance(x[k], str):
                x[k] = PRIDECONS_LOGO
            elif isinstance(x[k], dict):
                y = dict(x[k])
                if "contentUrl" in y:
                    y["contentUrl"] = PRIDECONS_LOGO
                if "url" in y:
                    y["url"] = PRIDECONS_LOGO
                x[k] = y

    if x.get("@type") == "BreadcrumbList":
        items = x.get("itemListElement")
        if isinstance(items, list):
            new_items = []
            for it in items:
                if not isinstance(it, dict):
                    continue
                it2 = dict(it)
                item = it2.get("item")
                if isinstance(item, dict):
                    item2 = dict(item)
                    pos = it2.get("position")
                    if pos == 1:
                        item2["@id"] = "https://pridecons.com/"
                        item2["name"] = item2.get("name") or "Home"
                    elif pos == 2:
                        item2["@id"] = "https://pridecons.com/nse"
                        item2["name"] = item2.get("name") or "NSE"
                    else:
                        item2["@id"] = canonical
                        item2["name"] = item2.get("name") or "Stock"
                    it2["item"] = item2
                new_items.append(it2)
            x["itemListElement"] = new_items

    return x


def to_pride_seo(merged: Dict[str, Any], *, symbol: str, stock_name: str, max_faq: int = 10) -> Dict[str, Any]:
    canonical = build_pride_url(symbol, stock_name)

    title = _clean_text(merged.get("title"))
    desc = _clean_text(merged.get("description"))
    robots = _clean_text(merged.get("robots")) or "index,follow"

    og = merged.get("open_graph") or {}
    tw = merged.get("twitter") or {}
    jsonlds: List[Dict[str, Any]] = merged.get("jsonld") or []

    kept: List[Dict[str, Any]] = []
    faq_added = False

    for item in jsonlds:
        if not isinstance(item, dict):
            continue
        t = item.get("@type")
        if t in ("WebPage", "BreadcrumbList"):
            kept.append(_rewrite_jsonld_urls(item, canonical))
        elif t == "FAQPage" and not faq_added:
            faq = dict(item)
            me = faq.get("mainEntity")
            if isinstance(me, list) and max_faq:
                faq["mainEntity"] = me[:max_faq]
            kept.append(_rewrite_jsonld_urls(faq, canonical))
            faq_added = True

    if not any(isinstance(x, dict) and x.get("@type") == "WebPage" for x in kept):
        kept.insert(
            0,
            _rewrite_jsonld_urls(
                {
                    "@context": "https://schema.org",
                    "@type": "WebPage",
                    "name": title,
                    "description": desc,
                    "url": canonical,
                    "publisher": {
                        "@type": "Organization",
                        "name": "PrideCons",
                        "url": PRIDECONS_HOME,
                        "logo": {"@type": "ImageObject", "contentUrl": PRIDECONS_LOGO},
                    },
                },
                canonical,
            ),
        )

    return {
        "symbol": symbol.strip().upper(),
        "stock_name": stock_name.strip(),
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
        "canonical": canonical,
        "title": title,
        "description": desc,
        "robots": robots,
        "open_graph": {
            "og:type": "website",
            "og:site_name": "PrideCons",
            "og:url": canonical,
            "og:title": _clean_text(og.get("og:title")) or title,
            "og:description": _clean_text(og.get("og:description")) or desc,
            "og:image": PRIDECONS_LOGO,
        },
        "twitter": {
            "twitter:card": _clean_text(tw.get("twitter:card")) or "summary_large_image",
            "twitter:title": _clean_text(tw.get("twitter:title")) or title,
            "twitter:description": _clean_text(tw.get("twitter:description")) or desc,
            "twitter:image": PRIDECONS_LOGO,
        },
        "jsonld": kept,
    }


# ---------------------------------------------------------------------
# Fetch one symbol (parallel-friendly; NO DB inside)
# ---------------------------------------------------------------------

def fetch_sources(
    symbol: str,
    company_name: str,
    cfg: FetchCfg,
    parser: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], str]:
    """
    Returns: (groww_seo, screener_seo, debug_line)
    """
    sym = symbol.strip().upper()
    dbg: List[str] = []

    # Screener
    screener_seo: Optional[Dict[str, Any]] = None
    screener_url = f"https://www.screener.in/company/{sym}/consolidated/"
    try:
        s_html = fetch_html(screener_url, cfg)
        screener_seo = extract_seo_from_html(s_html, source="screener", url=screener_url, parser=parser)
        dbg.append(f"✅ Screener OK")
    except Exception as e:
        dbg.append(f"⚠️ Screener FAIL: {e}")

    # Groww candidates
    groww_seo: Optional[Dict[str, Any]] = None
    groww_urls = resolve_groww_urls(sym, company_name_hint=company_name)
    last_groww_err: Optional[Exception] = None
    for gu in groww_urls:
        try:
            g_html = fetch_html(gu, cfg)
            groww_seo = extract_seo_from_html(g_html, source="groww", url=gu, parser=parser)
            dbg.append(f"✅ Groww OK")
            break
        except Exception as e:
            last_groww_err = e
            continue
    if groww_seo is None:
        dbg.append(f"⚠️ Groww FAIL: {last_groww_err}")

    return groww_seo, screener_seo, " | ".join(dbg)


# ---------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------

_SYMBOL_OK_RE = re.compile(r"^[A-Z0-9][A-Z0-9\-]{0,31}$")


def is_valid_symbol(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s:
        return False
    if "NSETEST" in s:
        return False
    if "$" in s:
        return False
    if not _SYMBOL_OK_RE.match(s):
        return False
    return True


def get_company_name_from_tuple(row: Tuple) -> str:
    # row = (symbol, company_name/security_name/...)
    # pick first non-empty after symbol
    for v in row[1:]:
        if v and str(v).strip():
            return str(v).strip()
    return "Stock"


def load_saved_symbols_for_today(db, fetch_date: date) -> set:
    """
    One query instead of per-row duplicate check.
    """
    try:
        rows = db.query(SEOKeyword.symbol).filter(SEOKeyword.fetch_date == fetch_date).all()
        return {str(r[0]).strip().upper() for r in rows if r and r[0]}
    except Exception:
        return set()


def upsert_seo(db, symbol: str, company_name: str, fetch_date: date, payload: Dict[str, Any]) -> None:
    sym = symbol.strip().upper()
    row = (
        db.query(SEOKeyword)
        .filter(SEOKeyword.symbol == sym, SEOKeyword.fetch_date == fetch_date)
        .first()
    )
    if row:
        row.company_name = company_name
        row.data = payload
        print(f"  🔁 UPDATED (symbol={sym}, fetch_date={fetch_date})")
    else:
        row = SEOKeyword(
            symbol=sym,
            company_name=company_name,
            fetch_date=fetch_date,
            data=payload,
        )
        db.add(row)
        print(f"  💾 SAVED (symbol={sym}, fetch_date={fetch_date})")


# ---------------------------------------------------------------------
# Worker task (NO DB writes here)
# ---------------------------------------------------------------------

def process_one_symbol(
    symbol: str,
    company_name: str,
    cfg: FetchCfg,
    today: date,
    parser: str,
    max_faq: int,
) -> Tuple[str, str, Optional[Dict[str, Any]], str]:
    """
    Returns: (symbol, company_name, payload_or_none, debug)
    """
    sym = symbol.strip().upper()
    cname = (company_name or "").strip() or "Stock"

    # fetch sources
    groww_seo, screener_seo, dbg = fetch_sources(sym, cname, cfg, parser)

    if groww_seo is None and screener_seo is None:
        return sym, cname, None, f"{dbg} | ❌ Both failed"

    merged = merge_seo(groww_seo or {}, screener_seo or {}, canonical_url=None)
    pride = to_pride_seo(merged, symbol=sym, stock_name=cname, max_faq=max_faq)

    payload = {
        "symbol": sym,
        "company_name": cname,
        "fetch_date": str(today),
        "merged_pride": pride,
    }
    return sym, cname, payload, f"{dbg} | ✅ OK"


# ---------------------------------------------------------------------
# Bulk runner (FAST: parallel network + single-thread DB writes)
# ---------------------------------------------------------------------

def run_bulk(
    limit: Optional[int],
    offset: int,
    batch_size: int,
    workers: int,
    commit_every: int,
    parser: str,
    max_faq: int,
) -> None:
    cfg = FetchCfg()
    today = datetime.now().date()

    db = SessionLocal()
    try:
        # choose columns: symbol + best-known name columns (adjust if your model differs)
        cols = [NseCmSecurity.symbol]

        # try common name columns if they exist
        for col_name in ("company_name", "security_name", "name", "symbol_name", "scrip_name"):
            if hasattr(NseCmSecurity, col_name):
                cols.append(getattr(NseCmSecurity, col_name))

        base_q = db.query(*cols)
        if hasattr(NseCmSecurity, "series"):
            base_q = base_q.filter(NseCmSecurity.series == "EQ")
        base_q = base_q.order_by(NseCmSecurity.symbol.asc())

        # preload saved symbols for today (one query)
        saved_today = load_saved_symbols_for_today(db, today)
        print(f"✅ Loaded {len(saved_today)} symbols already saved for {today}")
        print(f"🚀 Starting parallel fetch with workers={workers}, batch_size={batch_size}, commit_every={commit_every}\n")

        processed = 0
        saved_or_updated = 0
        skipped_invalid = 0
        skipped_existing = 0
        failed_both = 0

        page = 0
        pending_commit = 0

        while True:
            take = batch_size
            if limit is not None:
                remaining = limit - processed
                if remaining <= 0:
                    break
                take = min(take, remaining)

            page_offset = offset + (page * batch_size)
            rows: List[Tuple] = base_q.offset(page_offset).limit(take).all()
            if not rows:
                break

            # prepare tasks for this page
            tasks = []
            with ThreadPoolExecutor(max_workers=workers) as ex:
                for row in rows:
                    processed += 1
                    sym = str(row[0]).strip().upper()
                    cname = get_company_name_from_tuple(row)

                    if not is_valid_symbol(sym):
                        print(f"[{processed}] ⛔ invalid/test symbol skipped: {sym}")
                        skipped_invalid += 1
                        continue

                    if sym in saved_today:
                        print(f"[{processed}] ⏭️ Already exists for today. Skipping. ({sym})")
                        skipped_existing += 1
                        continue

                    print(f"\n[{processed}] {sym} | {cname}")
                    tasks.append(
                        ex.submit(
                            process_one_symbol,
                            sym,
                            cname,
                            cfg,
                            today,
                            parser,
                            max_faq,
                        )
                    )

                # collect results and write to DB (single thread)
                for fut in as_completed(tasks):
                    try:
                        sym, cname, payload, dbg = fut.result()
                    except Exception as e:
                        print(f"  ❌ Worker crashed: {e}")
                        continue

                    print(f"  {dbg}")

                    if payload is None:
                        failed_both += 1
                        continue

                    # DB upsert
                    try:
                        upsert_seo(db, sym, cname, today, payload)
                        saved_today.add(sym)  # prevent duplicates in same run
                        saved_or_updated += 1
                        pending_commit += 1

                        if commit_every > 0 and pending_commit >= commit_every:
                            db.commit()
                            pending_commit = 0
                            print(f"  ✅ COMMIT (batch={commit_every})")
                    except Exception as e:
                        db.rollback()
                        print(f"  ❌ DB save failed: {e}")

            page += 1

        # final commit
        if pending_commit:
            try:
                db.commit()
                print(f"\n✅ FINAL COMMIT ({pending_commit} records)")
            except Exception as e:
                db.rollback()
                print(f"\n❌ Final commit failed: {e}")

        print("\n✅ DONE")
        print(f"Total processed (including skipped): {processed}")
        print(f"Saved/Updated: {saved_or_updated}")
        print(f"Skipped invalid: {skipped_invalid}")
        print(f"Skipped already existing today: {skipped_existing}")
        print(f"Failed (both sources): {failed_both}")

    finally:
        db.close()


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="FAST: Fetch Groww+Screener SEO in parallel, rewrite for PrideCons, save into SEOKeyword."
    )
    parser.add_argument("--limit", type=int, default=None, help="Max EQ records to process")
    parser.add_argument("--offset", type=int, default=0, help="Start offset")
    parser.add_argument("--batch-size", type=int, default=200, help="DB page size")
    parser.add_argument("--workers", type=int, default=12, help="Parallel workers (network-bound). Start with 10-15.")
    parser.add_argument("--commit-every", type=int, default=50, help="Commit after N upserts (0 = only final commit).")
    parser.add_argument("--parser", type=str, default="lxml", help="BeautifulSoup parser: lxml (fast) or html.parser")
    parser.add_argument("--max-faq", type=int, default=10, help="Max FAQ items to keep in JSON-LD")
    args = parser.parse_args()

    # if lxml isn't installed, BS4 will raise FeatureNotFound; fallback automatically
    chosen_parser = args.parser
    if chosen_parser == "lxml":
        try:
            # quick sanity check - will raise if lxml not available
            BeautifulSoup("<html></html>", "lxml")
        except Exception:
            print("⚠️  lxml parser not available. Falling back to html.parser")
            chosen_parser = "html.parser"

    try:
        run_bulk(
            limit=args.limit,
            offset=args.offset,
            batch_size=args.batch_size,
            workers=args.workers,
            commit_every=args.commit_every,
            parser=chosen_parser,
            max_faq=args.max_faq,
        )
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user.")


if __name__ == "__main__":
    main()
