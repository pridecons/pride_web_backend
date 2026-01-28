# routes/ANGEL_ONE/angel_data.py
"""
Angel One data layer (sync + async) with:
- TokenManager (auto refresh via login_and_get_token)
- Robust request wrapper (_post_json / _apost_json) with retry + token refresh
- Quote (FULL bulk)
- Candle data (historical)
- Shared connection pooling for async (httpx.AsyncClient)

✅ Drop-in replacement for your current file.
"""

from __future__ import annotations

import json
import socket
import uuid
import time
import random
import atexit
from typing import Dict, Any, List, Optional

import requests
import httpx

from routes.ANGEL_ONE.angel_login import login_and_get_token  # refreshes tokens.json
from config import ANGEL_API_KEY

QUOTE_URL = "https://apiconnect.angelbroking.com/rest/secure/angelbroking/market/v1/quote/"
CANDLE_URL = "https://apiconnect.angelbroking.com/rest/secure/angelbroking/historical/v1/getCandleData"


# ---------------------------
# Token manager
# ---------------------------
class TokenManager:
    def __init__(self, tokens_path: str = "tokens.json"):
        self.tokens_path = tokens_path
        self._cache: Optional[Dict[str, Any]] = None

    def load(self) -> Dict[str, Any]:
        if self._cache is not None:
            return self._cache
        with open(self.tokens_path, "r", encoding="utf-8") as f:
            self._cache = json.load(f)
        return self._cache

    def get_jwt(self) -> str:
        data = self.load()
        jwt = data.get("jwtToken")
        if not jwt:
            raise RuntimeError(f"{self.tokens_path} missing 'jwtToken'")
        return jwt

    def refresh_and_reload(self) -> str:
        """
        Calls login flow that should update tokens.json, then reloads cache.
        """
        login_and_get_token()  # writes tokens.json
        self._cache = None
        return self.get_jwt()

    def save(self, data: Dict[str, Any]) -> None:
        with open(self.tokens_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        self._cache = data


# ---------------------------
# base helpers
# ---------------------------
def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        try:
            s.close()
        except Exception:
            pass
    return ip


def get_mac_address() -> str:
    mac = uuid.getnode()
    return ":".join(f"{(mac >> ele) & 0xff:02X}" for ele in range(40, -8, -8))


def build_headers(jwt_token: str) -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {jwt_token}",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": get_local_ip(),
        "X-ClientPublicIP": "1.1.1.1",
        "X-MACAddress": get_mac_address(),
        "X-PrivateKey": ANGEL_API_KEY,
    }


def _looks_like_token_issue(resp_json: Dict[str, Any]) -> bool:
    """
    Angel sometimes returns 200 with status=false + message about token.
    We'll detect common patterns.
    """
    if not isinstance(resp_json, dict):
        return False

    status = resp_json.get("status")
    msg = str(resp_json.get("message", "")).lower()

    if status is False and any(
        x in msg for x in ["invalid", "token", "jwt", "session", "expired", "unauthorized"]
    ):
        return True

    return False


# ---------------------------
# SYNC request wrapper
# ---------------------------
def _post_json(
    url: str,
    token_mgr: TokenManager,
    payload: Dict[str, Any],
    timeout: int = 20,
    auto_refresh: bool = True,
) -> Dict[str, Any]:
    """
    Makes request with current token.
    If 401/403 OR response indicates token problem, auto-refresh token and retry once.
    """
    jwt = token_mgr.get_jwt()
    headers = build_headers(jwt)

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=timeout)

        # token expired often 401/403
        if auto_refresh and r.status_code in (401, 403):
            token_mgr.refresh_and_reload()
            jwt2 = token_mgr.get_jwt()
            headers2 = build_headers(jwt2)
            r2 = requests.post(url, headers=headers2, json=payload, timeout=timeout)
            r2.raise_for_status()
            return r2.json()

        r.raise_for_status()
        data = r.json()

        # sometimes 200 but status false token error
        if auto_refresh and _looks_like_token_issue(data):
            token_mgr.refresh_and_reload()
            jwt2 = token_mgr.get_jwt()
            headers2 = build_headers(jwt2)
            r2 = requests.post(url, headers=headers2, json=payload, timeout=timeout)
            r2.raise_for_status()
            return r2.json()

        return data

    except requests.HTTPError as e:
        code = getattr(e.response, "status_code", None)
        if auto_refresh and code in (401, 403):
            token_mgr.refresh_and_reload()
            jwt2 = token_mgr.get_jwt()
            headers2 = build_headers(jwt2)
            r2 = requests.post(url, headers=headers2, json=payload, timeout=timeout)
            r2.raise_for_status()
            return r2.json()
        raise


# ---------------------------
# SYNC public APIs
# ---------------------------
def quote_full_bulk(
    exchange_tokens: Dict[str, List[str]],
    tokens_path: str = "tokens.json",
    max_retries: int = 3,
) -> Dict[str, Any]:
    """
    Bulk FULL quote. Auto refreshes token if invalid/expired.
    """
    token_mgr = TokenManager(tokens_path=tokens_path)
    payload = {"mode": "FULL", "exchangeTokens": exchange_tokens}

    last_err: Dict[str, Any] = {}
    for attempt in range(1, max_retries + 1):
        try:
            return _post_json(QUOTE_URL, token_mgr, payload, timeout=25, auto_refresh=True)

        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if code in (429, 500, 502, 503, 504):
                time.sleep(1.0 * attempt + random.random())
                last_err = {"error": str(e), "status_code": code}
                continue
            raise

        except (requests.Timeout, requests.ConnectionError) as e:
            time.sleep(1.0 * attempt + random.random())
            last_err = {"error": str(e)}
            continue

        except Exception as e:
            last_err = {"error": str(e)}
            break

    return {
        "status": False,
        "message": "FAILED",
        "error": last_err,
        "data": {"fetched": [], "unfetched": []},
    }


def get_candles(
    exchange: str,
    symboltoken: str,
    interval: str,
    fromdate: str,
    todate: str,
    tokens_path: str = "tokens.json",
    max_retries: int = 6,
) -> Dict[str, Any]:
    """
    Historical candles.
    Handles AB1004 (rate/temporary) by retrying.
    """
    token_mgr = TokenManager(tokens_path=tokens_path)
    payload = {
        "exchange": exchange,
        "symboltoken": str(symboltoken),
        "interval": interval,
        "fromdate": fromdate,
        "todate": todate,
    }

    last_err: Dict[str, Any] = {}
    for attempt in range(1, max_retries + 1):
        try:
            data = _post_json(CANDLE_URL, token_mgr, payload, timeout=25, auto_refresh=True)

            # AB1004 handling (200 but status=false)
            if isinstance(data, dict) and data.get("status") is False:
                if data.get("errorcode") == "AB1004":
                    time.sleep(1.5 * attempt)
                    last_err = data
                    continue

            return data

        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * attempt)
                last_err = {"error": str(e), "status_code": code}
                continue
            raise

        except (requests.Timeout, requests.ConnectionError) as e:
            time.sleep(1.5 * attempt)
            last_err = {"error": str(e)}
            continue

        except Exception as e:
            last_err = {"error": str(e)}
            break

    return {"status": False, "message": "FAILED", "error": last_err, "data": None}


# =============================================================================
# ASYNC VERSION (for high-throughput live server)
# =============================================================================

_async_client: Optional[httpx.AsyncClient] = None


def get_async_client() -> httpx.AsyncClient:
    """
    Shared AsyncClient for connection pooling.
    """
    global _async_client
    if _async_client is None:
        _async_client = httpx.AsyncClient(
            timeout=httpx.Timeout(25.0),
            limits=httpx.Limits(max_connections=60, max_keepalive_connections=20),
        )
    return _async_client


@atexit.register
def _close_async_client_on_exit():
    """
    Best-effort close when process exits. (In uvicorn workers, process exit closes sockets anyway.)
    """
    global _async_client
    if _async_client is None:
        return
    try:
        # Can't await in atexit; just close sync-style.
        _async_client.close()
    except Exception:
        pass
    _async_client = None


async def _apost_json(
    url: str,
    token_mgr: TokenManager,
    payload: Dict[str, Any],
    auto_refresh: bool = True,
) -> Dict[str, Any]:
    """
    Async request with token auto-refresh retry once.
    """
    jwt = token_mgr.get_jwt()
    headers = build_headers(jwt)
    client = get_async_client()

    r = await client.post(url, headers=headers, json=payload)
    if auto_refresh and r.status_code in (401, 403):
        token_mgr.refresh_and_reload()
        jwt2 = token_mgr.get_jwt()
        headers2 = build_headers(jwt2)
        r2 = await client.post(url, headers=headers2, json=payload)
        r2.raise_for_status()
        return r2.json()

    r.raise_for_status()
    data = r.json()

    if auto_refresh and _looks_like_token_issue(data):
        token_mgr.refresh_and_reload()
        jwt2 = token_mgr.get_jwt()
        headers2 = build_headers(jwt2)
        r2 = await client.post(url, headers=headers2, json=payload)
        r2.raise_for_status()
        return r2.json()

    return data


async def aquote_full_bulk(
    exchange_tokens: Dict[str, List[str]],
    tokens_path: str = "tokens.json",
    max_retries: int = 3,
) -> Dict[str, Any]:
    """
    Async bulk FULL quote with retries.
    """
    token_mgr = TokenManager(tokens_path=tokens_path)
    payload = {"mode": "FULL", "exchangeTokens": exchange_tokens}

    last_err: Dict[str, Any] = {}
    for attempt in range(1, max_retries + 1):
        try:
            return await _apost_json(QUOTE_URL, token_mgr, payload, auto_refresh=True)

        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            if code in (429, 500, 502, 503, 504):
                await _asleep_jitter(attempt, base=1.0)
                last_err = {"error": str(e), "status_code": code}
                continue
            raise

        except (httpx.TimeoutException, httpx.NetworkError) as e:
            await _asleep_jitter(attempt, base=1.0)
            last_err = {"error": str(e)}
            continue

        except Exception as e:
            last_err = {"error": str(e)}
            break

    return {
        "status": False,
        "message": "FAILED",
        "error": last_err,
        "data": {"fetched": [], "unfetched": []},
    }


async def aget_candles(
    exchange: str,
    symboltoken: str,
    interval: str,
    fromdate: str,
    todate: str,
    tokens_path: str = "tokens.json",
    max_retries: int = 6,
) -> Dict[str, Any]:
    """
    Async historical candles with AB1004 handling.
    """
    token_mgr = TokenManager(tokens_path=tokens_path)
    payload = {
        "exchange": exchange,
        "symboltoken": str(symboltoken),
        "interval": interval,
        "fromdate": fromdate,
        "todate": todate,
    }

    last_err: Dict[str, Any] = {}
    for attempt in range(1, max_retries + 1):
        try:
            data = await _apost_json(CANDLE_URL, token_mgr, payload, auto_refresh=True)

            if isinstance(data, dict) and data.get("status") is False:
                if data.get("errorcode") == "AB1004":
                    await _asleep_jitter(attempt, base=1.5)
                    last_err = data
                    continue

            return data

        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            if code in (429, 500, 502, 503, 504):
                await _asleep_jitter(attempt, base=1.5)
                last_err = {"error": str(e), "status_code": code}
                continue
            raise

        except (httpx.TimeoutException, httpx.NetworkError) as e:
            await _asleep_jitter(attempt, base=1.5)
            last_err = {"error": str(e)}
            continue

        except Exception as e:
            last_err = {"error": str(e)}
            break

    return {"status": False, "message": "FAILED", "error": last_err, "data": None}


async def _asleep_jitter(attempt: int, base: float = 1.0) -> None:
    """
    Backoff with jitter for async retries.
    """
    # simple: base * attempt + jitter [0..0.6)
    await asyncio_sleep(base * attempt + random.random() * 0.6)


# Avoid importing asyncio at top for faster import? (kept simple)
import asyncio
asyncio_sleep = asyncio.sleep
