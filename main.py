# main.py (FULL UPDATED FILE)

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from contextlib import asynccontextmanager
import uvicorn
import logging
import os
from datetime import datetime, date
from pathlib import Path
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from zoneinfo import ZoneInfo

from fastapi.responses import JSONResponse

from db.connection import engine, check_database_connection
from db import models

from sftp.NSE.sftp_client import SFTPClient

from config import LIVE_DATA_FETCH

# NSE Data
from utils.NSE_Formater.data_ingestor import process_cm30_for_date, process_cm30_security_for_date
from utils.NSE_Formater.bhavcopy_ingestor import process_cm_bhavcopy_for_date

from routes.NSE import Top_Marqee, Todays_Stock, Market_And_Sectors, Preopen_Movers, Most_Traded, Historical_data
from routes.Cloude_Data import corporateAction, faoOiParticipant, fiidiiTrade, resultCalendar, ipo
from routes.Cloude_Data.News import news
from routes.static_proxy import static_proxy

# NSE Detail Data
from routes.Nse_Stock_Details import Indian_Stock_Exchange_Details

# Service
from routes.Service.Payment import plan, Payment, Payment_status
from routes.Service.KYC import Otp_Kyc, Pan_Kyc, Data_Kyc
from routes.News import NewsAi

# testing
from routes.Missing_Logo import MissingLogo

# mutual fund
from routes.Mutual_Fund import MutualFund
from routes.Mutual_Fund import Home_Mf

# SEO
from routes.SEO import Seo_Keyword

# Angel One Market Movement (LIVE)
# from routes.Angel_One import live_server  # includes router + start_background_producer
# from routes.Angel_One.angel_login import login_and_get_token  


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("main")

BASE_DIR = Path(__file__).resolve().parent
STATIC_ROOT = Path(os.getenv("STATIC_ROOT", BASE_DIR / "static")).resolve()
STATIC_ROOT.mkdir(parents=True, exist_ok=True)

scheduler = AsyncIOScheduler()
IST = ZoneInfo("Asia/Kolkata")


# -------------------------------
# Helpers
# -------------------------------
def ist_today() -> date:
    return datetime.now(IST).date()


def build_cm30_security_remote_path(trade_date: date) -> str:
    # /CM30/SECURITY/<MonthDDYYYY>/Securities.dat
    folder = trade_date.strftime("%B%d%Y")
    return f"/CM30/SECURITY/{folder}/Securities.dat"


def build_bhavcopy_remote_path(trade_date: date) -> str:
    # /CM/BHAV/cmDDMONYYYYbhav.csv
    mon = trade_date.strftime("%b").upper()
    dd = trade_date.strftime("%d")
    yyyy = trade_date.strftime("%Y")
    file_name = f"cm{dd}{mon}{yyyy}bhav.csv"
    return f"/CM/BHAV/{file_name}"


def remote_file_ready(remote_path: str, min_bytes: int = 500) -> bool:
    """
    ✅ Only run ingestion when file exists AND non-empty on SFTP.
    Prevents 'empty / not uploaded yet' runs.
    """
    sftp = SFTPClient()
    try:
        sftp.connect()
        try:
            st = sftp.client.stat(remote_path)  # paramiko SFTPClient
            size = int(getattr(st, "st_size", 0) or 0)
            if size >= min_bytes:
                return True
            logger.info(f"[SFTP-CHECK] {remote_path} exists but too small: {size} bytes")
            return False
        except FileNotFoundError:
            logger.info(f"[SFTP-CHECK] Not found: {remote_path}")
            return False
        except Exception as e:
            logger.warning(f"[SFTP-CHECK] stat failed for {remote_path}: {e}")
            return False
    finally:
        try:
            sftp.close()
        except Exception:
            pass


# -------------------------------
# Scheduler Jobs
# -------------------------------
def _cm30_job():
    """
    Intraday CM30 folder ingestion.
    Runs every 1 minute.
    """
    today = ist_today()
    try:
        logger.info(f"[CM30-JOB] Running ingestion for {today}")
        process_cm30_for_date(today)
    except Exception as e:
        logger.error(f"[CM30-JOB] Error: {e}", exc_info=True)


def _bhavcopy_job():
    """
    ✅ Run ONLY when files are actually available on SFTP.
    - Securities.dat
    - Bhavcopy CSV
    """
    today = ist_today()

    try:
        sec_path = build_cm30_security_remote_path(today)
        bhav_path = build_bhavcopy_remote_path(today)

        logger.info(f"[CM-BHAV-JOB] Checking availability for {today}")
        logger.info(f"[CM-BHAV-JOB] Securities: {sec_path}")
        logger.info(f"[CM-BHAV-JOB] Bhavcopy  : {bhav_path}")

        sec_ready = remote_file_ready(sec_path, min_bytes=2000)  # securities.dat usually larger
        bhav_ready = remote_file_ready(bhav_path, min_bytes=500)  # csv non-empty

        if not sec_ready:
            logger.info("[CM-BHAV-JOB] Skip: Securities.dat not ready yet.")
            return

        if not bhav_ready:
            logger.info("[CM-BHAV-JOB] Skip: Bhavcopy CSV not ready yet.")
            return

        logger.info(f"[CM-BHAV-JOB] ✅ Files ready. Running ingestion for {today}")

        process_cm30_security_for_date(today)
        process_cm_bhavcopy_for_date(today)

    except Exception as e:
        logger.error(f"[CM-BHAV-JOB] Error: {e}", exc_info=True)


# -------------------------------
# Lifespan
# -------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Starting Backend...")

    try:
        FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")
        logger.info("✅ Cache initialized")

        if not check_database_connection():
            raise Exception("Database connection failed")
        logger.info("✅ Database connection verified")

        models.Base.metadata.create_all(bind=engine, checkfirst=True)
        logger.info("✅ DB tables created/verified")

        if LIVE_DATA_FETCH:
            # ✅ CM30 every minute
            scheduler.add_job(_cm30_job, "interval", minutes=1)

            # ✅ Bhavcopy check every 10 minutes (better than fixed 18:45)
            # Because file upload timing can vary day-to-day.
            scheduler.add_job(_bhavcopy_job, "interval", minutes=10)

        # login_and_get_token()  # one-time at startup
        # scheduler.add_job(login_and_get_token, "interval", hours=6)

        scheduler.start()
        logger.info("✅ Scheduler started")

        # ✅ ANGEL ONE LIVE PRODUCER (multi-worker safe)
        # Only ONE worker becomes leader and publishes snapshots to Redis;
        # All workers can serve SSE and all clients see identical snapshots.
        # try:
        #     live_server.start_background_producer(
        #         refresh_sec=int(os.getenv("ANGEL_REFRESH_SEC", "5")),
        #         stocklist_path=os.getenv("ANGEL_STOCKLIST_PATH", "routes/Angel_One/stockList.json"),
        #         tokens_path=os.getenv("ANGEL_TOKENS_PATH", "tokens.json"),
        #         interval_30m=os.getenv("ANGEL_INTERVAL_30M", "THIRTY_MINUTE"),
        #         interval_day=os.getenv("ANGEL_INTERVAL_DAY", "ONE_DAY"),
        #         lookback_days_30m=int(os.getenv("ANGEL_LOOKBACK_30M_DAYS", "60")),
        #         lookback_days_day=int(os.getenv("ANGEL_LOOKBACK_DAY_DAYS", "520")),
        #         candle_concurrency=int(os.getenv("ANGEL_CANDLE_CONCURRENCY", "15")),
        #     )
        #     logger.info("✅ Angel One live producer started (leader-lock enabled)")
        # except Exception as e:
        #     # don't fail whole app if redis is down; you can still use /signals/once
        #     logger.error(f"❌ Angel One producer start failed: {e}", exc_info=True)

        logger.info("🎉 Startup complete.")

    except Exception as e:
        logger.error("❌ Startup failed: %s", e, exc_info=True)
        raise

    try:
        yield
    finally:
        try:
            if scheduler.running:
                scheduler.shutdown(wait=False)
                logger.info("🛑 Scheduler stopped")
        except Exception as e:
            logger.error(f"Error while stopping scheduler: {e}", exc_info=True)

        logger.info("🛑 Backend shutdown complete.")


# -------------------------------
# App
# -------------------------------
app = FastAPI(
    title="CRM Backend API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/v1/health")
def health_check():
    try:
        db_status = check_database_connection()
        return {
            "status": "healthy" if db_status else "unhealthy",
            "database": "connected" if db_status else "disconnected",
            "version": "1.0.0",
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        raise HTTPException(status_code=503, detail="Service unhealthy")


# -------------------------------
# Register routers
# -------------------------------
try:
    # SEO
    app.include_router(Seo_Keyword.router, prefix="/api/v1")

    # testing
    app.include_router(MissingLogo.router, prefix="/api/v1")

    # Mutual fund
    app.include_router(Home_Mf.router, prefix="/api/v1")
    app.include_router(MutualFund.router, prefix="/api/v1")

    # Service
    app.include_router(Payment_status.router, prefix="/api/v1")
    app.include_router(NewsAi.router, prefix="/api/v1")
    app.include_router(Pan_Kyc.router, prefix="/api/v1")
    app.include_router(Data_Kyc.router, prefix="/api/v1")
    app.include_router(Otp_Kyc.router, prefix="/api/v1")
    app.include_router(Payment.router, prefix="/api/v1")
    app.include_router(plan.router, prefix="/api/v1")

    # NSE Data
    app.include_router(Historical_data.router, prefix="/api/v1")
    app.include_router(static_proxy.router, prefix="/api/v1")
    app.include_router(news.router, prefix="/api/v1")
    app.include_router(ipo.router, prefix="/api/v1")
    app.include_router(resultCalendar.router, prefix="/api/v1")
    app.include_router(fiidiiTrade.router, prefix="/api/v1")
    app.include_router(corporateAction.router, prefix="/api/v1")
    app.include_router(faoOiParticipant.router, prefix="/api/v1")
    app.include_router(Most_Traded.router, prefix="/api/v1")
    app.include_router(Preopen_Movers.router, prefix="/api/v1")
    app.include_router(Market_And_Sectors.router, prefix="/api/v1")
    app.include_router(Todays_Stock.router, prefix="/api/v1")
    app.include_router(Top_Marqee.router, prefix="/api/v1")

    # NSE Detail Data
    app.include_router(Indian_Stock_Exchange_Details.router, prefix="/api/v1")

    # ✅ Angel One Live (SSE)
    # app.include_router(live_server.router, prefix="/api/v1")

except Exception as e:
    logger.error(f"Failed to register routes: {e}", exc_info=True)
    raise


# -------------------------------
# Exception handler
# -------------------------------
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Global exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc),
        },
    )


# -------------------------------
# Run
# -------------------------------
if __name__ == "__main__":
    logger.info("🚀 Starting server with Uvicorn...")
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=True,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
        reload_excludes=[
            "static/*",
            "vbc_token_cache/*",
            "logs/*",
        ],
    )
