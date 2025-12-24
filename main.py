from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from contextlib import asynccontextmanager
import uvicorn
import logging
import os
from datetime import datetime  # ✅ needed for scheduler job
from datetime import date
from pathlib import Path

from db.connection import engine, check_database_connection
from db import models
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from utils.NSE_Formater.data_ingestor import process_cm30_for_date, process_cm30_security_for_date
from utils.NSE_Formater.bhavcopy_ingestor import process_cm_bhavcopy_for_date
from fastapi.responses import JSONResponse
from routes.NSE import Top_Marqee, Todays_Stock, Market_And_Sectors, Preopen_Movers, Most_Traded
from routes.Cloude_Data import corporateAction, faoOiParticipant, fiidiiTrade, resultCalendar, ipo
from routes.Cloude_Data.News import news
from routes.Payment import plan, Payment

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("main")

# --------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
STATIC_ROOT = Path(os.getenv("STATIC_ROOT", BASE_DIR / "static")).resolve()
STATIC_ROOT.mkdir(parents=True, exist_ok=True)

# Global scheduler instance
scheduler = AsyncIOScheduler()


def _cm30_job():
    today = datetime.now().date()
    try:
        logger.info(f"[CM30-JOB] Running ingestion for {today}")

        # 1) Pehle securities master load/update
        process_cm30_security_for_date(today)

        # 2) Fir intraday mkt + ind data
        process_cm30_for_date(today)

    except Exception as e:
        logger.error(f"[CM30-JOB] Error: {e}", exc_info=True)

def _bhavcopy_job():
    today = datetime.now().date()
    try:
        logger.info(f"[CM-BHAV-JOB] Running bhavcopy ingestion for {today}")
        process_cm_bhavcopy_for_date(today)
    except Exception as e:
        logger.error(f"[CM-BHAV-JOB] Error: {e}", exc_info=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Starting CRM Backend...")

    try:
        # Cache init
        FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")
        logger.info("✅ Cache initialized")

        # DB connection check
        if not check_database_connection():
            raise Exception("Database connection failed")
        logger.info("✅ Database connection verified")

        # Tables
        models.Base.metadata.create_all(bind=engine, checkfirst=True)
        logger.info("✅ DB tables created/verified")

        # Scheduler job setup
        scheduler.add_job(_cm30_job, "interval", minutes=1)
        scheduler.add_job(_bhavcopy_job, "cron", hour=18, minute=45)  
        scheduler.start()
        logger.info("✅ CM30 scheduler started (every 1 minute)")

        logger.info("🎉 Startup complete.")

    except Exception as e:
        logger.error("❌ Startup failed: %s", e, exc_info=True)
        raise

    # -------- app is running here --------
    try:
        yield
    finally:
        # Shutdown hooks
        try:
            if scheduler.running:
                scheduler.shutdown(wait=False)
                logger.info("🛑 Scheduler stopped")
        except Exception as e:
            logger.error(f"Error while stopping scheduler: {e}", exc_info=True)

        logger.info("🛑 Backend shutdown complete.")


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

app.mount("/api/v1/static", StaticFiles(directory="static"), name="static")


# Health check endpoint
@app.get("/health")
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

# Register all your existing routes
try:   
    app.include_router(Payment.router, prefix="/api/v1")
    app.include_router(plan.router, prefix="/api/v1")
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
    
    # Add other routes...
    
except Exception as e:
    logger.error(f"Failed to register routes: {e}")
    raise

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Global exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc),  # agar chaho to yahan prod me generic msg rakho
        },
    )


# Run the application
if __name__ == "__main__":
    logger.info("🚀 Starting server with Uvicorn...")
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8002,
        reload=True,
        log_level="info",
        reload_excludes=[
            "static/*",
            "vbc_token_cache/*",
            "logs/*",
        ],
    )
