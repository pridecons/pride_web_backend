from dotenv import load_dotenv
import os
from pydantic_settings import BaseSettings
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "crm_db")
DB_USERNAME = os.getenv("DB_USERNAME", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

CF_R2_ACCESS_KEY_ID=os.getenv("CF_R2_ACCESS_KEY_ID")
CF_R2_SECRET_ACCESS_KEY=os.getenv("CF_R2_SECRET_ACCESS_KEY")
CF_R2_ACCOUNT_ID=os.getenv("CF_R2_ACCOUNT_ID")
CF_R2_REGION=os.getenv("CF_R2_REGION")
BUCKET_NAME="pride-web"

PAN_API_KEY=os.getenv("PAN_API_KEY")
PAN_API_ID=os.getenv("PAN_API_ID")
PAN_TASK_ID_1=os.getenv("PAN_TASK_ID_1")

NEWS_AI_API=os.getenv("NEWS_AI_API")

CASHFREE_APP_ID=os.getenv("CASHFREE_APP_ID")
CASHFREE_SECRET_KEY=os.getenv("CASHFREE_SECRET_KEY")
RAPID_INDIAN_STOCK_EXCHANGE=os.getenv("RAPID_INDIAN_STOCK_EXCHANGE")

ANGEL_API_KEY= "1q65DSNZ"
ANGEL_SECRET_KEY = "a9961bd7-e2ac-4ebb-8abf-cd4c40a4ddd8"
ANGEL_CLIENT_ID = "D403666"
ANGEL_TOTP_KEY="FE6FEUP3HYZFV6OC5S73OFCQUI"
ANGEL_CLIENT_PIN=2580

class Settings(BaseSettings):
    # SFTP configuration
    # SFTP_HOSTS: list[str] = ['snapshotsftp1.nseindia.com', 'snapshotsftp2.nseindia.com']
    SFTP_HOSTS: list[str] = ['snapshotsftp2.nseindia.com']
    SFTP_PORT: int = 6010
    SFTP_USER: str = 'PTCPL_15MINCM'
    SFTP_PASS: str = ''
    SFTP_REMOTE_PATH: str = "/CM30"
    KEY_PATH: str = os.path.join(os.path.dirname(__file__), 'ssh', 'pride_sftp_key')

    # Polling interval for SFTP watcher (in seconds)
    POLL_INTERVAL_SECONDS: int = 60

settings = Settings()