# testing/code/debug_securities.py

from datetime import date
import os

from sftp.NSE.sftp_client import SFTPClient
from utils.NSE_Formater.bhavcopy_ingestor import process_cm_bhavcopy_for_date
from utils.NSE_Formater.data_ingestor import process_cm30_for_date, process_cm30_security_for_date

if __name__ == "__main__":
    # yahan wahi date use karo jahan tumne path dekha tha
    process_cm30_for_date(date(2025, 11, 24))
