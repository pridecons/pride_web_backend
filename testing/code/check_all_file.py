from sftp.NSE.sftp_client import SFTPClient

if __name__ == "__main__":
    sftp = SFTPClient()
    try:
        base_path = "/CM30/SECURITY"   # yaha NSE ne jo path diya hai vo daal
        dirs = sftp.list_files(base_path)
        print("Top level folders:")
        for d in dirs:
            print(d)
    finally:
        sftp.close()
