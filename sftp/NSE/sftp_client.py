#sftp/NSE/sftp_client.py
import os
import paramiko
import random
from typing import List, Optional
from config import settings
import logging

logger = logging.getLogger(__name__)

class SFTPClient:
    """
    SFTP client with multiple host support and reconnection logic.
    """

    def __init__(self):
        self.hosts: List[str] = settings.SFTP_HOSTS
        self.port: int = settings.SFTP_PORT
        self.username: str = settings.SFTP_USER
        self.password: str = settings.SFTP_PASS
        self.key_path: str = settings.KEY_PATH
        self.transport: Optional[paramiko.Transport] = None
        self.client: Optional[paramiko.SFTPClient] = None
        self.current_host: Optional[str] = None

    def connect(self) -> None:
        """
        Establishes SFTP connection with failover support.
        """
        if self.client and self.transport and self.transport.is_active():
            return

        # Shuffle hosts for load balancing
        hosts = self.hosts.copy()
        random.shuffle(hosts)

        last_exception = None
        for host in hosts:
            try:
                logger.info(f"Attempting to connect to SFTP {host}:{self.port} as {self.username}")
                
                self.transport = paramiko.Transport((host, self.port))
                
                # Try key-based authentication first
                if os.path.exists(self.key_path):
                    private_key = paramiko.RSAKey.from_private_key_file(self.key_path)
                    self.transport.connect(username=self.username, pkey=private_key)
                elif self.password:
                    self.transport.connect(username=self.username, password=self.password)
                else:
                    raise ValueError("No authentication method available")
                
                self.client = paramiko.SFTPClient.from_transport(self.transport)
                self.current_host = host
                logger.info(f"SFTP connection established to {host}")
                return
                
            except Exception as e:
                logger.warning(f"Failed to connect to {host}: {e}")
                last_exception = e
                if self.transport:
                    try:
                        self.transport.close()
                    except:
                        pass
                self.transport = None
                self.client = None

        raise ConnectionError(f"Failed to connect to any SFTP server. Last error: {last_exception}")

    def list_files(self, remote_dir: str) -> List[str]:
        """
        List all entries in the remote directory.
        """
        self.connect()
        try:
            names = self.client.listdir(remote_dir)
            paths = [os.path.join(remote_dir, name).replace('\\', '/') for name in names]
            logger.debug(f"Listed {len(paths)} items in {remote_dir}")
            return paths
        except Exception as e:
            logger.error(f"Error listing {remote_dir}: {e}")
            raise

    def download_file(self, remote_path: str) -> bytes:
        """
        Download file and return raw bytes.
        """
        self.connect()
        try:
            logger.info(f"Downloading SFTP file: {remote_path}")
            with self.client.open(remote_path, 'rb') as rf:
                data = rf.read()
            logger.debug(f"Downloaded {len(data)} bytes from {remote_path}")
            return data
        except Exception as e:
            logger.error(f"Error downloading {remote_path}: {e}")
            raise

    def close(self) -> None:
        """
        Close SFTP connection.
        """
        if self.client:
            try:
                self.client.close()
            except:
                pass
        if self.transport:
            try:
                self.transport.close()
            except:
                pass
        logger.info("SFTP connection closed")