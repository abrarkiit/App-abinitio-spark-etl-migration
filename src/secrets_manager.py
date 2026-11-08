"""
src/secrets_manager.py
HashiCorp Vault client supporting both Token (local dev) and AppRole (enterprise),
with local environment variable fallback.
"""

import os
import logging
from typing import Dict, Any

logger = logging.getLogger("ETL_FRAMEWORK")


class VaultSecretsManager:
    def __init__(self):
        self.vault_addr = os.getenv("VAULT_ADDR")
        self.vault_token = os.getenv("VAULT_TOKEN")
        self.role_id = os.getenv("VAULT_ROLE_ID")
        self.secret_id = os.getenv("VAULT_SECRET_ID")
        self.mount_point = os.getenv("VAULT_MOUNT_POINT", "secret")
        self.client = None

        if self.vault_addr:
            try:
                import hvac
                self.client = hvac.Client(url=self.vault_addr)

                # Option A: Direct Token Auth (Local Dev UI login)
                if self.vault_token:
                    self.client.token = self.vault_token
                    if self.client.is_authenticated():
                        logger.info("Successfully authenticated to Vault using Token.")
                    else:
                        logger.warning("Vault token authentication failed.")
                        self.client = None

                # Option B: AppRole Auth (Production/Enterprise)
                elif self.role_id and self.secret_id:
                    self.client.auth.approle.login(role_id=self.role_id, secret_id=self.secret_id)
                    logger.info("Successfully authenticated to Vault using AppRole.")
                else:
                    logger.warning("No Vault Token or AppRole provided. Using local fallback.")
                    self.client = None
            except Exception as e:
                logger.warning("Vault connection failed: %s. Using local fallback.", str(e))
                self.client = None
        else:
            logger.info("VAULT_ADDR not configured. Running in local fallback mode.")

    def get_database_credentials(self, secret_path: str) -> Dict[str, Any]:
        """Fetches DB credentials from Vault KV or falls back to local environment variables."""
        if self.client and self.client.is_authenticated():
            try:
                # Strip prefix if metadata path was passed
                clean_path = secret_path.replace("secret/data/", "").replace("secret/", "")
                logger.info("Reading secret from Vault mount '%s' at path '%s'", self.mount_point, clean_path)

                secret_resp = self.client.secrets.kv.v2.read_secret_version(
                    path=clean_path,
                    mount_point=self.mount_point
                )
                data = secret_resp["data"]["data"]
                logger.info("Retrieved credentials from Vault successfully for user '%s'", data.get("user"))
                return {
                    "url": data["url"],
                    "user": data["user"],
                    "password": data["password"],
                    "driver": data.get("driver", "oracle.jdbc.OracleDriver")
                }
            except Exception as e:
                logger.warning("Failed to fetch from Vault (%s). Using local env fallback.", str(e))

        logger.info("Using local environment variable credentials for Oracle.")
        return {
            "url": os.getenv("ORACLE_JDBC_URL", "jdbc:oracle:thin:@//localhost:1521/XEPDB1"),
            "user": os.getenv("ORACLE_DB_USER", "system"),
            "password": os.getenv("ORACLE_DB_PASSWORD", "system"),
            "driver": os.getenv("ORACLE_JDBC_DRIVER", "oracle.jdbc.OracleDriver")
        }