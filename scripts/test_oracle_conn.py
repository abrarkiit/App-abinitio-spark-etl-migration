import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv()

from src.connection_manager import SparkConnectionManager

spark = SparkConnectionManager.get_session("Oracle_Ping_Test")

url = os.getenv("ORACLE_JDBC_URL")
user = os.getenv("ORACLE_DB_USER")
password = os.getenv("ORACLE_DB_PASSWORD")
driver = os.getenv("ORACLE_JDBC_DRIVER", "oracle.jdbc.OracleDriver")

print(f"Connecting to: {url} with user: {user}...")

try:
    # Query DUAL table to verify JDBC handshake
    df = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", "(SELECT 'Oracle Connection Successful!' AS STATUS, SYSDATE FROM DUAL) test_db")
        .option("user", user)
        .option("password", password)
        .option("driver", driver)
        .load()
    )
    df.show(truncate=False)
    print("SUCCESS: Local Oracle is connected to PySpark!")
except Exception as e:
    print(f"\nFAILED: {e}")
finally:
    SparkConnectionManager.stop_session()