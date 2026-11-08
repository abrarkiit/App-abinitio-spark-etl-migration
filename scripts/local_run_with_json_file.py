import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

from src.connection_manager import SparkConnectionManager
from src.engine import ExecutionEngine

def test_engine_with_json():
    spark = SparkConnectionManager.get_session("Local_JSON_Engine_Test")
    try:
        engine = ExecutionEngine(spark)
        # Runs through the exact production engine code
        engine.run_task("jobs_metadata/DLY_C2T_EXTRACTS_INBOUND_B/mcl_extr_task_1_local.json")
    finally:
        SparkConnectionManager.stop_session()

if __name__ == "__main__":
    test_engine_with_json()