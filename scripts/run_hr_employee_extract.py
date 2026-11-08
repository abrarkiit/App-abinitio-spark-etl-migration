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


def run():
    spark = SparkConnectionManager.get_session("HR_Employee_Extract")
    try:
        engine = ExecutionEngine(spark)
        engine.run_task("jobs_metadata/DLY_HR_ANALYTICS_EXTRACTS_B/mcl_extr_hr_employee.json")
    finally:
        SparkConnectionManager.stop_session()


if __name__ == "__main__":
    run()