"""
scripts/dly_c2t_extracts_inbound_b.py
Entry point launcher for Box Job: DLY_C2T_EXTRACTS_INBOUND_B
"""

import sys
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("DLY_C2T_EXTRACTS_INBOUND_B")

from src.connection_manager import SparkConnectionManager
from src.engine import ExecutionEngine


def main():
    logger.info("Initializing execution for Box Job: DLY_C2T_EXTRACTS_INBOUND_B")
    spark = SparkConnectionManager.get_session("BoxJob_DLY_C2T_EXTRACTS_INBOUND_B")

    try:
        engine = ExecutionEngine(spark)

        logger.info(">>> Launching Task 1: mcl_extr_task_1")
        engine.run_task("jobs_metadata/DLY_C2T_EXTRACTS_INBOUND_B/mcl_extr_task_1.json")

        logger.info(">>> Launching Task 2: mcl_extr_task_2")
        engine.run_task("jobs_metadata/DLY_C2T_EXTRACTS_INBOUND_B/mcl_extr_task_2.json")

        logger.info("Box Job completed successfully.")

    except Exception as exc:
        logger.critical("Box Job failed during execution: %s", str(exc), exc_info=True)
        sys.exit(1)
    finally:
        SparkConnectionManager.stop_session()


if __name__ == "__main__":
    main()
