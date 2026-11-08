import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s")

from src.connection_manager import SparkConnectionManager
from src.component_registry import AbInitioComponentRegistry
from src.writer import Writer
from schemas.DLY_C2T_EXTRACTS_INBOUND_B.dly_c2t_box_schemas import TASK1_OUTPUT

spark = SparkConnectionManager.get_session("Local_Validation_Test")

print("\n--- 1. Creating Mock Ab Initio Input Datasets ---")
# Emulating Output of SQL Query 1 (task1_input_table1.sql)
df_primary = spark.createDataFrame([
    (101, 5001, "A", "N", "2026-11-09"),
    (102, 5002, "P", "Y", "2026-11-09"),
    (103, 5003, "A", "N", "2026-11-09")
], ["OFFER_ID", "CAMPAIGN_ID", "REC_STATUS", "ERROR_FLAG", "LOAD_DATE"])
df_primary.createOrReplaceTempView("IN_TBL_PRIMARY")

# Emulating Output of SQL Query 2 (task1_input_table2.sql)
df_secondary = spark.createDataFrame([
    (101, "Diwali 20% Special Discount", 20.00),
    (102, "Expired Offer", 10.00),
    (103, "Winter Bonus Offer", 15.50)
], ["OFFER_ID", "OFFER_DESC", "DISCOUNT_VALUE"])
df_secondary.createOrReplaceTempView("IN_TBL_SECONDARY")

# Emulating Reference Table (REF_OWNER.TBL_CAMPAIGN_DIM)
df_campaign = spark.createDataFrame([
    (5001, "Festive Deals"),
    (5002, "Clearance"),
    (5003, "Prime Rewards")
], ["CAMPAIGN_ID", "CAMPAIGN_NAME"])
df_campaign.createOrReplaceTempView("REF_LOOKUP_CAMPAIGN")

print("--- 2. Executing Ab Initio Pipeline Steps ---")
# Step 1: Join
joined = AbInitioComponentRegistry.join(
    df_primary, spark,
    {"right_source_table": "IN_TBL_SECONDARY", "join_keys": ["OFFER_ID"], "join_type": "inner"},
    None, {}
)

# Step 2: Filter by Expression
filtered = AbInitioComponentRegistry.filter_by_expression(
    joined, spark,
    {"filter_condition": "REC_STATUS = 'A' AND ERROR_FLAG = 'N'"},
    None, {}
)

# Step 3: Reformat
reformatted = AbInitioComponentRegistry.reformat(
    filtered, spark,
    {
        "expressions": {
            "OFFER_ID": "OFFER_ID",
            "CAMPAIGN_ID": "CAMPAIGN_ID",
            "OFFER_DESC": "UPPER(OFFER_DESC)",
            "DISCOUNT_AMT": "CAST(DISCOUNT_VALUE AS DECIMAL(15, 2))",
            "BATCH_DATE": "current_date()",
            "PROCESSED_TIMESTAMP": "current_timestamp()"
        }
    },
    None, {}
)

# Step 4: Lookup (Broadcast Join)
final_output = AbInitioComponentRegistry.lookup(
    reformatted, spark,
    {"lookup_table": "REF_LOOKUP_CAMPAIGN", "join_keys": ["CAMPAIGN_ID"]},
    None, {}
)

print("\n--- 3. Final Pipeline Transformed Result ---")
final_output.show(truncate=False)

print("--- 4. Writing Output .dat and .ctl Files ---")
Writer.write_flat_file(
    df=final_output,
    target_schema=TASK1_OUTPUT,
    flat_params={"dat_file_env": "P_TASK1_DATFILE", "ctl_file_env": "P_TASK1_CTLFILE", "delimiter": "|"},
    runtime_vars={
        "P_TASK1_DATFILE": "./data/outbound/mcl_extr_task_1.dat",
        "P_TASK1_CTLFILE": "./data/outbound/mcl_extr_task_1.ctl"
    }
)

print("\n--- SUCCESS: Check the generated files in ./data/outbound/ ---")
SparkConnectionManager.stop_session()