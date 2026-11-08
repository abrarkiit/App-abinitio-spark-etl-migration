"""
schemas/DLY_C2T_EXTRACTS_INBOUND_B/dly_c2t_box_schemas.py
Consolidated DML schema replacements for Box Job: DLY_C2T_EXTRACTS_INBOUND_B
Defines explicit PySpark StructTypes for Task 1 and Task 2.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    DecimalType,
    TimestampType
)

TASK1_OUTPUT = StructType([
    StructField("OFFER_ID", LongType(), nullable=False),
    StructField("CAMPAIGN_NAME", StringType(), nullable=True),
    StructField("OFFER_DESC", StringType(), nullable=True),
    StructField("DISCOUNT_AMT", DecimalType(15, 2), nullable=True),
    StructField("BATCH_DATE", StringType(), nullable=False),
    StructField("PROCESSED_TIMESTAMP", TimestampType(), nullable=False)
])

TASK2_OUTPUT = StructType([
    StructField("RECOMMENDATION_ID", LongType(), nullable=False),
    StructField("OFFER_ID", LongType(), nullable=False),
    StructField("SEGMENT_NAME", StringType(), nullable=True),
    StructField("RANKING_SCORE", IntegerType(), nullable=True),
    StructField("BATCH_DATE", StringType(), nullable=False),
    StructField("PROCESSED_TIMESTAMP", TimestampType(), nullable=False)
])
