"""
src/reader.py
Generic source reader supporting Oracle pushdown JDBC SQL and file sources.
Includes integer boundary casting for partitioned Oracle JDBC reads.
"""

import os
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame

from src.transformer import Transformer

logger = logging.getLogger("ETL_FRAMEWORK")


class Reader:
    """Manages ingestion of database views and flat file sources."""

    def __init__(self, spark: SparkSession, db_credentials: Dict[str, Any], runtime_vars: Dict[str, Any]):
        self.spark = spark
        self.creds = db_credentials
        self.runtime_vars = runtime_vars

    def ingest_source(self, source_cfg: Dict[str, Any]) -> DataFrame:
        source_type = source_cfg["type"].lower()
        view_name = source_cfg["name"]

        logger.info("Ingesting source '%s' of type '%s'", view_name, source_type)

        if source_type == "oracle_sql":
            df = self._read_oracle_sql(source_cfg)
        elif source_type == "oracle_table":
            df = self._read_oracle_table(source_cfg)
        elif source_type == "file":
            df = self._read_flat_file(source_cfg)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

        df.createOrReplaceTempView(view_name)
        return df

    def _read_oracle_sql(self, source_cfg: Dict[str, Any]) -> DataFrame:
        sql_path = os.path.join(self.runtime_vars.get("PROJECT_ROOT", "."), source_cfg["sql_file_path"])
        if not os.path.exists(sql_path):
            raise FileNotFoundError(f"Oracle input SQL file not found at: {sql_path}")

        with open(sql_path, "r", encoding="utf-8") as f:
            raw_sql = f.read()

        compiled_sql = Transformer.compile_sql_text(raw_sql, self.runtime_vars)
        pushdown_query = f"({compiled_sql}) src_query"

        jdbc_reader = (
            self.spark.read.format("jdbc")
            .option("url", self.creds["url"])
            .option("dbtable", pushdown_query)
            .option("user", self.creds["user"])
            .option("password", self.creds["password"])
            .option("driver", self.creds["driver"])
            .option("fetchsize", "50000")
        )

        if "partition_col" in source_cfg and "num_partitions" in source_cfg:
            try:
                bounds_query = (
                    f"(SELECT CAST(FLOOR(MIN({source_cfg['partition_col']})) AS NUMBER(19)) AS MIN_V, "
                    f"CAST(CEIL(MAX({source_cfg['partition_col']})) AS NUMBER(19)) AS MAX_V "
                    f"FROM ({compiled_sql})) b_query"
                )
                bounds_df = (
                    self.spark.read.format("jdbc")
                    .option("url", self.creds["url"])
                    .option("dbtable", bounds_query)
                    .option("user", self.creds["user"])
                    .option("password", self.creds["password"])
                    .option("driver", self.creds["driver"])
                    .load()
                ).collect()

                raw_min = bounds_df[0]["MIN_V"]
                raw_max = bounds_df[0]["MAX_V"]

                if raw_min is not None and raw_max is not None:
                    # Explicit integer truncation avoids Spark NumberFormatException
                    min_val = int(float(raw_min))
                    max_val = int(float(raw_max))

                    jdbc_reader = (
                        jdbc_reader
                        .option("partitionColumn", source_cfg["partition_col"])
                        .option("lowerBound", str(min_val))
                        .option("upperBound", str(max_val))
                        .option("numPartitions", str(source_cfg["num_partitions"]))
                    )
            except Exception as exc:
                logger.warning("Could not auto-partition Oracle read: %s. Falling back to single stream.", str(exc))

        return jdbc_reader.load()

    def _read_oracle_table(self, source_cfg: Dict[str, Any]) -> DataFrame:
        return (
            self.spark.read.format("jdbc")
            .option("url", self.creds["url"])
            .option("dbtable", source_cfg["table"])
            .option("user", self.creds["user"])
            .option("password", self.creds["password"])
            .option("driver", self.creds["driver"])
            .load()
        )

    def _read_flat_file(self, source_cfg: Dict[str, Any]) -> DataFrame:
        env_var = source_cfg["file_path_env"]
        file_path = os.getenv(env_var, self.runtime_vars.get(env_var))
        if not file_path:
            raise ValueError(f"Environment variable '{env_var}' for flat file path is unset.")
        
        delimiter = source_cfg.get("delimiter", "|")
        return self.spark.read.option("delimiter", delimiter).option("header", "true").csv(file_path)