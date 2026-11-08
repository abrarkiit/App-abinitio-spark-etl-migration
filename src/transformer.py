"""
src/transformer.py
Compiles SQL queries with runtime parameters and runs in-memory Spark transformations.
Windows-safe string substitution avoiding regex backslash escape errors.
"""

import os
import re
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger("ETL_FRAMEWORK")


class Transformer:
    """Compiles SQL templates and dynamic transformation rules."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @staticmethod
    def compile_sql_text(raw_sql: str, params: Dict[str, Any]) -> str:
        """Substitutes ${PARAM} with corresponding runtime parameter values safely."""
        compiled = raw_sql
        for key, value in params.items():
            pattern = re.compile(rf"\${{\s*{re.escape(key)}\s*}}")
            # Using lambda prevents re.sub from treating Windows path backslashes (like \P) as escape codes
            val_str = str(value)
            compiled = pattern.sub(lambda _: val_str, compiled)
        return compiled

    def compile_and_run_sql(self, sql_file_path: str, params: Dict[str, Any]) -> DataFrame:
        """Reads SQL file, injects runtime variables, and executes via Spark SQL."""
        if not os.path.exists(sql_file_path):
            raise FileNotFoundError(f"SQL file not found at path: {sql_file_path}")

        with open(sql_file_path, "r", encoding="utf-8") as f:
            raw_sql = f.read()

        resolved_sql = self.compile_sql_text(raw_sql, params)
        logger.debug("Executing compiled SQL:\n%s", resolved_sql)
        return self.spark.sql(resolved_sql)