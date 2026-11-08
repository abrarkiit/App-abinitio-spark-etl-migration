"""
src/writer.py
Target sink writer enforcing schema contracts, delimiter formatting,
atomic single-file output, and automatic path resolution with .env fallback.
"""

import os
import shutil
import logging
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import functions as F

logger = logging.getLogger("ETL_FRAMEWORK")


class Writer:
    """Handles serialization and emission of transformed datasets."""

    @staticmethod
    def _resolve_output_path(
        param_key_env: str,
        param_key_direct: str,
        flat_params: Dict[str, Any],
        runtime_vars: Dict[str, Any],
        default_filename: str
    ) -> str:
        """
        Resolves target path with the following precedence:
        1. Environment Variable lookup (flat_params[param_key_env])
        2. Direct Path provided in JSON (flat_params[param_key_direct])
        3. Automatic Default: <OUTBOUND_DIR>/<default_filename>
        """
        # 1. Environment Variable Check
        env_var_name = flat_params.get(param_key_env)
        if env_var_name:
            resolved_from_env = runtime_vars.get(env_var_name) or os.getenv(env_var_name)
            if resolved_from_env:
                return os.path.abspath(resolved_from_env)

        # 2. Direct Path in JSON
        direct_path = flat_params.get(param_key_direct)
        if direct_path:
            return os.path.abspath(direct_path)

        # 3. Convention-based Default
        base_dir = (
            runtime_vars.get("OUTBOUND_DIR")
            or os.getenv("OUTBOUND_DIR")
            or os.path.join(runtime_vars.get("PROJECT_ROOT", "."), "data", "outbound")
        )
        return os.path.abspath(os.path.join(base_dir, default_filename))

    @classmethod
    def write_flat_file(
        cls,
        df: DataFrame,
        target_schema: StructType,
        flat_params: Dict[str, Any],
        runtime_vars: Dict[str, Any]
    ) -> None:
        """
        Enforces target schema contract, serializes dataset to delimited single flat file,
        and generates an Ab Initio/enterprise style audit control (.ctl) file.
        """
        task_id = flat_params.get("task_id", "dataset_output")
        delimiter = flat_params.get("delimiter", "|")

        # Resolve output target destinations
        target_dat_path = cls._resolve_output_path(
            param_key_env="dat_file_env",
            param_key_direct="dat_path",
            flat_params=flat_params,
            runtime_vars=runtime_vars,
            default_filename=f"{task_id}.dat"
        )

        target_ctl_path = cls._resolve_output_path(
            param_key_env="ctl_file_env",
            param_key_direct="ctl_path",
            flat_params=flat_params,
            runtime_vars=runtime_vars,
            default_filename=f"{task_id}.ctl"
        )

        # Ensure target directories exist
        os.makedirs(os.path.dirname(target_dat_path), exist_ok=True)
        os.makedirs(os.path.dirname(target_ctl_path), exist_ok=True)

        logger.info("Outbound .dat target resolved to: %s", target_dat_path)
        logger.info("Outbound .ctl target resolved to: %s", target_ctl_path)

        # 1. Enforce Schema Contract (Column Order & Types)
        select_exprs = []
        for field in target_schema.fields:
            col_name = field.name
            if col_name in df.columns:
                select_exprs.append(F.col(col_name).cast(field.dataType).alias(col_name))
            else:
                logger.warning("Field '%s' in target schema not found in DataFrame. Defaulting to NULL.", col_name)
                select_exprs.append(F.lit(None).cast(field.dataType).alias(col_name))

        contracted_df = df.select(*select_exprs)

        # 2. Write to temporary staging directory as single coalesced file
        temp_staging_dir = os.path.abspath(target_dat_path + "_tmp_staging")
        if os.path.exists(temp_staging_dir):
            shutil.rmtree(temp_staging_dir)

        logger.info("Serializing contracted records with delimiter '%s'...", delimiter)
        (
            contracted_df.coalesce(1)
            .write
            .mode("overwrite")
            .option("delimiter", delimiter)
            .option("header", "false")
            .option("nullValue", "")
            .option("emptyValue", "")
            .csv(temp_staging_dir)
        )

        # 3. Locate part file and move to atomic target .dat file
        part_file = None
        for filename in os.listdir(temp_staging_dir):
            if filename.startswith("part-") and filename.endswith(".csv"):
                part_file = os.path.join(temp_staging_dir, filename)
                break

        if not part_file:
            raise FileNotFoundError(f"Spark part file was not produced in: {temp_staging_dir}")

        if os.path.exists(target_dat_path):
            os.remove(target_dat_path)

        shutil.move(part_file, target_dat_path)
        shutil.rmtree(temp_staging_dir)

        # 4. Record Count and Outbound Audit Metrics
        record_count = contracted_df.count()
        file_size_bytes = os.path.getsize(target_dat_path)

        # 5. Write Control (.ctl) File
        batch_date = runtime_vars.get("BATCH_DATE", datetime.utcnow().strftime("%Y-%m-%d"))
        timestamp_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        ctl_content = (
            f"TASK_ID={task_id}\n"
            f"DATA_FILE={os.path.basename(target_dat_path)}\n"
            f"RECORD_COUNT={record_count}\n"
            f"FILE_SIZE_BYTES={file_size_bytes}\n"
            f"DELIMITER={delimiter}\n"
            f"BATCH_DATE={batch_date}\n"
            f"CREATED_TIMESTAMP={timestamp_str}\n"
            f"STATUS=SUCCESS\n"
        )

        with open(target_ctl_path, "w", encoding="utf-8") as ctl_f:
            ctl_f.write(ctl_content)

        logger.info("Successfully written %d records to '%s'", record_count, target_dat_path)
        logger.info("Control file generated at '%s'", target_ctl_path)