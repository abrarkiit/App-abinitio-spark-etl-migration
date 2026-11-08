"""
src/engine.py
Execution engine that runs multi-step component pipelines defined in metadata JSON.
"""

import os
import json
import logging
import importlib
from typing import Dict, Any, List
from pyspark.sql import SparkSession

from src.reader import Reader
from src.transformer import Transformer
from src.component_registry import AbInitioComponentRegistry
from src.writer import Writer

logger = logging.getLogger("ETL_FRAMEWORK")


class ExecutionEngine:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.runtime_vars = self._load_runtime_environment()
        self.transformer = Transformer(spark)
        self._secrets_mgr = None  # Lazily instantiated

    def _load_runtime_environment(self) -> Dict[str, Any]:
        """Loads execution environment and default runtime fallback variables."""
        return {
            "PROJECT_ROOT": os.getenv("PROJECT_ROOT", os.getcwd()),
            "BATCH_DATE": os.getenv("BATCH_DATE", "2026-11-09"),
            **dict(os.environ)
        }

    @property
    def secrets_mgr(self):
        """Lazy loader for VaultSecretsManager to avoid network timeouts during local/file-only runs."""
        if self._secrets_mgr is None:
            from src.secrets_manager import VaultSecretsManager
            self._secrets_mgr = VaultSecretsManager()
        return self._secrets_mgr

    def _resolve_schema(self, module_path: str, schema_attr: str):
        """Dynamically imports and resolves the target StructType contract."""
        try:
            mod = importlib.import_module(module_path)
            schema = getattr(mod, schema_attr)
            return schema
        except Exception as exc:
            raise ImportError(f"Cannot resolve schema contract '{schema_attr}' from '{module_path}': {str(exc)}")

    def run_task(self, metadata_json_path: str) -> None:
        """Parses declarative task JSON and executes ingestion, transformation, and load."""
        logger.info("=========================================================")
        logger.info("Starting execution of metadata task: %s", metadata_json_path)
        logger.info("=========================================================")

        if not os.path.exists(metadata_json_path):
            raise FileNotFoundError(f"Metadata configuration file missing: {metadata_json_path}")

        with open(metadata_json_path, "r", encoding="utf-8") as f:
            task_cfg = json.load(f)

        task_id = task_cfg["task_id"]
        box_job = task_cfg.get("box_job_name", "STANDALONE")
        logger.info("Task ID: %s | Box Job: %s", task_id, box_job)

        sources: List[Dict[str, Any]] = task_cfg.get("sources", [])
        pipeline_steps: List[Dict[str, Any]] = task_cfg.get("pipeline_steps", [])

        # -------------------------------------------------------------
        # 1. Credentials & Source Ingestion
        # -------------------------------------------------------------
        has_database_sources = any(
            str(src.get("type", "")).lower().startswith("oracle") 
            for src in sources
        )

        db_creds = {}
        if has_database_sources:
            vault_path = task_cfg.get("vault_secret_path", "secret/data/etl/oracle_c2t")
            logger.info("Database sources detected. Fetching credentials for path: %s", vault_path)
            db_creds = self.secrets_mgr.get_database_credentials(vault_path)
        else:
            logger.info("No database sources declared. Bypassing Vault credential retrieval.")

        reader = Reader(self.spark, db_creds, self.runtime_vars)
        for src in sources:
            reader.ingest_source(src)

        # -------------------------------------------------------------
        # 2. Pipeline Step Execution
        # -------------------------------------------------------------
        current_df = None
        created_views = []

        for step in pipeline_steps:
            step_id = step["step_id"]
            component_type = step["component"]
            in_view = step.get("source_table")
            out_view = step["target_table"]

            logger.info("Executing Step: [%s] | Component: %s", step_id, component_type)
            in_df = self.spark.table(in_view) if in_view else current_df

            if in_df is None:
                raise ValueError(f"Step '{step_id}' has no resolved input DataFrame or source_table.")

            current_df = AbInitioComponentRegistry.execute_step(
                component_name=component_type,
                df_in=in_df,
                spark=self.spark,
                step_cfg=step,
                transformer=self.transformer,
                runtime_vars=self.runtime_vars
            )

            # Optional persist control for heavy join/rollup boundaries
            if step.get("persist", False):
                logger.info("Persisting intermediate step output: %s", out_view)
                current_df = current_df.persist()

            current_df.createOrReplaceTempView(out_view)
            created_views.append(out_view)

        # -------------------------------------------------------------
        # 3. Contract Enforcement & Target Output
        # -------------------------------------------------------------
        contract_cfg = task_cfg["contract"]
        output_schema = self._resolve_schema(contract_cfg["schema_module"], contract_cfg["output_schema"])

        target_type = task_cfg.get("target_type", "file").lower()
        if target_type == "file":
            logger.info("Routing final dataset to flat file writer...")
            Writer.write_flat_file(
                df=current_df,
                target_schema=output_schema,
                flat_params=task_cfg["flat_file_params"],
                runtime_vars=self.runtime_vars
            )
        else:
            raise NotImplementedError(f"Target sink type '{target_type}' is not currently supported.")

        logger.info("Completed Task: %s successfully.\n", task_id)