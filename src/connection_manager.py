"""
src/connection_manager.py
Thread-safe SparkSession singleton with automated Windows 8.3 short-path resolution.
"""

import os
import sys
import ctypes
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger("ETL_FRAMEWORK")


def _get_short_path(long_path: str) -> str:
    """Converts a Windows path with spaces into an 8.3 short path without spaces."""
    if os.name != "nt" or not os.path.exists(long_path):
        return long_path
    buffer = ctypes.create_unicode_buffer(500)
    res = ctypes.windll.kernel32.GetShortPathNameW(long_path, buffer, 500)
    return buffer.value if res > 0 else long_path


class SparkConnectionManager:
    _instance = None

    @classmethod
    def get_session(cls, app_name: str = "AbInitio_to_PySpark_Migration") -> SparkSession:
        if cls._instance is None:
            logger.info("Initializing SparkSession: %s", app_name)

            # 1. Resolve Python executable to 8.3 short path
            py_path = _get_short_path(sys.executable)
            os.environ["PYSPARK_PYTHON"] = py_path
            os.environ["PYSPARK_DRIVER_PYTHON"] = py_path

            # 2. Configure Windows HADOOP_HOME dynamically using 8.3 short path
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            local_hadoop = _get_short_path(os.path.join(project_root, "hadoop"))

            os.environ["HADOOP_HOME"] = local_hadoop
            hadoop_bin = os.path.join(local_hadoop, "bin")
            if hadoop_bin not in os.environ.get("PATH", ""):
                os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

            # 3. Base SparkSession Builder
            builder = (
                SparkSession.builder
                .appName(app_name)
                .master("local[*]")
                .config("spark.driver.host", "localhost")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.pyspark.python", py_path)
                .config("spark.pyspark.driver.python", py_path)
            )

            # 4. Use local JAR to prevent Windows Temp-locking
            local_jar = os.path.join(project_root, "jars", "ojdbc8.jar")
            if os.path.exists(local_jar):
                short_jar = _get_short_path(local_jar)
                builder = (
                    builder
                    .config("spark.jars", short_jar)
                    .config("spark.driver.extraClassPath", short_jar)
                    .config("spark.executor.extraClassPath", short_jar)
                )
            else:
                builder = builder.config("spark.jars.packages", "com.oracle.database.jdbc:ojdbc8:21.9.0.0")

            cls._instance = builder.getOrCreate()
            cls._instance.sparkContext.setLogLevel("ERROR")

        return cls._instance

    @classmethod
    def stop_session(cls) -> None:
        """Stops the SparkSession gracefully."""
        if cls._instance is not None:
            logger.info("Stopping active SparkSession.")
            try:
                cls._instance.stop()
            except Exception:
                pass
            cls._instance = None