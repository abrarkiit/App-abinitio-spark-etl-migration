# App-abinitio-spark-etl-migration

Enterprise PySpark framework modernizing legacy Ab Initio Co>Operating System artifacts into modular Spark pipelines.

## Project Structure
- `scripts/`: Orchestration launchers for schedulers (Airflow, AutoSys, Control-M).
- `src/`: Generic platform engine:
  - `connection_manager.py`: SparkSession singleton.
  - `secrets_manager.py`: HashiCorp Vault AppRole integration.
  - `reader.py`: Dynamic Oracle JDBC & flat file loader.
  - `transformer.py`: SQL compiler with dynamic variable substitution.
  - `component_registry.py`: Ab Initio graph primitive emulator (Join, Reformat, Filter, Lookup, Rollup, Dedup Sort).
  - `writer.py`: Delimited `.dat` single-file generation & `.ctl` control file auditing.
  - `engine.py`: Dynamic JSON metadata task runner.
- `jobs_metadata/`: Declarative JSON blueprints (1:1 with legacy Ab Initio `.pset` files).
- `schemas/`: Strict `StructType` data contract replacements for `.dml`.
- `sql_pool/`: Oracle pushdown queries and SQL transformation logic.

## Setup & Local Run
```bash
python3 -m venv venv
source venv/bin/activate

Run Below command for Windows
.\venv\Scripts\Activate.ps1

pip install -r requirements.txt
python scripts/dly_c2t_extracts_inbound_b.py
```
