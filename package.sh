#!/usr/bin/env bash
set -e
APP_NAME="App-1oms-spark-etl"
ZIP_NAME="${APP_NAME}.zip"

echo "=== Cleaning caches ==="
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true

echo "=== Packaging application archive: ${ZIP_NAME} ==="
zip -r "${ZIP_NAME}" src/ schemas/ jobs_metadata/ sql_pool/ -x "*.git*" "*__pycache__*" "*.pyc"
echo "=== Build Complete ==="
