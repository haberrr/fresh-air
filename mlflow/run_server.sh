#!/usr/bin/env bash
set -e

mlflow server \
  --host 0.0.0.0 \
  --port 8000 \
  --backend-store-uri "${MLFLOW_DB_URI}" \
  --artifacts-destination "${DEFAULT_ARTIFACT_ROOT}"
