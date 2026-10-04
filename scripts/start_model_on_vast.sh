#!/usr/bin/env bash
set -euo pipefail

: "${MLFLOW_TRACKING_URI:?Set MLFLOW_TRACKING_URI to the public MLflow URL}"

MODEL_URI="${MODEL_URI:-models:/stormmodel@production}"
MODEL_PORT="${MODEL_PORT:-8000}"
LOG_FILE="${LOG_FILE:-/workspace/model-server.log}"
PID_FILE="${PID_FILE:-/workspace/model-server.pid}"
MODEL_ROOT="${MODEL_ROOT:-/workspace/models}"

python -m pip install --disable-pip-version-check "mlflow==2.18.0"

mkdir -p "${MODEL_ROOT}"
MODEL_PATH="$(
  python - "${MODEL_URI}" "${MODEL_ROOT}" <<'PY'
import sys
import mlflow

print(mlflow.artifacts.download_artifacts(artifact_uri=sys.argv[1], dst_path=sys.argv[2]))
PY
)"

if [[ -f "${MODEL_PATH}/requirements.txt" ]]; then
  python -m pip install --disable-pip-version-check -r "${MODEL_PATH}/requirements.txt"
fi

nohup mlflow models serve \
  --model-uri "${MODEL_PATH}" \
  --host 0.0.0.0 \
  --port "${MODEL_PORT}" \
  --no-conda \
  >"${LOG_FILE}" 2>&1 &

echo "$!" >"${PID_FILE}"
echo "Model server started on port ${MODEL_PORT}; PID $(cat "${PID_FILE}")"
echo "Logs: ${LOG_FILE}"

