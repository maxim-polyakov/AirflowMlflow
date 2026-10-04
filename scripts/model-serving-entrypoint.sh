#!/usr/bin/env sh
set -eu

: "${MLFLOW_TRACKING_URI:?MLFLOW_TRACKING_URI is required}"
: "${MODEL_NAME:?MODEL_NAME is required}"

MODEL_ALIAS="${MODEL_ALIAS:-production}"
MODEL_SERVING_PORT="${MODEL_SERVING_PORT:-8000}"
MODEL_POLL_SECONDS="${MODEL_POLL_SECONDS:-60}"
MODEL_RUNTIME_ROOT="${MODEL_RUNTIME_ROOT:-/root/.mlflow/stormmodel}"

current_version=""
server_pid=""

mkdir -p "${MODEL_RUNTIME_ROOT}"

stop_server() {
  if [ -n "${server_pid}" ] && kill -0 "${server_pid}" 2>/dev/null; then
    kill "${server_pid}"
    wait "${server_pid}" || true
  fi
  server_pid=""
}

get_alias_version() {
  python - "${MODEL_NAME}" "${MODEL_ALIAS}" <<'PY'
import sys
from mlflow import MlflowClient

version = MlflowClient().get_model_version_by_alias(sys.argv[1], sys.argv[2])
print(version.version)
PY
}

start_server() {
  version="$1"
  version_root="${MODEL_RUNTIME_ROOT}/${version}"
  model_path_file="${version_root}/model-path"
  environment_path="${version_root}/venv"

  mkdir -p "${version_root}"
  if [ ! -f "${model_path_file}" ]; then
    model_path="$(
      python - "${MODEL_NAME}" "${version}" "${version_root}" <<'PY'
import sys
import mlflow

uri = f"models:/{sys.argv[1]}/{sys.argv[2]}"
print(mlflow.artifacts.download_artifacts(artifact_uri=uri, dst_path=sys.argv[3]))
PY
    )"
    virtualenv "${environment_path}"
    "${environment_path}/bin/pip" install \
      --disable-pip-version-check \
      "mlflow==2.18.0" \
      "sqlalchemy==2.0.36"
    if [ -f "${model_path}/requirements.txt" ]; then
      "${environment_path}/bin/pip" install \
        --disable-pip-version-check \
        -r "${model_path}/requirements.txt"
    fi
    # Keep pins compatible with serving Python 3.11 (mlflow deps can float higher).
    "${environment_path}/bin/pip" install \
      --disable-pip-version-check \
      "numpy>=1.26,<2.3" \
      "scikit-learn>=1.5,<1.7" \
      "pandas>=2.0,<3" \
      "joblib>=1.3,<2"
    "${environment_path}/bin/python" - <<'PY'
import numpy, sklearn, pandas, joblib, mlflow
print(
    "serving env:",
    "numpy", numpy.__version__,
    "sklearn", sklearn.__version__,
    "pandas", pandas.__version__,
    "joblib", joblib.__version__,
    "mlflow", mlflow.__version__,
)
PY
    printf '%s\n' "${model_path}" >"${model_path_file}"
  fi

  model_path="$(cat "${model_path_file}")"
  echo "Starting ${MODEL_NAME} version ${version} on port ${MODEL_SERVING_PORT}"
  if [ ! -x "${environment_path}/bin/python" ]; then
    echo "ERROR: broken venv at ${environment_path} (python missing); wiping"
    rm -rf "${version_root}"
    return 1
  fi
  # python -m: venv/bin/mlflow is sometimes missing after partial rebuilds
  "${environment_path}/bin/python" -m mlflow models serve \
    --model-uri "${model_path}" \
    --host 0.0.0.0 \
    --port "${MODEL_SERVING_PORT}" \
    --workers 1 \
    --env-manager local &
  server_pid="$!"
}

trap 'stop_server; exit 0' INT TERM

while true; do
  if version="$(get_alias_version 2>/dev/null)"; then
    if [ "${version}" != "${current_version}" ]; then
      stop_server
      start_server "${version}"
      current_version="${version}"
    elif [ -n "${server_pid}" ] && ! kill -0 "${server_pid}" 2>/dev/null; then
      echo "Model server stopped unexpectedly; restarting"
      current_version=""
      server_pid=""
    fi
  else
    echo "Waiting for models:/${MODEL_NAME}@${MODEL_ALIAS}"
  fi

  sleep "${MODEL_POLL_SECONDS}"
done
