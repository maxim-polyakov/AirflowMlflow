#!/usr/bin/env sh
set -eu

: "${MLFLOW_TRACKING_URI:?MLFLOW_TRACKING_URI is required}"
: "${MODEL_NAME:?MODEL_NAME is required}"

MODEL_ALIAS="${MODEL_ALIAS:-production}"
MODEL_SERVING_PORT="${MODEL_SERVING_PORT:-8000}"
MODEL_POLL_SECONDS="${MODEL_POLL_SECONDS:-60}"
MODEL_RUNTIME_ROOT="${MODEL_RUNTIME_ROOT:-/root/.mlflow/stormmodel}"

# Serving image is Python 3.11 — keep ABI-safe pins even if model was logged on 3.12.
PIN_NUMPY="numpy>=1.26,<2.3"
PIN_SKLEARN="scikit-learn>=1.5,<1.7"
PIN_PANDAS="pandas>=2.0,<3"
PIN_JOBLIB="joblib>=1.3,<2"
PIN_MARKER="pins-v2-numpyLT23-sklearnLT17"

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

env_pins_ok() {
  environment_path="$1"
  [ -x "${environment_path}/bin/python" ] || return 1
  "${environment_path}/bin/python" - <<'PY'
import sys
try:
    import numpy, sklearn
except Exception as exc:
    print("pin-check import failed:", exc)
    sys.exit(1)

def parse(v):
    return tuple(int(x) for x in v.split(".")[:3])

np_v = parse(numpy.__version__)
sk_v = parse(sklearn.__version__)
ok = (np_v < (2, 3, 0) and np_v >= (1, 26, 0) and sk_v < (1, 7, 0) and sk_v >= (1, 5, 0))
print("pin-check:", "numpy", numpy.__version__, "sklearn", sklearn.__version__, "ok" if ok else "BAD")
sys.exit(0 if ok else 1)
PY
}

write_constraints() {
  constraints_file="$1"
  cat >"${constraints_file}" <<EOF
${PIN_NUMPY}
${PIN_SKLEARN}
${PIN_PANDAS}
${PIN_JOBLIB}
EOF
}

build_env() {
  version="$1"
  version_root="${MODEL_RUNTIME_ROOT}/${version}"
  model_path_file="${version_root}/model-path"
  environment_path="${version_root}/venv"
  marker_file="${version_root}/${PIN_MARKER}"
  constraints_file="${version_root}/constraints.txt"

  rm -rf "${environment_path}"
  mkdir -p "${version_root}"
  write_constraints "${constraints_file}"

  if [ ! -f "${model_path_file}" ]; then
    model_path="$(
      python - "${MODEL_NAME}" "${version}" "${version_root}" <<'PY'
import sys
import mlflow

uri = f"models:/{sys.argv[1]}/{sys.argv[2]}"
print(mlflow.artifacts.download_artifacts(artifact_uri=uri, dst_path=sys.argv[3]))
PY
    )"
    printf '%s\n' "${model_path}" >"${model_path_file}"
  else
    model_path="$(cat "${model_path_file}")"
  fi

  echo "Building venv for ${MODEL_NAME} v${version} with pinned deps"
  virtualenv "${environment_path}"
  "${environment_path}/bin/pip" install \
    --disable-pip-version-check \
    "mlflow==2.18.0" \
    "sqlalchemy==2.0.36"

  if [ -f "${model_path}/requirements.txt" ]; then
    # Constraints force numpy/sklearn down even if requirements.txt floats.
    "${environment_path}/bin/pip" install \
      --disable-pip-version-check \
      --constraint "${constraints_file}" \
      -r "${model_path}/requirements.txt"
  fi

  # Final authority: force the serving pins after everything else.
  "${environment_path}/bin/pip" install \
    --disable-pip-version-check \
    --force-reinstall \
    --no-deps \
    "${PIN_NUMPY}" \
    "${PIN_SKLEARN}"
  "${environment_path}/bin/pip" install \
    --disable-pip-version-check \
    "${PIN_PANDAS}" \
    "${PIN_JOBLIB}"

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

  if ! env_pins_ok "${environment_path}"; then
    echo "ERROR: pins still wrong after rebuild"
    return 1
  fi
  : >"${marker_file}"
}

ensure_env() {
  version="$1"
  version_root="${MODEL_RUNTIME_ROOT}/${version}"
  environment_path="${version_root}/venv"
  marker_file="${version_root}/${PIN_MARKER}"

  mkdir -p "${version_root}"

  if [ -f "${marker_file}" ] && env_pins_ok "${environment_path}"; then
    echo "Reusing venv for ${MODEL_NAME} v${version} (pins ok)"
    return 0
  fi

  echo "Venv missing or pins stale for ${MODEL_NAME} v${version}; rebuilding"
  build_env "${version}"
}

start_server() {
  version="$1"
  version_root="${MODEL_RUNTIME_ROOT}/${version}"
  model_path_file="${version_root}/model-path"
  environment_path="${version_root}/venv"

  ensure_env "${version}" || return 1

  model_path="$(cat "${model_path_file}")"
  echo "Starting ${MODEL_NAME} version ${version} on port ${MODEL_SERVING_PORT}"

  if [ ! -x "${environment_path}/bin/python" ]; then
    echo "ERROR: broken venv at ${environment_path}; wiping"
    rm -rf "${version_root}/venv" "${version_root}/${PIN_MARKER}"
    return 1
  fi

  # Last chance downgrade before serve (cached venv from older entrypoint).
  "${environment_path}/bin/pip" install \
    --disable-pip-version-check \
    --force-reinstall \
    --no-deps \
    "${PIN_NUMPY}" \
    "${PIN_SKLEARN}" >/dev/null
  "${environment_path}/bin/python" -c "import numpy,sklearn; print('pre-serve pins:', numpy.__version__, sklearn.__version__)"

  if ! env_pins_ok "${environment_path}"; then
    echo "ERROR: pins bad right before serve; wiping venv"
    rm -rf "${environment_path}" "${version_root}/${PIN_MARKER}"
    return 1
  fi

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
      if start_server "${version}"; then
        current_version="${version}"
      else
        echo "Failed to start ${MODEL_NAME} v${version}; will retry"
        current_version=""
        server_pid=""
      fi
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
