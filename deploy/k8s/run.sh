#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <render|apply|delete> [kubectl args...]" >&2
  exit 1
fi

MODE="$1"
shift
KUBECTL_BIN="${KUBECTL:-kubectl}"
OVERLAY_NAME="${K8S_OVERLAY:-default}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TEMP_DIR}"' EXIT

sed_in_place() {
  local expression="$1"
  local file="$2"
  if sed --version >/dev/null 2>&1; then
    sed -i "${expression}" "${file}"
  else
    sed -i '' "${expression}" "${file}"
  fi
}

cp -R "${SCRIPT_DIR}" "${TEMP_DIR}/k8s"
cp "${REPO_ROOT}/config/default.toml" "${TEMP_DIR}/k8s/base/config/agentenv.toml"

if [[ "${SANDBOX_PROXY_DOMAINS+x}" == "x" ]]; then
  ESCAPED_SANDBOX_PROXY_DOMAINS="${SANDBOX_PROXY_DOMAINS//\\/\\\\}"
  ESCAPED_SANDBOX_PROXY_DOMAINS="${ESCAPED_SANDBOX_PROXY_DOMAINS//&/\\&}"
  ESCAPED_SANDBOX_PROXY_DOMAINS="${ESCAPED_SANDBOX_PROXY_DOMAINS//#/\\#}"
  sed_in_place "s#- SANDBOX_PROXY_DOMAINS=.*#- SANDBOX_PROXY_DOMAINS=${ESCAPED_SANDBOX_PROXY_DOMAINS}#" "${TEMP_DIR}/k8s/base/kustomization.yaml"
fi

OVERLAY_PATH="${TEMP_DIR}/k8s/overlays/${OVERLAY_NAME}"
if [[ ! -d "${OVERLAY_PATH}" ]]; then
  echo "unknown overlay: ${OVERLAY_NAME}" >&2
  exit 1
fi

if [[ "${OVERLAY_NAME}" == "local-dev" ]]; then
  REPO_ENV_PATH="${AENV_LOCAL_REPO_ENV_PATH:-${REPO_ROOT}/env}"
  if [[ ! -d "${REPO_ENV_PATH}" ]]; then
    echo "local-dev overlay requires a readable env directory at ${REPO_ENV_PATH}" >&2
    exit 1
  fi

  ESCAPED_REPO_ENV_PATH="${REPO_ENV_PATH//\\/\\\\}"
  ESCAPED_REPO_ENV_PATH="${ESCAPED_REPO_ENV_PATH//&/\\&}"
  sed_in_place "s#path: \"\"#path: \"${ESCAPED_REPO_ENV_PATH}\"#" "${OVERLAY_PATH}/kustomization.yaml"

  if grep -q 'path: ""' "${OVERLAY_PATH}/kustomization.yaml"; then
    echo "failed to render local-dev repo env hostPath; path is still empty" >&2
    exit 1
  fi
fi

case "${MODE}" in
  render)
    "${KUBECTL_BIN}" kustomize "${OVERLAY_PATH}" "$@"
    ;;
  apply)
    "${KUBECTL_BIN}" apply -k "${OVERLAY_PATH}" "$@"
    ;;
  delete)
    "${KUBECTL_BIN}" delete --ignore-not-found -k "${OVERLAY_PATH}" "$@"
    ;;
  *)
    echo "unsupported mode: ${MODE}" >&2
    exit 1
    ;;
esac
