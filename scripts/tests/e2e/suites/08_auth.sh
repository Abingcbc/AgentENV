#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "08_auth"

log "Suite: Authentication"

# -- Request without auth header returns 401 --
api_get_no_auth "/sandboxes"
assert_status "$HTTP_STATUS" "401" "no auth header returns 401"

# -- Request with valid API key succeeds --
api_get "/sandboxes"
assert_not_eq "$HTTP_STATUS" "401" "valid API key does not return 401"

# -- Health endpoint works without auth --
api_get_no_auth "/health"
assert_status "$HTTP_STATUS" "204" "/health works without auth"

suite_summary "08_auth"
