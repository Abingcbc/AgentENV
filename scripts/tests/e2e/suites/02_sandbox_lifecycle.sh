#!/usr/bin/env bash
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "02_lifecycle"

log "Suite: Sandbox Lifecycle"

# -- Create sandbox --
sandbox_id=$(create_sandbox); _sync_http
assert_status "$HTTP_STATUS" "201" "POST /sandboxes returns 201"
assert_not_empty "$sandbox_id" "sandboxID is present"
track_sandbox "$sandbox_id"

# -- Get sandbox --
api_get "/sandboxes/${sandbox_id}"
assert_status "$HTTP_STATUS" "200" "GET /sandboxes/{id} returns 200"
assert_json_field "$HTTP_BODY" '.state' "running" "sandbox state is running"

# -- List sandboxes --
api_get "/sandboxes"
assert_status "$HTTP_STATUS" "200" "GET /sandboxes returns 200"
assert_contains "$HTTP_BODY" "$sandbox_id" "sandbox appears in list"

# -- Pause sandbox --
api_post "/sandboxes/${sandbox_id}/pause"
assert_status "$HTTP_STATUS" "204" "POST /pause returns 204"
if wait_for_sandbox_state "$sandbox_id" "paused" 15; then
  _pass "sandbox state is paused"
else
  _fail "sandbox state is paused" "paused" "timeout"
fi

# -- Resume (connect) sandbox --
api_post "/sandboxes/${sandbox_id}/connect" '{"timeout":60}'
assert_status "$HTTP_STATUS" "201" "POST /connect returns 201"
if wait_for_sandbox_state "$sandbox_id" "running" 30; then
  _pass "sandbox state is running after connect"
else
  _fail "sandbox state is running after connect" "running" "timeout"
fi

# -- Delete sandbox --
api_delete "/sandboxes/${sandbox_id}"
assert_status "$HTTP_STATUS" "204" "DELETE /sandboxes/{id} returns 204"

# -- Get deleted sandbox returns 404 --
api_get "/sandboxes/${sandbox_id}"
assert_status "$HTTP_STATUS" "404" "GET deleted sandbox returns 404"

suite_summary "02_lifecycle"
