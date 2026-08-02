#!/bin/sh
set -eu

base_url="${BASE_URL:-http://localhost:8080}"
run_id="$(date +%s)-$$"
request_id="smoke-request-${run_id}"
near_driver_id="smoke-near-${run_id}"
far_driver_id="smoke-far-${run_id}"

wait_for_service() {
  attempts=0
  until curl -fsS "${base_url}/ping" >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "${attempts}" -ge 30 ]; then
      echo "Demo service did not become ready at ${base_url}" >&2
      exit 1
    fi
    sleep 1
  done
}

post_json() {
  path="$1"
  body="$2"
  curl -fsS \
    -H "Content-Type: application/json" \
    -X POST \
    -d "${body}" \
    "${base_url}${path}"
}

cleanup() {
  post_json "/driver/status" "{\"driver_id\":\"${near_driver_id}\",\"status\":\"offline\"}" >/dev/null 2>&1 || true
  post_json "/driver/status" "{\"driver_id\":\"${far_driver_id}\",\"status\":\"offline\"}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

wait_for_service

post_json "/driver/location" "{\"driver_id\":\"${near_driver_id}\",\"longitude\":-73.9857,\"latitude\":40.7484}" >/dev/null
post_json "/driver/location" "{\"driver_id\":\"${far_driver_id}\",\"longitude\":-73.9712,\"latitude\":40.7831}" >/dev/null
post_json "/driver/status" "{\"driver_id\":\"${near_driver_id}\",\"status\":\"available\"}" >/dev/null
post_json "/driver/status" "{\"driver_id\":\"${far_driver_id}\",\"status\":\"available\"}" >/dev/null

response="$(post_json "/dispatch/request" "{\"request_id\":\"${request_id}\",\"longitude\":-73.9855,\"latitude\":40.7486,\"radius_km\":10}")"

echo "${response}" | grep -q '"success":true'
echo "${response}" | grep -q "\"driver_id\":\"${near_driver_id}\""

echo "Demo smoke passed: ${response}"
