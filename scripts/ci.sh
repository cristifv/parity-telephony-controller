#!/usr/bin/env bash
# parity-telephony-controller's local CI (PLAN.md step 0). Stops at the first failure.
#   1. tools: Java 21, Maven >= 3.9, docker;
#   2. parity-contracts' cross-language masking cases are there and parse;
#   3. our Error Prone checks (tools/errorprone-checks), installed into the local repository;
#   4. mvn verify: the enforcer (Java/Maven versions, banned and snapshot dependencies), javac
#      with -Xlint:all -Werror and Error Prone (TC ADR 0002 §8), unit tests, the jar, the runtime
#      SBOM, the integration tests (real processes and real containers), the licence allow-list;
#   5. no pinned virtual thread in any test output (R1-2), after proving the check catches one;
#   6. the logs (log-record.v1): what the tests captured (target/test-logs/*.ndjson, and the
#      controllers the integration tests ran, target/it-logs/) through parity-contracts'
#      contract-check with TC's catalogue (logging/events.parity-telephony-controller.v1.json),
#      after proving it refuses a record without a catalogued event;
#   7. advisories: OSV-Scanner on the SBOM (needs the network).
#   CI_OFFLINE=1          mvn --offline, and the advisory scan skipped (said so, not hidden)
#   PARITY_CONTRACTS_DIR  where parity-contracts is (default: ../parity-contracts); its
#                         contract-check binary (rust/target/debug/contract-check) must be built
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CONTRACTS=${PARITY_CONTRACTS_DIR:-$ROOT/../parity-contracts}
# OSV-Scanner v2.7.0 (2026-10-12, the newest at least 14 days old), by digest.
OSV_IMAGE=ghcr.io/google/osv-scanner@sha256:4f3b7e2c64b9d38a5febc5a7e4a3dfef1d1362f726dea7a2ef4762b46469fcd0
step() { printf '== %s\n' "$*"; }
die() { printf 'ci: %s\n' "$*" >&2; exit 3; }
mvn_flags=(-B -ntp)
# Offline, the SBOM plugin can't run (it requires online mode), so it is skipped with the scan.
if [[ "${CI_OFFLINE:-0}" == 1 ]]; then mvn_flags+=(--offline -Dcyclonedx.skip=true); fi

# Containers the tests leave behind (a killed run) carry this label; removed at start and exit.
TEST_LABEL=parity.test=parity-telephony-controller
cleanup() {
  local ids
  ids=$(docker ps -aq --filter "label=$TEST_LABEL" 2>/dev/null || true)
  if [[ -n "$ids" ]]; then
    echo "removing leftover test containers: $(echo $ids | tr '\n' ' ')"
    docker rm -f $ids >/dev/null
  fi
}

step "tools"
java_version=$(java -version 2>&1 | head -1)
[[ "$java_version" == *'"21.'* ]] || die "need Java 21, found: $java_version"
mvn_version=$(mvn -v 2>/dev/null | head -1)
[[ "$mvn_version" =~ Apache\ Maven\ 3\.(9|[1-9][0-9])\. ]] || die "need Maven >= 3.9, found: ${mvn_version:-none}"
command -v docker >/dev/null || die "docker is missing (the container tests need it)"
docker info >/dev/null 2>&1 || die "docker is not reachable"
echo "$java_version; $mvn_version; $(docker --version)"
cleanup
trap cleanup EXIT

step "parity-contracts masking cases"
cases="$CONTRACTS/masking/cases.v1.json"
[[ -f "$cases" ]] || die "missing $cases"
python3 -I -c 'import json,sys; d=json.load(open(sys.argv[1])); assert d["schema_version"] == 1' "$cases"
echo "$cases"

step "Error Prone checks (tools/errorprone-checks: NoDataInMessages, NoSynchronized)"
mvn "${mvn_flags[@]}" -f "$ROOT/tools/errorprone-checks/pom.xml" install

step "mvn verify"
cd "$ROOT"
# The tests' log capture (src/test/resources/logback-test.xml) and the controllers' own output
# (ProcessLifecycleIT, NodeLockContainerIT): only this run's, for the "logs" step.
rm -rf "$ROOT/target/test-logs" "$ROOT/target/it-logs"
mvn "${mvn_flags[@]}" -Dparity.contracts.dir="$CONTRACTS" verify

step "pinned virtual threads (R1-2)"
# The check's own test first: a probe that pins must be caught, or the check proves nothing.
rm -rf "$ROOT/target/pin-probe" && mkdir -p "$ROOT/target/pin-probe"
java -Djdk.tracePinnedThreads=short "$ROOT/tools/pin-probe/PinProbe.java" >"$ROOT/target/pin-probe/probe.log" 2>&1
if "$ROOT/scripts/check-pins.sh" "$ROOT/target/pin-probe" >/dev/null 2>&1; then
  die "check-pins.sh missed the probe's pinned thread"
fi
"$ROOT/scripts/check-pins.sh"

step "logs (log-record.v1)"
"$ROOT/scripts/check-logs.sh" "$CONTRACTS"

step "advisories (OSV-Scanner on target/bom.json)"
if [[ "${CI_OFFLINE:-0}" == 1 ]]; then
  echo "SKIPPED: CI_OFFLINE=1 (OSV-Scanner needs osv.dev)"
else
  [[ -f target/bom.json ]] || die "no SBOM at target/bom.json"
  # osv-scanner.toml: the exceptions, each with a reason and an expiry date (ignoreUntil).
  [[ -f "$ROOT/osv-scanner.toml" ]] || die "no osv-scanner.toml"
  docker run --rm --network bridge --security-opt label=disable --read-only --cap-drop ALL \
    -v "$ROOT/target/bom.json:/src/bom.json:ro" -v "$ROOT/osv-scanner.toml:/src/osv-scanner.toml:ro" \
    "$OSV_IMAGE" scan source --config /src/osv-scanner.toml -L /src/bom.json
fi

step "parity-telephony-controller ok"
