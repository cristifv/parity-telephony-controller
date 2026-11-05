#!/usr/bin/env bash
# The logs TC's tests made, checked as parity-contracts' log-record.v1 (CONVENTIONS "Logging"):
# the schema, TC's catalogue (logging/events.parity-telephony-controller.v1.json) and the common
# one, masking, the xsid on call-scoped events, seq. Inputs: target/test-logs/*.ndjson (the test
# JVMs' capture, src/test/resources/logback-test.xml) and TC's own lines in target/it-logs/*.log
# (the controllers the integration tests ran, through the production logback-spring.xml).
# First a probe: a record of TC's own code without an event must be refused, or the check proves
# nothing. Argument: the parity-contracts directory (default: ../parity-contracts).
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CONTRACTS=${1:-${PARITY_CONTRACTS_DIR:-$ROOT/../parity-contracts}}
CC="$CONTRACTS/rust/target/debug/contract-check"
CATALOGUE="$ROOT/logging/events.parity-telephony-controller.v1.json"
die() { printf 'check-logs: %s\n' "$*" >&2; exit 3; }
[[ -x "$CC" ]] || die "contract-check is missing at $CC (build it: cd $CONTRACTS/rust && cargo build -p contract-check)"
[[ -f "$CATALOGUE" ]] || die "no catalogue at $CATALOGUE"

probe="$ROOT/target/log-probe"
rm -rf "$probe" && mkdir -p "$probe"
printf '%s\n' '{"ts":"2026-11-03T10:00:00.000Z","level":"info","service":"parity-telephony-controller","version":"0.1.0","instance":"probe","incarnation":"0192f6a1-7c3e-7b21-9a4d-5e6f70819203","seq":1,"event":"log.unclassified","msg":"probe"}' \
  >"$probe/probe.ndjson"
if "$CC" --root "$CONTRACTS" --catalogue "$CATALOGUE" --logs "$probe/probe.ndjson" >"$probe/out.txt" 2>&1; then
  die "contract-check let a record without a catalogued event through"
fi

logs="$ROOT/target/test-logs"
mkdir -p "$logs"
# A controller process's output: its log-record.v1 lines only (the JVM's or a failed start's
# plain text is skipped, as --skip-non-json would).
if [[ -d "$ROOT/target/it-logs" ]]; then
  for f in "$ROOT"/target/it-logs/*.log; do
    [[ -f "$f" ]] || continue
    out="$logs/it-$(basename "$f" .log).ndjson"
    grep -h '^{.*"service":"parity-telephony-controller"' "$f" >"$out" || rm -f "$out"
  done
fi
mapfile -t captured < <(find "$logs" -maxdepth 1 -name '*.ndjson' -type f -size +0 | sort)
(( ${#captured[@]} >= 1 )) || die "no captured logs in $logs (run the tests first)"
args=()
for f in "${captured[@]}"; do args+=(--logs "$f"); done
lines=$(cat "${captured[@]}" | grep -c '^{' || true)
echo "check-logs: ${#captured[@]} files, $lines records"
"$CC" --root "$CONTRACTS" --catalogue "$CATALOGUE" "${args[@]}" --skip-non-json
