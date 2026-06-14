#!/bin/bash
set -euo pipefail

WOS_ROOT="${WOS_WORKSPACE_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
REMOTE_SCRIPTS="$WOS_ROOT/scripts/remote"
REMOTE_USER="${WOS_LINUX_USER:-user}"
REMOTE_DIR="/home/${REMOTE_USER}/.local/lib/wos-bench/showcase"
LAUNCHER=""
HOSTS_CSV=""
SCALE="quick"
OUTPUT_ROOT=""
OUTPUT=""
SUMMARY=""

usage() {
  echo "Usage: $0 --launcher HOST --hosts host0,host1 [--scale quick|full|stress] [--output-root PATH] [--output result.json] [--summary summary.tsv]" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --launcher)
      LAUNCHER="$2"
      shift 2
      ;;
    --hosts)
      HOSTS_CSV="$2"
      shift 2
      ;;
    --scale)
      SCALE="$2"
      shift 2
      ;;
    --output-root)
      OUTPUT_ROOT="$2"
      shift 2
      ;;
    --output)
      OUTPUT="$2"
      shift 2
      ;;
    --summary)
      SUMMARY="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$LAUNCHER" || -z "$HOSTS_CSV" ]]; then
  usage
  exit 1
fi

case "$SCALE" in
  quick)
    TCP_BYTES=$((1024 * 1024))
    FILE_BYTES=$((4 * 1024 * 1024))
    CPU_ROUNDS=20000
    ;;
  full)
    TCP_BYTES=$((16 * 1024 * 1024))
    FILE_BYTES=$((64 * 1024 * 1024))
    CPU_ROUNDS=200000
    ;;
  stress)
    TCP_BYTES=$((128 * 1024 * 1024))
    FILE_BYTES=$((256 * 1024 * 1024))
    CPU_ROUNDS=1000000
    ;;
  *)
    echo "invalid scale: $SCALE" >&2
    exit 1
    ;;
esac

IFS=',' read -r -a HOSTS <<< "$HOSTS_CSV"
if [[ ${#HOSTS[@]} -lt 1 ]]; then
  echo "ERROR: --hosts must name at least one Linux host" >&2
  exit 1
fi

RUN_ID="$(date +%Y%m%d-%H%M%S)"
if [[ -z "$OUTPUT_ROOT" ]]; then
  OUTPUT_ROOT="/var/lib/wos-bench/results/showcase/linux-showcase-${RUN_ID}"
fi
if [[ -z "$SUMMARY" ]]; then
  SUMMARY="benchmarks/results/linux/linux-showcase-${RUN_ID}.summary.tsv"
fi
if [[ -z "$OUTPUT" ]]; then
  OUTPUT="benchmarks/results/linux/linux-showcase-${RUN_ID}.json"
fi

LOG_DIR="${OUTPUT%.json}-logs"
mkdir -p "$LOG_DIR" "$(dirname "$SUMMARY")" "$(dirname "$OUTPUT")"
: > "$SUMMARY"

PASS=0
FAIL=0
SKIP=0

record_summary() {
  printf '%s\t%s\t%s\n' "$1" "$2" "$3" >> "$SUMMARY"
}

run_case() {
  local name="$1"
  shift
  local log="$LOG_DIR/${name}.log"
  printf '\n=== RUN %s ===\n' "$name"
  if "$@" > "$log" 2>&1; then
    cat "$log"
    PASS=$((PASS + 1))
    record_summary "$name" PASS "log=$log"
    printf 'PASS %s\n' "$name"
  else
    local rc="$?"
    if [[ "$rc" == "77" ]]; then
      cat "$log"
      SKIP=$((SKIP + 1))
      record_summary "$name" SKIP "log=$log"
      printf 'SKIP %s\n' "$name"
    else
      cat "$log"
      FAIL=$((FAIL + 1))
      record_summary "$name" FAIL "rc=$rc log=$log"
      printf 'FAIL %s rc=%s\n' "$name" "$rc"
    fi
  fi
}

remote_quote() {
  printf '%q' "$1"
}

remote_mkdirs() {
  local host
  for host in "${HOSTS[@]}"; do
    "${REMOTE_SCRIPTS}/linux_ssh.sh" "$host" mkdir -p "$REMOTE_DIR" "$OUTPUT_ROOT"
  done
}

stage_tcp_helper() {
  local host
  for host in "${HOSTS[@]}"; do
    "${REMOTE_SCRIPTS}/linux_scp.sh" "$WOS_ROOT/scripts/bench/linux_showcase_tcp.py" "${REMOTE_USER}@${host}:${REMOTE_DIR}/linux_showcase_tcp.py" || return
    "${REMOTE_SCRIPTS}/linux_ssh.sh" "$host" chmod 755 "${REMOTE_DIR}/linux_showcase_tcp.py" || return
  done
}

case_identity() {
  local host
  for host in "${HOSTS[@]}"; do
    printf '\n$ ssh %s hostname/uname/cpu\n' "$host"
    "${REMOTE_SCRIPTS}/linux_ssh.sh" "$host" sh -lc 'hostname; uname -sr; getconf _NPROCESSORS_ONLN' || return
  done
}

case_sftp_fanout() {
  local payload
  local host
  local remote_path
  local fetched
  payload="$(mktemp /tmp/wos-linux-showcase-payload.XXXXXX)"
  python3 - "$payload" "$FILE_BYTES" <<'PY' || return
from pathlib import Path
import sys

path = Path(sys.argv[1])
size = int(sys.argv[2])
block = b"wos-linux-showcase\n"
with path.open("wb") as out:
    remaining = size
    while remaining > 0:
        chunk = block[:remaining] if remaining < len(block) else block
        out.write(chunk)
        remaining -= len(chunk)
PY
  for host in "${HOSTS[@]}"; do
    remote_path="${OUTPUT_ROOT}/payload-${host//[^A-Za-z0-9._-]/_}.bin"
    printf '\n$ scp payload %s:%s\n' "$host" "$remote_path"
    "${REMOTE_SCRIPTS}/linux_scp.sh" "$payload" "${REMOTE_USER}@${host}:${remote_path}" || {
      rm -f "$payload"
      return 1
    }
    "${REMOTE_SCRIPTS}/linux_ssh.sh" "$host" wc -c "$remote_path" || {
      rm -f "$payload"
      return 1
    }
  done
  fetched="${LOG_DIR}/payload-roundtrip.bin"
  "${REMOTE_SCRIPTS}/linux_sftp_get.sh" "$LAUNCHER" "${OUTPUT_ROOT}/payload-${LAUNCHER//[^A-Za-z0-9._-]/_}.bin" "$fetched" || {
    rm -f "$payload"
    return 1
  }
  cmp "$payload" "$fetched"
  local rc="$?"
  rm -f "$payload"
  return "$rc"
}

case_tcp_echo() {
  if [[ ${#HOSTS[@]} -lt 2 ]]; then
    echo "need at least two Linux hosts for TCP cross-node echo"
    return 77
  fi
  local client="${HOSTS[0]}"
  local server="${HOSTS[1]}"
  local port="${WOS_LINUX_SHOWCASE_TCP_PORT:-19380}"
  local server_log="${OUTPUT_ROOT}/tcp-server.log"
  local helper="${REMOTE_DIR}/linux_showcase_tcp.py"
  local server_cmd
  local server_pid

  stage_tcp_helper || return
  printf -v server_cmd 'python3 %q server --port %q --sessions 1 > %q 2>&1 & echo $!' "$helper" "$port" "$server_log"
  server_pid="$("${REMOTE_SCRIPTS}/linux_ssh.sh" "$server" sh -lc "$server_cmd" | tr -d '\r\n')" || return
  sleep 1
  printf '\n$ ssh %s python3 helper client --host %s --port %s --bytes %s\n' "$client" "$server" "$port" "$TCP_BYTES"
  if ! "${REMOTE_SCRIPTS}/linux_ssh.sh" "$client" python3 "$helper" client --host "$server" --port "$port" --bytes "$TCP_BYTES"; then
    "${REMOTE_SCRIPTS}/linux_ssh.sh" "$server" sh -lc "kill $(remote_quote "$server_pid") >/dev/null 2>&1 || true" || true
    return 1
  fi
  sleep 1
  "${REMOTE_SCRIPTS}/linux_ssh.sh" "$server" sh -lc "kill -0 $(remote_quote "$server_pid") >/dev/null 2>&1 && kill $(remote_quote "$server_pid") >/dev/null 2>&1 || true"
  printf '\n--- server log from %s ---\n' "$server"
  "${REMOTE_SCRIPTS}/linux_ssh.sh" "$server" cat "$server_log" || return
}

case_python_cpu() {
  local host
  for host in "${HOSTS[@]}"; do
    printf '\n$ ssh %s python3 sha256 rounds=%s\n' "$host" "$CPU_ROUNDS"
    if ! "${REMOTE_SCRIPTS}/linux_ssh.sh" "$host" python3 - "$CPU_ROUNDS" <<'PY'
import hashlib
import json
import sys
import time

rounds = int(sys.argv[1])
payload = b"wos-showcase-linux-cpu"
started = time.monotonic()
digest = b""
for index in range(rounds):
    digest = hashlib.sha256(digest + payload + str(index).encode()).digest()
elapsed = time.monotonic() - started
print(json.dumps({"benchmark": "python_sha256", "rounds": rounds, "seconds": elapsed, "digest": digest.hex()[:16]}, sort_keys=True))
PY
    then
      return 1
    fi
  done
}

printf 'Linux showcase suite\n'
printf 'RUN_ID=%s\n' "$RUN_ID"
printf 'SCALE=%s\n' "$SCALE"
printf 'LAUNCHER=%s\n' "$LAUNCHER"
printf 'HOSTS=%s\n' "$HOSTS_CSV"
printf 'REMOTE_RESULT_DIR=%s\n' "$OUTPUT_ROOT"

remote_mkdirs
run_case identity case_identity
run_case sftp_fanout case_sftp_fanout
run_case tcp_echo case_tcp_echo
run_case python_cpu case_python_cpu

printf '\n=== LINUX SHOWCASE SUMMARY ===\n'
cat "$SUMMARY"
printf '\nPASS=%s FAIL=%s SKIP=%s\n' "$PASS" "$FAIL" "$SKIP"

python3 - "$OUTPUT" "$SUMMARY" "$LOG_DIR" "$SCALE" "$LAUNCHER" "$HOSTS_CSV" "$OUTPUT_ROOT" "$PASS" "$FAIL" "$SKIP" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

output = Path(sys.argv[1])
summary = Path(sys.argv[2])
log_dir = Path(sys.argv[3])
rows = []
for line in summary.read_text(encoding="utf-8").splitlines():
    if not line.strip():
        continue
    name, status, detail = (line.split("\t", 2) + ["", ""])[:3]
    rows.append({"name": name, "status": status, "detail": detail})

payload = {
    "benchmark": "showcase",
    "os": "linux",
    "scale": sys.argv[4],
    "launcher": sys.argv[5],
    "hosts": [item for item in sys.argv[6].split(",") if item],
    "remote_result_dir": sys.argv[7],
    "pass": int(sys.argv[8]),
    "fail": int(sys.argv[9]),
    "skip": int(sys.argv[10]),
    "summary_file": str(summary),
    "log_dir": str(log_dir),
    "cases": rows,
}
output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
print(json.dumps(payload, sort_keys=True))
PY

if [[ "$FAIL" -eq 0 ]]; then
  exit 0
fi
exit 1
