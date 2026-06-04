#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: setup_host_kvm_trace_caps.sh [--dry-run] [--perf PATH] [--trace-cmd PATH]

Grant host Linux capabilities commonly needed for non-sudo KVM tracing:
  perf:      cap_perfmon,cap_sys_ptrace,cap_syslog,cap_ipc_lock+ep
  trace-cmd: cap_sys_admin,cap_sys_ptrace,cap_dac_read_search+ep

This changes capabilities on host binaries. Run with sudo, or as root.
EOF
}

DRY_RUN=0
PERF_BIN="${PERF_BIN:-$(command -v perf || true)}"
TRACE_CMD_BIN="${TRACE_CMD_BIN:-$(command -v trace-cmd || true)}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --perf)
      if [[ $# -lt 2 ]]; then
        usage
        exit 2
      fi
      PERF_BIN="$2"
      shift 2
      ;;
    --trace-cmd)
      if [[ $# -lt 2 ]]; then
        usage
        exit 2
      fi
      TRACE_CMD_BIN="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
done

run_setcap() {
  local caps="$1"
  local path="$2"
  if [[ -z "$path" ]]; then
    echo "skip: tool not found" >&2
    return
  fi
  if [[ ! -x "$path" ]]; then
    echo "skip: not executable: $path" >&2
    return
  fi
  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf 'setcap %q %q\n' "$caps" "$path"
    return
  fi
  setcap "$caps" "$path"
  getcap "$path" || true
}

run_setcap "cap_perfmon,cap_sys_ptrace,cap_syslog,cap_ipc_lock+ep" "$PERF_BIN"
run_setcap "cap_sys_admin,cap_sys_ptrace,cap_dac_read_search+ep" "$TRACE_CMD_BIN"
