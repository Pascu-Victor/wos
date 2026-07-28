#!/bin/bash
# Verify that one-shot placement keeps a payload's fork/exec tree on one system.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WOS_SSH="$WORKSPACE_ROOT/scripts/remote/wos_ssh.sh"

host="wos-0"
runs=80
systems="wos-0,wos-1,wos-2,wos-3"
run_timeout=300

usage() {
    cat <<'EOF'
Usage: scripts/test/run_wki_one_shot_affinity_stress.sh [options]

Options:
  --host NAME          Submitter contacted over SSH (default: wos-0)
  --runs N             One-shot process trees to launch (default: 80)
  --systems CSV        Expected placement systems
                       (default: wos-0,wos-1,wos-2,wos-3)
  --run-timeout N      Whole remote test timeout in seconds (default: 300)
  -h, --help           Show this help

Each run uses balanced one-shot placement with /dev/null stdin, creates an
output in node-local /tmp from a fork/exec child, and copies it to an explicitly
home-routed result. The output must be visible to the selected parent, and all
configured systems must receive work when the run count permits it.
EOF
}

positive_integer() {
    case "$2" in
        ''|*[!0-9]*|0)
            echo "$1 must be a positive integer" >&2
            exit 2
            ;;
    esac
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --host)
            host="${2:-}"
            shift 2
            ;;
        --runs)
            runs="${2:-}"
            shift 2
            ;;
        --systems)
            systems="${2:-}"
            shift 2
            ;;
        --run-timeout)
            run_timeout="${2:-}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

positive_integer "--runs" "$runs"
positive_integer "--run-timeout" "$run_timeout"
case "$host" in
    ''|*[!A-Za-z0-9._-]*)
        echo "--host contains unsafe characters" >&2
        exit 2
        ;;
esac
case "$systems" in
    ''|*[!A-Za-z0-9._,-]*|,*|*,|*,,*)
        echo "--systems must be a comma-separated hostname list" >&2
        exit 2
        ;;
esac

exec timeout --signal=TERM --kill-after=10s "${run_timeout}s" \
    "$WOS_SSH" "$host" /usr/bin/timeout -s TERM -k 10 "$run_timeout" \
    /usr/bin/bash -s -- "$runs" "$systems" <<'WOS_STRESS'
set -euo pipefail

runs="$1"
systems_csv="$2"
systems="${systems_csv//,/ }"
tag="wki-one-shot-affinity-$$"
launcher='
set -euo pipefail
report="$1"
host_output="$2"
child_script="$3"
local_output="/tmp/wki-one-shot-local-$$"
trap '\''rm -f -- "$local_output" 2>/dev/null || true'\'' EXIT HUP INT TERM
/usr/bin/hostname > "$report"
/usr/bin/bash -c "$child_script" _ "$local_output"
/usr/bin/cp -- "$local_output" "$host_output"
'
child_script='printf payload > "$1"'

for system in $systems; do
    case "$system" in
        ''|*[!A-Za-z0-9._-]*) exit 64 ;;
    esac
done

iteration=0
while [ "$iteration" -lt "$runs" ]; do
    report="/tmp/$tag-$iteration-report"
    host_output="/tmp/$tag-$iteration-output"
    : > "$report"
    : > "$host_output"

    forward --clear --target balanced --one-shot \
        -/usr -/bin -/lib -/lib64 -/libexec -/share -/etc -/proc -/dev -/run -/tmp \
        "+$report" "+$host_output" -- \
        /usr/bin/timeout -s TERM -k 2 10 /usr/bin/bash -c "$launcher" \
        _ "$report" "$host_output" "$child_script" </dev/null

    [ "$(cat "$host_output")" = payload ] || {
        echo "iteration=$iteration missing_staged_output" >&2
        exit 1
    }
    selected="$(cat "$report")"
    case ",$systems_csv," in
        *",$selected,"*) ;;
        *)
            echo "iteration=$iteration unexpected_system=$selected" >&2
            exit 1
            ;;
    esac
    iteration=$((iteration + 1))
done

for system in $systems; do
    count=0
    iteration=0
    while [ "$iteration" -lt "$runs" ]; do
        selected="$(cat "/tmp/$tag-$iteration-report")"
        [ "$selected" != "$system" ] || count=$((count + 1))
        iteration=$((iteration + 1))
    done
    printf 'system=%s trees=%s\n' "$system" "$count"
    if [ "$runs" -ge 4 ] && [ "$count" -eq 0 ]; then
        echo "system=$system received_no_work" >&2
        exit 1
    fi
done

rm -f "/tmp/$tag-"*-report "/tmp/$tag-"*-output
printf 'runs=%s systems=%s staged_outputs=%s status=pass\n' "$runs" "$systems_csv" "$runs"
WOS_STRESS
