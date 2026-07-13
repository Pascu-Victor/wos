#!/bin/sh
set -u

resolve_showcase_dir() {
    script_path="$0"

    case "$script_path" in
        /*) ;;
        *) script_path="$(pwd)/$script_path" ;;
    esac

    if command -v readlink >/dev/null 2>&1; then
        while [ -L "$script_path" ]; do
            link_target="$(readlink "$script_path")" || break
            case "$link_target" in
                /*) script_path="$link_target" ;;
                *) script_path="$(dirname -- "$script_path")/$link_target" ;;
            esac
        done
    fi

    CDPATH= cd -- "$(dirname -- "$script_path")" && pwd
}

SHOWCASE_DIR="$(resolve_showcase_dir)"

timestamp() {
    date +%Y%m%d-%H%M%S 2>/dev/null || echo unknown-time
}

usage() {
    echo "Usage: $0 [--scale quick|full|stress] [--hosts host0,host1] [--output-root PATH] [script ...]" >&2
}

SCALE="${WOS_SHOWCASE_SCALE:-quick}"
HOSTS="${WOS_SHOWCASE_HOSTS:-}"
RUN_ID="${WOS_SHOWCASE_RUN_ID:-$(timestamp)}"
OUTPUT_ROOT="${WOS_SHOWCASE_OUTPUT_ROOT:-/tmp/wos-showcase-$RUN_ID}"
SCRIPTS=""

while [ "$#" -gt 0 ]; do
    case "$1" in
        quick|full|stress)
            SCALE="$1"
            shift
            ;;
        --scale)
            [ "$#" -ge 2 ] || {
                usage
                exit 1
            }
            SCALE="$2"
            shift 2
            ;;
        --hosts)
            [ "$#" -ge 2 ] || {
                usage
                exit 1
            }
            HOSTS="$2"
            shift 2
            ;;
        --output-root)
            [ "$#" -ge 2 ] || {
                usage
                exit 1
            }
            OUTPUT_ROOT="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            SCRIPTS="${SCRIPTS}${SCRIPTS:+ }$1"
            shift
            ;;
    esac
done

case "$SCALE" in
    quick|full|stress)
        ;;
    *)
        echo "invalid scale: $SCALE" >&2
        exit 1
        ;;
esac

if [ -z "$SCRIPTS" ]; then
    SCRIPTS="00-wki-map.sh 10-placement-hop.sh 20-forward-vfs.sh 30-bench-wki.sh 40-live-cpp-distributed.sh 50-fixed-resource-workloads.sh"
fi

LOG_DIR="$OUTPUT_ROOT/logs"
SUMMARY_FILE="$OUTPUT_ROOT/summary.tsv"
METRICS_FILE="$OUTPUT_ROOT/metrics.jsonl"
PASS=0
FAIL=0
SKIP=0

mkdir -p "$LOG_DIR"
rm -f "$SUMMARY_FILE"
: > "$METRICS_FILE"

export WOS_SHOWCASE_SCALE="$SCALE"
export WOS_SHOWCASE_HOSTS="$HOSTS"
export WOS_SHOWCASE_OUTPUT_ROOT="$OUTPUT_ROOT"

record_summary() {
    printf '%s\t%s\t%s\n' "$1" "$2" "$3" >> "$SUMMARY_FILE"
}

collect_metrics() {
    sed -n '/^[[:space:]]*{.*}[[:space:]]*$/p' "$1" >> "$METRICS_FILE"
}

run_one() {
    script="$1"
    case "$script" in
        */*) path="$script" ;;
        *) path="$SHOWCASE_DIR/$script" ;;
    esac
    name="$(basename "$path" .sh)"
    log="$LOG_DIR/$name.log"

    if [ ! -x "$path" ]; then
        SKIP=$((SKIP + 1))
        printf '\n=== SKIP %s ===\nmissing executable: %s\n' "$name" "$path"
        record_summary "$name" SKIP "missing executable: $path"
        return 0
    fi

    printf '\n=== RUN %s ===\n' "$name"
    printf 'script=%s scale=%s hosts=%s\n' "$path" "$SCALE" "${HOSTS:-<auto>}"
    if "$path" > "$log" 2>&1; then
        rc=0
    else
        rc="$?"
    fi
    collect_metrics "$log"
    cat "$log"
    if [ "$rc" -eq 0 ]; then
        PASS=$((PASS + 1))
        printf 'PASS %s\n' "$name"
        record_summary "$name" PASS "log=$log"
    else
        FAIL=$((FAIL + 1))
        printf 'FAIL %s rc=%s\n' "$name" "$rc"
        record_summary "$name" FAIL "rc=$rc log=$log"
    fi
}

printf 'WOS showcase suite\n'
printf 'RUN_ID=%s\n' "$RUN_ID"
printf 'SCALE=%s\n' "$SCALE"
printf 'HOSTS=%s\n' "${HOSTS:-<auto>}"
printf 'RESULT_DIR=%s\n' "$OUTPUT_ROOT"

for script in $SCRIPTS; do
    run_one "$script"
done

printf '\n=== WOS SHOWCASE SUMMARY ===\n'
cat "$SUMMARY_FILE"
printf '\nPASS=%s FAIL=%s SKIP=%s\n' "$PASS" "$FAIL" "$SKIP"
printf 'RESULT_DIR=%s\n' "$OUTPUT_ROOT"
printf 'SUMMARY_FILE=%s\n' "$SUMMARY_FILE"
printf 'METRICS_FILE=%s\n' "$METRICS_FILE"

if [ "$FAIL" -eq 0 ]; then
    exit 0
fi
exit 1
