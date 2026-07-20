#!/bin/bash
# Capture repeatable full-process Linux baselines for WOS self-host comparisons.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

SELFHOST_RUNNER="${WOS_LINUX_SELFHOST_BASELINE_RUNNER:-$SCRIPT_DIR/run_wos_selfhost_build.sh}"

DEFAULT_RUNS=50
DEFAULT_JOBS=32
DEFAULT_RUN_TIMEOUT_SECONDS=7200
DEFAULT_WORKDIR="/tmp/wos-selfhost-linux-baseline"
DEFAULT_REPO="https://github.com/Pascu-Victor/wos.git"

usage() {
    cat <<EOF
Usage: scripts/bench/run_linux_selfhost_baseline.sh [options]

Run the exact fresh Linux clone/bootstrap/configure/wos_full flow 50 times by
default. Each run replaces the same scratch workdir and archives its reports,
command logs, source pins, host snapshots, and full outer wall time under a
distinct result directory.

Options:
  --runs N               Number of fresh runs (default: $DEFAULT_RUNS)
  --jobs N               Parallel jobs passed to the self-host runner
                         (default: $DEFAULT_JOBS)
  --workdir PATH         Linux scratch workdir (default: $DEFAULT_WORKDIR)
  --output-dir PATH      New result directory
                         (default: benchmarks/results/linux-selfhost-baseline-<UTC>)
  --expected-commit SHA  Require every checkout to use this root commit;
                         otherwise the first observed commit is the baseline
  --repo URL             Repository URL (default: $DEFAULT_REPO)
  --mirror-file PATH     Forward an iteration-only local file mirror
  --distdir PATH         Reuse source tarballs from this directory; the path
                         must be outside the scratch workdir
  --run-timeout-seconds N
                         Fail a run after N seconds (default: $DEFAULT_RUN_TIMEOUT_SECONDS)
  --no-heartbeat-sync    Do not sync after runner phases
  -h, --help             Show this help

The wrapper never passes --resume-checkout, --source-cache, --skip-bootstrap,
or a prebuilt host toolchain. The outer clock covers runner startup, fresh
workdir preparation, recursive checkout, tools/bootstrap.sh, configure,
wos_full, and final artifact verification. Every requested run is attempted
unless timed-out runner cleanup cannot be proven; in that case the script stops
without reusing the workdir. It exits nonzero unless every run and its evidence
collection pass.
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

require_positive_integer() {
    local label="$1"
    local value="$2"

    case "$value" in
        ''|*[!0-9]*|0)
            die "$label must be a positive integer: $value"
            ;;
    esac
}

reject_whitespace() {
    local label="$1"
    local value="$2"

    case "$value" in
        *[[:space:]]*)
            die "$label must not contain whitespace: $value"
            ;;
    esac
}

now_ms() {
    local epoch="${EPOCHREALTIME:-}"

    if [ -n "$epoch" ]; then
        local seconds="${epoch%.*}"
        local fraction="${epoch#*.}000"
        printf '%s\n' "$((10#$seconds * 1000 + 10#${fraction:0:3}))"
        return 0
    fi

    python3 - <<'PY'
import time

print(time.monotonic_ns() // 1_000_000)
PY
}

timestamp_utc() {
    date -u +%Y-%m-%dT%H:%M:%SZ
}

read_host_boot_id() {
    sed -n '1p' /proc/sys/kernel/random/boot_id 2>/dev/null
}

read_host_uptime() {
    sed -n '1{s/[[:space:]].*//;p;}' /proc/uptime 2>/dev/null
}

uptime_not_decreased() {
    local before="$1"
    local after="$2"

    [ -n "$before" ] && [ -n "$after" ] || return 1
    awk -v before="$before" -v after="$after" 'BEGIN { exit !(after + 0 >= before + 0) }'
}

capture_host_state() {
    local output="$1"
    local filesystem_path

    filesystem_path="$(dirname "$workdir")"
    while [ ! -e "$filesystem_path" ] && [ "$filesystem_path" != "/" ]; do
        filesystem_path="$(dirname "$filesystem_path")"
    done

    {
        echo timestamp_utc
        timestamp_utc
        echo uname
        uname -a
        echo uptime
        cat /proc/uptime
        echo meminfo
        cat /proc/meminfo
        echo filesystem
        df -k "$filesystem_path"
        echo tools
        git --version
        cmake --version | sed -n '1p'
        ninja --version
        clang --version | sed -n '1p'
    } > "$output" 2>&1
}

copy_evidence_file() {
    local source="$1"
    local destination="$2"
    local collection_log="$3"

    if [ -f "$source" ] && cp -- "$source" "$destination" >> "$collection_log" 2>&1; then
        return 0
    fi
    printf 'missing Linux evidence: %s\n' "$source" >> "$collection_log"
    return 1
}

archive_command_logs() {
    local source="$1"
    local destination="$2"
    local collection_log="$3"

    if [ ! -d "$source" ]; then
        printf 'missing Linux command log directory: %s\n' "$source" >> "$collection_log"
        return 1
    fi
    tar -C "$source" -cf "$destination" . >> "$collection_log" 2>&1
}

append_reason() {
    local reason="$1"
    if [ -n "$failure_reasons" ]; then
        failure_reasons="$failure_reasons,$reason"
    else
        failure_reasons="$reason"
    fi
}

canonical_path() {
    python3 - "$1" <<'PY'
import os
import sys

print(os.path.realpath(sys.argv[1]))
PY
}

write_distdir_manifest() {
    local root="$1"
    local output="$2"

    (
        cd "$root"
        find . -mindepth 1 -maxdepth 1 -type f -print | LC_ALL=C sort |
            while IFS= read -r file; do
                sha256sum "$file"
            done
    ) > "$output"
}

mirror_repo_relative_path() {
    case "$repo" in
        https://github.com/*)
            local relative="${repo#https://github.com/}"
            case "$relative" in
                ""|/*|*/*/*|*\?*|*\#*|*\\*|*[!A-Za-z0-9._/-]*)
                    die "unsafe mirror repository path derived from --repo: $repo"
                    ;;
            esac
            local owner="${relative%%/*}"
            local repository="${relative#*/}"
            case "$owner" in
                ""|.|..)
                    die "unsafe mirror owner path derived from --repo: $repo"
                    ;;
            esac
            case "$repository" in
                ""|.|..)
                    die "unsafe mirror repository name derived from --repo: $repo"
                    ;;
            esac
            printf '%s\n' "$relative"
            ;;
        *)
            die "--mirror-file requires an https://github.com/ repository URL: $repo"
            ;;
    esac
}

runs="$DEFAULT_RUNS"
jobs="$DEFAULT_JOBS"
workdir="$DEFAULT_WORKDIR"
output_dir=""
expected_commit=""
repo="$DEFAULT_REPO"
mirror_file=""
distdir=""
run_timeout_seconds="$DEFAULT_RUN_TIMEOUT_SECONDS"
heartbeat_sync=1

while (($# > 0)); do
    case "$1" in
        --runs)
            runs="${2:-}"
            [ -n "$runs" ] || die "--runs requires a value"
            shift
            ;;
        --jobs)
            jobs="${2:-}"
            [ -n "$jobs" ] || die "--jobs requires a value"
            shift
            ;;
        --workdir)
            workdir="${2:-}"
            [ -n "$workdir" ] || die "--workdir requires a value"
            shift
            ;;
        --output-dir)
            output_dir="${2:-}"
            [ -n "$output_dir" ] || die "--output-dir requires a value"
            shift
            ;;
        --expected-commit)
            expected_commit="${2:-}"
            [ -n "$expected_commit" ] || die "--expected-commit requires a value"
            shift
            ;;
        --repo)
            repo="${2:-}"
            [ -n "$repo" ] || die "--repo requires a value"
            shift
            ;;
        --mirror-file)
            mirror_file="${2:-}"
            [ -n "$mirror_file" ] || die "--mirror-file requires a value"
            shift
            ;;
        --distdir)
            distdir="${2:-}"
            [ -n "$distdir" ] || die "--distdir requires a value"
            shift
            ;;
        --run-timeout-seconds)
            run_timeout_seconds="${2:-}"
            [ -n "$run_timeout_seconds" ] || die "--run-timeout-seconds requires a value"
            shift
            ;;
        --no-heartbeat-sync)
            heartbeat_sync=0
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "unknown option: $1"
            ;;
    esac
    shift
done

require_positive_integer "--runs" "$runs"
require_positive_integer "--jobs" "$jobs"
require_positive_integer "--run-timeout-seconds" "$run_timeout_seconds"
reject_whitespace "--workdir" "$workdir"
reject_whitespace "--output-dir" "$output_dir"
reject_whitespace "--expected-commit" "$expected_commit"
reject_whitespace "--repo" "$repo"
reject_whitespace "--mirror-file" "$mirror_file"
reject_whitespace "--distdir" "$distdir"

if [ -n "$distdir" ]; then
    canonical_distdir="$(canonical_path "$distdir")"
    canonical_workdir="$(canonical_path "$workdir")"
    case "$canonical_distdir" in
        "$canonical_workdir"|"$canonical_workdir"/*)
            die "--distdir must be outside the scratch workdir: $distdir"
            ;;
    esac
    [ -d "$distdir" ] || die "--distdir must name an existing directory: $distdir"
fi

if [ -n "$mirror_file" ]; then
    [[ "$expected_commit" =~ ^[0-9a-f]{40}$ ]] ||
        die "--mirror-file requires --expected-commit as a full lowercase 40-hex commit"
fi

[ -x "$SELFHOST_RUNNER" ] || die "self-host runner is not executable: $SELFHOST_RUNNER"
command -v awk >/dev/null 2>&1 || die "awk is required"
command -v python3 >/dev/null 2>&1 || die "python3 is required"
command -v sha256sum >/dev/null 2>&1 || die "sha256sum is required"
command -v tar >/dev/null 2>&1 || die "tar is required"
command -v timeout >/dev/null 2>&1 || die "timeout is required"

if [ -z "$output_dir" ]; then
    output_dir="$WORKSPACE_ROOT/benchmarks/results/linux-selfhost-baseline-$(date -u +%Y%m%dT%H%M%SZ)"
fi
[ ! -e "$output_dir" ] || die "output directory already exists: $output_dir"
mkdir -p "$output_dir/runs"

mirror_commit=""
if [ -n "$mirror_file" ]; then
    canonical_mirror_root="$(canonical_path "$mirror_file")"
    [ -d "$canonical_mirror_root" ] || die "mirror root does not exist: $mirror_file"
    mirror_repo="$canonical_mirror_root/$(mirror_repo_relative_path)"
    [ -d "$mirror_repo" ] || die "mirror repository does not exist: $mirror_repo"
    canonical_mirror_repo="$(canonical_path "$mirror_repo")"
    case "$canonical_mirror_repo" in
        "$canonical_mirror_root"/*) ;;
        *) die "mirror repository escapes mirror root: $mirror_repo" ;;
    esac
    mirror_commit="$(git --git-dir="$mirror_repo" rev-parse HEAD 2>/dev/null || true)"
    [ "$mirror_commit" = "$expected_commit" ] ||
        die "mirror HEAD does not match --expected-commit: mirror=$mirror_commit expected=$expected_commit"
fi
distdir_enabled=0
distdir_manifest_sha256=""
if [ -n "$distdir" ]; then
    distdir_enabled=1
    write_distdir_manifest "$canonical_distdir" "$output_dir/distdir-manifest.txt"
    distdir_manifest_sha256="$(sha256sum "$output_dir/distdir-manifest.txt" | sed 's/[[:space:]].*//')"
fi

initial_boot_id="$(read_host_boot_id || true)"
[ -n "$initial_boot_id" ] || die "cannot read Linux boot ID"
initial_uptime="$(read_host_uptime || true)"
[ -n "$initial_uptime" ] || die "cannot read Linux uptime"

runs_tsv="$output_dir/runs.tsv"
summary_tsv="$output_dir/summary.tsv"
summary_json="$output_dir/summary.json"
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "run" "start_utc" "end_utc" "wall_ms" "runner_status" "evidence_status" \
    "accepted" "commit" "submodules_sha256" "uptime_before" "uptime_after" \
    "boot_id_before" "boot_id_after" "serial_start" "serial_end" "serial_sha256" "serial_failures" \
    "failure_reasons" "run_dir" "console_log" "repo" "mirror_commit" "distdir_enabled" \
    "distdir_manifest_sha256" "timed_out" > "$runs_tsv"

baseline_commit="$expected_commit"
baseline_submodules=""
previous_uptime="$initial_uptime"
attempted=0
runner_completed=0
accepted=0
failed=0
timed_out_runs=0
same_boot=1
same_commit=1
same_submodules=1

for ((run_number = 1; run_number <= runs; ++run_number)); do
    printf -v run_label 'run-%04d' "$run_number"
    run_dir="$output_dir/runs/$run_label"
    mkdir -p "$run_dir"
    console_log="$run_dir/console.log"
    collection_log="$run_dir/evidence-collection.log"
    local_log_dir="$workdir/logs"
    local_history_file="$workdir/selfhost-history.tsv"
    : > "$collection_log"

    failure_reasons=""
    evidence_status=0
    commit=""
    submodules_sha=""
    boot_id_before="$(read_host_boot_id || true)"
    uptime_before="$(read_host_uptime || true)"
    capture_host_state "$run_dir/host-before.txt" || {
        evidence_status=1
        append_reason "host_snapshot_before_failed"
    }

    if [ "$boot_id_before" != "$initial_boot_id" ]; then
        append_reason "linux_boot_id_mismatch_before"
        same_boot=0
    fi
    if ! uptime_not_decreased "$previous_uptime" "$uptime_before"; then
        append_reason "linux_reboot_or_uptime_unavailable_before"
        same_boot=0
    fi

    runner_cmd=("$SELFHOST_RUNNER" linux --jobs "$jobs" --workdir "$workdir" --repo "$repo"
        --log-dir "$local_log_dir" --history-file "$local_history_file")
    if [ "$heartbeat_sync" -eq 1 ]; then
        runner_cmd+=(--heartbeat-sync)
    fi
    if [ -n "$mirror_file" ]; then
        runner_cmd+=(--mirror-file "$mirror_file")
    fi
    if [ -n "$distdir" ]; then
        runner_cmd+=(--distdir "$distdir")
    fi

    echo "[$run_label/$runs] start $(printf '%q ' "${runner_cmd[@]}")"
    start_utc="$(timestamp_utc)"
    start_ms="$(now_ms)"
    set +e
    timeout --signal=TERM --kill-after=60s "${run_timeout_seconds}s" "${runner_cmd[@]}" > "$console_log" 2>&1
    runner_status=$?
    set -e
    end_ms="$(now_ms)"
    end_utc="$(timestamp_utc)"
    wall_ms=$((end_ms - start_ms))
    attempted=$((attempted + 1))
    timed_out=0
    cleanup_incomplete=0
    case "$runner_status" in
        124|137)
            timed_out=1
            timed_out_runs=$((timed_out_runs + 1))
            append_reason "runner_timeout"
            if [ -e "${workdir%/}.lock" ]; then
                append_reason "runner_cleanup_incomplete"
                cleanup_incomplete=1
            fi
            ;;
    esac
    if [ "$runner_status" -eq 0 ]; then
        runner_completed=$((runner_completed + 1))
    else
        append_reason "runner_status_$runner_status"
    fi

    if [ "$cleanup_incomplete" -eq 1 ]; then
        uptime_after="$(read_host_uptime || true)"
        boot_id_after="$(read_host_boot_id || true)"
        capture_host_state "$run_dir/host-after.txt" || true
        evidence_status=1
        append_reason "evidence_incomplete"
        run_accepted=0
        failed=$((failed + 1))
        empty_serial_sha="$(printf '' | sha256sum | sed 's/[[:space:]].*//')"
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$run_number" "$start_utc" "$end_utc" "$wall_ms" "$runner_status" "$evidence_status" \
            "$run_accepted" "" "" "$uptime_before" "$uptime_after" \
            "$boot_id_before" "$boot_id_after" "0" "0" "$empty_serial_sha" "0" \
            "$failure_reasons" "$run_dir" "$console_log" "$repo" "$mirror_commit" "$distdir_enabled" \
            "$distdir_manifest_sha256" "$timed_out" >> "$runs_tsv"
        echo "[$run_label/$runs] FAIL ${wall_ms}ms reasons=$failure_reasons; stopping to avoid unsafe workdir reuse" >&2
        break
    fi

    uptime_after="$(read_host_uptime || true)"
    boot_id_after="$(read_host_boot_id || true)"
    capture_host_state "$run_dir/host-after.txt" || {
        evidence_status=1
        append_reason "host_snapshot_after_failed"
    }
    if [ "$boot_id_after" != "$initial_boot_id" ]; then
        append_reason "linux_boot_id_mismatch_during"
        same_boot=0
    fi
    if ! uptime_not_decreased "$uptime_before" "$uptime_after"; then
        append_reason "linux_reboot_or_uptime_unavailable_during"
        same_boot=0
    fi
    if [ -n "$uptime_after" ]; then
        previous_uptime="$uptime_after"
    fi

    checkout_path="$workdir/wos"
    commit="$(git -C "$checkout_path" rev-parse HEAD 2>> "$collection_log" || true)"
    commit="$(printf '%s\n' "$commit" | sed -n '1p')"
    if ! [[ "$commit" =~ ^[0-9a-f]{40}$ ]]; then
        append_reason "commit_invalid_or_unavailable"
        same_commit=0
    elif [ -z "$baseline_commit" ]; then
        baseline_commit="$commit"
    elif [ "$commit" != "$baseline_commit" ]; then
        append_reason "commit_mismatch"
        same_commit=0
    fi
    if [ -n "$mirror_commit" ] && [ "$commit" != "$mirror_commit" ]; then
        append_reason "mirror_commit_mismatch"
        same_commit=0
    fi

    submodules_file="$workdir/submodules.txt"
    if [ -f "$submodules_file" ]; then
        submodules_sha="$(sha256sum "$submodules_file" | sed 's/[[:space:]].*//')"
    fi
    if ! [[ "$submodules_sha" =~ ^[0-9a-f]{64}$ ]]; then
        append_reason "submodule_manifest_invalid_or_unavailable"
        same_submodules=0
    elif [ -z "$baseline_submodules" ]; then
        baseline_submodules="$submodules_sha"
    elif [ "$submodules_sha" != "$baseline_submodules" ]; then
        append_reason "submodule_manifest_mismatch"
        same_submodules=0
    fi

    while IFS=$'\t' read -r source_relative local_name; do
        if ! copy_evidence_file "$workdir/$source_relative" "$run_dir/$local_name" "$collection_log"; then
            evidence_status=1
        fi
    done <<'EOF'
selfhost-report.tsv	selfhost-report.tsv
selfhost-detail.tsv	selfhost-detail.tsv
bootstrap-detail.tsv	bootstrap-detail.tsv
selfhost-cache-deltas.tsv	selfhost-cache-deltas.tsv
selfhost-history.tsv	selfhost-history.tsv
submodules.txt	submodules.txt
wos/build-selfhost/CMakeCache.txt	CMakeCache.txt
EOF

    if ! archive_command_logs "$local_log_dir" "$run_dir/command-logs.tar" "$collection_log"; then
        evidence_status=1
    fi
    if [ "$evidence_status" -ne 0 ]; then
        append_reason "evidence_incomplete"
    fi

    if [ -z "$failure_reasons" ]; then
        run_accepted=1
        accepted=$((accepted + 1))
    else
        run_accepted=0
        failed=$((failed + 1))
    fi

    empty_serial_sha="$(printf '' | sha256sum | sed 's/[[:space:]].*//')"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$run_number" "$start_utc" "$end_utc" "$wall_ms" "$runner_status" "$evidence_status" \
        "$run_accepted" "$commit" "$submodules_sha" "$uptime_before" "$uptime_after" \
        "$boot_id_before" "$boot_id_after" "0" "0" "$empty_serial_sha" "0" \
        "$failure_reasons" "$run_dir" "$console_log" "$repo" "$mirror_commit" "$distdir_enabled" \
        "$distdir_manifest_sha256" "$timed_out" >> "$runs_tsv"

    if [ "$run_accepted" -eq 1 ]; then
        echo "[$run_label/$runs] PASS ${wall_ms}ms commit=$commit"
    else
        echo "[$run_label/$runs] FAIL ${wall_ms}ms reasons=$failure_reasons" >&2
    fi
done

overall_pass=0
if [ "$attempted" -eq "$runs" ] && [ "$accepted" -eq "$runs" ] && [ "$failed" -eq 0 ] && [ "$same_boot" -eq 1 ] &&
    [ "$same_commit" -eq 1 ] && [ "$same_submodules" -eq 1 ]; then
    overall_pass=1
fi

{
    printf 'schema_version\t2\n'
    printf 'generated_utc\t%s\n' "$(timestamp_utc)"
    printf 'pass\t%s\n' "$overall_pass"
    printf 'requested_runs\t%s\n' "$runs"
    printf 'attempted_runs\t%s\n' "$attempted"
    printf 'runner_completed_runs\t%s\n' "$runner_completed"
    printf 'accepted_runs\t%s\n' "$accepted"
    printf 'failed_runs\t%s\n' "$failed"
    printf 'timed_out_runs\t%s\n' "$timed_out_runs"
    printf 'same_boot\t%s\n' "$same_boot"
    printf 'same_commit\t%s\n' "$same_commit"
    printf 'same_submodules\t%s\n' "$same_submodules"
    printf 'boot_id\t%s\n' "$initial_boot_id"
    printf 'expected_commit\t%s\n' "$baseline_commit"
    printf 'expected_submodules_sha256\t%s\n' "$baseline_submodules"
    printf 'operating_system\tLinux\n'
    printf 'jobs\t%s\n' "$jobs"
    printf 'workdir\t%s\n' "$workdir"
    printf 'repo\t%s\n' "$repo"
    printf 'mirror_file\t%s\n' "$mirror_file"
    printf 'mirror_commit\t%s\n' "$mirror_commit"
    printf 'distdir\t%s\n' "$distdir"
    printf 'canonical_distdir\t%s\n' "${canonical_distdir:-}"
    printf 'distdir_enabled\t%s\n' "$distdir_enabled"
    printf 'distdir_manifest_sha256\t%s\n' "$distdir_manifest_sha256"
    printf 'run_timeout_seconds\t%s\n' "$run_timeout_seconds"
    printf 'runs_tsv\t%s\n' "$runs_tsv"
} > "$summary_tsv"

python3 - "$summary_tsv" "$summary_json" <<'PY'
import json
import sys

source, output = sys.argv[1:]
data = {}
with open(source, encoding="utf-8") as rows:
    for line in rows:
        key, value = line.rstrip("\n").split("\t", 1)
        if key in {
            "schema_version",
            "pass",
            "requested_runs",
            "attempted_runs",
            "runner_completed_runs",
            "accepted_runs",
            "failed_runs",
            "timed_out_runs",
            "same_boot",
            "same_commit",
            "same_submodules",
            "jobs",
            "distdir_enabled",
            "run_timeout_seconds",
        }:
            data[key] = int(value)
        else:
            data[key] = value
with open(output, "w", encoding="ascii") as result:
    json.dump(data, result, indent=2, sort_keys=True)
    result.write("\n")
PY

echo "Linux baseline summary: $summary_json"
echo "accepted=$accepted/$runs runner_completed=$runner_completed failed=$failed pass=$overall_pass"

if [ "$overall_pass" -ne 1 ]; then
    exit 1
fi
