#!/bin/bash
# Run the complete WOS self-host build repeatedly in one already-booted VM and
# preserve enough host-side evidence to audit every attempt.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

SELFHOST_RUNNER="${WOS_SELFHOST_REPEAT_RUNNER:-$SCRIPT_DIR/run_wos_selfhost_build.sh}"
WOS_SSH="${WOS_SELFHOST_REPEAT_SSH:-$WORKSPACE_ROOT/scripts/remote/wos_ssh.sh}"
WOS_SFTP_GET="${WOS_SELFHOST_REPEAT_SFTP_GET:-$WORKSPACE_ROOT/scripts/remote/wos_sftp_get.sh}"

DEFAULT_RUNS=50
DEFAULT_HOST="wos-0"
DEFAULT_JOBS=32
DEFAULT_RUN_TIMEOUT_SECONDS=7200
DEFAULT_REPO="https://github.com/Pascu-Victor/wos.git"
DEFAULT_CLEANUP_WAIT_SECONDS=60
DEFAULT_CLEANUP_PROBE_SECONDS=5
DEFAULT_WORKDIR="/root/wos-selfhost-bench"
DEFAULT_SERIAL_LOG="$WORKSPACE_ROOT/serial-vm0.log"
DEFAULT_SERIAL_FAIL_REGEX='out of memory|oom killer|oom:|kernel panic|hung task|blocked for more than|allocation failure|allocator failure|failed to allocate|I/O error|input/output error|filesystem.*(corrupt|shutdown)|corruption detected|forced shutdown|error xfs:|xfs.*(error|corrupt|shutdown|failed)|\[xfs (btree|free|agfl|trans)\]|AGFL empty|KASAN:|UBSAN:'

usage() {
    cat <<EOF
Usage: scripts/bench/run_wos_selfhost_repeatability.sh [options]

Run the exact fresh WOS self-host clone/bootstrap/configure/wos_full flow 50
times by default in one already-booted VM. Each run replaces the same guest
workdir, while its reports, command logs, console output, and serial byte range
are archived under a distinct host-side result directory.

Options:
  --runs N                 Number of fresh runs (default: $DEFAULT_RUNS)
  --host NAME              WOS node alias/IP (default: $DEFAULT_HOST)
  --jobs N                 Parallel jobs passed to the self-host runner
                           (default: $DEFAULT_JOBS)
  --workdir PATH           Guest scratch workdir (default: $DEFAULT_WORKDIR)
  --output-dir PATH        New host result directory
                           (default: benchmarks/results/wos-selfhost-repeat-<UTC>)
  --serial-log PATH        Host serial log (default: serial-vm0.log)
  --expected-commit SHA    Require every checkout to use this root commit;
                           otherwise the first observed commit is the baseline
  --repo URL               Override the self-host runner repository URL
  --mirror-file PATH       Forward an iteration-only guest file mirror
  --distdir PATH           Reuse guest source tarballs from this directory;
                           the path must be outside the guest scratch workdir
  --run-timeout-seconds N  Fail a run after N seconds
                           (default: $DEFAULT_RUN_TIMEOUT_SECONDS)
  --serial-fail-regex ERE  Case-insensitive serial failure expression
  --no-heartbeat-sync      Do not request out-of-band sync after runner phases
  -h, --help               Show this help

The script assumes the VM is already running, for example:
  bin/wos-cluster --config configs/cluster_selfhost.json --launch --no-setup

It never passes --resume-checkout, --source-cache, or --skip-bootstrap. A
nonzero run, changed commit/submodule manifest, VM reboot, serial truncation,
serial failure match, or missing evidence marks that run failed. All requested
runs are attempted unless timed-out runner cleanup cannot be proven; in that
case the script stops without reusing the guest workdir. The script exits
nonzero unless every run passes.
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

reject_remote_whitespace() {
    local label="$1"
    local value="$2"

    case "$value" in
        *[[:space:]]*)
            die "$label must not contain whitespace: $value"
            ;;
    esac
}

shell_quote() {
    printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"
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

serial_size() {
    if [ ! -f "$serial_log" ]; then
        return 1
    fi
    wc -c < "$serial_log" | tr -d '[:space:]'
}

serial_boot_id() {
    if [ ! -f "$serial_log" ]; then
        return 1
    fi
    grep -aEo 'WOS version=[^[:space:]]+ boot_id=[[:xdigit:]]+' "$serial_log" 2>/dev/null |
        tail -n 1 | sed 's/.*boot_id=//'
}

capture_serial_range() {
    local before="$1"
    local after="$2"
    local output="$3"
    local expected_bytes
    local captured_bytes

    : > "$output"
    if [ ! -f "$serial_log" ] || [ "$after" -lt "$before" ]; then
        return 1
    fi
    if [ "$after" -eq "$before" ]; then
        return 0
    fi
    expected_bytes=$((after - before))
    dd if="$serial_log" of="$output" iflag=skip_bytes,count_bytes skip="$before" count="$expected_bytes" status=none
    captured_bytes="$(wc -c < "$output" | tr -d '[:space:]')"
    [ "$captured_bytes" -eq "$expected_bytes" ]
}

guest_output() {
    local command="$1"
    "$WOS_SSH" "$host" "$command"
}

capture_guest_state() {
    local output="$1"
    local quoted_workdir
    quoted_workdir="$(shell_quote "$workdir")"

    guest_output "echo uptime; cat /proc/uptime; echo meminfo; cat /proc/meminfo; echo filesystem; df -k $quoted_workdir" > "$output" 2>&1
}

read_guest_uptime() {
    guest_output "cat /proc/uptime" 2>/dev/null | sed -n '1{s/[[:space:]].*//;p;}'
}

uptime_not_decreased() {
    local before="$1"
    local after="$2"

    [ -n "$before" ] && [ -n "$after" ] || return 1
    awk -v before="$before" -v after="$after" 'BEGIN { exit !(after + 0 >= before + 0) }'
}

copy_guest_file() {
    local remote_path="$1"
    local local_path="$2"
    local collection_log="$3"

    if "$WOS_SFTP_GET" "$host" "$remote_path" "$local_path" >> "$collection_log" 2>&1; then
        return 0
    fi
    printf 'missing guest evidence: %s\n' "$remote_path" >> "$collection_log"
    return 1
}

archive_guest_logs() {
    local remote_log_dir="$1"
    local local_path="$2"
    local collection_log="$3"
    local remote_archive="/tmp/wos-selfhost-repeat-${session_id}-${run_label}-logs.tar"
    local make_archive

    make_archive="tar -C $(shell_quote "$remote_log_dir") -cf $(shell_quote "$remote_archive") ."
    if ! guest_output "$make_archive" >> "$collection_log" 2>&1; then
        printf 'failed to archive guest log directory: %s\n' "$remote_log_dir" >> "$collection_log"
        return 1
    fi
    if ! copy_guest_file "$remote_archive" "$local_path" "$collection_log"; then
        guest_output "rm -f $(shell_quote "$remote_archive")" >/dev/null 2>&1 || true
        return 1
    fi
    guest_output "rm -f $(shell_quote "$remote_archive")" >/dev/null 2>&1 || true
    return 0
}

append_reason() {
    local reason="$1"
    if [ -n "$failure_reasons" ]; then
        failure_reasons="$failure_reasons,$reason"
    else
        failure_reasons="$reason"
    fi
}

guest_canonical_path() {
    local python_code='import os, sys; print(os.path.realpath(sys.argv[1]))'
    local canonical

    canonical="$(guest_output "python3 -c $(shell_quote "$python_code") $(shell_quote "$1")" 2>/dev/null || true)"
    printf '%s\n' "$canonical" | sed -n '1p'
}

write_guest_distdir_manifest() {
    local root="$1"
    local output="$2"
    local command

    command="cd $(shell_quote "$root") && find . -mindepth 1 -maxdepth 1 -type f -print"
    command+=" | LC_ALL=C sort | while IFS= read -r file; do sha256sum \"\$file\"; done"
    guest_output "$command" > "$output"
}

mirror_repo_relative_path() {
    local effective_repo="${repo:-$DEFAULT_REPO}"
    case "$effective_repo" in
        https://github.com/*)
            local relative="${effective_repo#https://github.com/}"
            case "$relative" in
                ""|/*|*/*/*|*\?*|*\#*|*\\*|*[!A-Za-z0-9._/-]*)
                    die "unsafe mirror repository path derived from --repo: $effective_repo"
                    ;;
            esac
            local owner="${relative%%/*}"
            local repository="${relative#*/}"
            case "$owner" in
                ""|.|..)
                    die "unsafe mirror owner path derived from --repo: $effective_repo"
                    ;;
            esac
            case "$repository" in
                ""|.|..)
                    die "unsafe mirror repository name derived from --repo: $effective_repo"
                    ;;
            esac
            printf '%s\n' "$relative"
            ;;
        *)
            die "--mirror-file requires an https://github.com/ repository URL: $effective_repo"
            ;;
    esac
}

wait_for_guest_runner_cleanup() {
    local lock_path="${workdir%/}.lock"
    local deadline_ms now remaining_ms probe_seconds

    deadline_ms=$(( $(now_ms) + DEFAULT_CLEANUP_WAIT_SECONDS * 1000 ))
    while :; do
        now="$(now_ms)"
        remaining_ms=$((deadline_ms - now))
        if [ "$remaining_ms" -le 0 ]; then
            return 1
        fi
        probe_seconds=$(((remaining_ms + 999) / 1000))
        if [ "$probe_seconds" -gt "$DEFAULT_CLEANUP_PROBE_SECONDS" ]; then
            probe_seconds="$DEFAULT_CLEANUP_PROBE_SECONDS"
        fi
        if timeout --signal=KILL "${probe_seconds}s" "$WOS_SSH" "$host" \
            "test ! -e $(shell_quote "$lock_path")" >/dev/null 2>&1; then
            return 0
        fi
        now="$(now_ms)"
        if [ "$now" -lt "$deadline_ms" ]; then
            sleep 1
        fi
    done
}

runs="$DEFAULT_RUNS"
host="$DEFAULT_HOST"
jobs="$DEFAULT_JOBS"
workdir="$DEFAULT_WORKDIR"
output_dir=""
serial_log="$DEFAULT_SERIAL_LOG"
expected_commit=""
repo=""
mirror_file=""
distdir=""
run_timeout_seconds="$DEFAULT_RUN_TIMEOUT_SECONDS"
serial_fail_regex="$DEFAULT_SERIAL_FAIL_REGEX"
heartbeat_sync=1

while (($# > 0)); do
    case "$1" in
        --runs)
            runs="${2:-}"
            [ -n "$runs" ] || die "--runs requires a value"
            shift
            ;;
        --host)
            host="${2:-}"
            [ -n "$host" ] || die "--host requires a value"
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
        --serial-log)
            serial_log="${2:-}"
            [ -n "$serial_log" ] || die "--serial-log requires a value"
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
        --serial-fail-regex)
            serial_fail_regex="${2:-}"
            [ -n "$serial_fail_regex" ] || die "--serial-fail-regex requires a value"
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
reject_remote_whitespace "--host" "$host"
reject_remote_whitespace "--workdir" "$workdir"
reject_remote_whitespace "--expected-commit" "$expected_commit"
reject_remote_whitespace "--repo" "$repo"
reject_remote_whitespace "--mirror-file" "$mirror_file"
reject_remote_whitespace "--distdir" "$distdir"

if [ -n "$mirror_file" ]; then
    [[ "$expected_commit" =~ ^[0-9a-f]{40}$ ]] ||
        die "--mirror-file requires --expected-commit as a full lowercase 40-hex commit"
fi

[ -x "$SELFHOST_RUNNER" ] || die "self-host runner is not executable: $SELFHOST_RUNNER"
[ -x "$WOS_SSH" ] || die "WOS SSH helper is not executable: $WOS_SSH"
[ -x "$WOS_SFTP_GET" ] || die "WOS SFTP helper is not executable: $WOS_SFTP_GET"
command -v python3 >/dev/null 2>&1 || die "python3 is required"
command -v sha256sum >/dev/null 2>&1 || die "sha256sum is required"
command -v dd >/dev/null 2>&1 || die "dd is required"
command -v timeout >/dev/null 2>&1 || die "timeout is required"

if [ -z "$output_dir" ]; then
    output_dir="$WORKSPACE_ROOT/benchmarks/results/wos-selfhost-repeat-$(date -u +%Y%m%dT%H%M%SZ)"
fi
[ ! -e "$output_dir" ] || die "output directory already exists: $output_dir"
mkdir -p "$output_dir/runs"

[ -f "$serial_log" ] || die "serial log does not exist: $serial_log"
initial_boot_id="$(serial_boot_id || true)"
[ -n "$initial_boot_id" ] || die "cannot find the current WOS boot ID in serial log: $serial_log"
initial_uptime="$(read_guest_uptime || true)"
[ -n "$initial_uptime" ] || die "cannot read /proc/uptime from $host; launch the VM before running this harness"

if [ -n "$distdir" ]; then
    guest_output "test -d $(shell_quote "$distdir")" >/dev/null 2>&1 ||
        die "--distdir must name an existing guest directory: $distdir"
    canonical_distdir="$(guest_canonical_path "$distdir")"
    canonical_workdir="$(guest_canonical_path "$workdir")"
    [ -n "$canonical_distdir" ] && [ -n "$canonical_workdir" ] ||
        die "failed to canonicalize guest distdir/workdir paths"
    case "$canonical_distdir" in
        "$canonical_workdir"|"$canonical_workdir"/*)
            die "--distdir must be outside the guest scratch workdir: $distdir"
            ;;
    esac
fi

mirror_commit=""
if [ -n "$mirror_file" ]; then
    canonical_mirror_root="$(guest_canonical_path "$mirror_file")"
    [ -n "$canonical_mirror_root" ] || die "failed to canonicalize guest mirror root: $mirror_file"
    guest_output "test -d $(shell_quote "$canonical_mirror_root")" >/dev/null 2>&1 ||
        die "guest mirror root does not exist: $mirror_file"
    mirror_repo="$canonical_mirror_root/$(mirror_repo_relative_path)"
    guest_output "test -d $(shell_quote "$mirror_repo")" >/dev/null 2>&1 ||
        die "guest mirror repository does not exist: $mirror_repo"
    canonical_mirror_repo="$(guest_canonical_path "$mirror_repo")"
    case "$canonical_mirror_repo" in
        "$canonical_mirror_root"/*) ;;
        *) die "guest mirror repository escapes mirror root: $mirror_repo" ;;
    esac
    mirror_commit="$(guest_output "git --git-dir $(shell_quote "$mirror_repo") rev-parse HEAD" 2>/dev/null || true)"
    mirror_commit="$(printf '%s\n' "$mirror_commit" | sed -n '1p')"
    [ "$mirror_commit" = "$expected_commit" ] ||
        die "mirror HEAD does not match --expected-commit: mirror=$mirror_commit expected=$expected_commit"
fi
distdir_enabled=0
distdir_manifest_sha256=""
if [ -n "$distdir" ]; then
    distdir_enabled=1
    write_guest_distdir_manifest "$canonical_distdir" "$output_dir/distdir-manifest.txt" ||
        die "failed to capture guest distdir manifest: $canonical_distdir"
    distdir_manifest_sha256="$(sha256sum "$output_dir/distdir-manifest.txt" | sed 's/[[:space:]].*//')"
fi

session_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
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
    remote_log_dir="/tmp/wos-selfhost-repeat-${session_id}-${run_label}-logs"
    : > "$collection_log"

    failure_reasons=""
    evidence_status=0
    serial_failures=0
    commit=""
    submodules_sha=""
    boot_id_before="$(serial_boot_id || true)"
    uptime_before="$(read_guest_uptime || true)"
    capture_guest_state "$run_dir/guest-before.txt" || true

    if [ "$boot_id_before" != "$initial_boot_id" ]; then
        append_reason "vm_boot_id_mismatch_before"
        same_boot=0
    fi
    if ! uptime_not_decreased "$previous_uptime" "$uptime_before"; then
        append_reason "vm_reboot_or_unreachable_before"
        same_boot=0
    fi

    serial_start="$(serial_size || printf 0)"
    start_utc="$(timestamp_utc)"
    start_ms="$(now_ms)"
    runner_cmd=("$SELFHOST_RUNNER" wos --host "$host" --jobs "$jobs" --workdir "$workdir" --log-dir "$remote_log_dir"
        --history-file "$remote_log_dir/selfhost-history.tsv")
    if [ "$heartbeat_sync" -eq 1 ]; then
        runner_cmd+=(--heartbeat-sync)
    fi
    if [ -n "$repo" ]; then
        runner_cmd+=(--repo "$repo")
    fi
    if [ -n "$mirror_file" ]; then
        runner_cmd+=(--mirror-file "$mirror_file")
    fi
    if [ -n "$distdir" ]; then
        runner_cmd+=(--distdir "$distdir")
    fi

    echo "[$run_label/$runs] start $(printf '%q ' "${runner_cmd[@]}")"
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
            if ! wait_for_guest_runner_cleanup; then
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
        uptime_after=""
        boot_id_after="$(serial_boot_id || true)"
        serial_end="$(serial_size || printf 0)"
        if ! capture_serial_range "$serial_start" "$serial_end" "$run_dir/serial.log"; then
            append_reason "serial_missing_or_truncated"
        fi
        serial_sha="$(sha256sum "$run_dir/serial.log" | sed 's/[[:space:]].*//')"
        if grep -Ein "$serial_fail_regex" "$run_dir/serial.log" > "$run_dir/serial-failures.txt"; then
            serial_failures="$(wc -l < "$run_dir/serial-failures.txt" | tr -d '[:space:]')"
            append_reason "serial_failure_match"
        else
            : > "$run_dir/serial-failures.txt"
        fi
        evidence_status=1
        append_reason "evidence_incomplete"
        printf 'guest cleanup could not be proven; no further guest evidence collection or mutation was attempted\n' \
            >> "$collection_log"
        run_accepted=0
        failed=$((failed + 1))
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$run_number" "$start_utc" "$end_utc" "$wall_ms" "$runner_status" "$evidence_status" \
            "$run_accepted" "" "" "$uptime_before" "$uptime_after" \
            "$boot_id_before" "$boot_id_after" "$serial_start" "$serial_end" "$serial_sha" "$serial_failures" \
            "$failure_reasons" "$run_dir" "$console_log" "${repo:-$DEFAULT_REPO}" "$mirror_commit" "$distdir_enabled" \
            "$distdir_manifest_sha256" "$timed_out" >> "$runs_tsv"
        echo "[$run_label/$runs] FAIL ${wall_ms}ms reasons=$failure_reasons; stopping to avoid unsafe guest reuse" >&2
        break
    fi

    uptime_after="$(read_guest_uptime || true)"
    boot_id_after="$(serial_boot_id || true)"
    capture_guest_state "$run_dir/guest-after.txt" || true
    if [ "$boot_id_after" != "$initial_boot_id" ]; then
        append_reason "vm_boot_id_mismatch_during"
        same_boot=0
    fi
    if ! uptime_not_decreased "$uptime_before" "$uptime_after"; then
        append_reason "vm_reboot_or_unreachable_during"
        same_boot=0
    fi
    if [ -n "$uptime_after" ]; then
        previous_uptime="$uptime_after"
    fi

    serial_end="$(serial_size || printf 0)"
    if ! capture_serial_range "$serial_start" "$serial_end" "$run_dir/serial.log"; then
        append_reason "serial_missing_or_truncated"
        same_boot=0
    fi
    serial_sha="$(sha256sum "$run_dir/serial.log" | sed 's/[[:space:]].*//')"
    if grep -Ein "$serial_fail_regex" "$run_dir/serial.log" > "$run_dir/serial-failures.txt"; then
        serial_failures="$(wc -l < "$run_dir/serial-failures.txt" | tr -d '[:space:]')"
        append_reason "serial_failure_match"
    else
        : > "$run_dir/serial-failures.txt"
    fi

    checkout_path="$workdir/wos"
    commit="$(guest_output "git -C $(shell_quote "$checkout_path") rev-parse HEAD" 2>> "$collection_log" || true)"
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

    remote_submodules="$workdir/submodules.txt"
    submodules_sha="$(guest_output "sha256sum $(shell_quote "$remote_submodules")" 2>> "$collection_log" || true)"
    submodules_sha="$(printf '%s\n' "$submodules_sha" | sed -n '1{s/[[:space:]].*//;p;}')"
    if ! [[ "$submodules_sha" =~ ^[0-9a-f]{64}$ ]]; then
        append_reason "submodule_manifest_invalid_or_unavailable"
        same_submodules=0
    elif [ -z "$baseline_submodules" ]; then
        baseline_submodules="$submodules_sha"
    elif [ "$submodules_sha" != "$baseline_submodules" ]; then
        append_reason "submodule_manifest_mismatch"
        same_submodules=0
    fi

    while IFS=$'\t' read -r remote_relative local_name; do
        if ! copy_guest_file "$workdir/$remote_relative" "$run_dir/$local_name" "$collection_log"; then
            evidence_status=1
        fi
    done <<'EOF'
selfhost-report.tsv	selfhost-report.tsv
selfhost-detail.tsv	selfhost-detail.tsv
bootstrap-detail.tsv	bootstrap-detail.tsv
selfhost-cache-deltas.tsv	selfhost-cache-deltas.tsv
submodules.txt	submodules.txt
wos/build-selfhost/CMakeCache.txt	CMakeCache.txt
EOF

    if ! archive_guest_logs "$remote_log_dir" "$run_dir/command-logs.tar" "$collection_log"; then
        evidence_status=1
    fi
    guest_output "rm -rf $(shell_quote "$remote_log_dir")" >> "$collection_log" 2>&1 || {
        evidence_status=1
        printf 'failed to remove archived guest log directory: %s\n' "$remote_log_dir" >> "$collection_log"
    }
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

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$run_number" "$start_utc" "$end_utc" "$wall_ms" "$runner_status" "$evidence_status" \
        "$run_accepted" "$commit" "$submodules_sha" "$uptime_before" "$uptime_after" \
        "$boot_id_before" "$boot_id_after" "$serial_start" "$serial_end" "$serial_sha" "$serial_failures" \
        "$failure_reasons" "$run_dir" "$console_log" "${repo:-$DEFAULT_REPO}" "$mirror_commit" "$distdir_enabled" \
        "$distdir_manifest_sha256" "$timed_out" >> "$runs_tsv"

    if [ "$run_accepted" -eq 1 ]; then
        echo "[$run_label/$runs] PASS ${wall_ms}ms commit=$commit serial=${serial_start}-${serial_end}"
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
    printf 'host\t%s\n' "$host"
    printf 'jobs\t%s\n' "$jobs"
    printf 'workdir\t%s\n' "$workdir"
    printf 'repo\t%s\n' "${repo:-$DEFAULT_REPO}"
    printf 'mirror_file\t%s\n' "$mirror_file"
    printf 'mirror_commit\t%s\n' "$mirror_commit"
    printf 'distdir\t%s\n' "$distdir"
    printf 'canonical_distdir\t%s\n' "${canonical_distdir:-}"
    printf 'distdir_enabled\t%s\n' "$distdir_enabled"
    printf 'distdir_manifest_sha256\t%s\n' "$distdir_manifest_sha256"
    printf 'run_timeout_seconds\t%s\n' "$run_timeout_seconds"
    printf 'serial_log\t%s\n' "$serial_log"
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

echo "repeatability summary: $summary_json"
echo "accepted=$accepted/$runs runner_completed=$runner_completed failed=$failed pass=$overall_pass"

if [ "$overall_pass" -ne 1 ]; then
    exit 1
fi
