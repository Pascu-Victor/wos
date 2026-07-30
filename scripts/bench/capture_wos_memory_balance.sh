#!/bin/bash
# Capture and validate one coherent physical-memory checkpoint on each WOS node.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WOS_SSH="${WOS_MEMORY_BALANCE_SSH:-$WORKSPACE_ROOT/scripts/remote/wos_ssh.sh}"

systems="wos-0,wos-1,wos-2,wos-3"
output_dir=""
label=""
require_quiescent=0

usage() {
    cat <<'EOF'
Usage: scripts/bench/capture_wos_memory_balance.sh [options]

Options:
  --systems CSV       WOS hostnames (default: wos-0,wos-1,wos-2,wos-3)
  --output-dir PATH   New or existing parent evidence directory
  --label NAME        New checkpoint subdirectory name
  --require-quiescent Require all transient WKI/IPC/VFS counters to be zero
  -h, --help          Show this help

The script requires schema-2 /proc/memacc output and rejects any nonzero
physical identity mismatch or untracked-unreclaimable page count.
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --systems)
            systems="${2:-}"
            shift 2
            ;;
        --output-dir)
            output_dir="${2:-}"
            shift 2
            ;;
        --label)
            label="${2:-}"
            shift 2
            ;;
        --require-quiescent)
            require_quiescent=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "unknown option: $1"
            ;;
    esac
done

[ -n "$output_dir" ] || die "--output-dir is required"
case "$label" in
    ''|*[!A-Za-z0-9._-]*) die "--label must contain only A-Z, a-z, 0-9, dot, underscore, or dash" ;;
esac
case "$systems" in
    ''|*[!A-Za-z0-9._,-]*|,*|*,|*,,*) die "--systems must be a comma-separated hostname list" ;;
esac

checkpoint_dir="$output_dir/$label"
[ ! -e "$checkpoint_dir" ] || die "checkpoint already exists: $checkpoint_dir"
mkdir -p "$checkpoint_dir"

kv_value() {
    local line="$1"
    local key="$2"
    local field
    for field in $line; do
        case "$field" in
            "$key"=*)
                printf '%s\n' "${field#*=}"
                return 0
                ;;
        esac
    done
    return 1
}

require_zero_key() {
    local line="$1"
    local key="$2"
    local context="$3"
    local value
    value="$(kv_value "$line" "$key")" || die "$context is missing $key"
    [ "$value" = 0 ] || die "$context has $key=$value"
}

validate_quiescent_netdiag() {
    local file="$1"
    local host="$2"
    local compute ipc vfs_server

    compute="$(sed -n '/^wki_compute /p' "$file")"
    [ -n "$compute" ] || die "$host netdiag is missing wki_compute"
    require_zero_key "$compute" submitted_active "$host wki_compute"
    require_zero_key "$compute" running_active "$host wki_compute"
    require_zero_key "$compute" pending_complete "$host wki_compute"
    require_zero_key "$compute" truncated "$host wki_compute"

    ipc="$(sed -n '/^wki_ipc /p' "$file")"
    [ -n "$ipc" ] || die "$host netdiag is missing wki_ipc"
    for key in exports proxies active_pumps ring_used pending_deliveries pending_bytes export_flush_queue export_close_pending \
        export_close_waiting_for_bytes proxy_close_queue dev_op_queue; do
        require_zero_key "$ipc" "$key" "$host wki_ipc"
    done

    vfs_server="$(sed -n '/^wki_vfs_server /p' "$file")"
    [ -n "$vfs_server" ] || die "$host netdiag is missing wki_vfs_server"
    require_zero_key "$vfs_server" active "$host wki_vfs_server"
    require_zero_key "$vfs_server" retiring "$host wki_vfs_server"

    if grep -Eq '^wki_compute_task |^wki_ipc_diag ' "$file"; then
        die "$host retains WKI compute or IPC objects at a quiescent checkpoint"
    fi
    if ! awk '
        /^wki_vfs_proxy / {
            op_pending = attach_pending = -1
            for (i = 1; i <= NF; ++i) {
                if ($i ~ /^op_pending=/) {
                    split($i, value, "=")
                    op_pending = value[2]
                } else if ($i ~ /^attach_pending=/) {
                    split($i, value, "=")
                    attach_pending = value[2]
                }
            }
            if (op_pending != 0 || attach_pending != 0) bad = 1
        }
        END { exit bad ? 1 : 0 }
    ' "$file"; then
        die "$host has an active or pending persistent WKI VFS proxy operation"
    fi
    # Channel ACK state is transport sequencing, not a live workload/object
    # counter: an idle control channel may retain a delayed cumulative ACK
    # without retaining task, IPC, VFS, or physical-memory ownership.
}

timestamp_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf 'checkpoint\ttimestamp_utc\thost\ttotal_pages\tfree_pages\towner_pages\tidentity_pages\tidentity_mismatch_pages\tuntracked_unreclaimable_pages\tstatus\n' \
    > "$checkpoint_dir/status.tsv"

IFS=, read -r -a system_array <<< "$systems"
for host in "${system_array[@]}"; do
    case "$host" in
        ''|*[!A-Za-z0-9._-]*) die "unsafe hostname in --systems: $host" ;;
    esac

    summary_file="$checkpoint_dir/$host-memacc-summary.txt"
    all_file="$checkpoint_dir/$host-memacc-all.txt"
    netdiag_file="$checkpoint_dir/$host-wki-netdiag.txt"
    kipc_file="$checkpoint_dir/$host-kipcstat.txt"
    cpustate_file="$checkpoint_dir/$host-kcpustate.txt"
    meminfo_file="$checkpoint_dir/$host-meminfo.txt"

    # `raw all` takes one internally coherent memacc snapshot. Derive the
    # summary evidence from that same read so both the balance equation and
    # quiescence checks describe one bounded instant; a preceding remote
    # summary process would itself appear transiently on the dead queue.
    "$WOS_SSH" "$host" /usr/bin/memacc raw all > "$all_file"
    awk '
        NR == 1 && $0 == "== summary ==" { in_summary = 1; next }
        in_summary && /^== / { exit }
        in_summary { print }
    ' "$all_file" > "$summary_file"
    [ -s "$summary_file" ] || die "$host memacc all snapshot is missing the summary section"
    "$WOS_SSH" "$host" /usr/bin/cat /proc/wki/netdiag > "$netdiag_file"
    "$WOS_SSH" "$host" /usr/bin/cat /proc/kipcstat > "$kipc_file"
    "$WOS_SSH" "$host" /usr/bin/cat /proc/kcpustate > "$cpustate_file"
    "$WOS_SSH" "$host" /usr/bin/cat /proc/meminfo > "$meminfo_file"

    summary="$(sed -n '1p' "$summary_file")"
    [ "${summary%% *}" = summary ] || die "$host memacc summary row is missing"
    schema="$(kv_value "$summary" schema)" || die "$host summary is missing schema"
    [ "$schema" = 2 ] || die "$host summary schema is $schema, expected 2"

    total_pages="$(kv_value "$summary" total_pages)" || die "$host summary is missing total_pages"
    free_pages="$(kv_value "$summary" free_pages)" || die "$host summary is missing free_pages"
    owner_pages="$(kv_value "$summary" owner_pages)" || die "$host summary is missing owner_pages"
    identity_pages="$(kv_value "$summary" identity_pages)" || die "$host summary is missing identity_pages"
    physical_address_pages="$(kv_value "$summary" physical_address_pages)" || die "$host summary is missing physical_address_pages"
    physical_address_identity_pages="$(kv_value "$summary" physical_address_identity_pages)" ||
        die "$host summary is missing physical_address_identity_pages"
    mismatch_pages="$(kv_value "$summary" identity_mismatch_pages)" || die "$host summary is missing identity_mismatch_pages"
    untracked_pages="$(kv_value "$summary" untracked_unreclaimable_pages)" ||
        die "$host summary is missing untracked_unreclaimable_pages"

    [ "$total_pages" = "$identity_pages" ] || die "$host identity $identity_pages does not equal total $total_pages"
    [ "$physical_address_pages" = "$physical_address_identity_pages" ] ||
        die "$host physical-address identity $physical_address_identity_pages does not equal total $physical_address_pages"
    [ "$mismatch_pages" = 0 ] || die "$host physical identity mismatch is $mismatch_pages pages"
    [ "$untracked_pages" = 0 ] || die "$host untracked unreclaimable count is $untracked_pages pages"
    if grep -Eq '^physical_owner name=(unaccounted|unknown|other|estimated)([[:space:]]|$)' "$summary_file"; then
        die "$host exported a forbidden residual physical-owner category"
    fi
    if ! awk '
        /^physical_owner / {
            rows++
            have_pages = have_bytes = have_objects = have_lifetime = have_reclaim = have_bound = 0
            for (i = 1; i <= NF; ++i) {
                if ($i ~ /^pages=/) have_pages = 1
                else if ($i ~ /^bytes=/) have_bytes = 1
                else if ($i ~ /^objects=/) have_objects = 1
                else if ($i ~ /^lifetime=/) have_lifetime = 1
                else if ($i ~ /^reclaimability=/) have_reclaim = 1
                else if ($i ~ /^scaling_bound=/) have_bound = 1
            }
            if (!(have_pages && have_bytes && have_objects && have_lifetime && have_reclaim && have_bound)) bad = 1
        }
        END { exit rows > 0 && !bad ? 0 : 1 }
    ' "$summary_file"; then
        die "$host has an incomplete physical-owner row"
    fi

    if [ "$require_quiescent" -eq 1 ]; then
        validate_quiescent_netdiag "$netdiag_file" "$host"
        if grep -Eq '^proc .* (name|cmd)=[^[:space:]]*(cmake|ninja|clang|clang%2B%2B|ld.lld|bootstrap)' "$all_file"; then
            die "$host retains a self-host build process at a quiescent checkpoint"
        fi
        # The SSH probe itself can leave its preceding helper transiently on a
        # scheduler GC queue. Those pages remain attributed to that concrete
        # process until GC; reject named build processes above, not probe GC.
    fi

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\tpass\n' "$label" "$timestamp_utc" "$host" "$total_pages" "$free_pages" \
        "$owner_pages" "$identity_pages" "$mismatch_pages" "$untracked_pages" >> "$checkpoint_dir/status.tsv"
done

printf 'checkpoint=%s nodes=%s status=pass evidence=%s\n' "$label" "${#system_array[@]}" "$checkpoint_dir"
