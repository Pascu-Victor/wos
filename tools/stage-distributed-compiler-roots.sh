#!/bin/bash
set -euo pipefail

transport="${WOS_DISTRIBUTED_COMPILER_TRANSPORT:-source}"
if [ "$transport" != staged ]; then
    exit 0
fi

state="${WOS_DISTRIBUTED_COMPILER_STATE:-}"
hosts_csv="${WOS_DISTRIBUTED_COMPILER_HOSTS:-}"
stage_base="${WOS_DISTRIBUTED_COMPILER_STAGE_BASE:-}"
if [ -z "$state" ] || [ -z "$hosts_csv" ] || [ -z "$stage_base" ]; then
    echo "ERROR: staged compiler roots require state, hosts, and a stage base" >&2
    exit 1
fi
if [ "$#" -eq 0 ]; then
    echo "ERROR: staged compiler roots require at least one directory" >&2
    exit 1
fi

case "$stage_base" in
    /*) ;;
    *)
        echo "ERROR: distributed compiler stage base must be absolute: $stage_base" >&2
        exit 1
        ;;
esac
stage_base="$(realpath "$stage_base")"
if [ ! -d "$stage_base" ]; then
    echo "ERROR: distributed compiler stage base is not a directory: $stage_base" >&2
    exit 1
fi
if [ "${#stage_base}" -ge 256 ]; then
    echo "ERROR: distributed compiler stage base is too long for a WKI VFS rule: $stage_base" >&2
    exit 1
fi

roots=()
archive_entries=()
declare -A root_seen=()
for requested_root in "$@"; do
    case "$requested_root" in
        *$'\n'*|*$'\r'*|*[\*\?\[]*)
            echo "ERROR: distributed compiler root contains unsafe characters: $requested_root" >&2
            exit 1
            ;;
    esac
    root="$(realpath "$requested_root")"
    case "$root" in
        "$stage_base"/*) ;;
        *)
            echo "ERROR: distributed compiler root escapes stage base: $root" >&2
            exit 1
            ;;
    esac
    if [ ! -d "$root" ]; then
        echo "ERROR: distributed compiler root is not a directory: $root" >&2
        exit 1
    fi
    if [ "${#root}" -ge 256 ]; then
        echo "ERROR: distributed compiler root is too long for a WKI VFS rule: $root" >&2
        exit 1
    fi
    if [ -n "${root_seen[$root]:-}" ]; then
        continue
    fi
    root_seen[$root]=1
    roots+=("$root")
    archive_entries+=("${root#/}")
done

IFS=, read -r -a hosts <<< "$hosts_csv"
if [ "${#hosts[@]}" -lt 2 ]; then
    echo "ERROR: staged compiler roots require at least two hosts" >&2
    exit 1
fi
local_hostname="$(hostname)"
if [ "${hosts[0]}" != "$local_hostname" ]; then
    echo "ERROR: staged compiler host list must name the submitter first: expected $local_hostname, got ${hosts[0]}" >&2
    exit 1
fi

stage_dir="$state.staging"
manifest="$state.local-roots"
mkdir -p "$stage_dir"
archive="$stage_dir/roots.$$.tar"
manifest_tmp=""
manifest_sorted=""
cleanup() {
    rm -f -- "$archive" 2>/dev/null || true
    if [ -n "$manifest_tmp" ]; then
        rm -f -- "$manifest_tmp" 2>/dev/null || true
    fi
    if [ -n "$manifest_sorted" ]; then
        rm -f -- "$manifest_sorted" 2>/dev/null || true
    fi
}
trap cleanup EXIT HUP INT TERM

tar -C / -cf "$archive" "${archive_entries[@]}"

stage_pids=()
stage_names=()
for ((host_index = 1; host_index < ${#hosts[@]}; host_index++)); do
    peer="${hosts[$host_index]}"
    command=(
        on "$peer" forward "+$archive" "-$stage_base" -- locally
        sh -eu -c '
            archive=$1
            base=$2
            shift 2
            for root do
                case "$root" in
                    "$base"/*) rm -rf -- "$root" ;;
                    *) exit 64 ;;
                esac
            done
            tar -C / -xf "$archive"
            for root do
                [ -d "$root" ]
            done
        ' sh "$archive" "$stage_base" "${roots[@]}"
    )
    "${command[@]}" &
    stage_pids+=("$!")
    stage_names+=("$peer")
done

stage_status=0
for ((stage_index = 0; stage_index < ${#stage_pids[@]}; stage_index++)); do
    if ! wait "${stage_pids[$stage_index]}"; then
        echo "ERROR: distributed compiler root staging failed on ${stage_names[$stage_index]}" >&2
        stage_status=1
    fi
done
if [ "$stage_status" -ne 0 ]; then
    exit "$stage_status"
fi

manifest_tmp="$(mktemp "$stage_dir/local-roots.XXXXXX")"
if [ -s "$manifest" ]; then
    cat "$manifest" > "$manifest_tmp"
fi
printf '%s\n' "${roots[@]}" >> "$manifest_tmp"
manifest_sorted="$manifest_tmp.sorted"
sort -u "$manifest_tmp" > "$manifest_sorted"
mv -f -- "$manifest_sorted" "$manifest"
manifest_sorted=""
rm -f -- "$manifest_tmp"
manifest_tmp=""

printf '[distributed-stage] peers=%s roots=%s bytes=%s\n' \
    "$((${#hosts[@]} - 1))" "${#roots[@]}" "$(stat -c %s -- "$archive")"
