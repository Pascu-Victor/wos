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
active_roots=()
declare -A root_seen=()
declare -A active_root_seen=()
canonical_root=""
validate_root() {
    local requested_root="$1"
    case "$requested_root" in
        *$'\n'*|*$'\r'*|*[\*\?\[]*)
            echo "ERROR: distributed compiler root contains unsafe characters: $requested_root" >&2
            exit 1
            ;;
    esac
    if ! canonical_root="$(realpath "$requested_root")"; then
        echo "ERROR: distributed compiler root could not be resolved: $requested_root" >&2
        exit 1
    fi
    case "$canonical_root" in
        "$stage_base"/*) ;;
        *)
            echo "ERROR: distributed compiler root escapes stage base: $canonical_root" >&2
            exit 1
            ;;
    esac
    if [ ! -d "$canonical_root" ]; then
        echo "ERROR: distributed compiler root is not a directory: $canonical_root" >&2
        exit 1
    fi
    if [ "${#canonical_root}" -ge 256 ]; then
        echo "ERROR: distributed compiler root is too long for a WKI VFS rule: $canonical_root" >&2
        exit 1
    fi
}

add_active_root() {
    local root="$1"
    if [ -n "${active_root_seen[$root]:-}" ]; then
        return 0
    fi
    active_root_seen[$root]=1
    active_roots+=("$root")
}

for requested_root in "$@"; do
    validate_root "$requested_root"
    root="$canonical_root"
    if [ -n "${root_seen[$root]:-}" ]; then
        continue
    fi
    root_seen[$root]=1
    roots+=("$root")
    add_active_root "$root"
done

# Retained roots were staged by an earlier call and remain unchanged on every
# peer. Keep them active without re-copying them on every toolchain phase.
while IFS= read -r retained_root; do
    [ -n "$retained_root" ] || continue
    validate_root "$retained_root"
    add_active_root "$canonical_root"
done <<< "${WOS_DISTRIBUTED_COMPILER_RETAINED_ROOTS:-}"

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

stage_now_ms() {
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

stage_started_ms="$(stage_now_ms)"
stage_generation="${WOS_DISTRIBUTED_COMPILER_STAGE_GENERATION:-}"
case "$stage_generation" in
    ''|*[!A-Za-z0-9._:+-]*)
        if [ -n "$stage_generation" ]; then
            echo "ERROR: distributed compiler stage generation contains unsafe characters" >&2
            exit 1
        fi
        ;;
esac
if [ "${#stage_generation}" -gt 128 ]; then
    echo "ERROR: distributed compiler stage generation is too long" >&2
    exit 1
fi

immutable_roots=()
if [ -n "$stage_generation" ]; then
    while IFS= read -r immutable_root; do
        [ -n "$immutable_root" ] || continue
        validate_root "$immutable_root"
        immutable_roots+=("$canonical_root")
    done <<< "${WOS_DISTRIBUTED_COMPILER_IMMUTABLE_ROOTS:-}"
fi

stage_cache_dir="${WOS_DISTRIBUTED_COMPILER_STAGE_CACHE_DIR:-/tmp/wos-distributed-stage-cache}"
case "$stage_cache_dir" in
    /*) ;;
    *)
        echo "ERROR: distributed compiler stage cache directory must be absolute: $stage_cache_dir" >&2
        exit 1
        ;;
esac

stage_identity_for_root() {
    local root="$1"
    local immutable_root
    local digest

    for immutable_root in "${immutable_roots[@]}"; do
        case "$root" in
            "$immutable_root"|"$immutable_root"/*)
                digest="$(printf 'wos-distributed-stage-v1\n%s\n%s\n' "$stage_generation" "$root" | sha256sum)"
                printf '%s\n' "${digest%% *}"
                return 0
                ;;
        esac
    done
    return 1
}

stage_marker_for_root() {
    local root="$1"
    local digest
    digest="$(printf '%s\n' "$root" | sha256sum)"
    printf '%s/%s.identity\n' "${stage_cache_dir%/}" "${digest%% *}"
}

peer_has_cached_root() {
    local peer="$1"
    local root="$2"
    local marker="$3"
    local identity="$4"

    on "$peer" locally sh -eu -c '
        root=$1
        marker=$2
        identity=$3
        [ -d "$root" ]
        [ -f "$marker" ]
        IFS= read -r cached_identity < "$marker"
        [ "$cached_identity" = "$identity" ]
    ' sh "$root" "$marker" "$identity"
}

stage_roots=()
archive_entries=()
stage_root_markers=()
stage_marker_roots=()
stage_marker_paths=()
stage_marker_identities=()
reused_root_count=0
for ((root_index = 0; root_index < ${#roots[@]}; root_index++)); do
    root="${roots[$root_index]}"
    identity=""
    marker=""
    if identity="$(stage_identity_for_root "$root")"; then
        marker="$(stage_marker_for_root "$root")"
        cache_hit=1
        for ((host_index = 1; host_index < ${#hosts[@]}; host_index++)); do
            if ! peer_has_cached_root "${hosts[$host_index]}" "$root" "$marker" "$identity"; then
                cache_hit=0
                break
            fi
        done
        if [ "$cache_hit" -eq 1 ]; then
            reused_root_count=$((reused_root_count + 1))
            continue
        fi
    fi

    stage_roots+=("$root")
    archive_entries+=("${root#/}")
    if [ -n "$identity" ]; then
        stage_root_markers+=("$marker")
        stage_marker_roots+=("$root")
        stage_marker_paths+=("$marker")
        stage_marker_identities+=("$identity")
    else
        stage_root_markers+=("$(stage_marker_for_root "$root")")
    fi
done

stage_dir="$state.staging"
manifest="$state.local-roots"
stage_retries="${WOS_DISTRIBUTED_COMPILER_STAGE_RETRIES:-3}"
stage_retry_delay="${WOS_DISTRIBUTED_COMPILER_STAGE_RETRY_DELAY_SECONDS:-1}"
stage_compression="${WOS_DISTRIBUTED_COMPILER_STAGE_COMPRESSION:-auto}"
case "$stage_retries" in
    ''|*[!0-9]*|0)
        echo "ERROR: distributed compiler stage retries must be a positive integer" >&2
        exit 1
        ;;
esac
case "$stage_retry_delay" in
    ''|*[!0-9]*)
        echo "ERROR: distributed compiler stage retry delay must be a non-negative integer" >&2
        exit 1
        ;;
esac
case "$stage_compression" in
    auto)
        stage_compression_threshold_kib="${WOS_DISTRIBUTED_COMPILER_STAGE_COMPRESSION_THRESHOLD_KIB:-131072}"
        case "$stage_compression_threshold_kib" in
            ''|*[!0-9]*)
                echo "ERROR: distributed compiler compression threshold must be a non-negative integer" >&2
                exit 1
                ;;
        esac
        stage_size_kib=0
        if [ "${#stage_roots[@]}" -gt 0 ]; then
            while read -r root_size_kib _; do
                stage_size_kib=$((stage_size_kib + root_size_kib))
            done < <(du -sk "${stage_roots[@]}")
        fi
        if [ "$stage_size_kib" -ge "$stage_compression_threshold_kib" ]; then
            stage_compression="gzip"
        else
            stage_compression="none"
        fi
        ;;
    gzip)
        ;;
    none)
        ;;
    *)
        echo "ERROR: distributed compiler stage compression must be auto, gzip, or none" >&2
        exit 1
        ;;
esac
case "$stage_compression" in
    gzip)
        archive_suffix=".tar.gz"
        archive_create_flag="-czf"
        archive_list_flag="-tzf"
        archive_extract_mode="gzip"
        ;;
    none)
        archive_suffix=".tar"
        archive_create_flag="-cf"
        archive_list_flag="-tf"
        archive_extract_mode="none"
        ;;
esac
probe_finished_ms="$(stage_now_ms)"
mkdir -p "$stage_dir"
archive="$stage_dir/roots.$$${archive_suffix}"
archive_tmp=""
root_links="$stage_dir/links.$$.list"
snapshot_stderr="$stage_dir/snapshot.$$.stderr"
manifest_tmp=""
manifest_sorted=""
cleanup() {
    rm -f -- "$archive" 2>/dev/null || true
    if [ -n "$archive_tmp" ]; then
        rm -f -- "$archive_tmp" 2>/dev/null || true
    fi
    rm -f -- "$root_links" 2>/dev/null || true
    rm -f -- "$snapshot_stderr" 2>/dev/null || true
    if [ -n "$manifest_tmp" ]; then
        rm -f -- "$manifest_tmp" 2>/dev/null || true
    fi
    if [ -n "$manifest_sorted" ]; then
        rm -f -- "$manifest_sorted" 2>/dev/null || true
    fi
}
trap cleanup EXIT HUP INT TERM

publish_active_roots() {
    manifest_tmp="$(mktemp "$stage_dir/local-roots.XXXXXX")"
    printf '%s\n' "${active_roots[@]}" > "$manifest_tmp"
    manifest_sorted="$manifest_tmp.sorted"
    sort -u "$manifest_tmp" > "$manifest_sorted"
    mv -f -- "$manifest_sorted" "$manifest"
    manifest_sorted=""
    rm -f -- "$manifest_tmp"
    manifest_tmp=""
}

# WOS reports a readdir/open race as ENOENT and continues traversing the rest
# of the tree, but BusyBox find and tar still return failure. An inaccessible
# entry cannot be useful to a peer compiler, so accept only that narrow failure
# class and keep rejecting every other diagnostic. The archive is validated
# separately before it can be published.
run_snapshot_command() {
    local label="$1"
    shift
    local saw_missing=0
    local saw_unexpected=0
    local line

    : > "$snapshot_stderr"
    if "$@" 2> "$snapshot_stderr"; then
        cat "$snapshot_stderr" >&2
        return 0
    fi

    while IFS= read -r line; do
        case "$line" in
            *"No such file or directory"*)
                saw_missing=1
                ;;
            "tar: error exit delayed from previous errors")
                ;;
            *)
                saw_unexpected=1
                ;;
        esac
    done < "$snapshot_stderr"
    cat "$snapshot_stderr" >&2
    if [ "$saw_missing" -eq 1 ] && [ "$saw_unexpected" -eq 0 ]; then
        echo "warning: distributed compiler $label omitted an entry that disappeared during traversal" >&2
        return 0
    fi
    return 1
}

# WOS BusyBox tar currently drops leading ../ components from relative symlink
# targets while extracting. Dereference trusted in-tree links when creating the
# archive, but prove on every snapshot attempt that each followed target stays
# inside stage_base. Capturing find output in a file also preserves its failure
# status when a live build directory changes during traversal.
validate_snapshot_roots() {
    local root
    local resolved_root
    local root_link
    local root_link_target

    : > "$root_links"
    for root do
        if ! resolved_root="$(realpath "$root")" ||
            [ "$resolved_root" != "$root" ] ||
            [ ! -d "$resolved_root" ]; then
            echo "ERROR: distributed compiler root changed during snapshot: $root" >&2
            return 1
        fi
        if ! run_snapshot_command "symlink scan" find "$root" -type l -print0 >> "$root_links"; then
            echo "ERROR: distributed compiler root changed during traversal: $root" >&2
            return 1
        fi
    done

    while IFS= read -r -d '' root_link; do
        if ! root_link_target="$(realpath "$root_link")"; then
            echo "ERROR: distributed compiler root contains a broken symlink: $root_link" >&2
            return 1
        fi
        case "$root_link_target" in
            "$stage_base"/*) ;;
            *)
                echo "ERROR: distributed compiler root symlink escapes stage base: $root_link -> $root_link_target" >&2
                return 1
                ;;
        esac
    done < "$root_links"
}

snapshot_started_ms="$probe_finished_ms"
if [ "${#stage_roots[@]}" -eq 0 ]; then
    snapshot_finished_ms="$(stage_now_ms)"
    publish_active_roots
    stage_finished_ms="$(stage_now_ms)"
    printf '[distributed-stage] peers=%s roots=%s staged_roots=0 reused_roots=%s bytes=0 compression=%s probe_ms=%s snapshot_ms=%s transfer_ms=0 total_ms=%s\n' \
        "$((${#hosts[@]} - 1))" "${#roots[@]}" "$reused_root_count" "$stage_compression" \
        "$((probe_finished_ms - stage_started_ms))" "$((snapshot_finished_ms - snapshot_started_ms))" \
        "$((stage_finished_ms - stage_started_ms))"
    exit 0
fi

validation_attempt=1
while ! validate_snapshot_roots "${stage_roots[@]}"; do
    if [ "$validation_attempt" -ge "$stage_retries" ]; then
        echo "ERROR: distributed compiler root validation failed after $validation_attempt attempts" >&2
        exit 1
    fi
    echo "warning: distributed compiler root validation changed during attempt $validation_attempt/$stage_retries; retrying" >&2
    if [ "$stage_retry_delay" -gt 0 ]; then
        sleep "$stage_retry_delay"
    fi
    validation_attempt=$((validation_attempt + 1))
done

archive_attempt=1
while :; do
    archive_tmp="$stage_dir/roots.$$.${archive_attempt}${archive_suffix}.tmp"
    rm -f -- "$archive_tmp" 2>/dev/null || true
    if run_snapshot_command "archive" tar -h -C / "$archive_create_flag" "$archive_tmp" "${archive_entries[@]}" &&
        tar "$archive_list_flag" "$archive_tmp" >/dev/null &&
        validate_snapshot_roots "${stage_roots[@]}"; then
        mv -f -- "$archive_tmp" "$archive"
        archive_tmp=""
        break
    fi
    rm -f -- "$archive_tmp" 2>/dev/null || true
    archive_tmp=""
    if [ "$archive_attempt" -ge "$stage_retries" ]; then
        echo "ERROR: distributed compiler root archive failed after $archive_attempt attempts" >&2
        exit 1
    fi
    echo "warning: distributed compiler root archive changed during attempt $archive_attempt/$stage_retries; retrying" >&2
    if [ "$stage_retry_delay" -gt 0 ]; then
        sleep "$stage_retry_delay"
    fi
    archive_attempt=$((archive_attempt + 1))
done
snapshot_finished_ms="$(stage_now_ms)"

stage_peer() {
    local peer="$1"
    local attempt=1
    local command

    while [ "$attempt" -le "$stage_retries" ]; do
        command=(
            on "$peer" forward "+$archive" "-$stage_base" -- locally
            sh -eu -c '
                archive=$1
                base=$2
                requested_count=$3
                extract_mode=$4
                marker_count=$5
                shift 5
                requested_index=0
                while [ "$requested_index" -lt "$requested_count" ]; do
                    root=$1
                    marker=$2
                    shift 2
                    case "$root" in
                        "$base"/*)
                            if [ "$marker" != - ]; then
                                rm -f -- "$marker"
                            fi
                            rm -rf -- "$root"
                            ;;
                        *) exit 64 ;;
                    esac
                    requested_index=$((requested_index + 1))
                done
                case "$extract_mode" in
                    gzip)
                        local_archive=/tmp/wos-distributed-stage.$$.tar.gz
                        trap "rm -f -- $local_archive" EXIT HUP INT TERM
                        cp -- "$archive" "$local_archive"
                        tar -C / -xzf "$local_archive"
                        rm -f -- "$local_archive"
                        trap - EXIT HUP INT TERM
                        ;;
                    none) tar -C / -xf "$archive" ;;
                    *) exit 65 ;;
                esac
                marker_index=0
                while [ "$marker_index" -lt "$marker_count" ]; do
                    root=$1
                    marker=$2
                    identity=$3
                    shift 3
                    [ -d "$root" ]
                    mkdir -p -- "${marker%/*}"
                    marker_tmp="$marker.$$.tmp"
                    trap "rm -f -- $marker_tmp" EXIT HUP INT TERM
                    printf "%s\n" "$identity" > "$marker_tmp"
                    mv -f -- "$marker_tmp" "$marker"
                    trap - EXIT HUP INT TERM
                    marker_index=$((marker_index + 1))
                done
                for root do
                    [ -d "$root" ]
                done
            ' sh "$archive" "$stage_base" "${#stage_roots[@]}" "$archive_extract_mode" "${#stage_marker_roots[@]}"
        )
        for ((root_index = 0; root_index < ${#stage_roots[@]}; root_index++)); do
            command+=("${stage_roots[$root_index]}" "${stage_root_markers[$root_index]}")
        done
        for ((marker_index = 0; marker_index < ${#stage_marker_roots[@]}; marker_index++)); do
            command+=(
                "${stage_marker_roots[$marker_index]}"
                "${stage_marker_paths[$marker_index]}"
                "${stage_marker_identities[$marker_index]}"
            )
        done
        command+=("${active_roots[@]}")
        if "${command[@]}"; then
            return 0
        fi
        if [ "$attempt" -ge "$stage_retries" ]; then
            break
        fi
        echo "warning: distributed compiler root staging on $peer failed attempt $attempt/$stage_retries; retrying" >&2
        if [ "$stage_retry_delay" -gt 0 ]; then
            sleep "$stage_retry_delay"
        fi
        attempt=$((attempt + 1))
    done
    return 1
}

transfer_started_ms="$snapshot_finished_ms"
stage_pids=()
stage_names=()
for ((host_index = 1; host_index < ${#hosts[@]}; host_index++)); do
    peer="${hosts[$host_index]}"
    stage_peer "$peer" &
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

publish_active_roots

stage_finished_ms="$(stage_now_ms)"
printf '[distributed-stage] peers=%s roots=%s staged_roots=%s reused_roots=%s bytes=%s compression=%s probe_ms=%s snapshot_ms=%s transfer_ms=%s total_ms=%s\n' \
    "$((${#hosts[@]} - 1))" "${#roots[@]}" "${#stage_roots[@]}" "$reused_root_count" "$(stat -c %s -- "$archive")" \
    "$stage_compression" "$((probe_finished_ms - stage_started_ms))" "$((snapshot_finished_ms - snapshot_started_ms))" \
    "$((stage_finished_ms - transfer_started_ms))" \
    "$((stage_finished_ms - stage_started_ms))"
