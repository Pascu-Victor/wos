#!/bin/bash

wos_ccache_enabled() {
    case "${WOS_USE_CCACHE:-1}" in
        0|OFF|off|FALSE|false|NO|no)
            return 1
            ;;
        *)
            return 0
            ;;
    esac
}

wos_setup_ccache() {
    if ! wos_ccache_enabled; then
        WOS_CCACHE=""
        export WOS_CCACHE
        return 0
    fi

    if [ -z "${CCACHE_DIR:-}" ]; then
        export CCACHE_DIR="${TMPDIR:-/tmp}/wos-ccache"
        mkdir -p "$CCACHE_DIR"
    fi

    if [ -z "${WOS_CCACHE:-}" ]; then
        if command -v ccache >/dev/null 2>&1; then
            WOS_CCACHE="$(command -v ccache)"
        elif command -v cccache >/dev/null 2>&1; then
            WOS_CCACHE="$(command -v cccache)"
        else
            WOS_CCACHE=""
        fi
    fi

    export WOS_CCACHE
    if [ -n "$WOS_CCACHE" ]; then
        echo "Using compiler cache: $WOS_CCACHE"
    else
        echo "Compiler cache not found; building without compiler cache"
    fi
}

wos_setup_ccache_cmake_args() {
    WOS_CCACHE_CMAKE_ARGS=()
    if [ -n "${WOS_CCACHE:-}" ]; then
        WOS_CCACHE_CMAKE_ARGS=(
            "-DCMAKE_C_COMPILER_LAUNCHER=$WOS_CCACHE"
            "-DCMAKE_CXX_COMPILER_LAUNCHER=$WOS_CCACHE"
        )
    fi
}

wos_detail_file() {
    printf '%s\n' "${WOS_BUILD_DETAIL_TSV:-${WOS_BOOTSTRAP_DETAIL_TSV:-}}"
}

wos_now_ms() {
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

wos_timestamp_utc() {
    local epoch="${EPOCHREALTIME:-}"

    if [ -n "$epoch" ]; then
        local seconds="${epoch%.*}"
        local fraction="${epoch#*.}000"
        local prefix

        TZ=UTC printf -v prefix '%(%Y-%m-%dT%H:%M:%S)T' "$seconds"
        printf '%s.%sZ\n' "$prefix" "${fraction:0:3}"
        return 0
    fi

    python3 - <<'PY'
from datetime import datetime, timezone

print(datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"))
PY
}

wos_detail_header() {
    local output
    output="$(wos_detail_file)"
    [ -n "$output" ] || return 0

    case "$output" in
        */*) mkdir -p "${output%/*}" ;;
    esac
    if [ ! -s "$output" ]; then
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "timestamp_utc" "phase" "label" "elapsed_ms" "status" \
            "build_jobs" "ninja_jobs" "host_system" >> "$output"
    fi
}

wos_record_detail() {
    local phase="$1"
    local label="$2"
    local elapsed_ms="$3"
    local status="$4"
    local output

    output="$(wos_detail_file)"
    [ -n "$output" ] || return 0
    wos_detail_header
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$(wos_timestamp_utc)" "$phase" "$label" "$elapsed_ms" "$status" \
        "${WOS_BUILD_JOBS:-auto}" "${WOS_NINJA_JOBS:-auto}" \
        "$(uname -s 2>/dev/null || printf unknown)" >> "$output"
}

wos_run_timed_step_command() {
    local phase="$1"
    shift

    # Configure probes create transient source/build paths after the immutable
    # staged roots were copied to peers. Keep those small probes local; build
    # phases run only after their configured roots have been staged.
    if [ "$phase" = configure ] &&
        [ "${WOS_DISTRIBUTED_COMPILER:-0}" = 1 ] &&
        [ "${WOS_DISTRIBUTED_COMPILER_TRANSPORT:-source}" = staged ]; then
        WOS_DISTRIBUTED_COMPILER=0 "$@"
        return $?
    fi

    "$@"
}

wos_timed_step() {
    local phase="$1"
    local label="$2"
    shift 2

    if [ -z "$(wos_detail_file)" ]; then
        wos_run_timed_step_command "$phase" "$@"
        return $?
    fi

    local start_ms
    local end_ms
    local status
    local had_errexit=0

    start_ms="$(wos_now_ms)"
    case $- in
        *e*)
            had_errexit=1
            set +e
            ;;
    esac

    wos_run_timed_step_command "$phase" "$@"
    status=$?

    if [ "$had_errexit" -eq 1 ]; then
        set -e
    fi
    end_ms="$(wos_now_ms)"

    if [ "$status" -eq 0 ]; then
        wos_record_detail "$phase" "$label" "$((end_ms - start_ms))" "ok"
    else
        wos_record_detail "$phase" "$label" "$((end_ms - start_ms))" "fail:$status"
    fi
    return "$status"
}

wos_run_in_dir() {
    local dir="$1"
    shift

    (
        cd "$dir"
        "$@"
    )
}

wos_run_env_in_dir() {
    local dir="$1"
    shift

    (
        cd "$dir"
        env "$@"
    )
}

wos_remove_tree() {
    local path="$1"
    local attempts="${WOS_REMOVE_TREE_RETRIES:-5}"

    if [ -z "$path" ] || [ "$path" = "/" ]; then
        echo "ERROR: refusing to remove unsafe path '$path'" >&2
        return 1
    fi

    case "$attempts" in
        ''|*[!0-9]*|0)
            echo "ERROR: WOS_REMOVE_TREE_RETRIES must be a positive integer, got '$attempts'" >&2
            return 1
            ;;
    esac

    while [ "$attempts" -gt 0 ]; do
        rm -rf "$path" && return 0
        [ ! -e "$path" ] && return 0
        attempts=$((attempts - 1))
        [ "$attempts" -eq 0 ] && break
        if [ "$(uname -s 2>/dev/null || printf unknown)" = "WOS" ]; then
            sleep 1
        fi
    done

    rm -rf "$path"
}

wos_copy_tree_label() {
    local source_dir="$1"
    local dest_dir="$2"
    printf '%s->%s\n' "${source_dir##*/}" "${dest_dir##*/}"
}

wos_copy_tree_entries_excluding_impl() {
    local source_dir="$1"
    local dest_dir="$2"
    shift 2

    local entry
    local name
    local excluded
    local skip

    mkdir -p "$dest_dir"
    for entry in "$source_dir"/* "$source_dir"/.[!.]* "$source_dir"/..?*; do
        [ -e "$entry" ] || continue
        name="${entry##*/}"
        skip=0
        for excluded in "$@"; do
            if [ "$name" = "$excluded" ]; then
                skip=1
                break
            fi
        done
        [ "$skip" -eq 0 ] || continue
        cp -a "$entry" "$dest_dir/"
    done
}

wos_copy_tree_entries_excluding() {
    local source_dir="$1"
    local dest_dir="$2"
    local label

    label="$(wos_copy_tree_label "$source_dir" "$dest_dir")"
    wos_timed_step "copy_tree" "$label" wos_copy_tree_entries_excluding_impl "$@"
}

wos_dir_has_entries() {
    local dir="$1"
    local entry

    for entry in "$dir"/* "$dir"/.[!.]* "$dir"/..?*; do
        [ -e "$entry" ] && return 0
    done

    return 1
}

wos_refresh_file_mtime() {
    local file="$1"
    local tmp

    [ -f "$file" ] || return 0
    if touch "$file" 2>/dev/null; then
        return 0
    fi

    tmp="$file.wos-mtime.$$"
    rm -f "$tmp"
    cp "$file" "$tmp"
    rm -f "$file"
    mv "$tmp" "$file"
}

wos_source_strict_enabled() {
    case "${WOS_SOURCE_MODE:-}" in
        offline|strict)
            return 0
            ;;
        ""|developer|online)
            ;;
        *)
            echo "ERROR: WOS_SOURCE_MODE must be developer, online, strict, or offline." >&2
            return 2
            ;;
    esac

    case "${WOS_SOURCE_STRICT:-0}" in
        1|ON|on|TRUE|true|YES|yes)
            return 0
            ;;
        0|OFF|off|FALSE|false|NO|no|"")
            return 1
            ;;
        *)
            echo "ERROR: WOS_SOURCE_STRICT must be a boolean value." >&2
            return 2
            ;;
    esac
}

wos_source_lock_path() {
    printf '%s\n' "${WOS_SOURCE_LOCK:-$WORKSPACE_ROOT/configs/reproducibility/source-lock.v1.json}"
}

wos_source_store_path() {
    local store="${WOS_SOURCE_STORE:-}"
    if [ -z "$store" ]; then
        echo "ERROR: strict source mode requires WOS_SOURCE_STORE." >&2
        return 1
    fi
    printf '%s\n' "$store"
}

wos_source_lock_tool() {
    printf '%s\n' "${WOS_SOURCE_LOCK_TOOL:-$WORKSPACE_ROOT/scripts/build/source_lock.py}"
}

wos_verify_locked_archive() {
    local archive="$1"
    local strict_status=0
    wos_source_strict_enabled || strict_status=$?
    if [ "$strict_status" -eq 1 ]; then
        return 0
    fi
    [ "$strict_status" -eq 0 ] || return "$strict_status"
    python3 "$(wos_source_lock_tool)" --lock "$(wos_source_lock_path)" \
        verify-archive --file "$archive" --filename "${archive##*/}"
}

wos_materialize_locked_git() {
    local source_id="$1"
    local destination="$2"
    local strict_status=0
    wos_source_strict_enabled || strict_status=$?
    if [ "$strict_status" -eq 1 ]; then
        return 1
    fi
    [ "$strict_status" -eq 0 ] || return "$strict_status"
    python3 "$(wos_source_lock_tool)" --lock "$(wos_source_lock_path)" \
        materialize-git --store "$(wos_source_store_path)" \
        --id "$source_id" --destination "$destination"
}

wos_materialize_locked_archive_tree() {
    local filename="$1"
    local destination="$2"
    if ! wos_source_strict_enabled; then
        echo "ERROR: locked archive trees require strict source mode." >&2
        return 1
    fi
    python3 "$(wos_source_lock_tool)" --lock "$(wos_source_lock_path)" \
        materialize-archive-tree --store "$(wos_source_store_path)" \
        --filename "$filename" --destination "$destination" --strip-components 1
}

wos_download_file() {
    local label="$1"
    local dest="$2"
    local urls="$3"
    local attempts="${4:-${WOS_SOURCE_DOWNLOAD_ATTEMPTS:-3}}"
    local delay="${WOS_SOURCE_DOWNLOAD_RETRY_DELAY:-2}"
    local connect_timeout="${WOS_SOURCE_DOWNLOAD_CONNECT_TIMEOUT:-20}"
    local low_speed_limit="${WOS_SOURCE_DOWNLOAD_LOW_SPEED_LIMIT:-1}"
    local low_speed_time="${WOS_SOURCE_DOWNLOAD_LOW_SPEED_TIME:-60}"
    local url
    local attempt
    local status
    local distdir="${WOS_SOURCE_DISTDIR:-}"
    local candidate
    local basename
    local strict_status=0

    wos_source_strict_enabled || strict_status=$?
    if [ "$strict_status" -eq 0 ]; then
        basename="${dest##*/}"
        echo "Materializing locked $label from the offline source store..." >&2
        python3 "$(wos_source_lock_tool)" --lock "$(wos_source_lock_path)" \
            materialize-archive --store "$(wos_source_store_path)" \
            --filename "$basename" --destination "$dest"
        return $?
    fi
    [ "$strict_status" -eq 1 ] || return "$strict_status"

    case "$attempts" in
        ''|*[!0-9]*|0)
            echo "ERROR: download attempts must be a positive integer for $label, got '$attempts'" >&2
            return 1
            ;;
    esac
    case "$delay" in
        ''|*[!0-9]*)
            echo "ERROR: WOS_SOURCE_DOWNLOAD_RETRY_DELAY must be a non-negative integer, got '$delay'" >&2
            return 1
            ;;
    esac
    case "$connect_timeout" in
        ''|*[!0-9]*|0)
            echo "ERROR: WOS_SOURCE_DOWNLOAD_CONNECT_TIMEOUT must be a positive integer, got '$connect_timeout'" >&2
            return 1
            ;;
    esac
    case "$low_speed_limit" in
        ''|*[!0-9]*)
            echo "ERROR: WOS_SOURCE_DOWNLOAD_LOW_SPEED_LIMIT must be a non-negative integer, got '$low_speed_limit'" >&2
            return 1
            ;;
    esac
    case "$low_speed_time" in
        ''|*[!0-9]*|0)
            echo "ERROR: WOS_SOURCE_DOWNLOAD_LOW_SPEED_TIME must be a positive integer, got '$low_speed_time'" >&2
            return 1
            ;;
    esac
    if [ -z "$urls" ]; then
        echo "ERROR: no download URL configured for $label" >&2
        return 1
    fi

    if [ -n "$distdir" ]; then
        basename="${dest##*/}"
        for candidate in "$distdir/$basename"; do
            if [ -f "$candidate" ]; then
                echo "Using cached $label from $candidate" >&2
                cp "$candidate" "$dest.tmp"
                mv "$dest.tmp" "$dest"
                return 0
            fi
        done

        for url in $urls; do
            basename="${url##*/}"
            candidate="$distdir/$basename"
            if [ -f "$candidate" ]; then
                echo "Using cached $label from $candidate" >&2
                cp "$candidate" "$dest.tmp"
                mv "$dest.tmp" "$dest"
                return 0
            fi
        done
    fi

    for url in $urls; do
        attempt=1
        while [ "$attempt" -le "$attempts" ]; do
            if ! command -v curl >/dev/null 2>&1; then
                echo "ERROR: curl is unavailable while downloading $label." >&2
                return 1
            fi
            echo "Downloading $label from $url (attempt $attempt/$attempts)..." >&2
            if curl -fL \
                --connect-timeout "$connect_timeout" \
                --speed-limit "$low_speed_limit" \
                --speed-time "$low_speed_time" \
                "$url" -o "$dest.tmp"; then
                mv "$dest.tmp" "$dest"
                return 0
            fi
            status=$?
            rm -f "$dest.tmp"
            echo "warning: failed to download $url (status $status)" >&2
            if [ "$attempt" -lt "$attempts" ] && [ "$delay" -gt 0 ]; then
                sleep "$delay"
            fi
            attempt=$((attempt + 1))
        done
    done

    echo "ERROR: failed to download $label." >&2
    echo "Tried: $urls" >&2
    return 1
}

wos_prefetch_meson_subprojects() {
    local source_dir="$1"
    shift

    local attempts="${WOS_MESON_SUBPROJECT_FETCH_RETRIES:-3}"
    local delay="${WOS_MESON_SUBPROJECT_FETCH_RETRY_DELAY:-5}"
    case "$attempts" in
        ''|*[!0-9]*|0)
            echo "ERROR: WOS_MESON_SUBPROJECT_FETCH_RETRIES must be a positive integer, got '$attempts'" >&2
            return 1
            ;;
    esac
    case "$delay" in
        ''|*[!0-9]*)
            echo "ERROR: WOS_MESON_SUBPROJECT_FETCH_RETRY_DELAY must be a non-negative integer, got '$delay'" >&2
            return 1
            ;;
    esac

    local subproject
    local attempt
    for subproject in "$@"; do
        attempt=1
        while true; do
            if wos_fetch_meson_git_subproject "$source_dir" "$subproject"; then
                break
            fi
            if [ "$attempt" -ge "$attempts" ]; then
                echo "ERROR: failed to fetch Meson subproject '$subproject' after $attempts attempts" >&2
                return 1
            fi
            echo "Retrying Meson subproject '$subproject' after failed attempt $attempt/$attempts..."
            wos_remove_tree "$source_dir/subprojects/$subproject"
            attempt=$((attempt + 1))
            if [ "$delay" -gt 0 ]; then
                sleep "$delay"
            fi
        done
    done
}

wos_wrap_value() {
    local wrap="$1"
    local key="$2"

    sed -n "s/^[[:space:]]*$key[[:space:]]*=[[:space:]]*//p" "$wrap" | sed -n '1p'
}

wos_fetch_meson_git_subproject() {
    local source_dir="$1"
    local subproject="$2"
    local wrap="$source_dir/subprojects/$subproject.wrap"
    local dest="$source_dir/subprojects/$subproject"
    local url
    local revision
    local patch_directory
    local package_dir
    local current
    local low_speed_limit="${WOS_GIT_HTTP_LOW_SPEED_LIMIT:-1}"
    local low_speed_time="${WOS_GIT_HTTP_LOW_SPEED_TIME:-60}"
    local strict_status=0

    if [ ! -f "$wrap" ]; then
        echo "ERROR: missing Meson wrap file: $wrap" >&2
        return 1
    fi

    url="$(wos_wrap_value "$wrap" url)"
    revision="$(wos_wrap_value "$wrap" revision)"
    patch_directory="$(wos_wrap_value "$wrap" patch_directory)"
    if [ -z "$url" ] || [ -z "$revision" ]; then
        echo "ERROR: Meson wrap '$wrap' must provide url and revision" >&2
        return 1
    fi

    wos_source_strict_enabled || strict_status=$?
    if [ "$strict_status" -eq 0 ]; then
        wos_materialize_locked_git "mlibc-$subproject" "$dest" || return 1
    fi
    [ "$strict_status" -eq 0 ] || [ "$strict_status" -eq 1 ] || return "$strict_status"
    case "$low_speed_limit" in
        ''|*[!0-9]*)
            echo "ERROR: WOS_GIT_HTTP_LOW_SPEED_LIMIT must be a non-negative integer, got '$low_speed_limit'" >&2
            return 1
            ;;
    esac
    case "$low_speed_time" in
        ''|*[!0-9]*|0)
            echo "ERROR: WOS_GIT_HTTP_LOW_SPEED_TIME must be a positive integer, got '$low_speed_time'" >&2
            return 1
            ;;
    esac

    if [ -d "$dest/.git" ]; then
        current="$(git -C "$dest" rev-parse HEAD 2>/dev/null || true)"
        if [ "$current" = "$revision" ]; then
            if ! git -C "$dest" checkout -f HEAD >/dev/null 2>&1 ||
                ! git -C "$dest" diff-index --quiet HEAD --; then
                echo "warning: Meson subproject checkout at $dest is incomplete; refetching" >&2
            else
                if [ -n "$patch_directory" ]; then
                    package_dir="$source_dir/subprojects/packagefiles/$patch_directory"
                    if [ -d "$package_dir" ]; then
                        wos_copy_tree_entries_excluding "$package_dir" "$dest"
                    fi
                fi
                return 0
            fi
        fi
    fi

    wos_remove_tree "$dest"
    git init "$dest"
    git -C "$dest" remote add origin "$url"
    GIT_HTTP_LOW_SPEED_LIMIT="$low_speed_limit" \
        GIT_HTTP_LOW_SPEED_TIME="$low_speed_time" \
        git -C "$dest" fetch --depth 1 origin "$revision" || return 1
    git -C "$dest" checkout --detach FETCH_HEAD

    if ! git -C "$dest" diff-index --quiet HEAD --; then
        echo "ERROR: Meson subproject checkout at $dest is incomplete after fetch" >&2
        return 1
    fi

    if [ -n "$patch_directory" ]; then
        package_dir="$source_dir/subprojects/packagefiles/$patch_directory"
        if [ ! -d "$package_dir" ]; then
            echo "ERROR: missing Meson packagefiles directory: $package_dir" >&2
            return 1
        fi
        wos_copy_tree_entries_excluding "$package_dir" "$dest"
    fi
}

wos_ccache_prefix() {
    if [ -n "${WOS_CCACHE:-}" ]; then
        printf '%s ' "$WOS_CCACHE"
    fi
}

wos_meson_compiler_prefix() {
    if [ -n "${WOS_CCACHE:-}" ]; then
        printf "'%s', " "$WOS_CCACHE"
    fi
}

wos_build_jobs() {
    local jobs="${WOS_BUILD_JOBS:-}"

    if [ -n "$jobs" ]; then
        case "$jobs" in
            ''|*[!0-9]*|0)
                echo "ERROR: WOS_BUILD_JOBS must be a positive integer, got '$jobs'" >&2
                return 1
                ;;
            *)
                printf '%s\n' "$jobs"
                return 0
                ;;
        esac
    fi

    if [ -z "$jobs" ] && [ -n "${CMAKE_BUILD_PARALLEL_LEVEL:-}" ]; then
        jobs="$CMAKE_BUILD_PARALLEL_LEVEL"
    fi

    if [ "$(uname -s 2>/dev/null || printf unknown)" = "WOS" ] && [ -r /proc/stat ]; then
        local cpu_count=0
        local cpu_label
        while read -r cpu_label _; do
            case "$cpu_label" in
                cpu[0-9]*)
                    cpu_count=$((cpu_count + 1))
                    ;;
            esac
        done </proc/stat
        if [ "$cpu_count" -gt 0 ]; then
            jobs="$cpu_count"
        fi
    fi

    if [ -z "$jobs" ] && command -v nproc >/dev/null 2>&1; then
        jobs="$(nproc 2>/dev/null || true)"
    fi
    if [ -z "$jobs" ] && command -v getconf >/dev/null 2>&1; then
        jobs="$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
    fi
    if [ -z "$jobs" ] && command -v python3 >/dev/null 2>&1; then
        jobs="$(python3 - <<'PY' 2>/dev/null || true
import os
print(os.cpu_count() or 1)
PY
)"
    fi

    case "$jobs" in
        ''|*[!0-9]*|0)
            jobs=1
            ;;
    esac
    printf '%s\n' "$jobs"
}

wos_make_jobs() {
    local jobs="${WOS_MAKE_JOBS:-}"

    if [ -n "$jobs" ]; then
        case "$jobs" in
            ''|*[!0-9]*|0)
                echo "ERROR: WOS_MAKE_JOBS must be a positive integer, got '$jobs'" >&2
                return 1
                ;;
            *)
                printf '%s\n' "$jobs"
                return 0
                ;;
        esac
    fi

    wos_build_jobs
}

wos_make_jobserver_arg() {
    local jobs="${1:-${WOS_MAKE_JOBS:-}}"
    local style="${WOS_MAKE_JOBSERVER_STYLE:-}"

    case "$jobs" in
        ''|*[!0-9]*|0)
            echo "ERROR: GNU Make jobs must be a positive integer, got '$jobs'" >&2
            return 1
            ;;
        1)
            return 0
            ;;
    esac

    if [ -z "$style" ] && [ "$(uname -s 2>/dev/null || printf unknown)" = "WOS" ]; then
        style=pipe
    fi

    if [ -n "$style" ]; then
        case "$style" in
            pipe|fifo)
                printf '%s\n' "--jobserver-style=$style"
                ;;
            *)
                echo "ERROR: WOS_MAKE_JOBSERVER_STYLE must be pipe or fifo, got '$style'" >&2
                return 1
                ;;
        esac
    fi
}

wos_make() {
    local jobs="$1"
    shift

    local jobserver_arg
    local label
    jobserver_arg="$(wos_make_jobserver_arg "$jobs")"
    label="$(wos_make_label "$@")"
    if [ -n "$jobserver_arg" ]; then
        wos_timed_step "make" "$label" make "$jobserver_arg" -j"$jobs" "$@"
    else
        wos_timed_step "make" "$label" make -j"$jobs" "$@"
    fi
}

wos_make_label() {
    local make_dir=""
    local targets=""
    local expect_make_dir=0
    local arg

    for arg in "$@"; do
        if [ "$expect_make_dir" -eq 1 ]; then
            make_dir="$arg"
            expect_make_dir=0
            continue
        fi
        case "$arg" in
            -C)
                expect_make_dir=1
                ;;
            -C*)
                make_dir="${arg#-C}"
                ;;
            -*|*=*)
                ;;
            *)
                if [ -n "$targets" ]; then
                    targets="$targets,$arg"
                else
                    targets="$arg"
                fi
                ;;
        esac
    done

    if [ -z "$make_dir" ]; then
        make_dir="."
    fi
    if [ -z "$targets" ]; then
        targets="all"
    fi
    printf '%s:%s\n' "${make_dir##*/}" "$targets"
}

wos_ninja_jobs() {
    local jobs="${WOS_NINJA_JOBS:-}"

    if [ -n "$jobs" ]; then
        case "$jobs" in
            ''|*[!0-9]*|0)
                echo "ERROR: WOS_NINJA_JOBS must be a positive integer, got '$jobs'" >&2
                return 1
                ;;
            *)
                printf '%s\n' "$jobs"
                return 0
                ;;
        esac
    fi

    wos_build_jobs
}

wos_stage_distributed_build_roots() {
    local workspace_root="$1"
    local retained_root="$2"
    shift 2

    if [ "${WOS_DISTRIBUTED_COMPILER_TRANSPORT:-source}" != staged ]; then
        return 0
    fi

    local retained_roots="${WOS_DISTRIBUTED_COMPILER_RETAINED_ROOTS:-}"
    local requested_retained_root
    local canonical_retained_root
    local immutable_root
    local canonical_immutable_root
    while IFS= read -r requested_retained_root; do
        [ -n "$requested_retained_root" ] || continue
        if ! canonical_retained_root="$(realpath "$requested_retained_root")"; then
            echo "ERROR: distributed compiler retained root could not be resolved: $requested_retained_root" >&2
            return 1
        fi
        while IFS= read -r immutable_root; do
            [ -n "$immutable_root" ] || continue
            if ! canonical_immutable_root="$(realpath "$immutable_root")"; then
                echo "ERROR: distributed compiler immutable root could not be resolved: $immutable_root" >&2
                return 1
            fi
            case "$canonical_retained_root" in
                "$canonical_immutable_root"|"$canonical_immutable_root"/*)
                    # A generated build can name a nested source directory
                    # even though an earlier stage cached its immutable
                    # multi-project parent. Keep the complete cached parent
                    # active so sibling inputs stay peer-local.
                    if [ "${#canonical_immutable_root}" -lt "${#canonical_retained_root}" ]; then
                        canonical_retained_root="$canonical_immutable_root"
                    fi
                    ;;
            esac
        done <<< "${WOS_DISTRIBUTED_COMPILER_IMMUTABLE_ROOTS:-}"
        if [ -n "$retained_roots" ]; then
            retained_roots+=$'\n'
        fi
        retained_roots+="$canonical_retained_root"
    done <<< "$retained_root"

    WOS_DISTRIBUTED_COMPILER_RETAINED_ROOTS="$retained_roots" \
        "$workspace_root/tools/stage-distributed-compiler-roots.sh" "$@"
}
