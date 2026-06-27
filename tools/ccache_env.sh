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

wos_copy_tree_entries_excluding() {
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
    tmp="$file.wos-mtime.$$"
    rm -f "$tmp"
    cp "$file" "$tmp"
    rm -f "$file"
    mv "$tmp" "$file"
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
            if [ -n "$patch_directory" ]; then
                package_dir="$source_dir/subprojects/packagefiles/$patch_directory"
                if [ -d "$package_dir" ]; then
                    wos_copy_tree_entries_excluding "$package_dir" "$dest"
                fi
            fi
            return 0
        fi
    fi

    wos_remove_tree "$dest"
    git init "$dest"
    git -C "$dest" remote add origin "$url"
    GIT_HTTP_LOW_SPEED_LIMIT="$low_speed_limit" \
        GIT_HTTP_LOW_SPEED_TIME="$low_speed_time" \
        git -C "$dest" fetch --depth 1 origin "$revision"
    git -C "$dest" checkout --detach FETCH_HEAD

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

    if command -v nproc >/dev/null 2>&1; then
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

    if [ "$(uname -s 2>/dev/null || printf unknown)" = "WOS" ]; then
        printf '1\n'
        return 0
    fi

    wos_build_jobs
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

    if [ "$(uname -s 2>/dev/null || printf unknown)" = "WOS" ]; then
        printf '1\n'
        return 0
    fi

    wos_build_jobs
}
