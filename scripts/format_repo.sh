#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WOS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$WOS_ROOT/build"
FORMAT_JOBS="${WOS_FORMAT_JOBS:-4}"
EXCLUDED_PATHS=(
    "$WOS_ROOT/modules/testprog/src/mandelbench/tinycthread.cpp"
    "$WOS_ROOT/modules/testprog/src/mandelbench/tinycthread.hpp"
)
TIDY_EXCLUDED_PATHS=(
    "$WOS_ROOT/modules/testprog/src/mandelbench/tinycthread.cpp"
    "$WOS_ROOT/modules/testprog/src/mandelbench/tinycthread.hpp"
    "$WOS_ROOT/modules/testprog/src/mandelbench/mandelbench.cpp"
    "$WOS_ROOT/modules/testprog/src/mandelbench/mandelbench_wki.cpp"
)

usage() {
    cat <<'EOF'
Usage:
  ./scripts/format_repo.sh [options] [path ...]

Runs clang-format and clang-tidy using the repo's workspace configuration.

Defaults:
  path          modules
  format        enabled
  tidy          enabled
  tidy-fixes    disabled

Examples:
  ./scripts/format_repo.sh
  ./scripts/format_repo.sh modules/kern
  ./scripts/format_repo.sh tools/wosdbg
  ./scripts/format_repo.sh --check modules/kern/src/net/wki
  ./scripts/format_repo.sh --tidy-fix modules/kern/src/net/wki
  ./scripts/format_repo.sh --format-only

Options:
  --check         Run clang-format in check mode and clang-tidy without fixes
  --tidy-fix      Apply clang-tidy fixes
  --format-only   Run only clang-format
  --tidy-only     Run only clang-tidy
  --build-dir DIR Use a different compilation database directory
  --format-jobs N Run up to N clang-format workers in parallel (default: 4)

Environment:
  WOS_TIDY_CACHE=0   Disable clang-tidy-cache auto-detection
EOF
}

if ! command -v clang-format >/dev/null 2>&1; then
    echo "error: clang-format is not installed or not in PATH" >&2
    exit 1
fi

FORMAT_MODE="format"
RUN_FORMAT=1
RUN_TIDY=1
TIDY_FIX=0
TARGETS=()

while (($# > 0)); do
    case "$1" in
        --check)
            FORMAT_MODE="check"
            TIDY_FIX=0
            ;;
        --tidy-fix)
            TIDY_FIX=1
            ;;
        --format-only)
            RUN_TIDY=0
            ;;
        --tidy-only)
            RUN_FORMAT=0
            ;;
        --build-dir)
            BUILD_DIR="$2"
            shift
            ;;
        --format-jobs)
            FORMAT_JOBS="$2"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            TARGETS+=("$1")
            ;;
    esac
    shift
done

if [ "${#TARGETS[@]}" -eq 0 ]; then
    TARGETS=("modules")
fi

if ! [[ "$FORMAT_JOBS" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: --format-jobs must be a positive integer" >&2
    exit 1
fi

resolve_path() {
    realpath -m "$1"
}

for target in "${TARGETS[@]}"; do
    if [ ! -e "$WOS_ROOT/$target" ] && [ ! -e "$target" ]; then
        echo "error: target not found: $target" >&2
        exit 1
    fi
done

TARGET_ABS=()
for target in "${TARGETS[@]}"; do
    if [ -e "$target" ]; then
        TARGET_ABS+=("$(resolve_path "$target")")
    else
        TARGET_ABS+=("$(resolve_path "$WOS_ROOT/$target")")
    fi
done

path_is_in_targets() {
    local candidate="$1"

    for target in "${TARGET_ABS[@]}"; do
        if [ "$candidate" = "$target" ] || [[ "$candidate" == "$target/"* ]]; then
            return 0
        fi
    done

    return 1
}

path_is_excluded() {
    local candidate
    candidate="$(resolve_path "$1")"

    for excluded in "${EXCLUDED_PATHS[@]}"; do
        if [ "$candidate" = "$excluded" ]; then
            return 0
        fi
    done

    return 1
}

path_is_tidy_excluded() {
    local candidate
    candidate="$(resolve_path "$1")"

    for excluded in "${TIDY_EXCLUDED_PATHS[@]}"; do
        if [ "$candidate" = "$excluded" ]; then
            return 0
        fi
    done

    return 1
}

FIND_ARGS=(
    -type f
    "("
    -name "*.c"   -o
    -name "*.cc"  -o
    -name "*.cpp" -o
    -name "*.cxx" -o
    -name "*.h"   -o
    -name "*.hh"  -o
    -name "*.hpp" -o
    -name "*.hxx" -o
    -name "*.inc" -o
    -name "*.inl" -o
    -name "*.ipp" -o
    -name "*.tpp"
    ")"
)

fix_static_nodiscard_order() {
    local roots=("$@")
    local files=()
    local candidate

    for root in "${roots[@]}"; do
        if [ ! -e "$root" ]; then
            continue
        fi

        while IFS= read -r -d '' candidate; do
            if path_is_tidy_excluded "$candidate"; then
                continue
            fi
            files+=("$candidate")
        done < <(find "$root" "${FIND_ARGS[@]}" -print0)
    done

    if [ "${#files[@]}" -eq 0 ]; then
        return
    fi

    perl -0pi -e 's/\bstatic\s+\[\[nodiscard\]\]/[[nodiscard]] static/g' "${files[@]}"
}

CLANG_FORMAT_ARGS=(--style=file)
if [ "$FORMAT_MODE" = "format" ]; then
    CLANG_FORMAT_ARGS=(-i "${CLANG_FORMAT_ARGS[@]}")
else
    CLANG_FORMAT_ARGS=(--dry-run -Werror "${CLANG_FORMAT_ARGS[@]}")
fi

cd "$WOS_ROOT"

if [ "$RUN_FORMAT" -eq 1 ]; then
    mapfile -d '' FILES < <(find "${TARGETS[@]}" "${FIND_ARGS[@]}" -print0)

    FILTERED_FILES=()
    for file in "${FILES[@]}"; do
        if path_is_tidy_excluded "$file"; then
            continue
        fi
        FILTERED_FILES+=("$file")
    done
    FILES=("${FILTERED_FILES[@]}")

    if [ "${#FILES[@]}" -eq 0 ]; then
        echo "No clang-format-supported files found."
        exit 0
    fi

    echo "Running clang-format ($FORMAT_MODE) on ${#FILES[@]} files with $FORMAT_JOBS workers..."
    printf '%s\0' "${FILES[@]}" | xargs -0 -n 1 -P "$FORMAT_JOBS" clang-format "${CLANG_FORMAT_ARGS[@]}"
    echo "clang-format $FORMAT_MODE completed successfully."
fi

if [ "$RUN_TIDY" -eq 1 ]; then
    if ! command -v clang-tidy >/dev/null 2>&1; then
        echo "error: clang-tidy is not installed or not in PATH" >&2
        exit 1
    fi

    TIDY_RUNNER=(clang-tidy)
    TIDY_BINARY="$(command -v clang-tidy)"
    if [ "${WOS_TIDY_CACHE:-1}" != "0" ]; then
        if command -v clang-tidy-cache >/dev/null 2>&1; then
            TIDY_RUNNER=(clang-tidy-cache)
        elif command -v clang_tidy_cache >/dev/null 2>&1; then
            TIDY_RUNNER=(clang_tidy_cache)
        fi
    fi

    if ! command -v jq >/dev/null 2>&1; then
        echo "error: jq is required to filter compile_commands.json for clang-tidy" >&2
        exit 1
    fi

    COMPILE_DATABASE_DIRS=()
    add_compile_database_dir() {
        local compile_database_dir
        compile_database_dir="$(resolve_path "$1")"

        if [ ! -f "$compile_database_dir/compile_commands.json" ]; then
            return
        fi

        for existing in "${COMPILE_DATABASE_DIRS[@]}"; do
            if [ "$existing" = "$compile_database_dir" ]; then
                return
            fi
        done

        COMPILE_DATABASE_DIRS+=("$compile_database_dir")
    }

    add_compile_database_dir "$BUILD_DIR"
    add_compile_database_dir "$BUILD_DIR/tools"

    if [ "${#COMPILE_DATABASE_DIRS[@]}" -eq 0 ]; then
        echo "error: missing compilation database: $BUILD_DIR/compile_commands.json" >&2
        echo "hint: configure/build the repo first so clang-tidy can resolve compile flags" >&2
        exit 1
    fi

    TIDY_ENTRIES=()
    declare -A SEEN_TIDY_FILES=()
    for compile_database_dir in "${COMPILE_DATABASE_DIRS[@]}"; do
        compile_commands="$compile_database_dir/compile_commands.json"
        while IFS= read -r file; do
            case "$file" in
                "$WOS_ROOT"/modules/extern/*|"$WOS_ROOT"/toolchain/*)
                    continue
                    ;;
                *.c|*.cc|*.cpp|*.cxx)
                    ;;
                *)
                    continue
                    ;;
            esac

            if path_is_excluded "$file"; then
                continue
            fi

            if path_is_in_targets "$file" && [ -z "${SEEN_TIDY_FILES[$file]:-}" ]; then
                TIDY_ENTRIES+=("$compile_database_dir:$file")
                SEEN_TIDY_FILES["$file"]=1
            fi
        done < <(jq -r '.[].file' "$compile_commands" | sort -u)
    done

    if [ "${#TIDY_ENTRIES[@]}" -eq 0 ]; then
        echo "No clang-tidy compilation units matched the selected paths."
        exit 0
    fi

    TIDY_BASE_ARGS=(
        -quiet
        --use-color
    )

    TIDY_WOS_ARGS=(
        --extra-arg=-D_GLIBCXX_HOSTED
        --extra-arg=-D_DEFAULT_SOURCE
        --extra-arg=-D__WOS__=1
        --extra-arg=-D_GNU_SOURCE=1
    )

    TIDY_FIXUP_ROOTS=()
    collect_tidy_fixup_roots() {
        TIDY_FIXUP_ROOTS=("${TARGETS[@]}")
        for entry in "${TIDY_ENTRIES[@]}"; do
            file="${entry#*:}"
            TIDY_FIXUP_ROOTS+=("$(dirname "$file")")
        done
    }

    TIDY_CHECK_ARGS=()
    if [ "$TIDY_FIX" -eq 1 ]; then
        if ! command -v perl >/dev/null 2>&1; then
            echo "error: perl is required to normalize clang-tidy static/nodiscard fixes" >&2
            exit 1
        fi
        collect_tidy_fixup_roots
        fix_static_nodiscard_order "${TIDY_FIXUP_ROOTS[@]}"
        TIDY_CHECK_ARGS=(-fix)
        echo "Running clang-tidy with fixes on ${#TIDY_ENTRIES[@]} files..."
    else
        echo "Running clang-tidy on ${#TIDY_ENTRIES[@]} files..."
    fi

    if [ "${TIDY_RUNNER[0]}" != "clang-tidy" ]; then
        echo "Using clang-tidy-cache wrapper."
    fi

    for entry in "${TIDY_ENTRIES[@]}"; do
        compile_database_dir="${entry%%:*}"
        file="${entry#*:}"

        TIDY_ARGS=(-p "$compile_database_dir" "${TIDY_BASE_ARGS[@]}")
        case "$file" in
            "$WOS_ROOT"/modules/*)
                TIDY_ARGS+=("${TIDY_WOS_ARGS[@]}")
                ;;
        esac
        TIDY_ARGS+=("${TIDY_CHECK_ARGS[@]}")

        echo "clang-tidy: $file"
        if [ "${TIDY_RUNNER[0]}" != "clang-tidy" ]; then
            CLANG_TIDY_CACHE_BINARY="$TIDY_BINARY" "${TIDY_RUNNER[@]}" "${TIDY_ARGS[@]}" "$file"
        else
            "${TIDY_RUNNER[@]}" "${TIDY_ARGS[@]}" "$file"
        fi
    done
    if [ "$TIDY_FIX" -eq 1 ]; then
        fix_static_nodiscard_order "${TIDY_FIXUP_ROOTS[@]}"
    fi
    echo "clang-tidy completed successfully."
fi
