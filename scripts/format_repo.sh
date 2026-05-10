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

    COMPILE_COMMANDS="$BUILD_DIR/compile_commands.json"
    if [ ! -f "$COMPILE_COMMANDS" ]; then
        echo "error: missing compilation database: $COMPILE_COMMANDS" >&2
        echo "hint: configure/build the repo first so clang-tidy can resolve compile flags" >&2
        exit 1
    fi

    if ! command -v jq >/dev/null 2>&1; then
        echo "error: jq is required to filter compile_commands.json for clang-tidy" >&2
        exit 1
    fi

    TIDY_FILES=()
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

        if path_is_in_targets "$file"; then
            TIDY_FILES+=("$file")
        fi
    done < <(jq -r '.[].file' "$COMPILE_COMMANDS" | sort -u)

    if [ "${#TIDY_FILES[@]}" -eq 0 ]; then
        echo "No clang-tidy compilation units matched the selected paths."
        exit 0
    fi

    TIDY_ARGS=(
        -p "$BUILD_DIR"
        -quiet
        --use-color
        --header-filter="^$WOS_ROOT/modules/.*"
        --exclude-header-filter="^($WOS_ROOT/modules/extern/.*|$WOS_ROOT/toolchain/.*|$WOS_ROOT/modules/testprog/src/mandelbench/tinycthread\\.hpp|$WOS_ROOT/modules/testprog/src/mandelbench/tinycthread\\.cpp)"
        --extra-arg=-D_GLIBCXX_HOSTED
        --extra-arg=-D_DEFAULT_SOURCE
        --extra-arg=-D__WOS__=1
        --extra-arg=-D_GNU_SOURCE=1
    )
    if [ "$TIDY_FIX" -eq 1 ]; then
        TIDY_ARGS+=(-fix --checks=-portability-avoid-pragma-once)
        echo "Running clang-tidy with fixes on ${#TIDY_FILES[@]} files..."
    else
        TIDY_ARGS+=(--checks=-portability-avoid-pragma-once)
        echo "Running clang-tidy on ${#TIDY_FILES[@]} files..."
    fi

    if [ "${TIDY_RUNNER[0]}" != "clang-tidy" ]; then
        echo "Using clang-tidy-cache wrapper."
    fi

    for file in "${TIDY_FILES[@]}"; do
        echo "clang-tidy: $file"
        if [ "${TIDY_RUNNER[0]}" != "clang-tidy" ]; then
            CLANG_TIDY_CACHE_BINARY="$TIDY_BINARY" "${TIDY_RUNNER[@]}" "${TIDY_ARGS[@]}" "$file"
        else
            "${TIDY_RUNNER[@]}" "${TIDY_ARGS[@]}" "$file"
        fi
    done
    echo "clang-tidy completed successfully."
fi
