#!/bin/bash
# Validate artifacts produced by tools/bootstrap.sh before reusing them from a
# fresh CMake build tree.
set -euo pipefail

if [ "$#" -lt 3 ]; then
    echo "usage: $0 LABEL STAMP PATH_OR_GLOB..." >&2
    exit 64
fi

label="$1"
stamp="$2"
shift 2

missing=0
for required in "$@"; do
    case "$required" in
        *[\*\?\[]*)
            if ! compgen -G "$required" >/dev/null; then
                echo "ERROR: preseeded $label artifact missing: $required" >&2
                missing=1
            fi
            ;;
        *)
            if [ ! -e "$required" ]; then
                echo "ERROR: preseeded $label artifact missing: $required" >&2
                missing=1
            fi
            ;;
    esac
done

if [ "$missing" -ne 0 ]; then
    echo "Run tools/bootstrap.sh first, or configure without WOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN." >&2
    exit 1
fi

mkdir -p "$(dirname "$stamp")"
: > "$stamp"
