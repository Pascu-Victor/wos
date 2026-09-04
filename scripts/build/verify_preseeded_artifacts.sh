#!/bin/bash
# Validate artifacts produced by tools/bootstrap.sh before reusing them from a
# fresh CMake build tree.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=tools/ccache_env.sh
source "$WORKSPACE_ROOT/tools/ccache_env.sh"

if [ "$#" -lt 3 ]; then
    echo "usage: $0 LABEL STAMP PATH_OR_GLOB..." >&2
    exit 64
fi

label="$1"
stamp="$2"
shift 2

if wos_source_strict_enabled; then
    manifest="${WOS_PRESEEDED_ARTIFACT_MANIFEST:-}"
    if [ -z "$manifest" ]; then
        echo "ERROR: strict preseed verification requires WOS_PRESEEDED_ARTIFACT_MANIFEST." >&2
        echo "Create one before configuring with:" >&2
        echo "  python3 scripts/build/source_lock.py create-preseed-manifest --manifest FILE --root sysroot=SYSROOT --root busybox-install=BUSYBOX_INSTALL" >&2
        exit 1
    fi
    preseed_sysroot="${WOS_PRESEED_SYSROOT:-${WOS_SYSROOT_PATH:-$WORKSPACE_ROOT/toolchain/sysroot}}"
    preseed_busybox="${WOS_PRESEED_BUSYBOX_INSTALL:-${WOS_BUSYBOX_INSTALL_DIR:-$WORKSPACE_ROOT/toolchain/busybox-install}}"
    python3 "$(wos_source_lock_tool)" --lock "$(wos_source_lock_path)" \
        verify-preseed --manifest "$manifest" --label "$label" \
        --root "sysroot=$preseed_sysroot" \
        --root "busybox-install=$preseed_busybox" \
        "$@"
    mkdir -p "$(dirname "$stamp")"
    printf 'source-lock-sha256=%s\n' "$(sha256sum "$(wos_source_lock_path)" | awk '{print $1}')" > "$stamp"
    exit 0
fi

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
