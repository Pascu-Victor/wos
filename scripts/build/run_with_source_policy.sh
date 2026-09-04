#!/bin/bash
# CMake custom-command launcher for the strict/offline source policy. Keeping
# the policy in a real argv-preserving launcher avoids changing the quoting of
# commands such as `bash -c 'if ...'`.
set -euo pipefail

if [ "$#" -lt 7 ]; then
    echo "usage: $0 MODE LOCK STORE PRESEED_MANIFEST SYSROOT BUSYBOX_INSTALL COMMAND..." >&2
    exit 64
fi

export WOS_SOURCE_MODE="$1"
export WOS_SOURCE_LOCK="$2"
export WOS_SOURCE_STORE="$3"
export WOS_PRESEEDED_ARTIFACT_MANIFEST="$4"
export WOS_SYSROOT_PATH="$5"
export WOS_BUSYBOX_INSTALL_DIR="$6"
shift 6

exec "$@"
