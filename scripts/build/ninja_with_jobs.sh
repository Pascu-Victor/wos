#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"

WOS_NINJA_JOBS="$(wos_ninja_jobs)"
exec ninja -j"$WOS_NINJA_JOBS" "$@"
