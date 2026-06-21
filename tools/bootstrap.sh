#!/bin/bash
# Full WOS toolchain bootstrap.
# Builds the host compiler (clang/lld) then the WOS target toolchain.
#
# Usage: bootstrap.sh
#   Run from the WOS workspace root directory.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$WORKSPACE_ROOT"

echo "=== Phase 1: Host toolchain (clang/lld) ==="
"$SCRIPT_DIR/host-toolchain.sh"

echo ""
echo "=== Phase 2: Host CMake with WOS platform support ==="
"$WORKSPACE_ROOT/scripts/build/build_cmake_for_host.sh"

echo ""
echo "=== Phase 3: WOS target toolchain ==="
WOS_BUILD_CMAKE_FOR_HOST=0 "$SCRIPT_DIR/wos-toolchain.sh"

echo ""
echo "=== Bootstrap complete ==="
