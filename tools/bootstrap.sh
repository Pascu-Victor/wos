#!/bin/bash
# Full WOS toolchain bootstrap.
# Builds the host compiler (clang/lld) then the WOS target toolchain.
#
# Usage: bootstrap.sh
#   Run from the WOS workspace root directory.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Phase 1: Host toolchain (clang/lld) ==="
"$SCRIPT_DIR/host-toolchain.sh"

echo ""
echo "=== Phase 2: WOS target toolchain ==="
"$SCRIPT_DIR/wos-toolchain.sh"

echo ""
echo "=== Bootstrap complete ==="
