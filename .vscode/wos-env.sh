#!/bin/bash
# WOS Development Environment Profile
# Extracted from tools/build-llvm.sh
# This script sets up the environment variables needed for WOS development
#
# Usage:
#   1. Add to your ~/.bashrc: source /path/to/wos/.vscode/wos-env.sh
#   2. Or source manually: source .vscode/wos-env.sh
#   3. Or use VS Code terminal profile (automatically configured)

# Only initialize if not already loaded
if [ -n "$WOS_ENV_LOADED" ]; then
    return 0
fi

# Determine the workspace root (assuming this script is in .vscode/ subdirectory)
WOS_WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WOS_TOOLCHAIN_ROOT="$WOS_WORKSPACE_ROOT/toolchain"
WOS_TARGET_ARCH="x86_64-pc-wos"
WOS_HOST_TOOLS="$WOS_WORKSPACE_ROOT/tools/build/bin"

# Check if toolchain exists
if [ ! -d "$WOS_TOOLCHAIN_ROOT/target1" ]; then
    echo "Warning: WOS toolchain not found at $WOS_TOOLCHAIN_ROOT/target1"
    echo "Please run tools/build-llvm.sh to build the toolchain first."
    return 1
fi

# Save original environment if not already saved
if [ -z "$WOS_ORIGINAL_PATH" ]; then
    export WOS_ORIGINAL_PATH="$PATH"
fi
if [ -z "$WOS_ORIGINAL_LD_LIBRARY_PATH" ]; then
    export WOS_ORIGINAL_LD_LIBRARY_PATH="$LD_LIBRARY_PATH"
fi

# Core environment variables
export WOS_WORKSPACE_ROOT
export WOS_TOOLCHAIN_ROOT
export WOS_TARGET_ARCH

# Ninja status formatting
export NINJA_STATUS="[%f/%t %e] "

# Compiler and linker paths
export CC="$WOS_TOOLCHAIN_ROOT/target1/bin/clang"
export CXX="$WOS_TOOLCHAIN_ROOT/target1/bin/clang++"
export LD="$WOS_TOOLCHAIN_ROOT/target1/bin/ld.lld"

# Update PATH to include toolchain binaries (prepend to preserve user's PATH)
export PATH="$WOS_TOOLCHAIN_ROOT/target1/bin:$WOS_ORIGINAL_PATH"

# Library paths (overlay on existing LD_LIBRARY_PATH)
if [ -n "$WOS_ORIGINAL_LD_LIBRARY_PATH" ]; then
    export LD_LIBRARY_PATH="$WOS_TOOLCHAIN_ROOT/target1/lib:$WOS_ORIGINAL_LD_LIBRARY_PATH"
else
    export LD_LIBRARY_PATH="$WOS_TOOLCHAIN_ROOT/target1/lib"
fi

# Compiler flags for WOS development
export CFLAGS="--sysroot=$WOS_TOOLCHAIN_ROOT/target1 -fsanitize=safe-stack"
export CXXFLAGS="--sysroot=$WOS_TOOLCHAIN_ROOT/target1 -fsanitize=safe-stack"
export LDFLAGS="--sysroot=$WOS_TOOLCHAIN_ROOT/target1 -fsanitize=safe-stack"

# Additional useful variables
export WOS_SYSROOT="$WOS_TOOLCHAIN_ROOT/target1"
export WOS_CLANG_VERSION="21"
export WOS_CLANG_LIB_DIR="$WOS_TOOLCHAIN_ROOT/target1/lib/clang/$WOS_CLANG_VERSION/lib/$WOS_TARGET_ARCH"

# Path for tools
export PATH="$WOS_TOOLCHAIN_ROOT/target1/bin:$WOS_HOST_TOOLS:$PATH"

# Mark environment as loaded
export WOS_ENV_LOADED="1"

# Print status (only if running interactively)
if [ -t 1 ] && [ "${WOS_QUIET:-0}" != "1" ]; then
    echo "WOS development environment loaded:"
    echo "  Workspace: $WOS_WORKSPACE_ROOT"
    echo "  Toolchain: $WOS_TOOLCHAIN_ROOT"
    echo "  Target:    $WOS_TARGET_ARCH"
    echo "  CC:        $CC"
    echo "  CXX:       $CXX"
    echo "  Sysroot:   $WOS_SYSROOT"
fi

# Function to reset environment
wos_env_reset() {
    if [ -n "$WOS_ORIGINAL_PATH" ]; then
        export PATH="$WOS_ORIGINAL_PATH"
        unset WOS_ORIGINAL_PATH
    fi
    if [ -n "$WOS_ORIGINAL_LD_LIBRARY_PATH" ]; then
        export LD_LIBRARY_PATH="$WOS_ORIGINAL_LD_LIBRARY_PATH"
        unset WOS_ORIGINAL_LD_LIBRARY_PATH
    else
        unset LD_LIBRARY_PATH
    fi
    unset WOS_WORKSPACE_ROOT
    unset WOS_TOOLCHAIN_ROOT
    unset WOS_TARGET_ARCH
    unset WOS_SYSROOT
    unset WOS_CLANG_VERSION
    unset WOS_CLANG_LIB_DIR
    unset WOS_ENV_LOADED
    unset CC
    unset CXX
    unset LD
    unset CFLAGS
    unset CXXFLAGS
    unset LDFLAGS
    unset NINJA_STATUS
    echo "WOS environment reset."
}

# Function to show current environment
wos_env_show() {
    echo "WOS Environment Variables:"
    echo "  WOS_WORKSPACE_ROOT=$WOS_WORKSPACE_ROOT"
    echo "  WOS_TOOLCHAIN_ROOT=$WOS_TOOLCHAIN_ROOT"
    echo "  WOS_TARGET_ARCH=$WOS_TARGET_ARCH"
    echo "  WOS_SYSROOT=$WOS_SYSROOT"
    echo "  CC=$CC"
    echo "  CXX=$CXX"
    echo "  LD=$LD"
    echo "  LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
    echo "  CFLAGS=$CFLAGS"
    echo "  CXXFLAGS=$CXXFLAGS"
    echo "  LDFLAGS=$LDFLAGS"
    echo "  PATH (first part)=${PATH%%:*}:..."
}
