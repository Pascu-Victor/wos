#!/bin/bash
# WOS Development Environment Profile
# Extracted from tools/bootstrap.sh
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
WOS_BIN="$WOS_WORKSPACE_ROOT/bin"
WOS_HOST_CMAKE="$WOS_TOOLCHAIN_ROOT/host/bin/cmake"
WOS_HOST_CTEST="$WOS_TOOLCHAIN_ROOT/host/bin/ctest"
WOS_HOST_CPACK="$WOS_TOOLCHAIN_ROOT/host/bin/cpack"

# Remove a single path entry from a colon-separated PATH-like variable.
wos_strip_path_entry() {
    local input="$1"
    local remove="$2"
    local out=""
    local part

    IFS=':' read -r -a _wos_parts <<< "$input"
    for part in "${_wos_parts[@]}"; do
        if [ -n "$part" ] && [ "$part" != "$remove" ]; then
            if [ -n "$out" ]; then
                out="$out:$part"
            else
                out="$part"
            fi
        fi
    done

    printf '%s' "$out"
}

# Check if toolchain exists
if [ ! -d "$WOS_TOOLCHAIN_ROOT/host" ]; then
    echo "Warning: WOS host toolchain not found at $WOS_TOOLCHAIN_ROOT/host"
    echo "Please run tools/bootstrap.sh to build the toolchain first."
    return 1
fi

# Save original environment if not already saved
if [ -z "$WOS_ORIGINAL_PATH" ]; then
    _wos_clean_path="$(wos_strip_path_entry "$PATH" "$WOS_TOOLCHAIN_ROOT/host/bin")"
    _wos_clean_path="$(wos_strip_path_entry "$_wos_clean_path" "$WOS_HOST_TOOLS")"
    _wos_clean_path="$(wos_strip_path_entry "$_wos_clean_path" "$WOS_BIN")"
    export WOS_ORIGINAL_PATH="$_wos_clean_path"
    unset _wos_clean_path
fi
if [ -z "$WOS_ORIGINAL_LD_LIBRARY_PATH" ]; then
    _wos_clean_ld="$(wos_strip_path_entry "$LD_LIBRARY_PATH" "$WOS_TOOLCHAIN_ROOT/host/lib")"
    export WOS_ORIGINAL_LD_LIBRARY_PATH="$_wos_clean_ld"
    unset _wos_clean_ld
fi

# Core environment variables
export WOS_WORKSPACE_ROOT
export WOS_TOOLCHAIN_ROOT
export WOS_TARGET_ARCH
export WOS_BIN
export WOS_HOST_CMAKE
export WOS_HOST_CTEST
export WOS_HOST_CPACK

if [ -x "$WOS_HOST_CMAKE" ]; then
    export WOS_CMAKE="$WOS_HOST_CMAKE"
fi
if [ -x "$WOS_HOST_CTEST" ]; then
    export WOS_CTEST="$WOS_HOST_CTEST"
fi
if [ -x "$WOS_HOST_CPACK" ]; then
    export WOS_CPACK="$WOS_HOST_CPACK"
fi

# Ninja status formatting
export NINJA_STATUS="[%f/%t %e] "

# Compiler and linker paths
export CC="$WOS_TOOLCHAIN_ROOT/host/bin/clang"
export CXX="$WOS_TOOLCHAIN_ROOT/host/bin/clang++"
export LD="$WOS_TOOLCHAIN_ROOT/host/bin/ld.lld"

# Update PATH so WOS commands, host tools, and build tools are first, but preserve a clean host path tail.
export PATH="$WOS_BIN:$WOS_TOOLCHAIN_ROOT/host/bin:$WOS_HOST_TOOLS:$WOS_ORIGINAL_PATH"

# Library paths (overlay on existing LD_LIBRARY_PATH)
if [ -n "$WOS_ORIGINAL_LD_LIBRARY_PATH" ]; then
    export LD_LIBRARY_PATH="$WOS_TOOLCHAIN_ROOT/host/lib:$WOS_ORIGINAL_LD_LIBRARY_PATH"
else
    export LD_LIBRARY_PATH="$WOS_TOOLCHAIN_ROOT/host/lib"
fi

# Compiler flags for WOS development
export CFLAGS="--sysroot=$WOS_TOOLCHAIN_ROOT/sysroot -std=c23"
export CXXFLAGS="--sysroot=$WOS_TOOLCHAIN_ROOT/sysroot -std=c++23"
export LDFLAGS="--sysroot=$WOS_TOOLCHAIN_ROOT/sysroot"

# Additional useful variables
export WOS_SYSROOT="$WOS_TOOLCHAIN_ROOT/sysroot"
export WOS_CLANG_VERSION="22"
export WOS_CLANG_LIB_DIR="$WOS_TOOLCHAIN_ROOT/host/lib/clang/$WOS_CLANG_VERSION/lib/$WOS_TARGET_ARCH"

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
    if [ -n "${WOS_CMAKE:-}" ]; then
        echo "  CMake:     $WOS_CMAKE"
    fi
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
    unset WOS_BIN
    unset WOS_HOST_CMAKE
    unset WOS_HOST_CTEST
    unset WOS_HOST_CPACK
    unset WOS_CMAKE
    unset WOS_CTEST
    unset WOS_CPACK
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
    echo "  WOS_BIN=$WOS_BIN"
    echo "  WOS_TARGET_ARCH=$WOS_TARGET_ARCH"
    echo "  WOS_SYSROOT=$WOS_SYSROOT"
    echo "  WOS_CMAKE=${WOS_CMAKE:-}"
    echo "  WOS_CTEST=${WOS_CTEST:-}"
    echo "  WOS_CPACK=${WOS_CPACK:-}"
    echo "  CC=$CC"
    echo "  CXX=$CXX"
    echo "  LD=$LD"
    echo "  LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
    echo "  CFLAGS=$CFLAGS"
    echo "  CXXFLAGS=$CXXFLAGS"
    echo "  LDFLAGS=$LDFLAGS"
    echo "  PATH (first part)=${PATH%%:*}:..."
}
