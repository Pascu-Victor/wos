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
if [ -n "${WOS_ENV_LOADED:-}" ]; then
    return 0
fi
# Determine the workspace root (assuming this script is in .vscode/ subdirectory)
WOS_WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WOS_TOOLCHAIN_ROOT="$WOS_WORKSPACE_ROOT/toolchain"
WOS_TARGET_ARCH="x86_64-pc-wos"
WOS_HOST_TOOLS="$WOS_WORKSPACE_ROOT/tools/build/bin"
WOS_BIN="$WOS_WORKSPACE_ROOT/bin"
WOS_HOST_TOOLCHAIN_BIN="$WOS_TOOLCHAIN_ROOT/host/bin"
WOS_HOST_TOOLCHAIN_LIB="$WOS_TOOLCHAIN_ROOT/host/lib"
WOS_HOST_CMAKE="$WOS_HOST_TOOLCHAIN_BIN/cmake"
WOS_HOST_CTEST="$WOS_HOST_TOOLCHAIN_BIN/ctest"
WOS_HOST_CPACK="$WOS_HOST_TOOLCHAIN_BIN/cpack"
WOS_UNAME="$(uname -s 2>/dev/null || printf unknown)"
WOS_NATIVE_HOST=0
if [ "$WOS_UNAME" = "WOS" ]; then
    WOS_NATIVE_HOST=1
fi

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

# Join non-empty path entries with ':'.
wos_join_path() {
    local out=""
    local part

    for part in "$@"; do
        if [ -n "$part" ]; then
            if [ -n "$out" ]; then
                out="$out:$part"
            else
                out="$part"
            fi
        fi
    done

    printf '%s' "$out"
}

# Save original environment if not already saved
if [ -z "${WOS_ORIGINAL_PATH:-}" ]; then
    _wos_clean_path="$(wos_strip_path_entry "${PATH:-}" "$WOS_HOST_TOOLCHAIN_BIN")"
    _wos_clean_path="$(wos_strip_path_entry "$_wos_clean_path" "$WOS_HOST_TOOLS")"
    _wos_clean_path="$(wos_strip_path_entry "$_wos_clean_path" "$WOS_BIN")"
    export WOS_ORIGINAL_PATH="$_wos_clean_path"
    unset _wos_clean_path
fi
if [ -z "${WOS_ORIGINAL_LD_LIBRARY_PATH:-}" ]; then
    _wos_clean_ld="$(wos_strip_path_entry "${LD_LIBRARY_PATH:-}" "$WOS_HOST_TOOLCHAIN_LIB")"
    export WOS_ORIGINAL_LD_LIBRARY_PATH="$_wos_clean_ld"
    unset _wos_clean_ld
fi

wos_find_original_path_tool() {
    local tool="$1"

    PATH="$WOS_ORIGINAL_PATH" command -v "$tool" 2>/dev/null
}

# Linux hosts use the repo-built host toolchain. Native WOS hosts already ship
# clang/lld in the system image, so do not require or prepend toolchain/host.
if [ "$WOS_NATIVE_HOST" -eq 1 ]; then
    WOS_TOOLCHAIN_MODE="system"
    WOS_SYSROOT="/"
    WOS_CC="$(wos_find_original_path_tool clang || true)"
    WOS_CXX="$(wos_find_original_path_tool clang++ || true)"
    WOS_LD="$(wos_find_original_path_tool ld.lld || true)"

    if [ -z "$WOS_CC" ] || [ -z "$WOS_CXX" ] || [ -z "$WOS_LD" ]; then
        echo "Warning: WOS system clang toolchain not found in PATH."
        echo "Expected clang, clang++, and ld.lld to be shipped in the WOS system image."
        return 1
    fi

    WOS_HOST_CMAKE="$(wos_find_original_path_tool cmake || true)"
    WOS_HOST_CTEST="$(wos_find_original_path_tool ctest || true)"
    WOS_HOST_CPACK="$(wos_find_original_path_tool cpack || true)"
else
    WOS_TOOLCHAIN_MODE="workspace"
    WOS_SYSROOT="$WOS_TOOLCHAIN_ROOT/sysroot"
    WOS_CC="$WOS_HOST_TOOLCHAIN_BIN/clang"
    WOS_CXX="$WOS_HOST_TOOLCHAIN_BIN/clang++"
    WOS_LD="$WOS_HOST_TOOLCHAIN_BIN/ld.lld"

    if [ ! -x "$WOS_CC" ] || [ ! -x "$WOS_CXX" ] || [ ! -x "$WOS_LD" ]; then
        echo "Warning: WOS host toolchain not found at $WOS_HOST_TOOLCHAIN_BIN"
        echo "Please run tools/bootstrap.sh to build the toolchain first."
        return 1
    fi
fi

# Core environment variables
export WOS_WORKSPACE_ROOT
export WOS_TOOLCHAIN_ROOT
export WOS_TARGET_ARCH
export WOS_BIN
export WOS_HOST_TOOLCHAIN_BIN
export WOS_HOST_TOOLCHAIN_LIB
export WOS_HOST_CMAKE
export WOS_HOST_CTEST
export WOS_HOST_CPACK
export WOS_UNAME
export WOS_NATIVE_HOST
export WOS_TOOLCHAIN_MODE

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
export CC="$WOS_CC"
export CXX="$WOS_CXX"
export LD="$WOS_LD"

# Update PATH so WOS commands and build tools are first, but preserve a clean host path tail.
if [ "$WOS_NATIVE_HOST" -eq 1 ]; then
    export PATH="$(wos_join_path "$WOS_BIN" "$WOS_HOST_TOOLS" "$WOS_ORIGINAL_PATH")"
else
    export PATH="$(wos_join_path "$WOS_BIN" "$WOS_HOST_TOOLCHAIN_BIN" "$WOS_HOST_TOOLS" "$WOS_ORIGINAL_PATH")"
fi

# Library paths (overlay on existing LD_LIBRARY_PATH)
if [ "$WOS_NATIVE_HOST" -eq 1 ]; then
    if [ -n "$WOS_ORIGINAL_LD_LIBRARY_PATH" ]; then
        export LD_LIBRARY_PATH="$WOS_ORIGINAL_LD_LIBRARY_PATH"
    else
        unset LD_LIBRARY_PATH
    fi
else
    export LD_LIBRARY_PATH="$(wos_join_path "$WOS_HOST_TOOLCHAIN_LIB" "$WOS_ORIGINAL_LD_LIBRARY_PATH")"
fi

# Compiler flags for WOS development
export CFLAGS="--sysroot=$WOS_SYSROOT -std=c23"
if [ "$WOS_NATIVE_HOST" -eq 1 ]; then
    export CXXFLAGS="--sysroot=$WOS_SYSROOT -std=c++23 -isystem /usr/include/c++/v1"
    export LDFLAGS="--sysroot=$WOS_SYSROOT -L/usr/lib"
else
    export CXXFLAGS="--sysroot=$WOS_SYSROOT -std=c++23 -isystem $WOS_SYSROOT/include/c++/v1"
    export LDFLAGS="--sysroot=$WOS_SYSROOT"
fi

# Additional useful variables
export WOS_SYSROOT
_wos_clang_resource_dir="$("$CC" --target="$WOS_TARGET_ARCH" -print-resource-dir 2>/dev/null || true)"
if [ -n "$_wos_clang_resource_dir" ]; then
    WOS_CLANG_RESOURCE_DIR="$_wos_clang_resource_dir"
    WOS_CLANG_VERSION="${_wos_clang_resource_dir##*/}"
else
    WOS_CLANG_VERSION="22"
    WOS_CLANG_RESOURCE_DIR="$WOS_HOST_TOOLCHAIN_LIB/clang/$WOS_CLANG_VERSION"
fi
_wos_clang_runtime_dir="$("$CC" --target="$WOS_TARGET_ARCH" -print-runtime-dir 2>/dev/null || true)"
if [ -n "$_wos_clang_runtime_dir" ]; then
    WOS_CLANG_LIB_DIR="$_wos_clang_runtime_dir"
else
    WOS_CLANG_LIB_DIR="$WOS_CLANG_RESOURCE_DIR/lib/$WOS_TARGET_ARCH"
fi
export WOS_CLANG_VERSION
export WOS_CLANG_RESOURCE_DIR
export WOS_CLANG_LIB_DIR
unset _wos_clang_resource_dir _wos_clang_runtime_dir

# Mark environment as loaded
export WOS_ENV_LOADED="1"

# Print status (only if running interactively)
if [ -t 1 ] && [ "${WOS_QUIET:-0}" != "1" ]; then
    echo "WOS development environment loaded:"
    echo "  Workspace: $WOS_WORKSPACE_ROOT"
    echo "  Toolchain: $WOS_TOOLCHAIN_ROOT"
    echo "  Mode:      $WOS_TOOLCHAIN_MODE"
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
    if [ -n "${WOS_ORIGINAL_PATH:-}" ]; then
        export PATH="$WOS_ORIGINAL_PATH"
        unset WOS_ORIGINAL_PATH
    fi
    if [ -n "${WOS_ORIGINAL_LD_LIBRARY_PATH:-}" ]; then
        export LD_LIBRARY_PATH="$WOS_ORIGINAL_LD_LIBRARY_PATH"
        unset WOS_ORIGINAL_LD_LIBRARY_PATH
    else
        unset LD_LIBRARY_PATH
    fi
    unset WOS_WORKSPACE_ROOT
    unset WOS_TOOLCHAIN_ROOT
    unset WOS_TARGET_ARCH
    unset WOS_BIN
    unset WOS_HOST_TOOLCHAIN_BIN
    unset WOS_HOST_TOOLCHAIN_LIB
    unset WOS_HOST_CMAKE
    unset WOS_HOST_CTEST
    unset WOS_HOST_CPACK
    unset WOS_UNAME
    unset WOS_NATIVE_HOST
    unset WOS_TOOLCHAIN_MODE
    unset WOS_CMAKE
    unset WOS_CTEST
    unset WOS_CPACK
    unset WOS_SYSROOT
    unset WOS_CLANG_VERSION
    unset WOS_CLANG_RESOURCE_DIR
    unset WOS_CLANG_LIB_DIR
    unset WOS_ENV_LOADED
    unset WOS_CC
    unset WOS_CXX
    unset WOS_LD
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
    echo "  WOS_TOOLCHAIN_MODE=$WOS_TOOLCHAIN_MODE"
    echo "  WOS_CMAKE=${WOS_CMAKE:-}"
    echo "  WOS_CTEST=${WOS_CTEST:-}"
    echo "  WOS_CPACK=${WOS_CPACK:-}"
    echo "  WOS_CLANG_RESOURCE_DIR=$WOS_CLANG_RESOURCE_DIR"
    echo "  WOS_CLANG_LIB_DIR=$WOS_CLANG_LIB_DIR"
    echo "  CC=$CC"
    echo "  CXX=$CXX"
    echo "  LD=$LD"
    echo "  LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}"
    echo "  CFLAGS=$CFLAGS"
    echo "  CXXFLAGS=$CXXFLAGS"
    echo "  LDFLAGS=$LDFLAGS"
    echo "  PATH (first part)=${PATH%%:*}:..."
}
