#!/bin/bash

wos_ccache_enabled() {
    case "${WOS_USE_CCACHE:-1}" in
        0|OFF|off|FALSE|false|NO|no)
            return 1
            ;;
        *)
            return 0
            ;;
    esac
}

wos_setup_ccache() {
    if ! wos_ccache_enabled; then
        WOS_CCACHE=""
        export WOS_CCACHE
        return 0
    fi

    if [ -z "${WOS_CCACHE:-}" ]; then
        if command -v ccache >/dev/null 2>&1; then
            WOS_CCACHE="$(command -v ccache)"
        elif command -v cccache >/dev/null 2>&1; then
            WOS_CCACHE="$(command -v cccache)"
        else
            WOS_CCACHE=""
        fi
    fi

    export WOS_CCACHE
    if [ -n "$WOS_CCACHE" ]; then
        echo "Using compiler cache: $WOS_CCACHE"
    else
        echo "Compiler cache not found; building without compiler cache"
    fi
}

wos_setup_ccache_cmake_args() {
    WOS_CCACHE_CMAKE_ARGS=()
    if [ -n "${WOS_CCACHE:-}" ]; then
        WOS_CCACHE_CMAKE_ARGS=(
            "-DCMAKE_C_COMPILER_LAUNCHER=$WOS_CCACHE"
            "-DCMAKE_CXX_COMPILER_LAUNCHER=$WOS_CCACHE"
        )
    fi
}

wos_ccache_prefix() {
    if [ -n "${WOS_CCACHE:-}" ]; then
        printf '%s ' "$WOS_CCACHE"
    fi
}

wos_meson_compiler_prefix() {
    if [ -n "${WOS_CCACHE:-}" ]; then
        printf "'%s', " "$WOS_CCACHE"
    fi
}
