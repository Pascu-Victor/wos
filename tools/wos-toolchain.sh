#!/bin/bash
# Build the WOS target toolchain: sysroot, compiler-rt, mlibc, libc++, busybox,
# dropbear, GNU make, Bash, Ninja, CMake, Python, Meson, NASM, ncurses, nano,
# OpenSSL, curl, Git, and native WOS clang/lld.
# Requires host-toolchain.sh to have been run first.
#
# Layout:
#   toolchain/host/    - host LLVM binaries (clang, lld, llvm-ar, etc.)
#                       or WOS_HOST_TOOLCHAIN_ROOT when overridden
#   toolchain/sysroot/ - WOS target libraries and headers only
set -euo pipefail
set -E

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
wos_setup_ccache
wos_setup_ccache_cmake_args
WOS_BUILD_JOBS="$(wos_build_jobs)"
WOS_NINJA_JOBS="$(wos_ninja_jobs)"
WOS_CCACHE_PREFIX="$(wos_ccache_prefix)"
WOS_MESON_COMPILER_PREFIX="$(wos_meson_compiler_prefix)"

cd "$WORKSPACE_ROOT"

B="$WORKSPACE_ROOT/toolchain"
OLD_PATH=$PATH
TARGET_ARCH=x86_64-pc-wos
COMPILER_RT_ARCH=x86_64
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
SYSROOT=$B/sysroot
HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"
export WOS_HOST_TOOLCHAIN_ROOT="$HOST"
export NINJA_STATUS="[%f/%t %e] "

COMPILER_RT_CMAKE_SYSROOT_ARGS=("-DCMAKE_SYSROOT=$SYSROOT")
COMPILER_RT_NINJA_JOBS="$WOS_NINJA_JOBS"
if [ "$HOST_SYSTEM" = "WOS" ]; then
    COMPILER_RT_CMAKE_SYSROOT_ARGS=("-DCMAKE_SYSROOT_COMPILE=$SYSROOT")
fi
COMPILER_RT_CMAKE_CACHE_ARGS=(
    -DCOMPILER_RT_HAS_AARCH64_SME=False
    -DCOMPILER_RT_HAS_ARM_FP=False
    -DCOMPILER_RT_HAS_ARM_UNALIGNED=False
    -DCOMPILER_RT_HAS_ARRAY_BOUNDS_FLAG=1
    -DCOMPILER_RT_HAS_ARRAY_BOUNDS_POINTER_ARITHMETIC_FLAG=1
    -DCOMPILER_RT_HAS_ASM_LSE=False
    -DCOMPILER_RT_HAS_ATOMIC_KEYWORD=True
    -DCOMPILER_RT_HAS_BUILTIN_FORMAL_SECURITY_FLAG=1
    -DCOMPILER_RT_HAS_BUILTIN_MEMCPY_CHK_SIZE_FLAG=1
    -DCOMPILER_RT_HAS_CODE_OBJECT_VERSION_FLAG=True
    -DCOMPILER_RT_HAS_EMPTY_BODY_FLAG=1
    -DCOMPILER_RT_HAS_EXTERNAL_FLAG=
    -DCOMPILER_RT_HAS_FCF_PROTECTION_FLAG=1
    -DCOMPILER_RT_HAS_FCONVERGENT_FUNCTIONS_FLAG=True
    -DCOMPILER_RT_HAS_FFREESTANDING_FLAG=1
    -DCOMPILER_RT_HAS_FLTO_FLAG=True
    -DCOMPILER_RT_HAS_FNO_BUILTIN_FLAG=1
    -DCOMPILER_RT_HAS_FNO_COVERAGE_MAPPING_FLAG=1
    -DCOMPILER_RT_HAS_FNO_EXCEPTIONS_FLAG=1
    -DCOMPILER_RT_HAS_FNO_FUNCTION_SECTIONS_FLAG=1
    -DCOMPILER_RT_HAS_FNO_LTO_FLAG=1
    -DCOMPILER_RT_HAS_FNO_PARTIAL_INLINING_FLAG=
    -DCOMPILER_RT_HAS_FNO_PROFILE_GENERATE_FLAG=1
    -DCOMPILER_RT_HAS_FNO_PROFILE_INSTR_GENERATE_FLAG=1
    -DCOMPILER_RT_HAS_FNO_PROFILE_INSTR_USE_FLAG=1
    -DCOMPILER_RT_HAS_FNO_RTTI_FLAG=1
    -DCOMPILER_RT_HAS_FNO_SANITIZE_SAFE_STACK_FLAG=1
    -DCOMPILER_RT_HAS_FNO_STACK_PROTECTOR_FLAG=1
    -DCOMPILER_RT_HAS_FOMIT_FRAME_POINTER_FLAG=1
    -DCOMPILER_RT_HAS_FORMAT_INSUFFICIENT_ARGS_FLAG=1
    -DCOMPILER_RT_HAS_FPIC_FLAG=1
    -DCOMPILER_RT_HAS_FPIE_FLAG=1
    -DCOMPILER_RT_HAS_FRTTI_FLAG=1
    -DCOMPILER_RT_HAS_FUNC_SYMBOL=1
    -DCOMPILER_RT_HAS_FUNWIND_TABLES_FLAG=1
    -DCOMPILER_RT_HAS_FUSE_LD_LLD_FLAG=1
    -DCOMPILER_RT_HAS_FVISIBILITY_HIDDEN_FLAG=1
    -DCOMPILER_RT_HAS_FVISIBILITY_INLINES_HIDDEN_FLAG=1
    -DCOMPILER_RT_HAS_GLINE_TABLES_ONLY_FLAG=1
    -DCOMPILER_RT_HAS_GR_FLAG=
    -DCOMPILER_RT_HAS_GS_FLAG=
    -DCOMPILER_RT_HAS_G_FLAG=1
    -DCOMPILER_RT_HAS_MCRC32_FLAG=1
    -DCOMPILER_RT_HAS_MCRC_FLAG=
    -DCOMPILER_RT_HAS_MSSE4_2_FLAG=1
    -DCOMPILER_RT_HAS_MT_FLAG=
    -DCOMPILER_RT_HAS_NOGPULIB_FLAG=True
    -DCOMPILER_RT_HAS_NOSTDINCXX_FLAG=1
    -DCOMPILER_RT_HAS_NOSTDLIBXX_FLAG=1
    -DCOMPILER_RT_HAS_NO_DEFAULT_CONFIG_FLAG=1
    -DCOMPILER_RT_HAS_OMIT_FRAME_POINTER_FLAG=1
    -DCOMPILER_RT_HAS_Oy_FLAG=
    -DCOMPILER_RT_HAS_RETURN_STACK_ADDRESS_FLAG=1
    -DCOMPILER_RT_HAS_SHADOW_FLAG=1
    -DCOMPILER_RT_HAS_SIZEOF_ARRAY_ARGUMENT_FLAG=1
    -DCOMPILER_RT_HAS_SIZEOF_ARRAY_DECAY_FLAG=1
    -DCOMPILER_RT_HAS_SIZEOF_ARRAY_DIV_FLAG=1
    -DCOMPILER_RT_HAS_SIZEOF_POINTER_DIV_FLAG=1
    -DCOMPILER_RT_HAS_SIZEOF_POINTER_MEMACCESS_FLAG=1
    -DCOMPILER_RT_HAS_STD_C11_FLAG=1
    -DCOMPILER_RT_HAS_SUSPICIOUS_MEMACCESS_FLAG=1
    -DCOMPILER_RT_HAS_SYSROOT_FLAG=1
    -DCOMPILER_RT_HAS_UNINITIALIZED_FLAG=1
    -DCOMPILER_RT_HAS_VISIBILITY_HIDDEN_FLAG=True
    -DCOMPILER_RT_HAS_W4_FLAG=
    -DCOMPILER_RT_HAS_WALL_FLAG=1
    -DCOMPILER_RT_HAS_WBUILTIN_DECLARATION_MISMATCH_FLAG=False
    -DCOMPILER_RT_HAS_WC99_EXTENSIONS_FLAG=1
    -DCOMPILER_RT_HAS_WCOVERED_SWITCH_DEFAULT_FLAG=1
    -DCOMPILER_RT_HAS_WD4146_FLAG=
    -DCOMPILER_RT_HAS_WD4206_FLAG=
    -DCOMPILER_RT_HAS_WD4221_FLAG=
    -DCOMPILER_RT_HAS_WD4291_FLAG=
    -DCOMPILER_RT_HAS_WD4391_FLAG=
    -DCOMPILER_RT_HAS_WD4722_FLAG=
    -DCOMPILER_RT_HAS_WD4800_FLAG=
    -DCOMPILER_RT_HAS_WERROR_FLAG=1
    -DCOMPILER_RT_HAS_WFRAME_LARGER_THAN_FLAG=1
    -DCOMPILER_RT_HAS_WGLOBAL_CONSTRUCTORS_FLAG=1
    -DCOMPILER_RT_HAS_WGNU_ANONYMOUS_STRUCT_FLAG=1
    -DCOMPILER_RT_HAS_WGNU_FLAG=1
    -DCOMPILER_RT_HAS_WSUGGEST_OVERRIDE_FLAG=1
    -DCOMPILER_RT_HAS_WTHREAD_SAFETY_BETA_FLAG=1
    -DCOMPILER_RT_HAS_WTHREAD_SAFETY_FLAG=1
    -DCOMPILER_RT_HAS_WTHREAD_SAFETY_REFERENCE_FLAG=1
    -DCOMPILER_RT_HAS_WUNUSED_PARAMETER_FLAG=1
    -DCOMPILER_RT_HAS_WVARIADIC_MACROS_FLAG=1
    -DCOMPILER_RT_HAS_WX_FLAG=
    -DCOMPILER_RT_HAS_XRAY_COMPILER_FLAG=True
    -DCOMPILER_RT_HAS_ZL_FLAG=False
    -DCOMPILER_RT_HAS_Zi_FLAG=
    -DCOMPILER_RT_HAS_x86_64_BFLOAT16=1
    -DCOMPILER_RT_HAS_x86_64_FLOAT16=1
    -DCXX_SUPPORTS_UNWINDLIB_NONE_FLAG=1
    -DC_SUPPORTS_NODEFAULTLIBS_FLAG=1
)
LIBCXX_CMAKE_SUPPORT_CACHE_ARGS=(
    -DCXX_SUPPORTS_CTAD_MAYBE_UNSPPORTED_FLAG=1
    -DCXX_SUPPORTS_EHSC_FLAG=
    -DCXX_SUPPORTS_FALIGNED_ALLOCATION_FLAG=1
    -DCXX_SUPPORTS_FSIZED_DEALLOCATION_FLAG=1
    -DCXX_SUPPORTS_FSTRICT_ALIASING_FLAG=1
    -DCXX_SUPPORTS_FVISIBILITY_EQ_HIDDEN_FLAG=1
    -DCXX_SUPPORTS_FVISIBILITY_INLINES_HIDDEN_FLAG=1
    -DCXX_SUPPORTS_MISLEADING_INDENTATION_FLAG=1
    -DCXX_SUPPORTS_NOLIBC_FLAG=1
    -DCXX_SUPPORTS_NOSTDINCXX_FLAG=1
    -DCXX_SUPPORTS_NOSTDLIBINC_FLAG=1
    -DCXX_SUPPORTS_NOSTDLIBXX_FLAG=1
    -DCXX_SUPPORTS_SUGGEST_OVERRIDE_FLAG=1
    -DCXX_SUPPORTS_UNWINDLIB_EQ_NONE_FLAG=1
    -DCXX_SUPPORTS_WALL_FLAG=1
    -DCXX_SUPPORTS_WDEPRECATED_REDUNDANT_CONSTEXPR_STATIC_DEF_FLAG=1
    -DCXX_SUPPORTS_WERROR_EQ_RETURN_TYPE_FLAG=1
    -DCXX_SUPPORTS_WEXTRA_FLAG=1
    -DCXX_SUPPORTS_WEXTRA_SEMI_FLAG=1
    -DCXX_SUPPORTS_WFORMAT_NONLITERAL_FLAG=1
    -DCXX_SUPPORTS_WNEWLINE_EOF_FLAG=1
    -DCXX_SUPPORTS_WNO_COVERED_SWITCH_DEFAULT_FLAG=1
    -DCXX_SUPPORTS_WNO_ERROR_FLAG=1
    -DCXX_SUPPORTS_WNO_LONG_LONG_FLAG=1
    -DCXX_SUPPORTS_WNO_NULLABILITY_COMPLETENESS_FLAG=1
    -DCXX_SUPPORTS_WNO_SUGGEST_OVERRIDE_FLAG=1
    -DCXX_SUPPORTS_WNO_UNUSED_PARAMETER_FLAG=1
    -DCXX_SUPPORTS_WNO_USER_DEFINED_LITERALS_FLAG=1
    -DCXX_SUPPORTS_WSHADOW_FLAG=1
    -DCXX_SUPPORTS_WUNDEF_FLAG=1
    -DCXX_SUPPORTS_WUNUSED_TEMPLATE_FLAG=1
    -DCXX_SUPPORTS_WWRITE_STRINGS_FLAG=1
    -DCXX_SUPPORTS_WZERO_LENGTH_ARRAY_FLAG=1
    -DCXX_WSUGGEST_OVERRIDE_ALLOWS_ONLY_FINAL=1
    -DC_SUPPORTS_COMMENT_LIB_PRAGMA=1
    -DC_SUPPORTS_CTAD_MAYBE_UNSPPORTED_FLAG=1
    -DC_SUPPORTS_FUNWIND_TABLES_FLAG=1
    -DC_SUPPORTS_MISLEADING_INDENTATION_FLAG=1
    -DC_SUPPORTS_START_NO_UNUSED_ARGUMENTS=1
    -DLINKER_SUPPORTS_COLOR_DIAGNOSTICS=
    -DLLVM_DEFAULT_TO_GLIBCXX_USE_CXX11_ABI=
    -DLLVM_USES_LIBSTDCXX=
    -DSUPPORTS_FVISIBILITY_INLINES_HIDDEN_FLAG=1
)
LIBCXX_RUNTIME_CMAKE_SUPPORT_CACHE_ARGS=(
    "${LIBCXX_CMAKE_SUPPORT_CACHE_ARGS[@]}"
    -DCXX_SUPPORTS_FNO_EXCEPTIONS_FLAG=1
    -DCXX_SUPPORTS_FNO_RTTI_FLAG=1
    -DCXX_SUPPORTS_FUNWIND_TABLES_FLAG=1
    -DCXX_SUPPORTS_PEDANTIC_FLAG=1
)

BOOTSTRAP_DETAIL_TSV="${WOS_BOOTSTRAP_DETAIL_TSV:-}"
BOOTSTRAP_PHASE=""
BOOTSTRAP_PHASE_LABEL=""
BOOTSTRAP_PHASE_START_MS=""

bootstrap_now_ms() {
    local epoch="${EPOCHREALTIME:-}"

    if [ -n "$epoch" ]; then
        local seconds="${epoch%.*}"
        local fraction="${epoch#*.}000"

        printf '%s\n' "$((10#$seconds * 1000 + 10#${fraction:0:3}))"
        return 0
    fi

    python3 - <<'PY'
import time

print(time.monotonic_ns() // 1_000_000)
PY
}

bootstrap_timestamp_utc() {
    local epoch="${EPOCHREALTIME:-}"

    if [ -n "$epoch" ]; then
        local seconds="${epoch%.*}"
        local fraction="${epoch#*.}000"
        local prefix

        TZ=UTC printf -v prefix '%(%Y-%m-%dT%H:%M:%S)T' "$seconds"
        printf '%s.%sZ\n' "$prefix" "${fraction:0:3}"
        return 0
    fi

    python3 - <<'PY'
from datetime import datetime, timezone

print(datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"))
PY
}

bootstrap_detail_header() {
    [ -n "$BOOTSTRAP_DETAIL_TSV" ] || return 0

    case "$BOOTSTRAP_DETAIL_TSV" in
        */*) mkdir -p "${BOOTSTRAP_DETAIL_TSV%/*}" ;;
    esac
    if [ ! -s "$BOOTSTRAP_DETAIL_TSV" ]; then
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "timestamp_utc" "phase" "label" "elapsed_ms" "status" \
            "build_jobs" "ninja_jobs" "host_system" >> "$BOOTSTRAP_DETAIL_TSV"
    fi
}

bootstrap_record_detail() {
    local phase="$1"
    local label="$2"
    local elapsed_ms="$3"
    local status="$4"

    [ -n "$BOOTSTRAP_DETAIL_TSV" ] || return 0
    bootstrap_detail_header
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$(bootstrap_timestamp_utc)" "$phase" "$label" "$elapsed_ms" "$status" \
        "$WOS_BUILD_JOBS" "$WOS_NINJA_JOBS" "$HOST_SYSTEM" >> "$BOOTSTRAP_DETAIL_TSV"
}

bootstrap_phase_start() {
    BOOTSTRAP_PHASE="$1"
    BOOTSTRAP_PHASE_LABEL="$2"
    if [ -n "$BOOTSTRAP_DETAIL_TSV" ]; then
        BOOTSTRAP_PHASE_START_MS="$(bootstrap_now_ms)"
    else
        BOOTSTRAP_PHASE_START_MS=""
    fi
    echo "=== Phase $BOOTSTRAP_PHASE: $BOOTSTRAP_PHASE_LABEL ==="
}

bootstrap_phase_end() {
    local phase="$BOOTSTRAP_PHASE"
    local label="$BOOTSTRAP_PHASE_LABEL"
    local start_ms="$BOOTSTRAP_PHASE_START_MS"

    if [ -n "$start_ms" ]; then
        local end_ms
        end_ms="$(bootstrap_now_ms)"
        bootstrap_record_detail "$phase" "$label" "$((end_ms - start_ms))" "ok"
    fi

    echo "=== Phase $phase complete: $label ==="
    BOOTSTRAP_PHASE=""
    BOOTSTRAP_PHASE_LABEL=""
    BOOTSTRAP_PHASE_START_MS=""
}

bootstrap_phase_fail() {
    local status="$?"
    local phase="$BOOTSTRAP_PHASE"
    local label="$BOOTSTRAP_PHASE_LABEL"
    local start_ms="$BOOTSTRAP_PHASE_START_MS"

    if [ -n "$phase" ] && [ -n "$start_ms" ]; then
        local end_ms
        end_ms="$(bootstrap_now_ms)"
        bootstrap_record_detail "$phase" "$label" "$((end_ms - start_ms))" "fail:$status"
    fi
    exit "$status"
}

trap bootstrap_phase_fail ERR

meson_setup_rerunnable() {
    local build_dir="$1"
    shift

    if [ -d "$build_dir/meson-private" ] || [ -f "$build_dir/build.ninja" ]; then
        if meson setup --reconfigure "$build_dir" "$@"; then
            return 0
        fi

        echo "Meson reconfigure failed for $build_dir, retrying with --wipe..."
        meson setup --wipe "$build_dir" "$@"
        return 0
    fi

    meson setup "$build_dir" "$@"
}

install_compiler_rt_resource_dir() {
    local require_sanitizers="$1"
    local resource_dir="$HOST/lib/clang/22"
    local install_dir="$resource_dir/target"
    local runtime_dir="$resource_dir/lib/$TARGET_ARCH"

    if [ ! -d "$install_dir/lib" ]; then
        echo "ERROR: compiler-rt did not install runtime libraries under $install_dir/lib" >&2
        exit 1
    fi

    mkdir -p "$runtime_dir" "$resource_dir/include"
    cp -a "$install_dir/lib"/. "$runtime_dir"/
    if [ -d "$install_dir/include" ]; then
        cp -a "$install_dir/include"/. "$resource_dir/include"/
    fi
    rm -rf "$install_dir"

    ln -fs "$runtime_dir/libclang_rt.builtins-$COMPILER_RT_ARCH.a" "$resource_dir/lib/libclang_rt.builtins.a"
    ln -fs "$runtime_dir/libclang_rt.crtbegin-$COMPILER_RT_ARCH.a" "$resource_dir/lib/libclang_rt.crtbegin.a"
    ln -fs "$runtime_dir/libclang_rt.crtend-$COMPILER_RT_ARCH.a" "$resource_dir/lib/libclang_rt.crtend.a"

    ln -fs "$runtime_dir/libclang_rt.builtins-$COMPILER_RT_ARCH.a" "$runtime_dir/libclang_rt.builtins.a"
    ln -fs "$runtime_dir/libclang_rt.crtbegin-$COMPILER_RT_ARCH.a" "$runtime_dir/libclang_rt.crtbegin.a"
    ln -fs "$runtime_dir/libclang_rt.crtend-$COMPILER_RT_ARCH.a" "$runtime_dir/libclang_rt.crtend.a"

    if [ ! -f "$runtime_dir/libclang_rt.profile-$COMPILER_RT_ARCH.a" ]; then
        echo "ERROR: compiler-rt did not install libclang_rt.profile-$COMPILER_RT_ARCH.a" >&2
        exit 1
    fi
    ln -fs "$runtime_dir/libclang_rt.profile-$COMPILER_RT_ARCH.a" "$runtime_dir/libclang_rt.profile.a"

    if [ "$require_sanitizers" = "ON" ]; then
        local runtime_lib
        for runtime_lib in asan_static asan asan_cxx; do
            if [ ! -f "$runtime_dir/libclang_rt.$runtime_lib-$COMPILER_RT_ARCH.a" ]; then
                echo "ERROR: compiler-rt did not install libclang_rt.$runtime_lib-$COMPILER_RT_ARCH.a" >&2
                exit 1
            fi
            ln -fs "$runtime_dir/libclang_rt.$runtime_lib-$COMPILER_RT_ARCH.a" "$runtime_dir/libclang_rt.$runtime_lib.a"
        done
    fi
}

build_compiler_rt() {
    local build_sanitizers="$1"
    local detail_label="compiler_rt_builtins"

    if [ "$build_sanitizers" = "ON" ]; then
        detail_label="compiler_rt_sanitizers"
    fi

    mkdir -p "$B/compiler-rt-build"
    wos_timed_step "configure" "$detail_label" \
        wos_run_env_in_dir "$B/compiler-rt-build" \
        -u LDFLAGS \
        cmake -G Ninja \
     "${WOS_CCACHE_CMAKE_ARGS[@]}" \
     -DCMAKE_BUILD_TYPE=Release \
     -DCMAKE_INSTALL_PREFIX=$HOST/lib/clang/22/target \
     -DCMAKE_C_COMPILER=$CC \
     -DCMAKE_CXX_COMPILER=$CXX \
     "${COMPILER_RT_CMAKE_SYSROOT_ARGS[@]}" \
     -DCMAKE_SYSTEM_NAME=WOS \
     -DCMAKE_C_FLAGS="-fno-sanitize=safe-stack -fdiagnostics-color=always" \
     -DCMAKE_CXX_FLAGS="-fno-sanitize=safe-stack -fdiagnostics-color=always" \
     -DCMAKE_ASM_FLAGS="-fno-sanitize=safe-stack -fdiagnostics-color=always" \
     -DCMAKE_C_COMPILER_TARGET=$TARGET_ARCH \
     -DCMAKE_CXX_COMPILER_TARGET=$TARGET_ARCH \
     -DCMAKE_ASM_COMPILER_TARGET=$TARGET_ARCH \
     -DCMAKE_C_COMPILER_WORKS=ON \
     -DCMAKE_CXX_COMPILER_WORKS=ON \
     -DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY \
     -DCOMPILER_RT_BUILD_BUILTINS=ON \
     -DCOMPILER_RT_BUILD_MEMPROF=OFF \
     -DCOMPILER_RT_BUILD_ORC=OFF \
     -DCOMPILER_RT_BUILD_PROFILE=ON \
     -DCOMPILER_RT_BUILD_LIBFUZZER=OFF \
     -DCOMPILER_RT_HAS_SAFESTACK=OFF \
     -DCOMPILER_RT_OS_DIR="" \
     -DCOMPILER_RT_LIBCXXABI_ENABLE_LOCALIZATION=OFF \
     -DCOMPILER_RT_HAS_SCUDO_STANDALONE=OFF \
     -DCOMPILER_RT_BUILD_XRAY=OFF \
     -DCOMPILER_RT_BUILD_SANITIZERS=$build_sanitizers \
     -DCOMPILER_RT_SANITIZERS_TO_BUILD=asan \
     -DCOMPILER_RT_HAS_GCC_S_LIB=OFF \
     -DSANITIZER_CXX_ABI=none \
     -DSANITIZER_TEST_CXX=none \
     -DCOMPILER_RT_STANDALONE_BUILD=ON \
     -DCOMPILER_RT_INTERCEPT_LIBDISPATCH=OFF \
     -DCOMPILER_RT_HAS_PTHREAD_LIB=OFF \
     -DCAN_TARGET_AMD64=ON \
     -DWOS=ON \
     "${COMPILER_RT_CMAKE_CACHE_ARGS[@]}" \
     $B/src/llvm-project/compiler-rt

    wos_timed_step "build" "$detail_label" \
        wos_run_in_dir "$B/compiler-rt-build" \
        ninja -j"$COMPILER_RT_NINJA_JOBS" install
    install_compiler_rt_resource_dir "$build_sanitizers"
}

host_toolchain_clang_usable() {
    if [ "$HOST_SYSTEM" = "WOS" ]; then
        [ -f "$HOST/bin/clang" ] && "$HOST/bin/clang" --version >/dev/null 2>&1
        return $?
    fi

    [ -x "$HOST/bin/clang" ]
}

if ! host_toolchain_clang_usable; then
    echo "ERROR: Host toolchain not found at $HOST/bin/clang"
    echo "Run tools/host-toolchain.sh first, or run tools/bootstrap.sh on WOS to create a system-toolchain shim."
    exit 1
fi

# Set up environment - host tools on PATH, sysroot for target libs
export CC="$HOST/bin/clang"
export CXX="$HOST/bin/clang++"
export LD="$HOST/bin/ld.lld"
export PATH=$HOST/bin:$OLD_PATH
export LD_LIBRARY_PATH="$HOST/lib"

HOST_NINJA="$(command -v ninja)"
HOST_PYTHON="$(command -v python3)"
LIBCXX_CMAKE_HOST_TOOL_ARGS=(
    -DCMAKE_MAKE_PROGRAM="$HOST_NINJA"
    -DPython3_EXECUTABLE="$HOST_PYTHON"
    -DCMAKE_FIND_ROOT_PATH="$SYSROOT"
    -DCMAKE_FIND_ROOT_PATH_MODE_PROGRAM=NEVER
    -DCMAKE_FIND_ROOT_PATH_MODE_LIBRARY=ONLY
    -DCMAKE_FIND_ROOT_PATH_MODE_INCLUDE=ONLY
)

if [ "${WOS_BUILD_CMAKE_FOR_HOST:-1}" != "0" ]; then
    "$WORKSPACE_ROOT/scripts/build/build_cmake_for_host.sh"
fi

bootstrap_phase_start 1 "target sysroot skeleton and mlibc headers"
# 1. Create target directories and empty CRT files
mkdir -p $SYSROOT/bin $SYSROOT/lib $SYSROOT/include/abi-bits
[ ! -e $SYSROOT/usr ] && ln -sf . $SYSROOT/usr
[ ! -e $SYSROOT/lib64 ] && ln -sf lib $SYSROOT/lib64

# headers-only mlibc -- compiler-rt needs these
# Create a basic cross-file for headers-only build
mkdir -p $B/../tools
cat > $B/../tools/x86_64-pc-wos-mlibc.txt << EOF
[binaries]
c = [$WOS_MESON_COMPILER_PREFIX'clang', '--target=x86_64-pc-wos', '-mcmodel=small']
cpp = [$WOS_MESON_COMPILER_PREFIX'clang++', '--target=x86_64-pc-wos', '-mcmodel=small']
ar = 'llvm-ar'
strip = 'llvm-strip'
[host_machine]
system = 'wos'
cpu_family = 'x86_64'
cpu = 'x86_64'
endian = 'little'
EOF

# Build mlibc headers
mkdir -p $B/mlibc-headers
wos_timed_step "configure" "mlibc_headers" \
    meson_setup_rerunnable "$B/mlibc-headers" --prefix=$SYSROOT \
    --libdir=lib \
    --includedir=include \
    -Dheaders_only=true \
    -Ddefault_library=static \
    --cross-file=$B/../tools/x86_64-pc-wos-mlibc.txt \
    -Dbindir=bin \
    -Dwos_option=enabled \
    -Dglibc_option=enabled \
    -Db_staticpic=disabled \
    $B/src/mlibc
wos_timed_step "install" "mlibc_headers" \
    wos_run_in_dir "$B/mlibc-headers" \
    ninja -j"$WOS_NINJA_JOBS" install
cd $B/mlibc-headers

# Create minimal CRT files for compiler-rt build (these will be replaced by mlibc later)
touch empty.c
$CC -O3 -c empty.c       -o $SYSROOT/lib/crtbegin.o
$CC -O3 -c empty.c -fPIC -o $SYSROOT/lib/crtbeginS.o
$CC -O3 -c empty.c       -o $SYSROOT/lib/crtend.o
$CC -O3 -c empty.c -fPIC -o $SYSROOT/lib/crtendS.o

# Create temporary CRT startup files for compiler-rt (will be replaced by mlibc)
$CC -O3 -c empty.c       -o $SYSROOT/lib/Scrt1.o
$CC -O3 -c empty.c       -o $SYSROOT/lib/crt1.o
$CC -O3 -c empty.c       -o $SYSROOT/lib/crti.o
$CC -O3 -c empty.c       -o $SYSROOT/lib/crtn.o
bootstrap_phase_end

# 2. Build the compiler-rt pieces needed to finish the libc bootstrap.
# Sanitizers are built after mlibc installs real libc/libpthread/libm/etc.
bootstrap_phase_start 2 "compiler-rt builtins and profile bootstrap"
export CFLAGS="--sysroot=$SYSROOT -std=c23 -fno-sanitize=safe-stack "
export CXXFLAGS="--sysroot=$SYSROOT -std=c++23 -fno-sanitize=safe-stack "
unset LDFLAGS
build_compiler_rt OFF
bootstrap_phase_end

# 3. Bootstrap libcxx (headers only, needed by mlibc)

bootstrap_phase_start 3 "libcxx and libcxxabi headers"
mkdir -p $B/libcxx-bootstrap
wos_timed_step "configure" "libcxx_headers" \
    wos_run_in_dir "$B/libcxx-bootstrap" \
    cmake -G Ninja \
 "${WOS_CCACHE_CMAKE_ARGS[@]}" \
 "${LIBCXX_CMAKE_HOST_TOOL_ARGS[@]}" \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_INSTALL_PREFIX=$SYSROOT \
 -DCMAKE_C_COMPILER=$CC \
 -DCMAKE_CXX_COMPILER=$CXX \
 -DCMAKE_C_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_CXX_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_ASM_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_C_COMPILER_WORKS=ON \
 -DCMAKE_CXX_COMPILER_WORKS=ON \
 -DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY \
 -DCMAKE_SYSROOT=$SYSROOT \
 -DCMAKE_C_FLAGS="--sysroot=$SYSROOT" \
 -DCMAKE_CXX_FLAGS="--sysroot=$SYSROOT" \
 -DCMAKE_CROSSCOMPILING=True \
 -DLLVM_ENABLE_RUNTIMES='libcxx;libcxxabi' \
 -DLIBCXX_CXX_ABI=libcxxabi \
 -DLIBCXX_ENABLE_SHARED=OFF \
 -DLIBCXX_ENABLE_STATIC=OFF \
 -DLIBCXX_ENABLE_ABI_LINKER_SCRIPT=OFF \
 -DLIBCXX_INSTALL_LIBRARY=OFF \
 -DLIBCXX_USE_COMPILER_RT=On \
 -DLIBCXX_HAS_PTHREAD_API=On \
 -DLIBCXX_HAS_PTHREAD_LIB=On \
 -DLIBCXX_INCLUDE_BENCHMARKS=OFF \
 -DLIBCXX_INCLUDE_TESTS=OFF \
 -DLIBCXX_INSTALL_MODULES=OFF \
 -DLIBCXXABI_ENABLE_STATIC=OFF \
 -DLIBCXXABI_ENABLE_SHARED=ON \
 -DLIBCXXABI_INSTALL_LIBRARY=OFF \
 -DLIBCXX_INSTALL_HEADERS=ON \
 -DLIBCXXABI_INCLUDE_TESTS=OFF \
 -DLIBCXXABI_USE_LLVM_UNWINDER=OFF \
 -DLIBCXXABI_HAS_PTHREAD_API=ON \
 -DLIBCXXABI_HAS_PTHREAD_LIB=ON \
 -DLIBCXXABI_USE_COMPILER_RT=ON \
 -DHAVE_LIBPTHREAD=OFF \
 -DLIBCXX_ENABLE_LOCALIZATION=OFF \
 -DLIBCXX_ENABLE_FILESYSTEM=OFF \
 -DLIBCXX_ENABLE_LOCALIZATION=OFF \
 "${LIBCXX_CMAKE_SUPPORT_CACHE_ARGS[@]}" \
 $B/src/llvm-project/runtimes

wos_timed_step "install" "libcxx_headers" \
    wos_run_in_dir "$B/libcxx-bootstrap" \
    ninja -j"$WOS_NINJA_JOBS" install-cxx-headers install-cxxabi-headers
bootstrap_phase_end

# 4. Build mlibc

bootstrap_phase_start 4 "mlibc"
# Prepare cross-file (always regenerate to ensure correct paths)
mkdir -p $B/../tools
cat > $B/../tools/x86_64-pc-wos-mlibc.txt << EOF
[binaries]
c = [$WOS_MESON_COMPILER_PREFIX'clang', '--target=x86_64-pc-wos', '--sysroot=$SYSROOT', '-isystem', '$HOST/lib/clang/22/include', '-isystem', '$SYSROOT/include', '-mcmodel=small']
cpp = [$WOS_MESON_COMPILER_PREFIX'clang++', '--target=x86_64-pc-wos', '--sysroot=$SYSROOT', '-isystem', '$SYSROOT/include/c++/v1', '-isystem', '$HOST/lib/clang/22/include', '-isystem', '$SYSROOT/include', '-mcmodel=small']
ar = 'llvm-ar'
strip = 'llvm-strip'

[host_machine]
system = 'wos'
cpu_family = 'x86_64'
cpu = 'x86_64'
endian = 'little'
EOF

# Reset flags because the compiler gods want to i guess :D
unset CFLAGS CXXFLAGS LDFLAGS
export CFLAGS="--sysroot=$SYSROOT -std=c23 -fno-sanitize=safe-stack "
export CXXFLAGS="--sysroot=$SYSROOT -std=c++23 -fno-sanitize=safe-stack "
export LDFLAGS="--sysroot=$SYSROOT"

wos_prefetch_meson_subprojects "$B/src/mlibc" freestnd-c-hdrs freestnd-cxx-hdrs frigg

mkdir -p $B/mlibc-build
wos_timed_step "configure" "mlibc" \
    meson_setup_rerunnable "$B/mlibc-build" --prefix=$SYSROOT \
  --sysconfdir=etc \
  --buildtype=release \
  --cross-file=$B/../tools/x86_64-pc-wos-mlibc.txt \
  -Dheaders_only=false \
  -Dwos_option=enabled \
  -Dlinux_option=disabled \
  -Dglibc_option=enabled \
  -Ddefault_library=both \
  -Duse_freestnd_hdrs=enabled \
  -Dposix_option=enabled \
  -Dbsd_option=enabled \
  -Db_sanitize=none \
  $B/src/mlibc
wos_timed_step "build" "mlibc" \
    wos_run_in_dir "$B/mlibc-build" \
    ninja -j"$WOS_NINJA_JOBS"
wos_timed_step "install" "mlibc" \
    wos_run_in_dir "$B/mlibc-build" \
    ninja -j"$WOS_NINJA_JOBS" install
bootstrap_phase_end

# 5. Finish compiler-rt now that mlibc installed the libraries ASAN links to.
bootstrap_phase_start 5 "compiler-rt sanitizers"
unset LDFLAGS
build_compiler_rt ON
bootstrap_phase_end

# 6. Build libcxx, libcxxabi, and libunwind (now that mlibc is available)

bootstrap_phase_start 6 "libcxx libcxxabi and libunwind"
mkdir -p $B/libcxx-build
wos_timed_step "configure" "libcxx_runtime" \
    wos_run_in_dir "$B/libcxx-build" \
    cmake -G Ninja \
 "${WOS_CCACHE_CMAKE_ARGS[@]}" \
 "${LIBCXX_CMAKE_HOST_TOOL_ARGS[@]}" \
 -ULIBCXXABI_HAS_CXA_THREAD_ATEXIT_IMPL \
 -DLLVM_ENABLE_RUNTIMES='libcxx;libcxxabi;libunwind' \
 -DLIBCXXABI_USE_LLVM_UNWINDER=ON \
 -DLIBCXXABI_USE_COMPILER_RT=On \
 -DLIBCXXABI_INCLUDE_TESTS=OFF \
 -DLIBCXXABI_HAS_PTHREAD_LIB=ON \
 -DLIBCXXABI_HAS_PTHREAD_API=ON \
 -DLIBCXXABI_ENABLE_STATIC=ON \
 -DLIBCXXABI_ENABLE_SHARED=ON \
 -DLIBUNWIND_ENABLE_SHARED=ON \
 -DLIBUNWIND_ENABLE_STATIC=ON \
 -DLIBCXX_USE_COMPILER_RT=On \
 -DLIBCXX_INSTALL_HEADERS=ON \
 -DLIBCXX_INCLUDE_TESTS=OFF \
 -DLIBCXX_INCLUDE_BENCHMARKS=OFF \
 -DLIBCXX_HAS_PTHREAD_LIB=On \
 -DLIBCXX_HAS_PTHREAD_API=On \
 -DLIBCXX_ENABLE_LOCALIZATION=ON \
 -DLIBCXX_ENABLE_FILESYSTEM=ON \
 -DLIBCXX_ENABLE_WIDE_CHARACTERS=ON \
 -DLIBCXX_ENABLE_EXPERIMENTAL=ON \
 -DLIBCXX_ENABLE_UNICODE=ON \
 -DLIBCXX_ENABLE_RTTI=ON \
 -DLIBCXX_ENABLE_EXCEPTIONS=ON \
 -DLIBCXX_ENABLE_SHARED_CXX_ABI=ON \
 -DLIBCXX_CXX_ABI=libcxxabi \
 -DLIBCXX_HAS_ATOMIC_LIB=OFF \
 -DLIBCXXABI_HAS_ATOMIC_LIB=OFF \
 -DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY \
 -DCMAKE_SYSTEM_NAME=WOS \
 -DCMAKE_SYSROOT=$SYSROOT \
 -DCMAKE_INSTALL_PREFIX=$SYSROOT \
 -DCMAKE_CXX_FLAGS="-I$SYSROOT/include --sysroot=$SYSROOT -fno-sanitize=safe-stack -fdiagnostics-color=always" \
 -DCMAKE_CXX_COMPILER=$CXX \
 -DCMAKE_CXX_COMPILER_WORKS=ON \
 -DCMAKE_CXX_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_CROSSCOMPILING=True \
 -DCMAKE_C_FLAGS="-I$SYSROOT/include --sysroot=$SYSROOT -fno-sanitize=safe-stack -fdiagnostics-color=always" \
 -DCMAKE_C_COMPILER=$CC \
 -DCMAKE_C_COMPILER_WORKS=ON \
 -DCMAKE_C_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_ASM_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_SHARED_LINKER_FLAGS="-L$HOST/lib/clang/22/lib/$TARGET_ARCH" \
 -DCMAKE_EXE_LINKER_FLAGS="-L$HOST/lib/clang/22/lib/$TARGET_ARCH" \
 -DWOS=ON \
 "${LIBCXX_RUNTIME_CMAKE_SUPPORT_CACHE_ARGS[@]}" \
 $B/src/llvm-project/runtimes

WOS_LIBCXX_BUILD_DIR="$B/libcxx-build" "$B/../scripts/build/build_libcxx_for_wos.sh"

# Generate Clang config file for WOS target triple
# Must be after all library builds (mlibc, libc++) but before userspace binaries
cat > $HOST/bin/x86_64-pc-wos.cfg << 'CFGEOF'
-fPIE
-pie
-Wl,--dynamic-linker=/lib/ld.so
CFGEOF
bootstrap_phase_end

# 7. Build busybox for WOS userspace
bootstrap_phase_start 7 "BusyBox for WOS userspace"
cd $B/src
[ ! -d busybox ] && git clone --depth=1 --branch=wos-support https://github.com/Pascu-Victor/busybox.git

WOS_HOST_TOOLCHAIN_ROOT="$HOST" \
    WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_BUSYBOX_BUILD_DIR="$B/busybox-build" \
    WOS_BUSYBOX_INSTALL_DIR="$B/busybox-install" \
    "$B/../scripts/build/build_busybox.sh"
bootstrap_phase_end

# 8. Build Dropbear SSH for WOS userspace
bootstrap_phase_start 8 "Dropbear SSH for WOS userspace"
cd $B/src
[ ! -d dropbear ] && git clone --depth=1 --branch=wos-support https://github.com/Pascu-Victor/dropbear.git

WOS_HOST_TOOLCHAIN_ROOT="$HOST" \
    WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_DROPBEAR_BUILD_DIR="$B/dropbear-build" \
    "$B/../scripts/build/build_dropbear.sh"
bootstrap_phase_end

# 9. Build GNU make for WOS userspace
bootstrap_phase_start 9 "GNU make for WOS userspace"
WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_MAKE_BUILD_DIR="$B/make-build" \
    "$B/../scripts/build/build_make.sh"
bootstrap_phase_end

# 10. Build Bash for WOS userspace
bootstrap_phase_start 10 "Bash for WOS userspace"
WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_BASH_SOURCE_DIR="$B/src/bash" \
    WOS_BASH_BUILD_DIR="$B/bash-build" \
    "$B/../scripts/build/build_bash_for_wos.sh"
bootstrap_phase_end

# 11. Build Ninja for WOS userspace
bootstrap_phase_start 11 "Ninja for WOS userspace"
cd "$B/src"
if [ ! -f ninja/CMakeLists.txt ]; then
    if [ -d "$WORKSPACE_ROOT/.git" ]; then
        git -C "$WORKSPACE_ROOT" submodule update --init --depth=1 toolchain/src/ninja || true
    fi
fi
if [ ! -f ninja/CMakeLists.txt ]; then
    git clone --depth=1 https://github.com/Pascu-Victor/ninja.git ninja
fi

WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_NINJA_BUILD_DIR="$B/ninja-build" \
    "$B/../scripts/build/build_ninja_for_wos.sh"
bootstrap_phase_end

# 12. Build CMake for WOS userspace
bootstrap_phase_start 12 "CMake for WOS userspace"
cd "$B/src"
if [ ! -f cmake/CMakeLists.txt ]; then
    if [ -d "$WORKSPACE_ROOT/.git" ]; then
        git -C "$WORKSPACE_ROOT" submodule update --init toolchain/src/cmake || true
    fi
fi
if [ ! -f cmake/CMakeLists.txt ]; then
    git clone --branch=wos-support https://github.com/Pascu-Victor/CMake.git cmake
fi

WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_CMAKE_FOR_WOS_BUILD_DIR="$B/cmake-wos-build" \
    "$B/../scripts/build/build_cmake_for_wos.sh"
bootstrap_phase_end

# 13. Build NASM for WOS userspace
bootstrap_phase_start 13 "NASM for WOS userspace"
cd "$B/src"
NASM_GIT_BRANCH="${WOS_NASM_GIT_BRANCH:-wos-support}"
if [ ! -f nasm/configure.ac ]; then
    if [ -d "$WORKSPACE_ROOT/.git" ]; then
        git -C "$WORKSPACE_ROOT" submodule update --init --depth=1 toolchain/src/nasm || true
    fi
fi
if [ ! -f nasm/configure.ac ]; then
    git clone --depth=1 --branch "$NASM_GIT_BRANCH" https://github.com/Pascu-Victor/nasm.git nasm
fi

WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_NASM_SOURCE_DIR="$B/src/nasm" \
    WOS_NASM_BUILD_DIR="$B/nasm-build" \
    "$B/../scripts/build/build_nasm_for_wos.sh"
bootstrap_phase_end

# 14. Build ncurses and nano for WOS userspace
bootstrap_phase_start 14 "ncurses and nano for WOS userspace"
WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_NCURSES_SOURCE_DIR="$B/src/ncurses" \
    WOS_NCURSES_BUILD_DIR="$B/ncurses-build" \
    "$B/../scripts/build/build_ncurses_for_wos.sh"

WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_NANO_SOURCE_DIR="$B/src/nano" \
    WOS_NANO_BUILD_DIR="$B/nano-build" \
    "$B/../scripts/build/build_nano_for_wos.sh"
bootstrap_phase_end

# 15. Build zlib, LibreSSL, and curl before CPython so target feature probes
# cannot fall back to the self-host system's headers or libraries.
bootstrap_phase_start 15 "zlib LibreSSL and curl for WOS userspace"
WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_ZLIB_SOURCE_DIR="$B/src/zlib" \
    WOS_ZLIB_BUILD_DIR="$B/zlib-build" \
    "$B/../scripts/build/build_zlib_for_wos.sh"

WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_OPENSSL_SOURCE_DIR="$B/src/openssl" \
    WOS_OPENSSL_BUILD_DIR="$B/openssl-build" \
    "$B/../scripts/build/build_openssl_for_wos.sh"

WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_CURL_SOURCE_DIR="$B/src/curl" \
    WOS_CURL_BUILD_DIR="$B/curl-build" \
    "$B/../scripts/build/build_curl_for_wos.sh"
bootstrap_phase_end

# 16. Build CPython for WOS userspace
bootstrap_phase_start 16 "CPython for WOS userspace"
cd "$B/src"
PYTHON_GIT_BRANCH="${WOS_PYTHON_GIT_BRANCH:-wos-support}"
if [ ! -f python/configure ]; then
    if [ -d "$WORKSPACE_ROOT/.git" ]; then
        git -C "$WORKSPACE_ROOT" submodule update --init --depth=1 toolchain/src/python || true
    fi
fi
if [ ! -f python/configure ]; then
    git clone --depth=1 --branch "$PYTHON_GIT_BRANCH" https://github.com/Pascu-Victor/cpython.git python
fi

WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_PYTHON_SOURCE_DIR="$B/src/python" \
    WOS_PYTHON_BUILD_DIR="$B/python-build" \
    "$B/../scripts/build/build_python_for_wos.sh"
bootstrap_phase_end

# 17. Stage Meson for WOS userspace
bootstrap_phase_start 17 "Meson for WOS userspace"
WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_MESON_SOURCE_DIR="$B/src/meson" \
    WOS_MESON_BUILD_DIR="$B/meson-build" \
    "$B/../scripts/build/build_meson_for_wos.sh"
bootstrap_phase_end

# 18. Build Git for WOS userspace
bootstrap_phase_start 18 "Git for WOS userspace"
cd "$B/src"
if [ ! -f git/Makefile ]; then
    if [ -d "$WORKSPACE_ROOT/.git" ]; then
        git -C "$WORKSPACE_ROOT" submodule update --init --depth=1 toolchain/src/git || true
    fi
fi
if [ ! -f git/Makefile ]; then
    git clone --depth=1 --branch=wos-support https://github.com/Pascu-Victor/git.git git
fi

WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_GIT_SOURCE_DIR="$B/src/git" \
    WOS_GIT_BUILD_DIR="$B/git-build" \
    "$B/../scripts/build/build_git_for_wos.sh"
bootstrap_phase_end

# 19. Build clang/lld for WOS userspace
bootstrap_phase_start 19 "clang/lld for WOS userspace"
WOS_HOST_TOOLCHAIN_ROOT="$HOST" \
    WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_CLANG_FOR_WOS_BUILD_DIR="$B/clang-wos-build" \
    "$B/../scripts/build/build_clang_for_wos.sh"
bootstrap_phase_end
