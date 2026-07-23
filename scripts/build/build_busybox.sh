#!/bin/bash
# Incrementally rebuild busybox for WOS and install into the sysroot.
# Expects the toolchain to already be bootstrapped (tools/bootstrap.sh).
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-busybox-ccache"
    mkdir -p "$CCACHE_DIR"
fi
wos_setup_ccache
WOS_CCACHE_PREFIX="$(wos_ccache_prefix)"
WOS_BUILD_JOBS="$(wos_build_jobs)"
WOS_MAKE_JOBS="$(wos_make_jobs)"
HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
BB_BUILD="${WOS_BUSYBOX_BUILD_DIR:-$B/busybox-build}"
BB_INSTALL="${WOS_BUSYBOX_INSTALL_DIR:-$B/busybox-install}"
BB_CONFIG="${WOS_BUSYBOX_CONFIG:-$WORKSPACE_ROOT/configs/busybox/wos_full.config}"
BB_SHARED_DIR="$BB_BUILD/0_lib"
BB_KBUILD_TOOLS="$BB_BUILD/wos-kbuild-tools"

BB_SRC="$B/src/busybox"
if [ ! -d "$BB_SRC" ]; then
    echo "ERROR: busybox source directory not found at $BB_SRC"
    echo "Run tools/bootstrap.sh first to bootstrap sources."
    exit 1
fi
mkdir -p "$BB_BUILD"

# Cross-compilation variables - host tools, target sysroot
BB_CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=x86_64-pc-wos --sysroot=$TARGET_SYSROOT"
if [ "$HOST_SYSTEM" = "WOS" ] && [ -x /usr/bin/clang ]; then
    BB_CLANG_RESOURCE_DIR="$(/usr/bin/clang --target=x86_64-pc-wos -print-resource-dir 2>/dev/null || true)"
    if [ -z "$BB_CLANG_RESOURCE_DIR" ]; then
        BB_CLANG_RESOURCE_DIR="$(/usr/bin/clang -print-resource-dir)"
    fi
    BB_CC="${WOS_CCACHE_PREFIX}/usr/bin/clang --target=x86_64-pc-wos --sysroot=$TARGET_SYSROOT -resource-dir $BB_CLANG_RESOURCE_DIR"
    if [ -f "$HOST/bin/x86_64-pc-wos.cfg" ]; then
        BB_CC="$BB_CC --config=$HOST/bin/x86_64-pc-wos.cfg"
    fi
fi
BB_AR="$HOST/bin/llvm-ar"
BB_STRIP="$HOST/bin/llvm-strip"
BB_RANLIB="$HOST/bin/llvm-ranlib"
BB_OBJCOPY="$HOST/bin/llvm-objcopy"
BB_NM="$HOST/bin/llvm-nm"
strip_path_entry() {
    local input="$1"
    local remove="$2"
    local out=""
    local part
    local -a parts

    IFS=':' read -r -a parts <<< "$input"
    for part in "${parts[@]}"; do
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

native_host_path() {
    local search_path="${WOS_ORIGINAL_PATH:-$PATH}"

    search_path="$(strip_path_entry "$search_path" "$WORKSPACE_ROOT/bin")"
    search_path="$(strip_path_entry "$search_path" "$WORKSPACE_ROOT/toolchain/host/bin")"
    search_path="$(strip_path_entry "$search_path" "$WORKSPACE_ROOT/tools/build/bin")"
    printf '%s' "$search_path"
}

find_native_host_tool() {
    local tool="$1"
    local search_path

    search_path="$(native_host_path)"
    PATH="$search_path" command -v "$tool" 2>/dev/null
}

if [ -n "${HOSTCC:-}" ]; then
    case "$HOSTCC" in
        */*|*" "*)
            BB_NATIVE_HOSTCC="$HOSTCC"
            ;;
        *)
            BB_NATIVE_HOSTCC="$(find_native_host_tool "$HOSTCC" || true)"
            if [ -z "$BB_NATIVE_HOSTCC" ]; then
                BB_NATIVE_HOSTCC="$HOSTCC"
            fi
            ;;
    esac
else
    BB_NATIVE_HOSTCC="$(find_native_host_tool cc || true)"
    if [ -z "$BB_NATIVE_HOSTCC" ]; then
        BB_NATIVE_HOSTCC="$(find_native_host_tool clang || true)"
    fi
    if [ -z "$BB_NATIVE_HOSTCC" ]; then
        BB_NATIVE_HOSTCC="$(find_native_host_tool gcc || true)"
    fi
fi
if [ -z "$BB_NATIVE_HOSTCC" ]; then
    echo "ERROR: no native host C compiler found for BusyBox host tools" >&2
    exit 1
fi
BB_HOSTCC="${WOS_CCACHE_PREFIX}$BB_NATIVE_HOSTCC"
BB_CFLAGS="--sysroot=$TARGET_SYSROOT -fPIC -fno-sanitize=safe-stack -fno-stack-protector -Wno-string-plus-int"
BB_LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld"
BB_MAKE_SHELL_ARGS=()
if [ "$HOST_SYSTEM" = "WOS" ] && [ -x /bin/bash ]; then
    BB_MAKE_SHELL_ARGS=(SHELL=/bin/bash CONFIG_SHELL=/bin/bash)
fi

busybox_config_enabled() {
    local sym="$1"
    grep -q "^${sym}=y$" "$BB_BUILD/.config"
}

setup_busybox_kbuild_tools() {
    if [ "$HOST_SYSTEM" != "WOS" ]; then
        return 0
    fi
    if ! command -v python3 >/dev/null 2>&1; then
        return 0
    fi

    mkdir -p "$BB_KBUILD_TOOLS"
    cat > "$BB_KBUILD_TOOLS/cmp" <<'PY'
#!/usr/bin/python3
import os
import sys


def usage() -> int:
    print("cmp: supported usage: cmp [-s|--silent|--quiet] FILE1 FILE2", file=sys.stderr)
    return 2


def open_file(path: str):
    if path == "-":
        return sys.stdin.buffer
    return open(path, "rb")


args = []
silent = False
it = iter(sys.argv[1:])
for arg in it:
    if arg == "--":
        args.extend(it)
        break
    if arg in ("-s", "--silent", "--quiet"):
        silent = True
        continue
    if arg.startswith("-") and arg != "-":
        sys.exit(usage())
    args.append(arg)

if len(args) != 2:
    sys.exit(usage())

left_name, right_name = args
if left_name == right_name and left_name != "-":
    sys.exit(0)

try:
    left = open_file(left_name)
    right = open_file(right_name)
except OSError as err:
    if not silent:
        print(f"cmp: {err.filename}: {os.strerror(err.errno)}", file=sys.stderr)
    sys.exit(2)

offset = 0
line = 1
block_size = 1024 * 1024
try:
    while True:
        left_block = left.read(block_size)
        right_block = right.read(block_size)
        if left_block != right_block:
            common = min(len(left_block), len(right_block))
            diff_at = common
            for i in range(common):
                if left_block[i] != right_block[i]:
                    diff_at = i
                    break
            if not silent:
                line += left_block[:diff_at].count(b"\n")
                if diff_at == len(left_block):
                    print(f"cmp: EOF on {left_name}", file=sys.stderr)
                elif diff_at == len(right_block):
                    print(f"cmp: EOF on {right_name}", file=sys.stderr)
                else:
                    print(f"{left_name} {right_name} differ: byte {offset + diff_at + 1}, line {line}")
            sys.exit(1)
        if not left_block:
            sys.exit(0)
        offset += len(left_block)
        line += left_block.count(b"\n")
finally:
    if left is not sys.stdin.buffer:
        left.close()
    if right is not sys.stdin.buffer:
        right.close()
PY
    chmod +x "$BB_KBUILD_TOOLS/cmp"
    PATH="$BB_KBUILD_TOOLS:$PATH"
    export PATH
}

setup_busybox_kbuild_tools

if [ ! -f "$BB_CONFIG" ]; then
    echo "ERROR: BusyBox WOS full config not found at $BB_CONFIG" >&2
    exit 1
fi

if [ ! -f "$BB_BUILD/Makefile" ]; then
    if ! wos_make "$WOS_MAKE_JOBS" -C "$BB_SRC" O="$BB_BUILD" \
        "${BB_MAKE_SHELL_ARGS[@]}" \
        outputmakefile >/tmp/busybox_outputmakefile.log 2>&1; then
        cat /tmp/busybox_outputmakefile.log
        exit 1
    fi
fi

# Keep first-run WOS bootstrap out of BusyBox's interactive Kconfig path. This
# full config is generated from the project WOS defconfig and matches the
# resolved config produced by the previous allnoconfig+oldconfig flow.
tmp_config="$BB_BUILD/.config.wos"
cp "$BB_CONFIG" "$tmp_config"
if [ ! -f "$BB_BUILD/.config" ] || ! cmp -s "$tmp_config" "$BB_BUILD/.config"; then
    mv "$tmp_config" "$BB_BUILD/.config"
else
    rm -f "$tmp_config"
fi

# Force relink if any sysroot library is newer than the binary
if [ -f "$BB_BUILD/busybox" ]; then
    for lib in "$TARGET_SYSROOT"/lib/libc.so "$TARGET_SYSROOT"/lib/libc++.so \
               "$TARGET_SYSROOT"/lib/libc++abi.so "$TARGET_SYSROOT"/lib/libm.so; do
        if [ -f "$lib" ] && [ "$lib" -nt "$BB_BUILD/busybox" ]; then
            echo "Sysroot library $(basename "$lib") changed - forcing relink"
            rm -f "$BB_BUILD/busybox"
            break
        fi
    done
fi

# Build busybox
build_busybox_target() {
    local target="${1:-busybox}"

    wos_make "$WOS_MAKE_JOBS" -C "$BB_BUILD" \
        CC="$BB_CC" \
        AR="$BB_AR" \
        STRIP="$BB_STRIP" \
        RANLIB="$BB_RANLIB" \
        OBJCOPY="$BB_OBJCOPY" \
        NM="$BB_NM" \
        HOSTCC="$BB_HOSTCC" \
        CFLAGS="$BB_CFLAGS" \
        LDFLAGS="$BB_LDFLAGS" \
        "${BB_MAKE_SHELL_ARGS[@]}" \
        "$target"
}

cleanup_busybox_kbuild_temps() {
    find "$BB_BUILD" \
        \( -name '.*.tmp' -o -name '.*.d' \) \
        -type f -delete
}

if [ "$HOST_SYSTEM" = "WOS" ]; then
    cleanup_busybox_kbuild_temps
fi

build_busybox_target prepare
# Upstream's out-of-tree prepare rule unconditionally creates include2/asm,
# although this BusyBox tree has no include/asm-$ARCH directory. The compiler
# does not consume that path, but distributed staging dereferences symlinks and
# must not archive a dangling generated link.
if [ -L "$BB_BUILD/include2/asm" ] && [ ! -e "$BB_BUILD/include2/asm" ]; then
    rm -f "$BB_BUILD/include2/asm"
fi
wos_stage_distributed_build_roots \
    "$WORKSPACE_ROOT" "$BB_SRC" \
    "$BB_BUILD" "$TARGET_SYSROOT/include"

if ! build_busybox_target busybox; then
    echo "BusyBox build failed; cleaning Kbuild temp/dependency files and retrying once at WOS_MAKE_JOBS=$WOS_MAKE_JOBS" >&2
    cleanup_busybox_kbuild_temps
    build_busybox_target busybox
fi

# Install the generated BusyBox runtime tree for rootfs packaging.
rm -rf "$BB_INSTALL"
mkdir -p "$BB_INSTALL"
if ! wos_make "$WOS_MAKE_JOBS" -C "$BB_BUILD" \
    CC="$BB_CC" \
    AR="$BB_AR" \
    STRIP="$BB_STRIP" \
    RANLIB="$BB_RANLIB" \
    OBJCOPY="$BB_OBJCOPY" \
    NM="$BB_NM" \
    HOSTCC="$BB_HOSTCC" \
    CFLAGS="$BB_CFLAGS" \
    LDFLAGS="$BB_LDFLAGS" \
    "${BB_MAKE_SHELL_ARGS[@]}" \
    CONFIG_PREFIX="$BB_INSTALL" \
    install >/tmp/busybox_install.log 2>&1; then
    cat /tmp/busybox_install.log
    exit 1
fi

# Always stage the freshly linked top-level BusyBox binary. When shared BusyBox
# was enabled in an earlier build, 0_lib/ can keep stale launcher artifacts
# around even after the config flips back to monolithic mode.
if [ -f "$BB_BUILD/busybox" ]; then
    install -m 755 "$BB_BUILD/busybox" "$BB_INSTALL/bin/busybox"
fi

shared_lib=""
if busybox_config_enabled CONFIG_FEATURE_SHARED_BUSYBOX; then
    for candidate in "$BB_SHARED_DIR"/libbusybox.so.*; do
        case "$candidate" in
            *"_unstripped"*|*.map|*.out)
                continue
                ;;
        esac
        if [ -f "$candidate" ]; then
            shared_lib="$candidate"
            break
        fi
    done
fi

if [ -n "$shared_lib" ]; then
    mkdir -p "$BB_INSTALL/lib"
    rm -f "$BB_INSTALL/lib"/libbusybox.so*
    cp -pPR "$shared_lib" "$BB_INSTALL/lib/"
    ln -sfn "$(basename "$shared_lib")" "$BB_INSTALL/lib/libbusybox.so"
else
    rm -rf "$BB_INSTALL/lib"
fi

echo "busybox installed to $BB_INSTALL"
