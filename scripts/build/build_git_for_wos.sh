#!/bin/bash
# Cross-build the WOS Git fork and install it into the target sysroot.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-git-ccache"
    mkdir -p "$CCACHE_DIR"
fi
wos_setup_ccache
WOS_CCACHE_PREFIX="$(wos_ccache_prefix)"
WOS_MAKE_JOBS="$(wos_make_jobs)"

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
GIT_BUILD="${WOS_GIT_BUILD_DIR:-$B/git-build}"
GIT_SRC="${WOS_GIT_SOURCE_DIR:-$B/src/git}"
GIT_WORK="$GIT_BUILD/work"
WOS_GIT_STRIP="${WOS_GIT_STRIP:-0}"
WOS_GIT_OPT_FLAGS="${WOS_GIT_OPT_FLAGS:--O2}"

export PATH="$HOST/bin:$PATH"
export LD_LIBRARY_PATH="$HOST/lib"

require_file() {
    local path="$1"
    local hint="$2"

    if [ ! -e "$path" ]; then
        echo "ERROR: missing $path" >&2
        echo "$hint" >&2
        exit 1
    fi
}

copy_source_to_workdir() {
    wos_remove_tree "$GIT_WORK"
    mkdir -p "$GIT_WORK"
    wos_copy_tree_entries_excluding "$GIT_SRC" "$GIT_WORK" ".git" ".github"
}

require_file "$HOST/bin/clang" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ar" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ranlib" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-strip" "Run tools/host-toolchain.sh first."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building Git."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building Git."
require_file "$TARGET_SYSROOT/bin/bash" "Build Bash before building Git."
require_file "$TARGET_SYSROOT/lib/libz.a" "Run scripts/build/build_zlib_for_wos.sh before building Git."
require_file "$TARGET_SYSROOT/include/zlib.h" "Run scripts/build/build_zlib_for_wos.sh before building Git."
require_file "$TARGET_SYSROOT/lib/libcurl.a" "Run scripts/build/build_curl_for_wos.sh before building Git."
require_file "$TARGET_SYSROOT/bin/curl-config" "Run scripts/build/build_curl_for_wos.sh before building Git."
require_file "$TARGET_SYSROOT/include/curl/curl.h" "Run scripts/build/build_curl_for_wos.sh before building Git."
require_file "$TARGET_SYSROOT/lib/libssl.a" "Run scripts/build/build_openssl_for_wos.sh before building Git."
require_file "$TARGET_SYSROOT/lib/libcrypto.a" "Run scripts/build/build_openssl_for_wos.sh before building Git."
require_file "$GIT_SRC/Makefile" "Initialize the Git source with: git submodule update --init toolchain/src/git"

copy_source_to_workdir

mkdir -p "$TARGET_SYSROOT/bin" "$TARGET_SYSROOT/libexec" "$TARGET_SYSROOT/share"
if [ ! -e "$TARGET_SYSROOT/usr" ]; then
    ln -s . "$TARGET_SYSROOT/usr"
fi

TARGET_CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
GIT_CFLAGS="--sysroot=$TARGET_SYSROOT $WOS_GIT_OPT_FLAGS -g -fPIC -fPIE -fno-sanitize=safe-stack -fno-stack-protector -DHAVE_WOS_FSTAT_CLOSE=1 -I. -Icompat/regex -I$TARGET_SYSROOT/include"
GIT_LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--dynamic-linker=/lib/ld.so -Wl,-rpath,/usr/lib -fno-sanitize=safe-stack"
GIT_EXTLIBS="$TARGET_SYSROOT/lib/libz.a -lpthread -lrt -ldl -lm -lc"
GIT_CURL_CFLAGS="-I$TARGET_SYSROOT/include -DCURL_STATICLIB"
GIT_CURL_LDFLAGS="$TARGET_SYSROOT/lib/libcurl.a $TARGET_SYSROOT/lib/libssl.a $TARGET_SYSROOT/lib/libcrypto.a $TARGET_SYSROOT/lib/libz.a -lpthread -ldl"

GIT_MAKE_FLAGS=(
    "uname_S=WOS"
    "prefix="
    "bindir=/bin"
    "gitexecdir=/libexec/git-core"
    "template_dir=/share/git-core/templates"
    "sysconfdir=/etc"
    "CC=$TARGET_CC"
    "AR=$HOST/bin/llvm-ar"
    "RANLIB=$HOST/bin/llvm-ranlib"
    "CFLAGS=$GIT_CFLAGS"
    "LDFLAGS=$GIT_LDFLAGS"
    "EXTLIBS=$GIT_EXTLIBS"
    "CURL_CONFIG=$TARGET_SYSROOT/bin/curl-config"
    "CURL_CFLAGS=$GIT_CURL_CFLAGS"
    "CURL_LDFLAGS=$GIT_CURL_LDFLAGS"
    "PYTHON_PATH=/usr/bin/python3"
    "SHELL_PATH=/bin/bash"
    "SHELL=/bin/bash"
    "CSPRNG_METHOD=urandom"
    "HAVE_ALLOCA_H=YesPlease"
    "HAVE_PATHS_H=YesPlease"
    "HAVE_CLOCK_GETTIME=YesPlease"
    "HAVE_CLOCK_MONOTONIC=YesPlease"
    "HAVE_GETDELIM=YesPlease"
    "NO_OPENSSL=YesPlease"
    "NO_EXPAT=YesPlease"
    "NO_GETTEXT=YesPlease"
    "NO_ICONV=YesPlease"
    "NO_PCRE2=YesPlease"
    "NO_TCLTK=YesPlease"
    "NO_PERL=YesPlease"
    "NO_RUST=YesPlease"
    "NO_INSTALL_HARDLINKS=YesPlease"
    "INSTALL_SYMLINKS=YesPlease"
    "NO_IPV6=YesPlease"
    "NO_REGEX=NeedsStartEnd"
    "NO_TRUSTABLE_FILEMODE=YesPlease"
    "WOS_SKIP_TEST_ARTIFACTS=YesPlease"
)

wos_make "$WOS_MAKE_JOBS" -C "$GIT_WORK" "${GIT_MAKE_FLAGS[@]}" \
    generated-hdrs version-def.h
wos_stage_distributed_build_roots \
    "$WORKSPACE_ROOT" "" \
    "$GIT_WORK" "$TARGET_SYSROOT/include"

wos_make "$WOS_MAKE_JOBS" -C "$GIT_WORK" "${GIT_MAKE_FLAGS[@]}" "DESTDIR=$TARGET_SYSROOT" install

require_file "$TARGET_SYSROOT/bin/git" "Git install did not produce $TARGET_SYSROOT/bin/git."
require_file "$TARGET_SYSROOT/libexec/git-core/git" "Git install did not produce $TARGET_SYSROOT/libexec/git-core/git."
require_file "$TARGET_SYSROOT/libexec/git-core/git-remote-http" "Git install did not produce git-remote-http."
require_file "$TARGET_SYSROOT/libexec/git-core/git-remote-https" "Git install did not produce git-remote-https."
require_file "$TARGET_SYSROOT/share/git-core/templates" "Git install did not produce templates."

if [ "$WOS_GIT_STRIP" != "0" ]; then
    for binary in "$TARGET_SYSROOT"/bin/git "$TARGET_SYSROOT"/bin/git-shell "$TARGET_SYSROOT"/bin/scalar \
                  "$TARGET_SYSROOT"/libexec/git-core/git-daemon \
                  "$TARGET_SYSROOT"/libexec/git-core/git-http-backend \
                  "$TARGET_SYSROOT"/libexec/git-core/git-imap-send \
                  "$TARGET_SYSROOT"/libexec/git-core/git-sh-i18n--envsubst; do
        [ -f "$binary" ] || continue
        "$HOST/bin/llvm-strip" "$binary" || true
    done
fi

echo "Native WOS Git installed to $TARGET_SYSROOT/bin/git"
