#!/bin/bash
# Cross-build Git so it can run inside WOS and install it into the sysroot.
# This intentionally keeps Git's own optional helpers small, but enables
# HTTP(S) transport through the WOS curl/OpenSSL ports.
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
WOS_BUILD_JOBS="$(wos_build_jobs)"
WOS_MAKE_JOBS="$(wos_make_jobs)"

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
GIT_BUILD="${WOS_GIT_BUILD_DIR:-$B/git-build}"
GIT_SRC="${WOS_GIT_SOURCE_DIR:-$B/src/git}"
GIT_WORK="$GIT_BUILD/work"
GIT_VERSION="${WOS_GIT_VERSION:-2.54.0}"
GIT_TARBALL_URL="${WOS_GIT_TARBALL_URL:-https://www.kernel.org/pub/software/scm/git/git-$GIT_VERSION.tar.xz}"
GIT_TARBALL_SHA256="${WOS_GIT_TARBALL_SHA256:-f689162364c10de79ef89aa8dbf48731eb057e34edbbd20aca510ce0154681a3}"
WOS_GIT_STRIP="${WOS_GIT_STRIP:-0}"

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

download_git_source() {
    local dest="$1"
    local archive_dir="$GIT_BUILD/src"
    local archive="$archive_dir/git-$GIT_VERSION.tar.xz"
    local tmp_dest="$dest.tmp"

    mkdir -p "$archive_dir"
    if [ ! -f "$archive" ]; then
        if ! command -v curl >/dev/null 2>&1; then
            echo "ERROR: Git source not found at $GIT_SRC and curl is unavailable." >&2
            echo "Populate $GIT_SRC with a Git release tree or install curl." >&2
            exit 1
        fi
        echo "Downloading Git $GIT_VERSION source..." >&2
        curl -L "$GIT_TARBALL_URL" -o "$archive.tmp"
        mv "$archive.tmp" "$archive"
    fi

    echo "$GIT_TARBALL_SHA256  $archive" | sha256sum -c - >&2
    wos_remove_tree "$tmp_dest"
    wos_remove_tree "$dest"
    mkdir -p "$tmp_dest"
    tar -xJf "$archive" -C "$tmp_dest" --strip-components 1
    mv "$tmp_dest" "$dest"
}

resolve_git_source() {
    local fallback_src="$GIT_BUILD/src/git-$GIT_VERSION"

    if [ -f "$GIT_SRC/Makefile" ]; then
        printf '%s\n' "$GIT_SRC"
        return 0
    fi

    if [ -f "$fallback_src/Makefile" ]; then
        printf '%s\n' "$fallback_src"
        return 0
    fi

    if [ -d "$GIT_SRC" ] && wos_dir_has_entries "$GIT_SRC"; then
        echo "ERROR: Git source at $GIT_SRC does not contain Makefile." >&2
        echo "Use a Git release tree or clear the directory so the release tarball can be downloaded." >&2
        exit 1
    fi

    download_git_source "$fallback_src"
    printf '%s\n' "$fallback_src"
}

copy_source_to_workdir() {
    local source_dir="$1"

    wos_remove_tree "$GIT_WORK"
    mkdir -p "$GIT_WORK"
    wos_copy_tree_entries_excluding "$source_dir" "$GIT_WORK" ".git" ".github"
}

patch_git_source_for_wos() {
    local run_command="$GIT_WORK/run-command.c"
    local parallel_checkout="$GIT_WORK/parallel-checkout.c"
    local makefile="$GIT_WORK/Makefile"
    local templates_makefile="$GIT_WORK/templates/Makefile"

    python3 - "$run_command" "$parallel_checkout" "$makefile" "$templates_makefile" <<'PY'
from pathlib import Path
import sys

run_command = Path(sys.argv[1])
parallel_checkout = Path(sys.argv[2])
makefile = Path(sys.argv[3])
templates_makefile = Path(sys.argv[4])

text = run_command.read_text()
replacements = {
    "pipe(fdin)": ("pipe2(fdin, O_CLOEXEC)", 2),
    "pipe(fdout)": ("pipe2(fdout, O_CLOEXEC)", 2),
    "pipe(fderr)": ("pipe2(fderr, O_CLOEXEC)", 1),
    "pipe(notify_pipe)": ("pipe2(notify_pipe, O_CLOEXEC)", 1),
}

for old, (new, expected_count) in replacements.items():
    count = text.count(old)
    if count != expected_count:
        raise SystemExit(f"expected {expected_count} occurrences of {old!r} in {run_command}, found {count}")
    text = text.replace(old, new)

run_command.write_text(text)

text = parallel_checkout.read_text()
replacements = {
    "static const int DEFAULT_THRESHOLD_FOR_PARALLELISM = 100;": "static const int DEFAULT_THRESHOLD_FOR_PARALLELISM = 0;",
    "static const int DEFAULT_NUM_WORKERS = 1;": "static const int DEFAULT_NUM_WORKERS = 0;",
}

for old, new in replacements.items():
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one occurrence of {old!r} in {parallel_checkout}, found {count}")
    text = text.replace(old, new)

parallel_checkout.write_text(text)

text = makefile.read_text()
replacements = {
    "all:: $(FUZZ_OBJS)": "ifndef WOS_SKIP_TEST_ARTIFACTS\nall:: $(FUZZ_OBJS)\nendif",
    "all:: $(TEST_PROGRAMS) $(test_bindir_programs) $(UNIT_TEST_PROGS) $(CLAR_TEST_PROG)": (
        "ifndef WOS_SKIP_TEST_ARTIFACTS\n"
        "all:: $(TEST_PROGRAMS) $(test_bindir_programs) $(UNIT_TEST_PROGS) $(CLAR_TEST_PROG)\n"
        "endif"
    ),
}

for old, new in replacements.items():
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one occurrence of {old!r} in {makefile}, found {count}")
    text = text.replace(old, new)

makefile.write_text(text)

text = templates_makefile.read_text()
old = "\t(cd blt && $(TAR) cf - .) | \\\n\t(cd '$(DESTDIR_SQ)$(template_instdir_SQ)' && umask 022 && $(TAR) xof -)"
new = "\tcp -a blt/. '$(DESTDIR_SQ)$(template_instdir_SQ)'"
count = text.count(old)
if count != 1:
    raise SystemExit(f"expected one Git templates tar install pipeline in {templates_makefile}, found {count}")
templates_makefile.write_text(text.replace(old, new))
PY
}

patch_installed_git_scripts() {
    local submodule="$TARGET_SYSROOT/libexec/git-core/git-submodule"

    python3 - "$submodule" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text()
old = '"cmd_$(echo $command | sed -e s/-/_/g)" "$@"'
new = '''case "$command" in
set-branch)
\tcmd_set_branch "$@"
\t;;
set-url)
\tcmd_set_url "$@"
\t;;
*)
\t"cmd_$command" "$@"
\t;;
esac'''
if old not in text and new not in text:
    raise SystemExit(f"unable to find git-submodule dispatcher in {path}")
path.write_text(text.replace(old, new))
PY
}

require_file "$HOST/bin/clang" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ar" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ranlib" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-strip" "Run tools/host-toolchain.sh first."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building Git."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building Git."
require_file "$TARGET_SYSROOT/lib/libz.a" "Run scripts/build/build_zlib_for_wos.sh before building Git."
require_file "$TARGET_SYSROOT/include/zlib.h" "Run scripts/build/build_zlib_for_wos.sh before building Git."
require_file "$TARGET_SYSROOT/lib/libcurl.a" "Run scripts/build/build_curl_for_wos.sh before building Git."
require_file "$TARGET_SYSROOT/bin/curl-config" "Run scripts/build/build_curl_for_wos.sh before building Git."
require_file "$TARGET_SYSROOT/include/curl/curl.h" "Run scripts/build/build_curl_for_wos.sh before building Git."
require_file "$TARGET_SYSROOT/lib/libssl.a" "Run scripts/build/build_openssl_for_wos.sh before building Git."
require_file "$TARGET_SYSROOT/lib/libcrypto.a" "Run scripts/build/build_openssl_for_wos.sh before building Git."

GIT_SOURCE_DIR="$(resolve_git_source)"
copy_source_to_workdir "$GIT_SOURCE_DIR"
patch_git_source_for_wos

mkdir -p "$TARGET_SYSROOT/bin" "$TARGET_SYSROOT/libexec" "$TARGET_SYSROOT/share"
if [ ! -e "$TARGET_SYSROOT/usr" ]; then
    ln -s . "$TARGET_SYSROOT/usr"
fi

TARGET_CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
GIT_CFLAGS="--sysroot=$TARGET_SYSROOT -O2 -g -fPIC -fPIE -fno-sanitize=safe-stack -fno-stack-protector -I. -Icompat/regex -I$TARGET_SYSROOT/include"
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
    "NO_INSTALL_HARDLINKS=YesPlease"
    "INSTALL_SYMLINKS=YesPlease"
    "NO_IPV6=YesPlease"
    "NO_REGEX=NeedsStartEnd"
    "NO_TRUSTABLE_FILEMODE=YesPlease"
    "WOS_SKIP_TEST_ARTIFACTS=YesPlease"
)

wos_make "$WOS_MAKE_JOBS" -C "$GIT_WORK" "${GIT_MAKE_FLAGS[@]}" "DESTDIR=$TARGET_SYSROOT" install
patch_installed_git_scripts

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
