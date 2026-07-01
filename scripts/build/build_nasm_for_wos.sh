#!/bin/bash
# Cross-build NASM so it can run inside WOS and install it into the sysroot.
# Expects the WOS host toolchain and mlibc sysroot to already be available.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-nasm-ccache"
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
HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"
NASM_SRC="${WOS_NASM_SOURCE_DIR:-$B/src/nasm}"
NASM_BUILD="${WOS_NASM_BUILD_DIR:-$B/nasm-build}"
NASM_VERSION="${WOS_NASM_VERSION:-3.02rc9}"
NASM_TARBALL_URL="${WOS_NASM_TARBALL_URL:-https://www.nasm.us/pub/nasm/releasebuilds/$NASM_VERSION/nasm-$NASM_VERSION.tar.xz}"
NASM_TARBALL_SHA256="${WOS_NASM_TARBALL_SHA256:-1802d091f4b2c1b3f61ab9d9fca323b8da7674c1ced7d5b770e77604d9c7925b}"
NASM_TARBALL_URLS="${WOS_NASM_TARBALL_URLS:-$NASM_TARBALL_URL}"
NASM_DOWNLOAD_ATTEMPTS="${WOS_NASM_DOWNLOAD_ATTEMPTS:-${WOS_SOURCE_DOWNLOAD_ATTEMPTS:-3}}"
WOS_NASM_STRIP="${WOS_NASM_STRIP:-0}"

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

patch_config_sub_for_wos() {
    local config_sub="$1"

    require_file "$config_sub" "NASM source is missing autoconf/helpers/config.sub."
    if grep -q 'wos\*' "$config_sub"; then
        return 0
    fi

    echo "Patching NASM config.sub to recognise WOS..."
    if grep -q '| fiwix\* )' "$config_sub"; then
        sed -i 's/| fiwix\* )/| fiwix* | wos* )/' "$config_sub"
    elif grep -q '| fiwix\* |' "$config_sub"; then
        sed -i 's/| fiwix\* |/| fiwix* | wos* |/' "$config_sub"
    else
        echo "ERROR: do not know how to patch $config_sub for WOS." >&2
        echo "Create a wos-support branch in Pascu-Victor/nasm with config.sub WOS support." >&2
        exit 1
    fi
}

download_nasm_source() {
    local dest="$1"
    local archive_dir="$B/src"
    local archive="$archive_dir/nasm-$NASM_VERSION.tar.xz"
    local tmp_dest="$dest.tmp"

    mkdir -p "$archive_dir"
    if [ ! -f "$archive" ]; then
        if ! command -v curl >/dev/null 2>&1; then
            echo "ERROR: NASM source at $NASM_SRC lacks a generated configure script and curl is unavailable." >&2
            echo "Populate $NASM_SRC with a NASM release tree or install curl." >&2
            exit 1
        fi
        wos_download_file "NASM $NASM_VERSION source" "$archive" "$NASM_TARBALL_URLS" "$NASM_DOWNLOAD_ATTEMPTS"
    fi

    echo "$NASM_TARBALL_SHA256  $archive" | sha256sum -c - >&2
    wos_remove_tree "$tmp_dest"
    wos_remove_tree "$dest"
    mkdir -p "$tmp_dest"
    tar -xJf "$archive" -C "$tmp_dest" --strip-components 1
    mv "$tmp_dest" "$dest"
}

resolve_nasm_source() {
    local fallback_src="$B/src/nasm-$NASM_VERSION"

    if [ -f "$NASM_SRC/configure" ]; then
        printf '%s\n' "$NASM_SRC"
        return 0
    fi

    if [ -f "$fallback_src/configure" ]; then
        printf '%s\n' "$fallback_src"
        return 0
    fi

    if [ -d "$NASM_SRC" ] && wos_dir_has_entries "$NASM_SRC"; then
        echo "NASM source at $NASM_SRC lacks generated configure; using pinned release tarball." >&2
    fi

    download_nasm_source "$fallback_src"
    printf '%s\n' "$fallback_src"
}

seed_nasm_generated_files() {
    local generated_files=(
        x86/insns.xda
        x86/iflag.c
        x86/iflaggen.h
        x86/insnsb.c
        x86/insnsa.c
        x86/insnsd.c
        x86/insnsi.h
        x86/insnsn.c
        version.h
        version.mac
        version.sed
        version.mak
        nsis/version.nsh
        x86/regs.c
        x86/regflags.c
        x86/regdis.c
        x86/regdis.h
        x86/regvals.c
        x86/regs.h
        asm/tokhash.c
        asm/tokens.h
        asm/pptok.h
        asm/pptok.c
        asm/pptok.ph
        doc/pptok.src
        macros/macros.c
        asm/directiv.h
        asm/directbl.c
        editors/nasmtok.el
        editors/nasmtok.json
        asm/warnings_c.h
        include/warnings.h
        doc/warnings.src
    )
    local file

    for file in "${generated_files[@]}"; do
        [ -f "$NASM_SOURCE_DIR/$file" ] || continue
        mkdir -p "$NASM_BUILD/$(dirname "$file")"
        cp -p "$NASM_SOURCE_DIR/$file" "$NASM_BUILD/$file"
        wos_refresh_file_mtime "$NASM_BUILD/$file"
    done

    for file in \
        asm/tokhash.c \
        asm/tokens.h \
        macros/macros.c \
        editors/nasmtok.el \
        editors/nasmtok.json; do
        [ -f "$NASM_BUILD/$file" ] || continue
        wos_refresh_file_mtime "$NASM_BUILD/$file"
    done
}

write_nasm_config_site() {
    local tmp_config_site

    mkdir -p "$NASM_BUILD"
    tmp_config_site="$(mktemp "$NASM_BUILD/config.site.XXXXXX")"
    cat > "$tmp_config_site" <<'EOF'
ac_cv_c_bigendian=no
ac_cv_c_const=yes
ac_cv_c_future_darwin_options='none needed'
ac_cv_c_inline=inline
ac_cv_c_restrict=__restrict__
ac_cv_c_undeclared_builtin_options='none needed'
ac_cv_func__access=no
ac_cv_func__chsize=no
ac_cv_func__chsize_s=no
ac_cv_func__fileno=no
ac_cv_func__fseeki64=no
ac_cv_func__fullpath=no
ac_cv_func_access=yes
ac_cv_func_canonicalize_file_name=yes
ac_cv_func_faccessat=yes
ac_cv_func_fileno=yes
ac_cv_func_fseeko_ftello=yes
ac_cv_func_fstat=yes
ac_cv_func_ftruncate=yes
ac_cv_func_getgid=yes
ac_cv_func_getpagesize=yes
ac_cv_func_getrlimit=yes
ac_cv_func_getuid=yes
ac_cv_func_isascii=yes
ac_cv_func_iscntrl=yes
ac_cv_func_mempcpy=yes
ac_cv_func_mempset=no
ac_cv_func_mmap_fixed_mapped=no
ac_cv_func_pathconf=yes
ac_cv_func_realpath=yes
ac_cv_func_stat=yes
ac_cv_func_strcasecmp=yes
ac_cv_func_stricmp=no
ac_cv_func_strlcpy=yes
ac_cv_func_strncasecmp=yes
ac_cv_func_strnicmp=no
ac_cv_func_strnlen=yes
ac_cv_func_strsep=yes
ac_cv_func_sysconf=yes
ac_cv_have_decl_strcasecmp=yes
ac_cv_have_decl_stricmp=no
ac_cv_have_decl_strlcpy=yes
ac_cv_have_decl_strncasecmp=yes
ac_cv_have_decl_strnicmp=no
ac_cv_have_decl_strnlen=yes
ac_cv_have_decl_strsep=yes
ac_cv_header_arpa_inet_h=yes
ac_cv_header_byteswap_h=yes
ac_cv_header_endian_h=yes
ac_cv_header_fcntl_h=yes
ac_cv_header_intrin_h=no
ac_cv_header_inttypes_h=yes
ac_cv_header_io_h=no
ac_cv_header_machine_endian_h=yes
ac_cv_header_minix_config_h=no
ac_cv_header_stdarg_h=yes
ac_cv_header_stdbit_h=no
ac_cv_header_stdbool_h=yes
ac_cv_header_stdint_h=yes
ac_cv_header_stdio_h=yes
ac_cv_header_stdlib_h=yes
ac_cv_header_stdnoreturn_h=yes
ac_cv_header_string_h=yes
ac_cv_header_strings_h=yes
ac_cv_header_sys_endian_h=yes
ac_cv_header_sys_mman_h=yes
ac_cv_header_sys_param_h=yes
ac_cv_header_sys_resource_h=yes
ac_cv_header_sys_stat_h=yes
ac_cv_header_sys_types_h=yes
ac_cv_header_unistd_h=yes
ac_cv_header_wchar_h=yes
ac_cv_objext=o
ac_cv_prog_cc_c23=-std=gnu23
ac_cv_prog_cc_g=yes
ac_cv_prog_cc_stdc=-std=gnu23
ac_cv_prog_make_make_set=yes
ac_cv_safe_to_define___extensions__=yes
ac_cv_search_inflate=no
ac_cv_should_define__xopen_source=no
ac_cv_sys_largefile_opts='none needed'
ac_cv_type__Bool=yes
ac_cv_type_size_t=yes
ac_cv_type_struct__stati64=no
ac_cv_type_struct_stat=yes
ac_cv_type_uintmax_t=yes
ac_cv_type_uintptr_t=yes
ac_cv_type_unsigned_long_long_int=yes
pa_cv_CFLAGS__U__STRICT_ANSI__=yes
pa_cv_CFLAGS__W=yes
pa_cv_CFLAGS__Wall=yes
pa_cv_CFLAGS__Werror_comment=yes
pa_cv_CFLAGS__Werror_implicit=yes
pa_cv_CFLAGS__Werror_missing_braces=yes
pa_cv_CFLAGS__Werror_missing_declarations=yes
pa_cv_CFLAGS__Werror_missing_prototypes=yes
pa_cv_CFLAGS__Werror_pointer_arith=yes
pa_cv_CFLAGS__Werror_return_type=yes
pa_cv_CFLAGS__Werror_strict_prototypes=yes
pa_cv_CFLAGS__Werror_trigraphs=yes
pa_cv_CFLAGS__Werror_unknown_warning_option=yes
pa_cv_CFLAGS__Werror_vla=yes
pa_cv_CFLAGS__Wlong_long=yes
pa_cv_CFLAGS__Wpedantic_ms_format=no
pa_cv_CFLAGS__Wshift_negative_value=yes
pa_cv_CFLAGS__Wstringop_truncation=no
pa_cv_CFLAGS__Wvariadic_macros=yes
pa_cv_CFLAGS__fdata_sections=yes
pa_cv_CFLAGS__ffunction_sections=yes
pa_cv_CFLAGS__fno_common=yes
pa_cv_CFLAGS__ftrivial_auto_var_init_zero=yes
pa_cv_CFLAGS__fvisibility_hidden=yes
pa_cv_CFLAGS__fwrapv=yes
pa_cv_CFLAGS__g3=yes
pa_cv_CFLAGS__ggdb=yes
pa_cv_CFLAGS__pedantic=yes
pa_cv_CFLAGS__std_c23=yes
pa_cv_CPPFLAGS__Werror_attributes=yes
pa_cv_LDFLAGS__Wl___as_needed=yes
pa_cv_LDFLAGS__Wl___gc_sections=yes
pa_cv_func_S_ISREG=yes
pa_cv_func___builtin_choose_expr=yes
pa_cv_func___builtin_clz=yes
pa_cv_func___builtin_clzll=yes
pa_cv_func___builtin_constant_p=yes
pa_cv_func___builtin_expect=yes
pa_cv_func___builtin_prefetch=yes
pa_cv_func___builtin_unreachable=yes
pa_cv_func_htobe16=yes
pa_cv_func_htobe32=yes
pa_cv_func_htobe64=yes
pa_cv_func_htole16=yes
pa_cv_func_htole32=yes
pa_cv_func_htole64=yes
pa_cv_func_name=__func__
pa_cv_func_snprintf=snprintf
pa_cv_func_stdc_leading_zeros=no
pa_cv_func_vsnprintf=vsnprintf
pa_cv_typeof=typeof
pa_cv_variadic_macros=yes
EOF

    if [ ! -f "$NASM_BUILD/config.site" ] || ! cmp -s "$tmp_config_site" "$NASM_BUILD/config.site"; then
        mv "$tmp_config_site" "$NASM_BUILD/config.site"
    else
        rm -f "$tmp_config_site"
    fi
}

require_file "$HOST/bin/clang" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/ld.lld" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ar" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ranlib" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-strip" "Run tools/host-toolchain.sh first."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building NASM."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building NASM."

NASM_SOURCE_DIR="$(resolve_nasm_source)"
require_file "$NASM_SOURCE_DIR/configure" "NASM source is missing generated configure."
require_file "$NASM_SOURCE_DIR/autoconf/helpers/config.guess" "NASM source is missing config.guess."
require_file "$NASM_SOURCE_DIR/autoconf/helpers/config.sub" "NASM source is missing config.sub."

if [ -f "$NASM_BUILD/source.path" ] && [ "$(cat "$NASM_BUILD/source.path")" != "$NASM_SOURCE_DIR" ]; then
    wos_remove_tree "$NASM_BUILD"
fi
patch_config_sub_for_wos "$NASM_SOURCE_DIR/autoconf/helpers/config.sub"

if [ "$HOST_SYSTEM" = "WOS" ]; then
    BUILD_TRIPLE="$TARGET_ARCH"
else
    BUILD_TRIPLE="$("$NASM_SOURCE_DIR/autoconf/helpers/config.guess")"
fi

mkdir -p "$NASM_BUILD" "$TARGET_SYSROOT/bin"
printf '%s\n' "$NASM_SOURCE_DIR" > "$NASM_BUILD/source.path"
write_nasm_config_site
export CONFIG_SITE="$NASM_BUILD/config.site"

NASM_CFLAGS="--sysroot=$TARGET_SYSROOT -O2 -g -m64 -fPIC -fPIE -fno-sanitize=safe-stack -fno-stack-protector"
NASM_LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--dynamic-linker=/lib/ld.so -Wl,-rpath,/usr/lib -fno-sanitize=safe-stack"

export CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
export AR="$HOST/bin/llvm-ar"
export RANLIB="$HOST/bin/llvm-ranlib"
export STRIP="$HOST/bin/llvm-strip"
export CFLAGS="$NASM_CFLAGS"
# Keep NASM's project headers ahead of any sysroot headers.
export CPPFLAGS=""
export LDFLAGS="$NASM_LDFLAGS"

if [ -f "$NASM_BUILD/Makefile" ] && grep -F -- "-I$TARGET_SYSROOT/include" "$NASM_BUILD/Makefile" >/dev/null 2>&1; then
    echo "NASM build has stale sysroot CPPFLAGS - reconfiguring..."
    rm -f "$NASM_BUILD/Makefile" "$NASM_BUILD/config.status"
fi

if [ ! -f "$NASM_BUILD/Makefile" ] || [ "$NASM_SOURCE_DIR/configure" -nt "$NASM_BUILD/Makefile" ] ||
    [ "$NASM_BUILD/config.site" -nt "$NASM_BUILD/Makefile" ]; then
    echo "Configuring NASM for WOS..."
    NASM_CONFIGURE_CACHE_ARGS=()
    if [ "$HOST_SYSTEM" = "WOS" ]; then
        NASM_CONFIGURE_CACHE_ARGS=(
            "ac_cv_path_install=/usr/bin/install -c"
            ac_cv_path_mkdir=/usr/bin/mkdir
            ac_cv_prog_ASCIIDOC=false
            ac_cv_prog_XMLTO=false
            ac_cv_prog_XZ=false
        )
    fi
    wos_timed_step "configure" "nasm" \
        wos_run_env_in_dir "$NASM_BUILD" \
        ASCIIDOC=false \
        XMLTO=false \
        XZ=false \
        "$NASM_SOURCE_DIR/configure" \
        "${NASM_CONFIGURE_CACHE_ARGS[@]}" \
        --build="$BUILD_TRIPLE" \
        --host="$TARGET_ARCH" \
        --prefix=/usr \
        --with-zlib=no \
        --disable-pdf-compression
fi

seed_nasm_generated_files

if [ -f "$NASM_BUILD/nasm" ]; then
    for lib in "$TARGET_SYSROOT"/lib/libc.so "$TARGET_SYSROOT"/lib/libm.so; do
        if [ -f "$lib" ] && [ "$lib" -nt "$NASM_BUILD/nasm" ]; then
            echo "Sysroot library $(basename "$lib") changed - forcing relink"
            rm -f "$NASM_BUILD/nasm" "$NASM_BUILD/ndisasm"
            break
        fi
    done
fi

wos_make "$WOS_MAKE_JOBS" -C "$NASM_BUILD" nasm ndisasm

install -m 755 "$NASM_BUILD/nasm" "$TARGET_SYSROOT/bin/nasm"
install -m 755 "$NASM_BUILD/ndisasm" "$TARGET_SYSROOT/bin/ndisasm"

if [ "$WOS_NASM_STRIP" != "0" ]; then
    "$HOST/bin/llvm-strip" "$TARGET_SYSROOT/bin/nasm" "$TARGET_SYSROOT/bin/ndisasm"
fi

echo "Native WOS NASM installed to $TARGET_SYSROOT/bin/nasm"
