#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
BOOTSTRAP = ROOT / "tools" / "bootstrap.sh"
CCACHE_ENV = ROOT / "tools" / "ccache_env.sh"
ROOT_CMAKE = ROOT / "CMakeLists.txt"
WOS_TOOLCHAIN = ROOT / "tools" / "wos-toolchain.sh"
WOS_CLANG_BUILD = ROOT / "scripts" / "build" / "build_clang_for_wos.sh"
WOS_BUSYBOX_BUILD = ROOT / "scripts" / "build" / "build_busybox.sh"
WOS_CMAKE_BUILD = ROOT / "scripts" / "build" / "build_cmake_for_wos.sh"
WOS_CURL_BUILD = ROOT / "scripts" / "build" / "build_curl_for_wos.sh"
WOS_DROPBEAR_BUILD = ROOT / "scripts" / "build" / "build_dropbear.sh"
WOS_GIT_BUILD = ROOT / "scripts" / "build" / "build_git_for_wos.sh"
WOS_MESON_BUILD = ROOT / "scripts" / "build" / "build_meson_for_wos.sh"
WOS_MLIBC_BUILD = ROOT / "scripts" / "build" / "build_mlibc.sh"
WOS_NASM_BUILD = ROOT / "scripts" / "build" / "build_nasm_for_wos.sh"
WOS_NINJA_BUILD = ROOT / "scripts" / "build" / "build_ninja_for_wos.sh"
WOS_TLS_BUILD = ROOT / "scripts" / "build" / "build_openssl_for_wos.sh"
WOS_PYTHON_BUILD = ROOT / "scripts" / "build" / "build_python_for_wos.sh"
MLIBC_STDIO = ROOT / "toolchain" / "src" / "mlibc" / "options" / "ansi" / "generic" / "stdio.cpp"
MLIBC_SSCANF_TEST = ROOT / "toolchain" / "src" / "mlibc" / "tests" / "ansi" / "sscanf.c"
MLIBC_NAMESER = ROOT / "toolchain" / "src" / "mlibc" / "options" / "bsd" / "generic" / "arpa-nameser.cpp"
MLIBC_RESOLV = ROOT / "toolchain" / "src" / "mlibc" / "options" / "glibc" / "generic" / "resolv.cpp"
WOS_JOB_HELPER_USERS = [
    ROOT / "scripts" / "build" / "build_bash_for_wos.sh",
    WOS_BUSYBOX_BUILD,
    WOS_CLANG_BUILD,
    ROOT / "scripts" / "build" / "build_cmake_for_host.sh",
    ROOT / "scripts" / "build" / "build_cmake_for_wos.sh",
    ROOT / "scripts" / "build" / "build_curl_for_wos.sh",
    WOS_DROPBEAR_BUILD,
    ROOT / "scripts" / "build" / "build_git_for_wos.sh",
    ROOT / "scripts" / "build" / "build_make.sh",
    WOS_MLIBC_BUILD,
    WOS_NASM_BUILD,
    WOS_NINJA_BUILD,
    WOS_TLS_BUILD,
    WOS_PYTHON_BUILD,
    ROOT / "scripts" / "build" / "build_zlib_for_wos.sh",
    WOS_TOOLCHAIN,
]
WOS_MAKE_JOB_HELPER_USERS = [
    ROOT / "scripts" / "build" / "build_bash_for_wos.sh",
    WOS_BUSYBOX_BUILD,
    ROOT / "scripts" / "build" / "build_curl_for_wos.sh",
    WOS_DROPBEAR_BUILD,
    ROOT / "scripts" / "build" / "build_git_for_wos.sh",
    ROOT / "scripts" / "build" / "build_make.sh",
    WOS_NASM_BUILD,
    WOS_TLS_BUILD,
    WOS_PYTHON_BUILD,
    ROOT / "scripts" / "build" / "build_zlib_for_wos.sh",
]
WOS_NINJA_JOB_HELPER_USERS = [
    WOS_CLANG_BUILD,
    ROOT / "scripts" / "build" / "build_cmake_for_host.sh",
    WOS_CMAKE_BUILD,
    WOS_MLIBC_BUILD,
    WOS_NINJA_BUILD,
    WOS_TOOLCHAIN,
]
WOS_BUSYBOX_TAR_USERS = [
    ROOT / "scripts" / "build" / "build_bash_for_wos.sh",
    ROOT / "scripts" / "build" / "build_curl_for_wos.sh",
    ROOT / "scripts" / "build" / "build_git_for_wos.sh",
    ROOT / "scripts" / "build" / "build_make.sh",
    WOS_MESON_BUILD,
    WOS_NASM_BUILD,
    WOS_TLS_BUILD,
    ROOT / "scripts" / "build" / "build_zlib_for_wos.sh",
]
WOS_SOURCE_COPY_USERS = [
    ROOT / "scripts" / "build" / "build_bash_for_wos.sh",
    ROOT / "scripts" / "build" / "build_curl_for_wos.sh",
    ROOT / "scripts" / "build" / "build_git_for_wos.sh",
    WOS_MESON_BUILD,
    WOS_TLS_BUILD,
]


def fail(message: str) -> None:
    raise AssertionError(message)


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def compiler_rt_cmake_block(source: str) -> str:
    match = re.search(
        r"build_compiler_rt\(\)\s*\{(?P<body>.*?)\n\}",
        source,
        flags=re.DOTALL,
    )
    if match is None:
        fail("tools/wos-toolchain.sh is missing build_compiler_rt")
    return match.group("body")


def test_compiler_rt_runs_real_cmake_checks_without_forced_response_files() -> None:
    source = WOS_TOOLCHAIN.read_text()
    block = compiler_rt_cmake_block(source)

    require_tokens(
        block,
        [
            'env -u LDFLAGS cmake -G Ninja',
            "-DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY",
            "-DCOMPILER_RT_BUILD_SANITIZERS=$build_sanitizers",
            '"${COMPILER_RT_CMAKE_SYSROOT_ARGS[@]}"',
        ],
        "compiler-rt bootstrap CMake configuration",
    )

    forbidden = [
        "CMAKE_NINJA_FORCE_RESPONSE_FILE",
        "COMPILER_RT_LINK_FLAGS",
        "COMPILER_RT_TARGET_HAS_ATOMICS=ON",
        "COMPILER_RT_TARGET_HAS_FCNTL_LCK=ON",
        "COMPILER_RT_TARGET_HAS_FLOCK=ON",
        "COMPILER_RT_TARGET_HAS_UNAME=ON",
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("compiler-rt bootstrap must not force or preseed CMake checks: " + ", ".join(present))


def test_compiler_rt_sanitizers_are_built_after_mlibc() -> None:
    source = WOS_TOOLCHAIN.read_text()
    require_tokens(
        source,
        [
            "build_compiler_rt OFF",
            'ninja -j"$COMPILER_RT_NINJA_JOBS" && ninja -j"$COMPILER_RT_NINJA_JOBS" install',
            "build_compiler_rt ON",
            'if [ "$require_sanitizers" = "ON" ]; then',
        ],
        "compiler-rt staged build",
    )

    bootstrap_index = source.index("build_compiler_rt OFF")
    mlibc_install_index = source.index("cd $B/mlibc-build")
    sanitizer_index = source.index("build_compiler_rt ON")
    libcxx_index = source.index("# 6. Build libcxx")
    if not bootstrap_index < mlibc_install_index < sanitizer_index < libcxx_index:
        fail("compiler-rt must build builtins/profile before mlibc and ASAN after mlibc")


def test_native_wos_compiler_rt_does_not_link_with_workspace_sysroot() -> None:
    source = WOS_TOOLCHAIN.read_text()
    require_tokens(
        source,
        [
            'COMPILER_RT_CMAKE_SYSROOT_ARGS=("-DCMAKE_SYSROOT=$SYSROOT")',
            'WOS_NINJA_JOBS="$(wos_ninja_jobs)"',
            'COMPILER_RT_NINJA_JOBS="$WOS_NINJA_JOBS"',
            'if [ "$HOST_SYSTEM" = "WOS" ]; then',
            'COMPILER_RT_CMAKE_SYSROOT_ARGS=("-DCMAKE_SYSROOT_COMPILE=$SYSROOT")',
            "unset LDFLAGS\nbuild_compiler_rt OFF",
            "unset LDFLAGS\nbuild_compiler_rt ON",
        ],
        "native WOS compiler-rt sysroot split",
    )


def test_native_wos_build_defaults_keep_target_ports_enabled() -> None:
    source = ROOT_CMAKE.read_text()
    require_tokens(
        source,
        [
            "set(WOS_PORT_BUILD_DEFAULT ON)",
            "set(WOS_BUILD_CMAKE_FOR_HOST_DEFAULT ${WOS_PORT_BUILD_DEFAULT})",
            "if(WOS_NATIVE_SYSTEM_BUILD)\n    set(WOS_BUILD_CMAKE_FOR_HOST_DEFAULT OFF)\nendif()",
            'option(WOS_BUILD_WOSDBG "Build the Qt6-based wosdbg host tool" ${WOS_BUILD_WOSDBG_DEFAULT})',
            'option(WOS_BUILD_CLANG_FOR_WOS "Build a native WOS clang/lld toolchain and stage it into the rootfs" ${WOS_PORT_BUILD_DEFAULT})',
            'option(WOS_BUILD_CMAKE_FOR_HOST "Build host-side CMake from the WOS fork and install it into toolchain/host" ${WOS_BUILD_CMAKE_FOR_HOST_DEFAULT})',
            'option(WOS_BUILD_CMAKE_FOR_WOS "Build native WOS CMake tools and stage them into the rootfs" ${WOS_PORT_BUILD_DEFAULT})',
            'option(WOS_BUILD_GIT_FOR_WOS "Build native WOS Git and stage it into the rootfs" ${WOS_PORT_BUILD_DEFAULT})',
        ],
        "native WOS self-hosting build defaults",
    )

    if "set(WOS_PORT_BUILD_DEFAULT OFF)" in source:
        fail("native WOS builds must keep target port/toolchain targets enabled by default")


def test_wos_build_jobs_helper_has_self_hostable_fallbacks() -> None:
    source = CCACHE_ENV.read_text()
    require_tokens(
        source,
        [
            'export CCACHE_DIR="${TMPDIR:-/tmp}/wos-ccache"',
            "wos_build_jobs() {",
            'local jobs="${WOS_BUILD_JOBS:-}"',
            "WOS_BUILD_JOBS must be a positive integer",
            "command -v nproc",
            "command -v getconf",
            "getconf _NPROCESSORS_ONLN",
            "command -v python3",
            "os.cpu_count() or 1",
            "jobs=1",
            "wos_remove_tree()",
            'local attempts="${WOS_REMOVE_TREE_RETRIES:-5}"',
            "refusing to remove unsafe path",
            "WOS_REMOVE_TREE_RETRIES must be a positive integer",
            "wos_copy_tree_entries_excluding()",
            'for entry in "$source_dir"/* "$source_dir"/.[!.]* "$source_dir"/..?*; do',
            'cp -a "$entry" "$dest_dir/"',
            "wos_dir_has_entries()",
            'for entry in "$dir"/* "$dir"/.[!.]* "$dir"/..?*; do',
            "wos_refresh_file_mtime()",
            'tmp="$file.wos-mtime.$$"',
            "wos_prefetch_meson_subprojects()",
            'WOS_MESON_SUBPROJECT_FETCH_RETRIES',
            'WOS_MESON_SUBPROJECT_FETCH_RETRY_DELAY',
            'wos_fetch_meson_git_subproject "$source_dir" "$subproject"',
            'wos_remove_tree "$source_dir/subprojects/$subproject"',
            "wos_fetch_meson_git_subproject()",
            'url="$(wos_wrap_value "$wrap" url)"',
            'revision="$(wos_wrap_value "$wrap" revision)"',
            'local low_speed_limit="${WOS_GIT_HTTP_LOW_SPEED_LIMIT:-1}"',
            'local low_speed_time="${WOS_GIT_HTTP_LOW_SPEED_TIME:-60}"',
            'GIT_HTTP_LOW_SPEED_LIMIT="$low_speed_limit"',
            'GIT_HTTP_LOW_SPEED_TIME="$low_speed_time"',
            'git -C "$dest" fetch --depth 1 origin "$revision"',
            'wos_copy_tree_entries_excluding "$package_dir" "$dest"',
            "wos_make_jobs() {",
            'local jobs="${WOS_MAKE_JOBS:-}"',
            "WOS_MAKE_JOBS must be a positive integer",
            "wos_make_jobserver_arg() {",
            "--jobserver-style=pipe",
            "wos_make() {",
            'make "$jobserver_arg" -j"$jobs" "$@"',
            "wos_ninja_jobs() {",
            'local jobs="${WOS_NINJA_JOBS:-}"',
            "WOS_NINJA_JOBS must be a positive integer",
            'uname -s 2>/dev/null || printf unknown',
            '= "WOS" ]',
            "wos_build_jobs",
        ],
        "shared WOS build job helper",
    )


def test_mlibc_wrap_dependencies_are_prefetched_with_retries() -> None:
    bootstrap = WOS_TOOLCHAIN.read_text()
    build_script = WOS_MLIBC_BUILD.read_text()
    helper = CCACHE_ENV.read_text()
    require_tokens(
        helper,
        [
            "wos_prefetch_meson_subprojects()",
            "failed to fetch Meson subproject",
            'wos_fetch_meson_git_subproject "$source_dir" "$subproject"',
            'wos_remove_tree "$source_dir/subprojects/$subproject"',
            'GIT_HTTP_LOW_SPEED_LIMIT="$low_speed_limit"',
            'GIT_HTTP_LOW_SPEED_TIME="$low_speed_time"',
            'git -C "$dest" fetch --depth 1 origin "$revision"',
            'wos_copy_tree_entries_excluding "$package_dir" "$dest"',
        ],
        "Meson subproject retry helper",
    )
    require_tokens(
        bootstrap,
        [
            'wos_prefetch_meson_subprojects "$B/src/mlibc" freestnd-c-hdrs freestnd-cxx-hdrs frigg',
            'meson_setup_rerunnable "$B/mlibc-build"',
        ],
        "bootstrap mlibc Meson dependency prefetch",
    )
    require_tokens(
        build_script,
        [
            'wos_prefetch_meson_subprojects "$MLIBC_SRC" freestnd-c-hdrs freestnd-cxx-hdrs frigg',
            'meson setup --prefix="$TARGET_SYSROOT"',
        ],
        "incremental mlibc Meson dependency prefetch",
    )


def test_wos_port_build_scripts_use_shared_job_helper() -> None:
    for script in WOS_JOB_HELPER_USERS:
        source = script.read_text()
        require_tokens(
            source,
            ['WOS_BUILD_JOBS="$(wos_build_jobs)"'],
            f"{script.relative_to(ROOT)} shared WOS build job helper",
        )
        forbidden = ["$(nproc)", "`nproc`"]
        present = [token for token in forbidden if token in source]
        if present:
            fail(f"{script.relative_to(ROOT)} must not assume Linux nproc directly: {', '.join(present)}")


def test_gnu_make_port_build_scripts_use_make_job_helper() -> None:
    for script in WOS_MAKE_JOB_HELPER_USERS:
        source = script.read_text()
        require_tokens(
            source,
            [
                'WOS_MAKE_JOBS="$(wos_make_jobs)"',
                'wos_make "$WOS_MAKE_JOBS"',
            ],
            f"{script.relative_to(ROOT)} shared GNU Make job helper",
        )
        if re.search(r"(?:host_env\s+)?make\b[^\n]*-j\"\$WOS_BUILD_JOBS\"", source):
            fail(f"{script.relative_to(ROOT)} must use WOS_MAKE_JOBS for GNU Make parallelism")
        if re.search(r"\bmake\b(?:(?!WOS_MAKE_JOBSERVER_ARG).)*-j\"\$WOS_MAKE_JOBS\"", source):
            fail(f"{script.relative_to(ROOT)} must use wos_make or the WOS GNU Make jobserver arg")
        if "host_env make" in source:
            require_tokens(
                source,
                [
                    'WOS_MAKE_JOBSERVER_ARG="$(wos_make_jobserver_arg "$WOS_MAKE_JOBS")"',
                    'host_env make ${WOS_MAKE_JOBSERVER_ARG:+"$WOS_MAKE_JOBSERVER_ARG"}',
                ],
                f"{script.relative_to(ROOT)} host GNU Make jobserver compatibility",
            )


def test_ninja_port_build_scripts_use_ninja_job_helper() -> None:
    for script in WOS_NINJA_JOB_HELPER_USERS:
        source = script.read_text()
        require_tokens(
            source,
            ['WOS_NINJA_JOBS="$(wos_ninja_jobs)"'],
            f"{script.relative_to(ROOT)} shared Ninja job helper",
        )
        forbidden = [
            'ninja -j"$WOS_BUILD_JOBS"',
            'ninja -C "$MLIBC_BUILD" -j"$WOS_BUILD_JOBS"',
            '--parallel "$WOS_BUILD_JOBS"',
        ]
        present = [token for token in forbidden if token in source]
        if present:
            fail(f"{script.relative_to(ROOT)} must use WOS_NINJA_JOBS for Ninja/CMake parallelism")

    bootstrap = WOS_TOOLCHAIN.read_text()
    require_tokens(
        bootstrap,
        ['ninja -j"$WOS_NINJA_JOBS" install-cxx-headers install-cxxabi-headers'],
        "bootstrap libc++ header install parallelism",
    )
    if "ninja install-cxx-headers install-cxxabi-headers" in bootstrap:
        fail("bootstrap libc++ header install must use WOS_NINJA_JOBS")


def test_wos_tar_invocations_use_busybox_compatible_long_options() -> None:
    for script in WOS_BUSYBOX_TAR_USERS:
        source = script.read_text()
        forbidden = ["--strip-components=", "--exclude=", "--exclude ", "-print -quit"]
        present = [token for token in forbidden if token in source]
        if present:
            fail(
                f"{script.relative_to(ROOT)} must avoid BusyBox tar long-option forms "
                f"that are fragile in WOS wrapper copy/extract paths: {', '.join(present)}"
            )


def test_wos_source_copy_wrappers_avoid_busybox_tar_pipelines() -> None:
    for script in WOS_SOURCE_COPY_USERS:
        source = script.read_text()
        require_tokens(
            source,
            [
                "wos_remove_tree",
                "wos_copy_tree_entries_excluding",
            ],
            f"{script.relative_to(ROOT)} source copy helper",
        )
        forbidden = ["tar --exclude", "tar \\\n", "cp -a \"$source_dir/.\""]
        present = [token for token in forbidden if token in source]
        if present:
            fail(
                f"{script.relative_to(ROOT)} must avoid tar-copy pipelines and whole-tree "
                f"copy/remove patterns in WOS source staging: {', '.join(present)}"
            )


def test_gnu_make_script_passes_build_triplet_on_wos() -> None:
    source = (ROOT / "scripts" / "build" / "build_make.sh").read_text()
    require_tokens(
        source,
        [
            'TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"',
            'HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"',
            'GNU_MAKE_CONFIGURE_BUILD_ARGS=()',
            'if [ "$HOST_SYSTEM" = "WOS" ]; then',
            'GNU_MAKE_CONFIGURE_BUILD_ARGS=(--build="$TARGET_ARCH")',
            '"${GNU_MAKE_CONFIGURE_BUILD_ARGS[@]}"',
            '--host="$TARGET_ARCH"',
        ],
        "GNU make WOS configure build triplet",
    )


def test_gnu_make_script_handles_native_wos_autoconf_probes() -> None:
    source = (ROOT / "scripts" / "build" / "build_make.sh").read_text()
    require_tokens(
        source,
        [
            "rewrite_file_for_mtime()",
            "refresh_make_release_generated_files()",
            "refresh_make_build_generated_files()",
            "sleep 1",
            'if [ "$HOST_SYSTEM" = "WOS" ]; then\n    refresh_make_release_generated_files "$MAKE_SOURCE_DIR"\nfi',
            'if [ "$HOST_SYSTEM" = "WOS" ]; then\n    refresh_make_build_generated_files "$MAKE_BUILD"\nfi',
            'if [ "$HOST_SYSTEM" = "WOS" ] && { [ ! -x "$MAKE_BUILD/make" ] || [ ! -f "$MAKE_BUILD/config.status" ]; }; then',
            'rm -f "$MAKE_BUILD/Makefile"',
            '[ ! -f "$MAKE_BUILD/config.status" ]',
            "GNU_MAKE_CONFIGURE_CACHE_ARGS=()",
            "ac_cv_path_GREP=/usr/bin/grep",
            '"ac_cv_path_EGREP=/usr/bin/grep -E"',
            '"ac_cv_path_FGREP=/usr/bin/grep -F"',
            '"${GNU_MAKE_CONFIGURE_CACHE_ARGS[@]}"',
            "GNU_MAKE_BUILD_ARGS=()",
            "GNU_MAKE_BUILD_ARGS=(MAKEINFO=true)",
            'wos_make "$WOS_MAKE_JOBS" -C "$MAKE_BUILD" "${GNU_MAKE_BUILD_ARGS[@]}"',
        ],
        "GNU make native WOS Autoconf probe handling",
    )


def test_nasm_script_uses_release_tarball_without_self_hosted_autogen() -> None:
    source = WOS_NASM_BUILD.read_text()
    require_tokens(
        source,
        [
            'NASM_VERSION="${WOS_NASM_VERSION:-3.02rc9}"',
            "https://www.nasm.us/pub/nasm/releasebuilds/$NASM_VERSION/nasm-$NASM_VERSION.tar.xz",
            "1802d091f4b2c1b3f61ab9d9fca323b8da7674c1ced7d5b770e77604d9c7925b",
            "download_nasm_source()",
            "resolve_nasm_source()",
            "seed_nasm_generated_files()",
            'local fallback_src="$B/src/nasm-$NASM_VERSION"',
            'tar -xJf "$archive" -C "$tmp_dest" --strip-components 1',
            'NASM_SOURCE_DIR="$(resolve_nasm_source)"',
            'require_file "$NASM_SOURCE_DIR/configure"',
            'patch_config_sub_for_wos "$NASM_SOURCE_DIR/autoconf/helpers/config.sub"',
            'HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"',
            'if [ "$HOST_SYSTEM" = "WOS" ]; then',
            'BUILD_TRIPLE="$TARGET_ARCH"',
            'BUILD_TRIPLE="$("$NASM_SOURCE_DIR/autoconf/helpers/config.guess")"',
            '[ "$(cat "$NASM_BUILD/source.path")" != "$NASM_SOURCE_DIR" ]',
            'wos_remove_tree "$NASM_BUILD"',
            'printf \'%s\\n\' "$NASM_SOURCE_DIR" > "$NASM_BUILD/source.path"',
            '"$NASM_SOURCE_DIR/configure" \\',
            "x86/insns.xda",
            "asm/directiv.h",
            'cp -p "$NASM_SOURCE_DIR/$file" "$NASM_BUILD/$file"',
            'wos_refresh_file_mtime "$NASM_BUILD/$file"',
            "sleep 1",
            "editors/nasmtok.json",
            'export CPPFLAGS=""',
            'grep -F -- "-I$TARGET_SYSROOT/include" "$NASM_BUILD/Makefile"',
            'rm -f "$NASM_BUILD/Makefile" "$NASM_BUILD/config.status"',
            "seed_nasm_generated_files",
        ],
        "NASM WOS release source fallback",
    )

    forbidden = ["./autogen.sh", "Generating NASM configure script", 'CPPFLAGS="-I$TARGET_SYSROOT/include"']
    present = [token for token in forbidden if token in source]
    if present:
        fail("NASM WOS build must not require self-hosted Automake/autogen: " + ", ".join(present))


def test_cpython_script_uses_target_build_triplet_on_native_wos() -> None:
    source = WOS_PYTHON_BUILD.read_text()
    require_tokens(
        source,
        [
            'TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"',
            'HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"',
            'patch_config_sub_for_wos "$PYTHON_SRC/config.sub"',
            'if [ "$HOST_SYSTEM" = "WOS" ]; then',
            'BUILD_TRIPLE="$TARGET_ARCH"',
            'BUILD_TRIPLE="$("$PYTHON_SRC/config.guess")"',
            'PYTHON_HOST_CONFIGURE_BUILD_ARGS=()',
            'PYTHON_CONFIGURE_CACHE_ARGS=()',
            'PYTHON_HOST_CONFIGURE_BUILD_ARGS=(--build="$BUILD_TRIPLE")',
            "ac_cv_path_GREP=/usr/bin/grep",
            '"ac_cv_path_EGREP=/usr/bin/grep -E"',
            '"ac_cv_path_FGREP=/usr/bin/grep -F"',
            '"${PYTHON_CONFIGURE_CACHE_ARGS[@]}"',
            '"${PYTHON_HOST_CONFIGURE_BUILD_ARGS[@]}"',
            "--disable-ipv6",
            'grep -Fq -- "-isystem $HOST/lib/clang/22/include" "$makefile"',
            'grep -Fq -- "-isystem $TARGET_SYSROOT/include" "$makefile"',
            'PYTHON_CPPFLAGS="-isystem $HOST/lib/clang/22/include -isystem $TARGET_SYSROOT/include"',
            '--build="$BUILD_TRIPLE"',
            '--host="$TARGET_ARCH"',
        ],
        "CPython native WOS build triplet handling",
    )


def test_cpython_install_avoids_sysroot_usr_symlink() -> None:
    source = WOS_PYTHON_BUILD.read_text()
    require_tokens(
        source,
        [
            "--prefix=/usr",
            "--exec-prefix=/usr",
            "prefix= \\",
            "exec_prefix= \\",
            'DESTDIR="$TARGET_SYSROOT"',
            'require_file "$TARGET_SYSROOT/bin/python3"',
        ],
        "CPython native WOS install path",
    )


def test_native_wos_bootstrap_keeps_host_toolchain_shim_discoverable() -> None:
    source = BOOTSTRAP.read_text()
    require_tokens(
        source,
        [
            'local compat_root="$WORKSPACE_ROOT/toolchain/host"',
            'WOS_HOST_TOOLCHAIN_ROOT="$WORKSPACE_ROOT/toolchain/wos-host-shim"',
            'ln -sfn "$(basename "$shim_root")" host',
            'elif [ ! -x "$compat_root/bin/clang" ]; then',
            "llvm-tblgen clang-tblgen",
            "Host toolchain compatibility path",
        ],
        "native WOS bootstrap host-toolchain compatibility shim",
    )


def test_wos_toolchain_uses_shared_busybox_and_dropbear_build_scripts() -> None:
    source = WOS_TOOLCHAIN.read_text()
    require_tokens(
        source,
        [
            'WOS_HOST_TOOLCHAIN_ROOT="$HOST" \\',
            'WOS_BUSYBOX_BUILD_DIR="$B/busybox-build" \\',
            'WOS_BUSYBOX_INSTALL_DIR="$B/busybox-install" \\',
            '"$B/../scripts/build/build_busybox.sh"',
            'WOS_DROPBEAR_BUILD_DIR="$B/dropbear-build" \\',
            '"$B/../scripts/build/build_dropbear.sh"',
        ],
        "WOS target toolchain BusyBox/Dropbear script delegation",
    )

    forbidden = [
        'HOSTCC="${WOS_CCACHE_PREFIX}/usr/bin/clang"',
        "cp $B/busybox-build/busybox $SYSROOT/bin/busybox",
        "autoconf && autoheader",
        "cp $B/dropbear-build/dropbearmulti $SYSROOT/bin/dropbearmulti",
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("WOS target toolchain must not keep stale inline BusyBox/Dropbear build logic: " + ", ".join(present))


def test_native_wos_clang_port_stages_tablegen_for_next_self_host() -> None:
    source = WOS_CLANG_BUILD.read_text()
    require_tokens(
        source,
        [
            'HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"',
            "llvm-tblgen \\",
            "clang-tblgen",
            "install_tool llvm-tblgen",
            "install_tool clang-tblgen",
            '-DLLVM_TABLEGEN="$HOST/bin/llvm-tblgen"',
            '-DCLANG_TABLEGEN="$HOST/bin/clang-tblgen"',
        ],
        "native WOS clang tablegen staging",
    )


def test_busybox_and_dropbear_scripts_honor_host_toolchain_override() -> None:
    busybox = WOS_BUSYBOX_BUILD.read_text()
    dropbear = WOS_DROPBEAR_BUILD.read_text()
    require_tokens(
        busybox,
        [
            'export CCACHE_DIR="${TMPDIR:-/tmp}/wos-busybox-ccache"',
            'B="$WORKSPACE_ROOT/toolchain"',
            'HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"',
            "native_host_path() {",
            "find_native_host_tool() {",
            'BB_NATIVE_HOSTCC="$(find_native_host_tool cc || true)"',
            'BB_HOSTCC="${WOS_CCACHE_PREFIX}$BB_NATIVE_HOSTCC"',
            'BB_INSTALL="${WOS_BUSYBOX_INSTALL_DIR:-$B/busybox-install}"',
        ],
        "BusyBox WOS build script host-toolchain override",
    )
    require_tokens(
        busybox,
        [
            'wos_make "$WOS_MAKE_JOBS" -C "$BB_BUILD" \\',
            'install >/tmp/busybox_install.log 2>&1',
        ],
        "BusyBox WOS install uses GNU Make jobserver policy",
    )
    require_tokens(
        dropbear,
        [
            'export CCACHE_DIR="${TMPDIR:-/tmp}/wos-dropbear-ccache"',
            'B="$WORKSPACE_ROOT/toolchain"',
            'HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"',
            'TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"',
            'DB_BUILD="${WOS_DROPBEAR_BUILD_DIR:-$B/dropbear-build}"',
            'DROPBEAR_CONFIGURE_BUILD_ARGS=()',
            'uname -s 2>/dev/null || printf unknown',
            'DROPBEAR_CONFIGURE_BUILD_ARGS=(--build="$TARGET_ARCH")',
            '"${DROPBEAR_CONFIGURE_BUILD_ARGS[@]}"',
            '--host="$TARGET_ARCH"',
        ],
        "Dropbear WOS build script host-toolchain override",
    )


def test_mlibc_script_honors_host_toolchain_override_and_job_helper() -> None:
    source = WOS_MLIBC_BUILD.read_text()
    require_tokens(
        source,
        [
            'B="$WORKSPACE_ROOT/toolchain"',
            'HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"',
            'WOS_BUILD_JOBS="$(wos_build_jobs)"',
            'WOS_NINJA_JOBS="$(wos_ninja_jobs)"',
            'ninja -C "$MLIBC_BUILD" -j"$WOS_NINJA_JOBS"',
            'ninja -C "$MLIBC_BUILD" -j"$WOS_NINJA_JOBS" install',
        ],
        "mlibc WOS build script host-toolchain and job helper",
    )


def test_ninja_script_uses_target_header_stack() -> None:
    source = WOS_NINJA_BUILD.read_text()
    require_tokens(
        source,
        [
            'HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"',
            'TARGET_CXX_FLAGS="--sysroot=$TARGET_SYSROOT',
            "-isystem $TARGET_SYSROOT/include/c++/v1",
            "-isystem $HOST/lib/clang/22/include",
            "-isystem $TARGET_SYSROOT/include",
            'cached_cxx="$(sed -n \'s/^CMAKE_CXX_COMPILER:[^=]*=//p\' "$NINJA_BUILD/CMakeCache.txt")"',
            'if [ -n "$cached_cxx" ] && [ "$cached_cxx" != "$HOST/bin/clang++" ]; then',
            'rm -rf "$NINJA_BUILD"',
        ],
        "Ninja WOS build script target header stack",
    )


def test_cmake_script_uses_target_header_stack() -> None:
    source = WOS_CMAKE_BUILD.read_text()
    require_tokens(
        source,
        [
            'HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"',
            'TARGET_C_INCLUDE_FLAGS="-isystem $HOST/lib/clang/22/include -isystem $TARGET_SYSROOT/include"',
            'TARGET_C_FLAGS="$TARGET_COMMON_FLAGS $TARGET_C_INCLUDE_FLAGS"',
            'TARGET_CXX_FLAGS="$TARGET_COMMON_FLAGS -std=c++23 -isystem $TARGET_SYSROOT/include/c++/v1 $TARGET_C_INCLUDE_FLAGS"',
            'cached_cxx="$(sed -n \'s/^CMAKE_CXX_COMPILER:[^=]*=//p\' "$CMAKE_BUILD/CMakeCache.txt")"',
            'cached_cxx_flags="$(sed -n \'s/^CMAKE_CXX_FLAGS:[^=]*=//p\' "$CMAKE_BUILD/CMakeCache.txt")"',
            'if [ -n "$cached_cxx" ] && [ "$cached_cxx" != "$HOST/bin/clang++" ]; then',
            '*" -isystem $TARGET_SYSROOT/include "*)',
            'if [ -d "$CMAKE_BUILD/CMakeFiles" ] && [ ! -f "$CMAKE_BUILD/CMakeCache.txt" ]; then',
            'wos_remove_tree "$CMAKE_BUILD"',
            '-DCMAKE_INSTALL_PREFIX="$TARGET_SYSROOT"',
            'cmake --install "$CMAKE_BUILD" --prefix "$TARGET_SYSROOT"',
        ],
        "CMake WOS build script target header stack",
    )


def test_wos_tls_build_is_self_hostable_without_perl() -> None:
    source = WOS_TLS_BUILD.read_text()
    require_tokens(
        source,
        [
            "LIBRESSL_VERSION=\"${WOS_LIBRESSL_VERSION:-4.3.2}\"",
            "https://ftp.openbsd.org/pub/OpenBSD/LibreSSL/libressl-$LIBRESSL_VERSION.tar.gz",
            "edf01aee24c65d69e6a9efcb9d44bcda682ff9d4f3bbbd95e794e1dfa90847b5",
            "download_libressl_source",
            "resolve_tls_source",
            "patch_config_sub_for_wos \"$TLS_WORK/config.sub\"",
            "patch_arc4random_for_wos \"$TLS_WORK/crypto/compat/arc4random.h\"",
            "refresh_libressl_release_generated_files",
            "sed -i 's/| fiwix\\* | mlibc\\* )/| fiwix* | mlibc* | wos* )/'",
            "defined(__WOS__)",
            "#include \"arc4random_netbsd.h\"",
            "aclocal.m4",
            "apps/openssl/Makefile.in",
            "sleep 2",
            'wos_refresh_file_mtime "$TLS_WORK/$file"',
            "-D__WOS__=1",
            'HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"',
            "TLS_CONFIGURE_BUILD_ARGS=()",
            "TLS_CONFIGURE_CACHE_ARGS=()",
            'TLS_CONFIGURE_BUILD_ARGS=(--build="$TARGET_ARCH")',
            "ac_cv_path_GREP=/usr/bin/grep",
            '"ac_cv_path_EGREP=/usr/bin/grep -E"',
            '"ac_cv_path_FGREP=/usr/bin/grep -F"',
            '"${TLS_CONFIGURE_CACHE_ARGS[@]}"',
            '"${TLS_CONFIGURE_BUILD_ARGS[@]}"',
            "remove_cross_libtool_archives",
            "$TARGET_SYSROOT/lib/libssl.la",
            "./configure \\",
            "--host=\"$TARGET_ARCH\"",
            "--prefix=",
            "--libdir=/lib",
            "--disable-shared",
            "--enable-static",
            "--disable-tests",
            "--disable-asm",
            "wos_make \"$WOS_MAKE_JOBS\" -C \"$TLS_WORK\"",
            "prefix= \\",
            "exec_prefix= \\",
            "includedir=/include \\",
            "pkgconfigdir=/lib/pkgconfig \\",
            "DESTDIR=\"$TARGET_SYSROOT\" \\",
            "require_file \"$TARGET_SYSROOT/lib/libssl.a\"",
            "require_file \"$TARGET_SYSROOT/lib/libcrypto.a\"",
            "require_file \"$TARGET_SYSROOT/include/openssl/ssl.h\"",
        ],
        "WOS self-hosted TLS build path",
    )

    forbidden = [
        "command -v perl",
        "perl Configure",
        "OPENSSL_TARBALL_URL",
        "OPENSSL_TARBALL_SHA256",
        "--prefix=/usr",
        "--libdir=/usr/lib",
        "DESTDIR=\"$TARGET_SYSROOT\" install",
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("WOS TLS build path must avoid Perl/OpenSSL Configure dependency: " + ", ".join(present))


def test_zlib_install_avoids_sysroot_usr_symlink() -> None:
    source = (ROOT / "scripts" / "build" / "build_zlib_for_wos.sh").read_text()
    require_tokens(
        source,
        [
            '"$ZLIB_SOURCE_DIR/configure" --prefix= --static',
            "prefix= \\",
            "exec_prefix= \\",
            "libdir=/lib \\",
            "includedir=/include \\",
            "pkgconfigdir=/lib/pkgconfig \\",
            'DESTDIR="$TARGET_SYSROOT" \\',
            "install",
        ],
        "zlib WOS install path avoids sysroot /usr symlink",
    )

    forbidden = ['"$ZLIB_SOURCE_DIR/configure" --prefix=/usr --static', 'DESTDIR="$TARGET_SYSROOT" install']
    present = [token for token in forbidden if token in source]
    if present:
        fail("zlib WOS install must avoid installing through the sysroot /usr symlink: " + ", ".join(present))


def test_wos_curl_build_uses_native_triplet_and_real_sysroot_install() -> None:
    source = WOS_CURL_BUILD.read_text()
    require_tokens(
        source,
        [
            'HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"',
            "CURL_CONFIGURE_BUILD_ARGS=()",
            "CURL_CONFIGURE_CACHE_ARGS=()",
            'CURL_CONFIGURE_BUILD_ARGS=(--build="$TARGET_ARCH")',
            "ac_cv_path_GREP=/usr/bin/grep",
            '"ac_cv_path_EGREP=/usr/bin/grep -E"',
            '"ac_cv_path_FGREP=/usr/bin/grep -F"',
            '"${CURL_CONFIGURE_CACHE_ARGS[@]}"',
            '"${CURL_CONFIGURE_BUILD_ARGS[@]}"',
            '--host="$TARGET_ARCH"',
            "--prefix=",
            "--bindir=/bin",
            "--libdir=/lib",
            "--includedir=/include",
            "prefix= \\",
            "exec_prefix= \\",
            "bindir=/bin \\",
            "libdir=/lib \\",
            "includedir=/include \\",
            'DESTDIR="$TARGET_SYSROOT" \\',
            'require_file "$TARGET_SYSROOT/bin/curl"',
            'require_file "$TARGET_SYSROOT/lib/libcurl.a"',
        ],
        "curl native WOS build triplet and install path",
    )

    forbidden = ["--prefix=/usr", 'DESTDIR="$TARGET_SYSROOT" install']
    present = [token for token in forbidden if token in source]
    if present:
        fail("curl WOS install must avoid installing through the sysroot /usr symlink: " + ", ".join(present))


def test_wos_git_install_avoids_sysroot_usr_symlink() -> None:
    source = WOS_GIT_BUILD.read_text()
    require_tokens(
        source,
        [
            '"prefix="',
            '"bindir=/bin"',
            '"gitexecdir=/libexec/git-core"',
            '"template_dir=/share/git-core/templates"',
            '"sysconfdir=/etc"',
            'GIT_CFLAGS="--sysroot=$TARGET_SYSROOT -O2 -g -fPIC -fPIE -fno-sanitize=safe-stack -fno-stack-protector -I. -Icompat/regex -I$TARGET_SYSROOT/include"',
            '"NO_REGEX=NeedsStartEnd"',
            '"WOS_SKIP_TEST_ARTIFACTS=YesPlease"',
            "all:: $(FUZZ_OBJS)",
            "all:: $(TEST_PROGRAMS) $(test_bindir_programs) $(UNIT_TEST_PROGS) $(CLAR_TEST_PROG)",
            "ifndef WOS_SKIP_TEST_ARTIFACTS",
            "templates_makefile",
            "$(TAR) cf -",
            "cp -a blt/.",
            '"DESTDIR=$TARGET_SYSROOT"',
            'require_file "$TARGET_SYSROOT/bin/git"',
            'require_file "$TARGET_SYSROOT/libexec/git-core/git"',
            'require_file "$TARGET_SYSROOT/share/git-core/templates"',
        ],
        "Git WOS install path avoids sysroot /usr symlink",
    )

    forbidden = [
        '"prefix=/usr"',
        '"bindir=/usr/bin"',
        '"gitexecdir=/usr/libexec/git-core"',
        '"template_dir=/usr/share/git-core/templates"',
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("Git WOS install must avoid installing through the sysroot /usr symlink: " + ", ".join(present))


def test_wos_python_ssl_build_handles_libressl_sigalg_gap() -> None:
    source = WOS_PYTHON_BUILD.read_text()
    require_tokens(
        source,
        [
            "PYTHON_LIBRESSL_SIGALGS_COMPAT_HEADER",
            "write_libressl_sigalgs_compat_header()",
            "LIBRESSL_VERSION_NUMBER",
            "#define SSL_CTX_set1_client_sigalgs_list(ctx, sigalgslist) (0)",
            "#define SSL_CTX_set1_sigalgs_list(ctx, sigalgslist) (0)",
            'PYTHON_CPPFLAGS="$PYTHON_CPPFLAGS -include $PYTHON_LIBRESSL_SIGALGS_COMPAT_HEADER"',
            'CPPFLAGS="$PYTHON_CPPFLAGS"',
        ],
        "CPython _ssl LibreSSL compatibility patch",
    )


def test_mlibc_scanf_float_zero_counts_as_conversion() -> None:
    source = MLIBC_STDIO.read_text()
    require_tokens(
        source,
        [
            "if (c == '0') {\n\t\t\t\t\thandler.consume();\n\t\t\t\t\t++count;",
            "if (c == 'x' || c == 'X') {\n\t\t\t\t\t\tdivisor = 16;\n\t\t\t\t\t\tbase = 16;",
            "\t\t\t\t\t\tcount = 0;",
            "NOMATCH_CHECK(count == 0);",
        ],
        "mlibc scanf floating zero handling",
    )

    tests = MLIBC_SSCANF_TEST.read_text()
    require_tokens(
        tests,
        [
            'assert(sscanf("0", "%lg", &double_value) == 1);',
            'assert(sscanf("-0", "%lg", &double_value) == 1);',
            'assert(sscanf("+0", "%lg", &double_value) == 1);',
            'assert(sscanf("0e0", "%lg", &double_value) == 1);',
        ],
        "mlibc scanf floating zero regression tests",
    )


def test_mlibc_nameser_parser_does_not_panic_on_dns_packets() -> None:
    nameser = MLIBC_NAMESER.read_text()
    require_tokens(
        nameser,
        [
            "int ns_initparse(const unsigned char *msg, int msglen, ns_msg *handle)",
            "int ns_parserr(ns_msg *handle, ns_sect section, int rrnum, ns_rr *rr)",
            "int ns_name_uncompress(",
            "bool has_bytes(const unsigned char *ptr, const unsigned char *eom, size_t count)",
            "handle->_sections[section] = ptr;",
            "if ((label & NS_CMPRSFLGS) == NS_CMPRSFLGS)",
            "return fail_parse();",
        ],
        "mlibc DNS nameser parser",
    )
    if '__ensure(!"Not implemented")' in nameser:
        fail("mlibc nameser parser must reject malformed DNS packets instead of panicking")

    resolv = MLIBC_RESOLV.read_text()
    require_tokens(
        resolv,
        [
            "int dn_expand(",
            "return ns_name_uncompress(msg, eomorig, comp_dn, exp_dn, static_cast<size_t>(length));",
        ],
        "mlibc dn_expand DNS decompression wrapper",
    )


if __name__ == "__main__":
    test_compiler_rt_runs_real_cmake_checks_without_forced_response_files()
    test_compiler_rt_sanitizers_are_built_after_mlibc()
    test_native_wos_compiler_rt_does_not_link_with_workspace_sysroot()
    test_native_wos_build_defaults_keep_target_ports_enabled()
    test_wos_build_jobs_helper_has_self_hostable_fallbacks()
    test_wos_port_build_scripts_use_shared_job_helper()
    test_gnu_make_port_build_scripts_use_make_job_helper()
    test_ninja_port_build_scripts_use_ninja_job_helper()
    test_wos_tar_invocations_use_busybox_compatible_long_options()
    test_wos_source_copy_wrappers_avoid_busybox_tar_pipelines()
    test_gnu_make_script_passes_build_triplet_on_wos()
    test_gnu_make_script_handles_native_wos_autoconf_probes()
    test_nasm_script_uses_release_tarball_without_self_hosted_autogen()
    test_cpython_script_uses_target_build_triplet_on_native_wos()
    test_cpython_install_avoids_sysroot_usr_symlink()
    test_native_wos_bootstrap_keeps_host_toolchain_shim_discoverable()
    test_wos_toolchain_uses_shared_busybox_and_dropbear_build_scripts()
    test_native_wos_clang_port_stages_tablegen_for_next_self_host()
    test_busybox_and_dropbear_scripts_honor_host_toolchain_override()
    test_mlibc_script_honors_host_toolchain_override_and_job_helper()
    test_ninja_script_uses_target_header_stack()
    test_cmake_script_uses_target_header_stack()
    test_zlib_install_avoids_sysroot_usr_symlink()
    test_wos_tls_build_is_self_hostable_without_perl()
    test_wos_python_ssl_build_handles_libressl_sigalg_gap()
    test_mlibc_scanf_float_zero_counts_as_conversion()
    test_mlibc_nameser_parser_does_not_panic_on_dns_packets()
    print("WOS toolchain bootstrap source invariants hold")
