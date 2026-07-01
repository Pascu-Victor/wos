#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
ROOT_CMAKE = ROOT / "CMakeLists.txt"
MODULES_CMAKE = ROOT / "modules" / "CMakeLists.txt"


def fail(message: str) -> None:
    raise AssertionError(message)


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def test_native_repo_sysroot_has_explicit_c_include_path() -> None:
    source = MODULES_CMAKE.read_text()
    require_order(
        source,
        [
            'if(WOS_NATIVE_SYSTEM_BUILD AND WOS_SYSROOT_PATH STREQUAL "${WOS_NATIVE_INSTALLED_SYSROOT_PATH}")',
            'set(WOS_C_STDLIB_INCLUDE "/usr/include")',
            'set(WOS_CXX_STDLIB_INCLUDE "/usr/include/c++/v1")',
            "else()",
            'set(WOS_C_STDLIB_INCLUDE "${SYSROOT_PATH}/include")',
            'set(WOS_CXX_STDLIB_INCLUDE "${SYSROOT_PATH}/include/c++/v1")',
        ],
        "native/repo sysroot include selection",
    )


def test_compile_options_keep_libcxx_before_c_headers() -> None:
    source = MODULES_CMAKE.read_text()
    require_order(
        source,
        [
            '"$<$<COMPILE_LANGUAGE:C>:-std=gnu11>"',
            '"$<$<COMPILE_LANGUAGE:C>:SHELL:-isystem ${WOS_C_STDLIB_INCLUDE}>"',
            '"$<$<COMPILE_LANGUAGE:CXX>:-std=c++23>"',
            '"$<$<COMPILE_LANGUAGE:CXX>:SHELL:-isystem ${WOS_CXX_STDLIB_INCLUDE}>"',
            '"$<$<COMPILE_LANGUAGE:CXX>:SHELL:-isystem ${WOS_C_STDLIB_INCLUDE}>"',
        ],
        "module compile include options",
    )


def test_root_mlibc_dependencies_follow_userspace_target_list() -> None:
    modules = MODULES_CMAKE.read_text()
    root = ROOT_CMAKE.read_text()

    require_order(
        modules,
        [
            "set(WOS_USERSPACE_TARGETS",
            "debugserver",
            "journal_lib",
            "strace",
            "set(WOS_USERSPACE_TARGETS ${WOS_USERSPACE_TARGETS} PARENT_SCOPE)",
        ],
        "exported userspace target list",
    )
    require_order(
        root,
        [
            "set(WOS_SYSROOT_DEPENDENT_TARGETS wos)",
            "list(APPEND WOS_SYSROOT_DEPENDENT_TARGETS ${WOS_USERSPACE_TARGETS})",
            "foreach(mod IN LISTS WOS_SYSROOT_DEPENDENT_TARGETS)",
            "if(TARGET ${mod})",
            "add_dependencies(${mod} mlibc libcxx)",
        ],
        "root mlibc/libcxx dependency loop",
    )
    if "foreach(mod wos init testprog testd netd httpd perf top memacc journal wkictl powerctl renderbench sftpserver)" in root:
        fail("root CMake still uses the stale hard-coded mlibc/libcxx dependency target list")


def main() -> None:
    test_native_repo_sysroot_has_explicit_c_include_path()
    test_compile_options_keep_libcxx_before_c_headers()
    test_root_mlibc_dependencies_follow_userspace_target_list()
    print("Module CMake sysroot include invariants hold")


if __name__ == "__main__":
    main()
