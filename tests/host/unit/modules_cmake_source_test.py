#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
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


def main() -> None:
    test_native_repo_sysroot_has_explicit_c_include_path()
    test_compile_options_keep_libcxx_before_c_headers()
    print("Module CMake sysroot include invariants hold")


if __name__ == "__main__":
    main()
