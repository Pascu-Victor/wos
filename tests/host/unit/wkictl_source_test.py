#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WKICTL_MAIN = ROOT / "modules" / "wkictl" / "src" / "main.cpp"
ALIASES = ROOT / "configs" / "rootfs" / "aliases.tsv"
PROCESS_HEADER = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "include" / "sys" / "process.h"
VFS_HEADER = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "include" / "sys" / "vfs.h"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void|int|bool)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
        source,
        flags=re.DOTALL,
    )
    if match is None:
        fail(f"missing function {name}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[match.end() : pos - 1]


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def alias_targets() -> dict[str, tuple[str, str]]:
    targets: dict[str, tuple[str, str]] = {}
    for line in ALIASES.read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        action, source, target = parts[:3]
        targets[target] = (action, source)
    return targets


def test_wkictl_installed_aliases_match_persona_dispatch() -> None:
    source = WKICTL_MAIN.read_text()
    aliases = alias_targets()
    expected = {
        "/usr/bin/wkictl": ("copy", "build/modules/wkictl/wkictl"),
        "/usr/bin/locally": ("symlink", "/usr/bin/wkictl"),
        "/usr/bin/remotely": ("symlink", "/usr/bin/wkictl"),
        "/usr/bin/homeward": ("symlink", "/usr/bin/wkictl"),
        "/usr/bin/on": ("symlink", "/usr/bin/wkictl"),
        "/usr/bin/forward": ("symlink", "/usr/bin/wkictl"),
        "/usr/bin/wosid": ("symlink", "/usr/bin/wkictl"),
    }
    for target, wanted in expected.items():
        if aliases.get(target) != wanted:
            fail(f"rootfs alias mismatch for {target}: got {aliases.get(target)!r}, expected {wanted!r}")

    main_body = function_body(source, "main")
    require_tokens(
        main_body,
        [
            "command_basename(argc > 0 ? argv[0] : \"wkictl\")",
            'std::strcmp(name, "locally") == 0',
            'std::strcmp(name, "remotely") == 0',
            'std::strcmp(name, "homeward") == 0',
            'std::strcmp(name, "on") == 0',
            'std::strcmp(name, "forward") == 0',
            'std::strcmp(name, "wosid") == 0',
            "return run_wkictl(argc, argv)",
        ],
        "wkictl basename dispatch",
    )


def test_wkictl_target_personas_set_expected_policy() -> None:
    source = WKICTL_MAIN.read_text()
    run_locally = function_body(source, "run_locally")
    require_tokens(
        run_locally,
        [
            "ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL)",
            "return exec_command(argv + 1)",
        ],
        "locally persona",
    )

    run_remotely = function_body(source, "run_remotely")
    require_tokens(
        run_remotely,
        [
            "ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE)",
            "return exec_command(argv + 1)",
        ],
        "remotely persona",
    )

    run_on = function_body(source, "run_on")
    require_tokens(
        run_on,
        [
            "ker::process::setwkitarget(hostname, std::strlen(hostname), ker::process::WKI_TARGET_FLAG_STRICT)",
            "return exec_command(argv + 2)",
        ],
        "on persona",
    )

    run_homeward = function_body(source, "run_homeward")
    require_tokens(
        run_homeward,
        [
            "ker::process::wki_launcher_node(launcher.data(), launcher.size())",
            "ker::process::setwkitarget(launcher.data(), static_cast<uint64_t>(LAUNCHER_LEN), ker::process::WKI_TARGET_FLAG_STRICT)",
            "return exec_command(argv + 1)",
        ],
        "homeward persona",
    )

    flags_body = function_body(source, "parse_policy_flags")
    require_tokens(
        flags_body,
        [
            'std::strcmp(argv[i], "strict") == 0',
            'std::strcmp(argv[i], "fallback") == 0 || std::strcmp(argv[i], "best-effort") == 0',
            'std::strcmp(argv[i], "noinherit") == 0',
            "ker::process::WKI_TARGET_FLAG_NOINHERIT",
        ],
        "target policy flag parser",
    )


def test_wkictl_vfs_forward_and_commands_use_wki_wrappers() -> None:
    source = WKICTL_MAIN.read_text()
    forward_body = function_body(source, "run_forward")
    require_tokens(
        forward_body,
        [
            'std::strcmp(arg, "--") == 0',
            "arg[0] == '+' ? ker::abi::vfs::WKI_VFS_ROUTE_HOST : ker::abi::vfs::WKI_VFS_ROUTE_LOCAL",
            "add_forward_operand(arg + 1, ROUTE)",
            "return exec_command(argv + command_index)",
        ],
        "forward persona parser",
    )

    add_operand = function_body(source, "add_forward_operand")
    require_tokens(
        add_operand,
        [
            "has_glob_meta(operand)",
            "glob(operand, 0, nullptr, &matches)",
            "add_forward_rule(matches.gl_pathv[i], route)",
        ],
        "forward glob expansion",
    )

    add_rule = function_body(source, "add_forward_rule")
    require_tokens(
        add_rule,
        [
            "ker::abi::vfs::wki_rule_add_vfs(path, route)",
            "route_name(route)",
        ],
        "forward VFS rule add",
    )

    handle_vfs = function_body(source, "handle_vfs")
    require_tokens(
        handle_vfs,
        [
            'std::strcmp(argv[2], "list") == 0',
            'std::strcmp(argv[2], "defaults") == 0',
            'std::strcmp(argv[2], "clear") == 0',
            "ker::abi::vfs::wki_rule_clear_vfs()",
            'std::strcmp(argv[2], "add") == 0',
            "ker::abi::vfs::wki_rule_add_vfs(argv[3], route)",
            'std::strcmp(argv[2], "probe") == 0',
        ],
        "wkictl vfs command handling",
    )


def test_wkictl_headers_expose_matching_wki_wrappers() -> None:
    process = PROCESS_HEADER.read_text()
    vfs = VFS_HEADER.read_text()
    require_tokens(
        process,
        [
            "constexpr uint32_t WKI_TARGET_FLAG_STRICT = 1U << 0",
            "constexpr uint32_t WKI_TARGET_FLAG_LOCAL = 1U << 1",
            "constexpr uint32_t WKI_TARGET_FLAG_NOINHERIT = 1U << 2",
            "constexpr uint32_t WKI_TARGET_FLAG_REMOTE = 1U << 3",
            "inline int64_t setwkitarget",
            "inline int64_t getwkitarget",
        ],
        "process WKI target wrapper header",
    )
    require_tokens(
        vfs,
        [
            "constexpr uint32_t WKI_VFS_ROUTE_LOCAL = 0",
            "constexpr uint32_t WKI_VFS_ROUTE_HOST = 1",
            "static inline int wki_rule_add_vfs",
            "wki_rule_get_vfs(uint32_t index, char *prefix_buf, size_t prefix_buf_size, uint32_t *route_out)",
            "static inline int wki_rule_clear_vfs",
            "static inline int wki_rule_get_default_vfs",
        ],
        "VFS WKI route wrapper header",
    )


def main() -> None:
    test_wkictl_installed_aliases_match_persona_dispatch()
    test_wkictl_target_personas_set_expected_policy()
    test_wkictl_vfs_forward_and_commands_use_wki_wrappers()
    test_wkictl_headers_expose_matching_wki_wrappers()
    print("wkictl alias, persona, and WKI wrapper source checks passed")


if __name__ == "__main__":
    main()
