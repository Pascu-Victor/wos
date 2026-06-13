#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TOP_CPP = ROOT / "modules" / "top" / "src" / "main.cpp"


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


def require_order(source: str, first: str, second: str, context: str) -> None:
    first_pos = source.find(first)
    second_pos = source.find(second)
    if first_pos < 0 or second_pos < 0 or first_pos >= second_pos:
        fail(f"{context}: expected {first!r} before {second!r}")


def test_top_noninteractive_mode_is_one_shot_and_plain() -> None:
    source = TOP_CPP.read_text()
    require_tokens(
        source,
        [
            "bool const INTERACTIVE = isatty(STDIN_FILENO) && isatty(STDOUT_FILENO);",
            "TerminalMode const terminal_mode(INTERACTIVE)",
            "render(snap, previous.has_value() ? &*previous : nullptr, row_offset, col_offset, INTERACTIVE)",
            "if (!INTERACTIVE)",
        ],
        "top noninteractive mode",
    )

    require_tokens(
        source,
        [
            "explicit TerminalMode(bool interactive)",
            "if (!interactive)",
            "soft_clear_visible_screen()",
        ],
        "top TerminalMode interactive gate",
    )

    render_body = function_body(source, "render")
    if "auto render(const Snapshot& snap, const Snapshot* previous, int row_offset, int col_offset, bool interactive) -> std::string" not in source:
        fail("top render must accept an explicit interactive flag")
    require_tokens(
        render_body,
        [
            "if (interactive)",
            'out += "\\r\\x1b[H\\x1b[2J\\x1b[H\\x1b[?25l"',
        ],
        "top render interactive gate",
    )

    main_body = function_body(source, "main")
    require_order(main_body, "if (!INTERACTIVE)", "int remaining_ms = INTERVAL_MS;", "top one-shot exit before refresh wait")
    if "poll(&pfd, 1, WAIT_MS)" not in main_body:
        fail("top interactive mode should still poll for keyboard input")


def test_top_input_shutdown_paths_quit_refresh_loop() -> None:
    source = TOP_CPP.read_text()
    require_tokens(
        source,
        [
            "INPUT_CLOSED",
            "POLLHUP | POLLERR | POLLNVAL",
            "if (READY < 0)",
            "if (KEY == Key::QUIT || KEY == Key::INPUT_CLOSED)",
        ],
        "top input shutdown handling",
    )

    read_key_body = function_body(source, "read_key")
    require_tokens(
        read_key_body,
        [
            "if (N == 0)",
            "return Key::INPUT_CLOSED",
            "errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK",
        ],
        "top read_key EOF/error handling",
    )

    main_body = function_body(source, "main")
    require_tokens(
        main_body,
        [
            "if (READY < 0)",
            "if (errno == EINTR)",
            "quit = true",
            "if (READY > 0 && (pfd.revents & (POLLHUP | POLLERR | POLLNVAL)) != 0)",
        ],
        "top poll shutdown handling",
    )


def main() -> None:
    test_top_noninteractive_mode_is_one_shot_and_plain()
    test_top_input_shutdown_paths_quit_refresh_loop()
    print("top noninteractive and input shutdown liveness checks passed")


if __name__ == "__main__":
    main()
