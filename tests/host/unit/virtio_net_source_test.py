#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
VIRTIO_NET_CPP = ROOT / "modules" / "kern" / "src" / "dev" / "virtio" / "virtio_net.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    start = source.find(f"auto {name}(")
    if start < 0:
        fail(f"{name} function not found")
    brace = source.find("{", start)
    if brace < 0:
        fail(f"{name} function body not found")
    depth = 0
    for pos in range(brace, len(source)):
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
            if depth == 0:
                return source[brace + 1 : pos]
    fail(f"{name} function body is unterminated")


def require_order(source: str, *snippets: str) -> None:
    cursor = -1
    for snippet in snippets:
        pos = source.find(snippet, cursor + 1)
        if pos < 0:
            fail(f"missing ordered snippet: {snippet}")
        cursor = pos


def test_ctrl_ack_deadline_is_saturating() -> None:
    source = VIRTIO_NET_CPP.read_text()
    helper_body = function_body(source, "future_deadline_ms")
    send_body = function_body(source, "send_mq_ctrl_cmd")

    require_order(
        helper_body,
        "uint64_t const NOW_MS = ker::mod::time::get_ms()",
        "if (UINT64_MAX - NOW_MS < timeout_ms)",
        "return UINT64_MAX",
        "return NOW_MS + timeout_ms",
    )
    require_order(
        send_body,
        "constexpr uint64_t CTRL_ACK_TIMEOUT_MS = 5000",
        "uint64_t const DEADLINE_MS = future_deadline_ms(CTRL_ACK_TIMEOUT_MS)",
        "while (ker::mod::time::get_ms() < DEADLINE_MS)",
        "__atomic_thread_fence(__ATOMIC_ACQUIRE)",
        'net_log::warn("ctrl-queue MQ ack timeout")',
    )

    if "ker::mod::time::get_ms() + CTRL_ACK_TIMEOUT_MS" in send_body:
        fail("virtio-net control ACK wait must not use wrapping deadline arithmetic")


def main() -> None:
    test_ctrl_ack_deadline_is_saturating()
    print("virtio-net control ACK deadline uses saturating arithmetic")


if __name__ == "__main__":
    main()
