#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEV_PROXY_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "dev_proxy.cpp"
DEV_PROXY_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "dev_proxy.hpp"
WKI_DEV_PROXY_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_dev_proxy_ktest.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{", source)
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


def require_order(body: str, before: str, after: str, context: str) -> None:
    before_pos = body.find(before)
    after_pos = body.find(after)
    if before_pos < 0 or after_pos < 0 or before_pos >= after_pos:
        fail(f"{context}: expected {before!r} before {after!r}")


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def test_lifecycle_flags_are_atomic() -> None:
    header = DEV_PROXY_HPP.read_text()
    required = [
        "std::atomic<bool> active{false}",
        "std::atomic<bool> fenced{false}",
    ]
    missing = [token for token in required if token not in header]
    if missing:
        fail("ProxyBlockState lifecycle flags must be atomic: " + ", ".join(missing))


def test_lifecycle_helpers_use_acquire_release() -> None:
    source = DEV_PROXY_CPP.read_text()
    required = [
        ("proxy_block_active", "state->active.load(std::memory_order_acquire)"),
        ("proxy_block_fenced", "state->fenced.load(std::memory_order_acquire)"),
        ("set_proxy_block_active", "state->active.store(value, std::memory_order_release)"),
        ("set_proxy_block_fenced", "state->fenced.store(value, std::memory_order_release)"),
    ]
    for function, token in required:
        if token not in function_body(source, function):
            fail(f"{function} must contain {token!r}")


def test_lifecycle_flags_are_only_accessed_through_helpers() -> None:
    allowed = [
        "state->active.load(std::memory_order_acquire)",
        "state->fenced.load(std::memory_order_acquire)",
        "state->active.store(value, std::memory_order_release)",
        "state->fenced.store(value, std::memory_order_release)",
    ]
    offenders = []
    for line_no, line in enumerate(DEV_PROXY_CPP.read_text().splitlines(), start=1):
        if "->active" not in line and "->fenced" not in line:
            continue
        if any(token in line for token in allowed):
            continue
        offenders.append(f"{line_no}: {line.strip()}")
    if offenders:
        fail("dev_proxy.cpp must not access active/fenced outside lifecycle helpers: " + "; ".join(offenders))


def test_proxy_op_slot_wait_is_bounded() -> None:
    source = DEV_PROXY_CPP.read_text()
    require_tokens(
        source,
        [
            "#include <net/wki/timer_math.hpp>",
            "constexpr uint64_t DEV_PROXY_SLOT_WAIT_TIMEOUT_US = WKI_DEV_PROXY_TIMEOUT_US;",
            "auto acquire_block_op_slot_locked(ProxyBlockState* state, uint64_t start_us) -> int",
        ],
        "dev proxy slot timeout scaffolding",
    )

    acquire_body = function_body(source, "acquire_block_op_slot_locked")
    require_tokens(
        acquire_body,
        [
            "uint64_t const DEADLINE_US = wki_future_deadline_us(start_us, DEV_PROXY_SLOT_WAIT_TIMEOUT_US)",
            "state->lock.lock()",
            "if (!block_proxy_op_slot_busy(state))",
            "return WKI_OK",
            "state->lock.unlock()",
            "if (!block_proxy_op_slot_busy(state))",
            "continue",
            "if (wki_now_us() >= DEADLINE_US)",
            "return WKI_ERR_TIMEOUT",
            "ker::mod::sched::kern_sleep_us(DEV_PROXY_CONTENTION_SLEEP_US)",
        ],
        "dev proxy slot acquisition",
    )
    require_order(acquire_body, "state->lock.lock()", "return WKI_OK", "slot lock success path")
    require_order(acquire_body, "state->lock.unlock()", "return WKI_ERR_TIMEOUT", "slot timeout unlock path")

    prepare_body = function_body(source, "prepare_block_op_wait")
    require_order(
        prepare_body,
        "int const SLOT_RET = acquire_block_op_slot_locked(state, wki_now_us())",
        "state->op_wait_entry = &wait",
        "prepare wait publishes only after slot acquisition",
    )
    require_order(
        prepare_body,
        "if (SLOT_RET != WKI_OK)",
        "return SLOT_RET",
        "prepare wait propagates slot timeout",
    )
    if "ker::mod::sched::kern_sleep_us(DEV_PROXY_CONTENTION_SLEEP_US)" in prepare_body:
        fail("prepare_block_op_wait must not contain its own unbounded contention sleep loop")


def test_rdmaring_wait_deadlines_are_saturating() -> None:
    source = DEV_PROXY_CPP.read_text()
    require_tokens(
        source,
        [
            "constexpr uint64_t DEV_PROXY_BULK_WAIT_TIMEOUT_US = wki_saturating_mul_us(WKI_DEV_PROXY_TIMEOUT_US, 4);",
            "wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US)",
            "wki_future_deadline_us(wki_now_us(), DEV_PROXY_BULK_WAIT_TIMEOUT_US)",
        ],
        "dev proxy RDMA deadline helper use",
    )

    forbidden = [
        "wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US",
        "wki_now_us() + (WKI_DEV_PROXY_TIMEOUT_US * 4)",
        "WKI_DEV_PROXY_TIMEOUT_US * 4",
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("dev proxy waits must not use wrapping deadline arithmetic: " + ", ".join(present))

    for function in [
        "remote_block_read_rdma",
        "remote_block_write_rdma",
        "remote_block_flush_rdma",
        "rdma_batch_collect",
        "remote_block_bulk_read_rdma",
        "remote_block_bulk_write_rdma",
    ]:
        body = function_body(source, function)
        if "uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us()," not in body:
            fail(f"{function} must build wait deadlines with wki_future_deadline_us")

    attach_body = function_body(source, "wki_dev_proxy_attach_block")
    for token in [
        "uint64_t const ZONE_DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US)",
        "uint64_t const RKEY_DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US)",
        "uint64_t const READY_DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US)",
    ]:
        if token not in attach_body:
            fail(f"wki_dev_proxy_attach_block is missing saturating deadline: {token}")


def test_fence_wait_uses_ordered_lifecycle_helpers() -> None:
    body = function_body(DEV_PROXY_CPP.read_text(), "wait_for_fence_lift")
    required = [
        "while (proxy_block_fenced(state))",
        "if (!proxy_block_active(state))",
        "return proxy_block_active(state)",
        "ker::mod::sched::kern_yield()",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("fence wait must use ordered lifecycle helpers and yield: " + ", ".join(missing))
    forbidden = ["state->active", "state->fenced"]
    present = [token for token in forbidden if token in body]
    if present:
        fail("fence wait still has raw lifecycle access: " + ", ".join(present))


def test_timeout_marks_inactive_with_release_store() -> None:
    body = function_body(DEV_PROXY_CPP.read_text(), "wki_dev_proxy_fence_timeout_tick")
    if "set_proxy_block_active(p.get(), false)" not in body:
        fail("fence timeout must release-store active=false before unregistering")
    if "mfence" in body:
        fail("fence timeout must not rely on an x86 mfence instead of atomic ordering")
    require_order(body, "set_proxy_block_active(p.get(), false)", "entry.state = p.get()", "timeout teardown publication")


def test_dev_proxy_selftest_declared() -> None:
    ktest = WKI_DEV_PROXY_KTEST.read_text()
    required = [
        "KTEST(WkiDevProxyFenceFlags, LifecycleFlagsAreAtomic)",
        "KTEST(WkiDevProxyAttachAck, CookieFencesStaleBlockCompletion)",
        "std::is_same_v<decltype(std::declval<State&>().active), std::atomic<bool>>",
        "std::is_same_v<decltype(std::declval<State&>().fenced), std::atomic<bool>>",
        "wki_dev_proxy_selftest_attach_ack_cookie_fences_stale_completion()",
    ]
    missing = [token for token in required if token not in ktest]
    if missing:
        fail("missing dev proxy lifecycle KTEST coverage: " + ", ".join(missing))


def test_attach_ack_requires_expected_cookie_before_completion() -> None:
    header = DEV_PROXY_HPP.read_text()
    source = DEV_PROXY_CPP.read_text()
    require_tokens(
        header,
        [
            "uint8_t attach_expected_cookie = 0;",
            "auto wki_dev_proxy_selftest_attach_ack_cookie_fences_stale_completion() -> bool;",
        ],
        "dev proxy attach-cookie state",
    )
    require_tokens(
        source,
        [
            "uint8_t g_block_attach_next_cookie = 1;",
            "auto allocate_block_attach_cookie_locked() -> uint8_t",
            "auto block_attach_ack_matches_pending_locked(ProxyBlockState const* state, const DevAttachAckPayload& ack) -> bool",
            "state->attach_expected_cookie != 0",
            "wki_dev_attach_ack_matches_expected(state->attach_expected_cookie, ack)",
            "attach_req.attach_cookie = ATTACH_COOKIE",
        ],
        "dev proxy attach-cookie scaffolding",
    )

    attach_body = function_body(source, "wki_dev_proxy_attach_block")
    require_order(
        attach_body,
        "attach_req.attach_cookie = ATTACH_COOKIE",
        "state->attach_expected_cookie = ATTACH_COOKIE",
        "block attach publishes cookie before send",
    )
    require_order(
        attach_body,
        "state->attach_expected_cookie = ATTACH_COOKIE",
        "wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ",
        "block attach sends only after expected cookie is armed",
    )

    resume_body = function_body(source, "wki_dev_proxy_resume_for_peer")
    require_order(
        resume_body,
        "attach_req.attach_cookie = ATTACH_COOKIE",
        "p->attach_expected_cookie = ATTACH_COOKIE",
        "block resume publishes cookie before send",
    )
    require_order(
        resume_body,
        "p->attach_expected_cookie = ATTACH_COOKIE",
        "wki_send(node_id, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ",
        "block resume sends only after expected cookie is armed",
    )

    ack_body = function_body(source, "handle_dev_attach_ack")
    require_order(
        ack_body,
        "if (!block_attach_ack_matches_pending_locked(state, *ack))",
        "state->attach_status = ack->status",
        "block attach ACK validates cookie before publishing status",
    )
    require_order(
        ack_body,
        "state->attach_expected_cookie = 0",
        "state->attach_pending.store(false, std::memory_order_release)",
        "block attach ACK clears cookie before completing",
    )


def main() -> None:
    test_lifecycle_flags_are_atomic()
    test_lifecycle_helpers_use_acquire_release()
    test_lifecycle_flags_are_only_accessed_through_helpers()
    test_proxy_op_slot_wait_is_bounded()
    test_rdmaring_wait_deadlines_are_saturating()
    test_fence_wait_uses_ordered_lifecycle_helpers()
    test_timeout_marks_inactive_with_release_store()
    test_dev_proxy_selftest_declared()
    test_attach_ack_requires_expected_cookie_before_completion()
    print("WKI dev proxy source invariants hold")


if __name__ == "__main__":
    main()
