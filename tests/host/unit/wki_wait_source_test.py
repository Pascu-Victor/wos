#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WKI_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.cpp"
WKI_WAIT_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_wait_ktest.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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


def test_cleanup_publishes_only_when_it_claims_waiter() -> None:
    cleanup_body = function_body(WKI_CPP.read_text(), "wki_wait_cleanup_for_task")
    require_tokens(
        cleanup_body,
        [
            "uint8_t expected = WkiWaitEntry::PENDING",
            "compare_exchange_strong(expected, WkiWaitEntry::CLAIMED, std::memory_order_acq_rel, std::memory_order_acquire)",
            "cur->task.store(nullptr, std::memory_order_release)",
            "publish_claimed_wait_entry(cur, WKI_ERR_PEER_FENCED)",
        ],
        "WKI task cleanup claim/publish path",
    )
    if "cur->state.store(WkiWaitEntry::DONE" in cleanup_body:
        fail("wki_wait_cleanup_for_task must not force DONE after losing the waiter claim")
    if "cur->result =" in cleanup_body:
        fail("wki_wait_cleanup_for_task must publish results only through publish_claimed_wait_entry")


def test_ktest_covers_claimed_cleanup_race() -> None:
    source = WKI_WAIT_KTEST.read_text()
    require_tokens(
        source,
        [
            "TaskCleanupDoesNotPublishAlreadyClaimedWaiter",
            "wki_claim_op(&wait)",
            "wki_wait_cleanup_for_task(&task)",
            "KEXPECT_NULL(wait.task.load(std::memory_order_acquire))",
            "WkiWaitEntry::CLAIMED",
            "wki_finish_claimed_op(&wait, 77)",
        ],
        "WKI wait cleanup claimed-race KTEST",
    )


def main() -> None:
    test_cleanup_publishes_only_when_it_claims_waiter()
    test_ktest_covers_claimed_cleanup_race()
    print("WKI wait cleanup source invariants hold")


if __name__ == "__main__":
    main()
