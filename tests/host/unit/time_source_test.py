#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TIME_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "time" / "time.cpp"


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


def case_body(source: str, case_label: str) -> str:
    start = source.find(case_label)
    if start < 0:
        fail(f"{case_label} case not found")
    next_case = source.find("case ker::abi::sys_time_ops::", start + len(case_label))
    default_case = source.find("default:", start + len(case_label))
    ends = [pos for pos in [next_case, default_case] if pos >= 0]
    end = min(ends) if ends else len(source)
    return source[start:end]


def require_order(source: str, *snippets: str) -> None:
    cursor = -1
    for snippet in snippets:
        pos = source.find(snippet, cursor + 1)
        if pos < 0:
            fail(f"missing ordered snippet: {snippet}")
        cursor = pos


def test_setitimer_timevals_are_checked_and_saturating() -> None:
    source = TIME_CPP.read_text()
    helper_body = function_body(source, "relative_timeval_to_us")
    setitimer_body = case_body(source, "case ker::abi::sys_time_ops::SETITIMER:")

    require_order(
        helper_body,
        "out_us = 0",
        "tv.tv_sec < 0 || tv.tv_usec < 0",
        "auto const USEC = static_cast<uint64_t>(tv.tv_usec)",
        "if (USEC >= USEC_PER_SEC)",
        "auto const SEC = static_cast<uint64_t>(tv.tv_sec)",
        "if (SEC > (UINT64_MAX - USEC) / USEC_PER_SEC)",
        "out_us = (SEC * USEC_PER_SEC) + USEC",
    )
    require_order(
        setitimer_body,
        "uint64_t new_val_us = 0",
        "uint64_t new_interval_us = 0",
        "relative_timeval_to_us(nv->it_value, new_val_us)",
        "relative_timeval_to_us(nv->it_interval, new_interval_us)",
        "return static_cast<uint64_t>(-EINVAL)",
        "if (new_val_us == 0)",
        "task->itimer_real_expire_us = deadline_from_now_us(new_val_us)",
        "task->itimer_real_interval_us = new_interval_us",
        "ker::mod::sched::request_local_timer_recheck()",
    )

    forbidden = [
        "task->itimer_real_expire_us = ker::mod::time::get_us() + NEW_VAL_US",
        "static_cast<uint64_t>(nv->it_value.tv_sec) * 1000000ULL",
        "static_cast<uint64_t>(nv->it_interval.tv_sec) * 1000000ULL",
    ]
    present = [snippet for snippet in forbidden if snippet in setitimer_body]
    if present:
        fail(f"SETITIMER still has wrapping timeval/deadline arithmetic: {present[0]}")


def main() -> None:
    test_setitimer_timevals_are_checked_and_saturating()
    print("time syscall setitimer conversion and deadline are overflow guarded")


if __name__ == "__main__":
    main()
