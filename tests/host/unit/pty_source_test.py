#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PTY_CPP = ROOT / "modules" / "kern" / "src" / "dev" / "pty.cpp"
DEVICE_HPP = ROOT / "modules" / "kern" / "src" / "dev" / "device.hpp"
FILE_OPERATIONS_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "file_operations.hpp"
DEVFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "devfs.cpp"
SYS_NET_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "net" / "sys_net.cpp"
EPOLL_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "epoll.cpp"
VFS_CORE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"(?:auto|void|int|ssize_t)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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
    first_index = source.find(first)
    second_index = source.find(second)
    if first_index < 0:
        fail(f"{context}: missing {first}")
    if second_index < 0:
        fail(f"{context}: missing {second}")
    if first_index >= second_index:
        fail(f"{context}: expected {first!r} before {second!r}")


def test_detached_pty_device_is_not_logged_as_pointer_corruption() -> None:
    source = PTY_CPP.read_text()
    body = function_body(source, "pair_from_device")

    require_tokens(
        body,
        [
            "auto* pair = static_cast<PtyPair*>(device->private_data);",
            "if (pair == nullptr)",
            "return nullptr;",
            "if (!is_valid_kernel_pointer(pair))",
            '"pty_%s: invalid pair pointer %p from device %p"',
        ],
        "PTY pair lookup should distinguish detach from corruption",
    )
    require_order(body, "if (pair == nullptr)", "if (!is_valid_kernel_pointer(pair))", "null detach before corruption warning")


def test_pty_pair_detaches_only_after_both_sides_close() -> None:
    source = PTY_CPP.read_text()
    master_close = function_body(source, "master_close")
    slave_close = function_body(source, "slave_close")

    require_tokens(
        master_close,
        [
            "if (pair->master_opened > 0)",
            "pair->master_opened--;",
            "if (pair->master_opened <= 0 && pair->slave_opened <= 0 && !pair->freeing)",
            "pty_detach_devices(pair);",
        ],
        "PTY master close must keep pair attached while any endpoint remains open",
    )
    require_tokens(
        slave_close,
        [
            "if (pair->slave_opened > 0)",
            "pair->slave_opened--;",
            "if (pair->master_opened <= 0 && pair->slave_opened <= 0 && !pair->freeing)",
            "pty_detach_devices(pair);",
        ],
        "PTY slave close must keep pair attached while any endpoint remains open",
    )

    if "if (pair->slave_opened <= 0 && !pair->freeing)" in master_close:
        fail("PTY master close must not detach solely because the slave side is closed")
    if "if (pair->master_opened <= 0 && !pair->freeing)" in slave_close:
        fail("PTY slave close must not detach solely because the master side is closed")


def test_pty_waiter_wakes_preserve_deferred_switch_state() -> None:
    source = PTY_CPP.read_text()
    body = function_body(source, "wake_waiters")
    block_body = function_body(source, "block_current_task")
    master_read_body = function_body(source, "master_read")
    master_write_body = function_body(source, "master_write")
    slave_read_body = function_body(source, "slave_read")
    slave_write_body = function_body(source, "slave_write")

    require_tokens(
        body,
        [
            "auto* waiter = ker::mod::sched::find_task_by_pid_safe(WAITER_PID);",
            "ker::mod::sched::wake_task_from_event(waiter);",
            "waiter->release();",
        ],
        "PTY waiter wakes should use the central event-wake default",
    )
    if "EventWakeDeferredSwitch::CANCEL" in body:
        fail("PTY waiter wakes must not cancel unrelated deferred switch state")

    require_tokens(
        source,
        [
            "ker::mod::sched::task::WaitChannelKind wait_kind = ker::mod::sched::task::WaitChannelKind::GENERIC",
        ],
        "PTY blocker should accept a typed wait channel",
    )
    require_tokens(
        block_body,
        [
            "current_task->set_wait_channel(wchan, wait_kind);",
        ],
        "PTY blocker should publish a typed wait channel",
    )
    for context, wait_body in [
        ("master read", master_read_body),
        ("master write", master_write_body),
        ("slave read", slave_read_body),
        ("slave write", slave_write_body),
    ]:
        if "WaitChannelKind::LOCAL_PTY" not in wait_body:
            fail(f"PTY {context} path should park with LOCAL_PTY wait-channel kind")


def test_pty_poll_waits_publish_local_pty_wait_kind() -> None:
    pty_source = PTY_CPP.read_text()
    device_header = DEVICE_HPP.read_text()
    file_ops_header = FILE_OPERATIONS_HPP.read_text()
    devfs_source = DEVFS_CPP.read_text()
    sys_net_source = SYS_NET_CPP.read_text()
    epoll_source = EPOLL_CPP.read_text()
    vfs_core_source = VFS_CORE_CPP.read_text()

    require_tokens(
        file_ops_header,
        [
            "using vfs_poll_wait_kind_fn = mod::sched::task::WaitChannelKind (*)(struct File*);",
            "vfs_poll_wait_kind_fn vfs_poll_wait_kind = nullptr;",
        ],
        "VFS poll wait kind operation",
    )
    require_tokens(
        device_header,
        [
            "mod::sched::task::WaitChannelKind (*poll_wait_kind)(ker::vfs::File* file) = nullptr;",
        ],
        "character-device poll wait kind operation",
    )
    require_tokens(
        devfs_source,
        [
            "auto devfs_poll_wait_kind(File* f) -> ker::mod::sched::task::WaitChannelKind",
            "devfs_file->device->char_ops->poll_wait_kind(f)",
            ".vfs_poll_wait_kind = devfs_poll_wait_kind,",
        ],
        "devfs should forward char-device poll wait kinds",
    )
    require_tokens(
        pty_source,
        [
            "auto pty_poll_wait_kind(ker::vfs::File* /*file*/) -> WaitChannelKind",
            "return WaitChannelKind::LOCAL_PTY;",
            ".poll_wait_kind = pty_poll_wait_kind,",
        ],
        "PTY poll wait kind",
    )
    require_tokens(
        sys_net_source,
        [
            "auto poll_wait_kind_for_file(ker::vfs::File* file) -> ker::mod::sched::task::WaitChannelKind",
            "auto merge_poll_wait_kind(ker::mod::sched::task::WaitChannelKind current, ker::mod::sched::task::WaitChannelKind candidate)",
            "poll_wait_kind = merge_poll_wait_kind(poll_wait_kind, poll_wait_kind_for_file(file));",
            'ker::mod::sched::preemptible_syscall_park("poll", poll_wait_kind, DEADLINE_US);',
        ],
        "poll syscall should park with registered file wait kind",
    )
    require_tokens(
        epoll_source,
        [
            "auto poll_wait_kind_for_file(File* file) -> ker::mod::sched::task::WaitChannelKind",
            "auto merge_poll_wait_kind(ker::mod::sched::task::WaitChannelKind current, ker::mod::sched::task::WaitChannelKind candidate)",
            "poll_wait_kind = merge_poll_wait_kind(poll_wait_kind, poll_wait_kind_for_file(f));",
            'ker::mod::sched::preemptible_syscall_park("epoll_wait", poll_wait_kind, DEADLINE_US);',
        ],
        "epoll wait should park with registered file wait kind",
    )
    require_tokens(
        vfs_core_source,
        [
            ".vfs_poll_wait_kind = [](File*) -> ker::mod::sched::task::WaitChannelKind",
            "return ker::mod::sched::task::WaitChannelKind::LOCAL_PIPE;",
        ],
        "local pipe poll wait kind",
    )


def main() -> None:
    test_detached_pty_device_is_not_logged_as_pointer_corruption()
    test_pty_pair_detaches_only_after_both_sides_close()
    test_pty_waiter_wakes_preserve_deferred_switch_state()
    test_pty_poll_waits_publish_local_pty_wait_kind()
    print("PTY source invariants hold")


if __name__ == "__main__":
    main()
