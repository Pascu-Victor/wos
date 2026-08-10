#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
KERNEL_ABI = ROOT / "modules/kern/src/abi/init_control.hpp"
USER_ABI = ROOT / "toolchain/src/mlibc/sysdeps/wos/include/sys/init_control.h"
KERNEL_PROCESS_ABI = ROOT / "modules/kern/src/abi/callnums/process.h"
USER_PROCESS_ABI = ROOT / "toolchain/src/mlibc/sysdeps/wos/include/callnums/process.h"
BROKER = ROOT / "modules/kern/src/syscalls_impl/process/init_control.cpp"
PROCESS = ROOT / "modules/kern/src/syscalls_impl/process/process.cpp"
USER_PROCESS = ROOT / "toolchain/src/mlibc/sysdeps/wos/include/sys/process.h"
SERVICectl = ROOT / "modules/servicectl/src/main.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def require(source: str, token: str, context: str) -> None:
    if token not in source:
        fail(f"{context}: missing {token}")


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\bauto\s+{name}\([^)]*\)\s*->\s*uint64_t\s*\{{", source)
    if match is None:
        fail(f"missing function {name}")
    depth = 1
    position = match.end()
    while position < len(source) and depth != 0:
        if source[position] == "{":
            depth += 1
        elif source[position] == "}":
            depth -= 1
        position += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[match.end() : position - 1]


def test_fixed_abi_is_mirrored_and_append_only() -> None:
    kernel_abi = KERNEL_ABI.read_text()
    user_abi = USER_ABI.read_text()
    if re.sub(r"\s+", "", kernel_abi) != re.sub(r"\s+", "", user_abi):
        fail("kernel and userspace init-control ABI headers drifted")
    abi = kernel_abi
    for token in [
        "constexpr uint32_t ABI_VERSION = 1;",
        "constexpr uint32_t MAILBOX_CAPACITY = 16;",
        "constexpr uint32_t MAX_STATUS_SERVICES = 16;",
        "constexpr uint32_t MAX_TRANSITION_HISTORY = 4;",
        "struct Request",
        "struct Transition",
        "struct ServiceStatus",
        "struct StatusSnapshot",
        "static_assert(sizeof(Request) == 128);",
        "static_assert(sizeof(Transition) == 24);",
        "static_assert(sizeof(ServiceStatus) == 256);",
        "static_assert(sizeof(StatusSnapshot) == 4160);",
    ]:
        require(abi, token, "fixed init-control ABI")

    for path in [KERNEL_PROCESS_ABI, USER_PROCESS_ABI]:
        process_abi = path.read_text()
        require(process_abi, "SPAWN", f"{path} legacy selector")
        for name, value in [
            ("INIT_CONTROL_SUBMIT", 44),
            ("INIT_CONTROL_RECEIVE", 45),
            ("INIT_STATUS_PUBLISH", 46),
            ("INIT_STATUS_READ", 47),
        ]:
            if re.search(rf"\b{name}\s*=\s*{value}\b", process_abi) is None:
                fail(f"{path}: {name} must remain selector {value}")
        if process_abi.find("SPAWN") > process_abi.find("INIT_CONTROL_SUBMIT"):
            fail(f"{path}: init-control selectors must be appended after SPAWN")


def test_broker_authenticates_and_keeps_usercopy_outside_lock() -> None:
    source = BROKER.read_text()
    submit = function_body(source, "wos_proc_init_control_submit")
    receive = function_body(source, "wos_proc_init_control_receive")
    publish = function_body(source, "wos_proc_init_status_publish")
    read = function_body(source, "wos_proc_init_status_read")

    require(submit, "task->euid != 0", "root-only request submission")
    require(submit, "request.sender_pid = ker::mod::sched::task::process_pid(*task);", "authenticated sender PID")
    require(submit, "request.sender_euid = task->euid;", "authenticated sender euid")
    require(receive, "!is_init_process(*task)", "PID 1 request receive")
    require(publish, "!is_init_process(*task)", "PID 1 status publish")
    require(source, "std::array<init_abi::Request, init_abi::MAILBOX_CAPACITY>", "fixed mailbox")
    require(source, "return static_cast<uint64_t>(-EAGAIN);", "nonblocking empty/full result")
    require(receive, "if (COPIED)", "copyout failure preserves request")
    require(source, "status.services[index] = {};", "unused status slot sanitization")
    require(source, "service.history_count > init_abi::MAX_TRANSITION_HISTORY", "bounded transition history")
    require(source, "transition_reason_valid(transition.reason)", "transition reason validation")
    require(source, "transition.timestamp_mono_ns < previous_timestamp", "ordered transition history")
    require(source, "service.history[history_index] = {};", "unused transition sanitization")

    for body, context in [
        (submit, "submit"),
        (receive, "receive"),
        (publish, "publish"),
        (read, "read"),
    ]:
        cursor = 0
        while True:
            lock = body.find("g_init_control_lock.lock();", cursor)
            if lock < 0:
                break
            unlock = body.find("g_init_control_lock.unlock();", lock)
            if unlock < 0:
                fail(f"{context}: unmatched init-control lock")
            critical = body[lock:unlock]
            for forbidden in ["usercopy::", "log::", "new ", "ker::vfs", "ker::net", "kern_yield"]:
                if forbidden in critical:
                    fail(f"{context}: forbidden {forbidden!r} under init-control spinlock")
            cursor = unlock + 1

    require(receive, "copy_value_to_task(*task, request_addr, request)", "receive copyout")
    require(read, "copy_value_to_task(*task, status_addr, status)", "status copyout")


def test_dispatch_wrappers_and_servicectl_are_integrated() -> None:
    dispatch = PROCESS.read_text()
    wrappers = USER_PROCESS.read_text()
    utility = SERVICectl.read_text()
    modules_cmake = (ROOT / "modules/CMakeLists.txt").read_text()
    root_cmake = (ROOT / "CMakeLists.txt").read_text()
    aliases = (ROOT / "configs/rootfs/aliases.tsv").read_text()
    meson = (ROOT / "toolchain/src/mlibc/sysdeps/wos/meson.build").read_text()

    for name in ["INIT_CONTROL_SUBMIT", "INIT_CONTROL_RECEIVE", "INIT_STATUS_PUBLISH", "INIT_STATUS_READ"]:
        require(dispatch, f"procmgmt_ops::{name}", "kernel process dispatch")
    for name in ["init_control_submit", "init_control_receive", "init_status_publish", "init_status_read"]:
        require(wrappers, f"inline int64_t {name}", "raw userspace wrappers")

    require(meson, "'include/sys/init_control.h'", "installed userspace ABI")
    for command in ['"status"', '"start"', '"stop"', '"restart"']:
        require(utility, command, "servicectl command surface")
    require(utility, "ker::process::init_control_submit(&request)", "servicectl authenticated submission")
    require(utility, "ker::process::init_status_read(&status)", "servicectl status read")
    require(utility, "print_history(service)", "servicectl targeted transition history")
    require(modules_cmake, "add_subdirectory(servicectl)", "servicectl module")
    require(modules_cmake, "powerctl servicectl renderbench", "servicectl aggregate target")
    require(root_cmake, "$<TARGET_FILE:servicectl>", "servicectl rootfs artifact dependency")
    require(aliases, "build/modules/servicectl/servicectl\t/usr/bin/servicectl", "servicectl rootfs alias")


if __name__ == "__main__":
    test_fixed_abi_is_mirrored_and_append_only()
    test_broker_authenticates_and_keeps_usercopy_outside_lock()
    test_dispatch_wrappers_and_servicectl_are_integrated()
    print("init-control ABI, broker, and servicectl invariants hold")
