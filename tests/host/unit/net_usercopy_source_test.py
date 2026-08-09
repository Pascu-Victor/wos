#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
NET_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "net" / "sys_net.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def find_matching_brace(source: str, brace: int) -> int:
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    fail("unterminated braced block")


def function_body(source: str, name: str) -> str:
    start = source.find(f"auto {name}(")
    if start < 0:
        fail(f"{name} function not found")
    brace = source.find("{", start)
    return source[brace + 1 : find_matching_brace(source, brace)]


def case_body(source: str, name: str) -> str:
    marker = f"case ker::abi::net::ops::{name}: {{"
    start = source.find(marker)
    if start < 0:
        fail(f"network syscall case {name} not found")
    brace = source.find("{", start)
    return source[brace + 1 : find_matching_brace(source, brace)]


def require_bounded_socket_io(source: str) -> None:
    send = function_body(source, "socket_send_user_bounced")
    for snippet in [
        "usercopy::range_valid(user_addr, count)",
        "std::min(count, SOCKET_IO_BOUNCE_MAX_CHUNK)",
        "usercopy::copy_from_task(*task, user_addr, bounce, TO_COPY)",
        "clamp_io_count",
    ]:
        if snippet not in send:
            fail(f"socket send must use a bounded kernel bounce buffer: {snippet}")

    recv = function_body(source, "socket_recv_user_bounced")
    for snippet in [
        "usercopy::range_valid(user_addr, count)",
        "std::min(count, SOCKET_IO_BOUNCE_MAX_CHUNK)",
        "usercopy::ensure_writable(*task, user_addr, TO_READ)",
        "usercopy::copy_to_task_partial(*task, user_addr, bounce, BYTES_READ)",
        "OUTPUT.bytes_copied != 0",
    ]:
        if snippet not in recv:
            fail(f"socket receive must use bounded partial-progress usercopy: {snippet}")


def require_address_and_option_snapshots(source: str) -> None:
    for name in ["BIND", "CONNECT"]:
        body = case_body(source, name)
        if "ADDR_LEN > SOCKADDR_STORAGE_MAX" not in body or "usercopy::copy_from_task" not in body:
            fail(f"network {name} must bound and snapshot sockaddr input")

    sendto = case_body(source, "SENDTO")
    if "ALEN > addr_storage.size()" not in sendto or "usercopy::copy_from_task(*task, a5, addr_storage.data(), ALEN)" not in sendto:
        fail("network SENDTO must bound and snapshot its domain-sized sockaddr input")

    accept = case_body(source, "ACCEPT")
    preflight = accept.find("usercopy::ensure_writable")
    effect = accept.find("return sock->proto_ops->accept")
    install = accept.find("allocate_socket_fd(new_sock)")
    output = accept.find("usercopy::copy_to_task", effect)
    if min(preflight, effect, output, install) < 0 or not (preflight < effect < output < install):
        fail("accept must preflight outputs, accept, copy results, then publish the fd")

    get_option = case_body(source, "GETSOCKOPT")
    for snippet in [
        "usercopy::copy_value_from_task(*task, a5, option_len)",
        "usercopy::ensure_writable(*task, a5, sizeof(option_len))",
        "usercopy::ensure_writable(*task, a4, option_len)",
        "if (option_len > OPTION_CAPACITY)",
        "return static_cast<uint64_t>(-EOVERFLOW);",
        "usercopy::copy_to_task(*task, a4, OPTION, option_len)",
    ]:
        if snippet not in get_option:
            fail(f"getsockopt must snapshot and bound its in/out record: {snippet}")

    names = function_body(source, "socket_name_to_user")
    for snippet in [
        "addr_out_addr == 0 || addr_len_addr == 0",
        "usercopy::copy_value_from_task(*task, addr_len_addr, user_capacity)",
        "usercopy::ensure_writable(*task, addr_out_addr, SNAPSHOT_LEN)",
        "size_t returned_len = SNAPSHOT_LEN;",
        "std::min({user_capacity, returned_len, address.size()})",
        "usercopy::copy_value_to_task(*task, addr_len_addr, returned_len)",
    ]:
        if snippet not in names:
            fail(f"socket name output must obey its caller capacity: {snippet}")


def require_fixed_abi_records_and_wait_sets(source: str) -> None:
    for assertion in [
        "static_assert(sizeof(WosNetIfInfo) == 52);",
        "static_assert(sizeof(WosNetAddrInfo) == 76);",
        "static_assert(sizeof(WosNetAddrReq) == 48);",
        "static_assert(sizeof(WosNetLinkSetReq) == 68);",
    ]:
        if assertion not in source:
            fail(f"netctl ABI record layout is not pinned: {assertion}")

    for name in ["NETCTL_ADDR_SET", "NETCTL_ADDR_DEL", "NETCTL_LINK_SET"]:
        if "usercopy::copy_value_from_task" not in case_body(source, name):
            fail(f"{name} must snapshot its fixed user record")

    for name in ["NETCTL_IF_LIST", "NETCTL_ADDR_LIST"]:
        body = case_body(source, name)
        for snippet in ["usercopy::copy_value_from_task", "usercopy::ensure_writable", "usercopy::copy_to_task"]:
            if snippet not in body:
                fail(f"{name} must snapshot capacity and bound output: {snippet}")

    select = case_body(source, "SELECT")
    if "a1 > WOS_FD_SETSIZE" not in select or "snapshot_fd_set" not in select or "usercopy::copy_to_task" not in select:
        fail("select must cap, snapshot, and copy back fd sets")

    poll = case_body(source, "POLL")
    for snippet in [
        "NFDS > ker::mod::sched::task::Task::FD_TABLE_SIZE",
        "usercopy::ensure_writable(*task, a1, FDS_BYTES)",
        "usercopy::copy_from_task(*task, a1, fds, FDS_BYTES)",
        "usercopy::copy_to_task(*task, a1, fds, FDS_BYTES)",
    ]:
        if snippet not in poll:
            fail(f"poll must bound and bounce its array: {snippet}")


def main() -> None:
    source = NET_CPP.read_text()
    require_bounded_socket_io(source)
    require_address_and_option_snapshots(source)
    require_fixed_abi_records_and_wait_sets(source)
    print("network syscall usercopy invariants hold")


if __name__ == "__main__":
    main()
