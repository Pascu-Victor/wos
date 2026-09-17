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
        start = source.find(f"void {name}(")
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
    install = accept.find("allocate_socket_fd(new_sock, descriptor)")
    reserve = accept.find("ker::vfs::FdReservation descriptor(task)")
    output = accept.find("usercopy::copy_to_task", effect)
    if min(preflight, reserve, effect, output, install) < 0 or not (preflight < reserve < effect < output < install):
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


def ordered(body: str, tokens: list[str]) -> None:
    previous = -1
    for token in tokens:
        position = body.find(token, previous + 1)
        if position < 0:
            fail(f"network authority ordering missing {token}")
        previous = position

def require_network_authority_before_side_effects(source: str) -> None:
    if "case ker::abi::net::ops::NETCTL_ROUTE_SET:\n        case ker::abi::net::ops::NETCTL_ROUTE_DEL: {" not in source:
        fail("route add and delete must share the checked admission block")
    gate = ["task->credentials()", "authorize_network_administration(CREDENTIALS)", "if (AUTHORIZATION != 0)"]
    ordered(case_body(source, "SOCKET"), ["socket_protocol_supported", "SOCK_TYPE_MASK", *gate,
                                          "FdReservation descriptor", "socket_create"])
    ioctl = case_body(source, "IOCTL_NET")
    ordered(ioctl, ["usercopy::copy_from_task", "bool const MUTATES", *gate, "if (request == SIOC_ADDRT", "netdev_find_by_name_ref"])
    classification = ioctl.split("bool const MUTATES", 1)[1].split(";", 1)[0]
    for name in ["SIFFLAGS", "SIFADDR", "SIFNETMASK", "SIFMTU", "SIFNAME", "SIFHWADDR", "SIFHWBROADCAST", "SIFTXQLEN", "ADDRT", "DELRT"]:
        if "request == SIOC_" + name not in classification:
            fail(f"legacy network writer {name} has no authority admission")
    for name in ["SET_DEV_CPU_AFFINITY", "NETCTL_ADDR_SET", "NETCTL_ADDR_DEL", "NETCTL_ADDR_SET_V2", "NETCTL_ROUTE_DEL", "NETCTL_LINK_SET"]:
        body = case_body(source, name)
        if body.count("task->credentials()") != 1:
            fail(f"{name} must take one credential snapshot")
        if name == "NETCTL_LINK_SET":
            ordered(body, ["usercopy::copy_", "task->credentials()", "apply_link_request(req, CREDENTIALS)"])
            helper = function_body(source, "apply_link_request")
            ordered(helper, [*gate[1:], "dev_ref", "name_change.prepare(dev_ref.identity()", "apply_ifflags(",
                             "if (RET != 0) return", "dev->mtu =", "name_change.commit()"])
            if "task->credentials()" in helper:
                fail("link commit must retain the syscall's admitted snapshot")
        else:
            ordered(body, ["usercopy::copy_", *gate, "dev_ref"])
    flags = function_body(source, "apply_ifflags")
    ordered(flags, ["dev->ops->open_as(dev, CREDENTIALS)", "status = dev->ops->open(dev)", "if (status != 0) return", "dev->state = 1", "publish_ifflags"])
    ordered(flags, ["dev->ops->close_as(dev, CREDENTIALS)", "if (STATUS != 0) return", "dev->ops->close(dev)", "dev->state = 0", "publish_ifflags"])

    if "return static_cast<uint64_t>(apply_ifflags(dev, flags," not in ioctl:
        fail("legacy interface flags must return the driver's refusal")
    for name in ["NETCTL_IF_LIST", "NETCTL_ADDR_LIST", "NETCTL_ROUTE_LIST", "BIND", "CONNECT", "LISTEN"]:
        if "authorize_network_administration" in case_body(source, name):
            fail(f"public network operation {name} must retain its existing admission")


def require_combined_link_admission(source: str) -> None:
    body = function_body(source, "apply_link_request")
    ordered(body, ["authorize_network_administration(CREDENTIALS)", "control_guard.try_acquire(dev)",
                   "if (CONTROL_ADMISSION != 0) return", "name_change.prepare"])
    ioctl = case_body(source, "IOCTL_NET")
    ordered(ioctl, ["authorize_network_administration(CREDENTIALS)", "netdev_find_by_name_ref", "bool const LINK_MUTATION",
                    "control_guard.try_acquire(dev)", "if (CONTROL_ADMISSION != 0) return", "switch (request)"])
    classification = ioctl.split("bool const LINK_MUTATION", 1)[1].split(";", 1)[0]
    for name in ["SIFFLAGS", "SIFMTU", "SIFTXQLEN", "SIFHWADDR", "SIFNAME"]:
        if "request == SIOC_" + name not in classification:
            fail(f"legacy link writer {name} can bypass device control admission")
    ordered(body, ["name_change.prepare", "dev->ops->apply_link_as(dev, control, CREDENTIALS)", "if (STATUS != 0) return", "publish_ifflags", "dev->mtu =", "name_change.commit"])
    ordered(body, ["netdev_check_mac(dev, req.hwaddr)", "apply_ifflags(dev, req.flags", "commit_netdev_hwaddr", "dev->mtu ="])
    if body.count("apply_link_as(dev, control, CREDENTIALS)") != 1 or "set_netdev_hwaddr(" in body:
        fail("combined remote driver effects must use one admitted operation")
    ordered(function_body(source, "set_netdev_hwaddr"), ["apply_link_as(dev, control, caller)", "if (STATUS != 0) return"])
    if "set_netdev_hwaddr(dev, sa + 2, 6, CREDENTIALS)" not in source:
        fail("legacy MAC control must retain admitted caller")


def require_owned_interface_configuration(source: str) -> None:
    fill = function_body(source, "fill_if_info")
    if fill.count("netdev_config_snapshot(dev)") != 1:
        fail("one interface record must consume one owned configuration")
    for field in ["name", "mac", "mtu", "tx_queue_len", "link_flags", "state"]:
        if "dev->" + field in fill:
            fail(f"interface list reads live {field} during control commit")
    ioctl = case_body(source, "IOCTL_NET")
    for field in ["dev->mac", "effective_ifflags(dev)", "static_cast<int32_t>(dev->mtu)", "static_cast<int32_t>(dev->tx_queue_len)"]:
        if field in ioctl:
            fail("legacy interface query bypasses the owned configuration")


def main() -> None:
    require_owned_interface_configuration(NET_CPP.read_text())
    source = NET_CPP.read_text()
    require_bounded_socket_io(source)
    require_address_and_option_snapshots(source)
    require_fixed_abi_records_and_wait_sets(source)
    require_network_authority_before_side_effects(source)
    require_combined_link_admission(source)
    print("network syscall usercopy invariants hold")


if __name__ == "__main__":
    main()
