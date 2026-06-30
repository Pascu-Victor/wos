#pragma once

#include <bits/ssize_t.h>

#include <cstddef>
#include <cstdint>

#include "netd/dhcp.hpp"

namespace netd {

auto recv_with_timeout(int sock, uint8_t* buf, size_t len, int timeout_secs) -> ssize_t;
auto recv_dhcp_reply_until_timeout(int sock, uint8_t* buf, size_t len, uint32_t expected_xid, int timeout_secs, DhcpLease* lease)
    -> uint8_t;

}  // namespace netd
