#pragma once
#include <cstdint>

namespace ker::abi::net {
enum class ops : uint64_t {
    socket,
    bind,
    listen,
    accept,
    connect,
    send,
    recv,
    close,
};

}  // namespace ker::abi::net
