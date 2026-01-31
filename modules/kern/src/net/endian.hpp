#pragma once

#include <cstdint>

namespace ker::net {

inline auto htons(uint16_t host) -> uint16_t { return __builtin_bswap16(host); }

inline auto ntohs(uint16_t net) -> uint16_t { return __builtin_bswap16(net); }

inline auto htonl(uint32_t host) -> uint32_t { return __builtin_bswap32(host); }

inline auto ntohl(uint32_t net) -> uint32_t { return __builtin_bswap32(net); }

}  // namespace ker::net
