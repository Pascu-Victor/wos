#include "ipv6_ra.hpp"

#include <abi-bits/in.h>
#include <abi-bits/socket.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>
#include <wos/netctl.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <print>
#include <span>
#include <utility>

namespace {

constexpr uint8_t ICMPV6_ROUTER_ADVERT = 134;
constexpr uint8_t ND_OPT_PREFIX_INFORMATION = 3;
constexpr uint8_t ND_OPT_SOURCE_LINK_LAYER_ADDRESS = 1;
constexpr uint32_t DEFAULT_VALID_LIFETIME_S = 600;
constexpr uint32_t DEFAULT_PREFERRED_LIFETIME_S = 300;
constexpr uint16_t DEFAULT_ROUTER_LIFETIME_S = 180;
constexpr uint32_t DEFAULT_INTERVAL_MS = 1000;
constexpr uint32_t DEFAULT_COUNT = 3;
constexpr std::array<uint8_t, 16> TEST_PREFIX = {0x20, 0x01, 0x0D, 0xB8, 0x00, 0x06, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
constexpr std::array<uint8_t, 16> ALL_NODES = {0xFF, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};

enum class Mode {
    ADVERTISE,
    WITHDRAW,
    MALFORMED_ZERO_LENGTH,
    MALFORMED_TRUNCATED,
};

struct Options {
    const char* ifname = "eth0";
    Mode mode = Mode::ADVERTISE;
    uint32_t count = DEFAULT_COUNT;
    uint32_t interval_ms = DEFAULT_INTERVAL_MS;
};

void store_be16(uint8_t* out, uint16_t value) {
    out[0] = static_cast<uint8_t>(value >> 8U);
    out[1] = static_cast<uint8_t>(value);
}

void store_be32(uint8_t* out, uint32_t value) {
    out[0] = static_cast<uint8_t>(value >> 24U);
    out[1] = static_cast<uint8_t>(value >> 16U);
    out[2] = static_cast<uint8_t>(value >> 8U);
    out[3] = static_cast<uint8_t>(value);
}

auto parse_u32(const char* text, uint32_t& out) -> bool {
    if (text == nullptr || *text == '\0') {
        return false;
    }
    char* end = nullptr;
    unsigned long const VALUE = std::strtoul(text, &end, 10);
    if (end == text || *end != '\0' || VALUE > UINT32_MAX) {
        return false;
    }
    out = static_cast<uint32_t>(VALUE);
    return true;
}

auto parse_options(int argc, char** argv, Options& out) -> bool {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--ifname") == 0 && i + 1 < argc) {
            out.ifname = argv[++i];
        } else if (std::strcmp(argv[i], "--withdraw") == 0) {
            out.mode = Mode::WITHDRAW;
        } else if (std::strcmp(argv[i], "--malformed-zero-length") == 0) {
            out.mode = Mode::MALFORMED_ZERO_LENGTH;
        } else if (std::strcmp(argv[i], "--malformed-truncated") == 0) {
            out.mode = Mode::MALFORMED_TRUNCATED;
        } else if (std::strcmp(argv[i], "--count") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], out.count) || out.count == 0 || out.count > 1000) {
                return false;
            }
        } else if (std::strcmp(argv[i], "--interval-ms") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], out.interval_ms) || out.interval_ms > 60000) {
                return false;
            }
        } else {
            return false;
        }
    }
    return out.ifname != nullptr && out.ifname[0] != '\0';
}

auto get_interface_identity(const char* ifname, uint32_t& ifindex, std::array<uint8_t, 6>& mac, std::array<uint8_t, 16>& link_local)
    -> bool {
    std::array<wos_net_if_info, 16> interfaces{};
    size_t interface_count = interfaces.size();
    if (wos_net_if_list(interfaces.data(), &interface_count) != 0) {
        return false;
    }
    for (size_t i = 0; i < std::min(interface_count, interfaces.size()); ++i) {
        auto const& interface = interfaces[i];
        if (std::strncmp(interface.name, ifname, sizeof(interface.name)) == 0) {
            ifindex = interface.ifindex;
            std::copy_n(interface.addr, mac.size(), mac.data());
            break;
        }
    }
    if (ifindex == 0) {
        return false;
    }

    std::array<wos_net_addr_info, 64> addresses{};
    size_t address_count = addresses.size();
    if (wos_net_addr_list(addresses.data(), &address_count) != 0) {
        return false;
    }
    for (size_t i = 0; i < std::min(address_count, addresses.size()); ++i) {
        auto const& address = addresses[i];
        if (address.ifindex == ifindex && address.family == AF_INET6 && address.local[0] == 0xFE && (address.local[1] & 0xC0U) == 0x80U) {
            std::copy_n(address.local, link_local.size(), link_local.data());
            return true;
        }
    }
    return false;
}

auto build_ra(const Options& options, std::span<const uint8_t, 6> mac, std::array<uint8_t, 64>& packet) -> size_t {
    packet.fill(0);
    packet[0] = ICMPV6_ROUTER_ADVERT;
    packet[4] = 64;
    uint16_t const ROUTER_LIFETIME = options.mode == Mode::WITHDRAW ? 0 : DEFAULT_ROUTER_LIFETIME_S;
    store_be16(packet.data() + 6, ROUTER_LIFETIME);

    packet[16] = ND_OPT_SOURCE_LINK_LAYER_ADDRESS;
    packet[17] = 1;
    std::copy(mac.begin(), mac.end(), packet.begin() + 18);

    packet[24] = ND_OPT_PREFIX_INFORMATION;
    packet[25] = options.mode == Mode::MALFORMED_ZERO_LENGTH ? 0 : 4;
    packet[26] = 64;
    packet[27] = 0xC0;
    uint32_t const VALID = options.mode == Mode::WITHDRAW ? 0 : DEFAULT_VALID_LIFETIME_S;
    uint32_t const PREFERRED = options.mode == Mode::WITHDRAW ? 0 : DEFAULT_PREFERRED_LIFETIME_S;
    store_be32(packet.data() + 28, VALID);
    store_be32(packet.data() + 32, PREFERRED);
    std::copy(TEST_PREFIX.begin(), TEST_PREFIX.end(), packet.begin() + 40);
    return options.mode == Mode::MALFORMED_TRUNCATED ? 48 : 56;
}

void sleep_ms(uint32_t milliseconds) {
    timespec delay{.tv_sec = static_cast<time_t>(milliseconds / 1000), .tv_nsec = static_cast<long>((milliseconds % 1000) * 1000000)};
    while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {
    }
}

}  // namespace

auto run_ipv6_ra(int argc, char** argv) -> int {
    Options options{};
    if (!parse_options(argc, argv, options)) {
        std::println(stderr,
                     "usage: testprog ipv6-ra [--ifname IFACE] [--withdraw|--malformed-zero-length|--malformed-truncated] "
                     "[--count N] [--interval-ms N]");
        return 2;
    }

    int const SOCKET_FD = socket(AF_INET6, SOCK_RAW, IPPROTO_ICMPV6);
    if (SOCKET_FD < 0 || setsockopt(SOCKET_FD, SOL_SOCKET, SO_BINDTODEVICE, options.ifname, std::strlen(options.ifname) + 1) != 0) {
        std::println(stderr, "ipv6-ra: socket setup failed on {}: errno={}", options.ifname, errno);
        if (SOCKET_FD >= 0) {
            close(SOCKET_FD);
        }
        return 1;
    }

    uint32_t ifindex = 0;
    std::array<uint8_t, 6> mac{};
    std::array<uint8_t, 16> link_local{};
    if (!get_interface_identity(options.ifname, ifindex, mac, link_local)) {
        std::println(stderr, "ipv6-ra: cannot resolve {}", options.ifname);
        close(SOCKET_FD);
        return 1;
    }
    sockaddr_in6 local{};
    local.sin6_family = AF_INET6;
    local.sin6_scope_id = ifindex;
    std::memcpy(&local.sin6_addr, link_local.data(), link_local.size());
    int const HOPS = 255;
    if (bind(SOCKET_FD, reinterpret_cast<const sockaddr*>(&local), sizeof(local)) != 0 ||
        setsockopt(SOCKET_FD, IPPROTO_IPV6, IPV6_MULTICAST_HOPS, &HOPS, sizeof(HOPS)) != 0) {
        std::println(stderr, "ipv6-ra: bind/hop-limit setup failed: errno={}", errno);
        close(SOCKET_FD);
        return 1;
    }

    sockaddr_in6 destination{};
    destination.sin6_family = AF_INET6;
    destination.sin6_scope_id = ifindex;
    std::memcpy(&destination.sin6_addr, ALL_NODES.data(), ALL_NODES.size());
    std::array<uint8_t, 64> packet{};
    size_t const PACKET_LENGTH = build_ra(options, mac, packet);
    for (uint32_t i = 0; i < options.count; ++i) {
        ssize_t const SENT =
            sendto(SOCKET_FD, packet.data(), PACKET_LENGTH, 0, reinterpret_cast<const sockaddr*>(&destination), sizeof(destination));
        if (!std::cmp_equal(SENT, PACKET_LENGTH)) {
            std::println(stderr, "ipv6-ra: send failed: sent={} expected={} errno={}", SENT, PACKET_LENGTH, errno);
            close(SOCKET_FD);
            return 1;
        }
        if (i + 1 < options.count) {
            sleep_ms(options.interval_ms);
        }
    }
    std::println("ipv6-ra: sent {} advertisement(s) for 2001:db8:6::/64 on {} mode={}", options.count, options.ifname,
                 static_cast<unsigned>(options.mode));
    close(SOCKET_FD);
    return 0;
}
