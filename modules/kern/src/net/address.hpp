#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::net::proto {

struct IPv4Address {
    uint32_t value{};

    constexpr IPv4Address() = default;
    constexpr IPv4Address(uint32_t host_order) : value(host_order) {}

    [[nodiscard]] static constexpr auto from_host_order(uint32_t host_order) -> IPv4Address { return IPv4Address{host_order}; }
    [[nodiscard]] static constexpr auto from_network_order(uint32_t network_order) -> IPv4Address {
        return IPv4Address{__builtin_bswap32(network_order)};
    }
    [[nodiscard]] static constexpr auto any() -> IPv4Address { return IPv4Address{0}; }
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    [[nodiscard]] static constexpr auto broadcast() -> IPv4Address { return IPv4Address{0xFFFFFFFF}; }

    [[nodiscard]] constexpr auto to_host_order() const -> uint32_t { return value; }
    [[nodiscard]] constexpr auto to_network_order() const -> uint32_t { return __builtin_bswap32(value); }
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    [[nodiscard]] constexpr auto octet(size_t index) const -> uint8_t { return static_cast<uint8_t>((value >> ((3 - index) * 8)) & 0xFF); }
    [[nodiscard]] constexpr auto is_any() const -> bool { return value == any().value; }
    [[nodiscard]] constexpr auto is_broadcast() const -> bool { return value == broadcast().value; }
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    [[nodiscard]] constexpr auto is_loopback() const -> bool { return octet(0) == 127; }

    constexpr operator uint32_t() const { return value; }
    constexpr auto operator==(const IPv4Address& other) const -> bool { return value == other.value; }
    constexpr auto operator!=(const IPv4Address& other) const -> bool { return !(*this == other); }
} __attribute__((packed));

static_assert(sizeof(IPv4Address) == sizeof(uint32_t));

struct IPv6Address {
    static constexpr size_t SIZE_BYTES = 16;

    std::array<uint8_t, SIZE_BYTES> bytes{};

    constexpr auto operator==(const IPv6Address& other) const -> bool { return bytes == other.bytes; }

    constexpr auto operator!=(const IPv6Address& other) const -> bool { return !(*this == other); }

    constexpr auto operator[](size_t index) const -> uint8_t { return bytes.at(index); }

    constexpr auto operator[](size_t index) -> uint8_t& { return bytes.at(index); }

    [[nodiscard]] constexpr auto data() -> uint8_t* { return bytes.data(); }

    [[nodiscard]] constexpr auto data() const -> const uint8_t* { return bytes.data(); }

    [[nodiscard]] constexpr auto size() const -> size_t { return bytes.size(); }

    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    [[nodiscard]] constexpr auto is_multicast() const -> bool { return bytes.at(0) == 0xFF; }
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    [[nodiscard]] constexpr auto is_link_local_multicast() const -> bool { return bytes.at(0) == 0xFF && bytes.at(1) == 0x02; }

    [[nodiscard]] constexpr auto is_unspecified() const -> bool {
        return std::ranges::all_of(bytes, [](uint8_t byte) -> bool { return byte == 0; });
    }

    static constexpr auto from_bytes(const std::array<uint8_t, SIZE_BYTES>& raw) -> IPv6Address { return IPv6Address{.bytes = raw}; }

    static constexpr auto unspecified() -> IPv6Address { return {}; }

    static constexpr auto loopback() -> IPv6Address {
        IPv6Address addr{};
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
        addr.bytes.at(15) = 1;
        return addr;
    }
} __attribute__((packed));

static_assert(sizeof(IPv6Address) == sizeof(std::array<uint8_t, IPv6Address::SIZE_BYTES>));

struct MacAddress {
    static constexpr size_t SIZE_BYTES = 6;

    std::array<uint8_t, SIZE_BYTES> bytes{};

    constexpr MacAddress() = default;
    constexpr MacAddress(const std::array<uint8_t, SIZE_BYTES>& raw) : bytes(raw) {}

    [[nodiscard]] static constexpr auto from_bytes(const std::array<uint8_t, SIZE_BYTES>& raw) -> MacAddress { return MacAddress{raw}; }
    [[nodiscard]] static constexpr auto zero() -> MacAddress { return {}; }
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    [[nodiscard]] static constexpr auto broadcast() -> MacAddress { return MacAddress{{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}}; }

    [[nodiscard]] constexpr auto data() -> uint8_t* { return bytes.data(); }
    [[nodiscard]] constexpr auto data() const -> const uint8_t* { return bytes.data(); }
    [[nodiscard]] constexpr auto size() const -> size_t { return bytes.size(); }
    [[nodiscard]] constexpr auto is_broadcast() const -> bool { return *this == broadcast(); }
    [[nodiscard]] constexpr auto is_multicast() const -> bool { return (bytes.at(0) & 0x01) != 0; }

    [[nodiscard]] constexpr auto operator[](size_t index) -> uint8_t& { return bytes.at(index); }
    constexpr auto operator[](size_t index) const -> uint8_t { return bytes.at(index); }
    [[nodiscard]] constexpr auto at(size_t index) -> uint8_t& { return bytes.at(index); }
    [[nodiscard]] constexpr auto at(size_t index) const -> uint8_t { return bytes.at(index); }
    constexpr void fill(uint8_t value) { bytes.fill(value); }

    constexpr auto operator=(const std::array<uint8_t, SIZE_BYTES>& raw) -> MacAddress& {
        bytes = raw;
        return *this;
    }
    constexpr operator std::array<uint8_t, SIZE_BYTES>&() { return bytes; }
    constexpr operator const std::array<uint8_t, SIZE_BYTES>&() const { return bytes; }
    constexpr auto operator==(const MacAddress& other) const -> bool { return bytes == other.bytes; }
    constexpr auto operator!=(const MacAddress& other) const -> bool { return !(*this == other); }
    [[nodiscard]] constexpr auto compare(const MacAddress& other) const -> int {
        for (size_t i = 0; i < SIZE_BYTES; i++) {
            if (bytes.at(i) < other.bytes.at(i)) {
                return -1;
            }
            if (bytes.at(i) > other.bytes.at(i)) {
                return 1;
            }
        }
        return 0;
    }
} __attribute__((packed));

static_assert(sizeof(MacAddress) == MacAddress::SIZE_BYTES);

}  // namespace ker::net::proto
