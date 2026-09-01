#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <net/wki/auth_protocol.hpp>
#include <net/wki/wire.hpp>

namespace {

using namespace ker::net::wki;

constexpr auto make_crc32_table() -> std::array<uint32_t, 256> {
    std::array<uint32_t, 256> table{};
    for (uint32_t index = 0; index < table.size(); ++index) {
        uint32_t crc = index;
        for (unsigned bit = 0; bit < 8; ++bit) {
            crc = (crc & 1U) != 0 ? (crc >> 1U) ^ 0xEDB88320U : crc >> 1U;
        }
        table.at(index) = crc;
    }
    return table;
}

constexpr auto CRC32_TABLE = make_crc32_table();

auto crc32_continue(uint32_t crc, const void* data, size_t length) -> uint32_t {
    auto const* bytes = static_cast<const uint8_t*>(data);
    for (size_t index = 0; index < length; ++index) {
        crc = CRC32_TABLE.at((crc ^ bytes[index]) & 0xffU) ^ (crc >> 8U);
    }
    return crc;
}

[[gnu::noinline]] auto v2_crc(const WkiHeader& header, const uint8_t* payload) -> uint32_t {
    WkiHeader normalized = header;
    normalized.checksum = 0;
    uint32_t crc = crc32_continue(0xffffffffU, &normalized, sizeof(normalized));
    crc = crc32_continue(crc, payload, header.payload_len);
    return crc ^ 0xffffffffU;
}

struct Measurement {
    uint64_t elapsed_ns = 0;
    uint64_t sink = 0;
};

template <size_t PayloadSize>
[[gnu::noinline]] auto measure_crc(uint64_t iterations) -> Measurement {
    std::array<uint8_t, PayloadSize> payload{};
    payload.fill(0x5a);
    WkiHeader header{};
    header.version_flags = wki_version_flags(2, 0);
    header.msg_type = static_cast<uint8_t>(MsgType::DEV_OP_REQ);
    header.src_node = 1;
    header.dst_node = 2;
    header.payload_len = payload.size();
    uint64_t sink = 0;
    auto const START = std::chrono::steady_clock::now();
    for (uint64_t iteration = 1; iteration <= iterations; ++iteration) {
        header.seq_num = static_cast<uint32_t>(iteration);
        sink += v2_crc(header, payload.data());
    }
    auto const END = std::chrono::steady_clock::now();
    return {.elapsed_ns = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(END - START).count()), .sink = sink};
}

template <size_t PayloadSize>
[[gnu::noinline]] auto measure_auth(uint64_t iterations) -> Measurement {
    std::array<uint8_t, PayloadSize> payload{};
    payload.fill(0x5a);
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> key{};
    key.fill(0xa5);
    WkiHeader header{};
    header.version_flags = wki_version_flags(WKI_VERSION, 0);
    header.msg_type = static_cast<uint8_t>(MsgType::DEV_OP_REQ);
    header.src_node = 1;
    header.dst_node = 2;
    header.payload_len = payload.size();
    WkiAuthTrailer trailer{};
    trailer.session_id.fill(0x3c);
    uint64_t sink = 0;
    auto const START = std::chrono::steady_clock::now();
    for (uint64_t iteration = 1; iteration <= iterations; ++iteration) {
        header.seq_num = static_cast<uint32_t>(iteration);
        trailer.counter = iteration;
        auto const tag = auth_protocol::frame_tag(key, header, payload.data(), trailer, false);
        sink += tag.at(iteration % tag.size());
    }
    auto const END = std::chrono::steady_clock::now();
    return {.elapsed_ns = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(END - START).count()), .sink = sink};
}

template <size_t PayloadSize>
void run_size(uint64_t iterations) {
    constexpr size_t ROUNDS = 7;
    std::array<double, ROUNDS> crc_ns{};
    std::array<double, ROUNDS> auth_ns{};
    uint64_t sink = 0;
    for (size_t round = 0; round < ROUNDS; ++round) {
        Measurement crc{};
        Measurement auth{};
        if ((round & 1U) == 0) {
            crc = measure_crc<PayloadSize>(iterations);
            auth = measure_auth<PayloadSize>(iterations);
        } else {
            auth = measure_auth<PayloadSize>(iterations);
            crc = measure_crc<PayloadSize>(iterations);
        }
        crc_ns.at(round) = static_cast<double>(crc.elapsed_ns) / static_cast<double>(iterations);
        auth_ns.at(round) = static_cast<double>(auth.elapsed_ns) / static_cast<double>(iterations);
        sink += crc.sink + auth.sink;
    }
    std::ranges::sort(crc_ns);
    std::ranges::sort(auth_ns);
    double const CRC = crc_ns.at(ROUNDS / 2);
    double const AUTH = auth_ns.at(ROUNDS / 2);
    double const AUTH_MIB_S = (static_cast<double>(PayloadSize) * 1'000'000'000.0) / (AUTH * 1024.0 * 1024.0);
    std::printf(
        "wki_auth_bench payload=%zu iterations=%llu rounds=%zu crc_ns=%.2f auth_ns=%.2f delta_ns=%.2f ratio=%.2f "
        "auth_mib_s=%.2f sink=%llu\n",
        PayloadSize, static_cast<unsigned long long>(iterations), ROUNDS, CRC, AUTH, AUTH - CRC, AUTH / CRC, AUTH_MIB_S,
        static_cast<unsigned long long>(sink));
}

}  // namespace

auto main() -> int {
    run_size<64>(500'000);
    run_size<1400>(100'000);
    return 0;
}
