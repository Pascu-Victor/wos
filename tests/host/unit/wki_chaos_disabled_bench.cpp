#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>

namespace {

using TxCallback = int (*)(uint16_t neighbor, const void* data, uint16_t len);
using Route = int (*)(TxCallback callback, uint16_t neighbor, const void* data, uint16_t len);

std::atomic<bool> s_chaos_enabled{false};

[[gnu::noinline]] auto transport_callback(uint16_t neighbor, const void* data, uint16_t len) -> int {
    asm volatile("" : "+r"(neighbor), "+r"(len) : "r"(data) : "memory");
    return static_cast<int>(neighbor) + static_cast<int>(len);
}

[[gnu::noinline]] auto direct_route(TxCallback callback, uint16_t neighbor, const void* data, uint16_t len) -> int {
    return callback(neighbor, data, len);
}

[[gnu::noinline]] auto disabled_chaos_route(TxCallback callback, uint16_t neighbor, const void* data, uint16_t len) -> int {
    // Mirrors the production disabled path: one relaxed atomic load followed
    // by the original callback with unchanged arguments.
    if (!s_chaos_enabled.load(std::memory_order_relaxed)) {
        return callback(neighbor, data, len);
    }
    return -1;
}

struct Measurement {
    uint64_t elapsed_ns = 0;
    uint64_t sink = 0;
};

[[gnu::noinline]] auto measure(Route route, uint64_t iterations) -> Measurement {
    std::array<uint8_t, 64> payload{};
    uint64_t sink = 0;
    auto const START = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < iterations; ++i) {
        sink += static_cast<uint32_t>(route(transport_callback, static_cast<uint16_t>(i), payload.data(), payload.size()));
    }
    auto const END = std::chrono::steady_clock::now();
    return Measurement{
        .elapsed_ns = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(END - START).count()),
        .sink = sink,
    };
}

}  // namespace

auto main() -> int {
    constexpr uint64_t ITERATIONS = 20'000'000;
    constexpr size_t ROUNDS = 7;
    std::array<double, ROUNDS> direct{};
    std::array<double, ROUNDS> disabled{};
    uint64_t sink = 0;

    sink += measure(direct_route, ITERATIONS / 10).sink;
    sink += measure(disabled_chaos_route, ITERATIONS / 10).sink;
    for (size_t round = 0; round < ROUNDS; ++round) {
        Measurement direct_result{};
        Measurement disabled_result{};
        if ((round & 1U) == 0) {
            direct_result = measure(direct_route, ITERATIONS);
            disabled_result = measure(disabled_chaos_route, ITERATIONS);
        } else {
            disabled_result = measure(disabled_chaos_route, ITERATIONS);
            direct_result = measure(direct_route, ITERATIONS);
        }
        direct.at(round) = static_cast<double>(direct_result.elapsed_ns) / static_cast<double>(ITERATIONS);
        disabled.at(round) = static_cast<double>(disabled_result.elapsed_ns) / static_cast<double>(ITERATIONS);
        sink += direct_result.sink + disabled_result.sink;
    }

    std::ranges::sort(direct);
    std::ranges::sort(disabled);
    double const DIRECT_NS = direct.at(ROUNDS / 2);
    double const DISABLED_NS = disabled.at(ROUNDS / 2);
    std::printf(
        "wki_chaos_disabled_bench iterations=%llu rounds=%zu direct_ns_per_call=%.3f disabled_ns_per_call=%.3f "
        "delta_ns=%.3f ratio=%.4f sink=%llu\n",
        static_cast<unsigned long long>(ITERATIONS), ROUNDS, DIRECT_NS, DISABLED_NS, DISABLED_NS - DIRECT_NS, DISABLED_NS / DIRECT_NS,
        static_cast<unsigned long long>(sink));
    return 0;
}
