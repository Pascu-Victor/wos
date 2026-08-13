#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace perf {

struct TypedEventRecord {
    char type{};
    uint64_t ts_ns{};
    uint32_t cpu{};
    uint64_t pid{};
    uint64_t other_pid{};
    uint64_t data{};
    std::string callsite;
    std::string subsystem;
    std::string scope;
    std::string operation;
    std::string phase;
    int64_t lag{};
    uint8_t flags{};
    uint32_t aux{};
    uint64_t peer{};
    uint64_t channel{};
    uint64_t correlation{};
    int32_t status{};
    std::string wait_channel;
};

[[nodiscard]] auto serialize_typed_perf_event(const TypedEventRecord& event, std::optional<int64_t> realtime_offset_ns,
                                              std::string_view node_id, std::string& error) -> std::optional<std::string>;

}  // namespace perf
