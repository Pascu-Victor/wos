#pragma once

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <platform/acpi/numa_topology.hpp>

namespace ker::mod::mm::numa {
using NodeId = uint16_t;
using NodeMask = uint64_t;
inline constexpr NodeId INVALID_NODE = acpi::numa::INVALID_NODE;
inline constexpr size_t MAX_NODES = acpi::numa::MAX_NODES;

enum class Mode : uint8_t { LOCAL, PREFERRED, BIND, INTERLEAVE };

struct Policy {
    NodeMask nodes{};
    Mode mode{Mode::LOCAL};
    auto operator==(const Policy&) const -> bool = default;
};

[[nodiscard]] constexpr auto valid_policy(Policy policy, size_t node_count) -> bool {
    if (node_count == 0 || node_count > MAX_NODES) return false;
    NodeMask const ONLINE = node_count == 64 ? UINT64_MAX : (NodeMask{1} << node_count) - 1;
    if ((policy.nodes & ~ONLINE) != 0) return false;
    switch (policy.mode) {
        case Mode::LOCAL:
            return policy.nodes == 0;
        case Mode::PREFERRED:
            return std::has_single_bit(policy.nodes);
        case Mode::BIND:
        case Mode::INTERLEAVE:
            return policy.nodes != 0;
    }
    return false;
}

enum class SelectionError : uint8_t { NONE, INVALID_POLICY, INVALID_LOCAL_NODE };

struct Selection {
    std::array<NodeId, MAX_NODES> nodes{};
    NodeId requested{INVALID_NODE};
    uint16_t count{};
    SelectionError error{SelectionError::INVALID_POLICY};
    [[nodiscard]] auto valid() const -> bool { return error == SelectionError::NONE && count != 0; }
};

// Value-only plan: allocation must still enforce quota, physical ownership and
// node reserves at the actual claim. A plan is never a capacity reservation.
[[nodiscard]] inline auto select_nodes(Policy policy, const acpi::numa::Topology& topology, NodeId local, uint64_t virtual_page_index)
    -> Selection {
    Selection result;
    if (!valid_policy(policy, topology.node_count)) return result;
    if (local >= topology.node_count) {
        result.error = SelectionError::INVALID_LOCAL_NODE;
        return result;
    }
    NodeMask const ALL = topology.node_count == 64 ? UINT64_MAX : (NodeMask{1} << topology.node_count) - 1;
    NodeMask const ALLOWED = policy.mode == Mode::BIND || policy.mode == Mode::INTERLEAVE ? policy.nodes : ALL;
    if (policy.mode == Mode::INTERLEAVE) {
        size_t const FIRST = virtual_page_index % static_cast<size_t>(std::popcount(ALLOWED));
        std::array<NodeId, MAX_NODES> members{};
        size_t count = 0;
        for (NodeId node = 0; node < topology.node_count; ++node)
            if ((ALLOWED & (NodeMask{1} << node)) != 0) members[count++] = node;
        for (size_t i = 0; i < count; ++i) result.nodes[i] = members[(FIRST + i) % count];
        result.count = static_cast<uint16_t>(count);
        result.requested = result.nodes[0];
    } else {
        for (NodeId node = 0; node < topology.node_count; ++node) {
            if ((ALLOWED & (NodeMask{1} << node)) == 0) continue;
            size_t position = result.count;
            // Insertion order retains the lower dense ID for equal distances.
            while (position != 0 && topology.distances[local][result.nodes[position - 1]] > topology.distances[local][node]) {
                result.nodes[position] = result.nodes[position - 1];
                --position;
            }
            result.nodes[position] = node;
            ++result.count;
        }
        NodeId const PREFERRED = policy.mode == Mode::PREFERRED ? static_cast<NodeId>(std::countr_zero(policy.nodes)) : local;
        if ((ALLOWED & (NodeMask{1} << PREFERRED)) != 0) {
            size_t position = 0;
            while (result.nodes[position] != PREFERRED) ++position;
            for (; position != 0; --position) result.nodes[position] = result.nodes[position - 1];
            result.nodes[0] = PREFERRED;
        }
        result.requested = result.nodes[0];
    }
    result.error = SelectionError::NONE;
    return result;
}

}  // namespace ker::mod::mm::numa
