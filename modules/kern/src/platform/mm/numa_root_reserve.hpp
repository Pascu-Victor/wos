#pragma once

#include <array>
#include <cstdint>
#include <platform/mm/numa_policy.hpp>
#include <platform/mm/paging.hpp>

namespace ker::mod::mm::numa::root_reserve {

inline constexpr uint32_t PAGES_PER_NODE = 64;
inline constexpr uint32_t MAX_MEMBERS = MAX_NODES * PAGES_PER_NODE;

enum class Error : uint8_t { NONE, NOT_READY, NO_TOPOLOGY, ALREADY_INITIALIZED, INVALID_NODE, EMPTY, CORRUPT };
struct TakeResult {
    paging::PageTable* table{};
    Error error{Error::NOT_READY};
};
enum class ReturnResult : uint8_t { NOT_MEMBER, RETURNED, CORRUPT };

struct NodeStats {
    uint32_t capacity{};
    uint32_t available{};
    uint32_t loaned{};
    uint32_t transfers{};
    uint32_t shortfall{};
};
struct Snapshot {
    std::array<NodeStats, MAX_NODES> nodes{};
    uint64_t sequence{};
    uint32_t members{};
    uint16_t node_count{};
    bool published{};
    bool healthy{};
};

// BSP-only after immutable topology/MM publication, before starting APs. Fixed
// membership lives until shutdown. Ordinary pressure yields actual shortfall.
[[nodiscard]] auto initialize_after_topology() noexcept -> Error;

// Root-factory fallback only, after its ordinary fallible allocation failed.
// The caller supplies one captured valid local node; no cross-node borrowing.
[[nodiscard]] auto try_take(NodeId requested) noexcept -> TakeResult;

using ZeroTable = void (*)(paging::PageTable*);
// Caller owns the exact never-active private or genuinely retired sole root.
// Future virt integration supplies its existing safe zero_page_table_for_pool.
// Callback executes without the reserve or allocator lock; do not republish the
// root. Only NOT_MEMBER permits the ordinary PageTablePool/free fallback.
[[nodiscard]] auto try_return(paging::PageTable* table, ZeroTable zero_table) noexcept -> ReturnResult;

// Caller-owned preexisting output. Counts have one pool-lock instant; they are
// not atomic with a separate physical snapshot. No ownership reference escapes.
void snapshot(Snapshot& out) noexcept;

}  // namespace ker::mod::mm::numa::root_reserve
