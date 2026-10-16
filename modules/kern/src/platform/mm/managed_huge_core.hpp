#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace ker::mod::mm::huge {
inline constexpr uint64_t PAGE_BYTES = 4096;
inline constexpr uint64_t LEAF_PAGES = 512;
inline constexpr uint64_t LEAF_BYTES = PAGE_BYTES * LEAF_PAGES;
inline constexpr size_t MAX_LEAVES = 128;
inline constexpr size_t MAX_ROOT_LEAVES = 64;
inline constexpr size_t MAX_NODES = 64;
inline constexpr size_t MAX_PLANS = 128;
inline constexpr uint16_t MAX_PLAN_TABLES = 64;
inline constexpr uint16_t MAX_PREPARED_TABLES = 128;

enum class Error : uint8_t { None, NotReady, Invalid, Capacity, Stale, Conflict, GenerationExhausted, Exhausted, Again, Unsupported };
enum class State : uint8_t { Free, Preparing, Mapped, Splitting, Retired };
struct RootKey {
    uint64_t address_bits{}, incarnation{};
};
struct LeafKey {
    RootKey root{};
    uint64_t va_2m{}, pa_2m{};
};
struct LeafHandle {
    uint16_t slot{};
    uint64_t generation{};
};
struct PlanHandle {
    uint16_t slot{};
    uint64_t generation{};
};
struct CapsInput {
    uint16_t node_count{};
    uint64_t usable[MAX_NODES]{};
};
struct Caps {
    uint64_t global_pages{}, root_pages{}, node_pages[MAX_NODES]{};
};
struct Row {
    LeafKey key{};
    uint64_t generation{}, semantic_word{};
    uint16_t node{}, next_retired{};
    State state{};
};
struct MappedLeaf {
    LeafKey key{};
    LeafHandle handle{};
    uint64_t semantic_word{};
    uint16_t node{};
};
struct Snapshot {
    Caps caps{};
    uint64_t charged_pages{}, node_charged[MAX_NODES]{};
    uint64_t preparing_pages{}, mapped_pages{}, splitting_pages{}, retired_pages{};
    uint16_t plans{}, table_pages{}, node_count{};
    bool ready{};
};

// Private scalar state only. Every kernel call is serialized by the existing
// user mapping pin lock. Force-inline also at O0: an instrumented TU must never
// provide an out-of-line weak copy selected by a lock-held excluded MM caller.
class RegistryCore {
   public:
    [[gnu::always_inline]] auto initialize(const CapsInput& input) noexcept -> Error {
        if (ready_) return Error::Conflict;
        if (input.node_count == 0 || input.node_count > MAX_NODES) return Error::Invalid;
        uint64_t sum = 0;
        Caps prepared{};
        for (size_t i = 0; i < MAX_NODES; ++i) {
            if (i >= input.node_count && input.usable[i] != 0) return Error::Invalid;
            if (UINT64_MAX - sum < input.usable[i]) return Error::Invalid;
            sum += input.usable[i];
            prepared.node_pages[i] = (input.usable[i] / 16 / LEAF_PAGES) * LEAF_PAGES;
        }
        if (sum == 0) return Error::Invalid;
        uint64_t const FRACTION = (sum / 16 / LEAF_PAGES) * LEAF_PAGES;
        prepared.global_pages = FRACTION < MAX_LEAVES * LEAF_PAGES ? FRACTION : MAX_LEAVES * LEAF_PAGES;
        prepared.root_pages = MAX_ROOT_LEAVES * LEAF_PAGES;
        caps_ = prepared;
        node_count_ = input.node_count;
        ready_ = true;
        return Error::None;
    }

    [[gnu::always_inline]] static auto same_root(RootKey a, RootKey b) noexcept -> bool {
        return a.address_bits == b.address_bits && a.incarnation == b.incarnation;
    }
    [[gnu::always_inline]] static auto valid_root(RootKey root) noexcept -> bool {
        return root.address_bits != 0 && (root.address_bits & (PAGE_BYTES - 1)) == 0 && root.incarnation != 0;
    }
    [[gnu::always_inline]] static auto valid_va(uint64_t va) noexcept -> bool {
        return va != 0 && (va & (LEAF_BYTES - 1)) == 0 && va <= (uint64_t{1} << 47) - LEAF_BYTES;
    }
    [[gnu::always_inline]] static auto same_key(const LeafKey& a, const LeafKey& b) noexcept -> bool {
        return same_root(a.root, b.root) && a.va_2m == b.va_2m && a.pa_2m == b.pa_2m;
    }

    [[gnu::always_inline]] auto reserve_leaf(RootKey root, uint64_t va, uint16_t node, LeafHandle& empty) noexcept -> Error {
        if (!ready_) return Error::NotReady;
        if (empty.generation != 0 || !valid_root(root) || !valid_va(va) || node >= node_count_) return Error::Invalid;
        uint64_t root_pages = 0;
        size_t free_slot = MAX_LEAVES;
        for (size_t i = 0; i < MAX_LEAVES; ++i) {
            auto const& row = rows_[i];
            if (row.state == State::Free) {
                if (free_slot == MAX_LEAVES) free_slot = i;
                continue;
            }
            if (same_root(row.key.root, root)) {
                root_pages += LEAF_PAGES;
                if (row.key.va_2m == va) return Error::Conflict;
            }
        }
        if (free_slot == MAX_LEAVES || charged_pages_ > caps_.global_pages || LEAF_PAGES > caps_.global_pages - charged_pages_ ||
            root_pages > caps_.root_pages || LEAF_PAGES > caps_.root_pages - root_pages || node_charged_[node] > caps_.node_pages[node] ||
            LEAF_PAGES > caps_.node_pages[node] - node_charged_[node])
            return Error::Capacity;
        if (issuer_ == UINT64_MAX) return Error::GenerationExhausted;
        uint64_t const GENERATION = ++issuer_;
        rows_[free_slot] = {{root, va, 0}, GENERATION, 0, node, 0, State::Preparing};
        charged_pages_ += LEAF_PAGES;
        node_charged_[node] += LEAF_PAGES;
        empty = {static_cast<uint16_t>(free_slot), GENERATION};
        return Error::None;
    }
    [[gnu::always_inline]] auto cancel_preparing(LeafHandle handle) noexcept -> Error {
        if (!matches(handle, State::Preparing)) return Error::Stale;
        release_row(handle.slot);
        return Error::None;
    }
    [[gnu::always_inline]] auto publish_mapped(LeafHandle handle, const LeafKey& key, uint64_t semantic_word) noexcept -> Error {
        if (!matches(handle, State::Preparing)) return Error::Stale;
        auto& row = rows_[handle.slot];
        constexpr uint64_t FRAME = 0x000fffffffe00000ULL;
        constexpr uint64_t PERMISSIONS = 1 | 2 | 4 | (uint64_t{1} << 63);
        constexpr uint64_t PS = uint64_t{1} << 7;
        constexpr uint64_t AD = (uint64_t{1} << 5) | (uint64_t{1} << 6);
        if (!same_root(row.key.root, key.root) || row.key.va_2m != key.va_2m || (key.pa_2m & (LEAF_BYTES - 1)) != 0 ||
            (key.pa_2m & ~FRAME) != 0 || (semantic_word & FRAME) != key.pa_2m || (semantic_word & (1 | PS)) != (1 | PS) ||
            (semantic_word & ~(FRAME | PERMISSIONS | PS | AD)) != 0)
            return Error::Invalid;
        row.key = key;
        row.semantic_word = semantic_word & ~AD;
        row.state = State::Mapped;
        return Error::None;
    }
    [[gnu::always_inline]] auto match_mapped(LeafHandle handle, const LeafKey& key, uint64_t raw) const noexcept -> bool {
        constexpr uint64_t AD = (uint64_t{1} << 5) | (uint64_t{1} << 6);
        return matches(handle, State::Mapped) && same_key(rows_[handle.slot].key, key) && rows_[handle.slot].semantic_word == (raw & ~AD);
    }
    [[gnu::always_inline]] auto lookup_mapped(RootKey root, uint64_t va, uint64_t raw, MappedLeaf& empty) const noexcept -> Error {
        if (!ready_) return Error::NotReady;
        if (!valid_root(root) || !valid_va(va) || empty.handle.generation != 0) return Error::Invalid;
        for (size_t i = 0; i < MAX_LEAVES; ++i) {
            auto const& row = rows_[i];
            if (row.state != State::Free && same_root(row.key.root, root) && row.key.va_2m == va) {
                LeafHandle const handle{static_cast<uint16_t>(i), row.generation};
                if (!match_mapped(handle, row.key, raw)) return Error::Unsupported;
                empty = {row.key, handle, row.semantic_word, row.node};
                return Error::None;
            }
        }
        return Error::Unsupported;
    }
    [[gnu::always_inline]] auto validate_preparing(LeafHandle handle, const LeafKey& key, uint16_t node) const noexcept -> bool {
        if (!matches(handle, State::Preparing)) return false;
        auto const& row = rows_[handle.slot];
        return same_root(row.key.root, key.root) && row.key.va_2m == key.va_2m && row.node == node;
    }
    [[gnu::always_inline]] auto begin_split(LeafHandle handle, const LeafKey& key) noexcept -> Error {
        if (!matches(handle, State::Mapped) || !same_key(rows_[handle.slot].key, key)) return Error::Stale;
        rows_[handle.slot].state = State::Splitting;
        return Error::None;
    }
    // Only a future typed actual TLB completion adapter may call this in-kernel.
    // Pure core state cannot certify physical completion itself.
    [[gnu::always_inline]] auto complete_split_after_ack(LeafHandle handle) noexcept -> Error {
        if (!matches(handle, State::Splitting)) return Error::Stale;
        release_row(handle.slot);
        return Error::None;
    }
    [[gnu::always_inline]] auto detach_mapped(LeafHandle handle, const LeafKey& key) noexcept -> Error {
        if (!matches(handle, State::Mapped) || !same_key(rows_[handle.slot].key, key)) return Error::Stale;
        rows_[handle.slot].state = State::Retired;
        rows_[handle.slot].next_retired = 0;
        return Error::None;
    }
    [[gnu::always_inline]] auto advance_retired_handoff(LeafHandle handle, uint16_t expected_next, uint16_t count) noexcept -> Error {
        if (!matches(handle, State::Retired)) return Error::Stale;
        auto& row = rows_[handle.slot];
        if (row.next_retired != expected_next || count == 0 || count > LEAF_PAGES - row.next_retired) return Error::Invalid;
        row.next_retired = static_cast<uint16_t>(row.next_retired + count);
        return Error::None;
    }
    [[gnu::always_inline]] auto complete_retired_after_ack(LeafHandle handle) noexcept -> Error {
        if (!matches(handle, State::Retired)) return Error::Stale;
        if (rows_[handle.slot].next_retired != LEAF_PAGES) return Error::Conflict;
        release_row(handle.slot);
        return Error::None;
    }
    [[gnu::always_inline]] auto root_has_published_owners(RootKey root) const noexcept -> bool {
        for (auto const& row : rows_)
            if (row.state != State::Free && row.state != State::Preparing && same_root(row.key.root, root)) return true;
        return false;
    }
    [[gnu::always_inline]] auto reserve_table_plan(uint16_t pages, PlanHandle& empty) noexcept -> Error {
        if (!ready_) return Error::NotReady;
        if (empty.generation != 0 || pages == 0 || pages > MAX_PLAN_TABLES) return Error::Invalid;
        if (pages > MAX_PREPARED_TABLES - table_pages_) return Error::Capacity;
        size_t free_slot = MAX_PLANS;
        for (size_t i = 0; i < MAX_PLANS; ++i)
            if (plans_[i].generation == 0) {
                free_slot = i;
                break;
            }
        if (free_slot == MAX_PLANS) return Error::Capacity;
        if (issuer_ == UINT64_MAX) return Error::GenerationExhausted;
        uint64_t const GENERATION = ++issuer_;
        plans_[free_slot] = {GENERATION, pages};
        table_pages_ = static_cast<uint16_t>(table_pages_ + pages);
        ++plan_count_;
        empty = {static_cast<uint16_t>(free_slot), GENERATION};
        return Error::None;
    }
    [[gnu::always_inline]] auto release_table_plan(PlanHandle handle) noexcept -> Error {
        if (handle.slot >= MAX_PLANS || handle.generation == 0 || plans_[handle.slot].generation != handle.generation) return Error::Stale;
        table_pages_ = static_cast<uint16_t>(table_pages_ - plans_[handle.slot].pages);
        --plan_count_;
        plans_[handle.slot] = {};
        return Error::None;
    }
    [[gnu::always_inline]] void snapshot(Snapshot& output) const noexcept {
        output = {};
        output.caps = caps_;
        output.charged_pages = charged_pages_;
        for (size_t i = 0; i < MAX_NODES; ++i) output.node_charged[i] = node_charged_[i];
        for (auto const& row : rows_) {
            switch (row.state) {
                case State::Preparing:
                    output.preparing_pages += LEAF_PAGES;
                    break;
                case State::Mapped:
                    output.mapped_pages += LEAF_PAGES;
                    break;
                case State::Splitting:
                    output.splitting_pages += LEAF_PAGES;
                    break;
                case State::Retired:
                    output.retired_pages += LEAF_PAGES;
                    break;
                case State::Free:
                    break;
            }
        }
        output.plans = plan_count_;
        output.table_pages = table_pages_;
        output.node_count = node_count_;
        output.ready = ready_;
    }

   private:
#ifdef WOS_HOST_TEST
    friend struct RegistryCoreTestAccess;
#endif
    struct PlanRow {
        uint64_t generation{};
        uint16_t pages{};
    };
    [[gnu::always_inline]] auto matches(LeafHandle handle, State state) const noexcept -> bool {
        return handle.slot < MAX_LEAVES && handle.generation != 0 && rows_[handle.slot].generation == handle.generation &&
               rows_[handle.slot].state == state;
    }
    [[gnu::always_inline]] void release_row(size_t slot) noexcept {
        charged_pages_ -= LEAF_PAGES;
        node_charged_[rows_[slot].node] -= LEAF_PAGES;
        rows_[slot] = {};
    }
    Row rows_[MAX_LEAVES]{};
    PlanRow plans_[MAX_PLANS]{};
    Caps caps_{};
    uint64_t node_charged_[MAX_NODES]{}, charged_pages_{}, issuer_{};
    uint16_t node_count_{}, plan_count_{}, table_pages_{};
    bool ready_{};
};
static_assert(sizeof(RegistryCore) <= 48 * 1024);
static_assert(std::is_trivially_copyable_v<RegistryCore> && std::is_trivially_copyable_v<Row>);
static_assert(static_cast<unsigned>(State::Free) == 0);
}  // namespace ker::mod::mm::huge
