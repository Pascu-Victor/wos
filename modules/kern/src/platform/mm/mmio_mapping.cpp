#include <algorithm>
#include <array>
#include <atomic>
#include <dev/pci_inventory_limits.hpp>
#include <dev/pci_mmio_private.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/cache_policy.hpp>
#include <platform/mm/cpu_maintenance_controller.hpp>
#include <platform/mm/cpu_maintenance_platform.hpp>
#include <platform/mm/mmio_cache.hpp>
#include <utility>

#include "mmio_mapping_platform.hpp"
#include "mmio_mapping_private.hpp"

namespace ker::mod::mm::mmio {
namespace detail {
namespace {
namespace maintenance = cpu_maintenance;
using Controller = maintenance::detail::Controller;
using Table = paging::PageTable;
using log = dbg::logger<"mmio">;
constexpr uint64_t PAGE = 4096;
// Full resources consume fixed extent metadata. Only materialized view bytes
// drive page tables and CPU tickets; WC policy remains a separate authority.
constexpr uint64_t MAX_MATERIALIZED_BYTES = 16 * 1024 * 1024;
constexpr size_t RESOURCE_WALK_BUDGET = 128;
constexpr uint64_t FRAME = 0x000ffffffffff000ULL;
constexpr uint64_t NX = uint64_t{1} << 63;
constexpr uint64_t AD = (uint64_t{1} << 5) | (uint64_t{1} << 6);
constexpr uint64_t CLOSED = uint64_t{1} << 63;
constexpr uint64_t BANK_BASE = 0xfffffc8000000000ULL;
constexpr uint64_t BANK_BYTES = uint64_t{1} << 39;
constexpr uint64_t BANK_END = BANK_BASE + BANK_BYTES;
constexpr uint64_t TABLE_FLAGS = paging::PAGE_PRESENT | paging::PAGE_WRITE | NX;
constexpr uint64_t LEAF_FLAGS = TABLE_FLAGS | paging::PAGE_PWT | paging::PAGE_PCD;
constexpr size_t RECORDS = ker::dev::pci::inventory_limits::FUNCTION_CAPACITY;
constexpr size_t HISTORY = 4096;
constexpr size_t TABLE_HASH = 131072;
constexpr size_t TABLE_LIMIT = 65536;
constexpr size_t TABLE_PROBES = 128;
constexpr uint64_t WAIT_NS = 250'000'000;
static_assert((BANK_BASE >> 39 & 511) == 505 && (TABLE_HASH & (TABLE_HASH - 1)) == 0);

struct Interval {
    uint64_t start{}, end{};
};
auto intersects(Interval a, Interval b) noexcept -> bool { return a.start < b.end && b.start < a.end; }
auto canonical(uint64_t v) noexcept -> bool { return v >> 47 == 0 || v >> 47 == 0x1ffff; }
auto virtual_interval(uint64_t start, uint64_t bytes, Interval& result) noexcept -> bool {
    if (bytes == 0 || start > UINT64_MAX - bytes || !canonical(start) || !canonical(start + bytes - 1) ||
        start >> 47 != (start + bytes - 1) >> 47)
        return false;
    result = {start, start + bytes};
    return true;
}
auto physical_interval(uint64_t start, uint64_t bytes, Interval& result) noexcept -> bool {
    if (bytes == 0 || start > UINT64_MAX - bytes || start + bytes > uint64_t{1} << 52 || start + bytes > UINT64_MAX - (PAGE - 1))
        return false;
    result = {start & ~(PAGE - 1), (start + bytes + PAGE - 1) & ~(PAGE - 1)};
    return result.start < result.end;
}
auto index(uint64_t address, unsigned level) noexcept -> size_t { return address >> (12 + (level - 1) * 9) & 511; }
auto load(const Table* table, size_t at) noexcept -> uint64_t {
    return __atomic_load_n(reinterpret_cast<const uint64_t*>(reinterpret_cast<uintptr_t>(table) + at * 8), __ATOMIC_ACQUIRE);
}
void store(Table* table, size_t at, uint64_t raw) noexcept {
    __atomic_store_n(reinterpret_cast<uint64_t*>(reinterpret_cast<uintptr_t>(table) + at * 8), raw, __ATOMIC_RELEASE);
}
auto leaf_interval(uint64_t raw, unsigned level, Interval& result) noexcept -> bool {
    if ((raw & 1) == 0 || level < 1 || level > 3 || (level != 1 && (raw & 128) == 0)) return false;
    uint64_t const BYTES = uint64_t{1} << (12 + (level - 1) * 9);
    if (level != 1 && (raw & FRAME & (BYTES - 1) & ~uint64_t{4096}) != 0) return false;
    uint64_t const START = raw & FRAME & ~(BYTES - 1);
    return physical_interval(START, BYTES, result);
}

class Pin {
    uint64_t flags_{platform::lock_pin()};

   public:
    ~Pin() { platform::unlock_pin(flags_); }
    Pin() = default;
    Pin(const Pin&) = delete;
    auto operator=(const Pin&) -> Pin& = delete;
};

enum class Boot : uint8_t { cold, disabled, recording, ready, unavailable };
struct ChildView {
    uint8_t bar{};
    uint64_t offset{}, physical{}, address{}, bytes{};
    std::atomic<Phase> phase{Phase::empty};
    std::atomic<Status> status{Status::not_ready};
    std::atomic<size_t> table_pages{};
    maintenance::Ticket initial{}, withdrawal{};
    mmio_cache::QualifiedUcRange initial_policy{}, withdrawal_policy{};
    std::atomic<uint64_t> initial_id{}, withdrawal_id{}, incarnation{};
    std::atomic<bool> timed_out{};
    bool leaves_removed{};
};

struct ResourceScan {
    uint64_t root_frame{}, hhdm{}, epoch{}, cursor{}, visits{}, steps{}, restarts{};
    size_t span{};
};

struct FunctionRecord {
    std::atomic<bool> used{};
    std::atomic<bool> operation{};
    uint64_t id{};
    ker::dev::pci::detail::ColdFunctionClaim claim{};
    std::array<Interval, 6> physical{};
    size_t spans{}, view_count{};
    std::array<ChildView, VIEW_CAPACITY> views{};
    std::atomic<Phase> phase{Phase::empty};
    std::atomic<Status> status{Status::not_ready};
    std::atomic<uint64_t> gate{CLOSED};
    std::atomic<bool> terminal{};
    // PIN-owned admission state. Early used rows own only private metadata;
    // their zero child VAs are not reservations until final linearization.
    bool resources_committed{};
    ResourceScan resource_scan{};
    bool decode_off{}, hardware_stopped{};
#ifdef WOS_SELFTEST
    bool deferred{}, resumed{};
#endif
};

class Operation {
    FunctionRecord* held_{};

   public:
    explicit Operation(FunctionRecord& record) {
        bool empty = false;
        if (record.operation.compare_exchange_strong(empty, true, std::memory_order_acq_rel, std::memory_order_acquire)) held_ = &record;
    }
    ~Operation() {
        if (held_ != nullptr) held_->operation.store(false, std::memory_order_release);
    }
    explicit operator bool() const noexcept { return held_ != nullptr; }
    Operation(const Operation&) = delete;
    auto operator=(const Operation&) -> Operation& = delete;
};

struct Walk {
    bool valid{}, present{};
    uint64_t raw{}, physical{}, remaining{};
    unsigned level{};
};
}  // namespace

class Runtime {
    std::atomic<Boot> boot_{Boot::cold};
    std::atomic<bool> requested_{};
    std::array<Interval, HISTORY> history_{};
    size_t history_count_{};
    bool history_incomplete_{};
    std::array<Interval, 4096> boot_regions_{};
    size_t boot_count_{};
    std::array<Interval, RECORDS * 6> tombstones_{};
    std::atomic<size_t> tombstone_count_{};
    std::array<std::atomic<uint64_t>, TABLE_HASH> private_frames_{};
    std::array<Interval, HISTORY> private_ranges_{};
    size_t private_ranges_count_{}, private_count_{};
    std::array<FunctionRecord, RECORDS> records_{};
    // PIN-owned scratch avoids both large bootstrap stack arrays and a
    // quadratic restart scan for every staged sibling. Never an owner.
    std::array<Interval, RECORDS * VIEW_CAPACITY + VIEW_CAPACITY> virtual_scratch_{};
    uint64_t next_id_{1}, bank_frame_{};
    uint64_t publication_epoch_{1};
    bool publication_exhausted_{};
    std::atomic<bool> bank_live_{};
#ifdef WOS_SELFTEST
    bool defer_withdrawal_{};
    bool admission_steps_{};
#endif

    static auto token(const char* text, const char* expected) noexcept -> bool {
        if (text == nullptr) return false;
        size_t begin = 0;
        for (size_t at = 0; at < 4096; ++at) {
            char const C = text[at];
            if (C != 0 && C != ' ' && C != '\t' && C != '\r' && C != '\n') continue;
            size_t count = 0;
            while (expected[count] != 0 && begin + count < at && expected[count] == text[begin + count]) ++count;
            if (expected[count] == 0 && begin + count == at) return true;
            if (C == 0) return false;
            begin = at + 1;
        }
        return false;
    }
    static auto frame_bucket(uint64_t physical) noexcept -> size_t {
        uint64_t value = physical >> 12;
        value ^= value >> 30;
        value *= 0xbf58476d1ce4e5b9ULL;
        value ^= value >> 27;
        return static_cast<size_t>(value) & (TABLE_HASH - 1);
    }
    auto private_frame(uint64_t physical) const noexcept -> bool {
        if (physical == 0 || (physical & ~FRAME) != 0) return false;
        size_t const START = frame_bucket(physical);
        for (size_t i = 0; i < TABLE_PROBES; ++i) {
            uint64_t const VALUE = private_frames_[(START + i) & (TABLE_HASH - 1)].load(std::memory_order_acquire);
            if (VALUE == physical) return true;
            if (VALUE == 0) return false;
        }
        return false;
    }
    static auto merge(std::array<Interval, HISTORY>& rows, size_t& count, Interval incoming) noexcept -> bool {
        // Existing ranges are disjoint; coalesce even adjacency. No row is
        // dropped if capacity fails, so saturation can only weaken availability.
        for (size_t i = 0; i < count;) {
            if (rows[i].end < incoming.start || incoming.end < rows[i].start) {
                ++i;
                continue;
            }
            if (rows[i].start < incoming.start) incoming.start = rows[i].start;
            if (rows[i].end > incoming.end) incoming.end = rows[i].end;
            rows[i] = rows[--count];
            i = 0;
        }
        if (count == rows.size()) return false;
        rows[count++] = incoming;
        return true;
    }
    void remember(Interval range) noexcept {
        if (!history_incomplete_ && !merge(history_, history_count_, range)) history_incomplete_ = true;
    }
    auto protected_physical(Interval range) const noexcept -> bool {
        size_t const COUNT = tombstone_count_.load(std::memory_order_acquire);
        for (size_t i = 0; i < COUNT; ++i)
            if (intersects(range, tombstones_[i])) return true;
        return false;
    }
    auto private_physical_locked(Interval range) const noexcept -> bool {
        for (size_t i = 0; i < private_ranges_count_; ++i)
            if (intersects(range, private_ranges_[i])) return true;
        return false;
    }
    auto register_table_locked(Table* table, uint64_t physical) noexcept -> bool {
        uint64_t actual = 0;
        if (private_count_ == TABLE_LIMIT || physical == 0 || (physical & ~FRAME) != 0 || private_frame(physical) ||
            !platform::table_physical(table, actual) || actual != physical || !platform::table_frame_valid(physical) ||
            protected_physical({physical, physical + PAGE}))
            return false;
        uint64_t const HHDM = platform::hhdm_offset();
        if (physical > UINT64_MAX - HHDM) return false;
        auto const ALIAS = walk_locked(platform::kernel_root(), HHDM + physical);
        uint64_t const LEAF_POLICY =
            paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_USER | paging::PAGE_PWT | paging::PAGE_PCD | NX;
        uint64_t const PAT = ALIAS.level == 1 ? uint64_t{128} : uint64_t{4096};
        cache::CpuPat inherited{};
        if (!ALIAS.valid || !ALIAS.present || ALIAS.physical != physical || ALIAS.level < 1 || ALIAS.level > 3 ||
            (ALIAS.raw & LEAF_POLICY) != (paging::PAGE_PRESENT | paging::PAGE_WRITE | NX) || (ALIAS.raw & PAT) != 0 ||
            !cache::cpu_pat(0, inherited) || !inherited.supported || cache::detail::pat_entry(inherited.raw, 0) != 6)
            return false;
        for (size_t i = 0; i < 512; ++i)
            if (load(table, i) != 0) return false;
        size_t const START = frame_bucket(physical);
        for (size_t i = 0; i < TABLE_PROBES; ++i) {
            auto& cell = private_frames_[(START + i) & (TABLE_HASH - 1)];
            if (cell.load(std::memory_order_relaxed) != 0) continue;
            if (!merge(private_ranges_, private_ranges_count_, {physical, physical + PAGE})) return false;
            cell.store(physical, std::memory_order_release);
            ++private_count_;
            return true;
        }
        return false;
    }
    auto walk_locked(const Table* root, uint64_t address, bool allow_private = false) const noexcept -> Walk {
        const Table* table = root;
        for (unsigned level = 4; level != 0; --level) {
            uint64_t physical = 0;
            if (!platform::table_physical(table, physical) || !platform::table_frame_valid(physical) ||
                (!allow_private && private_frame(physical)))
                return {};
            uint64_t const RAW = load(table, index(address, level));
            uint64_t const BYTES = uint64_t{1} << (12 + (level - 1) * 9);
            if ((RAW & 1) == 0) return {true, false, RAW, 0, BYTES - (address & (BYTES - 1)), level};
            if ((RAW & ~(FRAME | 4095 | NX)) != 0 || (level == 4 && (RAW & 128) != 0)) return {};
            if (level == 1 || (RAW & 128) != 0) {
                Interval range{};
                if (!leaf_interval(RAW, level, range)) return {};
                return {true, true, RAW, range.start + (address & (BYTES - 1)), BYTES - (address & (BYTES - 1)), level};
            }
            if (protected_physical({RAW & FRAME, (RAW & FRAME) + PAGE})) return {};
            table = platform::table_virtual(RAW & FRAME);
        }
        return {};
    }
    auto virtual_allowed_locked(const Table* root, uint64_t address, uint64_t bytes, bool unchanged_hhdm = false) const noexcept -> bool {
        Interval range{};
        if (!virtual_interval(address, bytes, range) ||
            (bank_live_.load(std::memory_order_acquire) && intersects(range, {BANK_BASE, BANK_END})))
            return false;
        uint64_t physical = 0;
        if (!platform::table_physical(root, physical) || private_frame(physical) || protected_physical({physical, physical + PAGE}))
            return false;
        uint64_t cursor = range.start;
        for (size_t visits = 0; cursor < range.end && visits < 32768; ++visits) {
            auto const LEAF = walk_locked(root, cursor);
            if (!LEAF.valid || LEAF.remaining == 0) return false;
            if (LEAF.present) {
                Interval old{};
                if (!leaf_interval(LEAF.raw, LEAF.level, old) || protected_physical(old) ||
                    (private_physical_locked(old) && !unchanged_hhdm))
                    return false;
            }
            cursor += LEAF.remaining < range.end - cursor ? LEAF.remaining : range.end - cursor;
        }
        return cursor == range.end;
    }
    auto same_table_alias_locked(const Table* root, uint64_t address, uint64_t physical, uint64_t flags) const noexcept -> bool {
        uint64_t const HHDM = platform::hhdm_offset();
        if (physical > UINT64_MAX - HHDM || address != physical + HHDM ||
            (flags & (paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_USER | paging::PAGE_PWT | paging::PAGE_PCD | NX)) !=
                TABLE_FLAGS)
            return false;
        auto const OLD = walk_locked(root, address);
        return OLD.valid && OLD.present && OLD.physical == physical &&
               (OLD.raw & (paging::PAGE_PRESENT | paging::PAGE_WRITE | paging::PAGE_USER | paging::PAGE_PWT | paging::PAGE_PCD | NX)) ==
                   TABLE_FLAGS;
    }
    auto find(uint16_t slot, uint64_t id) noexcept -> FunctionRecord* {
        if (id == 0 || slot >= records_.size()) return nullptr;
        auto& record = records_[slot];
        return record.used.load(std::memory_order_acquire) && record.id == id ? &record : nullptr;
    }
    static auto context(const Controller* scope) noexcept -> bool {
        return scope != nullptr ? scope->context_status() == maintenance::Status::ready
                                : maintenance::detail::platform::controller_context_ready();
    }
    static auto deadline(uint64_t& result) noexcept -> bool {
        uint64_t now = 0;
        if (!maintenance::detail::platform::clock_now_ns(now) || now > UINT64_MAX - WAIT_NS) return false;
        result = now + WAIT_NS;
        return true;
    }
    auto retain(FunctionRecord& record, Status status) noexcept -> Status {
        record.gate.fetch_or(CLOSED, std::memory_order_acq_rel);
        record.status.store(status, std::memory_order_release);
        return status;
    }
    void advance_epoch_locked() noexcept {
        if (publication_exhausted_) return;
        if (publication_epoch_ >= UINT64_MAX - 1) {
            publication_epoch_ = UINT64_MAX;
            publication_exhausted_ = true;
            return;
        }
        ++publication_epoch_;
    }

   public:
    auto enabled() const noexcept -> bool { return requested_.load(std::memory_order_acquire); }
    void begin_boot() noexcept {
        if (!platform::boot_root_building() || platform::kernel_root() == nullptr) return;
        Boot cold = Boot::cold;
        if (!boot_.compare_exchange_strong(cold, Boot::disabled, std::memory_order_acq_rel)) return;
        if (!token(platform::command_line(), "--typed-pci-mmio")) return;
        requested_.store(true, std::memory_order_release);
#ifdef WOS_SELFTEST
        defer_withdrawal_ = token(platform::command_line(), "--selftest-e1000-mmio-withdraw-retry");
        admission_steps_ = token(platform::command_line(), "--selftest-e1000-mmio-admission-steps") &&
                           token(platform::command_line(), "--selftest-e1000-mmio") && token(platform::command_line(), "--typed-pci-mmio");
#endif
        auto const* map = platform::boot_memory_map();
        if (map == nullptr || map->entries == nullptr || map->entry_count == 0 || map->entry_count > boot_regions_.size()) {
            boot_.store(Boot::unavailable, std::memory_order_release);
            return;
        }
        for (size_t i = 0; i < map->entry_count; ++i) {
            auto const* row = map->entries[i];
            Interval range{};
            if (row == nullptr || (row->length != 0 && !physical_interval(row->base, row->length, range))) {
                boot_.store(Boot::unavailable, std::memory_order_release);
                return;
            }
            // RESERVED alone is not RAM ownership. A retained reserved HHDM
            // alias is still rejected by real exposure history/current walks.
            // Every other descriptor, including unknown kinds, is protected.
            if (row->length != 0 && row->type != LIMINE_MEMMAP_RESERVED) boot_regions_[boot_count_++] = range;
        }
        boot_.store(Boot::recording, std::memory_order_release);
    }
    void finish_boot() noexcept {
        if (boot_.load(std::memory_order_acquire) != Boot::recording || !platform::pre_task_boot_context()) return;
        auto fail = [&] { boot_.store(Boot::unavailable, std::memory_order_release); };
        if (!platform::nx_ready() || !platform::arena_layout_safe(BANK_BASE, BANK_BYTES) ||
            !ker::dev::pci::detail::cold_inventory_allows_arena(BANK_BASE, BANK_BYTES))
            return fail();
        auto* table = platform::allocate_zeroed_table();
        uint64_t physical = 0;
        if (table == nullptr) return fail();
        bool attached = false;
        if (platform::table_physical(table, physical) && platform::ensure_table_addressable(table)) {
            Pin const PIN;
            auto* root = platform::kernel_root();
            if (root != nullptr && load(root, 505) == 0 && register_table_locked(table, physical)) {
                bank_frame_ = physical;
                store(root, 505, physical | TABLE_FLAGS);
                bank_live_.store(true, std::memory_order_release);
                attached = true;
            }
        }
        if (!attached) {
            platform::release_unpublished_table(table);
            fail();
        } else {
            platform::reload_current_root();
            boot_.store(Boot::ready, std::memory_order_release);
        }
    }
    auto generic_table(const Table* table) const noexcept -> bool {
        if (!enabled()) return true;
        uint64_t const ADDRESS = reinterpret_cast<uint64_t>(table);
        if (ADDRESS >= BANK_BASE && ADDRESS < BANK_END) return false;
        uint64_t physical = 0;
        return platform::table_physical(table, physical) && !private_frame(physical) && !protected_physical({physical, physical + PAGE});
    }
    auto generic_virtual(const Table* root, uint64_t address, uint64_t bytes) noexcept -> bool {
        if (!enabled()) return true;
        Pin const PIN;
        return virtual_allowed_locked(root, address, bytes);
    }
    auto record_mapping(const Table* root, uint64_t address, uint64_t physical, uint64_t bytes, uint64_t flags) noexcept -> bool {
        if (!enabled()) return true;
        Interval range{};
        if ((address | physical | bytes) & (PAGE - 1) || !physical_interval(physical, bytes, range)) return false;
        Pin const PIN;
        bool const UNCHANGED = bytes == PAGE && same_table_alias_locked(root, address, physical, flags);
        if (!virtual_allowed_locked(root, address, bytes, UNCHANGED) || protected_physical(range)) return false;
        if (private_physical_locked(range) && !(bytes == PAGE && same_table_alias_locked(root, address, physical, flags))) return false;
        if ((flags & paging::PAGE_PRESENT) != 0) remember(range);
        return true;
    }
    auto record_boot(uint64_t physical, uint64_t bytes) noexcept -> bool {
        if (!enabled() || bytes == 0) return true;
        Interval range{};
        if (!physical_interval(physical, bytes, range)) {
            Pin const PIN;
            history_incomplete_ = true;
            return true;
        }
        Pin const PIN;
        if (protected_physical(range) || private_physical_locked(range)) return false;
        remember(range);
        return true;
    }
    auto record_access(uint64_t physical, uint64_t bytes) noexcept -> bool {
        if (!enabled()) return true;
        Interval range{};
        if (!physical_interval(physical, bytes, range)) return false;
        Pin const PIN;
        if (protected_physical(range) || private_physical_locked(range)) return false;
        remember(range);
        return true;
    }
    auto generic_leaf(const Table* table, uint64_t address, uint64_t old_raw, uint64_t new_raw, unsigned level) noexcept -> bool {
        if (!enabled()) return true;
        Interval va{};
        uint64_t const BYTES = level >= 1 && level <= 3 ? uint64_t{1} << (12 + (level - 1) * 9) : 0;
        if (BYTES == 0 || !generic_table(table) || !virtual_interval(address & ~(BYTES - 1), BYTES, va) ||
            (bank_live_.load(std::memory_order_acquire) && intersects(va, {BANK_BASE, BANK_END})))
            return false;
        Interval old{}, incoming{};
        bool const OLD = (old_raw & 1) != 0;
        bool const NEW = (new_raw & 1) != 0;
        if ((OLD && (!leaf_interval(old_raw, level, old) || protected_physical(old))) ||
            (NEW && (!leaf_interval(new_raw, level, incoming) || protected_physical(incoming))))
            return false;
        bool const PRIVATE = (OLD && private_physical_locked(old)) || (NEW && private_physical_locked(incoming));
        if (PRIVATE) {
            uint64_t const HHDM = platform::hhdm_offset();
            if (!OLD || !NEW || (old_raw & ~AD) != (new_raw & ~AD) || old.start > UINT64_MAX - HHDM || va.start != HHDM + old.start)
                return false;
        }
        if (OLD) remember(old);
        if (NEW) remember(incoming);
        // The native caller retains this exact PIN through its actual store.
        // Earlier preflights cannot linearize a scan against that later effect.
        advance_epoch_locked();
        return true;
    }
    void note_topology() noexcept {
        if (enabled()) advance_epoch_locked();
    }

   private:
    auto collect_virtual_locked(size_t& count) noexcept -> bool {
        count = 0;
        for (const auto& record : records_) {
            if (!record.used.load(std::memory_order_acquire) || !record.resources_committed) continue;
            for (size_t i = 0; i < record.view_count; ++i) {
                auto const& view = record.views[i];
                if (view.phase.load(std::memory_order_acquire) == Phase::complete) continue;
                if (count == virtual_scratch_.size()) return false;
                virtual_scratch_[count++] = {view.address - PAGE, view.address + view.bytes + PAGE};
            }
        }
        // Fixed-capacity in-place heapsort: O(N log N), no heap allocation,
        // recursive stack, callback, or iterator-debug runtime under PIN.
        auto sift = [&](size_t root, size_t limit) {
            while (root < limit / 2) {
                size_t child = root * 2 + 1;
                if (child + 1 < limit && virtual_scratch_[child].start < virtual_scratch_[child + 1].start) ++child;
                if (virtual_scratch_[root].start >= virtual_scratch_[child].start) break;
                std::swap(virtual_scratch_[root], virtual_scratch_[child]);
                root = child;
            }
        };
        for (size_t root = count / 2; root != 0;) sift(--root, count);
        for (size_t end = count; end > 1;) {
            std::swap(virtual_scratch_[0], virtual_scratch_[--end]);
            sift(0, end);
        }
        return true;
    }
    auto reserve_virtual_locked(uint64_t bytes, uint64_t& output, size_t& count) noexcept -> bool {
        if (bytes == 0 || (bytes & (PAGE - 1)) != 0 || bytes > BANK_BYTES - 2 * PAGE || count == virtual_scratch_.size()) return false;
        uint64_t start = BANK_BASE + PAGE;
        for (size_t i = 0; i < count; ++i) {
            if (start > BANK_END - PAGE || bytes > BANK_END - PAGE - start) return false;
            auto const occupied = virtual_scratch_[i];
            if (occupied.end <= start - PAGE) continue;
            if (start + bytes + PAGE <= occupied.start) break;
            start = occupied.end + PAGE;
        }
        if (start > BANK_END - PAGE || bytes > BANK_END - PAGE - start) return false;
        Interval const reserved{start - PAGE, start + bytes + PAGE};
        size_t at = count;
        while (at != 0 && virtual_scratch_[at - 1].start > reserved.start) {
            virtual_scratch_[at] = virtual_scratch_[at - 1];
            --at;
        }
        virtual_scratch_[at] = reserved;
        ++count;
        output = start;
        return true;
    }
    auto validate_claim_plan(const ker::dev::pci::detail::ColdFunctionClaim& claim, std::array<Interval, 6>& ranges) const noexcept
        -> Status {
        if (claim.owner_id == 0 || claim.claim_epoch == 0 || claim.inventory_generation == 0 || claim.function_index >= RECORDS ||
            claim.span_count == 0 || claim.span_count > claim.spans.size() || claim.bar0_bytes == 0 ||
            ((claim.bar0_physical | claim.bar0_bytes) & (PAGE - 1)) != 0 || claim.view_count == 0 || claim.view_count > VIEW_CAPACITY)
            return Status::invalid_claim;
        bool bar0_found = false;
        for (size_t i = 0; i < claim.span_count; ++i) {
            auto const& bar = claim.bars[i];
            Interval exact{};
            if (bar.index >= 6 || (bar.attributes & ~uint8_t{0x0c}) != 0 || ((bar.attributes & 4U) != 0 && bar.index == 5) ||
                bar.physical == 0 || bar.bytes < 16 || (bar.bytes & (bar.bytes - 1)) != 0 || (bar.physical & (bar.bytes - 1)) != 0 ||
                ((claim.spans[i].start | claim.spans[i].bytes) & (PAGE - 1)) != 0 || !physical_interval(bar.physical, bar.bytes, exact) ||
                !physical_interval(claim.spans[i].start, claim.spans[i].bytes, ranges[i]) || exact.start != ranges[i].start ||
                exact.end != ranges[i].end || ((bar.attributes & 4U) == 0 && bar.physical + bar.bytes > (uint64_t{1} << 32)))
                return Status::invalid_claim;
            if (bar.index == 0) {
                if (bar.physical != claim.bar0_physical || bar.bytes != claim.bar0_bytes) return Status::invalid_claim;
                bar0_found = true;
            }
            for (size_t j = 0; j < i; ++j) {
                auto const& prior = claim.bars[j];
                if (intersects(ranges[i], ranges[j]) || bar.index == prior.index ||
                    ((prior.attributes & 4U) != 0 && bar.index == prior.index + 1) ||
                    ((bar.attributes & 4U) != 0 && prior.index == bar.index + 1))
                    return Status::invalid_claim;
            }
        }
        if (!bar0_found) return Status::invalid_claim;
        std::array<Interval, VIEW_CAPACITY> view_ranges{};
        uint64_t materialized_bytes = 0;
        for (size_t i = 0; i < claim.view_count; ++i) {
            auto const& view = claim.views[i];
            if (view.bytes == 0 || ((view.offset | view.physical | view.bytes) & (PAGE - 1)) != 0 ||
                !physical_interval(view.physical, view.bytes, view_ranges[i]))
                return Status::invalid_claim;
            if (view.bytes > MAX_MATERIALIZED_BYTES - materialized_bytes) return Status::capacity;
            materialized_bytes += view.bytes;
            bool found = false;
            for (size_t b = 0; b < claim.span_count; ++b) {
                auto const& bar = claim.bars[b];
                if (bar.index != view.bar) continue;
                found = view.offset <= bar.bytes && view.bytes <= bar.bytes - view.offset && view.physical == bar.physical + view.offset;
                break;
            }
            if (!found) return Status::invalid_claim;
            for (size_t j = 0; j < i; ++j)
                if (intersects(view_ranges[i], view_ranges[j])) return Status::invalid_claim;
        }
        return resource_geometry(ranges, claim.span_count, platform::hhdm_offset());
    }
    static auto resource_geometry(const std::array<Interval, 6>& ranges, size_t count, uint64_t hhdm) noexcept -> Status {
        for (size_t i = 0; i < count; ++i) {
            Interval alias{};
            if (ranges[i].start > UINT64_MAX - hhdm || !virtual_interval(hhdm + ranges[i].start, ranges[i].end - ranges[i].start, alias) ||
                intersects(alias, {BANK_BASE, BANK_END}))
                return Status::alias_present;
        }
        return Status::ready;
    }
    auto physical_exclusions_locked(const std::array<Interval, 6>& ranges, size_t count) const noexcept -> Status {
        if (boot_.load(std::memory_order_acquire) != Boot::ready || !bank_live_.load(std::memory_order_acquire))
            return Status::arena_unavailable;
        if (history_incomplete_) return Status::history_incomplete;
        if (publication_exhausted_) return Status::generation_exhausted;
        for (size_t i = 0; i < count; ++i) {
            for (size_t j = 0; j < boot_count_; ++j)
                if (intersects(ranges[i], boot_regions_[j])) return Status::boot_memory;
            for (size_t j = 0; j < history_count_; ++j)
                if (intersects(ranges[i], history_[j])) return Status::history_exposed;
            if (protected_physical(ranges[i]) || private_physical_locked(ranges[i])) return Status::foreign_overlap;
        }
        return Status::ready;
    }
    auto resource_identity_locked(uint64_t& frame, uint64_t& hhdm) const noexcept -> Status {
        auto const* root = platform::kernel_root();
        if (root == nullptr || !platform::table_physical(root, frame) || frame == 0 || !platform::table_frame_valid(frame) ||
            private_frame(frame) || !bank_matches_locked())
            return Status::bad_hierarchy;
        hhdm = platform::hhdm_offset();
        return Status::ready;
    }
    auto reserve_pending_claim(ker::dev::pci::detail::ColdFunctionClaim&& claim,
                               std::array<mmio_cache::QualifiedUcRange, VIEW_CAPACITY>&& policies, const std::array<Interval, 6>& ranges,
                               PreparedMapping& output) noexcept -> Status {
        uint64_t incarnation = 0;
        for (size_t i = 0; i < claim.view_count; ++i) {
            auto const& policy = policies[i];
            if (policy.empty() || policy.physical() != claim.views[i].physical || policy.bytes() != claim.views[i].bytes ||
                (i != 0 && policy.incarnation() != incarnation))
                return Status::invalid_claim;
            incarnation = policy.incarnation();
        }
        Pin const PIN;
        auto const EXCLUDED = physical_exclusions_locked(ranges, claim.span_count);
        if (EXCLUDED != Status::ready) return EXCLUDED;
        uint64_t frame = 0, hhdm = 0;
        auto const IDENTITY = resource_identity_locked(frame, hhdm);
        if (IDENTITY != Status::ready) return IDENTITY;
        auto const GEOMETRY = resource_geometry(ranges, claim.span_count, hhdm);
        if (GEOMETRY != Status::ready) return GEOMETRY;
        uint16_t slot = UINT16_MAX;
        for (size_t i = 0; i < records_.size(); ++i) {
            if (records_[i].used.load(std::memory_order_acquire)) {
                if (records_[i].claim.function_index == claim.function_index) return Status::invalid_claim;
            } else if (slot == UINT16_MAX)
                slot = static_cast<uint16_t>(i);
        }
        if (slot == UINT16_MAX) return Status::capacity;
        if (next_id_ == UINT64_MAX) return Status::generation_exhausted;
        auto& record = records_[slot];
        record.id = next_id_++;
        record.spans = claim.span_count;
        record.physical = ranges;
        record.view_count = claim.view_count;
        record.resource_scan = {frame, hhdm, publication_epoch_, ranges[0].start};
        for (size_t i = 0; i < record.view_count; ++i) {
            auto const& plan = claim.views[i];
            auto& view = record.views[i];
            view.bar = plan.bar;
            view.offset = plan.offset;
            view.physical = plan.physical;
            view.bytes = plan.bytes;
            view.incarnation.store(policies[i].incarnation(), std::memory_order_relaxed);
            view.initial_policy = std::move(policies[i]);
            view.status.store(Status::pending_retained, std::memory_order_relaxed);
        }
        record.claim = std::move(claim);
        record.phase.store(Phase::physical_pending, std::memory_order_relaxed);
        record.status.store(Status::pending_retained, std::memory_order_relaxed);
        record.used.store(true, std::memory_order_release);
        output.slot_ = slot;
        output.id_ = record.id;
        return Status::ready;
    }
    auto reject_resources_locked(FunctionRecord& record, Status status) noexcept -> Status {
        record.phase.store(Phase::retained, std::memory_order_release);
        return retain(record, status);
    }
    auto restart_resources_locked(FunctionRecord& record) noexcept -> Status {
        auto const EXCLUDED = physical_exclusions_locked(record.physical, record.spans);
        if (EXCLUDED != Status::ready) return reject_resources_locked(record, EXCLUDED);
        auto& scan = record.resource_scan;
        if (scan.restarts == UINT64_MAX) return reject_resources_locked(record, Status::generation_exhausted);
        ++scan.restarts;
        scan.epoch = publication_epoch_;
        scan.span = 0;
        scan.cursor = record.physical[0].start;
        return Status::ready;
    }
    auto commit_resources_locked(FunctionRecord& record) noexcept -> Status {
        auto& scan = record.resource_scan;
        uint64_t frame = 0, hhdm = 0;
        auto const IDENTITY = resource_identity_locked(frame, hhdm);
        if (IDENTITY != Status::ready || frame != scan.root_frame || hhdm != scan.hhdm)
            return reject_resources_locked(record, Status::bad_hierarchy);
        if (publication_exhausted_) return reject_resources_locked(record, Status::generation_exhausted);
        if (publication_epoch_ != scan.epoch) {
            auto const RESTART = restart_resources_locked(record);
            return RESTART == Status::ready ? Status::pending_retained : RESTART;
        }
        auto const EXCLUDED = physical_exclusions_locked(record.physical, record.spans);
        if (EXCLUDED != Status::ready) return reject_resources_locked(record, EXCLUDED);
        size_t const TOMBS = tombstone_count_.load(std::memory_order_relaxed);
        if (record.spans > tombstones_.size() - TOMBS) return reject_resources_locked(record, Status::capacity);
        size_t virtual_count = 0;
        if (!collect_virtual_locked(virtual_count)) return reject_resources_locked(record, Status::capacity);
        std::array<Interval, VIEW_CAPACITY> virtual_ranges{};
        for (size_t i = 0; i < record.view_count; ++i) {
            uint64_t address = 0;
            if (!reserve_virtual_locked(record.views[i].bytes, address, virtual_count))
                return reject_resources_locked(record, Status::capacity);
            virtual_ranges[i] = {address, address + record.views[i].bytes};
        }
        // This same PIN spans the final root/epoch/history check, every VA
        // assignment and publication of ALL full-resource exclusion spans.
        for (size_t i = 0; i < record.view_count; ++i) {
            auto& view = record.views[i];
            view.address = virtual_ranges[i].start;
            view.phase.store(Phase::reserved, std::memory_order_release);
        }
        for (size_t i = 0; i < record.spans; ++i) tombstones_[TOMBS + i] = record.physical[i];
        tombstone_count_.store(TOMBS + record.spans, std::memory_order_release);
        record.resources_committed = true;
        record.phase.store(Phase::reserved, std::memory_order_release);
        return Status::ready;
    }
    auto advance_resource_scan_locked(FunctionRecord& record, size_t budget, size_t& step_visits) noexcept -> Status {
        if (record.resources_committed || record.phase.load(std::memory_order_acquire) != Phase::physical_pending)
            return Status::failed_retained;
        auto& scan = record.resource_scan;
        if (scan.steps == UINT64_MAX || publication_exhausted_) return reject_resources_locked(record, Status::generation_exhausted);
        ++scan.steps;
        uint64_t frame = 0, hhdm = 0;
        auto const IDENTITY = resource_identity_locked(frame, hhdm);
        if (IDENTITY != Status::ready || frame != scan.root_frame || hhdm != scan.hhdm)
            return reject_resources_locked(record, Status::bad_hierarchy);
        if (scan.epoch != publication_epoch_) {
            auto const RESTART = restart_resources_locked(record);
            if (RESTART != Status::ready) return RESTART;
        }
        while (step_visits < budget && scan.span < record.spans) {
            auto const range = record.physical[scan.span];
            if (scan.cursor < range.start || scan.cursor >= range.end) return reject_resources_locked(record, Status::bad_hierarchy);
            if (scan.visits == UINT64_MAX) return reject_resources_locked(record, Status::generation_exhausted);
            ++scan.visits;
            ++step_visits;
            auto const WALK = walk_locked(platform::kernel_root(), scan.hhdm + scan.cursor);
            if (!WALK.valid || WALK.remaining == 0) return reject_resources_locked(record, Status::bad_hierarchy);
            if (WALK.present) return reject_resources_locked(record, Status::alias_present);
            auto const STEP = std::min<uint64_t>(WALK.remaining, range.end - scan.cursor);
            scan.cursor += STEP;
            if (scan.cursor == range.end) {
                ++scan.span;
                if (scan.span < record.spans) scan.cursor = record.physical[scan.span].start;
            }
        }
        return scan.span == record.spans ? commit_resources_locked(record) : Status::pending_retained;
    }
    auto advance_resource_scan(FunctionRecord& record) noexcept -> Status {
        size_t budget = RESOURCE_WALK_BUDGET;
#ifdef WOS_SELFTEST
        if (admission_steps_) budget = 1;
#endif
        size_t step_visits = 0;
        Status status = Status::failed_retained;
        ResourceScan observed{};
        bool committed = false;
        {
            Pin const PIN;
            status = advance_resource_scan_locked(record, budget, step_visits);
            record.status.store(status, std::memory_order_release);
            observed = record.resource_scan;
            committed = record.resources_committed;
        }
        // Operation still owns this record, but PIN never spans diagnostics.
        if (status == Status::ready) {
            log::info(
                "MMIO_RESOURCE_COMMITTED mapping=%lu spans=%zu steps=%lu visits=%lu restarts=%lu epoch=%lu root=%lx hhdm=%lx views=%zu",
                record.id, record.spans, observed.steps, observed.visits, observed.restarts, observed.epoch, observed.root_frame,
                observed.hhdm, record.view_count);
        }
#ifdef WOS_SELFTEST
        else if (admission_steps_) {
            log::info(
                "MMIO_RESOURCE_PROGRESS mapping=%lu status=%s steps=%lu visits=%lu step_visits=%zu restarts=%lu epoch=%lu root=%lx "
                "hhdm=%lx span=%zu cursor=%lx spans=%zu committed=%u budget=%zu",
                record.id, status_name(status), observed.steps, observed.visits, step_visits, observed.restarts, observed.epoch,
                observed.root_frame, observed.hhdm, observed.span, observed.cursor, record.spans, committed ? 1U : 0U, budget);
        }
#else
        (void)committed;
#endif
        return status;
    }
    auto bank_matches_locked() const noexcept -> bool {
        auto* root = platform::kernel_root();
        return bank_live_.load(std::memory_order_acquire) && root != nullptr && (load(root, 505) & ~AD) == (bank_frame_ | TABLE_FLAGS) &&
               private_frame(bank_frame_);
    }
    auto child(Table* parent, size_t at, ChildView& record, Table*& output) noexcept -> Status {
        Table* fresh = nullptr;
        uint64_t physical = 0;
        {
            Pin const PIN;
            uint64_t const RAW = load(parent, at);
            if (RAW != 0) {
                if ((RAW & ~AD & ~FRAME) != TABLE_FLAGS || !private_frame(RAW & FRAME)) return Status::bad_hierarchy;
                output = platform::table_virtual(RAW & FRAME);
                return output != nullptr ? Status::ready : Status::bad_hierarchy;
            }
            if (private_count_ == TABLE_LIMIT) return Status::capacity;
        }
        // Allocator/addressability work can fault or reclaim; no PIN/COW/config
        // lock spans it. Only an exact zero fresh frame can be attached below.
        fresh = platform::allocate_zeroed_table();
        if (fresh == nullptr) return Status::allocation_failed;
        if (!platform::table_physical(fresh, physical) || !platform::ensure_table_addressable(fresh)) {
            platform::release_unpublished_table(fresh);
            return Status::allocation_failed;
        }
        bool attached = false;
        Status status = Status::ready;
        {
            Pin const PIN;
            uint64_t const RAW = load(parent, at);
            if (RAW == 0) {
                if (!register_table_locked(fresh, physical))
                    status = Status::capacity;
                else {
                    store(parent, at, physical | TABLE_FLAGS);
                    record.table_pages.fetch_add(1, std::memory_order_relaxed);
                    output = fresh;
                    attached = true;
                }
            } else if ((RAW & ~AD & ~FRAME) != TABLE_FLAGS || !private_frame(RAW & FRAME))
                status = Status::bad_hierarchy;
            else
                output = platform::table_virtual(RAW & FRAME);
        }
        if (!attached) platform::release_unpublished_table(fresh);
        return output != nullptr ? status : status == Status::ready ? Status::bad_hierarchy : status;
    }
    auto prepare_paths(ChildView& record) noexcept -> Status {
        uint64_t const FIRST = record.address & ~((uint64_t{1} << 21) - 1);
        uint64_t const END = record.address + record.bytes;
        for (uint64_t address = FIRST; address < END; address += uint64_t{1} << 21) {
            {
                Pin const PIN;
                if (!bank_matches_locked()) return Status::bad_hierarchy;
            }
            auto* pml3 = platform::table_virtual(bank_frame_);
            Table* pml2 = nullptr;
            Table* pml1 = nullptr;
            auto status = child(pml3, index(address, 3), record, pml2);
            if (status != Status::ready) return status;
            status = child(pml2, index(address, 2), record, pml1);
            if (status != Status::ready) return status;
        }
        return Status::ready;
    }
    auto private_leaf_locked(uint64_t address, Table*& leaf) const noexcept -> bool {
        if (!bank_matches_locked() || address < BANK_BASE || address >= BANK_END) return false;
        Table* table = platform::table_virtual(bank_frame_);
        for (unsigned level = 3; level > 1; --level) {
            if (table == nullptr) return false;
            uint64_t const RAW = load(table, index(address, level));
            if ((RAW & ~AD & ~FRAME) != TABLE_FLAGS || !private_frame(RAW & FRAME)) return false;
            table = platform::table_virtual(RAW & FRAME);
        }
        leaf = table;
        return leaf != nullptr;
    }
    auto leaves(ChildView& record, bool remove, bool verify_empty = false) noexcept -> Status {
        // Record exclusivity + permanently private paths exclude software
        // writers between short PIN batches. Validate the whole range before
        // the first leaf effect; hardware A/D is the only permitted drift.
        constexpr uint64_t BATCH = 512 * PAGE;
        for (uint64_t first = 0; first < record.bytes; first += BATCH) {
            uint64_t const END = record.bytes - first < BATCH ? record.bytes : first + BATCH;
            Pin const PIN;
            for (uint64_t offset = first; offset < END; offset += PAGE) {
                Table* table = nullptr;
                if (!private_leaf_locked(record.address + offset, table)) return Status::bad_hierarchy;
                uint64_t const RAW = load(table, index(record.address + offset, 1));
                uint64_t const EXPECTED = remove ? (record.physical + offset) | LEAF_FLAGS : 0;
                if (verify_empty ? RAW != 0 : (RAW & ~AD) != EXPECTED) return Status::bad_hierarchy;
            }
        }
        if (verify_empty) return Status::ready;
        for (uint64_t first = 0; first < record.bytes; first += BATCH) {
            uint64_t const END = record.bytes - first < BATCH ? record.bytes : first + BATCH;
            Pin const PIN;
            for (uint64_t offset = first; offset < END; offset += PAGE) {
                Table* table = nullptr;
                if (!private_leaf_locked(record.address + offset, table)) __builtin_trap();
                uint64_t const EXPECTED = remove ? (record.physical + offset) | LEAF_FLAGS : 0;
                if ((load(table, index(record.address + offset, 1)) & ~AD) != EXPECTED) __builtin_trap();
                store(table, index(record.address + offset, 1), remove ? 0 : (record.physical + offset) | LEAF_FLAGS);
            }
        }
        return Status::ready;
    }
    static void emit(const FunctionRecord& record, size_t view_index, const maintenance::Report& receipt, bool withdrawal) {
        log::info(
            "MMIO_MAINTENANCE stage=%s mapping=%lu view=%zu ticket=%lu status=%s targets=%lx tlb=%lx cache=%lx readers=%lu closed=%u "
            "timeout=%u",
            withdrawal ? "withdrawal" : "initial", record.id, view_index, receipt.id, maintenance::status_name(receipt.status),
            receipt.targets, receipt.translation_completed, receipt.cache_completed, receipt.readers, receipt.admission_closed ? 1U : 0U,
            receipt.timed_out_ever ? 1U : 0U);
        for (size_t i = 0; i < receipt.cpus.size(); ++i) {
            if ((receipt.targets & (uint64_t{1} << i)) == 0) continue;
            auto const& cpu = receipt.cpus[i];
            log::info(
                "MMIO_MAINTENANCE_CPU stage=%s mapping=%lu view=%zu ticket=%lu cpu=%zu apic=%u tlb=%lu cache=%lu tlb_apic=%u cache_apic=%u "
                "tlb_status=%s cache_status=%s tlb_cr3=%lx cache_cr3=%lx",
                withdrawal ? "withdrawal" : "initial", record.id, view_index, receipt.id, i, cpu.target_apic, cpu.translation_completed,
                cpu.cache_completed, cpu.translation_apic, cpu.cache_apic, mmio_cache::status_name(cpu.translation_status),
                mmio_cache::status_name(cpu.cache_status), cpu.translation_cr3, cpu.cache_cr3);
        }
    }
    auto maintain(FunctionRecord& record, ChildView& view, size_t view_index, const Controller* scope, bool withdrawal) -> Status {
        if (!context(scope)) return Status::invalid_context;
        auto& ticket = withdrawal ? view.withdrawal : view.initial;
        auto& policy = withdrawal ? view.withdrawal_policy : view.initial_policy;
        uint64_t until = 0;
        if (!deadline(until)) return Status::clock_unavailable;
        if (ticket.empty()) {
            if (policy.empty() && mmio_cache::qualify_uc_range(view.physical, view.bytes, policy) != mmio_cache::Status::ready)
                return Status::not_ready;
            if (policy.incarnation() != view.incarnation.load(std::memory_order_acquire)) return Status::failed_retained;
            auto const KIND = withdrawal ? maintenance::Kind::translations : maintenance::Kind::translations_then_cache;
            auto const PREPARED = scope != nullptr ? maintenance::detail::prepare(*scope, std::move(policy), KIND, ticket)
                                                   : maintenance::prepare(std::move(policy), KIND, ticket);
            if (PREPARED != maintenance::Status::ready) return Status::pending_retained;
            (withdrawal ? view.withdrawal_id : view.initial_id).store(ticket.id(), std::memory_order_release);
#ifdef WOS_SELFTEST
            if (withdrawal && view_index == 0 && defer_withdrawal_ && !record.deferred && scope != nullptr) {
                auto const SERVICE = maintenance::service_observation();
                size_t target = 63;
                while (target != 0 && (SERVICE.expected_mask & (uint64_t{1} << target)) == 0) --target;
                if (target == 0 || maintenance::detail::diagnostic_defer(*scope, ticket, maintenance::Phase::translations, target) !=
                                       maintenance::Status::ready)
                    return Status::failed_retained;
                record.deferred = true;
            }
#endif
        }
        {
            auto const BEFORE = scope != nullptr ? maintenance::detail::observe(*scope, ticket) : maintenance::observe(ticket);
            if (BEFORE.id != ticket.id()) return Status::failed_retained;
            if (BEFORE.phase == maintenance::Phase::prepared) {
                auto const PUBLISHED = scope != nullptr ? maintenance::detail::publish(*scope, ticket) : maintenance::publish(ticket);
                if (PUBLISHED != maintenance::Status::pending) return Status::pending_retained;
            }
        }
        auto const RECEIPT =
            scope != nullptr ? maintenance::detail::wait_until(*scope, ticket, until) : maintenance::wait_until(ticket, until);
        emit(record, view_index, RECEIPT, withdrawal);
        if (RECEIPT.status == maintenance::Status::complete && RECEIPT.admission_closed && RECEIPT.readers == 0) return Status::ready;
        if (RECEIPT.timed_out_ever) view.timed_out.store(true, std::memory_order_release);
#ifdef WOS_SELFTEST
        if (withdrawal && view_index == 0 && scope != nullptr && record.deferred && !record.resumed &&
            RECEIPT.status == maintenance::Status::timeout_retained) {
            // Resume only the original first-child action; no copied report or
            // sibling completion can acknowledge this CPU on its behalf.
            if (maintenance::detail::diagnostic_resume(*scope, ticket, maintenance::Phase::translations) == maintenance::Status::ready)
                record.resumed = true;
        }
#endif
        return RECEIPT.status == maintenance::Status::failed_retained ? Status::failed_retained : Status::pending_retained;
    }
    auto release_ticket(maintenance::Ticket& ticket, const Controller* scope) -> bool {
        return (scope != nullptr ? maintenance::detail::release_complete(*scope, ticket) : maintenance::release_complete(ticket)) ==
               maintenance::Status::complete;
    }
    auto prepare_view(FunctionRecord& record, ChildView& view, size_t view_index, const Controller& scope) -> Status {
        auto phase = view.phase.load(std::memory_order_acquire);
        if (phase == Phase::leaves_ready)
            return view.initial.empty() || release_ticket(view.initial, &scope) ? Status::ready : Status::pending_retained;
        if (phase != Phase::reserved && phase != Phase::cache_pending) return Status::failed_retained;
        if (phase == Phase::reserved) {
            auto const PATHS = prepare_paths(view);
            if (PATHS != Status::ready) return PATHS;
            view.phase.store(Phase::cache_pending, std::memory_order_release);
        }
        auto const ROUND = maintain(record, view, view_index, &scope, false);
        if (ROUND != Status::ready) return ROUND;
        // Re-issue only through the real immutable registry and revalidate the
        // actual publishing CPU; no copied completion/report adopts a token.
        mmio_cache::QualifiedUcRange final_policy;
        if (mmio_cache::qualify_uc_range(view.physical, view.bytes, final_policy) != mmio_cache::Status::ready ||
            final_policy.incarnation() != view.incarnation.load(std::memory_order_acquire) ||
            mmio_cache::revalidate_current_cpu(final_policy) != mmio_cache::Status::ready || !context(&scope))
            return Status::failed_retained;
        auto const written = leaves(view, false);
        if (written != Status::ready) return written;
        view.phase.store(Phase::leaves_ready, std::memory_order_release);
        if (!release_ticket(view.initial, &scope)) return Status::pending_retained;
        view.status.store(Status::ready, std::memory_order_release);
        return Status::ready;
    }
    auto prepare_record(FunctionRecord& record, const Controller& scope) -> Status {
        if (!context(&scope)) return retain(record, Status::invalid_context);
        auto phase = record.phase.load(std::memory_order_acquire);
        if (phase == Phase::physical_pending) {
            auto const RESOURCE = advance_resource_scan(record);
            if (RESOURCE != Status::ready) return retain(record, RESOURCE);
            phase = record.phase.load(std::memory_order_acquire);
        }
        if (phase == Phase::leaves_ready) return Status::ready;
        if (phase != Phase::reserved && phase != Phase::cache_pending) return Status::failed_retained;
        record.phase.store(Phase::cache_pending, std::memory_order_release);
        for (size_t i = 0; i < record.view_count; ++i) {
            auto& view = record.views[i];
            auto const STATUS = prepare_view(record, view, i, scope);
            view.status.store(STATUS, std::memory_order_release);
            if (STATUS != Status::ready) return retain(record, STATUS);
        }
        record.phase.store(Phase::leaves_ready, std::memory_order_release);
        record.status.store(Status::ready, std::memory_order_release);
        return Status::ready;
    }

   public:
    auto prepare_pci(ker::dev::pci::detail::ColdFunctionClaim&& claim, const Controller& scope, PreparedMapping& output) -> Status {
        if (!output.empty()) return Status::output_not_empty;
        if (!enabled()) return Status::disabled;
        if (!context(&scope)) return Status::invalid_context;
        std::array<Interval, 6> ranges{};
        auto const VALIDATED = validate_claim_plan(claim, ranges);
        if (VALIDATED != Status::ready) return VALIDATED;
        uint64_t until = 0;
        if (!deadline(until)) return Status::clock_unavailable;
        if (maintenance::detail::await_ready(scope, until) != maintenance::Status::ready) return Status::not_ready;
        std::array<mmio_cache::QualifiedUcRange, VIEW_CAPACITY> policies{};
        for (size_t i = 0; i < claim.view_count; ++i)
            if (mmio_cache::qualify_uc_range(claim.views[i].physical, claim.views[i].bytes, policies[i]) != mmio_cache::Status::ready)
                return Status::not_ready;
        auto const RESERVED = reserve_pending_claim(std::move(claim), std::move(policies), ranges, output);
        if (RESERVED != Status::ready) return RESERVED;
        return resume(output, scope);
    }
    auto resume(PreparedMapping& prepared, const Controller& scope) -> Status {
        auto* record = find(prepared.slot_, prepared.id_);
        if (record == nullptr) return Status::invalid_mapping;
        Operation const OP(*record);
        return OP ? prepare_record(*record, scope) : Status::busy;
    }
    auto publish(PreparedMapping& prepared, const Controller& scope, Mapping& output) -> Status {
        if (!output.empty()) return Status::output_not_empty;
        auto* record = find(prepared.slot_, prepared.id_);
        if (record == nullptr) return Status::invalid_mapping;
        Operation const OP(*record);
        if (!OP) return Status::busy;
        if (!context(&scope) || record->phase.load(std::memory_order_acquire) != Phase::leaves_ready) return Status::invalid_context;
        auto const PCI = ker::dev::pci::detail::validate_cold_activation(record->claim);
        if (PCI != Status::ready) return retain(*record, PCI);
        // One parent owns the whole immutable set. No sibling can escape while
        // another is still preparing, and generic writers cannot enter paths.
        for (size_t i = 0; i < record->view_count; ++i) {
            auto& view = record->views[i];
            if (view.phase.load(std::memory_order_acquire) != Phase::leaves_ready) return retain(*record, Status::failed_retained);
            for (uint64_t begin = 0; begin < view.bytes;) {
                uint64_t const end = begin + std::min<uint64_t>(view.bytes - begin, 512 * PAGE);
                Pin const PIN;
                for (uint64_t offset = begin; offset < end; offset += PAGE) {
                    Table* table = nullptr;
                    if (!private_leaf_locked(view.address + offset, table) ||
                        (load(table, index(view.address + offset, 1)) & ~AD) != ((view.physical + offset) | LEAF_FLAGS))
                        return retain(*record, Status::bad_hierarchy);
                }
                begin = end;
            }
        }
        {
            Pin const PIN;
            for (size_t i = 0; i < record->view_count; ++i) {
                record->views[i].phase.store(Phase::active, std::memory_order_release);
                record->views[i].status.store(Status::ready, std::memory_order_release);
            }
            record->phase.store(Phase::active, std::memory_order_release);
            record->gate.store(0, std::memory_order_release);
            record->status.store(Status::ready, std::memory_order_release);
            output.slot_ = prepared.slot_;
            output.id_ = prepared.id_;
            prepared.slot_ = UINT16_MAX;
            prepared.id_ = 0;
        }
        auto const& first = record->views[0];
        log::info("MMIO_PUBLISHED mapping=%lu physical=%lx virtual=%lx bytes=%lu spans=%zu initial_ticket=%lu views=%zu", record->id,
                  first.physical, first.address, first.bytes, record->spans, first.initial_id.load(std::memory_order_acquire),
                  record->view_count);
        for (size_t i = 0; i < record->view_count; ++i) {
            auto const& view = record->views[i];
            log::info(
                "MMIO_VIEW_PUBLISHED mapping=%lu view=%zu bar=%u bar_offset=%lx physical=%lx virtual=%lx bytes=%lu initial_ticket=%lu",
                record->id, i, static_cast<unsigned>(view.bar), view.offset, view.physical, view.address, view.bytes,
                view.initial_id.load(std::memory_order_acquire));
        }
        return Status::ready;
    }

    auto borrow(uint16_t slot, uint64_t id, bool terminal) noexcept -> Borrow {
        auto* record = find(slot, id);
        if (record == nullptr) return {};
        if (terminal) {
            Pin const PIN;
            auto const PHASE = record->phase.load(std::memory_order_acquire);
            if ((PHASE != Phase::active && PHASE != Phase::closed) || record->gate.load(std::memory_order_acquire) != CLOSED) return {};
            bool empty = false;
            if (!record->terminal.compare_exchange_strong(empty, true, std::memory_order_acq_rel, std::memory_order_acquire)) return {};
            return Borrow{slot, id, Borrow::Kind::terminal};
        }
        uint64_t gate = record->gate.load(std::memory_order_acquire);
        for (unsigned attempt = 0; attempt < 16 && (gate & CLOSED) == 0 && gate != CLOSED - 1; ++attempt) {
            if (record->gate.compare_exchange_weak(gate, gate + 1, std::memory_order_acq_rel, std::memory_order_acquire))
                return Borrow{slot, id, Borrow::Kind::normal};
        }
        return {};
    }
    auto valid(const Borrow& borrow) noexcept -> bool {
        auto* record = find(borrow.slot_, borrow.id_);
        if (record == nullptr) return false;
        auto const PHASE = record->phase.load(std::memory_order_acquire);
        if (PHASE != Phase::active && PHASE != Phase::closed) return false;
        if (borrow.kind_ == Borrow::Kind::terminal)
            return record->terminal.load(std::memory_order_acquire) && record->gate.load(std::memory_order_acquire) == CLOSED;
        return borrow.kind_ == Borrow::Kind::normal && (record->gate.load(std::memory_order_acquire) & ~CLOSED) != 0;
    }
    void drop(Borrow& borrow) noexcept {
        auto* record = find(borrow.slot_, borrow.id_);
        auto const KIND = borrow.kind_;
        borrow.slot_ = UINT16_MAX;
        borrow.id_ = 0;
        borrow.kind_ = Borrow::Kind::empty;
        if (record == nullptr) return;
        if (KIND == Borrow::Kind::normal)
            record->gate.fetch_sub(1, std::memory_order_release);
        else if (KIND == Borrow::Kind::terminal)
            record->terminal.store(false, std::memory_order_release);
        // The release above is the borrow's final access to its retained row.
    }
    auto register_address(const Borrow& borrow, size_t view_index, size_t offset) noexcept -> uint64_t {
        if (!valid(borrow)) __builtin_trap();
        auto* record = find(borrow.slot_, borrow.id_);
        if (view_index >= record->view_count) __builtin_trap();
        auto const& view = record->views[view_index];
        if ((offset & 3) != 0 || view.bytes < 4 || offset > view.bytes - 4) __builtin_trap();
        return view.address + offset;
    }
    void close(uint16_t slot, uint64_t id) noexcept {
        auto* record = find(slot, id);
        if (record == nullptr) return;
        record->gate.fetch_or(CLOSED, std::memory_order_acq_rel);
        Phase active = Phase::active;
        (void)record->phase.compare_exchange_strong(active, Phase::closed, std::memory_order_acq_rel, std::memory_order_acquire);
        for (size_t i = 0; i < record->view_count; ++i) {
            active = Phase::active;
            (void)record->views[i].phase.compare_exchange_strong(active, Phase::closed, std::memory_order_acq_rel,
                                                                 std::memory_order_acquire);
        }
    }
    auto drained(uint16_t slot, uint64_t id) noexcept -> bool {
        auto* record = find(slot, id);
        return record != nullptr && record->gate.load(std::memory_order_acquire) == CLOSED &&
               !record->terminal.load(std::memory_order_acquire);
    }
    auto report(uint16_t slot, uint64_t id) noexcept -> Report {
        auto* record = find(slot, id);
        if (record == nullptr) return {};
        Report result{};
        result.status = record->status.load(std::memory_order_acquire);
        result.phase = record->phase.load(std::memory_order_acquire);
        result.id = record->id;
        uint64_t const GATE = record->gate.load(std::memory_order_acquire);
        result.normal_readers = GATE & ~CLOSED;
        result.admission_closed = (GATE & CLOSED) != 0;
        result.terminal_active = record->terminal.load(std::memory_order_acquire);
        result.physical_spans = record->spans;
        result.view_count = record->view_count;
        for (size_t i = 0; i < record->view_count; ++i) {
            auto const& view = record->views[i];
            auto& observed = result.views[i];
            observed.status = view.status.load(std::memory_order_acquire);
            observed.phase = view.phase.load(std::memory_order_acquire);
            observed.bar = view.bar;
            observed.bar_offset = view.offset;
            observed.physical = view.physical;
            observed.virtual_base = view.address;
            observed.bytes = view.bytes;
            observed.table_pages = view.table_pages.load(std::memory_order_acquire);
            observed.initial_ticket = view.initial_id.load(std::memory_order_acquire);
            observed.withdrawal_ticket = view.withdrawal_id.load(std::memory_order_acquire);
            observed.cohort_incarnation = view.incarnation.load(std::memory_order_acquire);
            observed.timed_out_ever = view.timed_out.load(std::memory_order_acquire);
            result.table_pages += observed.table_pages;
            result.timed_out_ever |= observed.timed_out_ever;
        }
        auto const& first = result.views[0];
        result.physical = first.physical;
        result.virtual_base = first.virtual_base;
        result.bytes = first.bytes;
        result.initial_ticket = first.initial_ticket;
        result.withdrawal_ticket = first.withdrawal_ticket;
        result.cohort_incarnation = first.cohort_incarnation;
        // Resource scan observations are immutable before the public Mapping
        // is issued; they grant no root, physical or completion authority.
        result.resource_steps = record->resource_scan.steps;
        result.resource_visits = record->resource_scan.visits;
        result.resource_restarts = record->resource_scan.restarts;
        result.resource_epoch = record->resource_scan.epoch;
        result.resource_root_frame = record->resource_scan.root_frame;
        return result;
    }
    void abandon(PreparedMapping& prepared) noexcept {
        auto* record = find(prepared.slot_, prepared.id_);
        prepared.slot_ = UINT16_MAX;
        prepared.id_ = 0;
        if (record != nullptr) {
            record->phase.store(Phase::retained, std::memory_order_release);
            (void)retain(*record, Status::failed_retained);
        }
    }
    auto withdraw(uint16_t slot, uint64_t id, const Controller* scope, const QuiescedMapping& proof) -> Status {
        if (proof.slot_ != slot || proof.id_ != id || id == 0) return Status::not_quiesced;
        auto* record = find(slot, id);
        if (record == nullptr) return Status::invalid_mapping;
        if (!context(scope)) return Status::invalid_context;
        Operation const OP(*record);
        if (!OP) return Status::busy;
        {
            Pin const PIN;
            if (record->phase.load(std::memory_order_acquire) == Phase::complete) return Status::complete;
            if (record->gate.load(std::memory_order_acquire) != CLOSED || record->terminal.load(std::memory_order_acquire))
                return Status::borrow_busy;
            auto const PHASE = record->phase.load(std::memory_order_acquire);
            if (PHASE != Phase::closed && PHASE != Phase::withdrawal_pending) return Status::not_quiesced;
            record->hardware_stopped = true;
            // One terminal lease covers every child. This same PIN prevents a
            // terminal acquisition once any child can begin withdrawal.
            record->phase.store(Phase::withdrawal_pending, std::memory_order_release);
        }
        if (!record->decode_off) {
            auto const PCI = ker::dev::pci::detail::disable_cold_decode(record->claim);
            if (PCI != Status::ready && PCI != Status::complete) return retain(*record, PCI);
            record->decode_off = true;
        }
        for (size_t i = 0; i < record->view_count; ++i) {
            auto& view = record->views[i];
            if (view.phase.load(std::memory_order_acquire) == Phase::complete) continue;
            view.phase.store(Phase::withdrawal_pending, std::memory_order_release);
            if (!view.leaves_removed) {
                auto const REMOVED = leaves(view, true);
                if (REMOVED != Status::ready) {
                    view.status.store(REMOVED, std::memory_order_release);
                    return retain(*record, REMOVED);
                }
                view.leaves_removed = true;
                log::info("MMIO_WITHDRAWN mapping=%lu view=%zu virtual=%lx bytes=%lu normal=0 terminal=0 decode_off=1", record->id, i,
                          view.address, view.bytes);
            }
            auto const ROUND = maintain(*record, view, i, scope, true);
            if (ROUND != Status::ready) {
                view.status.store(ROUND, std::memory_order_release);
                return retain(*record, ROUND);
            }
            if (!release_ticket(view.withdrawal, scope)) {
                view.status.store(Status::pending_retained, std::memory_order_release);
                return retain(*record, Status::pending_retained);
            }
            auto const EMPTY = leaves(view, false, true);
            if (EMPTY != Status::ready) {
                view.status.store(EMPTY, std::memory_order_release);
                return retain(*record, EMPTY);
            }
            view.phase.store(Phase::complete, std::memory_order_release);
            view.status.store(Status::complete, std::memory_order_release);
            log::info(
                "MMIO_VIEW_COMPLETE mapping=%lu view=%zu virtual=%lx bytes=%lu withdrawal_ticket=%lu leaves=0 normal=0 terminal=0 "
                "physical_retained=1 tables_retained=1",
                record->id, i, view.address, view.bytes, view.withdrawal_id.load(std::memory_order_acquire));
        }
        record->phase.store(Phase::complete, std::memory_order_release);
        record->status.store(Status::complete, std::memory_order_release);
        auto const& first = record->views[0];
        log::info(
            "MMIO_COMPLETE mapping=%lu virtual=%lx bytes=%lu withdrawal_ticket=%lu leaves=0 normal=0 terminal=0 physical_retained=1 "
            "tables_retained=1 views=%zu",
            record->id, first.address, first.bytes, first.withdrawal_id.load(std::memory_order_acquire), record->view_count);
        return Status::complete;
    }
};

namespace {
constinit Runtime runtime{};
}

auto prepare_from_pci(ker::dev::pci::detail::ColdFunctionClaim&& claim, const Controller& controller, PreparedMapping& output) -> Status {
    return runtime.prepare_pci(std::move(claim), controller, output);
}
auto resume_prepared(PreparedMapping& prepared, const Controller& controller) -> Status { return runtime.resume(prepared, controller); }
auto publish_from_pci(PreparedMapping& prepared, const Controller& controller, Mapping& output) -> Status {
    return runtime.publish(prepared, controller, output);
}
PreparedMapping::~PreparedMapping() { runtime.abandon(*this); }
PreparedMapping::PreparedMapping(PreparedMapping&& other) noexcept
    : id_(std::exchange(other.id_, 0)), slot_(std::exchange(other.slot_, UINT16_MAX)) {}
auto PreparedMapping::operator=(PreparedMapping&& other) noexcept -> PreparedMapping& {
    if (this != &other) {
        runtime.abandon(*this);
        id_ = std::exchange(other.id_, 0);
        slot_ = std::exchange(other.slot_, UINT16_MAX);
    }
    return *this;
}
void begin_boot_history() noexcept { runtime.begin_boot(); }
void finish_boot_arena() noexcept { runtime.finish_boot(); }
auto generic_virtual_allowed(const Table* root, uint64_t address, uint64_t bytes) noexcept -> bool {
    return runtime.generic_virtual(root, address, bytes);
}
auto record_generic_mapping(const Table* root, uint64_t address, uint64_t physical, uint64_t bytes, uint64_t flags) noexcept -> bool {
    return runtime.record_mapping(root, address, physical, bytes, flags);
}
auto record_boot_physical_range(uint64_t address, uint64_t bytes) noexcept -> bool { return runtime.record_boot(address, bytes); }
auto record_generic_physical_access(uint64_t address, uint64_t bytes) noexcept -> bool { return runtime.record_access(address, bytes); }
auto generic_table_allowed(const Table* table) noexcept -> bool { return runtime.generic_table(table); }
auto generic_root_allowed_locked(const Table* root) noexcept -> bool { return runtime.generic_table(root); }
void note_topology_publication_locked() noexcept { runtime.note_topology(); }
auto generic_leaf_allowed_locked(const Table* table, uint64_t address, uint64_t old_raw, uint64_t new_raw, unsigned level) noexcept
    -> bool {
    return runtime.generic_leaf(table, address, old_raw, new_raw, level);
}
}  // namespace detail

Borrow::Borrow(uint16_t slot, uint64_t id, Kind kind) noexcept : id_(id), slot_(slot), kind_(kind) {}
Borrow::~Borrow() { reset(); }
Borrow::Borrow(Borrow&& other) noexcept
    : id_(std::exchange(other.id_, 0)), slot_(std::exchange(other.slot_, UINT16_MAX)), kind_(std::exchange(other.kind_, Kind::empty)) {}
auto Borrow::operator=(Borrow&& other) noexcept -> Borrow& {
    if (this != &other) {
        reset();
        id_ = std::exchange(other.id_, 0);
        slot_ = std::exchange(other.slot_, UINT16_MAX);
        kind_ = std::exchange(other.kind_, Kind::empty);
    }
    return *this;
}
auto Borrow::valid() const noexcept -> bool { return detail::runtime.valid(*this); }
void Borrow::reset() noexcept { detail::runtime.drop(*this); }
auto Borrow::read32(size_t offset) const noexcept -> uint32_t { return read32(0, offset); }
void Borrow::write32(size_t offset, uint32_t value) const noexcept { write32(0, offset, value); }
auto Borrow::read32(size_t view_index, size_t offset) const noexcept -> uint32_t {
    return detail::platform::read_register32(detail::runtime.register_address(*this, view_index, offset));
}
void Borrow::write32(size_t view_index, size_t offset, uint32_t value) const noexcept {
    detail::platform::write_register32(detail::runtime.register_address(*this, view_index, offset), value);
}
Mapping::Mapping(uint16_t slot, uint64_t id) noexcept : id_(id), slot_(slot) {}
Mapping::~Mapping() { close(); }
Mapping::Mapping(Mapping&& other) noexcept : id_(std::exchange(other.id_, 0)), slot_(std::exchange(other.slot_, UINT16_MAX)) {}
auto Mapping::operator=(Mapping&& other) noexcept -> Mapping& {
    if (this != &other) {
        close();
        id_ = std::exchange(other.id_, 0);
        slot_ = std::exchange(other.slot_, UINT16_MAX);
    }
    return *this;
}
auto Mapping::borrow() const noexcept -> Borrow { return detail::runtime.borrow(slot_, id_, false); }
void Mapping::close() noexcept { detail::runtime.close(slot_, id_); }
auto Mapping::borrows_drained() const noexcept -> bool { return detail::runtime.drained(slot_, id_); }
auto Mapping::terminal_borrow() const noexcept -> Borrow { return detail::runtime.borrow(slot_, id_, true); }
auto Mapping::report() const noexcept -> Report { return detail::runtime.report(slot_, id_); }
auto Mapping::withdraw(const QuiescedMapping& proof) -> Status { return detail::runtime.withdraw(slot_, id_, nullptr, proof); }
auto Mapping::withdraw(const cpu_maintenance::detail::Controller& controller, const QuiescedMapping& proof) -> Status {
    return detail::runtime.withdraw(slot_, id_, &controller, proof);
}
QuiescedMapping::QuiescedMapping(const Mapping& mapping) noexcept : id_(mapping.id_), slot_(mapping.slot_) {}
auto enabled() noexcept -> bool { return detail::runtime.enabled(); }
auto status_name(Status status) noexcept -> const char* {
    static constexpr std::array names{"ready",
                                      "disabled",
                                      "complete",
                                      "busy",
                                      "not_ready",
                                      "invalid_context",
                                      "invalid_claim",
                                      "output_not_empty",
                                      "capacity",
                                      "generation_exhausted",
                                      "incomplete_inventory",
                                      "unsupported_profile",
                                      "stale_geometry",
                                      "foreign_overlap",
                                      "legacy_exposed",
                                      "initial_bme_active",
                                      "irq_active",
                                      "dma_present",
                                      "bad_probe",
                                      "decode_readback",
                                      "boot_memory",
                                      "alias_present",
                                      "history_exposed",
                                      "history_incomplete",
                                      "arena_unavailable",
                                      "allocation_failed",
                                      "bad_hierarchy",
                                      "pending_retained",
                                      "failed_retained",
                                      "invalid_mapping",
                                      "borrow_busy",
                                      "not_quiesced",
                                      "clock_unavailable"};
    auto const index = static_cast<size_t>(status);
    return index < names.size() ? names[index] : "invalid_status";
}
}  // namespace ker::mod::mm::mmio
