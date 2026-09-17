#include "tmpfs.hpp"

#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/reclaim.hpp>
#include <platform/mm/swap.hpp>
#include <platform/sched/file_growth.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/security/file_policy.hpp>
#include <platform/sys/mutex.hpp>
#include <util/smallvec.hpp>
#include <utility>
#include <vfs/file.hpp>
#include <vfs/mount.hpp>
#include <vfs/stat.hpp>

#include "vfs/file_operations.hpp"
#include "vfs/vfs.hpp"

namespace {
auto kstrcmp(const char* a, const char* b) -> int {
    if ((a == nullptr) || (b == nullptr)) {
        if (a == b) {
            return 0;
        }
        if (a != nullptr) {
            return 1;
        }
        return -1;
    }
    while ((*a != 0) && (*b != 0)) {
        if (*a != *b) {
            return static_cast<unsigned char>(*a) - static_cast<unsigned char>(*b);
        }
        ++a;
        ++b;
    }
    return static_cast<unsigned char>(*a) - static_cast<unsigned char>(*b);
}

// Copy a string into a fixed-size array, ensuring null termination
void copy_name(std::array<char, ker::vfs::tmpfs::TMPFS_NAME_MAX>& dst, const char* src) {
    size_t i = 0;
    if (src != nullptr) {
        while (src[i] != '\0' && i < ker::vfs::tmpfs::TMPFS_NAME_MAX - 1) {
            dst.at(i) = src[i];
            i++;
        }
    }
    dst.at(i) = '\0';
}
}  // namespace

namespace ker::vfs::tmpfs {

constexpr size_t DEFAULT_TMPFS_BLOCK_SIZE = 4096;
static_assert(DEFAULT_TMPFS_BLOCK_SIZE == ker::mod::mm::paging::PAGE_SIZE);
constexpr size_t INITIAL_CHILDREN_CAPACITY = 8;
constexpr int O_CREAT = 0100;  // octal = 64 decimal = 0x40 hex
constexpr int O_EXCL = 0200;
constexpr uint64_t TMPFS_MIN_FREE_RESERVE_BYTES = 16ULL * 1024ULL * 1024ULL;
constexpr uint64_t TMPFS_MAX_FREE_RESERVE_BYTES = 256ULL * 1024ULL * 1024ULL;
constexpr uint64_t TMPFS_FREE_RESERVE_DIVISOR = 8;
constexpr uint64_t NS_PER_SEC = 1000000000ULL;

namespace {
TmpNode* root_node = nullptr;                   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Mutex tmpfs_lock;                // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Mutex tmpfs_node_registry_lock;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::util::SmallVec<TmpNode*, 64> tmpfs_nodes;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> tmpfs_resident_pages{0};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> tmpfs_reclaim_node_cursor{0};
std::atomic<bool> tmpfs_shrinker_registered{false};
std::atomic<bool> tmpfs_swap_consumer_registered{false};
uint64_t tmpfs_node_registry_generation{};             // Guarded by tmpfs_node_registry_lock.
std::atomic<uint64_t> tmpfs_next_node_incarnation{1};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> tmpfs_search_authority_generation_value{1};
std::atomic<uint64_t> tmpfs_namespace_generation_value{1};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
}  // namespace

// --- Internal helpers ---

namespace {
auto current_timespec() -> Timespec {
    uint64_t const NOW_NS = ker::mod::time::get_epoch_ns();
    return Timespec{
        .tv_sec = static_cast<int64_t>(NOW_NS / NS_PER_SEC),
        .tv_nsec = static_cast<int64_t>(NOW_NS % NS_PER_SEC),
    };
}

void stamp_new_node(TmpNode* node) {
    if (node == nullptr) {
        return;
    }
    Timespec const NOW = current_timespec();
    node->atime = NOW;
    node->mtime = NOW;
    node->ctime = NOW;
}

void assign_node_incarnation(TmpNode* node) {
    if (node == nullptr) {
        return;
    }
    node->incarnation = tmpfs_next_node_incarnation.fetch_add(1, std::memory_order_relaxed);
    node->directory_generation = 1;
}

void touch_modified(TmpNode* node) {
    if (node == nullptr) {
        return;
    }
    Timespec const NOW = current_timespec();
    node->mtime = NOW;
    node->ctime = NOW;
}

auto tmpfs_free_reserve_bytes() -> uint64_t {
    uint64_t const TOTAL = ker::mod::mm::phys::get_total_mem_bytes();
    if (TOTAL == 0) {
        return 0;
    }

    uint64_t reserve = TOTAL / TMPFS_FREE_RESERVE_DIVISOR;
    reserve = std::max(reserve, TMPFS_MIN_FREE_RESERVE_BYTES);
    reserve = std::min(reserve, TMPFS_MAX_FREE_RESERVE_BYTES);
    reserve = std::min(reserve, TOTAL / 2);
    return reserve;
}

auto tmpfs_can_allocate_page() -> bool {
    if (ker::mod::mm::phys::get_total_mem_bytes() == 0) {
        return true;
    }
    return ker::mod::mm::phys::page_alloc_can_satisfy(ker::mod::mm::paging::PAGE_SIZE, tmpfs_free_reserve_bytes());
}

auto page_count_for_size(size_t size, size_t* out_pages) -> bool {
    if (out_pages == nullptr) {
        return false;
    }
    if (size == 0) {
        *out_pages = 0;
        return true;
    }
    if (size > static_cast<size_t>(-1) - (DEFAULT_TMPFS_BLOCK_SIZE - 1)) {
        return false;
    }
    *out_pages = (size + DEFAULT_TMPFS_BLOCK_SIZE - 1) / DEFAULT_TMPFS_BLOCK_SIZE;
    return true;
}

auto parse_decimal(const char*& p, uint64_t* out) -> bool {
    if (out == nullptr || *p < '0' || *p > '9') {
        return false;
    }
    uint64_t value = 0;
    while (*p >= '0' && *p <= '9') {
        auto const DIGIT = static_cast<uint64_t>(*p - '0');
        if (value > (UINT64_MAX - DIGIT) / 10) {
            return false;
        }
        value = (value * 10) + DIGIT;
        ++p;
    }
    *out = value;
    return true;
}

auto parse_size_value(const char* text, size_t* out_bytes) -> bool {
    if (text == nullptr || out_bytes == nullptr) {
        return false;
    }
    const char* p = text;
    uint64_t value = 0;
    if (!parse_decimal(p, &value)) {
        return false;
    }

    uint64_t multiplier = 1;
    if (*p == 'K' || *p == 'k') {
        multiplier = 1024ULL;
        ++p;
    } else if (*p == 'M' || *p == 'm') {
        multiplier = 1024ULL * 1024ULL;
        ++p;
    } else if (*p == 'G' || *p == 'g') {
        multiplier = 1024ULL * 1024ULL * 1024ULL;
        ++p;
    } else if (*p == '%') {
        uint64_t const TOTAL = ker::mod::mm::phys::get_total_mem_bytes();
        if (value > 100) {
            return false;
        }
        *out_bytes = static_cast<size_t>((TOTAL / 100ULL) * value);
        ++p;
        return *p == '\0';
    }

    if (*p != '\0' || value > UINT64_MAX / multiplier) {
        return false;
    }
    uint64_t const BYTES = value * multiplier;
    if (BYTES > static_cast<uint64_t>(static_cast<size_t>(-1))) {
        return false;
    }
    *out_bytes = static_cast<size_t>(BYTES);
    return true;
}

auto parse_mount_options(const char* options, bool root_compat, size_t* out_max_bytes) -> int {
    if (out_max_bytes == nullptr) {
        return -EINVAL;
    }
    *out_max_bytes = root_compat ? 0 : static_cast<size_t>(ker::mod::mm::phys::get_total_mem_bytes() / 2);
    if (options == nullptr || options[0] == '\0') {
        return 0;
    }

    const char* cursor = options;
    while (*cursor != '\0') {
        while (*cursor == ',' || *cursor == ' ' || *cursor == '\t') {
            ++cursor;
        }
        if (*cursor == '\0') {
            break;
        }
        const char* const START = cursor;
        while (*cursor != '\0' && *cursor != ',') {
            ++cursor;
        }
        auto const LEN = static_cast<size_t>(cursor - START);
        if (LEN == 0 || (LEN == 8 && std::strncmp(START, "defaults", LEN) == 0)) {
            continue;
        }
        constexpr size_t SIZE_PREFIX_LEN = 5;
        if (LEN > SIZE_PREFIX_LEN && std::strncmp(START, "size=", SIZE_PREFIX_LEN) == 0) {
            std::array<char, 32> value{};
            size_t const VALUE_LEN = LEN - SIZE_PREFIX_LEN;
            if (VALUE_LEN == 0 || VALUE_LEN >= value.size()) {
                return -EINVAL;
            }
            std::memcpy(value.data(), START + SIZE_PREFIX_LEN, VALUE_LEN);
            value[VALUE_LEN] = '\0';
            size_t bytes = 0;
            if (!parse_size_value(value.data(), &bytes)) {
                return -EINVAL;
            }
            *out_max_bytes = bytes;
            continue;
        }
        return -EINVAL;
    }
    return 0;
}

void register_tmp_node(TmpNode* node) {
    if (node == nullptr) {
        return;
    }
    ker::mod::sys::MutexGuard guard(tmpfs_node_registry_lock);
    node->reclaim_registered = tmpfs_nodes.push_back(node);
    if (node->reclaim_registered && node->resident_pages != 0) {
        tmpfs_resident_pages.fetch_add(node->resident_pages, std::memory_order_relaxed);
    }
    if (node->reclaim_registered) {
        tmpfs_node_registry_generation++;
    }
}

void unregister_tmp_node(TmpNode* node) {
    if (node == nullptr) {
        return;
    }
    ker::mod::sys::MutexGuard guard(tmpfs_node_registry_lock);
    if (node->reclaim_registered) {
        static_cast<void>(tmpfs_nodes.remove(node));
        tmpfs_resident_pages.fetch_sub(node->resident_pages, std::memory_order_relaxed);
        node->reclaim_registered = false;
        tmpfs_node_registry_generation++;
    }
}

auto mount_try_charge(TmpNode* node) -> bool {
    if (node == nullptr || node->mount == nullptr || node->mount->max_bytes == 0) {
        return true;
    }
    auto* mount = node->mount;
    ker::mod::sys::MutexGuard guard(mount->accounting_lock);
    if (mount->used_bytes > mount->max_bytes || mount->max_bytes - mount->used_bytes < DEFAULT_TMPFS_BLOCK_SIZE) {
        return false;
    }
    mount->used_bytes += DEFAULT_TMPFS_BLOCK_SIZE;
    return true;
}

void mount_uncharge(TmpNode* node) {
    if (node == nullptr || node->mount == nullptr || node->mount->max_bytes == 0) {
        return;
    }
    auto* mount = node->mount;
    ker::mod::sys::MutexGuard guard(mount->accounting_lock);
    mount->used_bytes = (mount->used_bytes >= DEFAULT_TMPFS_BLOCK_SIZE) ? mount->used_bytes - DEFAULT_TMPFS_BLOCK_SIZE : 0;
}

void node_set_mount_recursive(TmpNode* node, TmpfsMount* mount) {
    if (node == nullptr) {
        return;
    }
    node->mount = mount;
    for (size_t i = 0; i < node->children_count; ++i) {
        node_set_mount_recursive(node->children[i], mount);
    }
}

auto ensure_page_descriptors(TmpNode* node, size_t required_pages) -> bool {
    if (node == nullptr || required_pages <= node->page_count) {
        return true;
    }
    auto* pages = new TmpPage[required_pages];
    if (pages == nullptr) {
        return false;
    }
    for (size_t i = 0; i < node->page_count; ++i) {
        pages[i] = node->pages[i];
    }
    delete[] node->pages;
    node->pages = pages;
    node->page_count = required_pages;
    return true;
}

void release_page_locked(TmpNode* node, TmpPage& page, bool uncharge) {
    if (page.state == TmpPageState::RESIDENT) {
        if (page.data != nullptr) {
            ker::mod::mm::phys::page_free(page.data);
        }
        if (node != nullptr && node->resident_pages != 0) {
            node->resident_pages--;
            if (node->reclaim_registered) {
                tmpfs_resident_pages.fetch_sub(1, std::memory_order_relaxed);
            }
        }
    } else if (page.state == TmpPageState::SWAPPED && ker::mod::mm::swap::slot_valid(page.swap_slot)) {
        static_cast<void>(ker::mod::mm::swap::free_slot(page.swap_slot));
    }
    page.state = TmpPageState::HOLE;
    page.data = nullptr;
    page.swap_slot = ker::mod::mm::swap::invalid_slot();
    if (uncharge && node != nullptr && node->charged_pages != 0) {
        node->charged_pages--;
        mount_uncharge(node);
    }
}

auto evict_page_locked(TmpNode* node, size_t index) -> int {
    if (node == nullptr || index >= node->page_count) {
        return -EINVAL;
    }
    TmpPage& page = node->pages[index];
    if (page.state != TmpPageState::RESIDENT || page.data == nullptr) {
        return -EAGAIN;
    }
    ker::mod::mm::swap::SwapSlot slot{};
    int ret = ker::mod::mm::swap::allocate_slot(&slot);
    if (ret < 0) {
        return ret;
    }
    ret = ker::mod::mm::swap::write_slot(slot, page.data);
    if (ret < 0) {
        static_cast<void>(ker::mod::mm::swap::free_slot(slot));
        return ret;
    }
    ker::mod::mm::phys::page_free(page.data);
    page.data = nullptr;
    page.swap_slot = slot;
    page.state = TmpPageState::SWAPPED;
    if (node->resident_pages != 0) {
        node->resident_pages--;
        if (node->reclaim_registered) {
            tmpfs_resident_pages.fetch_sub(1, std::memory_order_relaxed);
        }
    }
    return 0;
}

struct TmpfsReclaimResult {
    size_t reclaimed{};
    size_t scanned{};
};

auto reclaim_from_node_locked_bounded(TmpNode* node, size_t target_pages, size_t scan_budget, size_t skip_index = static_cast<size_t>(-1))
    -> TmpfsReclaimResult {
    TmpfsReclaimResult result{};
    if (node == nullptr || target_pages == 0 || scan_budget == 0 || node->page_count == 0 || !ker::mod::mm::swap::swap_available()) {
        return result;
    }
    size_t const START = node->reclaim_cursor % node->page_count;
    size_t const MAX_SCAN = std::min(scan_budget, node->page_count);
    while (result.scanned < MAX_SCAN && result.reclaimed < target_pages) {
        size_t const INDEX = (START + result.scanned) % node->page_count;
        result.scanned++;
        if (INDEX == skip_index) {
            continue;
        }
        if (evict_page_locked(node, INDEX) == 0) {
            result.reclaimed++;
        }
    }
    node->reclaim_cursor = (START + result.scanned) % node->page_count;
    return result;
}

auto reclaim_from_node_locked(TmpNode* node, size_t target_pages, size_t skip_index = static_cast<size_t>(-1)) -> size_t {
    return reclaim_from_node_locked_bounded(node, target_pages, SIZE_MAX, skip_index).reclaimed;
}

auto tmpfs_reclaim_bounded(size_t target_pages, size_t scan_budget) -> TmpfsReclaimResult {
    TmpfsReclaimResult result{};
    if (target_pages == 0 || scan_budget == 0 || !ker::mod::mm::swap::swap_available()) {
        return result;
    }

    size_t const NODE_BUDGET = scan_budget;
    uint64_t const START = tmpfs_reclaim_node_cursor.fetch_add(NODE_BUDGET, std::memory_order_relaxed);
    size_t nodes_examined = 0;
    while (nodes_examined < NODE_BUDGET && result.scanned < scan_budget && result.reclaimed < target_pages) {
        TmpNode* node = nullptr;
        tmpfs_node_registry_lock.lock();
        size_t const NODE_COUNT = tmpfs_nodes.size();
        if (NODE_COUNT != 0) {
            size_t const INDEX = static_cast<size_t>((START + nodes_examined) % NODE_COUNT);
            node = tmpfs_nodes.at(INDEX);
            if (node != nullptr && !node->io_lock.try_lock()) {
                node = nullptr;
            }
        }
        nodes_examined++;
        tmpfs_node_registry_lock.unlock();

        if (node == nullptr) {
            continue;
        }
        TmpNode* canonical = tmpfs_canonical_node(node);
        if (canonical == node && node->type == TmpNodeType::FILE) {
            TmpfsReclaimResult const NODE_RESULT =
                reclaim_from_node_locked_bounded(node, target_pages - result.reclaimed, scan_budget - result.scanned);
            result.reclaimed += NODE_RESULT.reclaimed;
            result.scanned += NODE_RESULT.scanned;
        }
        node->io_lock.unlock();
    }
    return result;
}

auto tmpfs_reclaim_count(void*, const ker::mod::mm::reclaim::ReclaimRequest&) -> ker::mod::mm::reclaim::ReclaimCount {
    uint64_t const RESIDENT = tmpfs_resident_pages.load(std::memory_order_relaxed);
    ker::mod::mm::swap::SwapStats stats{};
    ker::mod::mm::swap::get_stats(&stats);
    uint64_t const FREE_SLOTS = stats.free_bytes / DEFAULT_TMPFS_BLOCK_SIZE;
    uint64_t const RECLAIMABLE = std::min(RESIDENT, FREE_SLOTS);
    return {.reclaimable = RECLAIMABLE, .unreclaimable = RESIDENT - RECLAIMABLE};
}

auto tmpfs_reclaim_scan(void*, const ker::mod::mm::reclaim::ReclaimRequest& request) -> ker::mod::mm::reclaim::ReclaimScanResult {
    size_t const PAGE_BUDGET = static_cast<size_t>(std::min<uint64_t>(request.budget_units, SIZE_MAX));
    size_t const SCAN_BUDGET = static_cast<size_t>(std::min<uint64_t>(request.scan_budget_units, SIZE_MAX));
    TmpfsReclaimResult const RESULT = tmpfs_reclaim_bounded(PAGE_BUDGET, SCAN_BUDGET);
    return {
        .scanned = RESULT.scanned,
        .reclaimed = RESULT.reclaimed,
        .has_more = tmpfs_resident_pages.load(std::memory_order_relaxed) != 0,
    };
}

void register_tmpfs_shrinker() {
    bool expected = false;
    if (!tmpfs_shrinker_registered.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
    }
    ker::mod::mm::reclaim::Shrinker const SHRINKER{
        .name = "tmpfs",
        .unit = ker::mod::mm::reclaim::ReclaimUnit::PAGES,
        .scan_unit = ker::mod::mm::reclaim::ReclaimUnit::PAGES,
        .rank = 7,
        .min_priority = ker::mod::mm::reclaim::ReclaimPriority::CRITICAL,
        .capabilities =
            ker::mod::mm::reclaim::RECLAIM_MAY_BLOCK | ker::mod::mm::reclaim::RECLAIM_MAY_IO | ker::mod::mm::reclaim::RECLAIM_MAY_ALLOCATE,
        .max_batch_units = 32,
        .max_scan_units = 4096,
        .count = tmpfs_reclaim_count,
        .scan = tmpfs_reclaim_scan,
    };
    if (!ker::mod::mm::reclaim::register_shrinker(SHRINKER)) {
        tmpfs_shrinker_registered.store(false, std::memory_order_release);
    }
}

auto allocate_resident_page(TmpNode* node, size_t page_index) -> void* {
    if (!tmpfs_can_allocate_page()) {
        static_cast<void>(reclaim_from_node_locked(node, 1, page_index));
    }
    void* page =
        ker::mod::mm::phys::page_alloc_may_fail(ker::mod::mm::PhysicalPageOwner::TMPFS_DATA, DEFAULT_TMPFS_BLOCK_SIZE, "tmpfs_page");
    if (page != nullptr) {
        return page;
    }
    static_cast<void>(reclaim_from_node_locked(node, 4, page_index));
    return ker::mod::mm::phys::page_alloc_may_fail(ker::mod::mm::PhysicalPageOwner::TMPFS_DATA, DEFAULT_TMPFS_BLOCK_SIZE, "tmpfs_page");
}

auto ensure_page_resident_locked(TmpNode* node, size_t page_index, TmpPage** out_page) -> int {
    if (node == nullptr || out_page == nullptr) {
        return -EINVAL;
    }
    if (!ensure_page_descriptors(node, page_index + 1)) {
        return -ENOMEM;
    }
    TmpPage& page = node->pages[page_index];
    if (page.state == TmpPageState::RESIDENT) {
        *out_page = &page;
        return 0;
    }
    if (page.state == TmpPageState::HOLE && !mount_try_charge(node)) {
        return -ENOSPC;
    }
    void* data = allocate_resident_page(node, page_index);
    if (data == nullptr) {
        if (page.state == TmpPageState::HOLE) {
            mount_uncharge(node);
        }
        return -ENOSPC;
    }
    if (page.state == TmpPageState::SWAPPED) {
        int const RET = ker::mod::mm::swap::read_slot(page.swap_slot, data);
        if (RET < 0) {
            ker::mod::mm::phys::page_free(data);
            return RET;
        }
        int const FREE_RET = ker::mod::mm::swap::free_slot(page.swap_slot);
        if (FREE_RET < 0) {
            ker::mod::mm::phys::page_free(data);
            return FREE_RET;
        }
    } else {
        std::memset(data, 0, DEFAULT_TMPFS_BLOCK_SIZE);
        node->charged_pages++;
    }
    page.state = TmpPageState::RESIDENT;
    page.data = data;
    page.swap_slot = ker::mod::mm::swap::invalid_slot();
    node->resident_pages++;
    if (node->reclaim_registered) {
        tmpfs_resident_pages.fetch_add(1, std::memory_order_relaxed);
    }
    *out_page = &page;
    return 0;
}

auto migrate_node_swap_area_locked(TmpNode* node, uint32_t area_id) -> int {
    if (node == nullptr || node->type != TmpNodeType::FILE || tmpfs_canonical_node(node) != node) {
        return 0;
    }
    for (size_t i = 0; i < node->page_count; ++i) {
        TmpPage& page = node->pages[i];
        if (page.state != TmpPageState::SWAPPED || !ker::mod::mm::swap::slot_valid(page.swap_slot) || page.swap_slot.area != area_id) {
            continue;
        }
        TmpPage* resident = nullptr;
        int const RET = ensure_page_resident_locked(node, i, &resident);
        if (RET < 0) {
            return RET;
        }
    }
    return 0;
}

auto migrate_tmpfs_swap_area(void*, uint32_t area_id) -> int {
    for (;;) {
        size_t index = 0;
        bool saw_busy_node = false;
        uint64_t registry_generation = 0;
        {
            ker::mod::sys::MutexGuard guard(tmpfs_node_registry_lock);
            registry_generation = tmpfs_node_registry_generation;
        }

        for (;;) {
            TmpNode* node = nullptr;
            bool done = false;
            tmpfs_node_registry_lock.lock();
            if (index < tmpfs_nodes.size()) {
                node = tmpfs_nodes.at(index++);
                if (node != nullptr && !node->io_lock.try_lock()) {
                    node = nullptr;
                    saw_busy_node = true;
                }
            } else {
                done = true;
            }
            tmpfs_node_registry_lock.unlock();
            if (done) {
                break;
            }
            if (node == nullptr) {
                continue;
            }
            int const RET = migrate_node_swap_area_locked(node, area_id);
            node->io_lock.unlock();
            if (RET < 0) {
                return RET;
            }
        }

        bool registry_stable = false;
        {
            ker::mod::sys::MutexGuard guard(tmpfs_node_registry_lock);
            registry_stable = registry_generation == tmpfs_node_registry_generation;
        }
        if (!saw_busy_node && registry_stable) {
            return 0;
        }
        ker::mod::sched::kern_yield();
    }
}

void register_tmpfs_swap_consumer() {
    bool expected = false;
    if (!tmpfs_swap_consumer_registered.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
    }
    ker::mod::mm::swap::SwapConsumer const CONSUMER{
        .name = "tmpfs",
        .context = nullptr,
        .migrate_area = migrate_tmpfs_swap_area,
    };
    if (!ker::mod::mm::swap::register_consumer(CONSUMER)) {
        tmpfs_swap_consumer_registered.store(false, std::memory_order_release);
    }
}

// Grow the children slot array of a directory node if needed.
void ensure_children_capacity(TmpNode* dir, size_t slot) {
    if (slot < dir->children_capacity) {
        return;
    }
    size_t new_cap = (dir->children_capacity == 0) ? INITIAL_CHILDREN_CAPACITY : dir->children_capacity * 2;
    while (slot >= new_cap) {
        new_cap *= 2;
    }
    auto** new_arr = new TmpNode*[new_cap];
    for (size_t i = 0; i < dir->children_count; ++i) {
        new_arr[i] = dir->children[i];
    }
    for (size_t i = dir->children_count; i < new_cap; ++i) {
        new_arr[i] = nullptr;
    }
    delete[] dir->children;
    dir->children = new_arr;
    dir->children_capacity = new_cap;
}

void add_child(TmpNode* parent, TmpNode* child) {
    size_t slot = parent->children_count;
    if (parent->open_count.load(std::memory_order_acquire) == 0) {
        for (size_t i = 0; i < parent->children_count; ++i) {
            if (parent->children[i] == nullptr) {
                slot = i;
                break;
            }
        }
    }

    ensure_children_capacity(parent, slot);
    parent->children[slot] = child;
    if (slot == parent->children_count) {
        parent->children_count++;
    }
    parent->children_live_count++;
    child->parent = parent;
    child->unlinked = false;
    parent->directory_generation++;
    tmpfs_namespace_generation_value.fetch_add(1, std::memory_order_release);
    touch_modified(parent);
}

auto tmpfs_write_locked(TmpNode* n, const void* buf, size_t count, size_t offset) -> ssize_t {
    if (count == 0) {
        return 0;
    }
    if (n == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    if (!n->exec_content.can_mutate()) return -ETXTBSY;
    if (offset > static_cast<size_t>(-1) - count) {
        return -EFBIG;
    }

    auto const* src = static_cast<const uint8_t*>(buf);
    size_t total_written = 0;
    while (total_written < count) {
        size_t const POS = offset + total_written;
        size_t const PAGE_INDEX = POS / DEFAULT_TMPFS_BLOCK_SIZE;
        size_t const PAGE_OFF = POS % DEFAULT_TMPFS_BLOCK_SIZE;
        size_t const CHUNK = std::min(count - total_written, DEFAULT_TMPFS_BLOCK_SIZE - PAGE_OFF);

        TmpPage* page = nullptr;
        int const RET = ensure_page_resident_locked(n, PAGE_INDEX, &page);
        if (RET < 0) {
            if (total_written != 0) {
                break;
            }
            return RET;
        }
        std::memcpy(static_cast<uint8_t*>(page->data) + PAGE_OFF, src + total_written, CHUNK);
        total_written += CHUNK;
    }

    size_t const NEED = offset + total_written;
    n->size = std::max(NEED, n->size);
    touch_modified(n);
    return static_cast<ssize_t>(total_written);
}

auto tmpfs_resize_locked(TmpNode* n, size_t new_size) -> int {
    if (n == nullptr) {
        return -EINVAL;
    }
    if (!n->exec_content.can_mutate()) return -ETXTBSY;
    size_t new_pages = 0;
    if (!page_count_for_size(new_size, &new_pages)) {
        return -EFBIG;
    }

    if (new_size < n->size && new_pages != 0 && (new_size % DEFAULT_TMPFS_BLOCK_SIZE) != 0 && new_pages <= n->page_count) {
        TmpPage& existing_tail = n->pages[new_pages - 1];
        if (existing_tail.state != TmpPageState::HOLE) {
            TmpPage* tail = nullptr;
            int const RET = ensure_page_resident_locked(n, new_pages - 1, &tail);
            if (RET < 0) {
                return RET;
            }
            size_t const TAIL_OFF = new_size % DEFAULT_TMPFS_BLOCK_SIZE;
            std::memset(static_cast<uint8_t*>(tail->data) + TAIL_OFF, 0, DEFAULT_TMPFS_BLOCK_SIZE - TAIL_OFF);
        }
    }

    if (new_pages != n->page_count) {
        if (new_pages == 0) {
            for (size_t i = 0; i < n->page_count; ++i) {
                release_page_locked(n, n->pages[i], true);
            }
            delete[] n->pages;
            n->pages = nullptr;
            n->page_count = 0;
        } else {
            auto* pages = new TmpPage[new_pages];
            if (pages == nullptr) {
                return -ENOMEM;
            }
            for (size_t i = 0; i < std::min(new_pages, n->page_count); ++i) {
                pages[i] = n->pages[i];
            }
            for (size_t i = new_pages; i < n->page_count; ++i) {
                release_page_locked(n, n->pages[i], true);
            }
            delete[] n->pages;
            n->pages = pages;
            n->page_count = new_pages;
        }
    }
    n->size = new_size;
    touch_modified(n);
    return 0;
}

auto create_root_node_internal() -> TmpNode* {
    auto* node = new TmpNode;
    if (node == nullptr) {
        return nullptr;
    }
    copy_name(node->name, "/");
    node->type = TmpNodeType::DIRECTORY;
    node->mode = 0755;
    assign_node_incarnation(node);
    stamp_new_node(node);
    register_tmp_node(node);
    return node;
}
}  // namespace

void tmpfs_free_node(TmpNode* node) {
    if (node == nullptr) {
        return;
    }
    if (node->lookup_ref_count.load(std::memory_order_acquire) != 0) {
        node->unlinked = true;
        return;
    }
    ker::mod::sys::MutexGuard guard(node->io_lock);
    unregister_tmp_node(node);
    for (size_t i = 0; i < node->page_count; ++i) {
        release_page_locked(node, node->pages[i], true);
    }
    delete[] node->pages;
    delete[] node->symlink_target;
    delete[] node->children;
    delete node;
}

void tmpfs_lock_tree() { tmpfs_lock.lock(); }
void tmpfs_unlock_tree() { tmpfs_lock.unlock(); }

// --- Node operations ---

auto tmpfs_canonical_node(TmpNode* node) -> TmpNode* {
    while (node != nullptr && node->hardlink_target != nullptr) {
        node = node->hardlink_target;
    }
    return node;
}

auto tmpfs_canonical_node(const TmpNode* node) -> const TmpNode* {
    while (node != nullptr && node->hardlink_target != nullptr) {
        node = node->hardlink_target;
    }
    return node;
}

auto tmpfs_link_count(const TmpNode* node) -> uint32_t {
    auto const* canonical = tmpfs_canonical_node(node);
    return canonical != nullptr ? canonical->link_count.load(std::memory_order_acquire) : 0;
}

auto tmpfs_lookup(TmpNode* dir, const char* name) -> TmpNode* {
    if (dir == nullptr || name == nullptr || dir->type != TmpNodeType::DIRECTORY) {
        return nullptr;
    }
    for (size_t i = 0; i < dir->children_count; ++i) {
        if (dir->children[i] != nullptr && kstrcmp(dir->children[i]->name.data(), name) == 0) {
            return dir->children[i];
        }
    }
    return nullptr;
}

auto tmpfs_mkdir(TmpNode* parent, const char* name) -> TmpNode* {
    if (parent == nullptr || name == nullptr || parent->type != TmpNodeType::DIRECTORY) {
        return nullptr;
    }
    // Check if it already exists
    TmpNode* existing = tmpfs_lookup(parent, name);
    if (existing != nullptr) {
        if (existing->type == TmpNodeType::DIRECTORY) {
            return existing;  // Already exists as directory
        }
        return nullptr;  // Exists as non-directory
    }
    auto* node = new TmpNode;
    copy_name(node->name, name);
    node->type = TmpNodeType::DIRECTORY;
    node->mount = parent->mount;
    node->mode = 0755;
    assign_node_incarnation(node);
    stamp_new_node(node);
    register_tmp_node(node);
    add_child(parent, node);
    return node;
}

namespace {
auto tmpfs_create_file_with_identity(TmpNode* parent, const char* name, const ker::mod::security::FileIdentity& identity) -> TmpNode* {
    if (parent == nullptr || name == nullptr || parent->type != TmpNodeType::DIRECTORY) {
        return nullptr;
    }
    TmpNode* existing = tmpfs_lookup(parent, name);
    if (existing != nullptr) {
        return existing;  // Return existing node
    }
    auto* node = new TmpNode;
    if (node == nullptr) return nullptr;
    copy_name(node->name, name);
    node->type = TmpNodeType::FILE;
    node->mount = parent->mount;
    node->mode = identity.mode & 07777;
    node->uid = identity.uid;
    node->gid = identity.gid;
    assign_node_incarnation(node);
    stamp_new_node(node);
    register_tmp_node(node);
    add_child(parent, node);
    return node;
}
}  // namespace

auto tmpfs_create_file(TmpNode* parent, const char* name, uint32_t create_mode) -> TmpNode* {
    return tmpfs_create_file_with_identity(parent, name, {0, 0, 0100000U | (create_mode & 07777U)});
}

auto tmpfs_create_symlink(TmpNode* parent, const char* name, const char* target) -> TmpNode* {
    if (parent == nullptr || name == nullptr || target == nullptr || parent->type != TmpNodeType::DIRECTORY) {
        return nullptr;
    }
    TmpNode const* existing = tmpfs_lookup(parent, name);
    if (existing != nullptr) {
        return nullptr;  // Already exists
    }
    auto* node = new TmpNode;
    copy_name(node->name, name);
    node->type = TmpNodeType::SYMLINK;
    node->mount = parent->mount;
    node->mode = 0777;
    assign_node_incarnation(node);
    stamp_new_node(node);
    // Allocate and copy the symlink target
    size_t target_len = 0;
    while (target[target_len] != '\0') {
        target_len++;
    }
    node->size = target_len;
    node->symlink_target = new char[target_len + 1];
    if (node->symlink_target == nullptr) {
        delete node;
        return nullptr;
    }
    std::memcpy(node->symlink_target, target, target_len + 1);
    register_tmp_node(node);
    add_child(parent, node);
    return node;
}

auto tmpfs_create_hardlink(TmpNode* parent, const char* name, TmpNode* target) -> TmpNode* {
    TmpNode* canonical = tmpfs_canonical_node(target);
    if (parent == nullptr || name == nullptr || parent->type != TmpNodeType::DIRECTORY || canonical == nullptr ||
        canonical->type == TmpNodeType::DIRECTORY) {
        return nullptr;
    }
    if (tmpfs_lookup(parent, name) != nullptr) {
        return nullptr;
    }

    auto* node = new TmpNode;
    copy_name(node->name, name);
    node->type = canonical->type;
    node->mount = parent->mount;
    node->mode = canonical->mode;
    node->uid = canonical->uid;
    node->gid = canonical->gid;
    node->atime = canonical->atime;
    node->mtime = canonical->mtime;
    node->ctime = canonical->ctime;
    node->hardlink_target = canonical;
    assign_node_incarnation(node);
    node->link_count.store(0, std::memory_order_relaxed);

    register_tmp_node(node);
    add_child(parent, node);
    canonical->link_count.fetch_add(1, std::memory_order_acq_rel);
    touch_modified(canonical);
    return node;
}

auto tmpfs_attach_child(TmpNode* parent, TmpNode* child) -> bool {
    if (parent == nullptr || child == nullptr || parent->type != TmpNodeType::DIRECTORY) {
        return false;
    }
    add_child(parent, child);
    return true;
}

auto tmpfs_detach_child(TmpNode* parent, TmpNode* child) -> bool {
    if (parent == nullptr || child == nullptr || parent->type != TmpNodeType::DIRECTORY) {
        return false;
    }

    for (size_t i = 0; i < parent->children_count; ++i) {
        if (parent->children[i] != child) {
            continue;
        }

        parent->children[i] = nullptr;
        if (parent->children_live_count > 0) {
            parent->children_live_count--;
        }
        child->parent = nullptr;
        parent->directory_generation++;
        tmpfs_namespace_generation_value.fetch_add(1, std::memory_order_release);
        touch_modified(parent);

        if (parent->open_count.load(std::memory_order_acquire) == 0) {
            while (parent->children_count > 0 && parent->children[parent->children_count - 1] == nullptr) {
                parent->children_count--;
            }
        }
        return true;
    }

    return false;
}

void tmpfs_drop_detached_node(TmpNode* node) {
    if (node == nullptr) {
        return;
    }

    TmpNode* canonical = tmpfs_canonical_node(node);
    bool last_link = false;
    if (canonical != nullptr && canonical->type != TmpNodeType::DIRECTORY) {
        uint32_t const PREV = canonical->link_count.fetch_sub(1, std::memory_order_acq_rel);
        last_link = PREV <= 1;
    }

    node->unlinked = true;
    if (node != canonical) {
        tmpfs_free_node(node);
    }

    if (canonical != nullptr && last_link) {
        canonical->unlinked = true;
        if (canonical->open_count.load(std::memory_order_acquire) == 0 &&
            canonical->lookup_ref_count.load(std::memory_order_acquire) == 0) {
            tmpfs_free_node(canonical);
        }
    }
}

auto tmpfs_directory_is_empty(const TmpNode* dir) -> bool {
    return dir != nullptr && dir->type == TmpNodeType::DIRECTORY && dir->children_live_count == 0;
}

namespace {
void tmpfs_retain_node_locked(TmpNode* node) {
    if (node != nullptr) {
        node->lookup_ref_count.fetch_add(1, std::memory_order_relaxed);
    }
}

auto tmpfs_node_can_be_freed_locked(const TmpNode* node) -> bool {
    if (node == nullptr || !node->unlinked || node->open_count.load(std::memory_order_acquire) != 0 ||
        node->lookup_ref_count.load(std::memory_order_acquire) != 0) {
        return false;
    }
    if (node->type == TmpNodeType::DIRECTORY || node->hardlink_target != nullptr) {
        return true;
    }
    return node->link_count.load(std::memory_order_acquire) == 0;
}

void tmpfs_release_node_ref_locked(TmpNode* node) {
    if (node == nullptr) {
        return;
    }
    uint32_t const PREV = node->lookup_ref_count.fetch_sub(1, std::memory_order_acq_rel);
    if (PREV == 1 && tmpfs_node_can_be_freed_locked(node)) {
        tmpfs_free_node(node);
    }
}

}  // namespace

auto TmpfsNodeRef::acquire_locked(TmpNode* node) -> TmpfsNodeRef {
    if (node == nullptr) {
        return {};
    }
    TmpNode* canonical = tmpfs_canonical_node(node);
    tmpfs_retain_node_locked(node);
    if (canonical != node) {
        tmpfs_retain_node_locked(canonical);
    }
    return {node, canonical};
}

TmpfsNodeRef::TmpfsNodeRef(TmpfsNodeRef&& other) noexcept : node_(other.node_), canonical_(other.canonical_) {
    other.node_ = nullptr;
    other.canonical_ = nullptr;
}

auto TmpfsNodeRef::operator=(TmpfsNodeRef&& other) noexcept -> TmpfsNodeRef& {
    if (this != &other) {
        reset();
        node_ = other.node_;
        canonical_ = other.canonical_;
        other.node_ = nullptr;
        other.canonical_ = nullptr;
    }
    return *this;
}

TmpfsNodeRef::~TmpfsNodeRef() { reset(); }

void TmpfsNodeRef::reset() {
    if (node_ == nullptr) {
        return;
    }
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    TmpNode* const NODE = node_;
    TmpNode* const CANONICAL = canonical_;
    node_ = nullptr;
    canonical_ = nullptr;
    tmpfs_release_node_ref_locked(NODE);
    if (CANONICAL != NODE) {
        tmpfs_release_node_ref_locked(CANONICAL);
    }
}

auto tmpfs_acquire_node(TmpNode* node) -> TmpfsNodeRef {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    return TmpfsNodeRef::acquire_locked(node);
}

auto tmpfs_acquire_lookup(const ker::mod::security::Snapshot& credentials, TmpNode* root, const char* path, TmpfsLookupHandle* out) -> int {
    if (root == nullptr || path == nullptr || out == nullptr) {
        return -EINVAL;
    }

    *out = TmpfsLookupHandle{};
    TmpfsLookupHandle result{};
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    constexpr size_t LOOKUP_PATH_CAPACITY = 512;
    constexpr unsigned MAX_LOOKUP_SYMLINKS = 8;
    std::array<char, LOOKUP_PATH_CAPACITY> pending{};
    size_t input_length = 0;
    while (path[input_length] != '\0') {
        if (input_length >= pending.size() - 1) {
            return -ENAMETOOLONG;
        }
        pending.at(input_length) = path[input_length];
        ++input_length;
    }
    pending.at(input_length) = '\0';

    TmpNode* current = root;
    const char* cursor = pending.data();
    unsigned symlink_depth = 0;
    while (*cursor == '/') {
        ++cursor;
    }
    if (*cursor == '\0') {
        result.parent_ = TmpfsNodeRef::acquire_locked(root);
        result.target_ = TmpfsNodeRef::acquire_locked(root);
        result.parent_incarnation_ = root->incarnation;
        result.parent_generation_ = root->directory_generation;
        result.namespace_generation_ = tmpfs_namespace_generation_locked();
        result.target_incarnation_ = root->incarnation;
        result.target_existed_ = true;
        result.root_target_ = true;
        *out = std::move(result);
        return 0;
    }

    for (;;) {
        std::array<char, TMPFS_NAME_MAX> component{};
        size_t component_len = 0;
        while (cursor[component_len] != '\0' && cursor[component_len] != '/') {
            if (component_len >= component.size() - 1) {
                return -ENAMETOOLONG;
            }
            component.at(component_len) = cursor[component_len];
            ++component_len;
        }
        component.at(component_len) = '\0';
        cursor += component_len;
        while (*cursor == '/') {
            ++cursor;
        }

        if (current->type != TmpNodeType::DIRECTORY) return -ENOTDIR;
        int const SEARCH = ker::mod::security::authorize_file_access(credentials, {current->uid, current->gid, S_IFDIR | current->mode}, 1);
        if (SEARCH != 0) return SEARCH;

        bool const FINAL_COMPONENT = *cursor == '\0';
        if (FINAL_COMPONENT) {
            bool const SELF = kstrcmp(component.data(), ".") == 0;
            bool const PARENT = kstrcmp(component.data(), "..") == 0;
            if (SELF || PARENT) {
                TmpNode* selected = PARENT && current->parent != nullptr ? current->parent : current;
                result.parent_ = TmpfsNodeRef::acquire_locked(selected);
                result.target_ = TmpfsNodeRef::acquire_locked(selected);
                result.parent_incarnation_ = selected->incarnation;
                result.parent_generation_ = selected->directory_generation;
                result.namespace_generation_ = tmpfs_namespace_generation_locked();
                result.target_incarnation_ = selected->incarnation;
                result.target_existed_ = true;
                result.root_target_ = true;
                *out = std::move(result);
                return 0;
            }
            TmpNode* target = tmpfs_lookup(current, component.data());
            result.parent_ = TmpfsNodeRef::acquire_locked(current);
            result.target_ = TmpfsNodeRef::acquire_locked(target);
            result.name_ = component;
            result.parent_incarnation_ = current->incarnation;
            result.parent_generation_ = current->directory_generation;
            result.namespace_generation_ = tmpfs_namespace_generation_locked();
            result.target_incarnation_ = target != nullptr ? target->incarnation : 0;
            result.target_existed_ = target != nullptr;
            *out = std::move(result);
            return 0;
        }

        if (kstrcmp(component.data(), ".") == 0) {
            continue;
        }
        if (kstrcmp(component.data(), "..") == 0) {
            if (current->parent != nullptr) {
                current = current->parent;
            }
            continue;
        }
        if (current->type != TmpNodeType::DIRECTORY) {
            return -ENOTDIR;
        }
        TmpNode* child = tmpfs_lookup(current, component.data());
        if (child == nullptr) {
            return -ENOENT;
        }
        TmpNode* const CANONICAL_CHILD = tmpfs_canonical_node(child);
        if (CANONICAL_CHILD != nullptr && CANONICAL_CHILD->type == TmpNodeType::SYMLINK) {
            if (CANONICAL_CHILD->symlink_target == nullptr || CANONICAL_CHILD->symlink_target[0] == '\0') {
                return -ENOENT;
            }
            if (symlink_depth == MAX_LOOKUP_SYMLINKS) {
                return -ELOOP;
            }
            ++symlink_depth;

            size_t target_length = 0;
            while (CANONICAL_CHILD->symlink_target[target_length] != '\0') {
                ++target_length;
            }
            size_t const REST_LENGTH = std::strlen(cursor);
            bool const NEED_SEPARATOR = target_length != 0 && REST_LENGTH != 0 && CANONICAL_CHILD->symlink_target[target_length - 1] != '/';
            if (target_length + (NEED_SEPARATOR ? 1 : 0) + REST_LENGTH >= pending.size()) {
                return -ENAMETOOLONG;
            }
            std::array<char, LOOKUP_PATH_CAPACITY> replacement{};
            std::memcpy(replacement.data(), CANONICAL_CHILD->symlink_target, target_length);
            size_t replacement_length = target_length;
            if (NEED_SEPARATOR) {
                replacement.at(replacement_length++) = '/';
            }
            if (REST_LENGTH != 0) {
                std::memcpy(replacement.data() + replacement_length, cursor, REST_LENGTH);
                replacement_length += REST_LENGTH;
            }
            replacement.at(replacement_length) = '\0';
            bool const ABSOLUTE_TARGET = replacement.front() == '/';
            pending = replacement;
            cursor = pending.data();
            if (ABSOLUTE_TARGET) {
                current = root->mount != nullptr && root->mount->root != nullptr ? root->mount->root : root;
            }
            while (*cursor == '/') {
                ++cursor;
            }
            continue;
        }
        current = child;
    }
}

auto tmpfs_validate_lookup_locked(const TmpfsLookupHandle& handle) -> bool {
    TmpNode* const PARENT = handle.parent_.get();
    if (PARENT == nullptr || PARENT->type != TmpNodeType::DIRECTORY || PARENT->unlinked ||
        PARENT->incarnation != handle.parent_incarnation_ || PARENT->directory_generation != handle.parent_generation_ ||
        tmpfs_namespace_generation_locked() != handle.namespace_generation_) {
        return false;
    }
    if (handle.root_target_) {
        TmpNode* const TARGET = handle.target_.get();
        // Empty/dot lookups bind their selected directory itself, including a
        // retained directory below the filesystem root.
        return TARGET == PARENT && TARGET->incarnation == handle.target_incarnation_;
    }
    TmpNode* const CURRENT = tmpfs_lookup(PARENT, handle.name_.data());
    if (!handle.target_existed_) {
        return CURRENT == nullptr;
    }
    return CURRENT == handle.target_.get() && CURRENT != nullptr && CURRENT->incarnation == handle.target_incarnation_;
}

namespace {
auto directory_path(TmpNode* root, TmpNode* directory, char* out, size_t capacity, bool bounded,
                    const ker::mod::security::Snapshot* credentials) -> int {
    if (root == nullptr || directory == nullptr || out == nullptr || capacity == 0) return -EINVAL;
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    if (directory->type != TmpNodeType::DIRECTORY || (bounded && root->type != TmpNodeType::DIRECTORY)) return -ENOTDIR;
    if (bounded && (root->unlinked || directory->unlinked)) return -ENOENT;
    if (credentials != nullptr) {
        int const SEARCH =
            ker::mod::security::authorize_file_access(*credentials, {directory->uid, directory->gid, 0040000U | directory->mode}, 1);
        if (SEARCH != 0) return SEARCH;
    }
    std::array<char, ker::vfs::MOUNT_PATH_MAX> path{};
    size_t cursor = path.size() - 1;
    auto* node = directory;
    while (node != root) {
        if (node->unlinked) return -ENOENT;
        if (bounded && (node->parent == nullptr || node->parent == node)) return -EPERM;
        if (node->parent == nullptr) return -ENOENT;
        size_t length = 0;
        while (length < node->name.size() && node->name[length] != '\0') {
            if (node->name[length] == '/') return -EIO;
            ++length;
        }
        if (length == 0 || length == node->name.size() || node->parent == node || tmpfs_lookup(node->parent, node->name.data()) != node)
            return -EIO;
        if (length + 1 > cursor) return -ENAMETOOLONG;
        cursor -= length;
        std::memcpy(path.data() + cursor, node->name.data(), length);
        path[--cursor] = '/';
        node = node->parent;
    }
    if (cursor == path.size() - 1) path[--cursor] = '/';
    size_t const LENGTH = path.size() - cursor;
    if (LENGTH > capacity) return -ENAMETOOLONG;
    std::memcpy(out, path.data() + cursor, LENGTH);
    return 0;
}
}  // namespace

auto tmpfs_directory_path(TmpNode* root, TmpNode* directory, char* out, size_t capacity) -> int {
    return directory_path(root, directory, out, capacity, false, nullptr);
}

auto tmpfs_directory_path_beneath(TmpNode* root, TmpNode* directory, char* out, size_t capacity,
                                  const ker::mod::security::Snapshot* credentials) -> int {
    return directory_path(root, directory, out, capacity, true, credentials);
}

auto tmpfs_lookup_directory_path_beneath(TmpNode* root, const TmpfsLookupHandle& lookup, char* out, size_t capacity) -> int {
    TmpNode* selected = nullptr;
    {
        ker::mod::sys::MutexGuard guard(tmpfs_lock);
        if (!tmpfs_validate_lookup_locked(lookup)) return -EAGAIN;
        auto* target = lookup.target();
        selected = target != nullptr && target->type == TmpNodeType::DIRECTORY ? target : lookup.parent();
    }
    // The handle retains either selection. Do not recursively take tmpfs_lock.
    return directory_path(root, selected, out, capacity, true, nullptr);
}

auto tmpfs_validate_lookup(const TmpfsLookupHandle& handle) -> bool {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    return tmpfs_validate_lookup_locked(handle);
}

auto tmpfs_search_authority_generation() -> uint64_t { return tmpfs_search_authority_generation_value.load(std::memory_order_acquire); }

auto tmpfs_namespace_generation_locked() -> uint64_t { return tmpfs_namespace_generation_value.load(std::memory_order_acquire); }

auto tmpfs_namespace_generation() -> uint64_t {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    return tmpfs_namespace_generation_locked();
}

namespace {

auto tmpfs_lookup_target_locked(const TmpfsLookupHandle& handle, TmpNode** entry_out, TmpNode** canonical_out) -> int {
    if (!tmpfs_validate_lookup_locked(handle)) {
        return -EAGAIN;
    }
    TmpNode* const ENTRY = handle.target();
    if (!handle.target_existed() || ENTRY == nullptr) {
        return -ENOENT;
    }
    TmpNode* const CANONICAL = tmpfs_canonical_node(ENTRY);
    if (CANONICAL == nullptr) {
        return -ENOENT;
    }
    if (entry_out != nullptr) {
        *entry_out = ENTRY;
    }
    if (canonical_out != nullptr) {
        *canonical_out = CANONICAL;
    }
    return 0;
}

void tmpfs_fill_metadata_locked(const TmpNode* node, TmpfsObjectMetadata* out) {
    if (out == nullptr) {
        return;
    }
    auto const* canonical = tmpfs_canonical_node(node);
    if (canonical == nullptr) {
        *out = {};
        return;
    }
    *out = {
        .type = canonical->type,
        .inode = reinterpret_cast<uintptr_t>(canonical),
        .size = canonical->size,
        .link_count = tmpfs_link_count(canonical),
        .mode = canonical->mode,
        .uid = canonical->uid,
        .gid = canonical->gid,
        .atime = canonical->atime,
        .mtime = canonical->mtime,
        .ctime = canonical->ctime,
    };
}

void tmpfs_metadata_to_stat_impl(const TmpfsObjectMetadata& metadata, uint32_t dev_id, Stat* out) {
    if (out == nullptr) {
        return;
    }
    std::memset(out, 0, sizeof(*out));
    out->st_dev = dev_id;
    out->st_ino = static_cast<ino_t>(metadata.inode);
    out->st_nlink = metadata.link_count;
    out->st_uid = metadata.uid;
    out->st_gid = metadata.gid;
    out->st_size = static_cast<off_t>(metadata.size);
    out->st_blksize = DEFAULT_TMPFS_BLOCK_SIZE;
    out->st_blocks = static_cast<blkcnt_t>((metadata.size + 511) / 512);
    out->st_atim = metadata.atime;
    out->st_mtim = metadata.mtime;
    out->st_ctim = metadata.ctime;
    switch (metadata.type) {
        case TmpNodeType::FILE:
            out->st_mode = S_IFREG | metadata.mode;
            break;
        case TmpNodeType::DIRECTORY:
            out->st_mode = S_IFDIR | metadata.mode;
            break;
        case TmpNodeType::SYMLINK:
            out->st_mode = S_IFLNK | metadata.mode;
            break;
    }
}

void tmpfs_fill_stat_locked(const TmpNode* node, uint32_t dev_id, Stat* out) {
    TmpfsObjectMetadata metadata{};
    tmpfs_fill_metadata_locked(node, &metadata);
    tmpfs_metadata_to_stat_impl(metadata, dev_id, out);
}

auto tmpfs_lookup_precedes(const TmpfsLookupHandle& left, const TmpfsLookupHandle& right) -> bool {
    if (left.parent_incarnation() != right.parent_incarnation()) {
        return left.parent_incarnation() < right.parent_incarnation();
    }
    return kstrcmp(left.name(), right.name()) <= 0;
}

auto tmpfs_validate_pair_locked(const TmpfsLookupHandle& left, const TmpfsLookupHandle& right) -> bool {
    if (tmpfs_lookup_precedes(left, right)) {
        return tmpfs_validate_lookup_locked(left) && tmpfs_validate_lookup_locked(right);
    }
    return tmpfs_validate_lookup_locked(right) && tmpfs_validate_lookup_locked(left);
}

auto tmpfs_tree_root_locked(TmpNode* node) -> TmpNode* {
    for (size_t depth = 0; node != nullptr && node->parent != nullptr && depth < 1024; ++depth) {
        node = node->parent;
    }
    return node != nullptr && node->parent == nullptr ? node : nullptr;
}

auto tmpfs_same_tree_locked(TmpNode* left, TmpNode* right) -> bool {
    return tmpfs_tree_root_locked(left) != nullptr && tmpfs_tree_root_locked(left) == tmpfs_tree_root_locked(right);
}

}  // namespace

void tmpfs_metadata_to_stat(const TmpfsObjectMetadata& metadata, uint32_t dev_id, Stat* out) {
    tmpfs_metadata_to_stat_impl(metadata, dev_id, out);
}

auto tmpfs_lookup_metadata(const TmpfsLookupHandle& handle, bool require_directory, TmpfsObjectMetadata* out) -> int {
    if (out == nullptr) {
        return -EINVAL;
    }
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    TmpNode* canonical = nullptr;
    int const RET = tmpfs_lookup_target_locked(handle, nullptr, &canonical);
    if (RET < 0) {
        return RET;
    }
    if (require_directory && canonical->type != TmpNodeType::DIRECTORY) {
        return -ENOTDIR;
    }
    tmpfs_fill_metadata_locked(canonical, out);
    return 0;
}

auto tmpfs_clone_exec_file(File* source, File** output) -> int {
    if (source == nullptr || output == nullptr || source->fops != get_tmpfs_fops()) return -EINVAL;
    auto* node = tmpfs_canonical_node(static_cast<TmpNode*>(source->private_data));
    if (node == nullptr) return -EBADF;
    std::unique_ptr<File> file(new (std::nothrow) File{});
    if (file == nullptr) return -ENOMEM;
    file->exec_read_view.reset(new (std::nothrow) ExecReadView{});
    if (file->exec_read_view == nullptr) return -ENOMEM;
    ker::mod::sys::MutexGuard tree_guard(tmpfs_lock);
    ker::mod::sys::MutexGuard io_guard(node->io_lock);
    if (node->type != TmpNodeType::FILE) return -ENOEXEC;
    tmpfs_fill_stat_locked(node, source->mount_dev_id, &file->exec_read_view->metadata);
    int const RESULT = node->exec_content.acquire(*file->exec_read_view);
    if (RESULT != 0) return RESULT;
    node->open_count.fetch_add(1, std::memory_order_relaxed);
    file->private_data = node;
    file->fops = get_tmpfs_fops();
    file->fs_type = FSType::TMPFS;
    file->fd = -1;
    file->refcount = 1;
    file->open_flags = O_NO_CACHE;
    *output = file.release();
    return 0;
}

auto tmpfs_stat_node(const TmpNode* node, uint32_t dev_id, Stat* out) -> int {
    if (out == nullptr) return -EINVAL;
    if (node == nullptr) return -EBADF;
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    tmpfs_fill_stat_locked(node, dev_id, out);
    return 0;
}

auto tmpfs_stat_lookup(const TmpfsLookupHandle& handle, bool require_directory, uint32_t dev_id, Stat* out) -> int {
    if (out == nullptr) {
        return -EINVAL;
    }
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    TmpNode* canonical = nullptr;
    int const RET = tmpfs_lookup_target_locked(handle, nullptr, &canonical);
    if (RET < 0) {
        return RET;
    }
    if (require_directory && canonical->type != TmpNodeType::DIRECTORY) {
        return -ENOTDIR;
    }
    tmpfs_fill_stat_locked(canonical, dev_id, out);
    return 0;
}

auto tmpfs_readlink_lookup(const TmpfsLookupHandle& handle, char* buf, size_t bufsize) -> ssize_t {
    if (buf == nullptr || bufsize == 0) {
        return -EINVAL;
    }
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    TmpNode* canonical = nullptr;
    int const RET = tmpfs_lookup_target_locked(handle, nullptr, &canonical);
    if (RET < 0) {
        return RET;
    }
    if (canonical->type != TmpNodeType::SYMLINK || canonical->symlink_target == nullptr) {
        return -EINVAL;
    }
    size_t len = 0;
    while (canonical->symlink_target[len] != '\0') {
        ++len;
    }
    size_t const TO_COPY = std::min(len, bufsize);
    std::memcpy(buf, canonical->symlink_target, TO_COPY);
    return static_cast<ssize_t>(TO_COPY);
}

auto tmpfs_create_lookup(const TmpfsLookupHandle& handle, uint32_t mode, TmpfsObjectMetadata* out) -> int {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    if (!tmpfs_validate_lookup_locked(handle)) {
        return -EAGAIN;
    }
    if (handle.target_existed()) {
        return -EEXIST;
    }
    TmpNode* const NODE = tmpfs_create_file(handle.parent(), handle.name(), mode & 07777);
    if (NODE == nullptr) {
        return -ENOMEM;
    }
    tmpfs_fill_metadata_locked(NODE, out);
    return 0;
}

auto tmpfs_mkdir_lookup(const ker::mod::security::Snapshot& credentials, const TmpfsLookupHandle& handle, uint32_t mode,
                        TmpfsObjectMetadata* out, bool read_only) -> int {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    if (!tmpfs_validate_lookup_locked(handle)) {
        return -EAGAIN;
    }
    TmpNode* const PARENT = handle.parent();
    ker::mod::security::FileIdentity identity{};
    int const AUTH =
        ker::mod::security::prepare_directory_create(credentials, {PARENT->uid, PARENT->gid, 0040000U | PARENT->mode}, mode, identity);
    if (AUTH != 0) return AUTH;
    if (handle.target_existed()) {
        return -EEXIST;
    }
    if (read_only) {
        return -EROFS;
    }
    TmpNode* const NODE = tmpfs_mkdir(handle.parent(), handle.name());
    if (NODE == nullptr) {
        return -ENOMEM;
    }
    NODE->uid = identity.uid;
    NODE->gid = identity.gid;
    NODE->mode = identity.mode & 07777U;
    tmpfs_fill_metadata_locked(NODE, out);
    return 0;
}

auto tmpfs_symlink_lookup(const ker::mod::security::Snapshot& credentials, const TmpfsLookupHandle& handle, const char* target,
                          TmpfsObjectMetadata* out, bool require_directory, bool read_only) -> int {
    if (target == nullptr) {
        return -EINVAL;
    }
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    if (!tmpfs_validate_lookup_locked(handle)) {
        return -EAGAIN;
    }
    TmpNode* const PARENT = handle.parent();
    ker::mod::security::FileIdentity identity{};
    int const AUTH = ker::mod::security::prepare_symlink_create(credentials, {PARENT->uid, PARENT->gid, 0040000U | PARENT->mode}, identity);
    if (AUTH != 0) return AUTH;
    if (handle.target_existed()) {
        // The legacy tmpfs symlink path reports a generic failure for both an
        // existing destination and allocation failure.
        return -1;
    }
    if (require_directory) return -ENOENT;
    if (read_only) {
        return -EROFS;
    }
    TmpNode* const NODE = tmpfs_create_symlink(handle.parent(), handle.name(), target);
    if (NODE == nullptr) {
        // Preserve the legacy tmpfs symlink branch's generic allocation error.
        return -1;
    }
    NODE->uid = identity.uid;
    NODE->gid = identity.gid;
    NODE->mode = identity.mode & 07777U;
    tmpfs_fill_metadata_locked(NODE, out);
    return 0;
}

namespace {
auto metadata_identity_locked(const TmpNode& node) -> ker::mod::security::FileIdentity {
    uint32_t const TYPE = node.type == TmpNodeType::DIRECTORY ? S_IFDIR : (node.type == TmpNodeType::SYMLINK ? S_IFLNK : S_IFREG);
    return {.uid = node.uid, .gid = node.gid, .mode = TYPE | node.mode};
}
}  // namespace

auto tmpfs_unlink_lookup(const ker::mod::security::Snapshot& credentials, const TmpfsLookupHandle& handle, bool* hardlink_count_changed,
                         bool read_only) -> int {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    TmpNode* entry = nullptr;
    TmpNode* canonical = nullptr;
    int const RET = tmpfs_lookup_target_locked(handle, &entry, &canonical);
    if (RET < 0) {
        return RET;
    }
    if (entry->type == TmpNodeType::DIRECTORY) {
        return -EISDIR;
    }
    int const AUTH = ker::mod::security::authorize_file_removal(credentials, metadata_identity_locked(*handle.parent()),
                                                                metadata_identity_locked(*canonical));
    if (AUTH != 0) return AUTH;
    if (read_only) {
        return -EROFS;
    }
    bool const LINK_COUNT_CHANGED = tmpfs_link_count(entry) > 1;
    if (!tmpfs_detach_child(handle.parent(), entry)) {
        return -ENOENT;
    }
    tmpfs_drop_detached_node(entry);
    if (hardlink_count_changed != nullptr) {
        *hardlink_count_changed = LINK_COUNT_CHANGED;
    }
    return 0;
}

auto tmpfs_rmdir_lookup(const ker::mod::security::Snapshot& credentials, const TmpfsLookupHandle& handle, bool read_only) -> int {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    TmpNode* entry = nullptr;
    TmpNode* canonical = nullptr;
    int const RET = tmpfs_lookup_target_locked(handle, &entry, &canonical);
    if (RET < 0) {
        return RET;
    }
    if (entry->type != TmpNodeType::DIRECTORY) {
        return -ENOTDIR;
    }
    int const AUTH = ker::mod::security::authorize_file_removal(credentials, metadata_identity_locked(*handle.parent()),
                                                                metadata_identity_locked(*canonical));
    if (AUTH != 0) return AUTH;
    if (!tmpfs_directory_is_empty(entry)) {
        return -ENOTEMPTY;
    }
    // A root lookup binds the root as its own parent; it has no detachable
    // membership. Preserve the existing detach-failure result on readonly too.
    if (handle.root_target()) {
        return -ENOENT;
    }
    if (read_only) {
        return -EROFS;
    }
    if (!tmpfs_detach_child(handle.parent(), entry)) {
        return -ENOENT;
    }
    entry->unlinked = true;
    if (entry->open_count.load(std::memory_order_acquire) == 0) {
        tmpfs_free_node(entry);
    }
    return 0;
}

namespace {

void commit_metadata_locked(TmpNode& node, const ker::mod::security::FileIdentity& prepared, TmpfsObjectMetadata* out) {
    bool const SEARCH_AUTHORITY_CHANGED = node.type == TmpNodeType::DIRECTORY &&
                                          (node.uid != prepared.uid || node.gid != prepared.gid || node.mode != (prepared.mode & 07777U));
    node.uid = prepared.uid;
    node.gid = prepared.gid;
    node.mode = prepared.mode & 07777U;
    node.ctime = current_timespec();
    // A component lookup may already have searched this ancestor. Invalidate
    // that admission before publishing changed directory authority, even when
    // the directory is not the retained lookup's final parent or target.
    if (SEARCH_AUTHORITY_CHANGED) tmpfs_search_authority_generation_value.fetch_add(1, std::memory_order_release);
    tmpfs_fill_metadata_locked(&node, out);
}

auto chmod_locked(TmpNode& node, const ker::mod::security::Snapshot& credentials, uint32_t mode, TmpfsObjectMetadata* out, bool read_only)
    -> int {
    ker::mod::security::FileIdentity prepared;
    int const RET = ker::mod::security::prepare_file_chmod(credentials, metadata_identity_locked(node), mode, prepared);
    if (RET == 0 && read_only) {
        return -EROFS;
    }
    if (RET == 0) {
        commit_metadata_locked(node, prepared, out);
    }
    return RET;
}

auto chown_locked(TmpNode& node, const ker::mod::security::Snapshot& credentials, uint32_t owner, uint32_t group, TmpfsObjectMetadata* out)
    -> int {
    ker::mod::security::FileIdentity prepared;
    int const RET = ker::mod::security::prepare_file_chown(credentials, metadata_identity_locked(node), owner, group, prepared);
    if (RET == 0) {
        commit_metadata_locked(node, prepared, out);
    }
    return RET;
}
}  // namespace

auto tmpfs_chmod_lookup(const TmpfsLookupHandle& handle, const ker::mod::security::Snapshot& credentials, uint32_t mode,
                        TmpfsObjectMetadata* out, bool read_only) -> int {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    TmpNode* canonical = nullptr;
    int const RET = tmpfs_lookup_target_locked(handle, nullptr, &canonical);
    return RET < 0 ? RET : chmod_locked(*canonical, credentials, mode, out, read_only);
}

auto tmpfs_chown_lookup(const TmpfsLookupHandle& handle, const ker::mod::security::Snapshot& credentials, uint32_t owner, uint32_t group,
                        TmpfsObjectMetadata* out) -> int {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    TmpNode* canonical = nullptr;
    int const RET = tmpfs_lookup_target_locked(handle, nullptr, &canonical);
    return RET < 0 ? RET : chown_locked(*canonical, credentials, owner, group, out);
}

auto tmpfs_chmod_node(TmpNode* node, const ker::mod::security::Snapshot& credentials, uint32_t mode, TmpfsObjectMetadata* out) -> int {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    auto* canonical = tmpfs_canonical_node(node);
    return canonical == nullptr ? -EBADF : chmod_locked(*canonical, credentials, mode, out, false);
}

auto tmpfs_chown_node(TmpNode* node, const ker::mod::security::Snapshot& credentials, uint32_t owner, uint32_t group,
                      TmpfsObjectMetadata* out) -> int {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    auto* canonical = tmpfs_canonical_node(node);
    return canonical == nullptr ? -EBADF : chown_locked(*canonical, credentials, owner, group, out);
}

namespace {
auto utimens_locked(TmpNode& canonical, const ker::mod::security::Snapshot& credentials, const TmpfsTimesUpdate& times,
                    TmpfsObjectMetadata* out, bool read_only) -> int {
    int const RET =
        ker::mod::security::authorize_file_timestamps(credentials, {canonical.uid, canonical.gid, canonical.mode}, times.intent());
    if (RET != 0) return RET;
    if (read_only && times.changed()) {
        return -EROFS;
    }
    if (times.set_atime()) canonical.atime = times.atime();
    if (times.set_mtime()) canonical.mtime = times.mtime();
    if (times.changed()) canonical.ctime = times.ctime();
    tmpfs_fill_metadata_locked(&canonical, out);
    return 0;
}
}  // namespace

auto tmpfs_utimens_lookup(const TmpfsLookupHandle& handle, const ker::mod::security::Snapshot& credentials, const TmpfsTimesUpdate& times,
                          TmpfsObjectMetadata* out, bool read_only) -> int {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    TmpNode* canonical = nullptr;
    int const RET = tmpfs_lookup_target_locked(handle, nullptr, &canonical);
    return RET < 0 ? RET : utimens_locked(*canonical, credentials, times, out, read_only);
}

auto tmpfs_utimens_node(TmpNode* node, const ker::mod::security::Snapshot& credentials, const TmpfsTimesUpdate& times,
                        TmpfsObjectMetadata* out) -> int {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    auto* canonical = tmpfs_canonical_node(node);
    return canonical == nullptr ? -EBADF : utimens_locked(*canonical, credentials, times, out, false);
}

auto tmpfs_link_lookup(const ker::mod::security::Snapshot& credentials, const TmpfsLookupHandle& source,
                       const TmpfsLookupHandle& destination, TmpfsObjectMetadata* out, bool source_requires_directory,
                       bool destination_requires_directory, bool read_only) -> int {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    if (!tmpfs_validate_pair_locked(source, destination)) {
        return -EAGAIN;
    }
    TmpNode* const SOURCE_ENTRY = source.target();
    TmpNode* const SOURCE = tmpfs_canonical_node(SOURCE_ENTRY);
    if (!source.target_existed() || SOURCE == nullptr) {
        return -ENOENT;
    }
    if (!tmpfs_same_tree_locked(source.parent(), destination.parent())) {
        return -EXDEV;
    }
    if (SOURCE->type == TmpNodeType::DIRECTORY) {
        return -EPERM;
    }
    int const AUTH =
        ker::mod::security::authorize_file_link(credentials, metadata_identity_locked(*source.parent()),
                                                metadata_identity_locked(*destination.parent()), metadata_identity_locked(*SOURCE));
    if (AUTH != 0) return AUTH;
    if (source_requires_directory) return -ENOTDIR;
    if (destination.target_existed()) {
        return -EEXIST;
    }
    if (destination_requires_directory) return -ENOENT;
    if (read_only) {
        return -EROFS;
    }
    TmpNode* const LINK = tmpfs_create_hardlink(destination.parent(), destination.name(), SOURCE);
    if (LINK == nullptr) {
        return -ENOMEM;
    }
    tmpfs_fill_metadata_locked(SOURCE, out);
    return 0;
}

auto tmpfs_rename_lookup(const ker::mod::security::Snapshot& credentials, const TmpfsLookupHandle& source,
                         const TmpfsLookupHandle& destination, bool source_requires_directory, bool destination_requires_directory,
                         TmpfsObjectMetadata* out, bool* replaced_hardlink_count_changed, bool* namespace_changed, bool read_only) -> int {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    if (!tmpfs_validate_pair_locked(source, destination)) return -EAGAIN;
    TmpNode* const SOURCE = source.target();
    TmpNode* const DESTINATION = destination.target();
    if (!source.target_existed() || SOURCE == nullptr) return -ENOENT;
    if (!tmpfs_same_tree_locked(source.parent(), destination.parent())) return -EXDEV;
    TmpNode* const CANONICAL = tmpfs_canonical_node(SOURCE);
    TmpNode* const DISPLACED = tmpfs_canonical_node(DESTINATION);
    auto const DISPLACED_IDENTITY = DISPLACED != nullptr ? metadata_identity_locked(*DISPLACED) : ker::mod::security::FileIdentity{};
    int const AUTH = ker::mod::security::authorize_file_rename(
        credentials, metadata_identity_locked(*source.parent()), metadata_identity_locked(*CANONICAL),
        metadata_identity_locked(*destination.parent()), DISPLACED != nullptr ? &DISPLACED_IDENTITY : nullptr,
        source.parent() != destination.parent());
    if (AUTH != 0) return AUTH;
    if (source.root_target() || destination.root_target()) return -EBUSY;
    auto special_name = [](const char* name) { return kstrcmp(name, ".") == 0 || kstrcmp(name, "..") == 0; };
    if (special_name(source.name()) || special_name(destination.name())) return -EINVAL;
    bool const SOURCE_DIRECTORY = SOURCE->type == TmpNodeType::DIRECTORY;
    if (source_requires_directory && !SOURCE_DIRECTORY) return -ENOTDIR;
    if (destination_requires_directory) {
        if (DESTINATION == nullptr) return -ENOENT;
        if (DESTINATION->type != TmpNodeType::DIRECTORY) return -ENOTDIR;
        if (!SOURCE_DIRECTORY) return -EISDIR;
    }
    if (CANONICAL == DISPLACED) {
        tmpfs_fill_metadata_locked(CANONICAL, out);
        if (replaced_hardlink_count_changed != nullptr) *replaced_hardlink_count_changed = false;
        if (namespace_changed != nullptr) *namespace_changed = false;
        return 0;
    }
    if (DESTINATION != nullptr) {
        bool const DESTINATION_DIRECTORY = DESTINATION->type == TmpNodeType::DIRECTORY;
        if (SOURCE_DIRECTORY && !DESTINATION_DIRECTORY) return -ENOTDIR;
        if (!SOURCE_DIRECTORY && DESTINATION_DIRECTORY) return -EISDIR;
        if (DESTINATION_DIRECTORY && !tmpfs_directory_is_empty(DESTINATION)) return -ENOTEMPTY;
    }
    if (SOURCE_DIRECTORY) {
        auto* ancestor = destination.parent();
        for (size_t depth = 0; ancestor != nullptr; ++depth, ancestor = ancestor->parent) {
            if (ancestor == SOURCE) return -EINVAL;
            if (depth == 1024) return -ELOOP;
        }
    }
    if (read_only) {
        return -EROFS;
    }
    // Pair validation pins both memberships under this lock. Reserve the
    // append slot before detaching either entry; attach cannot allocate later.
    auto* parent = destination.parent();
    if (parent->children_count >= parent->children_capacity) {
        constexpr size_t MAX_CAPACITY = SIZE_MAX / sizeof(TmpNode*);
        if (parent->children_count >= MAX_CAPACITY) return -ENOMEM;
        size_t const CAPACITY = parent->children_capacity == 0                 ? INITIAL_CHILDREN_CAPACITY
                                : parent->children_capacity > MAX_CAPACITY / 2 ? MAX_CAPACITY
                                                                               : parent->children_capacity * 2;
        auto** children = new (std::nothrow) TmpNode*[CAPACITY];
        if (children == nullptr) return -ENOMEM;
        for (size_t i = 0; i < parent->children_count; ++i) children[i] = parent->children[i];
        for (size_t i = parent->children_count; i < CAPACITY; ++i) children[i] = nullptr;
        delete[] parent->children;
        parent->children = children;
        parent->children_capacity = CAPACITY;
    }
    bool const LINK_COUNT_CHANGED = DISPLACED != nullptr && DISPLACED->type != TmpNodeType::DIRECTORY && tmpfs_link_count(DISPLACED) > 1;
    if (DESTINATION != nullptr) {
        (void)tmpfs_detach_child(destination.parent(), DESTINATION);
        if (DESTINATION->type == TmpNodeType::DIRECTORY) {
            DESTINATION->unlinked = true;
            if (DESTINATION->open_count.load(std::memory_order_acquire) == 0) tmpfs_free_node(DESTINATION);
        } else {
            DISPLACED->ctime = current_timespec();
            tmpfs_drop_detached_node(DESTINATION);
        }
    }
    (void)tmpfs_detach_child(source.parent(), SOURCE);
    copy_name(SOURCE->name, destination.name());
    (void)tmpfs_attach_child(destination.parent(), SOURCE);
    CANONICAL->ctime = current_timespec();
    tmpfs_fill_metadata_locked(CANONICAL, out);
    if (replaced_hardlink_count_changed != nullptr) *replaced_hardlink_count_changed = LINK_COUNT_CHANGED;
    if (namespace_changed != nullptr) *namespace_changed = true;
    return 0;
}

namespace {

// Internal unlocked version - caller must hold tmpfs_lock
auto tmpfs_walk_path_unlocked(TmpNode* root, const char* path, bool create_intermediate) -> TmpNode* {
    if (root == nullptr) {
        return nullptr;
    }

    // Skip leading slashes
    while (*path == '/') {
        path++;
    }

    // Empty path means root
    if (*path == '\0') {
        return root;
    }

    TmpNode* current = root;

    // Parse path component by component
    while (*path != '\0') {
        // Skip consecutive slashes
        while (*path == '/') {
            path++;
        }
        if (*path == '\0') {
            break;
        }

        // Extract the next component
        std::array<char, TMPFS_NAME_MAX> component{};
        size_t comp_len = 0;
        while (path[comp_len] != '\0' && path[comp_len] != '/' && comp_len < TMPFS_NAME_MAX - 1) {
            component.at(comp_len) = path[comp_len];
            comp_len++;
        }
        component.at(comp_len) = '\0';
        path += comp_len;

        // Handle "." and ".."
        if (kstrcmp(component.data(), ".") == 0) {
            continue;
        }
        if (kstrcmp(component.data(), "..") == 0) {
            if (current->parent != nullptr) {
                current = current->parent;
            }
            continue;
        }

        // Current node must be a directory to descend into
        if (current->type != TmpNodeType::DIRECTORY) {
            return nullptr;
        }

        TmpNode* child = tmpfs_lookup(current, component.data());
        if (child == nullptr) {
            if (!create_intermediate) {
                return nullptr;
            }
            // All missing components are created as directories;
            // the caller can convert the final node to a different type if needed
            child = tmpfs_mkdir(current, component.data());
            if (child == nullptr) {
                return nullptr;
            }
        }

        current = child;
    }

    return current;
}

}  // namespace

auto tmpfs_walk_path(TmpNode* root, const char* path, bool create_intermediate) -> TmpNode* {
    if (path == nullptr || root == nullptr) {
        return nullptr;
    }
    tmpfs_lock.lock();
    TmpNode* result = tmpfs_walk_path_unlocked(root, path, create_intermediate);
    tmpfs_lock.unlock();
    return result;
}

auto tmpfs_walk_path(const char* path, bool create_intermediate) -> TmpNode* {
    return tmpfs_walk_path(root_node, path, create_intermediate);
}

auto tmpfs_mkdir_path(const ker::mod::security::Snapshot& credentials, TmpNode* root, const char* path, uint32_t mode, bool read_only)
    -> int {
    if (root == nullptr || path == nullptr) {
        return -EINVAL;
    }

    while (*path == '/') {
        ++path;
    }
    if (*path == '\0') {
        return -EEXIST;
    }

    tmpfs_lock.lock();
    int const RESULT = [&]() -> int {
        TmpNode* parent = root;
        for (;;) {
            std::array<char, TMPFS_NAME_MAX> component{};
            size_t component_len = 0;
            while (path[component_len] != '\0' && path[component_len] != '/') {
                if (component_len >= component.size() - 1) {
                    return -ENAMETOOLONG;
                }
                component.at(component_len) = path[component_len];
                ++component_len;
            }
            component.at(component_len) = '\0';
            path += component_len;
            while (*path == '/') {
                ++path;
            }

            if (parent->type != TmpNodeType::DIRECTORY) {
                return -ENOTDIR;
            }

            TmpNode* child = tmpfs_lookup(parent, component.data());
            if (*path == '\0') {
                ker::mod::security::FileIdentity identity{};
                int const AUTH = ker::mod::security::prepare_directory_create(
                    credentials, {parent->uid, parent->gid, 0040000U | parent->mode}, mode, identity);
                if (AUTH != 0) return AUTH;
                if (child != nullptr) {
                    return -EEXIST;
                }
                if (read_only) {
                    return -EROFS;
                }
                child = tmpfs_mkdir(parent, component.data());
                if (child == nullptr) {
                    return -ENOMEM;
                }
                child->uid = identity.uid;
                child->gid = identity.gid;
                child->mode = identity.mode & 07777U;
                return 0;
            }

            if (child == nullptr) {
                return -ENOENT;
            }
            parent = child;
        }
    }();
    tmpfs_lock.unlock();
    return RESULT;
}

// --- Initialization ---

void register_tmpfs() {
    vfs_debug_log("tmpfs: register_tmpfs called\n");
    if (root_node == nullptr) {
        root_node = create_root_node_internal();
    }
    register_tmpfs_shrinker();
    register_tmpfs_swap_consumer();
}

auto create_root_node() -> TmpNode* { return create_root_node_internal(); }

auto get_root_node() -> TmpNode* { return root_node; }

namespace {
void free_tree(TmpNode* node) {
    if (node == nullptr) {
        return;
    }
    for (size_t i = 0; i < node->children_count; ++i) {
        free_tree(node->children[i]);
        node->children[i] = nullptr;
    }
    node->children_count = 0;
    node->children_live_count = 0;
    tmpfs_free_node(node);
}
}  // namespace

auto create_mount_context(TmpNode* root, const char* options, bool root_compat, int* error_out) -> TmpfsMount* {
    if (error_out != nullptr) {
        *error_out = 0;
    }
    if (root == nullptr) {
        if (error_out != nullptr) {
            *error_out = -EINVAL;
        }
        return nullptr;
    }
    size_t max_bytes = 0;
    int const OPTIONS_RET = parse_mount_options(options, root_compat, &max_bytes);
    if (OPTIONS_RET < 0) {
        if (error_out != nullptr) {
            *error_out = OPTIONS_RET;
        }
        return nullptr;
    }
    auto* mount = new TmpfsMount;
    if (mount == nullptr) {
        if (error_out != nullptr) {
            *error_out = -ENOMEM;
        }
        return nullptr;
    }
    mount->root = root;
    mount->max_bytes = max_bytes;
    mount->used_bytes = 0;
    mount->root_compat = root_compat;
    node_set_mount_recursive(root, mount);
    return mount;
}

void destroy_mount_context(TmpfsMount* mount) {
    if (mount == nullptr) {
        return;
    }
    if (!mount->root_compat) {
        free_tree(mount->root);
    } else if (mount->root != nullptr) {
        node_set_mount_recursive(mount->root, nullptr);
    }
    delete mount;
}

auto mount_root(TmpfsMount* mount) -> TmpNode* { return mount != nullptr ? mount->root : get_root_node(); }

auto tmpfs_statvfs(TmpfsMount* mount, ker::vfs::Statvfs* buf) -> int {
    if (buf == nullptr) {
        return -EINVAL;
    }
    std::memset(buf, 0, sizeof(*buf));
    size_t max_bytes = 0;
    size_t used_bytes = 0;
    if (mount != nullptr) {
        ker::mod::sys::MutexGuard guard(mount->accounting_lock);
        max_bytes = mount->max_bytes;
        used_bytes = mount->used_bytes;
    }
    if (max_bytes == 0) {
        max_bytes = static_cast<size_t>(ker::mod::mm::phys::get_total_mem_bytes());
        used_bytes = max_bytes - std::min<size_t>(max_bytes, static_cast<size_t>(ker::mod::mm::phys::get_free_mem_bytes()));
    }
    size_t const FREE_BYTES = (used_bytes < max_bytes) ? max_bytes - used_bytes : 0;
    buf->f_bsize = DEFAULT_TMPFS_BLOCK_SIZE;
    buf->f_frsize = DEFAULT_TMPFS_BLOCK_SIZE;
    buf->f_blocks = max_bytes / DEFAULT_TMPFS_BLOCK_SIZE;
    buf->f_bfree = FREE_BYTES / DEFAULT_TMPFS_BLOCK_SIZE;
    buf->f_bavail = buf->f_bfree;
    buf->f_files = buf->f_blocks;
    buf->f_ffree = buf->f_bfree;
    buf->f_favail = buf->f_bfree;
    buf->f_namemax = TMPFS_NAME_MAX - 1;
    return 0;
}

auto tmpfs_reclaim_pages(size_t target_pages) -> size_t {
    if (target_pages == 0 || !ker::mod::mm::swap::swap_available()) {
        return 0;
    }
    size_t reclaimed = 0;
    size_t index = 0;
    while (reclaimed < target_pages) {
        TmpNode* node = nullptr;
        bool done = false;
        tmpfs_node_registry_lock.lock();
        if (index < tmpfs_nodes.size()) {
            node = tmpfs_nodes.at(index++);
            if (node != nullptr && !node->io_lock.try_lock()) {
                node = nullptr;
            }
        } else {
            done = true;
        }
        tmpfs_node_registry_lock.unlock();
        if (done) {
            break;
        }
        if (node == nullptr) {
            continue;
        }
        TmpNode* canonical = tmpfs_canonical_node(node);
        if (canonical != nullptr && canonical->type == TmpNodeType::FILE && canonical == node) {
            reclaimed += reclaim_from_node_locked(canonical, target_pages - reclaimed);
        }
        node->io_lock.unlock();
    }
    return reclaimed;
}

// --- File-level operations ---

namespace {

auto create_root_file_with_flags(TmpNode* root, int open_flags) -> ker::vfs::File* {
    if (root == nullptr) {
        return nullptr;
    }
    root->open_count.fetch_add(1, std::memory_order_relaxed);
    auto* f = new File;
    f->private_data = root;
    f->fd = -1;
    f->pos = 0;
    f->is_directory = true;
    f->fs_type = FSType::TMPFS;
    f->refcount = 1;
    f->open_flags = open_flags;
    f->fd_flags = 0;
    return f;
}

}  // namespace

auto create_root_file(TmpNode* root) -> ker::vfs::File* { return create_root_file_with_flags(root, 0); }

auto create_root_file() -> ker::vfs::File* { return create_root_file_with_flags(root_node, 0); }

namespace {
void tmpfs_set_open_result(int* result_out, int result) {
    if (result_out != nullptr) {
        *result_out = result;
    }
}

}  // namespace

auto tmpfs_open_lookup(const ker::mod::security::Snapshot& credentials, const TmpfsLookupHandle& handle, int flags, int mode,
                       bool require_directory, int* result_out) -> ker::vfs::File* {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    auto fail = [&](int error) -> File* {
        tmpfs_set_open_result(result_out, error);
        return nullptr;
    };
    if (!tmpfs_validate_lookup_locked(handle)) return fail(-EAGAIN);
    unsigned const ACCESS_MODE = static_cast<unsigned>(flags) & 3U;
    if (ACCESS_MODE > 2) return fail(-EINVAL);
    bool const REQUIRE_DIRECTORY = require_directory || (flags & O_DIRECTORY) != 0;
    if ((flags & (O_DIRECTORY | O_CREAT)) == (O_DIRECTORY | O_CREAT)) return fail(-EINVAL);
    TmpNode* const PARENT = handle.parent();
    if (!handle.root_target()) {
        if (int const RET = ker::mod::security::authorize_file_access(credentials, {PARENT->uid, PARENT->gid, 0040000U | PARENT->mode}, 1);
            RET != 0)
            return fail(RET);
    }
    bool const CREATED = !handle.target_existed();
    TmpNode* node = tmpfs_canonical_node(handle.target());
    ker::mod::security::FileIdentity created_identity{};
    if (CREATED) {
        if ((flags & O_CREAT) == 0 || REQUIRE_DIRECTORY) return fail(-ENOENT);
        int const RET = ker::mod::security::prepare_file_create(credentials, {PARENT->uid, PARENT->gid, 0040000U | PARENT->mode},
                                                                static_cast<uint32_t>(mode), created_identity);
        if (RET != 0) return fail(RET);
    } else {
        if ((flags & (O_CREAT | O_EXCL)) == (O_CREAT | O_EXCL)) return fail(-EEXIST);
        if ((flags & O_NOFOLLOW) != 0 && handle.target()->type == TmpNodeType::SYMLINK) return fail(-ELOOP);
        if (node == nullptr) return fail(-ENOENT);
        if (REQUIRE_DIRECTORY && node->type != TmpNodeType::DIRECTORY) return fail(-ENOTDIR);
        uint32_t const TYPE = node->type == TmpNodeType::DIRECTORY ? 0040000U : (node->type == TmpNodeType::SYMLINK ? 0120000U : 0100000U);
        int const RET = ker::mod::security::authorize_file_open(credentials, {node->uid, node->gid, TYPE | node->mode}, ACCESS_MODE,
                                                                (flags & O_TRUNC) != 0);
        if (RET != 0) return fail(RET);
    }
    // Complete all authorization and File allocation before publishing a new
    // dentry or truncating an existing canonical inode.
    auto* file = new File;
    if (file == nullptr) return fail(-ENOMEM);
    if (CREATED) {
        node = tmpfs_create_file_with_identity(PARENT, handle.name(), created_identity);
        if (node == nullptr) {
            delete file;
            return fail(-ENOMEM);
        }
    }
    if (!CREATED && (flags & O_TRUNC) != 0 && node->type == TmpNodeType::FILE) {
        ker::mod::sys::MutexGuard io_guard(node->io_lock);
        int const RET = tmpfs_resize_locked(node, 0);
        if (RET != 0) {
            delete file;
            return fail(RET);
        }
    }
    node->open_count.fetch_add(1, std::memory_order_relaxed);
    file->private_data = node;
    file->fd = -1;
    file->pos = 0;
    file->is_directory = node->type == TmpNodeType::DIRECTORY;
    file->fs_type = FSType::TMPFS;
    file->refcount = 1;
    file->open_flags = flags;
    file->fd_flags = 0;
    file->open_create_result_known = (flags & O_CREAT) != 0;
    file->created_by_open = CREATED;
    tmpfs_set_open_result(result_out, 0);
    return file;
}

auto tmpfs_pin_directory_lookup(const ker::mod::security::Snapshot& credentials, const TmpfsLookupHandle& handle, int* result_out,
                                bool require_target_search) -> File* {
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    auto fail = [&](int error) -> File* {
        tmpfs_set_open_result(result_out, error);
        return nullptr;
    };
    if (!tmpfs_validate_lookup_locked(handle)) return fail(-EAGAIN);
    auto* node = tmpfs_canonical_node(handle.target());
    if (node == nullptr) return fail(-ENOENT);
    if (node->type != TmpNodeType::DIRECTORY) return fail(-ENOTDIR);
    if (!handle.root_target()) {
        auto* parent = handle.parent();
        int const SEARCH = ker::mod::security::authorize_file_access(credentials, {parent->uid, parent->gid, 0040000U | parent->mode}, 1);
        if (SEARCH != 0) return fail(SEARCH);
    }
    if (require_target_search) {
        int const SEARCH = ker::mod::security::authorize_file_access(credentials, {node->uid, node->gid, 0040000U | node->mode}, 1);
        if (SEARCH != 0) return fail(SEARCH);
    }
    auto* file = new (std::nothrow) File{};
    if (file == nullptr) return fail(-ENOMEM);
    node->open_count.fetch_add(1, std::memory_order_relaxed);
    file->private_data = node;
    file->fops = get_tmpfs_fops();
    file->fd = -1;
    file->is_directory = true;
    file->fs_type = FSType::TMPFS;
    file->refcount = 1;
    file->open_flags = O_DIRECTORY | FILE_ACCESS_SEARCH_ONLY | O_NO_CACHE;
    tmpfs_set_open_result(result_out, 0);
    return file;
}

auto tmpfs_open_path(const ker::mod::security::Snapshot& credentials, TmpNode* root, const char* path, int flags, int mode, int* result_out)
    -> ker::vfs::File* {
    if (root == nullptr || path == nullptr) {
        tmpfs_set_open_result(result_out, -EINVAL);
        return nullptr;
    }
    size_t const LENGTH = std::strlen(path);
    bool const REQUIRE_DIRECTORY = LENGTH != 0 && path[LENGTH - 1] == '/';
    for (unsigned attempt = 0; attempt < 4; ++attempt) {
        TmpfsLookupHandle lookup;
        int result = tmpfs_acquire_lookup(credentials, root, path, &lookup);
        File* file = result == 0 ? tmpfs_open_lookup(credentials, lookup, flags, mode, REQUIRE_DIRECTORY, &result) : nullptr;
        if (result != -EAGAIN || attempt == 3) {
            tmpfs_set_open_result(result_out, result);
            return file;
        }
    }
    tmpfs_set_open_result(result_out, -EAGAIN);
    return nullptr;
}

auto tmpfs_open_path(const ker::mod::security::Snapshot& credentials, const char* path, int flags, int mode, int* result_out)
    -> ker::vfs::File* {
    return tmpfs_open_path(credentials, root_node, path, flags, mode, result_out);
}

auto tmpfs_read(ker::vfs::File* f, void* buf, size_t count, size_t offset) -> ssize_t {
    if (f != nullptr && (static_cast<unsigned>(f->open_flags) & 3U) == FILE_ACCESS_SEARCH_ONLY) return -EBADF;
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return -EBADF;
    }
    if (buf == nullptr && count != 0) {
        return -EINVAL;
    }
    auto* n = tmpfs_canonical_node(static_cast<TmpNode*>(f->private_data));
    if (n == nullptr) {
        return -EBADF;
    }
    ker::mod::sys::MutexGuard guard(n->io_lock);
    if (offset >= n->size) {
        return 0;
    }
    size_t to_read = n->size - offset;
    to_read = std::min(to_read, count);
    auto* dst = static_cast<uint8_t*>(buf);
    size_t total = 0;
    while (total < to_read) {
        size_t const POS = offset + total;
        size_t const PAGE_INDEX = POS / DEFAULT_TMPFS_BLOCK_SIZE;
        size_t const PAGE_OFF = POS % DEFAULT_TMPFS_BLOCK_SIZE;
        size_t const CHUNK = std::min(to_read - total, DEFAULT_TMPFS_BLOCK_SIZE - PAGE_OFF);
        if (PAGE_INDEX >= n->page_count || n->pages[PAGE_INDEX].state == TmpPageState::HOLE) {
            std::memset(dst + total, 0, CHUNK);
        } else {
            TmpPage* page = nullptr;
            int const RET = ensure_page_resident_locked(n, PAGE_INDEX, &page);
            if (RET < 0) {
                return total != 0 ? static_cast<ssize_t>(total) : RET;
            }
            std::memcpy(dst + total, static_cast<uint8_t*>(page->data) + PAGE_OFF, CHUNK);
        }
        total += CHUNK;
    }
    return static_cast<ssize_t>(to_read);
}

auto tmpfs_write(ker::vfs::File* f, const void* buf, size_t count, size_t offset) -> ssize_t {
    if (f != nullptr && (static_cast<unsigned>(f->open_flags) & 3U) == FILE_ACCESS_SEARCH_ONLY) return -EBADF;
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return -EBADF;
    }
    auto* n = tmpfs_canonical_node(static_cast<TmpNode*>(f->private_data));
    if (n == nullptr) {
        return -EBADF;
    }
    if (n->type != TmpNodeType::FILE) {
        return -EISDIR;
    }
    ker::mod::sched::resource::FileGrowth growth;
    ker::mod::sys::MutexGuard guard(n->io_lock);
    if (!growth.bound_write(offset, count)) {
        return -EFBIG;
    }
    if (count == 0) {
        return 0;
    }
    ssize_t const RET = tmpfs_write_locked(n, buf, count, offset);
    if (RET > 0) {
        growth.committed(n->size);
    }
    return RET;
}

auto tmpfs_write_append(ker::vfs::File* f, const void* buf, size_t count, size_t* offset_out) -> ssize_t {
    if (f != nullptr && (static_cast<unsigned>(f->open_flags) & 3U) == FILE_ACCESS_SEARCH_ONLY) return -EBADF;
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return -EBADF;
    }
    auto* n = tmpfs_canonical_node(static_cast<TmpNode*>(f->private_data));
    if (n == nullptr) {
        return -EBADF;
    }
    if (n->type != TmpNodeType::FILE) {
        return -EISDIR;
    }
    ker::mod::sched::resource::FileGrowth growth;
    ker::mod::sys::MutexGuard guard(n->io_lock);
    size_t const OFFSET = n->size;
    if (!growth.bound_write(OFFSET, count)) {
        return -EFBIG;
    }
    ssize_t const RET = count == 0 ? 0 : tmpfs_write_locked(n, buf, count, OFFSET);
    if (RET > 0) {
        growth.committed(n->size);
    }
    if (RET >= 0 && offset_out != nullptr) {
        *offset_out = OFFSET;
    }
    return RET;
}

auto tmpfs_get_size(ker::vfs::File* f) -> size_t {
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return 0;
    }
    auto* n = tmpfs_canonical_node(static_cast<TmpNode*>(f->private_data));
    if (n == nullptr) {
        return 0;
    }
    ker::mod::sys::MutexGuard guard(n->io_lock);
    return n->size;
}

auto tmpfs_copy_file_contents(TmpNode* dst, TmpNode* src) -> int {
    dst = tmpfs_canonical_node(dst);
    src = tmpfs_canonical_node(src);
    if (dst == nullptr || src == nullptr || dst->type != TmpNodeType::FILE || src->type != TmpNodeType::FILE) {
        return -EINVAL;
    }
    if (dst == src) {
        return 0;
    }
    TmpNode* first = dst < src ? dst : src;
    TmpNode* second = dst < src ? src : dst;
    ker::mod::sys::MutexGuard first_guard(first->io_lock);
    ker::mod::sys::MutexGuard second_guard(second->io_lock);

    int ret = tmpfs_resize_locked(dst, 0);
    if (ret < 0) {
        return ret;
    }
    if (src->size == 0) {
        return 0;
    }
    if (!ensure_page_descriptors(dst, src->page_count)) {
        return -ENOMEM;
    }
    for (size_t i = 0; i < src->page_count; ++i) {
        if (src->pages[i].state == TmpPageState::HOLE) {
            continue;
        }
        TmpPage* src_page = nullptr;
        ret = ensure_page_resident_locked(src, i, &src_page);
        if (ret < 0) {
            return ret;
        }
        TmpPage* dst_page = nullptr;
        ret = ensure_page_resident_locked(dst, i, &dst_page);
        if (ret < 0) {
            return ret;
        }
        std::memcpy(dst_page->data, src_page->data, DEFAULT_TMPFS_BLOCK_SIZE);
    }
    dst->size = src->size;
    touch_modified(dst);
    return 0;
}

// --- FileOperations callbacks ---

auto tmpfs_fops_read(ker::vfs::File* f, void* buf, size_t count, size_t offset) -> ssize_t { return tmpfs_read(f, buf, count, offset); }

auto tmpfs_fops_write(ker::vfs::File* f, const void* buf, size_t count, size_t offset) -> ssize_t {
    return tmpfs_write(f, buf, count, offset);
}

auto tmpfs_fops_close(ker::vfs::File* f) -> int {
    if (f == nullptr || f->private_data == nullptr) {
        return 0;
    }
    auto* node = tmpfs_canonical_node(static_cast<TmpNode*>(f->private_data));
    if (node == nullptr) {
        f->private_data = nullptr;
        return 0;
    }
    ker::mod::sys::MutexGuard guard(tmpfs_lock);
    if (f->exec_read_view != nullptr) {
        ker::mod::sys::MutexGuard io_guard(node->io_lock);
        node->exec_content.release();
    }
    node->open_count.fetch_sub(1, std::memory_order_acq_rel);
    if (tmpfs_node_can_be_freed_locked(node)) {
        // Last open/lookup reference to an unlinked node — free it now.
        tmpfs_free_node(node);
    }
    f->private_data = nullptr;
    return 0;
}

auto tmpfs_fops_lseek(ker::vfs::File* f, off_t offset, int whence) -> off_t {
    if (f == nullptr) {
        return -EBADF;
    }

    size_t const FILE_SIZE = tmpfs_get_size(f);
    off_t newpos = 0;

    switch (whence) {
        case 0:  // SEEK_SET
            newpos = offset;
            break;
        case 1:  // SEEK_CUR
            newpos = f->pos + offset;
            break;
        case 2:  // SEEK_END
            newpos = static_cast<off_t>(FILE_SIZE) + offset;
            break;
        default:
            return -EINVAL;
    }

    if (newpos < 0) {
        return -EINVAL;
    }
    f->pos = newpos;
    return f->pos;
}

auto tmpfs_fops_isatty(ker::vfs::File* f) -> bool {
    (void)f;
    return false;
}

namespace {
auto tmpfs_fops_readdir(ker::vfs::File* f, DirEntry* entry, size_t index) -> int {
    if (f != nullptr && (static_cast<unsigned>(f->open_flags) & 3U) == FILE_ACCESS_SEARCH_ONLY) return -EBADF;
    if (entry == nullptr) {
        return -EINVAL;
    }
    if (f == nullptr || f->private_data == nullptr) {
        return -EBADF;
    }

    auto* n = static_cast<TmpNode*>(f->private_data);

    if (n->type != TmpNodeType::DIRECTORY) {
        return -ENOTDIR;
    }

    // Indices 0 and 1 are synthetic "." and ".." entries
    if (index == 0) {
        entry->d_ino = reinterpret_cast<uint64_t>(n);
        entry->d_off = 1;
        entry->d_reclen = sizeof(DirEntry);
        entry->d_type = DT_DIR;
        // DirEntry is a public ABI-style record with a raw d_name buffer.
        // NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        entry->d_name[0] = '.';
        entry->d_name[1] = '\0';
        // NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        return 0;
    }
    if (index == 1) {
        TmpNode const* parent = (n->parent != nullptr) ? n->parent : n;
        entry->d_ino = reinterpret_cast<uint64_t>(parent);
        entry->d_off = 2;
        entry->d_reclen = sizeof(DirEntry);
        entry->d_type = DT_DIR;
        // DirEntry is a public ABI-style record with a raw d_name buffer.
        // NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        entry->d_name[0] = '.';
        entry->d_name[1] = '.';
        entry->d_name[2] = '\0';
        // NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        return 0;
    }

    // Real children start at index 2.  The child array is sparse so open
    // directory streams keep stable offsets while entries are removed.
    size_t child_index = index - 2;

    while (child_index < n->children_count && n->children[child_index] == nullptr) {
        child_index++;
    }

    if (child_index >= n->children_count) {
        return -ENOENT;
    }

    TmpNode* child = n->children[child_index];
    TmpNode const* child_identity = tmpfs_canonical_node(child);
    entry->d_ino = reinterpret_cast<uint64_t>(child_identity != nullptr ? child_identity : child);
    entry->d_off = child_index + 3;
    entry->d_reclen = sizeof(DirEntry);

    switch (child->type) {
        case TmpNodeType::DIRECTORY:
            entry->d_type = DT_DIR;
            break;
        case TmpNodeType::SYMLINK:
            entry->d_type = DT_LNK;
            break;
        default:
            entry->d_type = DT_REG;
            break;
    }

    size_t name_len = 0;
    while (child->name.at(name_len) != '\0' && name_len < DIRENT_NAME_MAX - 1) {
        // DirEntry is a public ABI-style record with a raw d_name buffer.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        entry->d_name[name_len] = child->name.at(name_len);
        name_len++;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    entry->d_name[name_len] = '\0';

    return 0;
}

auto tmpfs_fops_readlink(ker::vfs::File* f, char* buf, size_t bufsize) -> ssize_t {
    if (buf == nullptr || bufsize == 0) {
        return -EINVAL;
    }
    if (f == nullptr || f->private_data == nullptr) {
        return -EBADF;
    }
    auto* n = tmpfs_canonical_node(static_cast<TmpNode*>(f->private_data));
    if (n == nullptr) {
        return -EBADF;
    }
    if (n->type != TmpNodeType::SYMLINK || n->symlink_target == nullptr) {
        return -EINVAL;
    }
    size_t len = 0;
    while (n->symlink_target[len] != '\0') {
        len++;
    }
    size_t const TO_COPY = (len < bufsize) ? len : bufsize;
    std::memcpy(buf, n->symlink_target, TO_COPY);
    return static_cast<ssize_t>(TO_COPY);
}

// --- FileOperations instance ---

auto tmpfs_fops_truncate(ker::vfs::File* f, off_t length) -> int {
    if (f != nullptr && (static_cast<unsigned>(f->open_flags) & 3U) == FILE_ACCESS_SEARCH_ONLY) return -EBADF;
    if (f == nullptr || f->private_data == nullptr) {
        return -EBADF;
    }
    auto* n = tmpfs_canonical_node(static_cast<TmpNode*>(f->private_data));
    if (n == nullptr) {
        return -EBADF;
    }
    if (n->type != TmpNodeType::FILE) {
        return -EISDIR;
    }
    if (length < 0) {
        return -EINVAL;
    }
    ker::mod::sched::resource::FileGrowth growth;
    auto const NEW_SIZE = static_cast<size_t>(length);
    if (!growth.allow_resize(NEW_SIZE)) {
        return -EFBIG;
    }
    ker::mod::sys::MutexGuard guard(n->io_lock);
    int const RET = tmpfs_resize_locked(n, NEW_SIZE);
    if (RET == 0) {
        growth.committed(n->size);
    }
    return RET;
}

ker::vfs::FileOperations tmpfs_fops_instance = {
    .vfs_open = nullptr,
    .vfs_close = tmpfs_fops_close,
    .vfs_read = tmpfs_fops_read,
    .vfs_write = tmpfs_fops_write,
    .vfs_lseek = tmpfs_fops_lseek,
    .vfs_isatty = tmpfs_fops_isatty,
    .vfs_readdir = tmpfs_fops_readdir,
    .vfs_readlink = tmpfs_fops_readlink,
    .vfs_truncate = tmpfs_fops_truncate,
    .vfs_poll_check = nullptr,
    .vfs_ioctl = nullptr,
    .vfs_poll_register_waiter = nullptr,
};
}  // namespace

auto get_tmpfs_fops() -> ker::vfs::FileOperations* { return &tmpfs_fops_instance; }

}  // namespace ker::vfs::tmpfs
