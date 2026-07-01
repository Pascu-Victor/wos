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
#include <platform/ktime/ktime.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/swap.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/smallvec.hpp>
#include <vfs/file.hpp>
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
constexpr uint64_t TMPFS_MIN_FREE_RESERVE_BYTES = 16ULL * 1024ULL * 1024ULL;
constexpr uint64_t TMPFS_MAX_FREE_RESERVE_BYTES = 256ULL * 1024ULL * 1024ULL;
constexpr uint64_t TMPFS_FREE_RESERVE_DIVISOR = 8;
constexpr uint64_t NS_PER_SEC = 1000000000ULL;

namespace {
TmpNode* root_node = nullptr;                   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock tmpfs_lock;             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Mutex tmpfs_node_registry_lock;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::util::SmallVec<TmpNode*, 64> tmpfs_nodes;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
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
    static_cast<void>(tmpfs_nodes.push_back(node));
}

void unregister_tmp_node(TmpNode* node) {
    if (node == nullptr) {
        return;
    }
    ker::mod::sys::MutexGuard guard(tmpfs_node_registry_lock);
    static_cast<void>(tmpfs_nodes.remove(node));
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
    if (page.state == TmpPageState::RESIDENT && page.data != nullptr) {
        ker::mod::mm::phys::page_free(page.data);
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
    return 0;
}

auto reclaim_from_node_locked(TmpNode* node, size_t target_pages, size_t skip_index = static_cast<size_t>(-1)) -> size_t {
    if (node == nullptr || target_pages == 0 || !ker::mod::mm::swap::swap_available()) {
        return 0;
    }
    size_t reclaimed = 0;
    for (size_t i = 0; i < node->page_count && reclaimed < target_pages; ++i) {
        if (i == skip_index) {
            continue;
        }
        if (evict_page_locked(node, i) == 0) {
            reclaimed++;
        }
    }
    return reclaimed;
}

auto allocate_resident_page(TmpNode* node, size_t page_index) -> void* {
    if (!tmpfs_can_allocate_page()) {
        static_cast<void>(reclaim_from_node_locked(node, 1, page_index));
    }
    void* page = ker::mod::mm::phys::page_alloc_may_fail(DEFAULT_TMPFS_BLOCK_SIZE, "tmpfs_page");
    if (page != nullptr) {
        return page;
    }
    static_cast<void>(reclaim_from_node_locked(node, 4, page_index));
    return ker::mod::mm::phys::page_alloc_may_fail(DEFAULT_TMPFS_BLOCK_SIZE, "tmpfs_page");
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
        static_cast<void>(ker::mod::mm::swap::free_slot(page.swap_slot));
    } else {
        std::memset(data, 0, DEFAULT_TMPFS_BLOCK_SIZE);
        node->charged_pages++;
    }
    page.state = TmpPageState::RESIDENT;
    page.data = data;
    page.swap_slot = ker::mod::mm::swap::invalid_slot();
    *out_page = &page;
    return 0;
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
    touch_modified(parent);
}

auto tmpfs_write_locked(TmpNode* n, const void* buf, size_t count, size_t offset) -> ssize_t {
    if (count == 0) {
        return 0;
    }
    if (n == nullptr || buf == nullptr) {
        return -EINVAL;
    }
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
    stamp_new_node(node);
    register_tmp_node(node);
    return node;
}
}  // namespace

void tmpfs_free_node(TmpNode* node) {
    if (node == nullptr) {
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
    stamp_new_node(node);
    register_tmp_node(node);
    add_child(parent, node);
    return node;
}

auto tmpfs_create_file(TmpNode* parent, const char* name, uint32_t create_mode) -> TmpNode* {
    if (parent == nullptr || name == nullptr || parent->type != TmpNodeType::DIRECTORY) {
        return nullptr;
    }
    TmpNode* existing = tmpfs_lookup(parent, name);
    if (existing != nullptr) {
        return existing;  // Return existing node
    }
    auto* node = new TmpNode;
    copy_name(node->name, name);
    node->type = TmpNodeType::FILE;
    node->mount = parent->mount;
    node->mode = create_mode & 07777;
    stamp_new_node(node);
    register_tmp_node(node);
    add_child(parent, node);
    return node;
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
    stamp_new_node(node);
    // Allocate and copy the symlink target
    size_t target_len = 0;
    while (target[target_len] != '\0') {
        target_len++;
    }
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

    if (node != canonical) {
        tmpfs_free_node(node);
    }

    if (canonical != nullptr && last_link) {
        canonical->unlinked = true;
        if (canonical->open_count.load(std::memory_order_acquire) == 0) {
            tmpfs_free_node(canonical);
        }
    }
}

auto tmpfs_directory_is_empty(const TmpNode* dir) -> bool {
    return dir != nullptr && dir->type == TmpNodeType::DIRECTORY && dir->children_live_count == 0;
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

// --- Initialization ---

void register_tmpfs() {
    vfs_debug_log("tmpfs: register_tmpfs called\n");
    if (root_node == nullptr) {
        root_node = create_root_node_internal();
    }
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

auto create_root_file(TmpNode* root) -> ker::vfs::File* {
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
    return f;
}

auto create_root_file() -> ker::vfs::File* { return create_root_file(root_node); }

auto tmpfs_open_path(TmpNode* root, const char* path, int flags, int mode) -> ker::vfs::File* {
    // mode is now used for O_CREAT
    if (path == nullptr || root == nullptr) {
        return nullptr;
    }

    // Handle root path
    if (path[0] == '\0' || (path[0] == '/' && path[1] == '\0')) {
        return create_root_file(root);
    }

    // Skip leading slash for walk_path
    const char* rel_path = path;
    if (rel_path[0] == '/') {
        rel_path++;
    }
    if (rel_path[0] == '\0') {
        return create_root_file(root);
    }

    // Split path into parent path and final component
    // Find the last '/' to separate parent from name
    const char* last_slash = nullptr;
    for (const char* p = rel_path; *p != '\0'; p++) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    TmpNode* node = nullptr;
    bool created_by_open = false;

    tmpfs_lock.lock();
    if (last_slash == nullptr) {
        // Single component path (e.g., "file.txt")
        node = tmpfs_lookup(root, rel_path);
        if (node == nullptr && (flags & O_CREAT) != 0) {
            node = tmpfs_create_file(root, rel_path, static_cast<uint32_t>(mode) & 07777);
            created_by_open = node != nullptr;
        }
    } else {
        // Multi-component path (e.g., "etc/fstab")
        // Walk to the parent directory
        constexpr size_t MAX_PATH_LEN = 512;
        auto parent_len = static_cast<size_t>(last_slash - rel_path);
        std::array<char, MAX_PATH_LEN> parent_path{};
        if (parent_len >= MAX_PATH_LEN) {
            tmpfs_lock.unlock();
            return nullptr;
        }
        std::memcpy(parent_path.data(), rel_path, parent_len);
        parent_path.at(parent_len) = '\0';

        const char* final_name = last_slash + 1;
        if (*final_name == '\0') {
            // Path ends with '/' - open the directory itself
            node = tmpfs_walk_path_unlocked(root, parent_path.data(), (flags & O_CREAT) != 0);
        } else {
            // Walk to parent, then lookup/create the final component
            TmpNode* parent = tmpfs_walk_path_unlocked(root, parent_path.data(), (flags & O_CREAT) != 0);
            if (parent == nullptr) {
                tmpfs_lock.unlock();
                return nullptr;
            }
            node = tmpfs_lookup(parent, final_name);
            if (node == nullptr && (flags & O_CREAT) != 0) {
                node = tmpfs_create_file(parent, final_name, static_cast<uint32_t>(mode) & 07777);
                created_by_open = node != nullptr;
            }
        }
    }
    TmpNode* file_node = tmpfs_canonical_node(node);
    if (file_node != nullptr) {
        file_node->open_count.fetch_add(1, std::memory_order_relaxed);
    }
    tmpfs_lock.unlock();

    if (file_node == nullptr) {
        return nullptr;
    }

    if ((flags & ker::vfs::O_TRUNC) != 0 && file_node->type == TmpNodeType::FILE) {
        int truncate_ret = 0;
        {
            ker::mod::sys::MutexGuard guard(file_node->io_lock);
            truncate_ret = tmpfs_resize_locked(file_node, 0);
        }
        int const TRUNCATE_RET = truncate_ret;
        if (TRUNCATE_RET < 0) {
            uint32_t const PREV = file_node->open_count.fetch_sub(1, std::memory_order_acq_rel);
            if (PREV == 1 && file_node->unlinked) {
                tmpfs_free_node(file_node);
            }
            return nullptr;
        }
    }

    auto* f = new File;
    f->private_data = file_node;
    f->fd = -1;
    f->pos = 0;
    f->is_directory = (file_node->type == TmpNodeType::DIRECTORY);
    f->fs_type = FSType::TMPFS;
    f->refcount = 1;
    f->open_create_result_known = (flags & O_CREAT) != 0;
    f->created_by_open = created_by_open;
    return f;
}

auto tmpfs_open_path(const char* path, int flags, int mode) -> ker::vfs::File* { return tmpfs_open_path(root_node, path, flags, mode); }

auto tmpfs_read(ker::vfs::File* f, void* buf, size_t count, size_t offset) -> ssize_t {
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
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return -EBADF;
    }
    auto* n = tmpfs_canonical_node(static_cast<TmpNode*>(f->private_data));
    if (n == nullptr) {
        return -EBADF;
    }
    ker::mod::sys::MutexGuard guard(n->io_lock);
    return tmpfs_write_locked(n, buf, count, offset);
}

auto tmpfs_write_append(ker::vfs::File* f, const void* buf, size_t count, size_t* offset_out) -> ssize_t {
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return -EBADF;
    }
    auto* n = tmpfs_canonical_node(static_cast<TmpNode*>(f->private_data));
    if (n == nullptr) {
        return -EBADF;
    }
    ker::mod::sys::MutexGuard guard(n->io_lock);
    size_t const OFFSET = n->size;
    ssize_t const RET = tmpfs_write_locked(n, buf, count, OFFSET);
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
    uint32_t const PREV = node->open_count.fetch_sub(1, std::memory_order_acq_rel);
    if (PREV == 1 && node->unlinked) {
        // Last close of an unlinked node — free it now
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
    ker::mod::sys::MutexGuard guard(n->io_lock);
    auto const NEW_SIZE = static_cast<size_t>(length);
    return tmpfs_resize_locked(n, NEW_SIZE);
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
