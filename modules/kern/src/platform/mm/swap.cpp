#include "swap.hpp"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dev/block_device.hpp>
#include <platform/mm/paging.hpp>
#include <platform/sys/mutex.hpp>
#include <util/crc32c.hpp>
#include <util/smallvec.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/fs/xfs/xfs_vfs.hpp>
#include <vfs/mount.hpp>
#include <vfs/vfs.hpp>

namespace ker::mod::mm::swap {
namespace {

constexpr size_t MAX_SWAP_NAME = 256;
constexpr uint32_t MAX_SWAP_AREAS = 32;
constexpr size_t MAX_SWAP_CONSUMERS = 8;
constexpr size_t PAGE_SIZE = ker::mod::mm::paging::PAGE_SIZE;
constexpr uint64_t NO_FREE_SLOT = UINT64_MAX;

enum class SlotState : uint8_t { FREE, RESERVED, STORED, RETIRED };
enum class AreaState : uint8_t { ACTIVE, DRAINING };

struct SlotMetadata {
    uint64_t generation{};
    uint64_t next_free = NO_FREE_SLOT;
    uint32_t checksum{};
    SlotState state = SlotState::FREE;
    bool io_in_flight{};
};

struct SwapArea {
    uint32_t id{};
    char* name{};
    SwapExtent* extents{};
    size_t extent_count{};
    SlotMetadata* slots{};
    uint64_t free_head = NO_FREE_SLOT;
    uint64_t total_pages{};
    uint64_t free_pages{};
    uint64_t used_pages{};
    AreaState state = AreaState::ACTIVE;
    ker::vfs::File* backing_file{};
};

ker::mod::sys::Mutex g_swap_lock;                               // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::util::SmallVec<SwapArea*, 4> g_swap_areas;                 // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<SwapConsumer, MAX_SWAP_CONSUMERS> g_swap_consumers;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t g_swap_consumer_count{};                                 // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_next_area_id = 1;                                    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto copy_name(const char* name) -> char* {
    if (name == nullptr) {
        name = "";
    }
    size_t len = std::strlen(name);
    if (len >= MAX_SWAP_NAME) {
        len = MAX_SWAP_NAME - 1;
    }
    auto* out = new char[len + 1];
    if (out == nullptr) {
        return nullptr;
    }
    std::memcpy(out, name, len);
    out[len] = '\0';
    return out;
}

void destroy_area(SwapArea* area) {
    if (area == nullptr) {
        return;
    }
    delete[] area->name;
    delete[] area->extents;
    delete[] area->slots;
    if (area->backing_file != nullptr) {
        ker::vfs::vfs_close_file(area->backing_file);
    }
    delete area;
}

auto block_device_usable_for_swap(ker::dev::BlockDevice* device) -> bool {
    return device != nullptr && device->block_size != 0 && PAGE_SIZE % device->block_size == 0 && device->read_blocks != nullptr &&
           device->write_blocks != nullptr && !ker::dev::block_device_is_read_only(device);
}

auto slot_to_extent_locked(SwapArea* area, uint64_t slot, SwapExtent* out) -> bool {
    if (area == nullptr || out == nullptr || slot >= area->total_pages) {
        return false;
    }
    uint64_t base = 0;
    for (size_t i = 0; i < area->extent_count; ++i) {
        SwapExtent const& extent = area->extents[i];
        if (slot < base + extent.page_count) {
            *out = extent;
            out->start_block += (slot - base) * (PAGE_SIZE / extent.device->block_size);
            out->page_count = 1;
            return true;
        }
        base += extent.page_count;
    }
    return false;
}

auto slot_metadata_locked(SwapArea* area, SwapSlot slot, SlotMetadata** out) -> int {
    if (area == nullptr || out == nullptr || slot.index >= area->total_pages) {
        return -EINVAL;
    }
    SlotMetadata& metadata = area->slots[slot.index];
    if (metadata.generation != slot.generation) {
        return -ESTALE;
    }
    if (metadata.state == SlotState::FREE || metadata.state == SlotState::RETIRED) {
        return -EINVAL;
    }
    *out = &metadata;
    return 0;
}

auto find_area_locked(uint32_t area_id) -> SwapArea* {
    for (auto* area : g_swap_areas) {
        if (area != nullptr && area->id == area_id) {
            return area;
        }
    }
    return nullptr;
}

auto find_area_by_name_locked(const char* name) -> SwapArea* {
    if (name == nullptr) {
        return nullptr;
    }
    for (auto* area : g_swap_areas) {
        if (area != nullptr && area->name != nullptr && std::strcmp(area->name, name) == 0) {
            return area;
        }
    }
    return nullptr;
}

auto activate_extents_locked(const char* name, const SwapExtent* extents, size_t extent_count, ker::vfs::File* backing_file) -> int {
    if (name == nullptr || std::strlen(name) >= MAX_SWAP_NAME || extents == nullptr || extent_count == 0 ||
        g_swap_areas.size() >= MAX_SWAP_AREAS || find_area_by_name_locked(name) != nullptr) {
        return -EINVAL;
    }

    uint64_t total_pages = 0;
    for (size_t i = 0; i < extent_count; ++i) {
        if (!block_device_usable_for_swap(extents[i].device) || extents[i].page_count == 0 ||
            total_pages > UINT64_MAX - extents[i].page_count) {
            return -EINVAL;
        }
        uint64_t const BLOCKS_PER_PAGE = PAGE_SIZE / extents[i].device->block_size;
        if (extents[i].page_count > UINT64_MAX / BLOCKS_PER_PAGE) {
            return -EINVAL;
        }
        uint64_t const EXTENT_BLOCKS = extents[i].page_count * BLOCKS_PER_PAGE;
        if (extents[i].start_block > extents[i].device->total_blocks ||
            EXTENT_BLOCKS > extents[i].device->total_blocks - extents[i].start_block) {
            return -EINVAL;
        }
        total_pages += extents[i].page_count;
    }
    if (total_pages == 0 || total_pages > SIZE_MAX) {
        return -EINVAL;
    }

    auto* area = new SwapArea;
    if (area == nullptr) {
        return -ENOMEM;
    }
    area->name = copy_name(name);
    area->extents = new SwapExtent[extent_count];
    area->slots = new SlotMetadata[static_cast<size_t>(total_pages)];
    if (area->name == nullptr || area->extents == nullptr || area->slots == nullptr) {
        destroy_area(area);
        return -ENOMEM;
    }
    for (size_t i = 0; i < extent_count; ++i) {
        area->extents[i] = extents[i];
    }
    for (uint64_t i = 0; i < total_pages; ++i) {
        area->slots[i].next_free = i + 1 < total_pages ? i + 1 : NO_FREE_SLOT;
    }
    area->free_head = 0;
    area->extent_count = extent_count;
    area->total_pages = total_pages;
    area->free_pages = total_pages;
    area->used_pages = 0;
    area->state = AreaState::ACTIVE;
    area->id = 0;
    for (uint32_t attempt = 0; attempt <= MAX_SWAP_AREAS; ++attempt) {
        uint32_t const CANDIDATE = g_next_area_id++;
        if (CANDIDATE != 0 && CANDIDATE != UINT32_MAX && find_area_locked(CANDIDATE) == nullptr) {
            area->id = CANDIDATE;
            break;
        }
    }
    if (area->id == 0) {
        destroy_area(area);
        return -EOVERFLOW;
    }
    if (!g_swap_areas.push_back(area)) {
        destroy_area(area);
        return -ENOMEM;
    }
    // Ownership transfers only after every fallible activation step. Keeping
    // the open file pins its mount and XFS private state until swapoff.
    area->backing_file = backing_file;
    return 0;
}

auto resolve_dev_path(const char* path) -> ker::dev::BlockDevice* {
    if (path == nullptr || std::strncmp(path, "/dev/", 5) != 0) {
        return nullptr;
    }
    const char* name = path + 5;
    auto* device = ker::dev::block_device_find_by_name(name);
    if (device != nullptr) {
        return device;
    }
    return ker::vfs::devfs::devfs_resolve_block_device(name);
}

}  // namespace

auto activate_block_device(ker::dev::BlockDevice* device, const char* name) -> int {
    if (!block_device_usable_for_swap(device)) {
        return -EINVAL;
    }
    if (ker::vfs::mounted_block_device_overlaps(device)) {
        return -EBUSY;
    }
    uint64_t const BLOCKS_PER_PAGE = PAGE_SIZE / device->block_size;
    uint64_t const PAGES = device->total_blocks / BLOCKS_PER_PAGE;
    if (PAGES == 0) {
        return -EINVAL;
    }
    SwapExtent extent{.device = device, .start_block = 0, .page_count = PAGES};
    return activate_extents(name, &extent, 1);
}

auto activate_extents(const char* name, const SwapExtent* extents, size_t extent_count) -> int {
    ker::mod::sys::MutexGuard guard(g_swap_lock);
    return activate_extents_locked(name, extents, extent_count, nullptr);
}

auto swapon_path(const char* path, int flags) -> int {
    (void)flags;
    if (path == nullptr || path[0] == '\0') {
        return -EINVAL;
    }

    if (auto* device = resolve_dev_path(path); device != nullptr) {
        return activate_block_device(device, path);
    }

    constexpr int O_RDWR = 2;
    auto* file = ker::vfs::vfs_open_file(path, O_RDWR | ker::vfs::O_LOCAL | ker::vfs::O_NO_CACHE, 0);
    if (file == nullptr) {
        return -ENOENT;
    }
    if (file->fs_type != ker::vfs::FSType::XFS || file->is_directory) {
        ker::vfs::vfs_close_file(file);
        return -EINVAL;
    }

    SwapExtent* extents = nullptr;
    size_t extent_count = 0;
    int ret = ker::vfs::xfs::xfs_collect_swap_extents(file, &extents, &extent_count);
    if (ret == 0) {
        ker::mod::sys::MutexGuard guard(g_swap_lock);
        ret = activate_extents_locked(path, extents, extent_count, file);
        if (ret == 0) {
            file = nullptr;
        }
    }
    delete[] extents;
    if (file != nullptr) {
        ker::vfs::vfs_close_file(file);
    }
    return ret;
}

auto swapoff_path(const char* path) -> int {
    if (path == nullptr || path[0] == '\0') {
        return -EINVAL;
    }
    uint32_t area_id = 0;
    std::array<SwapConsumer, MAX_SWAP_CONSUMERS> consumers{};
    size_t consumer_count = 0;
    {
        ker::mod::sys::MutexGuard guard(g_swap_lock);
        auto* area = find_area_by_name_locked(path);
        if (area == nullptr) {
            return -ENOENT;
        }
        if (area->state == AreaState::DRAINING) {
            return -EBUSY;
        }
        area->state = AreaState::DRAINING;
        area_id = area->id;
        consumer_count = g_swap_consumer_count;
        for (size_t i = 0; i < consumer_count; ++i) {
            consumers[i] = g_swap_consumers[i];
        }
    }

    int migration_ret = 0;
    for (size_t i = 0; i < consumer_count; ++i) {
        migration_ret = consumers[i].migrate_area(consumers[i].context, area_id);
        if (migration_ret < 0) {
            break;
        }
    }

    SwapArea* removed_area = nullptr;
    {
        ker::mod::sys::MutexGuard guard(g_swap_lock);
        for (size_t i = 0; i < g_swap_areas.size(); ++i) {
            auto* area = g_swap_areas.at(i);
            if (area == nullptr || area->id != area_id) {
                continue;
            }
            if (migration_ret < 0 || area->used_pages != 0) {
                area->state = AreaState::ACTIVE;
                return migration_ret < 0 ? migration_ret : -EBUSY;
            }
            g_swap_areas.remove_at(i);
            removed_area = area;
            break;
        }
    }
    if (removed_area == nullptr) {
        return -ENOENT;
    }
    // Backend close may perform I/O; the area is no longer discoverable and
    // the swap mutex must not be held while releasing the VFS mount pin.
    destroy_area(removed_area);
    return 0;
}

auto swap_available() -> bool {
    ker::mod::sys::MutexGuard guard(g_swap_lock);
    for (auto* area : g_swap_areas) {
        if (area != nullptr && area->state == AreaState::ACTIVE && area->free_pages != 0) {
            return true;
        }
    }
    return false;
}

auto allocate_slot(SwapSlot* out) -> int {
    if (out == nullptr) {
        return -EINVAL;
    }
    ker::mod::sys::MutexGuard guard(g_swap_lock);
    for (auto* area : g_swap_areas) {
        if (area == nullptr || area->state != AreaState::ACTIVE || area->free_pages == 0) {
            continue;
        }
        uint64_t const INDEX = area->free_head;
        if (INDEX == NO_FREE_SLOT || INDEX >= area->total_pages) {
            return -EIO;
        }
        SlotMetadata& metadata = area->slots[INDEX];
        if (metadata.state != SlotState::FREE || metadata.generation == UINT64_MAX) {
            return metadata.generation == UINT64_MAX ? -EOVERFLOW : -EIO;
        }
        area->free_head = metadata.next_free;
        metadata.next_free = NO_FREE_SLOT;
        metadata.generation++;
        metadata.checksum = 0;
        metadata.state = SlotState::RESERVED;
        metadata.io_in_flight = false;
        area->free_pages--;
        area->used_pages++;
        *out = SwapSlot{.area = area->id, .index = INDEX, .generation = metadata.generation};
        return 0;
    }
    return -ENOSPC;
}

auto free_slot(SwapSlot slot) -> int {
    if (!slot_valid(slot)) {
        return 0;
    }
    ker::mod::sys::MutexGuard guard(g_swap_lock);
    auto* area = find_area_locked(slot.area);
    SlotMetadata* metadata = nullptr;
    int const LOOKUP_RET = slot_metadata_locked(area, slot, &metadata);
    if (LOOKUP_RET < 0) {
        return LOOKUP_RET;
    }
    if (metadata->io_in_flight) {
        return -EBUSY;
    }
    metadata->state = SlotState::FREE;
    metadata->checksum = 0;
    metadata->next_free = area->free_head;
    area->free_head = slot.index;
    area->free_pages++;
    area->used_pages--;
    return 0;
}

auto write_slot(SwapSlot slot, const void* page) -> int {
    if (!slot_valid(slot) || page == nullptr) {
        return -EINVAL;
    }
    SwapExtent extent{};
    {
        ker::mod::sys::MutexGuard guard(g_swap_lock);
        auto* area = find_area_locked(slot.area);
        SlotMetadata* metadata = nullptr;
        int const LOOKUP_RET = slot_metadata_locked(area, slot, &metadata);
        if (LOOKUP_RET < 0) {
            return LOOKUP_RET;
        }
        if (metadata->io_in_flight) {
            return -EBUSY;
        }
        if (!slot_to_extent_locked(area, slot.index, &extent)) {
            return -EIO;
        }
        metadata->state = SlotState::RESERVED;
        metadata->checksum = 0;
        metadata->io_in_flight = true;
    }
    uint32_t const CHECKSUM = ker::util::crc32c_compute(page, PAGE_SIZE);
    size_t const BLOCKS = PAGE_SIZE / extent.device->block_size;
    int const RET = ker::dev::block_write(extent.device, extent.start_block, BLOCKS, page);
    {
        ker::mod::sys::MutexGuard guard(g_swap_lock);
        auto* area = find_area_locked(slot.area);
        SlotMetadata* metadata = nullptr;
        int const LOOKUP_RET = slot_metadata_locked(area, slot, &metadata);
        if (LOOKUP_RET < 0 || !metadata->io_in_flight) {
            return LOOKUP_RET < 0 ? LOOKUP_RET : -EIO;
        }
        metadata->io_in_flight = false;
        if (RET == 0) {
            metadata->checksum = CHECKSUM;
            metadata->state = SlotState::STORED;
        }
    }
    return RET;
}

auto read_slot(SwapSlot slot, void* page) -> int {
    if (!slot_valid(slot) || page == nullptr) {
        return -EINVAL;
    }
    SwapExtent extent{};
    uint32_t expected_checksum = 0;
    {
        ker::mod::sys::MutexGuard guard(g_swap_lock);
        auto* area = find_area_locked(slot.area);
        SlotMetadata* metadata = nullptr;
        int const LOOKUP_RET = slot_metadata_locked(area, slot, &metadata);
        if (LOOKUP_RET < 0) {
            return LOOKUP_RET;
        }
        if (metadata->state != SlotState::STORED) {
            return -ENODATA;
        }
        if (metadata->io_in_flight) {
            return -EBUSY;
        }
        if (!slot_to_extent_locked(area, slot.index, &extent)) {
            return -EIO;
        }
        expected_checksum = metadata->checksum;
        metadata->io_in_flight = true;
    }
    size_t const BLOCKS = PAGE_SIZE / extent.device->block_size;
    int ret = ker::dev::block_read(extent.device, extent.start_block, BLOCKS, page);
    if (ret == 0 && ker::util::crc32c_compute(page, PAGE_SIZE) != expected_checksum) {
        ret = -EILSEQ;
    }
    if (ret < 0) {
        std::memset(page, 0, PAGE_SIZE);
    }
    {
        ker::mod::sys::MutexGuard guard(g_swap_lock);
        auto* area = find_area_locked(slot.area);
        SlotMetadata* metadata = nullptr;
        int const LOOKUP_RET = slot_metadata_locked(area, slot, &metadata);
        if (LOOKUP_RET < 0 || !metadata->io_in_flight) {
            return LOOKUP_RET < 0 ? LOOKUP_RET : -EIO;
        }
        metadata->io_in_flight = false;
    }
    return ret;
}

void get_stats(SwapStats* out) {
    if (out == nullptr) {
        return;
    }
    SwapStats stats{};
    ker::mod::sys::MutexGuard guard(g_swap_lock);
    for (auto* area : g_swap_areas) {
        if (area == nullptr || area->state != AreaState::ACTIVE) {
            continue;
        }
        stats.active_areas++;
        stats.total_bytes += area->total_pages * PAGE_SIZE;
        stats.free_bytes += area->free_pages * PAGE_SIZE;
        stats.used_bytes += area->used_pages * PAGE_SIZE;
    }
    *out = stats;
}

auto register_consumer(const SwapConsumer& consumer) -> bool {
    if (consumer.name == nullptr || consumer.name[0] == '\0' || consumer.migrate_area == nullptr) {
        return false;
    }
    ker::mod::sys::MutexGuard guard(g_swap_lock);
    for (size_t i = 0; i < g_swap_consumer_count; ++i) {
        SwapConsumer const& current = g_swap_consumers[i];
        if (current.context == consumer.context && current.migrate_area == consumer.migrate_area) {
            return true;
        }
    }
    if (g_swap_consumer_count == MAX_SWAP_CONSUMERS) {
        return false;
    }
    g_swap_consumers[g_swap_consumer_count++] = consumer;
    return true;
}

}  // namespace ker::mod::mm::swap
