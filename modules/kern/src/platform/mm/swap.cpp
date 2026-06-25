#include "swap.hpp"

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dev/block_device.hpp>
#include <platform/mm/paging.hpp>
#include <platform/sys/mutex.hpp>
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
constexpr size_t PAGE_SIZE = ker::mod::mm::paging::PAGE_SIZE;

struct SwapArea {
    uint32_t id{};
    char* name{};
    SwapExtent* extents{};
    size_t extent_count{};
    uint8_t* used{};
    uint64_t total_pages{};
    uint64_t free_pages{};
    uint64_t used_pages{};
    bool active{};
};

ker::mod::sys::Mutex g_swap_lock;                // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::util::SmallVec<SwapArea*, 4> g_swap_areas;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_next_area_id = 1;                     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

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
    delete[] area->used;
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

auto find_area_locked(uint32_t area_id) -> SwapArea* {
    for (auto* area : g_swap_areas) {
        if (area != nullptr && area->active && area->id == area_id) {
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
        if (area != nullptr && area->active && area->name != nullptr && std::strcmp(area->name, name) == 0) {
            return area;
        }
    }
    return nullptr;
}

auto activate_extents_locked(const char* name, const SwapExtent* extents, size_t extent_count) -> int {
    if (name == nullptr || extents == nullptr || extent_count == 0 || g_swap_areas.size() >= MAX_SWAP_AREAS ||
        find_area_by_name_locked(name) != nullptr) {
        return -EINVAL;
    }

    uint64_t total_pages = 0;
    for (size_t i = 0; i < extent_count; ++i) {
        if (!block_device_usable_for_swap(extents[i].device) || extents[i].page_count == 0 ||
            total_pages > UINT64_MAX - extents[i].page_count) {
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
    area->used = new uint8_t[static_cast<size_t>(total_pages)];
    if (area->name == nullptr || area->extents == nullptr || area->used == nullptr) {
        destroy_area(area);
        return -ENOMEM;
    }
    for (size_t i = 0; i < extent_count; ++i) {
        area->extents[i] = extents[i];
    }
    std::memset(area->used, 0, static_cast<size_t>(total_pages));
    area->extent_count = extent_count;
    area->total_pages = total_pages;
    area->free_pages = total_pages;
    area->used_pages = 0;
    area->active = true;
    area->id = g_next_area_id++;
    if (area->id == UINT32_MAX) {
        area->id = g_next_area_id++;
    }
    if (!g_swap_areas.push_back(area)) {
        destroy_area(area);
        return -ENOMEM;
    }
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
    uint64_t const BYTES = device->total_blocks * device->block_size;
    uint64_t const PAGES = BYTES / PAGE_SIZE;
    if (PAGES == 0) {
        return -EINVAL;
    }
    SwapExtent extent{.device = device, .start_block = 0, .page_count = PAGES};
    return activate_extents(name, &extent, 1);
}

auto activate_extents(const char* name, const SwapExtent* extents, size_t extent_count) -> int {
    ker::mod::sys::MutexGuard guard(g_swap_lock);
    return activate_extents_locked(name, extents, extent_count);
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
        ret = activate_extents(path, extents, extent_count);
    }
    delete[] extents;
    ker::vfs::vfs_close_file(file);
    return ret;
}

auto swapoff_path(const char* path) -> int {
    if (path == nullptr || path[0] == '\0') {
        return -EINVAL;
    }
    ker::mod::sys::MutexGuard guard(g_swap_lock);
    for (size_t i = 0; i < g_swap_areas.size(); ++i) {
        auto* area = g_swap_areas.at(i);
        if (area == nullptr || area->name == nullptr || std::strcmp(area->name, path) != 0) {
            continue;
        }
        if (area->used_pages != 0) {
            return -EBUSY;
        }
        area->active = false;
        g_swap_areas.remove_at(i);
        destroy_area(area);
        return 0;
    }
    return -ENOENT;
}

auto swap_available() -> bool {
    ker::mod::sys::MutexGuard guard(g_swap_lock);
    for (auto* area : g_swap_areas) {
        if (area != nullptr && area->active && area->free_pages != 0) {
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
        if (area == nullptr || !area->active || area->free_pages == 0) {
            continue;
        }
        for (uint64_t i = 0; i < area->total_pages; ++i) {
            if (area->used[i] != 0) {
                continue;
            }
            area->used[i] = 1;
            area->free_pages--;
            area->used_pages++;
            *out = SwapSlot{.area = area->id, .index = i};
            return 0;
        }
    }
    return -ENOSPC;
}

auto free_slot(SwapSlot slot) -> int {
    if (!slot_valid(slot)) {
        return 0;
    }
    ker::mod::sys::MutexGuard guard(g_swap_lock);
    auto* area = find_area_locked(slot.area);
    if (area == nullptr || slot.index >= area->total_pages || area->used[slot.index] == 0) {
        return -EINVAL;
    }
    area->used[slot.index] = 0;
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
        if (area == nullptr || slot.index >= area->total_pages || area->used[slot.index] == 0 ||
            !slot_to_extent_locked(area, slot.index, &extent)) {
            return -EINVAL;
        }
    }
    size_t const BLOCKS = PAGE_SIZE / extent.device->block_size;
    int const RET = ker::dev::block_write(extent.device, extent.start_block, BLOCKS, page);
    return RET == 0 ? 0 : -EIO;
}

auto read_slot(SwapSlot slot, void* page) -> int {
    if (!slot_valid(slot) || page == nullptr) {
        return -EINVAL;
    }
    SwapExtent extent{};
    {
        ker::mod::sys::MutexGuard guard(g_swap_lock);
        auto* area = find_area_locked(slot.area);
        if (area == nullptr || slot.index >= area->total_pages || area->used[slot.index] == 0 ||
            !slot_to_extent_locked(area, slot.index, &extent)) {
            return -EINVAL;
        }
    }
    size_t const BLOCKS = PAGE_SIZE / extent.device->block_size;
    int const RET = ker::dev::block_read(extent.device, extent.start_block, BLOCKS, page);
    return RET == 0 ? 0 : -EIO;
}

void get_stats(SwapStats* out) {
    if (out == nullptr) {
        return;
    }
    SwapStats stats{};
    ker::mod::sys::MutexGuard guard(g_swap_lock);
    for (auto* area : g_swap_areas) {
        if (area == nullptr || !area->active) {
            continue;
        }
        stats.active_areas++;
        stats.total_bytes += area->total_pages * PAGE_SIZE;
        stats.free_bytes += area->free_pages * PAGE_SIZE;
        stats.used_bytes += area->used_pages * PAGE_SIZE;
    }
    *out = stats;
}

}  // namespace ker::mod::mm::swap
