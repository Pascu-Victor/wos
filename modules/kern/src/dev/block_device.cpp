#include "block_device.hpp"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dev/gpt.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sys/spinlock.hpp>
#include <span>

#include "device.hpp"
#include "util/string.hpp"

namespace ker::dev {

using log = ker::mod::dbg::logger<"bdev">;

// Block device registry
namespace {
constexpr size_t MAX_BLOCK_IO_BYTES = size_t{16} * 1024 * 1024;
enum class RegistryPhase : uint8_t { PREPARING, PUBLISHED, RETIRING };
struct BlockRegistryEntry {
    BlockDevice* device = nullptr;
    Device node{};
    BlockRegistryEntry* next = nullptr;
    RegistryPhase phase = RegistryPhase::PREPARING;
};

ker::mod::sys::Spinlock block_registry_lock;
BlockRegistryEntry* registry_head = nullptr;
BlockRegistryEntry* registry_tail = nullptr;
size_t registry_count = 0;

class BlockRegistryGuard {
   public:
    BlockRegistryGuard() : flags(block_registry_lock.lock_irqsave()) {}
    ~BlockRegistryGuard() { block_registry_lock.unlock_irqrestore(flags); }
    BlockRegistryGuard(const BlockRegistryGuard&) = delete;
    auto operator=(const BlockRegistryGuard&) -> BlockRegistryGuard& = delete;

   private:
    uint64_t flags;
};
ker::mod::sys::Spinlock block_writer_lease_lock;
BlockWriterLease* block_writer_leases = nullptr;

auto max_io_blocks(BlockDevice const* bdev) -> size_t {
    if (bdev == nullptr || bdev->block_size == 0) {
        return 1;
    }
    return std::max<size_t>(1, MAX_BLOCK_IO_BYTES / bdev->block_size);
}

auto whole_disk_for(BlockDevice const* bdev) -> BlockDevice const* {
    while (bdev != nullptr && bdev->is_partition && bdev->parent_disk != nullptr) {
        bdev = bdev->parent_disk;
    }
    return bdev;
}

auto block_range_start(BlockDevice const* bdev) -> uint64_t {
    return (bdev != nullptr && bdev->is_partition) ? bdev->partition_start_lba : 0;
}

auto block_range_end(BlockDevice const* bdev) -> uint64_t {
    if (bdev == nullptr) {
        return 0;
    }
    if (bdev->is_partition) {
        return bdev->partition_end_lba;
    }
    return bdev->total_blocks == 0 ? 0 : bdev->total_blocks - 1;
}

auto normalize_io_result(int rc) -> int {
    if (rc == 0) {
        return 0;
    }
    if (rc == -1) {
        return -EIO;
    }
    return (rc < 0) ? rc : -rc;
}

// Caller holds the registry guard. Published count is changed separately at
// the visibility transition, not when a preparing/retiring entry is unlinked.
void unlink_entry_locked(BlockRegistryEntry* target) {
    BlockRegistryEntry* previous = nullptr;
    for (auto* entry = registry_head; entry != nullptr; entry = entry->next) {
        if (entry == target) {
            if (previous != nullptr) {
                previous->next = entry->next;
            } else {
                registry_head = entry->next;
            }
            if (registry_tail == entry) {
                registry_tail = previous;
            }
            return;
        }
        previous = entry;
    }
}
}  // namespace

auto block_device_register(BlockDevice* bdev) -> int {
    if (bdev == nullptr) {
        log::warn("block_device_register: invalid device");
        return -1;
    }
    auto* entry = new (std::nothrow) BlockRegistryEntry;
    if (entry == nullptr) {
        return -1;
    }
    entry->device = bdev;
    entry->node.major = bdev->major;
    entry->node.minor = bdev->minor;
    entry->node.name = bdev->name.data();
    entry->node.type = DeviceType::BLOCK;
    entry->node.private_data = bdev;

    bool reserved = true;
    {
        BlockRegistryGuard const GUARD;
        for (auto* existing = registry_head; existing != nullptr; existing = existing->next) {
            if (existing->device == bdev || std::strcmp(existing->device->name.data(), bdev->name.data()) == 0) {
                reserved = false;
                break;
            }
        }
        if (reserved) {
            if (registry_tail != nullptr) {
                registry_tail->next = entry;
            } else {
                registry_head = entry;
            }
            registry_tail = entry;
        }
    }
    if (!reserved) {
        delete entry;
        return -1;
    }

    // This can allocate, log and reenter either registry. The hidden entry
    // reserves its key, but no block reader can select it before success.
    if (dev_register(&entry->node) != 0) {
        {
            BlockRegistryGuard const GUARD;
            unlink_entry_locked(entry);
        }
        delete entry;
        log::warn("block_device_register: /dev registration failed");
        return -1;
    }
    size_t count = 0;
    {
        BlockRegistryGuard const GUARD;
        entry->phase = RegistryPhase::PUBLISHED;
        count = ++registry_count;
    }
    mod::perf::record_container_stat(0, 0, mod::perf::PerfSubsystem::BLOCK_DEV, 0, mod::perf::PERF_FLAG_CT_INSERT,
                                     static_cast<int64_t>(count), 0, 0);
    log::debug("block_device_register: registered %s", bdev->name.data());
    return 0;
}

auto block_device_unregister(BlockDevice* bdev) -> int {
    if (bdev == nullptr) {
        return -1;
    }
    BlockRegistryEntry* retired = nullptr;
    size_t count = 0;
    {
        BlockRegistryGuard const GUARD;
        for (auto* entry = registry_head; entry != nullptr; entry = entry->next) {
            if (entry->device == bdev && entry->phase == RegistryPhase::PUBLISHED) {
                entry->phase = RegistryPhase::RETIRING;
                retired = entry;
                count = --registry_count;
                break;
            }
        }
    }
    if (retired == nullptr) {
        return -1;
    }
    // Keep the name reserved until wrapper removal finishes. Neither registry
    // guard spans the other registry's callbacks or entry reclamation.
    static_cast<void>(dev_unregister(&retired->node));
    {
        BlockRegistryGuard const GUARD;
        unlink_entry_locked(retired);
    }
    delete retired;
    mod::perf::record_container_stat(0, 0, mod::perf::PerfSubsystem::BLOCK_DEV, 0, mod::perf::PERF_FLAG_CT_REMOVE,
                                     static_cast<int64_t>(count), 0, 0);
    log::debug("block_device_unregister: removed %s", bdev->name.data());
    return 0;
}

auto block_device_find(unsigned major, unsigned minor) -> BlockDevice* {
    BlockRegistryGuard const GUARD;
    for (auto* entry = registry_head; entry != nullptr; entry = entry->next) {
        auto* device = entry->device;
        if (entry->phase == RegistryPhase::PUBLISHED && device->major == major && device->minor == minor) {
            return device;
        }
    }
    return nullptr;
}

auto block_device_find_by_name(const char* name) -> BlockDevice* {
    if (name == nullptr) {
        return nullptr;
    }
    BlockRegistryGuard const GUARD;
    for (auto* entry = registry_head; entry != nullptr; entry = entry->next) {
        if (entry->phase == RegistryPhase::PUBLISHED && std::strcmp(entry->device->name.data(), name) == 0) {
            return entry->device;
        }
    }
    return nullptr;
}

auto block_device_find_by_wrapper(const Device* wrapper) -> BlockDevice* {
    BlockRegistryGuard const GUARD;
    for (auto* entry = registry_head; entry != nullptr; entry = entry->next) {
        if (entry->phase == RegistryPhase::PUBLISHED && &entry->node == wrapper) {
            return entry->device;
        }
    }
    return nullptr;
}

auto block_device_is_registered(const BlockDevice* device) -> bool {
    BlockRegistryGuard const GUARD;
    for (auto* entry = registry_head; entry != nullptr; entry = entry->next) {
        if (entry->phase == RegistryPhase::PUBLISHED && entry->device == device) {
            return true;
        }
    }
    return false;
}

auto block_read(BlockDevice* bdev, uint64_t block, size_t count, void* buffer) -> int {
    if (bdev == nullptr || buffer == nullptr) {
        return -EINVAL;
    }

    if (block > bdev->total_blocks || count > bdev->total_blocks - block) {
        log::warn("block_read: read past end of device: block=%lu count=%lu total=%lu caller=%p", static_cast<unsigned long>(block),
                  static_cast<unsigned long>(count), static_cast<unsigned long>(bdev->total_blocks), __builtin_return_address(0));
        return -EINVAL;
    }

    if (bdev->read_blocks == nullptr) {
        log::error("block_read: read_blocks not implemented");
        return -ENOSYS;
    }

    if (count == 0) {
        return 0;
    }

    if (bdev->block_size == 0) {
        log::error("block_read: invalid zero block size for %s", bdev->name.data());
        return -EINVAL;
    }

    auto* out = static_cast<uint8_t*>(buffer);
    uint64_t current_block = block;
    size_t remaining = count;
    size_t const MAX_BLOCKS = max_io_blocks(bdev);

    while (remaining > 0) {
        size_t const CHUNK_BLOCKS = std::min(remaining, MAX_BLOCKS);
        int const RC = bdev->read_blocks(bdev, current_block, CHUNK_BLOCKS, out);
        if (RC != 0) {
            return normalize_io_result(RC);
        }

        remaining -= CHUNK_BLOCKS;
        current_block += CHUNK_BLOCKS;
        out += CHUNK_BLOCKS * bdev->block_size;
    }

    return 0;
}

auto block_write(BlockDevice* bdev, uint64_t block, size_t count, const void* buffer) -> int {
    if (bdev == nullptr || buffer == nullptr) {
        return -EINVAL;
    }

    if (block_device_is_read_only(bdev)) {
        return -EROFS;
    }

    if (block > bdev->total_blocks || count > bdev->total_blocks - block) {
        log::warn("block_write: write past end of device: block=%lu count=%lu total=%lu caller=%p", static_cast<unsigned long>(block),
                  static_cast<unsigned long>(count), static_cast<unsigned long>(bdev->total_blocks), __builtin_return_address(0));
        return -EINVAL;
    }

    if (bdev->write_blocks == nullptr) {
        log::error("block_write: write_blocks not implemented");
        return -ENOSYS;
    }

    if (count == 0) {
        return 0;
    }

    if (bdev->block_size == 0) {
        log::error("block_write: invalid zero block size for %s", bdev->name.data());
        return -EINVAL;
    }

    auto const* in = static_cast<uint8_t const*>(buffer);
    uint64_t current_block = block;
    size_t remaining = count;
    size_t const MAX_BLOCKS = max_io_blocks(bdev);

    while (remaining > 0) {
        size_t const CHUNK_BLOCKS = std::min(remaining, MAX_BLOCKS);
        int const RC = bdev->write_blocks(bdev, current_block, CHUNK_BLOCKS, in);
        if (RC != 0) {
            return normalize_io_result(RC);
        }

        remaining -= CHUNK_BLOCKS;
        current_block += CHUNK_BLOCKS;
        in += CHUNK_BLOCKS * bdev->block_size;
    }

    return 0;
}

auto block_flush(BlockDevice* bdev) -> int {
    if (bdev == nullptr) {
        return -EINVAL;
    }

    if (bdev->flush == nullptr) {
        // A caller asking for a durability barrier must never receive a
        // false-success result.  Devices whose writes are inherently durable
        // (for example the RAM disk) advertise an explicit no-op callback.
        return -EOPNOTSUPP;
    }

    return normalize_io_result(bdev->flush(bdev));
}

auto block_device_count() -> size_t {
    BlockRegistryGuard const GUARD;
    return registry_count;
}

auto block_device_at(size_t index) -> BlockDevice* {
    BlockRegistryGuard const GUARD;
    for (auto* entry = registry_head; entry != nullptr; entry = entry->next) {
        if (entry->phase != RegistryPhase::PUBLISHED) {
            continue;
        }
        if (index == 0) {
            return entry->device;
        }
        --index;
    }
    return nullptr;
}

auto block_device_find_by_partuuid(const char* uuid_str) -> BlockDevice* {
    if (uuid_str == nullptr) {
        return nullptr;
    }
    BlockRegistryGuard const GUARD;
    for (auto* entry = registry_head; entry != nullptr; entry = entry->next) {
        auto* device = entry->device;
        if (entry->phase == RegistryPhase::PUBLISHED && device->is_partition && std::strcmp(device->partuuid_str.data(), uuid_str) == 0) {
            return device;
        }
    }
    return nullptr;
}

namespace {
// Read function for partition block devices: offsets LBA by partition start
auto partition_read(BlockDevice* dev, uint64_t block, size_t count, void* buffer) -> int {
    if (dev == nullptr || dev->parent_disk == nullptr) {
        return -EINVAL;
    }
    return block_read(dev->parent_disk, dev->partition_start_lba + block, count, buffer);
}

// Write function for partition block devices: offsets LBA by partition start
auto partition_write(BlockDevice* dev, uint64_t block, size_t count, const void* buffer) -> int {
    if (dev == nullptr || dev->parent_disk == nullptr) {
        return -EINVAL;
    }
    return block_write(dev->parent_disk, dev->partition_start_lba + block, count, buffer);
}

auto partition_flush(BlockDevice* dev) -> int {
    if (dev == nullptr || dev->parent_disk == nullptr) {
        return -EINVAL;
    }
    return block_flush(dev->parent_disk);
}

}  // namespace

auto block_device_create_partition(BlockDevice* parent_disk, uint64_t start_lba, uint64_t end_lba, const uint8_t* partuuid,
                                   uint32_t partition_index) -> BlockDevice* {
    if (parent_disk == nullptr || partuuid == nullptr) {
        return nullptr;
    }

    auto* part = new (std::nothrow) BlockDevice;
    if (part == nullptr) {
        return nullptr;
    }
    part->major = parent_disk->major;
    part->minor = static_cast<unsigned>(partition_index + 1);

    // Build name: e.g. "sda" + "1" -> "sda1"
    // Find end of parent name
    auto* const PARENT_NAME_END = std::ranges::find(parent_disk->name, '\0');
    auto const PARENT_NAME_LEN = std::min(static_cast<size_t>(PARENT_NAME_END - parent_disk->name.begin()), BLOCK_NAME_SIZE - 1);
    // Copy parent name
    std::copy_n(parent_disk->name.begin(), PARENT_NAME_LEN, part->name.begin());
    // Append 1-based partition number (simple: single digit for index < 9, two digits otherwise)
    uint32_t const PART_NUM = partition_index + 1;
    auto const NUM_OFFSET = static_cast<size_t>(
        ker::util::string::u64toa(PART_NUM, std::span<char>(part->name.data() + PARENT_NAME_LEN, BLOCK_NAME_SIZE - PARENT_NAME_LEN)));
    part->name.at(PARENT_NAME_LEN + NUM_OFFSET) = '\0';

    part->block_size = parent_disk->block_size;
    part->total_blocks = end_lba - start_lba + 1;
    part->read_blocks = partition_read;
    part->write_blocks = partition_write;
    part->flush = partition_flush;
    part->private_data = nullptr;
    part->remotable = parent_disk->remotable;
    part->read_only = block_device_is_read_only(parent_disk);

    part->is_partition = true;
    std::copy_n(partuuid, part->partuuid.size(), part->partuuid.begin());
    gpt::guid_to_string(partuuid, part->partuuid_str.data());
    part->parent_disk = parent_disk;
    part->partition_start_lba = start_lba;
    part->partition_end_lba = end_lba;

    if (block_device_register(part) != 0) {
        delete part;
        return nullptr;
    }

    log::debug("Created partition %s PARTUUID=%s", part->name.data(), part->partuuid_str.data());

    return part;
}

// Initializes block devices and enumerates GPT partitions on all registered disks
auto block_device_init() -> void {
    log::debug("Initializing block devices");

    // Enumerate GPT partitions on all registered whole-disk block devices
    // We snapshot the current count since enumeration will register new partition devices
    size_t const DISK_COUNT = block_device_count();
    for (size_t i = 0; i < DISK_COUNT; ++i) {
        BlockDevice* disk = block_device_at(i);
        if (disk == nullptr || disk->is_partition) {
            continue;
        }

        gpt::GPTDiskInfo disk_info{};
        if (gpt::gpt_enumerate_partitions(disk, &disk_info) != 0) {
            log::debug("No GPT found on %s", disk->name.data());
            continue;
        }

        log::debug("GPT: %s has %d partitions", disk->name.data(), disk_info.partition_count);

        for (uint32_t p = 0; p < disk_info.partition_count; ++p) {
            auto& part = disk_info.partitions.at(p);
            block_device_create_partition(disk, part.starting_lba, part.ending_lba, part.unique_partition_guid.data(), p);
        }
    }

    log::debug("Block device init complete: %d devices registered", static_cast<unsigned>(block_device_count()));
}

auto block_devices_overlap(const BlockDevice* lhs, const BlockDevice* rhs) -> bool {
    if (lhs == nullptr || rhs == nullptr) {
        return false;
    }

    if (whole_disk_for(lhs) != whole_disk_for(rhs)) {
        return false;
    }

    uint64_t const LHS_START = block_range_start(lhs);
    uint64_t const LHS_END = block_range_end(lhs);
    uint64_t const RHS_START = block_range_start(rhs);
    uint64_t const RHS_END = block_range_end(rhs);
    return LHS_START <= RHS_END && RHS_START <= LHS_END;
}

void BlockWriterLease::unlink_locked() {
    if (device_ == nullptr) {
        return;
    }
    if (prev_ != nullptr) {
        prev_->next_ = next_;
    } else {
        block_writer_leases = next_;
    }
    if (next_ != nullptr) {
        next_->prev_ = prev_;
    }
    device_ = nullptr;
    prev_ = nullptr;
    next_ = nullptr;
}

void BlockWriterLease::take_locked(BlockWriterLease& other) {
    if (!other.active()) {
        return;
    }
    device_ = other.device_;
    owner_ = other.owner_;
    prev_ = other.prev_;
    next_ = other.next_;
    if (prev_ != nullptr) {
        prev_->next_ = this;
    } else {
        block_writer_leases = this;
    }
    if (next_ != nullptr) {
        next_->prev_ = this;
    }
    other.device_ = nullptr;
    other.prev_ = nullptr;
    other.next_ = nullptr;
}

BlockWriterLease::BlockWriterLease(BlockWriterLease&& other) noexcept {
    uint64_t const FLAGS = block_writer_lease_lock.lock_irqsave();
    take_locked(other);
    block_writer_lease_lock.unlock_irqrestore(FLAGS);
}

auto BlockWriterLease::operator=(BlockWriterLease&& other) noexcept -> BlockWriterLease& {
    if (this == &other) {
        return *this;
    }
    uint64_t const FLAGS = block_writer_lease_lock.lock_irqsave();
    unlink_locked();
    take_locked(other);
    block_writer_lease_lock.unlock_irqrestore(FLAGS);
    return *this;
}

BlockWriterLease::~BlockWriterLease() { release(); }

auto BlockWriterLease::try_acquire(const BlockDevice* device, BlockWriterLeaseOwner owner) -> bool {
    if (device == nullptr) {
        return false;
    }

    uint64_t const FLAGS = block_writer_lease_lock.lock_irqsave();
    if (active()) {
        block_writer_lease_lock.unlock_irqrestore(FLAGS);
        return false;
    }
    for (BlockWriterLease* lease = block_writer_leases; lease != nullptr; lease = lease->next_) {
        bool const EXCLUSIVE = owner == BlockWriterLeaseOwner::REMOTE_BINDING || lease->owner_ == BlockWriterLeaseOwner::REMOTE_BINDING;
        if (EXCLUSIVE && block_devices_overlap(lease->device_, device)) {
            block_writer_lease_lock.unlock_irqrestore(FLAGS);
            return false;
        }
    }

    device_ = device;
    owner_ = owner;
    prev_ = nullptr;
    next_ = block_writer_leases;
    if (next_ != nullptr) {
        next_->prev_ = this;
    }
    block_writer_leases = this;
    block_writer_lease_lock.unlock_irqrestore(FLAGS);
    return true;
}

void BlockWriterLease::release() {
    uint64_t const FLAGS = block_writer_lease_lock.lock_irqsave();
    unlink_locked();
    block_writer_lease_lock.unlock_irqrestore(FLAGS);
}

auto block_device_set_read_only(BlockDevice* bdev, bool read_only) -> void {
    if (bdev == nullptr) {
        return;
    }

    BlockRegistryGuard const GUARD;
    for (auto* entry = registry_head; entry != nullptr; entry = entry->next) {
        auto* dev = entry->device;
        if (block_devices_overlap(dev, bdev)) {
            dev->read_only = read_only;
        }
    }
    bdev->read_only = read_only;
}

auto block_device_is_read_only(const BlockDevice* bdev) -> bool {
    BlockRegistryGuard const GUARD;
    return bdev != nullptr && (bdev->read_only || (bdev->is_partition && bdev->parent_disk != nullptr && bdev->parent_disk->read_only));
}

}  // namespace ker::dev
