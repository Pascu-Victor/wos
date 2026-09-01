// XFS on-disk format and mount-path ktest.
// Two classes of tests:
//   (a) Pure struct-level checks — sizes, magic constants, endian wrappers.
//       No block device or kmalloc required.
//   (b) xfs_mount() error-path — a null-zeroed block device returns zeros for
//       sector 0, so sb_magicnum == 0 != XFS_SB_MAGIC and xfs_mount must
//       return a non-zero error code without leaking the ctx.

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dev/block_device.hpp>
#include <memory>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <test/fault_block_device.hpp>
#include <test/fault_block_device_impl.hpp>
#include <test/ktest.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_alloc.hpp>
#include <vfs/fs/xfs/xfs_attr.hpp>
#include <vfs/fs/xfs/xfs_bmap.hpp>
#include <vfs/fs/xfs/xfs_btree.hpp>
#include <vfs/fs/xfs/xfs_dir2.hpp>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_inode.hpp>
#include <vfs/fs/xfs/xfs_log.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>
#include <vfs/fs/xfs/xfs_vfs.hpp>
#include <vfs/vfs.hpp>

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

auto xfs_null_read(ker::dev::BlockDevice* dev, uint64_t /*blk*/, size_t count, void* buf) -> int {
    std::memset(buf, 0, count * dev->block_size);
    return 0;
}

auto xfs_null_write(ker::dev::BlockDevice* /*dev*/, uint64_t /*blk*/, size_t /*count*/, const void* /*buf*/) -> int { return 0; }

struct XfsReadCounter {
    size_t calls = 0;
};

auto xfs_counting_read(ker::dev::BlockDevice* dev, uint64_t /*blk*/, size_t count, void* buf) -> int {
    auto* counter = static_cast<XfsReadCounter*>(dev->private_data);
    if (counter != nullptr) {
        counter->calls++;
    }
    std::memset(buf, 0, count * dev->block_size);
    return 0;
}

auto make_xfs_null_bdev() -> ker::dev::BlockDevice {
    ker::dev::BlockDevice d{};
    d.block_size = 512;
    d.total_blocks = 1024;
    d.read_blocks = xfs_null_read;
    d.write_blocks = xfs_null_write;
    return d;
}

auto make_xfs_counting_bdev(XfsReadCounter* counter) -> ker::dev::BlockDevice {
    ker::dev::BlockDevice d = make_xfs_null_bdev();
    d.read_blocks = xfs_counting_read;
    d.private_data = counter;
    return d;
}

constexpr size_t FAULT_BLOCK_SIZE = 512;
constexpr uint64_t FAULT_TOTAL_BLOCKS = 512;
constexpr uint64_t FAULT_HOME_BLOCK = 16;
constexpr uint64_t FAULT_DATA_BLOCK = 64;
constexpr ker::vfs::xfs::xfs_fsblock_t FAULT_LOG_START = 128;
constexpr uint32_t FAULT_LOG_BLOCKS = 64;
// Concrete metadata post-images used by create/counter, inode, extent/write,
// directory/link, rename-source, rename-replacement, and truncate scenarios.
// The compact journal stores all of them as buffer items, so replay must
// preserve the entire set across any item write failure.
constexpr std::array<uint64_t, 7> SEMANTIC_HOME_BLOCKS{{16, 17, 18, 19, 20, 21, 22}};
constexpr std::array<uint8_t, 7> SEMANTIC_HOME_VALUES{{0xC1, 0xD2, 0xE3, 0xA4, 0xB5, 0x96, 0x87}};

void configure_synthetic_mount(ker::vfs::xfs::XfsMountContext& mount, ker::dev::BlockDevice* device, uint8_t uuid_byte,
                               uint32_t log_blocks = FAULT_LOG_BLOCKS) {
    mount.device = device;
    mount.block_size = FAULT_BLOCK_SIZE;
    mount.block_log = 9;
    mount.total_blocks = FAULT_TOTAL_BLOCKS;
    mount.ag_count = 1;
    mount.ag_blocks = FAULT_TOTAL_BLOCKS;
    mount.ag_blk_log = 9;
    mount.sect_size = FAULT_BLOCK_SIZE;
    mount.sect_log = 9;
    mount.log_start = FAULT_LOG_START;
    mount.log_blocks = log_blocks;
    mount.uuid.b.fill(uuid_byte);
    mount.meta_uuid.b.fill(uuid_byte);
    mount.read_only = false;
}

class SyntheticXfsLogFixture {
   public:
    explicit SyntheticXfsLogFixture(uint8_t uuid_byte, uint32_t log_blocks = FAULT_LOG_BLOCKS)
        : storage_(new (std::nothrow) ker::test::FaultBlockDevice(FAULT_BLOCK_SIZE, FAULT_TOTAL_BLOCKS)) {
        if (storage_ == nullptr || !storage_->valid()) {
            return;
        }

        configure_synthetic_mount(mount_, storage_->device(), uuid_byte, log_blocks);
        ker::vfs::invalidate_bdev(storage_->device());
    }

    ~SyntheticXfsLogFixture() {
        if (storage_ == nullptr) {
            return;
        }
        storage_->clear_fault();
        if (mounted_) {
            ker::vfs::xfs::xfs_log_unmount(&mount_, false);
        }
        ker::vfs::invalidate_bdev(storage_->device());
    }

    SyntheticXfsLogFixture(const SyntheticXfsLogFixture&) = delete;
    SyntheticXfsLogFixture(SyntheticXfsLogFixture&&) = delete;
    auto operator=(const SyntheticXfsLogFixture&) -> SyntheticXfsLogFixture& = delete;
    auto operator=(SyntheticXfsLogFixture&&) -> SyntheticXfsLogFixture& = delete;

    auto valid() const -> bool { return storage_ != nullptr && storage_->valid(); }

    auto mount_log() -> int {
        if (!valid()) {
            return -ENOMEM;
        }
        int const RC = ker::vfs::xfs::xfs_log_mount(&mount_);
        mounted_ = RC == 0;
        return RC;
    }

    template <size_t N>
    auto stage_homes(const std::array<uint64_t, N>& home_blocks, const std::array<uint8_t, N>& values) -> int {
        static_assert(N != 0 && N <= static_cast<size_t>(ker::vfs::xfs::XFS_TRANS_MAX_ITEMS));
        if (!mounted_) {
            return -ENODEV;
        }
        std::array<ker::vfs::BufHead*, N> homes{};
        std::array<ker::vfs::xfs::XfsTransItem, N> items{};
        int rc = 0;
        for (size_t i = 0; i < N; ++i) {
            homes.at(i) = ker::vfs::xfs::xfs_buf_get(&mount_, home_blocks.at(i));
            ker::vfs::BufHead* const HOME = homes.at(i);
            if (HOME == nullptr || HOME->data == nullptr || HOME->size > UINT32_MAX) {
                rc = -EIO;
                break;
            }
            std::memset(HOME->data, values.at(i), HOME->size);
            items.at(i).type = ker::vfs::xfs::XfsLogItemType::BUFFER;
            items.at(i).buf = {
                .bp = HOME,
                .offset = 0,
                .len = static_cast<uint32_t>(HOME->size),
                .dirty = true,
            };
        }

        bool owns_metadata = false;
        if (rc == 0) {
            rc = ker::vfs::xfs::xfs_log_write(&mount_, items.data(), static_cast<int>(items.size()), &owns_metadata);
        }
        for (ker::vfs::BufHead* home : homes) {
            ker::vfs::brelse(home);
        }
        return rc != 0 ? rc : (owns_metadata ? 0 : -EIO);
    }

    auto stage_home(uint64_t home_block, uint8_t value) -> int {
        return stage_homes(std::array<uint64_t, 1>{home_block}, std::array<uint8_t, 1>{value});
    }

    auto stage_file_data(uint64_t block, uint8_t value) -> int {
        if (!mounted_) {
            return -ENODEV;
        }
        ker::vfs::BufHead* data = ker::vfs::bget(storage_->device(), block, ker::vfs::BufferReadClass::FILE_DATA);
        if (data == nullptr || data->data == nullptr) {
            ker::vfs::brelse(data);
            return -EIO;
        }
        std::memset(data->data, value, data->size);
        ker::vfs::bdirty(data);
        ker::vfs::brelse(data);
        return 0;
    }

    auto flush() -> int { return ker::vfs::xfs::xfs_log_flush(&mount_); }

    auto unmount_clean() -> int {
        if (!mounted_) {
            return -ENODEV;
        }
        int const RC = ker::vfs::xfs::xfs_log_unmount(&mount_, true);
        if (RC == 0) {
            mounted_ = false;
        }
        return RC;
    }

    auto crash_forget() -> bool {
        if (!mounted_ || !ker::vfs::xfs::xfs_selftest_log_crash_forget(&mount_)) {
            return false;
        }
        mounted_ = false;
        return true;
    }

    auto storage() -> ker::test::FaultBlockDevice& { return *storage_; }
    auto mount() -> ker::vfs::xfs::XfsMountContext& { return mount_; }

   private:
    std::unique_ptr<ker::test::FaultBlockDevice> storage_;
    ker::vfs::xfs::XfsMountContext mount_{};
    bool mounted_{};
};

auto media_byte(const ker::test::FaultBlockDevice& storage, bool durable, uint64_t block, size_t byte_offset = 0) -> uint8_t {
    size_t const OFFSET = (static_cast<size_t>(block) * storage.device()->block_size) + byte_offset;
    return durable ? storage.durable_data()[OFFSET] : storage.volatile_data()[OFFSET];
}

template <size_t N>
auto media_blocks_match(const ker::test::FaultBlockDevice& storage, bool durable, const std::array<uint64_t, N>& blocks,
                        const std::array<uint8_t, N>& values) -> bool {
    for (size_t i = 0; i < N; ++i) {
        if (media_byte(storage, durable, blocks.at(i)) != values.at(i)) {
            return false;
        }
    }
    return true;
}

auto media_range_has_nonzero(const ker::test::FaultBlockDevice& storage, bool durable, uint64_t block, size_t count) -> bool {
    size_t const OFFSET = static_cast<size_t>(block) * storage.device()->block_size;
    size_t const BYTES = count * storage.device()->block_size;
    const uint8_t* const DATA = durable ? storage.durable_data() : storage.volatile_data();
    for (size_t i = 0; i < BYTES; ++i) {
        if (DATA[OFFSET + i] != 0) {
            return true;
        }
    }
    return false;
}

struct CheckpointEventOrder {
    size_t log_write{SIZE_MAX};
    size_t log_flush{SIZE_MAX};
    size_t home_write{SIZE_MAX};
    size_t home_flush{SIZE_MAX};
};

auto checkpoint_event_order(const ker::test::FaultBlockDevice& storage, uint64_t home_block) -> CheckpointEventOrder {
    CheckpointEventOrder order{};
    for (size_t i = 0; i < storage.event_count(); ++i) {
        const ker::test::FaultBlockEvent* const EVENT = storage.event(i);
        if (EVENT == nullptr || EVENT->result != 0) {
            continue;
        }
        bool const OVERLAPS_LOG =
            EVENT->block < FAULT_LOG_START + FAULT_LOG_BLOCKS && EVENT->count != 0 && EVENT->block + EVENT->count > FAULT_LOG_START;
        if (EVENT->operation == ker::test::FaultBlockOperation::WRITE && OVERLAPS_LOG && order.log_write == SIZE_MAX) {
            order.log_write = i;
        } else if (EVENT->operation == ker::test::FaultBlockOperation::FLUSH && order.log_write != SIZE_MAX &&
                   order.log_flush == SIZE_MAX) {
            order.log_flush = i;
        } else if (EVENT->operation == ker::test::FaultBlockOperation::WRITE && EVENT->block == home_block && order.log_flush != SIZE_MAX &&
                   order.home_write == SIZE_MAX) {
            order.home_write = i;
        } else if (EVENT->operation == ker::test::FaultBlockOperation::FLUSH && order.home_write != SIZE_MAX) {
            order.home_flush = i;
            break;
        }
    }
    return order;
}

auto has_failed_event(const ker::test::FaultBlockDevice& storage, ker::test::FaultBlockOperation operation, uint64_t operation_index,
                      int error) -> bool {
    for (size_t i = 0; i < storage.event_count(); ++i) {
        const ker::test::FaultBlockEvent* const EVENT = storage.event(i);
        if (EVENT != nullptr && EVENT->operation == operation && EVENT->operation_index == operation_index && EVENT->result == error) {
            return true;
        }
    }
    return false;
}

auto recover_cold_log(ker::test::FaultBlockDevice& storage, uint8_t uuid_byte, uint64_t home_block, uint8_t expected_value) -> bool {
    ker::vfs::xfs::XfsMountContext recovered{};
    configure_synthetic_mount(recovered, storage.device(), uuid_byte);
    int const MOUNT_RC = ker::vfs::xfs::xfs_log_mount(&recovered);
    if (MOUNT_RC != 0) {
        ker::vfs::invalidate_bdev(storage.device());
        return false;
    }

    bool const OK = !ker::vfs::xfs::xfs_log_needs_recovery(&recovered) && media_byte(storage, true, home_block) == expected_value;
    int const UNMOUNT_RC = ker::vfs::xfs::xfs_log_unmount(&recovered, false);
    ker::vfs::invalidate_bdev(storage.device());
    return OK && UNMOUNT_RC == 0;
}

template <size_t N>
auto recover_cold_log_set(ker::test::FaultBlockDevice& storage, uint8_t uuid_byte, const std::array<uint64_t, N>& blocks,
                          const std::array<uint8_t, N>& values) -> bool {
    ker::vfs::xfs::XfsMountContext recovered{};
    configure_synthetic_mount(recovered, storage.device(), uuid_byte);
    int const MOUNT_RC = ker::vfs::xfs::xfs_log_mount(&recovered);
    if (MOUNT_RC != 0) {
        ker::vfs::invalidate_bdev(storage.device());
        return false;
    }

    bool const OK = !ker::vfs::xfs::xfs_log_needs_recovery(&recovered) && media_blocks_match(storage, true, blocks, values);
    int const UNMOUNT_RC = ker::vfs::xfs::xfs_log_unmount(&recovered, false);
    ker::vfs::invalidate_bdev(storage.device());
    return OK && UNMOUNT_RC == 0;
}

constexpr const char* DUAL_SOURCE_PATH = "goal07-source";
constexpr const char* DUAL_LINK_PATH = "goal07-link";
constexpr const char* DUAL_REPLACEMENT_PATH = "goal07-replacement";
constexpr const char* DUAL_AFTER_UNMOUNT_PATH = "goal07-after-unmount";

void cleanup_dual_mount_paths(ker::vfs::xfs::XfsMountContext* mount) {
    if (mount == nullptr) {
        return;
    }
    static_cast<void>(ker::vfs::xfs::xfs_unlink_path(DUAL_AFTER_UNMOUNT_PATH, mount));
    static_cast<void>(ker::vfs::xfs::xfs_unlink_path(DUAL_LINK_PATH, mount));
    static_cast<void>(ker::vfs::xfs::xfs_unlink_path(DUAL_SOURCE_PATH, mount));
    static_cast<void>(ker::vfs::xfs::xfs_unlink_path(DUAL_REPLACEMENT_PATH, mount));
}

auto close_xfs_test_file(ker::vfs::File*& file) -> int {
    if (file == nullptr) {
        return 0;
    }
    int const RC = ker::vfs::vfs_close_file(file);
    file = nullptr;
    return RC;
}

auto exercise_dual_mount_namespace(ker::vfs::xfs::XfsMountContext* mount, uint8_t marker) -> bool {
    if (mount == nullptr) {
        return false;
    }
    cleanup_dual_mount_paths(mount);

    int open_result = 0;
    ker::vfs::File* source =
        ker::vfs::xfs::xfs_open_path(DUAL_SOURCE_PATH, ker::vfs::O_CREAT | ker::vfs::O_EXCL | 1, 0644, mount, &open_result);
    if (source == nullptr || open_result != 0 || source->fops == nullptr || source->fops->vfs_write == nullptr ||
        source->fops->vfs_truncate == nullptr) {
        close_xfs_test_file(source);
        return false;
    }

    std::array<uint8_t, 8192> payload{};
    payload.fill(marker);
    bool ok = source->fops->vfs_write(source, payload.data(), payload.size(), 0) == static_cast<ssize_t>(payload.size());
    ok = ok && ker::vfs::xfs::xfs_fsync(source) == 0;
    ok = ok && source->fops->vfs_truncate(source, 1024) == 0;
    ok = ok && ker::vfs::xfs::xfs_fsync(source) == 0;
    ok = close_xfs_test_file(source) == 0 && ok;
    if (!ok) {
        cleanup_dual_mount_paths(mount);
        return false;
    }

    ok = ker::vfs::xfs::xfs_link_path(DUAL_SOURCE_PATH, DUAL_LINK_PATH, mount) == 0;
    ker::vfs::File* replacement =
        ker::vfs::xfs::xfs_open_path(DUAL_REPLACEMENT_PATH, ker::vfs::O_CREAT | ker::vfs::O_EXCL | 1, 0644, mount, &open_result);
    ok = replacement != nullptr && open_result == 0 && ok;
    ok = close_xfs_test_file(replacement) == 0 && ok;
    ok = ker::vfs::xfs::xfs_rename_path(DUAL_SOURCE_PATH, DUAL_REPLACEMENT_PATH, mount) == 0 && ok;

    ker::vfs::Stat replacement_stat{};
    ker::vfs::Stat link_stat{};
    ok = ker::vfs::xfs::xfs_stat(DUAL_REPLACEMENT_PATH, &replacement_stat, mount) == 0 && ok;
    ok = ker::vfs::xfs::xfs_stat(DUAL_LINK_PATH, &link_stat, mount) == 0 && ok;
    ok = replacement_stat.st_ino == link_stat.st_ino && replacement_stat.st_size == 1024 && link_stat.st_size == 1024 &&
         replacement_stat.st_nlink == 2 && link_stat.st_nlink == 2 && ok;
    return ker::vfs::xfs::xfs_sync_mount(mount) == 0 && ok;
}

auto mutate_after_peer_unmount(ker::vfs::xfs::XfsMountContext* mount) -> bool {
    int open_result = 0;
    ker::vfs::File* file =
        ker::vfs::xfs::xfs_open_path(DUAL_AFTER_UNMOUNT_PATH, ker::vfs::O_CREAT | ker::vfs::O_EXCL | 1, 0644, mount, &open_result);
    if (file == nullptr || open_result != 0 || file->fops == nullptr || file->fops->vfs_write == nullptr) {
        close_xfs_test_file(file);
        return false;
    }
    uint8_t const VALUE = 0x6D;
    bool ok = file->fops->vfs_write(file, &VALUE, 1, 0) == 1;
    ok = ker::vfs::xfs::xfs_fsync(file) == 0 && ok;
    ok = close_xfs_test_file(file) == 0 && ok;
    ok = ker::vfs::xfs::xfs_unlink_path(DUAL_AFTER_UNMOUNT_PATH, mount) == 0 && ok;
    return ker::vfs::xfs::xfs_sync_mount(mount) == 0 && ok;
}

auto verify_and_clean_dual_mount(ker::dev::BlockDevice* device) -> bool {
    ker::vfs::xfs::XfsMountContext* mount = nullptr;
    if (ker::vfs::xfs::xfs_mount(device, false, &mount) != 0 || mount == nullptr) {
        return false;
    }
    ker::vfs::Stat replacement_stat{};
    ker::vfs::Stat link_stat{};
    bool ok = ker::vfs::xfs::xfs_stat(DUAL_REPLACEMENT_PATH, &replacement_stat, mount) == 0;
    ok = ker::vfs::xfs::xfs_stat(DUAL_LINK_PATH, &link_stat, mount) == 0 && ok;
    ok = replacement_stat.st_ino == link_stat.st_ino && replacement_stat.st_size == 1024 && replacement_stat.st_nlink == 2 && ok;
    cleanup_dual_mount_paths(mount);
    ok = ker::vfs::xfs::xfs_sync_mount(mount) == 0 && ok;
    int const UNMOUNT_RC = ker::vfs::xfs::xfs_unmount(mount);
    if (UNMOUNT_RC != 0) {
        ker::vfs::xfs::xfs_unmount_force(mount);
    }
    return UNMOUNT_RC == 0 && ok;
}

auto disposable_dual_xfs_mounts_are_independent() -> bool {
    ker::dev::BlockDevice* const FIRST_DEVICE = ker::dev::block_device_find_by_name("sdb1");
    ker::dev::BlockDevice* const SECOND_DEVICE = ker::dev::block_device_find_by_name("sdb2");
    if (FIRST_DEVICE == nullptr || SECOND_DEVICE == nullptr || ker::dev::block_devices_overlap(FIRST_DEVICE, SECOND_DEVICE)) {
        return false;
    }

    ker::vfs::xfs::XfsMountContext* first = nullptr;
    ker::vfs::xfs::XfsMountContext* second = nullptr;
    if (ker::vfs::xfs::xfs_mount(FIRST_DEVICE, false, &first) != 0 || first == nullptr) {
        return false;
    }
    if (ker::vfs::xfs::xfs_mount(SECOND_DEVICE, false, &second) != 0 || second == nullptr) {
        ker::vfs::xfs::xfs_unmount_force(first);
        return false;
    }

    bool ok = exercise_dual_mount_namespace(first, 0x35);
    ok = exercise_dual_mount_namespace(second, 0xCA) && ok;
    int const FIRST_UNMOUNT_RC = ker::vfs::xfs::xfs_unmount(first);
    if (FIRST_UNMOUNT_RC == 0) {
        first = nullptr;
    }
    ok = FIRST_UNMOUNT_RC == 0 && ok;
    ok = mutate_after_peer_unmount(second) && ok;
    int const SECOND_UNMOUNT_RC = ker::vfs::xfs::xfs_unmount(second);
    if (SECOND_UNMOUNT_RC == 0) {
        second = nullptr;
    }
    ok = SECOND_UNMOUNT_RC == 0 && ok;

    if (first != nullptr) {
        ker::vfs::xfs::xfs_unmount_force(first);
    }
    if (second != nullptr) {
        ker::vfs::xfs::xfs_unmount_force(second);
    }
    if (!ok) {
        return false;
    }
    return verify_and_clean_dual_mount(FIRST_DEVICE) && verify_and_clean_dual_mount(SECOND_DEVICE);
}

constexpr const char* XATTR_FIXTURE_PATH = "linux-xattr-fixture";
constexpr const char* XATTR_MUTATION_PATH = "wos-xattr-cow";
constexpr size_t XATTR_GROWTH_COUNT = 96;
constexpr size_t XATTR_GROWTH_VALUE_SIZE = 192;
constexpr size_t XATTR_REMOTE_VALUE_SIZE = 64 * 1024;

struct AttrFragmentMappingGuard {
    ~AttrFragmentMappingGuard() { ker::vfs::xfs::xfs_selftest_attr_fragment_mappings(false); }
};

auto xattr_growth_name(size_t index) -> std::array<char, 10> {
    return {'u',
            's',
            'e',
            'r',
            '.',
            'k',
            static_cast<char>('0' + ((index / 100) % 10)),
            static_cast<char>('0' + ((index / 10) % 10)),
            static_cast<char>('0' + (index % 10)),
            '\0'};
}

auto xattr_read_matches(ker::vfs::xfs::XfsMountContext* mount, const char* path, const char* name, const uint8_t* expected, size_t size)
    -> bool {
    auto value = std::unique_ptr<uint8_t[]>(size == 0 ? nullptr : new (std::nothrow) uint8_t[size]);
    if (size != 0 && value == nullptr) {
        return false;
    }
    ssize_t const RESULT = ker::vfs::xfs::xfs_getxattr_path(path, name, value.get(), size, mount);
    return RESULT == static_cast<ssize_t>(size) && (size == 0 || std::memcmp(value.get(), expected, size) == 0);
}

auto populate_xattr_growth(ker::vfs::xfs::XfsMountContext* mount) -> bool {
    std::array<uint8_t, XATTR_GROWTH_VALUE_SIZE> value{};
    for (size_t i = 0; i < XATTR_GROWTH_COUNT; ++i) {
        value.fill(static_cast<uint8_t>(i));
        auto const NAME = xattr_growth_name(i);
        if (ker::vfs::xfs::xfs_setxattr_path(XATTR_MUTATION_PATH, NAME.data(), value.data(), value.size(), 0, mount) != 0) {
            return false;
        }
    }
    return true;
}

auto disposable_xattr_growth_remote_shrink_and_teardown() -> bool {
    ker::dev::BlockDevice* const DEVICE = ker::dev::block_device_find_by_name("sdb2");
    if (DEVICE == nullptr) {
        return false;
    }
    ker::vfs::xfs::XfsMountContext* mount = nullptr;
    if (ker::vfs::xfs::xfs_mount(DEVICE, false, &mount) != 0 || mount == nullptr) {
        return false;
    }
    AttrFragmentMappingGuard fragment_guard{};
    auto unmount = [&mount]() {
        if (mount == nullptr) {
            return true;
        }
        int const RC = ker::vfs::xfs::xfs_unmount(mount);
        if (RC != 0) {
            ker::vfs::xfs::xfs_unmount_force(mount);
        }
        mount = nullptr;
        return RC == 0;
    };
    constexpr std::array<uint8_t, 5> LINUX_VALUE{'l', 'i', 'n', 'u', 'x'};
    constexpr std::array<uint8_t, 3> WOS_FIXTURE_VALUE{'w', 'o', 's'};
    constexpr std::array<uint8_t, 4> SF_VALUE{0x53, 0x46, 0x31, 0x30};
    std::array<uint8_t, LINUX_VALUE.size()> linux_readback{};
    ssize_t const LINUX_READ_RC =
        ker::vfs::xfs::xfs_getxattr_path(XATTR_FIXTURE_PATH, "user.linux", linux_readback.data(), linux_readback.size(), mount);
    int const WOS_SET_RC =
        LINUX_READ_RC == static_cast<ssize_t>(LINUX_VALUE.size()) && linux_readback == LINUX_VALUE
            ? ker::vfs::xfs::xfs_setxattr_path(XATTR_FIXTURE_PATH, "user.wos", WOS_FIXTURE_VALUE.data(), WOS_FIXTURE_VALUE.size(), 0, mount)
            : -ECANCELED;
    bool const LINUX_REREAD =
        WOS_SET_RC == 0 && xattr_read_matches(mount, XATTR_FIXTURE_PATH, "user.linux", LINUX_VALUE.data(), LINUX_VALUE.size());
    if (LINUX_READ_RC != static_cast<ssize_t>(LINUX_VALUE.size()) || linux_readback != LINUX_VALUE || WOS_SET_RC != 0 || !LINUX_REREAD) {
        ker::mod::dbg::log("[xfs ktest] Linux fixture failure: get=%ld value_ok=%d set=%d reread=%d", static_cast<long>(LINUX_READ_RC),
                           linux_readback == LINUX_VALUE, WOS_SET_RC, LINUX_REREAD);
        unmount();
        return false;
    }

    static_cast<void>(ker::vfs::xfs::xfs_unlink_path(XATTR_MUTATION_PATH, mount));
    int open_result = 0;
    ker::vfs::File* file =
        ker::vfs::xfs::xfs_open_path(XATTR_MUTATION_PATH, ker::vfs::O_CREAT | ker::vfs::O_EXCL | 1, 0644, mount, &open_result);
    if (file == nullptr || open_result != 0) {
        ker::mod::dbg::log("[xfs ktest] mutation create failure: file=%p rc=%d", file, open_result);
        close_xfs_test_file(file);
        unmount();
        return false;
    }
    int const CLOSE_RC = close_xfs_test_file(file);
    int const SF_SET_RC = CLOSE_RC == 0
                              ? ker::vfs::xfs::xfs_setxattr_path(XATTR_MUTATION_PATH, "user.sf", SF_VALUE.data(), SF_VALUE.size(), 0, mount)
                              : -ECANCELED;
    bool const POPULATED = SF_SET_RC == 0 && populate_xattr_growth(mount);
    if (CLOSE_RC != 0 || SF_SET_RC != 0 || !POPULATED) {
        ker::mod::dbg::log("[xfs ktest] initial growth failure: close=%d sf_set=%d populated=%d", CLOSE_RC, SF_SET_RC, POPULATED);
        unmount();
        return false;
    }

    auto remote_value = std::unique_ptr<uint8_t[]>(new (std::nothrow) uint8_t[XATTR_REMOTE_VALUE_SIZE]);
    if (remote_value == nullptr) {
        unmount();
        return false;
    }
    for (size_t i = 0; i < XATTR_REMOTE_VALUE_SIZE; ++i) {
        remote_value[i] = static_cast<uint8_t>((i * 37U) ^ (i >> 8U));
    }
    ker::vfs::xfs::xfs_selftest_attr_fragment_mappings(true);
    int const REMOTE_SET_RC =
        ker::vfs::xfs::xfs_setxattr_path(XATTR_MUTATION_PATH, "user.remote64k", remote_value.get(), XATTR_REMOTE_VALUE_SIZE, 0, mount);
    if (REMOTE_SET_RC != 0) {
        ker::mod::dbg::log("[xfs ktest] remote value set failure: rc=%d", REMOTE_SET_RC);
        unmount();
        return false;
    }
    ker::vfs::xfs::xfs_selftest_attr_fragment_mappings(false);
    uint8_t format = 0;
    uint16_t extent_count = 0;
    bool const FORMAT_QUERIED = ker::vfs::xfs::xfs_selftest_attr_fork_format(XATTR_MUTATION_PATH, mount, &format, &extent_count);
    bool const REMOTE_MATCHED =
        xattr_read_matches(mount, XATTR_MUTATION_PATH, "user.remote64k", remote_value.get(), XATTR_REMOTE_VALUE_SIZE);
    if (!FORMAT_QUERIED || format != ker::vfs::xfs::XFS_DINODE_FMT_BTREE || extent_count == 0 || !REMOTE_MATCHED) {
        ker::mod::dbg::log("[xfs ktest] fragmented fork failure: queried=%d format=%u extents=%u remote_match=%d", FORMAT_QUERIED, format,
                           extent_count, REMOTE_MATCHED);
        unmount();
        return false;
    }

    std::array<uint8_t, XATTR_GROWTH_VALUE_SIZE> original{};
    std::array<uint8_t, XATTR_GROWTH_VALUE_SIZE> cancelled{};
    original.fill(0);
    cancelled.fill(0xEE);
    int const CANCEL_RC =
        ker::vfs::xfs::xfs_selftest_cancel_setxattr_path(XATTR_MUTATION_PATH, "user.k000", cancelled.data(), cancelled.size(), mount);
    bool const ROLLBACK_MATCHED = xattr_read_matches(mount, XATTR_MUTATION_PATH, "user.k000", original.data(), original.size());
    int const REMOTE_REMOVE_RC = ker::vfs::xfs::xfs_removexattr_path(XATTR_MUTATION_PATH, "user.remote64k", mount);
    bool const REMOVE_FORMAT_QUERIED = ker::vfs::xfs::xfs_selftest_attr_fork_format(XATTR_MUTATION_PATH, mount, &format, &extent_count);
    if (CANCEL_RC != 0 || !ROLLBACK_MATCHED || REMOTE_REMOVE_RC != 0 || !REMOVE_FORMAT_QUERIED ||
        format != ker::vfs::xfs::XFS_DINODE_FMT_EXTENTS) {
        ker::mod::dbg::log("[xfs ktest] rollback/remove failure: cancel=%d rollback_match=%d remove=%d queried=%d format=%u extents=%u",
                           CANCEL_RC, ROLLBACK_MATCHED, REMOTE_REMOVE_RC, REMOVE_FORMAT_QUERIED, format, extent_count);
        unmount();
        return false;
    }
    for (size_t i = 0; i < XATTR_GROWTH_COUNT; ++i) {
        auto const NAME = xattr_growth_name(i);
        int const REMOVE_RC = ker::vfs::xfs::xfs_removexattr_path(XATTR_MUTATION_PATH, NAME.data(), mount);
        if (REMOVE_RC != 0) {
            ker::mod::dbg::log("[xfs ktest] growth remove failure: index=%lu rc=%d", static_cast<unsigned long>(i), REMOVE_RC);
            unmount();
            return false;
        }
    }
    bool const SHRINK_FORMAT_QUERIED = ker::vfs::xfs::xfs_selftest_attr_fork_format(XATTR_MUTATION_PATH, mount, &format, &extent_count);
    bool const SF_MATCHED = xattr_read_matches(mount, XATTR_MUTATION_PATH, "user.sf", SF_VALUE.data(), SF_VALUE.size());
    int const SYNC_RC = ker::vfs::xfs::xfs_sync_mount(mount);
    bool const FIRST_UNMOUNTED = unmount();
    if (!SHRINK_FORMAT_QUERIED || format != ker::vfs::xfs::XFS_DINODE_FMT_LOCAL || !SF_MATCHED || SYNC_RC != 0 || !FIRST_UNMOUNTED) {
        ker::mod::dbg::log("[xfs ktest] shrink failure: queried=%d format=%u extents=%u sf_match=%d sync=%d unmount=%d",
                           SHRINK_FORMAT_QUERIED, format, extent_count, SF_MATCHED, SYNC_RC, FIRST_UNMOUNTED);
        return false;
    }

    int const REMOUNT_RC = ker::vfs::xfs::xfs_mount(DEVICE, false, &mount);
    bool const REMOUNT_LINUX = REMOUNT_RC == 0 && mount != nullptr &&
                               xattr_read_matches(mount, XATTR_FIXTURE_PATH, "user.linux", LINUX_VALUE.data(), LINUX_VALUE.size());
    bool const REMOUNT_WOS =
        REMOUNT_LINUX && xattr_read_matches(mount, XATTR_FIXTURE_PATH, "user.wos", WOS_FIXTURE_VALUE.data(), WOS_FIXTURE_VALUE.size());
    bool const REMOUNT_SF = REMOUNT_WOS && xattr_read_matches(mount, XATTR_MUTATION_PATH, "user.sf", SF_VALUE.data(), SF_VALUE.size());
    bool const REPOPULATED = REMOUNT_SF && populate_xattr_growth(mount);
    if (REMOUNT_RC != 0 || mount == nullptr || !REMOUNT_LINUX || !REMOUNT_WOS || !REMOUNT_SF || !REPOPULATED) {
        ker::mod::dbg::log("[xfs ktest] remount failure: mount=%d linux=%d wos=%d sf=%d repopulated=%d", REMOUNT_RC, REMOUNT_LINUX,
                           REMOUNT_WOS, REMOUNT_SF, REPOPULATED);
        unmount();
        return false;
    }
    ker::vfs::xfs::xfs_selftest_attr_fragment_mappings(true);
    bool const REGREW = ker::vfs::xfs::xfs_setxattr_path(XATTR_MUTATION_PATH, "user.remote64k", remote_value.get(), XATTR_REMOTE_VALUE_SIZE,
                                                         0, mount) == 0 &&
                        ker::vfs::xfs::xfs_selftest_attr_fork_format(XATTR_MUTATION_PATH, mount, &format, &extent_count) &&
                        format == ker::vfs::xfs::XFS_DINODE_FMT_BTREE;
    ker::vfs::xfs::xfs_selftest_attr_fragment_mappings(false);
    bool const TORN_DOWN =
        REGREW && ker::vfs::xfs::xfs_unlink_path(XATTR_MUTATION_PATH, mount) == 0 && ker::vfs::xfs::xfs_sync_mount(mount) == 0;
    bool const SECOND_UNMOUNTED = unmount();
    int const READ_ONLY_MOUNT_RC = TORN_DOWN && SECOND_UNMOUNTED ? ker::vfs::xfs::xfs_mount(DEVICE, true, &mount) : -ECANCELED;
    if (!SECOND_UNMOUNTED || !TORN_DOWN || READ_ONLY_MOUNT_RC != 0 || mount == nullptr) {
        ker::mod::dbg::log("[xfs ktest] regrow/teardown failure: regrew=%d torn_down=%d unmount=%d ro_mount=%d", REGREW, TORN_DOWN,
                           SECOND_UNMOUNTED, READ_ONLY_MOUNT_RC);
        unmount();
        return false;
    }
    uint8_t const DENIED_VALUE = 0x7A;
    bool const READ_ONLY_DENIED =
        ker::vfs::xfs::xfs_setxattr_path(XATTR_FIXTURE_PATH, "user.denied", &DENIED_VALUE, 1, 0, mount) == -EROFS &&
        ker::vfs::xfs::xfs_removexattr_path(XATTR_FIXTURE_PATH, "user.linux", mount) == -EROFS;
    bool const READ_ONLY_UNMOUNTED = unmount();
    if (!READ_ONLY_DENIED || !READ_ONLY_UNMOUNTED) {
        ker::mod::dbg::log("[xfs ktest] read-only failure: denied=%d unmount=%d", READ_ONLY_DENIED, READ_ONLY_UNMOUNTED);
    }
    return READ_ONLY_UNMOUNTED && READ_ONLY_DENIED;
}

}  // namespace

// ---------------------------------------------------------------------------
// Struct-level checks
// ---------------------------------------------------------------------------

KTEST(XFS, MagicConstant) {
    // 'XFSB' stored big-endian = 0x58 0x46 0x53 0x42 = 0x58465342
    KEXPECT_EQ(ker::vfs::xfs::XFS_SB_MAGIC, 0x58465342U);
}

KTEST(XFS, SuperblockStructSize) { KEXPECT_EQ(sizeof(ker::vfs::xfs::XfsDsb), static_cast<size_t>(264)); }

KTEST(XFS, InodeStructSize) { KEXPECT_EQ(sizeof(ker::vfs::xfs::XfsDinode), static_cast<size_t>(176)); }

KTEST(XFS, InodeCacheRetentionBoundsHashLoadAndReclaim) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_inode_cache_reclaim_hysteresis()); }

KTEST(XFS, TransactionCancelRestoresLinkCount) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_transaction_cancel_restores_nlink()); }

KTEST(XFS, TransactionCancelRestoresAttrFork) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_transaction_cancel_restores_attr_fork()); }

KTEST(XFS, AttrCowPreservesStoredParentHash) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_attr_parent_hash_preserved()); }

KTEST(XFS, AttrCowRejectsIncompleteLeafEntry) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_attr_incomplete_detected()); }

KTEST(XFS, AttrCowLinksSameLevelDaNodes) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_attr_node_sibling_links()); }

KTEST(XFS, TransactionRetiredRangesCommitOnly) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_transaction_retired_ranges_commit_only()); }

KTEST(XFS, TransactionCancelRestoresReplacedBufferAlias) {
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_transaction_cancel_restores_replaced_buffer_alias());
}

KTEST(XFS, LogRecycledBufferIsDistinct) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_log_recycled_buffer_is_distinct()); }

KTEST(XFS, LogCheckpointIsOrderedAndBounded) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_log_checkpoint_is_ordered_and_bounded()); }

KTEST(XFS, DisposableDualMountNamespaceMutationsAreIndependent) { KEXPECT_TRUE(disposable_dual_xfs_mounts_are_independent()); }

KTEST(XFS, DisposableXattrGrowthRemoteShrinkRollbackRemountAndTeardown) {
    KEXPECT_TRUE(disposable_xattr_growth_remote_shrink_and_teardown());
}

KTEST(FaultBlockDevice, VolatileDurableTornWriteAndPowerCutAreDeterministic) {
    auto storage =
        std::unique_ptr<ker::test::FaultBlockDevice>(new (std::nothrow) ker::test::FaultBlockDevice(FAULT_BLOCK_SIZE, FAULT_TOTAL_BLOCKS));
    KREQUIRE_NE(storage.get(), nullptr);
    KREQUIRE_TRUE(storage->valid());

    std::array<uint8_t, FAULT_BLOCK_SIZE> first{};
    std::array<uint8_t, FAULT_BLOCK_SIZE> second{};
    std::array<uint8_t, FAULT_BLOCK_SIZE> readback{};
    first.fill(0x31);
    second.fill(0x72);

    constexpr uint64_t SCENARIO_SEED = 0x58465357414CULL;
    storage->set_scenario_seed(SCENARIO_SEED);
    KREQUIRE_TRUE(storage->fail_operation(ker::test::FaultBlockOperation::WRITE, 2, -EIO, ker::test::FaultBlockWriteMode::TORN_PREFIX, 17));
    KEXPECT_EQ(ker::dev::block_write(storage->device(), 4, 1, first.data()), 0);
    KEXPECT_EQ(ker::dev::block_write(storage->device(), 5, 1, second.data()), -EIO);
    KREQUIRE_EQ(storage->event_count(), static_cast<size_t>(2));
    KEXPECT_EQ(storage->event(1)->scenario_seed, SCENARIO_SEED);
    KEXPECT_EQ(storage->event(1)->global_index, static_cast<uint64_t>(2));
    KEXPECT_EQ(storage->event(1)->operation_index, static_cast<uint64_t>(2));
    KEXPECT_EQ(storage->event(1)->transferred_bytes, static_cast<size_t>(17));
    KEXPECT_EQ(media_byte(*storage, false, 5), static_cast<uint8_t>(0x72));
    KEXPECT_EQ(media_byte(*storage, false, 5, 16), static_cast<uint8_t>(0x72));
    KEXPECT_EQ(media_byte(*storage, false, 5, 17), static_cast<uint8_t>(0));
    KEXPECT_EQ(media_byte(*storage, true, 4), static_cast<uint8_t>(0));

    storage->power_cut();
    KEXPECT_TRUE(storage->volatile_matches_durable());
    KEXPECT_EQ(storage->power_cut_count(), static_cast<uint64_t>(1));
    KEXPECT_EQ(media_byte(*storage, false, 4), static_cast<uint8_t>(0));
    KEXPECT_EQ(media_byte(*storage, false, 5), static_cast<uint8_t>(0));

    storage->clear_history();
    KEXPECT_EQ(ker::dev::block_write(storage->device(), 6, 1, first.data()), 0);
    KREQUIRE_TRUE(storage->fail_operation(ker::test::FaultBlockOperation::FLUSH, 1, -EIO));
    KEXPECT_EQ(ker::dev::block_flush(storage->device()), -EIO);
    KEXPECT_EQ(media_byte(*storage, true, 6), static_cast<uint8_t>(0));
    storage->power_cut();
    KEXPECT_EQ(media_byte(*storage, false, 6), static_cast<uint8_t>(0));

    KEXPECT_EQ(ker::dev::block_write(storage->device(), 6, 1, second.data()), 0);
    KEXPECT_EQ(ker::dev::block_flush(storage->device()), 0);
    KEXPECT_EQ(storage->durability_generation(), static_cast<uint64_t>(1));
    storage->power_cut();
    KEXPECT_EQ(media_byte(*storage, false, 6), static_cast<uint8_t>(0x72));

    auto cold_storage =
        std::unique_ptr<ker::test::FaultBlockDevice>(new (std::nothrow) ker::test::FaultBlockDevice(FAULT_BLOCK_SIZE, FAULT_TOTAL_BLOCKS));
    KREQUIRE_NE(cold_storage.get(), nullptr);
    KREQUIRE_TRUE(cold_storage->seed_bytes(0, storage->durable_data(), storage->media_bytes()));
    KEXPECT_EQ(ker::dev::block_read(cold_storage->device(), 6, 1, readback.data()), 0);
    KEXPECT_EQ(readback.at(0), static_cast<uint8_t>(0x72));

    storage->clear_history();
    KREQUIRE_TRUE(storage->fail_operation(ker::test::FaultBlockOperation::READ, 1, -EIO));
    readback.fill(0xCC);
    KEXPECT_EQ(ker::dev::block_read(storage->device(), 6, 1, readback.data()), -EIO);
    KEXPECT_EQ(readback.at(0), static_cast<uint8_t>(0xCC));
    KEXPECT_TRUE(has_failed_event(*storage, ker::test::FaultBlockOperation::READ, 1, -EIO));
    KEXPECT_FALSE(storage->history_overflowed());

    storage->reset(0x9D);
    KEXPECT_EQ(storage->event_count(), static_cast<size_t>(0));
    KEXPECT_EQ(storage->durability_generation(), static_cast<uint64_t>(0));
    KEXPECT_TRUE(storage->volatile_matches_durable());
    KEXPECT_EQ(media_byte(*storage, true, 0), static_cast<uint8_t>(0x9D));

    KREQUIRE_TRUE(storage->fail_global(2, -EIO));
    KEXPECT_EQ(ker::dev::block_read(storage->device(), 0, 1, readback.data()), 0);
    KEXPECT_EQ(ker::dev::block_flush(storage->device()), -EIO);
    KREQUIRE_EQ(storage->event_count(), static_cast<size_t>(2));
    KEXPECT_EQ(storage->event(1)->global_index, static_cast<uint64_t>(2));
    KEXPECT_EQ(storage->event(1)->operation, ker::test::FaultBlockOperation::FLUSH);
}

KTEST(XFS, LogMountReadFaultIsRetryableAtExactOperation) {
    SyntheticXfsLogFixture fixture(0x11);
    KREQUIRE_TRUE(fixture.valid());
    KREQUIRE_TRUE(fixture.storage().fail_operations(ker::test::FaultBlockOperation::READ, 1, 3, -EIO));

    KEXPECT_EQ(fixture.mount_log(), -EIO);
    KEXPECT_TRUE(has_failed_event(fixture.storage(), ker::test::FaultBlockOperation::READ, 1, -EIO));
    KEXPECT_TRUE(has_failed_event(fixture.storage(), ker::test::FaultBlockOperation::READ, 3, -EIO));

    fixture.storage().clear_history();
    KEXPECT_EQ(fixture.mount_log(), 0);
    KEXPECT_FALSE(ker::vfs::xfs::xfs_log_needs_recovery(&fixture.mount()));
    KEXPECT_FALSE(fixture.storage().history_overflowed());
}

KTEST(XFS, StandardCleanUnmountAllowsOpaqueTransactionIdAndStaleMarkers) {
    constexpr uint8_t UUID_BYTE = 0xD4;
    SyntheticXfsLogFixture fixture(UUID_BYTE);
    KREQUIRE_TRUE(fixture.valid());

    ker::vfs::xfs::XlogRecHeader header{};
    header.h_magicno = Be32::from_cpu(ker::vfs::xfs::XLOG_HEADER_MAGIC_NUM);
    header.h_cycle = Be32::from_cpu(1);
    header.h_version = Be32::from_cpu(ker::vfs::xfs::XLOG_VERSION_2);
    header.h_len = Be32::from_cpu(ker::vfs::xfs::XLOG_HEADER_SIZE);
    header.h_lsn = Be64::from_cpu(uint64_t{1} << 32);
    header.h_tail_lsn = header.h_lsn;
    header.h_prev_block = Be32::from_cpu(UINT32_MAX);
    header.h_num_logops = Be32::from_cpu(1);
    header.h_cycle_data.at(0) = Be32::from_cpu(0xB0C0D0D0);
    header.h_fmt = Be32::from_cpu(ker::vfs::xfs::XLOG_FMT_LINUX_LE);
    header.h_fs_uuid.b.fill(UUID_BYTE);
    header.h_size = Be32::from_cpu(32U * 1024U);

    std::array<uint8_t, ker::vfs::xfs::XLOG_HEADER_SIZE> body{};
    Be32 const PACKED_CYCLE = Be32::from_cpu(1);
    // Runtime XFS unmount records use oh_len=0; mkfs.xfs uses 8. Both carry
    // the same unmount flag/magic and are accepted by Linux recovery.
    Be32 const OPERATION_BYTES = Be32::from_cpu(0);
    uint16_t const UNMOUNT_MAGIC = ker::vfs::xfs::XLOG_UNMOUNT_TYPE;
    std::memcpy(body.data(), &PACKED_CYCLE, sizeof(PACKED_CYCLE));
    std::memcpy(body.data() + 4, &OPERATION_BYTES, sizeof(OPERATION_BYTES));
    body.at(8) = ker::vfs::xfs::XFS_LOG;
    body.at(9) = ker::vfs::xfs::XLOG_UNMOUNT_TRANS;
    std::memcpy(body.data() + 12, &UNMOUNT_MAGIC, sizeof(UNMOUNT_MAGIC));

    size_t const LOG_OFFSET = static_cast<size_t>(FAULT_LOG_START) * FAULT_BLOCK_SIZE;
    KREQUIRE_TRUE(fixture.storage().seed_bytes(LOG_OFFSET, &header, sizeof(header)));
    KREQUIRE_TRUE(fixture.storage().seed_bytes(LOG_OFFSET + sizeof(header), body.data(), body.size()));

    // xlog_clear_stale_blocks() writes exact zero-length headers ahead of the
    // clean head. They carry an old cycle and must not be parsed as records.
    ker::vfs::xfs::XlogRecHeader stale{};
    stale.h_magicno = Be32::from_cpu(ker::vfs::xfs::XLOG_HEADER_MAGIC_NUM);
    stale.h_cycle = Be32::from_cpu(0);
    stale.h_version = Be32::from_cpu(ker::vfs::xfs::XLOG_VERSION_2);
    stale.h_lsn = Be64::from_cpu(3);
    stale.h_tail_lsn = Be64::from_cpu(uint64_t{1} << 32);
    stale.h_fmt = Be32::from_cpu(ker::vfs::xfs::XLOG_FMT_LINUX_LE);
    stale.h_fs_uuid.b.fill(UUID_BYTE);
    KREQUIRE_TRUE(fixture.storage().seed_bytes(LOG_OFFSET + (3 * FAULT_BLOCK_SIZE), &stale, sizeof(stale)));
    KREQUIRE_EQ(fixture.mount_log(), 0);
    KEXPECT_FALSE(ker::vfs::xfs::xfs_log_needs_recovery(&fixture.mount()));
}

KTEST(XFS, TwoMountedLogsKeepDeviceStateAndCheckpointOrderIsolated) {
    constexpr std::array<uint8_t, SEMANTIC_HOME_VALUES.size()> FIRST_VALUES{{0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7}};
    constexpr std::array<uint8_t, SEMANTIC_HOME_VALUES.size()> SECOND_VALUES{{0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7}};
    constexpr std::array<uint8_t, SEMANTIC_HOME_VALUES.size()> PRE_STATE{};
    SyntheticXfsLogFixture first(0xA1);
    SyntheticXfsLogFixture second(0xB2);
    KREQUIRE_TRUE(first.valid());
    KREQUIRE_TRUE(second.valid());
    KREQUIRE_EQ(first.mount_log(), 0);
    KREQUIRE_EQ(second.mount_log(), 0);
    first.storage().clear_history();
    second.storage().clear_history();

    KREQUIRE_EQ(first.stage_homes(SEMANTIC_HOME_BLOCKS, FIRST_VALUES), 0);
    KREQUIRE_EQ(second.stage_homes(SEMANTIC_HOME_BLOCKS, SECOND_VALUES), 0);
    size_t const FIRST_EVENTS_BEFORE_FLUSH = first.storage().event_count();
    size_t const SECOND_EVENTS_BEFORE_FIRST_FLUSH = second.storage().event_count();

    KREQUIRE_EQ(first.flush(), 0);
    KEXPECT_TRUE(first.storage().event_count() > FIRST_EVENTS_BEFORE_FLUSH);
    KEXPECT_EQ(second.storage().event_count(), SECOND_EVENTS_BEFORE_FIRST_FLUSH);
    KEXPECT_TRUE(media_blocks_match(first.storage(), true, SEMANTIC_HOME_BLOCKS, FIRST_VALUES));
    KEXPECT_TRUE(media_blocks_match(second.storage(), true, SEMANTIC_HOME_BLOCKS, PRE_STATE));

    CheckpointEventOrder const FIRST_ORDER = checkpoint_event_order(first.storage(), SEMANTIC_HOME_BLOCKS.front());
    KEXPECT_TRUE(FIRST_ORDER.log_write < FIRST_ORDER.log_flush);
    KEXPECT_TRUE(FIRST_ORDER.log_flush < FIRST_ORDER.home_write);
    KEXPECT_TRUE(FIRST_ORDER.home_write < FIRST_ORDER.home_flush);
    KREQUIRE_EQ(first.unmount_clean(), 0);
    KEXPECT_EQ(first.mount().log, nullptr);
    KEXPECT_NE(second.mount().log, nullptr);
    KEXPECT_TRUE(ker::vfs::xfs::xfs_log_needs_recovery(&second.mount()));
    KEXPECT_EQ(second.storage().event_count(), SECOND_EVENTS_BEFORE_FIRST_FLUSH);

    KREQUIRE_EQ(second.flush(), 0);
    KEXPECT_TRUE(media_blocks_match(second.storage(), true, SEMANTIC_HOME_BLOCKS, SECOND_VALUES));
    CheckpointEventOrder const SECOND_ORDER = checkpoint_event_order(second.storage(), SEMANTIC_HOME_BLOCKS.front());
    KEXPECT_TRUE(SECOND_ORDER.log_write < SECOND_ORDER.log_flush);
    KEXPECT_TRUE(SECOND_ORDER.log_flush < SECOND_ORDER.home_write);
    KEXPECT_TRUE(SECOND_ORDER.home_write < SECOND_ORDER.home_flush);
    KREQUIRE_EQ(second.unmount_clean(), 0);
    KEXPECT_EQ(second.mount().log, nullptr);
    KEXPECT_FALSE(first.storage().history_overflowed());
    KEXPECT_FALSE(second.storage().history_overflowed());
}

KTEST(XFS, CheckpointFaultMatrixRetainsOrderAndRetriesExactOperation) {
    struct FaultCase {
        ker::test::FaultBlockOperation operation;
        uint64_t operation_index;
        bool data_is_durable;
        bool data_is_volatile;
        bool log_is_durable;
        bool home_is_volatile;
        uint8_t marker;
    };

    CheckpointEventOrder baseline_order{};
    size_t data_write_event = SIZE_MAX;
    size_t data_flush_event = SIZE_MAX;
    uint64_t data_write_operation = 0;
    uint64_t data_flush_operation = 0;
    uint64_t log_write_operation = 0;
    uint64_t log_flush_operation = 0;
    uint64_t home_write_operation = 0;
    uint64_t home_flush_operation = 0;
    {
        SyntheticXfsLogFixture baseline(0xBF);
        KREQUIRE_TRUE(baseline.valid());
        KREQUIRE_EQ(baseline.mount_log(), 0);
        baseline.storage().clear_history();
        KREQUIRE_EQ(baseline.stage_file_data(FAULT_DATA_BLOCK, 0x2F), 0);
        KREQUIRE_EQ(baseline.stage_home(FAULT_HOME_BLOCK, 0x3F), 0);
        KREQUIRE_EQ(baseline.flush(), 0);
        for (size_t i = 0; i < baseline.storage().event_count(); ++i) {
            const ker::test::FaultBlockEvent* const EVENT = baseline.storage().event(i);
            if (EVENT == nullptr || EVENT->result != 0) {
                continue;
            }
            if (EVENT->operation == ker::test::FaultBlockOperation::WRITE && EVENT->block == FAULT_DATA_BLOCK &&
                data_write_event == SIZE_MAX) {
                data_write_event = i;
                data_write_operation = EVENT->operation_index;
            } else if (EVENT->operation == ker::test::FaultBlockOperation::FLUSH && data_write_event != SIZE_MAX &&
                       data_flush_event == SIZE_MAX) {
                data_flush_event = i;
                data_flush_operation = EVENT->operation_index;
            }
        }
        baseline_order = checkpoint_event_order(baseline.storage(), FAULT_HOME_BLOCK);
        KREQUIRE_NE(data_write_event, SIZE_MAX);
        KREQUIRE_NE(data_flush_event, SIZE_MAX);
        KREQUIRE_NE(baseline_order.log_write, SIZE_MAX);
        KREQUIRE_NE(baseline_order.log_flush, SIZE_MAX);
        KREQUIRE_NE(baseline_order.home_write, SIZE_MAX);
        KREQUIRE_NE(baseline_order.home_flush, SIZE_MAX);
        KEXPECT_TRUE(data_write_event < data_flush_event);
        KEXPECT_TRUE(data_flush_event < baseline_order.log_write);
        log_write_operation = baseline.storage().event(baseline_order.log_write)->operation_index;
        log_flush_operation = baseline.storage().event(baseline_order.log_flush)->operation_index;
        home_write_operation = baseline.storage().event(baseline_order.home_write)->operation_index;
        home_flush_operation = baseline.storage().event(baseline_order.home_flush)->operation_index;
    }

    std::array<FaultCase, 6> const CASES{{
        {.operation = ker::test::FaultBlockOperation::WRITE,
         .operation_index = data_write_operation,
         .data_is_durable = false,
         .data_is_volatile = false,
         .log_is_durable = false,
         .home_is_volatile = false,
         .marker = 0x41},
        {.operation = ker::test::FaultBlockOperation::FLUSH,
         .operation_index = data_flush_operation,
         .data_is_durable = false,
         .data_is_volatile = true,
         .log_is_durable = false,
         .home_is_volatile = false,
         .marker = 0x42},
        {.operation = ker::test::FaultBlockOperation::WRITE,
         .operation_index = log_write_operation,
         .data_is_durable = true,
         .data_is_volatile = true,
         .log_is_durable = false,
         .home_is_volatile = false,
         .marker = 0x43},
        {.operation = ker::test::FaultBlockOperation::FLUSH,
         .operation_index = log_flush_operation,
         .data_is_durable = true,
         .data_is_volatile = true,
         .log_is_durable = false,
         .home_is_volatile = false,
         .marker = 0x44},
        {.operation = ker::test::FaultBlockOperation::WRITE,
         .operation_index = home_write_operation,
         .data_is_durable = true,
         .data_is_volatile = true,
         .log_is_durable = true,
         .home_is_volatile = false,
         .marker = 0x45},
        {.operation = ker::test::FaultBlockOperation::FLUSH,
         .operation_index = home_flush_operation,
         .data_is_durable = true,
         .data_is_volatile = true,
         .log_is_durable = true,
         .home_is_volatile = true,
         .marker = 0x46},
    }};

    for (size_t case_index = 0; case_index < CASES.size(); ++case_index) {
        FaultCase const& fault_case = CASES.at(case_index);
        SyntheticXfsLogFixture fixture(static_cast<uint8_t>(0xC0 + case_index));
        KREQUIRE_TRUE(fixture.valid());
        KREQUIRE_EQ(fixture.mount_log(), 0);
        fixture.storage().clear_history();
        uint8_t const DATA_MARKER = static_cast<uint8_t>(fault_case.marker ^ 0x80U);
        KREQUIRE_EQ(fixture.stage_file_data(FAULT_DATA_BLOCK, DATA_MARKER), 0);
        KREQUIRE_EQ(fixture.stage_home(FAULT_HOME_BLOCK, fault_case.marker), 0);
        KREQUIRE_TRUE(fixture.storage().fail_operation(fault_case.operation, fault_case.operation_index, -EIO));

        KEXPECT_EQ(fixture.flush(), -EIO);
        KEXPECT_TRUE(has_failed_event(fixture.storage(), fault_case.operation, fault_case.operation_index, -EIO));
        KEXPECT_EQ(media_byte(fixture.storage(), true, FAULT_DATA_BLOCK) == DATA_MARKER, fault_case.data_is_durable);
        KEXPECT_EQ(media_byte(fixture.storage(), false, FAULT_DATA_BLOCK) == DATA_MARKER, fault_case.data_is_volatile);
        KEXPECT_EQ(media_range_has_nonzero(fixture.storage(), true, FAULT_LOG_START, FAULT_LOG_BLOCKS), fault_case.log_is_durable);
        KEXPECT_EQ(media_byte(fixture.storage(), true, FAULT_HOME_BLOCK), static_cast<uint8_t>(0));
        KEXPECT_EQ(media_byte(fixture.storage(), false, FAULT_HOME_BLOCK) == fault_case.marker, fault_case.home_is_volatile);

        fixture.storage().clear_fault();
        KEXPECT_EQ(fixture.flush(), 0);
        KEXPECT_EQ(media_byte(fixture.storage(), true, FAULT_DATA_BLOCK), DATA_MARKER);
        KEXPECT_EQ(media_byte(fixture.storage(), true, FAULT_HOME_BLOCK), fault_case.marker);
        KEXPECT_FALSE(fixture.storage().history_overflowed());
    }
}

KTEST(XFS, DurableWalRecoversAfterColdPowerCutAndReplaysIdempotently) {
    uint64_t home_write_operation = 0;
    {
        SyntheticXfsLogFixture baseline(0xD6);
        KREQUIRE_TRUE(baseline.valid());
        KREQUIRE_EQ(baseline.mount_log(), 0);
        baseline.storage().clear_history();
        KREQUIRE_EQ(baseline.stage_home(FAULT_HOME_BLOCK, 0x66), 0);
        KREQUIRE_EQ(baseline.flush(), 0);
        CheckpointEventOrder const ORDER = checkpoint_event_order(baseline.storage(), FAULT_HOME_BLOCK);
        KREQUIRE_NE(ORDER.home_write, SIZE_MAX);
        home_write_operation = baseline.storage().event(ORDER.home_write)->operation_index;
    }

    constexpr uint8_t UUID_BYTE = 0xD7;
    constexpr uint8_t HOME_VALUE = 0x7D;
    SyntheticXfsLogFixture crashed(UUID_BYTE);
    KREQUIRE_TRUE(crashed.valid());
    KREQUIRE_EQ(crashed.mount_log(), 0);
    crashed.storage().clear_history();
    KREQUIRE_EQ(crashed.stage_home(FAULT_HOME_BLOCK, HOME_VALUE), 0);
    KREQUIRE_TRUE(crashed.storage().fail_operation(ker::test::FaultBlockOperation::WRITE, home_write_operation, -EIO));

    KEXPECT_EQ(crashed.flush(), -EIO);
    KEXPECT_TRUE(has_failed_event(crashed.storage(), ker::test::FaultBlockOperation::WRITE, home_write_operation, -EIO));
    KEXPECT_TRUE(media_range_has_nonzero(crashed.storage(), true, FAULT_LOG_START, FAULT_LOG_BLOCKS));
    KEXPECT_EQ(media_byte(crashed.storage(), true, FAULT_HOME_BLOCK), static_cast<uint8_t>(0));

    KREQUIRE_TRUE(crashed.crash_forget());
    crashed.storage().power_cut();
    ker::vfs::invalidate_bdev(crashed.storage().device());
    KEXPECT_FALSE(ker::vfs::has_cached_bdev_range(crashed.storage().device(), 0, FAULT_TOTAL_BLOCKS));
    KEXPECT_TRUE(crashed.storage().volatile_matches_durable());

    KEXPECT_TRUE(recover_cold_log(crashed.storage(), UUID_BYTE, FAULT_HOME_BLOCK, HOME_VALUE));
    KEXPECT_EQ(media_byte(crashed.storage(), true, FAULT_HOME_BLOCK), HOME_VALUE);

    crashed.storage().power_cut();
    ker::vfs::invalidate_bdev(crashed.storage().device());
    KEXPECT_TRUE(recover_cold_log(crashed.storage(), UUID_BYTE, FAULT_HOME_BLOCK, HOME_VALUE));
    KEXPECT_EQ(media_byte(crashed.storage(), true, FAULT_HOME_BLOCK), HOME_VALUE);
    KEXPECT_FALSE(crashed.storage().history_overflowed());
}

KTEST(XFS, SemanticMetadataMatrixFailsClosedAtEveryReplayWriteAndConverges) {
    uint64_t first_home_write_operation = 0;
    {
        SyntheticXfsLogFixture baseline(0xA7);
        KREQUIRE_TRUE(baseline.valid());
        KREQUIRE_EQ(baseline.mount_log(), 0);
        baseline.storage().clear_history();
        KREQUIRE_EQ(baseline.stage_homes(SEMANTIC_HOME_BLOCKS, SEMANTIC_HOME_VALUES), 0);
        KREQUIRE_EQ(baseline.flush(), 0);
        CheckpointEventOrder const ORDER = checkpoint_event_order(baseline.storage(), SEMANTIC_HOME_BLOCKS.front());
        KREQUIRE_NE(ORDER.home_write, SIZE_MAX);
        first_home_write_operation = baseline.storage().event(ORDER.home_write)->operation_index;
    }

    std::array<uint8_t, SEMANTIC_HOME_VALUES.size()> const PRE_STATE{};
    for (size_t failed_item = 0; failed_item < SEMANTIC_HOME_BLOCKS.size(); ++failed_item) {
        uint8_t const UUID_BYTE = static_cast<uint8_t>(0xB0 + failed_item);
        SyntheticXfsLogFixture crashed(UUID_BYTE);
        KREQUIRE_TRUE(crashed.valid());
        KREQUIRE_EQ(crashed.mount_log(), 0);
        crashed.storage().clear_history();
        KREQUIRE_EQ(crashed.stage_homes(SEMANTIC_HOME_BLOCKS, SEMANTIC_HOME_VALUES), 0);
        KREQUIRE_TRUE(
            crashed.storage().fail_operation(ker::test::FaultBlockOperation::WRITE, first_home_write_operation + failed_item, -EIO));
        KREQUIRE_EQ(crashed.flush(), -EIO);
        KREQUIRE_TRUE(crashed.crash_forget());
        crashed.storage().power_cut();
        ker::vfs::invalidate_bdev(crashed.storage().device());
        KREQUIRE_TRUE(media_blocks_match(crashed.storage(), true, SEMANTIC_HOME_BLOCKS, PRE_STATE));
        KREQUIRE_TRUE(media_range_has_nonzero(crashed.storage(), true, FAULT_LOG_START, FAULT_LOG_BLOCKS));

        crashed.storage().clear_history();
        uint64_t const REPLAY_WRITE_OPERATION = failed_item + 1;
        KREQUIRE_TRUE(crashed.storage().fail_operation(ker::test::FaultBlockOperation::WRITE, REPLAY_WRITE_OPERATION, -EIO));
        ker::vfs::xfs::XfsMountContext failed_recovery{};
        configure_synthetic_mount(failed_recovery, crashed.storage().device(), UUID_BYTE);
        KEXPECT_EQ(ker::vfs::xfs::xfs_log_mount(&failed_recovery), -EIO);
        KEXPECT_TRUE(has_failed_event(crashed.storage(), ker::test::FaultBlockOperation::WRITE, REPLAY_WRITE_OPERATION, -EIO));
        KEXPECT_EQ(failed_recovery.log, nullptr);
        KEXPECT_EQ(failed_recovery.log_batch, nullptr);
        KEXPECT_TRUE(media_blocks_match(crashed.storage(), true, SEMANTIC_HOME_BLOCKS, PRE_STATE));
        KEXPECT_TRUE(media_range_has_nonzero(crashed.storage(), true, FAULT_LOG_START, FAULT_LOG_BLOCKS));

        // The rejected mount may have submitted a volatile replay prefix, but
        // the dirty WAL is still durable. A cold retry must replay the full
        // post-state, and another cold mount must observe the same state.
        crashed.storage().power_cut();
        crashed.storage().clear_fault();
        ker::vfs::invalidate_bdev(crashed.storage().device());
        KEXPECT_TRUE(recover_cold_log_set(crashed.storage(), UUID_BYTE, SEMANTIC_HOME_BLOCKS, SEMANTIC_HOME_VALUES));
        KEXPECT_TRUE(media_blocks_match(crashed.storage(), true, SEMANTIC_HOME_BLOCKS, SEMANTIC_HOME_VALUES));

        crashed.storage().power_cut();
        ker::vfs::invalidate_bdev(crashed.storage().device());
        KEXPECT_TRUE(recover_cold_log_set(crashed.storage(), UUID_BYTE, SEMANTIC_HOME_BLOCKS, SEMANTIC_HOME_VALUES));
        KEXPECT_FALSE(crashed.storage().history_overflowed());
    }
}

KTEST(XFS, CleanPublicationFailuresRetainEvidenceForRetryAndColdRecovery) {
    struct CleanFaultCase {
        ker::test::FaultBlockOperation operation;
        uint64_t operation_index;
    };

    uint64_t clean_write_operation = 0;
    uint64_t clean_flush_operation = 0;
    {
        SyntheticXfsLogFixture baseline(0xC7);
        KREQUIRE_TRUE(baseline.valid());
        KREQUIRE_EQ(baseline.mount_log(), 0);
        KREQUIRE_EQ(baseline.stage_home(FAULT_HOME_BLOCK, 0x71), 0);
        KREQUIRE_EQ(baseline.flush(), 0);
        baseline.storage().clear_history();
        KREQUIRE_EQ(baseline.unmount_clean(), 0);
        for (size_t i = 0; i < baseline.storage().event_count(); ++i) {
            const ker::test::FaultBlockEvent* const EVENT = baseline.storage().event(i);
            if (EVENT == nullptr || EVENT->result != 0) {
                continue;
            }
            if (EVENT->operation == ker::test::FaultBlockOperation::WRITE && clean_write_operation == 0) {
                clean_write_operation = EVENT->operation_index;
            } else if (EVENT->operation == ker::test::FaultBlockOperation::FLUSH && clean_flush_operation == 0) {
                clean_flush_operation = EVENT->operation_index;
            }
        }
        KREQUIRE_NE(clean_write_operation, static_cast<uint64_t>(0));
        KREQUIRE_NE(clean_flush_operation, static_cast<uint64_t>(0));
    }

    std::array<CleanFaultCase, 2> const CASES{{
        {.operation = ker::test::FaultBlockOperation::WRITE, .operation_index = clean_write_operation},
        {.operation = ker::test::FaultBlockOperation::FLUSH, .operation_index = clean_flush_operation},
    }};
    for (size_t case_index = 0; case_index < CASES.size(); ++case_index) {
        for (size_t crash_mode = 0; crash_mode < 2; ++crash_mode) {
            uint8_t const UUID_BYTE = static_cast<uint8_t>(0xD0 + case_index * 2 + crash_mode);
            uint8_t const HOME_VALUE = static_cast<uint8_t>(0x72 + case_index * 2 + crash_mode);
            SyntheticXfsLogFixture fixture(UUID_BYTE);
            KREQUIRE_TRUE(fixture.valid());
            KREQUIRE_EQ(fixture.mount_log(), 0);
            KREQUIRE_EQ(fixture.stage_home(FAULT_HOME_BLOCK, HOME_VALUE), 0);
            KREQUIRE_EQ(fixture.flush(), 0);
            fixture.storage().clear_history();
            CleanFaultCase const& FAULT = CASES.at(case_index);
            KREQUIRE_TRUE(fixture.storage().fail_operation(FAULT.operation, FAULT.operation_index, -EIO));

            KEXPECT_EQ(fixture.unmount_clean(), -EIO);
            KEXPECT_TRUE(has_failed_event(fixture.storage(), FAULT.operation, FAULT.operation_index, -EIO));
            KEXPECT_NE(fixture.mount().log, nullptr);
            KEXPECT_NE(fixture.mount().log_batch, nullptr);
            KEXPECT_TRUE(ker::vfs::xfs::xfs_log_needs_recovery(&fixture.mount()));

            fixture.storage().clear_fault();
            if (crash_mode == 0) {
                KEXPECT_EQ(fixture.unmount_clean(), 0);
            } else {
                KREQUIRE_TRUE(fixture.crash_forget());
            }
            fixture.storage().power_cut();
            ker::vfs::invalidate_bdev(fixture.storage().device());
            KEXPECT_TRUE(recover_cold_log(fixture.storage(), UUID_BYTE, FAULT_HOME_BLOCK, HOME_VALUE));
            KEXPECT_EQ(media_byte(fixture.storage(), true, FAULT_HOME_BLOCK), HOME_VALUE);
            KEXPECT_FALSE(fixture.storage().history_overflowed());
        }
    }
}

KTEST(XFS, RepeatedCheckpointsReuseAndWrapBoundedLog) {
    constexpr uint32_t SMALL_LOG_BLOCKS = 8;
    constexpr size_t MAX_CHECKPOINTS = 16;
    SyntheticXfsLogFixture fixture(0xD8, SMALL_LOG_BLOCKS);
    KREQUIRE_TRUE(fixture.valid());
    KREQUIRE_EQ(fixture.mount_log(), 0);
    KREQUIRE_NE(fixture.mount().log, nullptr);
    uint32_t const INITIAL_CYCLE = fixture.mount().log->head_cycle;

    size_t completed = 0;
    while (completed < MAX_CHECKPOINTS && fixture.mount().log->head_cycle == INITIAL_CYCLE) {
        size_t const i = completed;
        uint8_t const MARKER = static_cast<uint8_t>(0x80 + i);
        KREQUIRE_EQ(fixture.stage_home(FAULT_HOME_BLOCK, MARKER), 0);
        KREQUIRE_EQ(fixture.flush(), 0);
        KEXPECT_EQ(media_byte(fixture.storage(), true, FAULT_HOME_BLOCK), MARKER);
        completed++;
    }

    KEXPECT_NE(completed, static_cast<size_t>(0));
    KEXPECT_TRUE(fixture.mount().log->head_cycle > INITIAL_CYCLE);
    KEXPECT_TRUE(fixture.mount().log->head_block < SMALL_LOG_BLOCKS);
    KEXPECT_TRUE(fixture.mount().log->tail_block < SMALL_LOG_BLOCKS);
    KEXPECT_FALSE(fixture.storage().history_overflowed());
}

KTEST(XFS, ColdRecoveryReadFaultPreservesDurableWalForRetry) {
    uint64_t home_write_operation = 0;
    {
        SyntheticXfsLogFixture baseline(0xD9);
        KREQUIRE_TRUE(baseline.valid());
        KREQUIRE_EQ(baseline.mount_log(), 0);
        baseline.storage().clear_history();
        KREQUIRE_EQ(baseline.stage_home(FAULT_HOME_BLOCK, 0x29), 0);
        KREQUIRE_EQ(baseline.flush(), 0);
        CheckpointEventOrder const ORDER = checkpoint_event_order(baseline.storage(), FAULT_HOME_BLOCK);
        KREQUIRE_NE(ORDER.home_write, SIZE_MAX);
        home_write_operation = baseline.storage().event(ORDER.home_write)->operation_index;
    }

    constexpr uint8_t UUID_BYTE = 0xDA;
    constexpr uint8_t HOME_VALUE = 0xAD;
    SyntheticXfsLogFixture crashed(UUID_BYTE);
    KREQUIRE_TRUE(crashed.valid());
    KREQUIRE_EQ(crashed.mount_log(), 0);
    crashed.storage().clear_history();
    KREQUIRE_EQ(crashed.stage_home(FAULT_HOME_BLOCK, HOME_VALUE), 0);
    KREQUIRE_TRUE(crashed.storage().fail_operation(ker::test::FaultBlockOperation::WRITE, home_write_operation, -EIO));
    KREQUIRE_EQ(crashed.flush(), -EIO);
    KREQUIRE_TRUE(crashed.crash_forget());
    crashed.storage().power_cut();
    ker::vfs::invalidate_bdev(crashed.storage().device());

    crashed.storage().clear_history();
    KREQUIRE_TRUE(crashed.storage().fail_operations(ker::test::FaultBlockOperation::READ, 1, 3, -EIO));
    ker::vfs::xfs::XfsMountContext failed_recovery{};
    configure_synthetic_mount(failed_recovery, crashed.storage().device(), UUID_BYTE);
    KEXPECT_EQ(ker::vfs::xfs::xfs_log_mount(&failed_recovery), -EIO);
    KEXPECT_TRUE(has_failed_event(crashed.storage(), ker::test::FaultBlockOperation::READ, 1, -EIO));
    KEXPECT_TRUE(has_failed_event(crashed.storage(), ker::test::FaultBlockOperation::READ, 3, -EIO));
    KEXPECT_EQ(failed_recovery.log, nullptr);
    KEXPECT_EQ(media_byte(crashed.storage(), true, FAULT_HOME_BLOCK), static_cast<uint8_t>(0));

    crashed.storage().clear_fault();
    ker::vfs::invalidate_bdev(crashed.storage().device());
    KEXPECT_FALSE(ker::vfs::has_cached_bdev_range(crashed.storage().device(), 0, FAULT_TOTAL_BLOCKS));
    KEXPECT_TRUE(recover_cold_log(crashed.storage(), UUID_BYTE, FAULT_HOME_BLOCK, HOME_VALUE));
}

KTEST(XFS, CorruptDurableWalIsRejectedWithoutHomeMutation) {
    uint64_t home_write_operation = 0;
    {
        SyntheticXfsLogFixture baseline(0xDB);
        KREQUIRE_TRUE(baseline.valid());
        KREQUIRE_EQ(baseline.mount_log(), 0);
        baseline.storage().clear_history();
        KREQUIRE_EQ(baseline.stage_home(FAULT_HOME_BLOCK, 0x2B), 0);
        KREQUIRE_EQ(baseline.flush(), 0);
        CheckpointEventOrder const ORDER = checkpoint_event_order(baseline.storage(), FAULT_HOME_BLOCK);
        KREQUIRE_NE(ORDER.home_write, SIZE_MAX);
        home_write_operation = baseline.storage().event(ORDER.home_write)->operation_index;
    }

    constexpr uint8_t UUID_BYTE = 0xDC;
    SyntheticXfsLogFixture crashed(UUID_BYTE);
    KREQUIRE_TRUE(crashed.valid());
    KREQUIRE_EQ(crashed.mount_log(), 0);
    crashed.storage().clear_history();
    KREQUIRE_EQ(crashed.stage_home(FAULT_HOME_BLOCK, 0xCD), 0);
    KREQUIRE_TRUE(crashed.storage().fail_operation(ker::test::FaultBlockOperation::WRITE, home_write_operation, -EIO));
    KREQUIRE_EQ(crashed.flush(), -EIO);
    KREQUIRE_TRUE(crashed.crash_forget());
    crashed.storage().power_cut();
    ker::vfs::invalidate_bdev(crashed.storage().device());

    size_t const CRC_OFFSET = (static_cast<size_t>(FAULT_LOG_START) * FAULT_BLOCK_SIZE) + offsetof(ker::vfs::xfs::XlogRecHeader, h_crc);
    uint8_t corrupt_crc_byte = static_cast<uint8_t>(crashed.storage().durable_data()[CRC_OFFSET] ^ 0x80U);
    KREQUIRE_TRUE(crashed.storage().seed_bytes(CRC_OFFSET, &corrupt_crc_byte, sizeof(corrupt_crc_byte)));

    ker::vfs::xfs::XfsMountContext corrupted{};
    configure_synthetic_mount(corrupted, crashed.storage().device(), UUID_BYTE);
    int const MOUNT_RC = ker::vfs::xfs::xfs_log_mount(&corrupted);
    KEXPECT_NE(MOUNT_RC, 0);
    KEXPECT_EQ(media_byte(crashed.storage(), true, FAULT_HOME_BLOCK), static_cast<uint8_t>(0));
    if (MOUNT_RC == 0) {
        static_cast<void>(ker::vfs::xfs::xfs_log_unmount(&corrupted, false));
    }
    ker::vfs::invalidate_bdev(crashed.storage().device());
}

KTEST(XFS, AgfStructSize) { KEXPECT_EQ(sizeof(ker::vfs::xfs::XfsAgf), static_cast<size_t>(224)); }

KTEST(XFS, BmbtRecSize) { KEXPECT_EQ(sizeof(ker::vfs::xfs::XfsBmbtRec), static_cast<size_t>(16)); }

KTEST(XFS, BmapInsertMergeCases) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_bmap_insert_merge_cases()); }

KTEST(XFS, BmapSyntheticBtreeLookup) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_bmap_synthetic_btree_lookup()); }

KTEST(XFS, BmapExtentPromotion) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_bmap_extent_promotion()); }

KTEST(XFS, AgflSkipsLiveAllocationBtreeBlocks) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_agfl_skips_live_allocation_btree_blocks()); }

KTEST(XFS, FullAgflAllocationDrainsTransactionally) {
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_full_agfl_allocation_drains_transactionally());
}

KTEST(XFS, EmptyAgflRefillRebalances) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_empty_agfl_refill_rebalances()); }

KTEST(XFS, FreeSpaceTreeChurnPreservesTopology) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_free_space_tree_churn_preserves_topology()); }

KTEST(XFS, SmallHoleWriteAllocatesOnlyNeededBlocks) {
    constexpr size_t BLOCK_SIZE = 4096;
    constexpr uint32_t BLOCK_LOG = 12;
    constexpr ker::vfs::xfs::xfs_filblks_t UNBOUNDED_HOLE = ~static_cast<ker::vfs::xfs::xfs_filblks_t>(0);

    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, 12, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG),
               static_cast<ker::vfs::xfs::xfs_extlen_t>(1));
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, 4096, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG),
               static_cast<ker::vfs::xfs::xfs_extlen_t>(1));
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, 4097, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG),
               static_cast<ker::vfs::xfs::xfs_extlen_t>(2));
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(4095, 2, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG),
               static_cast<ker::vfs::xfs::xfs_extlen_t>(2));
}

KTEST(XFS, SequentialAppendDefersSpeculativePreallocUntilStream) {
    constexpr size_t BLOCK_SIZE = 4096;
    constexpr uint32_t BLOCK_LOG = 12;
    constexpr size_t SMALL_APPEND_POS = 4096;
    constexpr size_t STREAM_APPEND_POS = size_t{16} * 1024;
    constexpr ker::vfs::xfs::xfs_filblks_t UNBOUNDED_HOLE = ~static_cast<ker::vfs::xfs::xfs_filblks_t>(0);

    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, 4096, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG, SMALL_APPEND_POS, true),
               static_cast<ker::vfs::xfs::xfs_extlen_t>(1));
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, 4096, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG, STREAM_APPEND_POS, true),
               static_cast<ker::vfs::xfs::xfs_extlen_t>(16));
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, 4096, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG, SMALL_APPEND_POS, false),
               static_cast<ker::vfs::xfs::xfs_extlen_t>(1));
    KEXPECT_EQ(
        ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, 4096, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG, STREAM_APPEND_POS, false),
        static_cast<ker::vfs::xfs::xfs_extlen_t>(1));
}

KTEST(XFS, MappedAppendSkipsReadOnlyAtCleanBlockBoundary) {
    constexpr size_t BLOCK_SIZE = 4096;

    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_mapped_append_can_zero_without_read(0, 0, BLOCK_SIZE));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_mapped_append_can_zero_without_read(BLOCK_SIZE, BLOCK_SIZE, BLOCK_SIZE));
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_mapped_append_can_zero_without_read(BLOCK_SIZE - 1, BLOCK_SIZE - 1, BLOCK_SIZE));
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_mapped_append_can_zero_without_read(BLOCK_SIZE, BLOCK_SIZE - 1, BLOCK_SIZE));
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_mapped_append_can_zero_without_read(BLOCK_SIZE, BLOCK_SIZE * 2, BLOCK_SIZE));
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_mapped_append_can_zero_without_read(BLOCK_SIZE, BLOCK_SIZE, 0));
}

KTEST(XFS, FreshBlockZeroingPreservesWriteRange) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_zero_fresh_block_preserves_write_range()); }

KTEST(XFS, CachedReadBatchIsBoundedAndBlockAligned) {
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_read_batch_max_bytes(4096), static_cast<size_t>(2 * 1024 * 1024));
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_read_batch_max_bytes(512), static_cast<size_t>(2 * 1024 * 1024));
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_read_batch_max_bytes(3 * 1024 * 1024), static_cast<size_t>(3 * 1024 * 1024));
}

KTEST(XFS, BufGetMultiSkipsDeviceRead) {
    XfsReadCounter counter{};
    ker::dev::BlockDevice dev = make_xfs_counting_bdev(&counter);
    ker::vfs::invalidate_bdev(&dev);

    ker::vfs::xfs::XfsMountContext ctx{};
    ctx.device = &dev;
    ctx.block_size = 4096;
    ctx.block_log = 12;
    ctx.ag_blocks = 1024;
    ctx.ag_blk_log = 10;

    constexpr size_t COUNT = 2;
    ker::vfs::BufHead* bh = ker::vfs::xfs::xfs_buf_get_multi(&ctx, ker::vfs::xfs::xfs_agbno_to_fsbno(0, 8, ctx.ag_blk_log), COUNT);
    KREQUIRE_NE(bh, nullptr);
    KEXPECT_EQ(counter.calls, static_cast<size_t>(0));
    KEXPECT_EQ(bh->size, COUNT * ctx.block_size);

    ker::vfs::brelse(bh);
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(XFS, HoleWriteStillCapsLargeAllocations) {
    constexpr size_t BLOCK_SIZE = 4096;
    constexpr uint32_t BLOCK_LOG = 12;
    constexpr size_t BIG_WRITE_BYTES = size_t{512} * 1024 * 1024;
    constexpr ker::vfs::xfs::xfs_extlen_t MAX_TRANSACTION_BLOCKS = 1024;
    constexpr ker::vfs::xfs::xfs_filblks_t UNBOUNDED_HOLE = ~static_cast<ker::vfs::xfs::xfs_filblks_t>(0);

    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, BIG_WRITE_BYTES, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG),
               MAX_TRANSACTION_BLOCKS);
}

KTEST(XFS, StreamWritesPreferContiguousAllocationRuns) {
    constexpr ker::vfs::xfs::xfs_extlen_t STREAM_BLOCKS = 16;
    constexpr ker::vfs::xfs::xfs_extlen_t SMALL_RUN = 8;
    constexpr ker::vfs::xfs::xfs_extlen_t LARGE_RUN = 1024;

    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_write_alloc_min_blocks(LARGE_RUN), static_cast<ker::vfs::xfs::xfs_extlen_t>(1));
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_write_alloc_min_blocks(LARGE_RUN, true), STREAM_BLOCKS);
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_write_alloc_min_blocks(LARGE_RUN, false, true), STREAM_BLOCKS);
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_write_alloc_min_blocks(SMALL_RUN, false, true), SMALL_RUN);
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, 8192, LARGE_RUN, 4096, 12, 0, true), STREAM_BLOCKS);
}

KTEST(XFS, TruncateZeroResetsStaleDataFork) {
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_truncate_zero_resets_data(12, 1));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_truncate_zero_resets_data(0, 1024));
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_truncate_zero_resets_data(0, 0));
}

KTEST(XFS, SameSizeTruncateStampsMetadata) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_same_size_truncate_stamps_metadata()); }

KTEST(XFS, ReadOnlyCloseSkipsPreallocTrim) {
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_close_should_trim_prealloc(0));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_close_should_trim_prealloc(ker::vfs::O_CREAT));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_close_should_trim_prealloc(ker::vfs::O_TRUNC));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_close_should_trim_prealloc(1));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_close_should_trim_prealloc(2));
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_close_should_trim_prealloc(ker::vfs::O_CREAT | 1, true, false));
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_close_should_trim_prealloc(ker::vfs::O_CREAT | 1, true, true));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_close_should_trim_prealloc(ker::vfs::O_CREAT | 1, false, true));
}

KTEST(XFS, CleanFreshCreateCloseSkipsCommit) {
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_close_should_commit_inode(false, ker::vfs::O_CREAT | 1, true, false));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_close_should_commit_inode(true, ker::vfs::O_CREAT | 1, true, false));
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_close_should_commit_inode(false, ker::vfs::O_CREAT | 1, true, true));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_close_should_commit_inode(false, ker::vfs::O_CREAT | 1, false, true));
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_close_should_commit_inode(false, 0, false, false));
}

KTEST(XFS, CloseTrimDetectsActualEofPrealloc) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_inode_has_eof_prealloc()); }

KTEST(XFS, DentryCacheHitsAndInvalidates) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_dentry_cache_shortform()); }

KTEST(XFS, AuthoritativeLookupRepairsStaleNegative) {
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_authoritative_lookup_repairs_stale_negative());
}

KTEST(XFS, DirectoryEntryIndexMembership) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_directory_entry_index_membership()); }

KTEST(XFS, BlockLookupFallsBackForUnindexedEntries) {
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_block_lookup_falls_back_for_unindexed_entries());
}

KTEST(XFS, LeafIndexCompleteMarker) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_leaf_index_complete_marker()); }

KTEST(XFS, NodeDirectoryGrowthLayout) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_node_directory_growth_layout()); }

KTEST(XFS, NodeDirectoryStaleCompaction) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_node_directory_stale_compaction()); }

KTEST(XFS, NodeDirectoryFreeLayout) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_node_directory_free_layout()); }

KTEST(XFS, NewDirectoryNameFilterProvesOnlySafeMisses) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_directory_name_filter()); }

KTEST(XFS, DentryCacheInvalidationKeepsUnrelatedDirectoryHot) {
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_dentry_cache_keeps_unrelated_dir_hot());
}

KTEST(XFS, DentryCacheAddKeepsSiblingHot) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_dentry_cache_add_keeps_sibling_hot()); }

KTEST(XFS, DentryCacheRemoveKeepsSiblingHot) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_dentry_cache_remove_keeps_sibling_hot()); }

KTEST(XFS, ParentPathCacheHitsAndPurges) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_parent_path_cache()); }

KTEST(XFS, PathInodeCacheGenerationInvalidates) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_path_inode_cache_generation()); }

KTEST(XFS, PathInodeCacheExactInvalidateKeepsSiblingHot) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_path_inode_cache_exact_invalidate()); }

KTEST(XFS, DirectoryLookupSeedsParentPathCache) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_directory_lookup_seeds_parent_path_cache()); }

KTEST(XFS, WalkPathSeedsAncestorParentCache) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_walk_path_seeds_ancestor_parent_cache()); }

KTEST(XFS, CachedParentMissingLookupStaysNegative) {
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_cached_parent_missing_lookup_stays_negative());
}

KTEST(XFS, NamespaceMutationLookupRepairsStaleNegative) {
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_namespace_mutation_lookup_repairs_stale_negative());
}

KTEST(XFS, ReaddirCacheBatchesSequentialScan) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_readdir_cache_batches_sequential_scan()); }

KTEST(XFS, ReadlinkPathUsesDentryType) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_readlink_path_uses_dentry_type()); }

KTEST(XFS, PathExistsUsesDentryType) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_path_exists_uses_dentry_type()); }

KTEST(XFS, StatRequireDirectoryUsesDentryType) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_stat_require_directory_uses_dentry_type()); }

KTEST(XFS, OpenRequireDirectoryUsesDentryType) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_open_require_directory_uses_dentry_type()); }

KTEST(XFS, ShortformReaddirCookiesProgressAcrossDuplicateOffsets) {
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_shortform_readdir_cookies_are_monotonic());
}

KTEST(XFS, ShortformOffsetsMatchDataLayout) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_shortform_offsets_match_data_layout()); }

KTEST(XFS, ShortformReaddirCookiesResumeAfterRemovals) {
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_shortform_readdir_resume_after_removals());
}

// ---------------------------------------------------------------------------
// xfs_mount() error path: corrupt magic (all zeros) must return != 0
// ---------------------------------------------------------------------------

KTEST(XFS, NullMagicReturnsError) {
    ker::dev::BlockDevice dev = make_xfs_null_bdev();
    ker::vfs::xfs::XfsMountContext* ctx = nullptr;
    int const RET = ker::vfs::xfs::xfs_mount(&dev, true, &ctx);
    KEXPECT_NE(RET, 0);
    // ctx must not have been allocated on error
    KEXPECT_EQ(ctx, nullptr);
}

KTEST(XFS, NullDeviceReturnsEINVAL) {
    ker::vfs::xfs::XfsMountContext* ctx = nullptr;
    int const RET = ker::vfs::xfs::xfs_mount(nullptr, true, &ctx);
    KEXPECT_EQ(RET, -EINVAL);
    KEXPECT_EQ(ctx, nullptr);
}

KTEST(XFS, NullCtxOutReturnsEINVAL) {
    ker::dev::BlockDevice dev = make_xfs_null_bdev();
    int const RET = ker::vfs::xfs::xfs_mount(&dev, true, nullptr);
    KEXPECT_EQ(RET, -EINVAL);
}

KTEST(XFS, BtreeLookupRejectsZeroDepth) {
    ker::vfs::xfs::XfsBtreeCursor<ker::vfs::xfs::XfsCntbtTraits> cur;
    ker::vfs::xfs::XfsCntbtTraits::IRec target{};

    int const RET = ker::vfs::xfs::xfs_btree_lookup(&cur, 0, 0, target, ker::vfs::xfs::XfsBtreeLookup::GE);

    KEXPECT_EQ(RET, -EINVAL);
}

KTEST(XFS, BtreeLookupRejectsOverMaxDepth) {
    ker::vfs::xfs::XfsBtreeCursor<ker::vfs::xfs::XfsCntbtTraits> cur;
    ker::vfs::xfs::XfsCntbtTraits::IRec target{};

    int const RET = ker::vfs::xfs::xfs_btree_lookup(&cur, 0, static_cast<uint8_t>(ker::vfs::xfs::XFS_BTREE_MAXLEVELS + 1), target,
                                                    ker::vfs::xfs::XfsBtreeLookup::GE);

    KEXPECT_EQ(RET, -EINVAL);
}

KTEST(XFS, BtreeCollapsesSingleChildRoot) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_btree_collapses_single_child_root()); }

KTEST(XFS, BtreeDeleteRebalances) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_btree_delete_rebalances()); }
