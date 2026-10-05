#include <array>
#include <cerrno>
#include <cstring>
#include <dev/block_device.hpp>
#include <dev/device.hpp>
#include <memory>
#include <net/wki/auth_policy.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/coredump.hpp>
#include <platform/dbg/journal.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/security/authorization.hpp>
#include <platform/security/credential_record.hpp>
#include <platform/security/credential_store.hpp>
#include <platform/security/file_policy.hpp>
#include <syscalls_impl/process/process.hpp>
#include <test/ktest.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/fs/fat32.hpp>
#include <vfs/fs/procfs.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>
#include <vfs/fs/xfs/xfs_vfs.hpp>
#include <vfs/lookup.hpp>
#include <vfs/mount.hpp>
#include <vfs/vfs.hpp>

namespace security = ker::mod::security;

namespace {
struct DevfsOpenFixture {
    unsigned opens = 0;
    unsigned closes = 0;
    int result = 0;
};

auto credential_device_open(ker::vfs::File* file) -> int {
    auto* node = ker::vfs::devfs::devfs_file_node(file);
    auto* fixture = static_cast<DevfsOpenFixture*>(node->device->private_data);
    ++fixture->opens;
    return fixture->result;
}

auto credential_device_close(ker::vfs::File* file) -> int {
    auto* node = ker::vfs::devfs::devfs_file_node(file);
    ++static_cast<DevfsOpenFixture*>(node->device->private_data)->closes;
    return 0;
}

struct FatRenameFixture {
    std::array<std::array<uint8_t, 512>, 12> clusters{};
    uint64_t fail_read = UINT64_MAX;
    unsigned writes = 0;
    unsigned fail_write = 0;
    unsigned flushes = 0;
    unsigned fail_flush = 0;
    bool persistent_write_failure = false;
};

auto fat_rename_read(ker::dev::BlockDevice* device, uint64_t block, size_t count, void* buffer) -> int {
    auto* fixture = static_cast<FatRenameFixture*>(device->private_data);
    if (block == fixture->fail_read || block >= fixture->clusters.size() || count != 1) return -EIO;
    std::memcpy(buffer, fixture->clusters[block].data(), 512);
    return 0;
}

auto fat_rename_write(ker::dev::BlockDevice* device, uint64_t block, size_t count, const void* buffer) -> int {
    auto* fixture = static_cast<FatRenameFixture*>(device->private_data);
    ++fixture->writes;
    if (block >= fixture->clusters.size() || count != 1) return -EIO;
    if (fixture->writes == fixture->fail_write || (fixture->persistent_write_failure && fixture->writes >= fixture->fail_write)) {
        // A failed request can have changed part of its block.
        std::memcpy(fixture->clusters[block].data(), buffer, 127);
        return -EIO;
    }
    std::memcpy(fixture->clusters[block].data(), buffer, 512);
    return 0;
}

auto fat_rename_flush(ker::dev::BlockDevice* device) -> int {
    auto* fixture = static_cast<FatRenameFixture*>(device->private_data);
    return ++fixture->flushes == fixture->fail_flush ? -EIO : 0;
}

struct FatOpenFixture {
    std::array<uint8_t, 512> directory{};
    unsigned writes = 0;
};

auto fat_open_read(ker::dev::BlockDevice* device, uint64_t block, size_t count, void* buffer) -> int {
    auto* fixture = static_cast<FatOpenFixture*>(device->private_data);
    if (block != 0 || count != 1) return -EIO;
    std::memcpy(buffer, fixture->directory.data(), fixture->directory.size());
    return 0;
}

auto fat_open_write(ker::dev::BlockDevice* device, uint64_t block, size_t count, const void* buffer) -> int {
    auto* fixture = static_cast<FatOpenFixture*>(device->private_data);
    ++fixture->writes;
    if (block != 0 || count != 1) return -EIO;
    std::memcpy(fixture->directory.data(), buffer, fixture->directory.size());
    return 0;
}
}  // namespace

KTEST(Credentials, LegacyRemoteExecPreservesRestrictionsByRejectingUnsupportedIdentity) {
    auto const ROOT = security::Snapshot::bootstrap_root();
    KEXPECT_EQ(security::authorize_legacy_remote_exec(ROOT), 0);
    KEXPECT_EQ(security::authorize_legacy_remote_exec(security::Snapshot{}), -ENOTSUP);
    security::Snapshot changed;
    KREQUIRE_EQ(ROOT.prepare({.change = security::Change::NO_NEW_PRIVILEGES}, changed), 0);
    KEXPECT_EQ(security::authorize_legacy_remote_exec(changed), -ENOTSUP);
    KREQUIRE_EQ(ROOT.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, changed), 0);
    KEXPECT_EQ(security::authorize_legacy_remote_exec(changed), -ENOTSUP);
}

KTEST(Credentials, LegacyRemoteExecRejectsGroupAndDumpabilityLoss) {
    auto caller = security::Snapshot::bootstrap_root();
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_DUMPABLE, .dumpable = false}, caller), 0);
    KEXPECT_EQ(security::authorize_legacy_remote_exec(caller), -ENOTSUP);
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    KEXPECT_EQ(security::authorize_legacy_remote_exec(caller), 0);
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_DUMPABLE, .dumpable = true}, caller), 0);
    KEXPECT_EQ(security::authorize_legacy_remote_exec(caller), -ENOTSUP);
    caller = security::Snapshot::bootstrap_root();
    constexpr std::array<uint32_t, security::MAX_GROUPS> GROUPS{3000, 3001, 3002, 3003, 3004, 3005, 3006, 3007};
    for (size_t count = 1; count <= GROUPS.size(); ++count) {
        auto grouped = caller;
        KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_GROUPS, .groups = std::span(GROUPS).first(count)}, grouped), 0);
        KEXPECT_EQ(security::authorize_legacy_remote_exec(grouped), -ENOTSUP);
        KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_GID, .real = 2000}, grouped), 0);
        KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_UID, .real = 1000}, grouped), 0);
        security::Snapshot restored;
        KREQUIRE_EQ(security::restore_legacy_remote_exec_identity(grouped.user(), grouped.group(), restored), 0);
        security::IpcOwnership const OBJECT{4000, 3000, 4000, 3000, 0004};
        KEXPECT_FALSE(security::may_access_ipc(grouped, OBJECT, 4));
        KEXPECT_TRUE(security::may_access_ipc(restored, OBJECT, 4));
        KEXPECT_EQ(security::authorize_legacy_remote_exec(grouped), -ENOTSUP);
    }
}

KTEST(Credentials, LegacyRemoteExecRoundTripsIdsAndRejectsMalformedIdentity) {
    for (unsigned mask = 0; mask < 64; ++mask) {
        std::array<uint32_t, 6> ids{};
        for (unsigned slot = 0; slot < ids.size(); ++slot) ids[slot] = (mask & (1U << slot)) != 0 ? 1000 + slot : 0;
        auto caller = security::Snapshot::bootstrap_root();
        KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_RESGID, .real = ids[3], .effective = ids[4], .saved = ids[5]}, caller),
                    0);
        KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_RESUID, .real = ids[0], .effective = ids[1], .saved = ids[2]}, caller),
                    0);
        security::Snapshot restored;
        KREQUIRE_EQ(security::restore_legacy_remote_exec_identity(caller.user(), caller.group(), restored), 0);
        KEXPECT_TRUE(restored == caller);
        KEXPECT_EQ(security::authorize_legacy_remote_exec(caller), 0);
        auto const BEFORE = caller;
        KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_DUMPABLE, .dumpable = !caller.dumpable()}, caller), 0);
        KEXPECT_EQ(security::authorize_legacy_remote_exec(caller), -ENOTSUP);
        for (unsigned slot = 0; slot < ids.size(); ++slot) {
            auto invalid = ids;
            invalid[slot] = security::UNCHANGED_ID;
            KEXPECT_EQ(security::restore_legacy_remote_exec_identity({invalid[0], invalid[1], invalid[2]},
                                                                     {invalid[3], invalid[4], invalid[5]}, restored),
                       -EINVAL);
            KEXPECT_TRUE(restored == BEFORE);
        }
    }
}

KTEST(Credentials, MountAdministrationRequiresEffectiveRootAndAuthorityCeiling) {
    auto root = security::Snapshot::bootstrap_root();
    KEXPECT_EQ(security::authorize_mount_administration(root), 0);
    KEXPECT_EQ(security::authorize_mount_administration(security::Snapshot{}), -EPERM);
    auto caller = root;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_EUID, .effective = 1000}, caller), 0);
    KEXPECT_EQ(caller.user().saved, 0U);
    KEXPECT_EQ(security::authorize_mount_administration(caller), -EPERM);
    uint64_t const MOUNT = uint64_t{1} << static_cast<unsigned>(security::Authority::MOUNT);
    caller = root;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = security::ALL_AUTHORITY & ~MOUNT}, caller),
                0);
    KEXPECT_EQ(caller.user().effective, 0U);
    KEXPECT_EQ(security::authorize_mount_administration(caller), -EPERM);
    caller = root;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = MOUNT}, caller), 0);
    KEXPECT_EQ(security::authorize_mount_administration(caller), 0);
}

KTEST(Credentials, SyntheticMkdirDenialsPreserveNamespaceAndDeviceState) {
    namespace vfs = ker::vfs;
    namespace devfs = vfs::devfs;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    constexpr const char* DEVICE = "pts/ktest_mkdir_device";
    constexpr const char* LINK = "pts/ktest_mkdir_link";
    DevfsOpenFixture fixture{};
    ker::dev::CharDeviceOps operations{};
    operations.open = credential_device_open;
    operations.close = credential_device_close;
    ker::dev::Device device{};
    device.char_ops = &operations;
    device.private_data = &fixture;
    auto* node = devfs::devfs_add_device_node(DEVICE, &device);
    KREQUIRE_TRUE(node != nullptr);
    KREQUIRE_TRUE(devfs::devfs_create_symlink(LINK, "/dev/ktest_mkdir_absent") != nullptr);
    vfs::Stat before{}, after{};
    KREQUIRE_EQ(devfs::devfs_stat_node(node, 0, &before), 0);
    auto const REFS = node->pts_refs.load();
    for (auto const& caller : {ROOT, user}) {
        ker::mod::sched::task::Task task{};
        task.pid = UINT64_MAX;
        KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(task, caller), 0);
        vfs::LookupHandle epoch_before{}, epoch_after{};
        KREQUIRE_EQ(vfs::vfs_acquire_lookup(&task, vfs::AT_FDCWD, "/dev", vfs::LookupIntent::STAT, vfs::LookupFollowPolicy::NOFOLLOW_FINAL,
                                            false, false, &epoch_before),
                    0);
        for (const char* path : {"/dev", "/dev/pts", "/dev/pts/ktest_mkdir_device", "/dev/pts/ktest_mkdir_link",
                                 "/dev/pts/ktest_mkdir_link/", "/proc", "/proc/version", "/proc/self"}) {
            KEXPECT_EQ(vfs::vfs_mkdirat(&task, vfs::AT_FDCWD, path, 0777), -EEXIST);
        }
        for (const char* path : {"/dev/ktest_mkdir_absent", "/proc/ktest_mkdir_absent"}) {
            KEXPECT_EQ(vfs::vfs_mkdirat(&task, vfs::AT_FDCWD, path, 0777), -ENOSYS);
            vfs::Stat missing{};
            KEXPECT_EQ(vfs::vfs_statat(&task, vfs::AT_FDCWD, path, 0, &missing), -ENOENT);
        }
        KEXPECT_EQ(vfs::vfs_mkdirat(&task, vfs::AT_FDCWD, "/dev/ktest_mkdir_absent/child", 0777), -ENOENT);
        KEXPECT_EQ(vfs::vfs_mkdirat(&task, vfs::AT_FDCWD, "/dev/pts/ktest_mkdir_device/child", 0777), -ENOTDIR);
        KEXPECT_EQ(vfs::vfs_mkdirat(&task, vfs::AT_FDCWD, "/proc/version/child", 0777), -ENOTDIR);
        KREQUIRE_EQ(vfs::vfs_acquire_lookup(&task, vfs::AT_FDCWD, "/dev", vfs::LookupIntent::STAT, vfs::LookupFollowPolicy::NOFOLLOW_FINAL,
                                            false, false, &epoch_after),
                    0);
        KEXPECT_EQ(epoch_before.namespace_generation(), epoch_after.namespace_generation());
        KEXPECT_EQ(epoch_before.mount_generation(), epoch_after.mount_generation());
        KEXPECT_TRUE(task.credentials() == caller);
        KEXPECT_TRUE(task.fd_table.empty());
    }
    KEXPECT_EQ(devfs::devfs_stat_node(node, 0, &after), 0);
    KEXPECT_EQ(before.st_mode, after.st_mode);
    KEXPECT_EQ(before.st_uid, after.st_uid);
    KEXPECT_EQ(before.st_gid, after.st_gid);
    KEXPECT_EQ(before.st_ctim.tv_sec, after.st_ctim.tv_sec);
    KEXPECT_EQ(before.st_ctim.tv_nsec, after.st_ctim.tv_nsec);
    KEXPECT_EQ(node->pts_refs.load(), REFS);
    KEXPECT_EQ(fixture.opens, 0);
    KEXPECT_EQ(fixture.closes, 0);
    KEXPECT_TRUE(devfs::devfs_remove_node(LINK));
    KEXPECT_TRUE(devfs::devfs_remove_node(DEVICE));
}

KTEST(Credentials, DevfsDenialPrecedesDeviceCallback) {
    namespace devfs = ker::vfs::devfs;
    auto const root = security::Snapshot::bootstrap_root();
    auto caller = root;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    DevfsOpenFixture fixture{};
    ker::dev::CharDeviceOps operations{};
    operations.open = credential_device_open;
    operations.close = credential_device_close;
    ker::dev::Device device{};
    device.char_ops = &operations;
    device.private_data = &fixture;
    constexpr const char* PATH = "pts/ktest_credential_open";
    auto* node = devfs::devfs_add_device_node(PATH, &device);
    KREQUIRE_TRUE(node != nullptr);
    KREQUIRE_EQ(devfs::devfs_chmod_node(node, root, 0600, 0), 0);
    auto const REFS = node->pts_refs.load();
    auto denied = [&](const security::Snapshot& credentials, int flags, int expected) {
        int result = 123;
        auto* file = devfs::devfs_open_path(credentials, PATH, flags, 0, &result);
        KEXPECT_TRUE(file == nullptr);
        KEXPECT_EQ(result, expected);
        if (file != nullptr) {
            devfs::get_devfs_fops()->vfs_close(file);
            delete file;
        }
        KEXPECT_EQ(node->pts_refs.load(), REFS);
        KEXPECT_EQ(fixture.opens, 0);
        KEXPECT_EQ(fixture.closes, 0);
    };
    for (int flags : {0, 1, 2, ker::vfs::O_TRUNC}) denied(caller, flags, -EACCES);
    denied(root, ker::vfs::O_DIRECTORY, -ENOTDIR);
    denied(root, ker::vfs::O_CREAT | ker::vfs::O_EXCL | 2, -EEXIST);
    std::array<char, 16> output{};
    output.fill('!');
    KEXPECT_EQ(devfs::devfs_readlink_path(PATH, output.data(), output.size()), static_cast<ssize_t>(-EINVAL));
    KEXPECT_EQ(fixture.opens, 0);
    KEXPECT_EQ(node->pts_refs.load(), REFS);
    KEXPECT_EQ(output.front(), '!');
    int result = 123;
    fixture.result = -EBUSY;
    auto* file = devfs::devfs_open_path(root, PATH, 2, 0, &result);
    KEXPECT_TRUE(file == nullptr);
    KEXPECT_EQ(result, -EBUSY);
    KEXPECT_EQ(fixture.opens, 1);
    KEXPECT_EQ(fixture.closes, 0);
    KEXPECT_EQ(node->pts_refs.load(), REFS);
    fixture.result = 0;
    KEXPECT_EQ(devfs::devfs_chmod_node(node, root, 0644, 0), 0);
    file = devfs::devfs_open_path(caller, PATH, 0, 0, &result);
    KEXPECT_TRUE(file != nullptr);
    KEXPECT_EQ(result, 0);
    KEXPECT_EQ(fixture.opens, 2);
    KEXPECT_EQ(node->pts_refs.load(), REFS + 1);
    if (file != nullptr) {
        KEXPECT_EQ(devfs::get_devfs_fops()->vfs_close(file), 0);
        delete file;
    }
    KEXPECT_EQ(fixture.closes, 1);
    KEXPECT_EQ(node->pts_refs.load(), REFS);
    KEXPECT_TRUE(devfs::devfs_remove_node(PATH));
}

KTEST(Credentials, FatOpenDenialPrecedesDeviceWrites) {
    namespace fat = ker::vfs::fat32;
    auto const root = security::Snapshot::bootstrap_root();
    auto caller = root;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    FatOpenFixture fixture{};
    fat::FAT32DirectoryEntry entry{};
    std::memcpy(entry.name, "FILE    TXT", sizeof(entry.name));
    entry.attributes = 0x20;
    entry.file_size = 4;
    std::memcpy(fixture.directory.data(), &entry, sizeof(entry));
    auto const ORIGINAL = fixture.directory;
    ker::dev::BlockDevice device{};
    device.block_size = 512;
    device.total_blocks = 1;
    device.read_blocks = fat_open_read;
    device.write_blocks = fat_open_write;
    device.private_data = &fixture;
    fat::FAT32MountContext context{};
    context.device = &device;
    context.root_cluster = 2;
    context.bytes_per_sector = 512;
    context.sectors_per_cluster = 1;
    context.total_sectors = 1;
    auto check_denial = [&](const char* path, int flags, int expected) {
        int result = 123;
        auto* file = fat::fat32_open_path(caller, path, flags, 0600, &context, &result);
        KEXPECT_TRUE(file == nullptr);
        KEXPECT_EQ(result, expected);
        if (file != nullptr) {
            fat::fat32_close(file);
            delete file;
        }
        KEXPECT_EQ(fixture.writes, 0);
        KEXPECT_TRUE(fixture.directory == ORIGINAL);
    };
    for (int flags : {1, 2, ker::vfs::O_TRUNC, ker::vfs::O_TRUNC | 1, ker::vfs::O_CREAT | ker::vfs::O_TRUNC | 2}) {
        check_denial("file.txt", flags, -EACCES);
    }
    check_denial("new.txt", ker::vfs::O_CREAT | 2, -EACCES);
    check_denial("file.txt", ker::vfs::O_CREAT | ker::vfs::O_EXCL | 2, -EEXIST);
    check_denial("file.txt", ker::vfs::O_DIRECTORY, -ENOTDIR);
    check_denial("/", ker::vfs::O_TRUNC | 1, -EISDIR);
    check_denial("file.txt", 3, -EINVAL);
    context.read_only = true;
    check_denial("new.txt", ker::vfs::O_CREAT | 2, -EROFS);
    context.read_only = false;
    int result = 123;
    auto* readable = fat::fat32_open_path(caller, "file.txt", 0, 0, &context, &result);
    KEXPECT_TRUE(readable != nullptr);
    KEXPECT_EQ(result, 0);
    if (readable != nullptr) {
        KEXPECT_EQ(fat::fat32_close(readable), 0);
        delete readable;
    }
    constexpr const char* CREATED_NAME = "credential-created-long.txt";
    auto* created = fat::fat32_open_path(root, CREATED_NAME, ker::vfs::O_CREAT | ker::vfs::O_EXCL | 2, 0600, &context, &result);
    KEXPECT_TRUE(created != nullptr);
    KEXPECT_EQ(result, 0);
    KEXPECT_EQ(fixture.writes, 1);
    if (created != nullptr) {
        ker::vfs::Stat stat{};
        KEXPECT_EQ(fat::fat32_fstat(created, &stat), 0);
        KEXPECT_EQ(stat.st_uid, 0);
        KEXPECT_EQ(stat.st_gid, 0);
        KEXPECT_EQ(stat.st_mode, 0100644);
        KEXPECT_TRUE(created->open_create_result_known && created->created_by_open);
        KEXPECT_EQ(fat::fat32_close(created), 0);
        delete created;
    }
    auto* reopened = fat::fat32_open_path(caller, CREATED_NAME, 0, 0, &context, &result);
    KEXPECT_TRUE(reopened != nullptr);
    KEXPECT_EQ(result, 0);
    KEXPECT_EQ(fixture.writes, 1);
    if (reopened != nullptr) {
        KEXPECT_EQ(fat::fat32_close(reopened), 0);
        delete reopened;
    }
}

KTEST(Credentials, DevfsRootPublicationPreservesConcurrentGrowthWinner) {
    KEXPECT_TRUE(ker::vfs::devfs::selftest_root_child_publication());
}

KTEST(Credentials, DevfsLocalReferencesSurviveUnlinkAndRejectReplacementMetadata) {
    namespace devfs = ker::vfs::devfs;
    constexpr const char* PATH = "ktest_local_lifetime";
    constexpr const char* CHILD = "ktest_local_lifetime/child";
    struct Cleanup {
        ker::vfs::File* file{};
        ~Cleanup() {
            if (file != nullptr) {
                static_cast<void>(ker::vfs::vfs_close_file(file));
            }
            static_cast<void>(devfs::devfs_remove_node(CHILD));
            static_cast<void>(devfs::devfs_remove_node(PATH));
        }
    } cleanup;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto caller = ROOT;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    auto* node = devfs::devfs_create_directory(PATH);
    KREQUIRE_TRUE(node != nullptr);
    auto retained = devfs::devfs_acquire_path(PATH);
    KREQUIRE_TRUE(retained.get() == node);
    cleanup.file = devfs::devfs_open_path(ROOT, PATH, 0, 0);
    KREQUIRE_TRUE(cleanup.file != nullptr);
    KEXPECT_EQ(node->local_refs.load(), 3);
    KEXPECT_EQ(devfs::devfs_chmod_lookup(PATH, retained, caller, 0700, 0, true), -EPERM);
    KEXPECT_EQ(node->mode, 0755);
    KREQUIRE_TRUE(devfs::devfs_add_device_node(CHILD, nullptr) != nullptr);
    KEXPECT_TRUE(!devfs::devfs_remove_node(PATH));
    KEXPECT_EQ(node->local_refs.load(), 3);
    KEXPECT_TRUE(devfs::devfs_remove_node(CHILD));
    KEXPECT_TRUE(devfs::devfs_remove_node(PATH));
    KEXPECT_EQ(node->local_refs.load(), 2);
    KEXPECT_TRUE(node->parent == nullptr);
    auto* replacement = devfs::devfs_create_directory(PATH);
    KREQUIRE_TRUE(replacement != nullptr && replacement != node);
    KEXPECT_EQ(devfs::devfs_chmod_lookup(PATH, retained, ROOT, 0700, 0, true), -EAGAIN);
    KEXPECT_EQ(node->mode, 0755);
    KEXPECT_EQ(replacement->mode, 0755);
    ker::vfs::Stat stat{};
    KEXPECT_EQ(ker::vfs::vfs_fstat_file(cleanup.file, &stat), 0);
    KEXPECT_EQ(stat.st_ino, reinterpret_cast<ker::vfs::ino_t>(node));
    KEXPECT_EQ(stat.st_mode, ker::vfs::S_IFDIR | 0755);
    ker::vfs::DirEntry entry{};
    KEXPECT_EQ(cleanup.file->fops->vfs_readdir(cleanup.file, &entry, 0), 0);
    KEXPECT_EQ(entry.d_ino, reinterpret_cast<uintptr_t>(node));
    KEXPECT_EQ(cleanup.file->fops->vfs_readdir(cleanup.file, &entry, 2), -ENOENT);
    KEXPECT_EQ(ker::vfs::vfs_close_file(cleanup.file), 0);
    cleanup.file = nullptr;
    KEXPECT_EQ(node->local_refs.load(), 1);
    devfs::DevFSNodeRef moved(std::move(retained));
    KEXPECT_TRUE(!retained);
    retained = std::move(moved);
    KEXPECT_TRUE(!moved);
    KEXPECT_TRUE(retained.get() == node);
    retained.reset();
    retained.reset();
}

KTEST(Credentials, DevfsLocalFailedOpenAllowsNamespaceReentryAndReleasesPin) {
    namespace devfs = ker::vfs::devfs;
    constexpr const char* PATH = "ktest_local_failed_open";
    constexpr const char* NESTED = "ktest_local_callback";
    ker::dev::CharDeviceOps operations{};
    operations.open = [](ker::vfs::File*) -> int {
        auto* node = devfs::devfs_create_directory(NESTED);
        bool const OK = node != nullptr && devfs::devfs_remove_node(NESTED);
        return OK ? -EIO : -EINVAL;
    };
    ker::dev::Device device{};
    device.char_ops = &operations;
    struct Cleanup {
        ~Cleanup() {
            static_cast<void>(devfs::devfs_remove_node(NESTED));
            static_cast<void>(devfs::devfs_remove_node(PATH));
        }
    } cleanup;
    auto* node = devfs::devfs_add_device_node(PATH, &device);
    KREQUIRE_TRUE(node != nullptr);
    auto retained = devfs::devfs_acquire_path(PATH);
    int result = 0;
    auto* file = devfs::devfs_open_path(security::Snapshot::bootstrap_root(), PATH, 0, 0, &result);
    KEXPECT_TRUE(file == nullptr);
    if (file != nullptr) {
        static_cast<void>(ker::vfs::vfs_close_file(file));
    }
    KEXPECT_EQ(result, -EIO);
    KEXPECT_EQ(node->local_refs.load(), 2);
    KEXPECT_TRUE(!devfs::devfs_acquire_path(NESTED));
    KEXPECT_TRUE(devfs::devfs_remove_node(PATH));
    KEXPECT_EQ(retained->local_refs.load(), 1);
}

KTEST(Credentials, DevfsDeviceNamespaceCannotBeRemovedWithChildren) {
    namespace devfs = ker::vfs::devfs;
    constexpr const char* PATH = "ktest_local_parent_device";
    constexpr const char* CHILD = "ktest_local_parent_device/child";
    struct Cleanup {
        ~Cleanup() {
            static_cast<void>(devfs::devfs_remove_node(CHILD));
            static_cast<void>(devfs::devfs_remove_node(PATH));
        }
    } cleanup;
    auto* node = devfs::devfs_add_device_node(PATH, nullptr);
    KREQUIRE_TRUE(node != nullptr);
    KREQUIRE_TRUE(devfs::devfs_create_directory(CHILD) != nullptr);
    KEXPECT_TRUE(!devfs::devfs_remove_node(PATH));
    KEXPECT_TRUE(devfs::devfs_acquire_path(PATH).get() == node);
    KEXPECT_TRUE(devfs::devfs_remove_node(CHILD));
    KEXPECT_TRUE(devfs::devfs_remove_node(PATH));
}

KTEST(Credentials, DevfsPtsRetainsUnlinkedNodeAcrossDeviceRetirement) {
    namespace devfs = ker::vfs::devfs;
    constexpr const char* PATH = "pts/ktest_credential_node";
    auto* device = new ker::dev::Device{};
    KREQUIRE_TRUE(device != nullptr);
    device->type = ker::dev::DeviceType::BLOCK;
    auto* node = devfs::devfs_add_device_node(PATH, device);
    KREQUIRE_TRUE(node != nullptr);
    auto retained = devfs::devfs_acquire_path(PATH);
    KEXPECT_TRUE(retained.get() == node);
    auto* file = devfs::devfs_open_path(ker::mod::security::Snapshot::bootstrap_root(), PATH, 0, 0);
    KREQUIRE_TRUE(file != nullptr);
    {
        auto aliased = devfs::devfs_acquire_path("///pts/ktest_credential_node");
        KEXPECT_TRUE(aliased.get() == node);
        KEXPECT_EQ(node->pts_refs.load(), 4);
        KEXPECT_EQ(node->local_refs.load(), 1);
    }
    KEXPECT_EQ(node->pts_refs.load(), 3);
    KEXPECT_TRUE(devfs::devfs_remove_node(PATH));
    KEXPECT_TRUE(!devfs::devfs_acquire_path(PATH));
    KEXPECT_EQ(node->pts_refs.load(), 2);
    KEXPECT_TRUE(node->parent == nullptr);
    ker::vfs::Stat stat{};
    KEXPECT_EQ(ker::vfs::vfs_fstat_file(file, &stat), 0);
    KEXPECT_EQ(stat.st_ino, reinterpret_cast<ker::vfs::ino_t>(node));
    KEXPECT_EQ(stat.st_mode, ker::vfs::S_IFBLK | 0666);
    // A replacement name must not retarget either the file or path reference.
    auto* replacement = devfs::devfs_add_device_node(PATH, nullptr);
    KREQUIRE_TRUE(replacement != nullptr);
    KEXPECT_TRUE(replacement != node);
    KEXPECT_TRUE(devfs::devfs_file_node(file) == node);
    KEXPECT_TRUE(devfs::devfs_remove_node(PATH));
    KEXPECT_EQ(ker::vfs::vfs_close_file(file), 0);
    KEXPECT_EQ(node->pts_refs.load(), 1);
    delete device;
    // The borrowed device is now gone; retained metadata remains accessible.
    KEXPECT_TRUE(retained->is_block_device);
    KEXPECT_EQ(retained->mode, 0666);
    KEXPECT_EQ(retained->uid, 0);
    devfs::DevFSNodeRef moved(std::move(retained));
    KEXPECT_TRUE(!retained);
    KEXPECT_TRUE(moved.get() == node);
    retained = std::move(moved);
    KEXPECT_TRUE(!moved);
    KEXPECT_EQ(retained->pts_refs.load(), 1);
    retained.reset();
    retained.reset();
}

KTEST(Credentials, DevfsPtsFailedOpenReleasesNodePin) {
    namespace devfs = ker::vfs::devfs;
    constexpr const char* PATH = "pts/ktest_credential_rejected_open";
    ker::dev::CharDeviceOps operations{};
    operations.open = [](ker::vfs::File*) -> int { return -EIO; };
    ker::dev::Device device{};
    device.char_ops = &operations;
    auto* node = devfs::devfs_add_device_node(PATH, &device);
    KREQUIRE_TRUE(node != nullptr);
    auto retained = devfs::devfs_acquire_path(PATH);
    KEXPECT_TRUE(devfs::devfs_open_path(ker::mod::security::Snapshot::bootstrap_root(), PATH, 0, 0) == nullptr);
    KEXPECT_EQ(node->pts_refs.load(), 2);
    KEXPECT_TRUE(devfs::devfs_remove_node(PATH));
    KEXPECT_EQ(retained->pts_refs.load(), 1);
}

KTEST(Credentials, DevfsMetadataDenialAndStaleBinding) {
    namespace devfs = ker::vfs::devfs;
    auto const root = security::Snapshot::bootstrap_root();
    auto caller = root;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_GID, .real = 2000}, caller), 0);
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    constexpr const char* PATH = "pts/ktest_credential_policy";
    KREQUIRE_TRUE(devfs::devfs_add_device_node(PATH, nullptr) != nullptr);
    auto retained = devfs::devfs_acquire_path(PATH);
    auto* node = retained.get();
    ker::vfs::Stat before{};
    KEXPECT_EQ(devfs::devfs_stat_node(node, 123, &before), 0);
    ker::vfs::Stat output{};
    output.st_uid = 12345;
    KEXPECT_EQ(devfs::devfs_chmod_node(node, caller, 0777, 123, &output), -EPERM);
    KEXPECT_EQ(devfs::devfs_chown_node(node, caller, UINT32_MAX, UINT32_MAX, 123, &output), -EPERM);
    KEXPECT_EQ(devfs::devfs_chmod_lookup(PATH, retained, caller, 0777, 123, false, &output), -EPERM);
    KEXPECT_EQ(devfs::devfs_chown_lookup(PATH, retained, caller, 1000, 2000, 123, false, &output), -EPERM);
    KEXPECT_EQ(output.st_uid, 12345);
    ker::vfs::Stat after{};
    KEXPECT_EQ(devfs::devfs_stat_node(node, 123, &after), 0);
    KEXPECT_EQ(after.st_uid, before.st_uid);
    KEXPECT_EQ(after.st_gid, before.st_gid);
    KEXPECT_EQ(after.st_mode, before.st_mode);
    KEXPECT_EQ(after.st_ctim.tv_sec, before.st_ctim.tv_sec);
    KEXPECT_EQ(after.st_ctim.tv_nsec, before.st_ctim.tv_nsec);
    KEXPECT_EQ(devfs::devfs_chown_node(node, root, 1000, 3000, 123), 0);
    KEXPECT_EQ(devfs::devfs_chmod_node(node, caller, 06755, 123, &output), 0);
    KEXPECT_EQ(output.st_mode, ker::vfs::S_IFCHR | 04755);
    KEXPECT_EQ(devfs::devfs_chown_node(node, caller, UINT32_MAX, 2000, 123, &output), 0);
    KEXPECT_EQ(output.st_mode, ker::vfs::S_IFCHR | 04755);
    KEXPECT_EQ(output.st_uid, 1000);
    KEXPECT_EQ(output.st_gid, 2000);
    KEXPECT_EQ(devfs::devfs_chmod_lookup(PATH, retained, caller, 06755, 123, false, &output), 0);
    KEXPECT_EQ(output.st_mode, ker::vfs::S_IFCHR | 06755);
    KEXPECT_EQ(devfs::devfs_chmod_lookup(PATH, retained, root, 0777, 123, true, &output), -ENOTDIR);
    KEXPECT_TRUE(devfs::devfs_remove_node(PATH));
    KREQUIRE_TRUE(devfs::devfs_add_device_node(PATH, nullptr) != nullptr);
    KEXPECT_EQ(devfs::devfs_chmod_lookup(PATH, retained, root, 0777, 123, false, &output), -EAGAIN);
    KEXPECT_EQ(devfs::devfs_chown_lookup(PATH, retained, root, 0, 0, 123, false, &output), -EAGAIN);
    KEXPECT_EQ(output.st_uid, 1000);
    KEXPECT_EQ(output.st_mode, ker::vfs::S_IFCHR | 06755);
    KEXPECT_EQ(devfs::devfs_chmod_node(node, caller, 0600, 123, &output), 0);
    KEXPECT_EQ(output.st_mode, ker::vfs::S_IFCHR | 0600);
    KEXPECT_EQ(devfs::devfs_stat_node(nullptr, 123, &output), -EBADF);
    KEXPECT_EQ(devfs::devfs_stat_node(node, 123, nullptr), -EINVAL);
    KEXPECT_TRUE(devfs::devfs_remove_node(PATH));
}

KTEST(Credentials, DevfsPeerNamespaceRejectsStaleMetadataAndRetainsOpenIdentity) {
    namespace devfs = ker::vfs::devfs;
    constexpr const char* HOST = "ktest_credential_peer";
    constexpr const char* DIRECTORY = "nodes/ktest_credential_peer";
    constexpr const char* ID = "nodes/ktest_credential_peer/id";
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto user = ROOT;
    KREQUIRE_EQ(ROOT.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    struct Cleanup {
        ker::vfs::File* directory{};
        ker::vfs::File* identity{};
        ker::vfs::File* replacement{};
        ~Cleanup() {
            for (auto* file : {directory, identity, replacement}) {
                if (file != nullptr) static_cast<void>(ker::vfs::vfs_close_file(file));
            }
            devfs::devfs_nodes_remove_peer(HOST);
        }
    } cleanup;
    devfs::devfs_nodes_add_peer(HOST, 0xa17a);
    auto retained = devfs::devfs_acquire_path(ID);
    KREQUIRE_TRUE(retained);
    cleanup.directory = devfs::devfs_open_path(ROOT, DIRECTORY, ker::vfs::O_DIRECTORY, 0);
    cleanup.identity = devfs::devfs_open_path(ROOT, ID, 0, 0);
    KREQUIRE_TRUE(cleanup.directory != nullptr && cleanup.identity != nullptr);
    ker::vfs::Stat before{}, after{};
    KEXPECT_EQ(devfs::devfs_stat_node(retained.get(), 123, &before), 0);
    KEXPECT_EQ(devfs::devfs_chmod_lookup(ID, retained, user, 0777, 123, false), -EPERM);
    KEXPECT_EQ(devfs::devfs_chown_lookup(ID, retained, user, 1000, 1000, 123, false), -EPERM);
    KEXPECT_EQ(devfs::devfs_stat_node(retained.get(), 123, &after), 0);
    KEXPECT_EQ(before.st_mode, after.st_mode);
    KEXPECT_EQ(before.st_uid, after.st_uid);
    KEXPECT_EQ(before.st_gid, after.st_gid);
    KEXPECT_EQ(before.st_ctim.tv_sec, after.st_ctim.tv_sec);
    KEXPECT_EQ(before.st_ctim.tv_nsec, after.st_ctim.tv_nsec);
    ker::vfs::DirEntry entry{};
    KEXPECT_EQ(cleanup.directory->fops->vfs_readdir(cleanup.directory, &entry, 2), 0);
    devfs::devfs_nodes_remove_peer(HOST);
    KEXPECT_TRUE(!devfs::devfs_acquire_path(ID));
    KEXPECT_EQ(cleanup.directory->fops->vfs_readdir(cleanup.directory, &entry, 2), -ENOENT);
    KEXPECT_EQ(devfs::devfs_chmod_lookup(ID, retained, ROOT, 0777, 123, false), -EAGAIN);
    devfs::devfs_nodes_add_peer(HOST, 0xb28b);
    auto replacement = devfs::devfs_acquire_path(ID);
    KREQUIRE_TRUE(replacement && replacement.get() != retained.get());
    KEXPECT_EQ(devfs::devfs_chown_lookup(ID, retained, ROOT, 1000, 1000, 123, false), -EAGAIN);
    KEXPECT_EQ(devfs::devfs_stat_node(replacement.get(), 123, &after), 0);
    KEXPECT_EQ(after.st_uid, 0);
    KEXPECT_EQ(after.st_mode, before.st_mode);
    std::array<char, 16> buffer{};
    KEXPECT_EQ(cleanup.identity->fops->vfs_read(cleanup.identity, buffer.data(), buffer.size(), 0), 7);
    KEXPECT_TRUE(std::memcmp(buffer.data(), "0xa17a\n", 7) == 0);
    cleanup.replacement = devfs::devfs_open_path(ROOT, ID, 0, 0);
    KREQUIRE_TRUE(cleanup.replacement != nullptr);
    KEXPECT_EQ(cleanup.replacement->fops->vfs_read(cleanup.replacement, buffer.data(), buffer.size(), 0), 7);
    KEXPECT_TRUE(std::memcmp(buffer.data(), "0xb28b\n", 7) == 0);
}

KTEST(Credentials, DevfsPeerOpenReleasesNamespaceBeforeSelectedDeviceCallback) {
    namespace devfs = ker::vfs::devfs;
    static constexpr const char* PATH = "nodes/ktest_credential_callback";
    ker::dev::CharDeviceOps replacement_ops{};
    replacement_ops.read = [](ker::vfs::File*, void* buffer, size_t count) -> ssize_t {
        if (count == 0) return 0;
        *static_cast<char*>(buffer) = 'B';
        return 1;
    };
    ker::dev::Device replacement{};
    replacement.type = ker::dev::DeviceType::CHAR;
    replacement.char_ops = &replacement_ops;
    ker::dev::CharDeviceOps original_ops{};
    original_ops.open = [](ker::vfs::File* file) -> int {
        auto* next = static_cast<ker::dev::Device*>(devfs::devfs_file_node(file)->device->private_data);
        // Reenter the same namespace while opening. No namespace lock may be
        // held across this callback, and the admitted file keeps its endpoint.
        return devfs::devfs_add_device_node(PATH, next) != nullptr ? 0 : -EIO;
    };
    original_ops.read = [](ker::vfs::File*, void* buffer, size_t count) -> ssize_t {
        if (count == 0) return 0;
        *static_cast<char*>(buffer) = 'A';
        return 1;
    };
    ker::dev::Device original{};
    original.type = ker::dev::DeviceType::CHAR;
    original.char_ops = &original_ops;
    original.private_data = &replacement;
    struct Cleanup {
        ker::vfs::File* original{};
        ker::vfs::File* replacement{};
        ~Cleanup() {
            if (original != nullptr) static_cast<void>(ker::vfs::vfs_close_file(original));
            if (replacement != nullptr) static_cast<void>(ker::vfs::vfs_close_file(replacement));
            static_cast<void>(devfs::devfs_remove_node(PATH));
        }
    } cleanup;
    KREQUIRE_TRUE(devfs::devfs_add_device_node(PATH, &original) != nullptr);
    auto const ROOT = security::Snapshot::bootstrap_root();
    cleanup.original = devfs::devfs_open_path(ROOT, PATH, 0, 0);
    KREQUIRE_TRUE(cleanup.original != nullptr);
    cleanup.replacement = devfs::devfs_open_path(ROOT, PATH, 0, 0);
    KREQUIRE_TRUE(cleanup.replacement != nullptr);
    char result{};
    KEXPECT_EQ(cleanup.original->fops->vfs_read(cleanup.original, &result, 1, 0), 1);
    KEXPECT_EQ(result, 'A');
    KEXPECT_EQ(cleanup.replacement->fops->vfs_read(cleanup.replacement, &result, 1, 0), 1);
    KEXPECT_EQ(result, 'B');
}

KTEST(Credentials, FatMetadataRejectsUnauthorizedAndUnrepresentableChanges) {
    namespace fat = ker::vfs::fat32;
    auto const root = security::Snapshot::bootstrap_root();
    auto caller = root;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    fat::FAT32MountContext context{};
    context.root_cluster = 2;
    context.bytes_per_sector = 512;
    context.sectors_per_cluster = 1;
    auto* file = fat::fat32_open_path(root, "", 0, 0, &context);
    KREQUIRE_TRUE(file != nullptr);
    ker::vfs::Stat before{};
    KEXPECT_EQ(fat::fat32_fstat(file, &before), 0);
    KEXPECT_EQ(fat::fat32_chmod_path("", root, 0755, &context), 0);
    KEXPECT_EQ(fat::fat32_chown_path("", root, 0, UINT32_MAX, &context), 0);
    KEXPECT_EQ(fat::fat32_fchmod(file, root, 0755), 0);
    KEXPECT_EQ(fat::fat32_fchown(file, root, UINT32_MAX, UINT32_MAX), 0);
    KEXPECT_EQ(fat::fat32_chmod_path("", root, 0700, &context), -EOPNOTSUPP);
    KEXPECT_EQ(fat::fat32_chown_path("", root, 1000, 0, &context), -EOPNOTSUPP);
    KEXPECT_EQ(fat::fat32_fchmod(file, root, 04755), -EOPNOTSUPP);
    KEXPECT_EQ(fat::fat32_fchown(file, root, 0, 2000), -EOPNOTSUPP);
    KEXPECT_EQ(fat::fat32_chmod_path("", caller, 0755, &context), -EPERM);
    KEXPECT_EQ(fat::fat32_chown_path("", caller, UINT32_MAX, UINT32_MAX, &context), -EPERM);
    KEXPECT_EQ(fat::fat32_fchmod(file, caller, 0755), -EPERM);
    KEXPECT_EQ(fat::fat32_fchown(file, caller, UINT32_MAX, UINT32_MAX), -EPERM);
    KEXPECT_EQ(fat::fat32_fchmod(nullptr, root, 0755), -EBADF);
    KEXPECT_EQ(fat::fat32_fchown(nullptr, root, 0, 0), -EBADF);
    ker::vfs::TimestampUpdate now, explicit_times, omit;
    std::array<ker::vfs::Timespec, 2> requested{{{123, 4}, {456, 7}}};
    KEXPECT_EQ(ker::vfs::TimestampUpdate::prepare(nullptr, {500, 1}, now), 0);
    KEXPECT_EQ(ker::vfs::TimestampUpdate::prepare(requested.data(), {500, 1}, explicit_times), 0);
    KEXPECT_EQ(fat::fat32_utimens_path("", caller, now, &context), -EACCES);
    KEXPECT_EQ(fat::fat32_futimens(file, caller, explicit_times), -EPERM);
    KEXPECT_EQ(fat::fat32_futimens(file, root, now), -EOPNOTSUPP);
    KEXPECT_EQ(fat::fat32_utimens_path("", caller, omit, &context), 0);
    KEXPECT_EQ(fat::fat32_futimens(file, caller, omit), 0);

    ker::vfs::Stat after{};
    KEXPECT_EQ(fat::fat32_fstat(file, &after), 0);
    KEXPECT_EQ(before.st_uid, after.st_uid);
    KEXPECT_EQ(before.st_gid, after.st_gid);
    KEXPECT_EQ(before.st_mode, after.st_mode);
    KEXPECT_EQ(before.st_ctim.tv_sec, after.st_ctim.tv_sec);
    KEXPECT_EQ(before.st_ctim.tv_nsec, after.st_ctim.tv_nsec);
    KEXPECT_EQ(fat::fat32_close(file), 0);
    delete file;
    for (unsigned attr = 0; attr < 256; ++attr) {
        uint32_t const EXPECTED = (attr & 0x10) != 0 ? 0040755 : ((attr & 1) != 0 ? 0100444 : 0100644);
        KEXPECT_EQ(fat::fat32_posix_mode(static_cast<uint8_t>(attr)), EXPECTED);
    }
}

KTEST(Credentials, TmpfsOpenDenialPrecedesCreationAndTruncation) {
    namespace tmpfs = ker::vfs::tmpfs;
    auto const root = security::Snapshot::bootstrap_root();
    auto caller = root;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_GID, .real = 2000}, caller), 0);
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    ker::vfs::vfs_mkdir("/tmp", 0755);
    constexpr const char* PATH = "/tmp/ktest_credential_open";
    ker::vfs::vfs_unlink(PATH);
    auto* file = ker::vfs::vfs_open_file(PATH, ker::vfs::O_CREAT | ker::vfs::O_EXCL | 2, 0644);
    KREQUIRE_TRUE(file != nullptr);
    std::array<char, 4> const ORIGINAL{'k', 'e', 'e', 'p'};
    KEXPECT_EQ(tmpfs::tmpfs_write(file, ORIGINAL.data(), ORIGINAL.size(), 0), ORIGINAL.size());
    auto* node = static_cast<tmpfs::TmpNode*>(file->private_data);
    tmpfs::TmpfsLookupHandle lookup;
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(),
                                            "tmp/ktest_credential_open", &lookup),
                0);
    tmpfs::TmpfsObjectMetadata before{}, after{};
    KEXPECT_EQ(tmpfs::tmpfs_lookup_metadata(lookup, false, &before), 0);
    auto const OPEN_COUNT = node->open_count.load();
    for (int flags : {ker::vfs::O_TRUNC, ker::vfs::O_TRUNC | 1, ker::vfs::O_CREAT | ker::vfs::O_TRUNC | 2}) {
        int result = 123;
        auto* denied = tmpfs::tmpfs_open_lookup(caller, lookup, flags, 0777, false, &result);
        KEXPECT_TRUE(denied == nullptr);
        KEXPECT_EQ(result, -EACCES);
        KEXPECT_EQ(node->open_count.load(), OPEN_COUNT);
        KEXPECT_EQ(tmpfs::tmpfs_lookup_metadata(lookup, false, &after), 0);
        KEXPECT_EQ(after.size, before.size);
        KEXPECT_EQ(after.ctime.tv_sec, before.ctime.tv_sec);
        KEXPECT_EQ(after.ctime.tv_nsec, before.ctime.tv_nsec);
    }
    std::array<char, 4> readback{};
    KEXPECT_EQ(tmpfs::tmpfs_read(file, readback.data(), readback.size(), 0), readback.size());
    KEXPECT_TRUE(readback == ORIGINAL);
    tmpfs::TmpfsLookupHandle missing;
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(),
                                            "tmp/ktest_credential_new", &missing),
                0);
    auto const GENERATION = missing.parent()->directory_generation;
    int result = 123;
    auto* denied = tmpfs::tmpfs_open_lookup(caller, missing, ker::vfs::O_CREAT | 2, 0000, false, &result);
    KEXPECT_TRUE(denied == nullptr);
    KEXPECT_EQ(result, -EACCES);
    KEXPECT_EQ(missing.parent()->directory_generation, GENERATION);
    KEXPECT_TRUE(tmpfs::tmpfs_lookup(missing.parent(), missing.name()) == nullptr);
    KEXPECT_EQ(ker::vfs::vfs_unlink(PATH), 0);
    denied = tmpfs::tmpfs_open_lookup(root, lookup, 0, 0, false, &result);
    KEXPECT_TRUE(denied == nullptr);
    KEXPECT_EQ(result, -EAGAIN);
    KEXPECT_EQ(ker::vfs::vfs_close_file(file), 0);
}

KTEST(Credentials, TimestampDenialAndRetainedBackendObjects) {
    namespace tmpfs = ker::vfs::tmpfs;
    namespace devfs = ker::vfs::devfs;
    using ker::vfs::TimestampUpdate;
    auto const root = security::Snapshot::bootstrap_root();
    auto caller = root;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_GID, .real = 2000}, caller), 0);
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    std::array<ker::vfs::Timespec, 2> requested{{{101, 123}, {202, 456}}};
    TimestampUpdate explicit_times, now, omit, mixed;
    KREQUIRE_EQ(TimestampUpdate::prepare(requested.data(), {303, 789}, explicit_times), 0);
    KREQUIRE_EQ(TimestampUpdate::prepare(nullptr, {404, 999}, now), 0);
    requested = {{{-1, TimestampUpdate::OMIT}, {-2, TimestampUpdate::OMIT}}};
    KREQUIRE_EQ(TimestampUpdate::prepare(requested.data(), {505, 888}, omit), 0);
    requested[0].tv_nsec = TimestampUpdate::NOW;
    KREQUIRE_EQ(TimestampUpdate::prepare(requested.data(), {606, 777}, mixed), 0);
    constexpr const char* PATH = "/tmp/ktest_credential_timestamps";
    ker::vfs::vfs_mkdir("/tmp", 0755);
    ker::vfs::vfs_unlink(PATH);
    auto* file = ker::vfs::vfs_open_file(PATH, ker::vfs::O_CREAT | ker::vfs::O_EXCL, 0644);
    KREQUIRE_TRUE(file != nullptr);
    auto* node = static_cast<tmpfs::TmpNode*>(file->private_data);
    tmpfs::TmpfsLookupHandle lookup;
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(),
                                            "tmp/ktest_credential_timestamps", &lookup),
                0);
    tmpfs::TmpfsObjectMetadata before{}, after{};
    KEXPECT_EQ(tmpfs::tmpfs_lookup_metadata(lookup, false, &before), 0);
    after.uid = 12345;
    KEXPECT_EQ(tmpfs::tmpfs_utimens_node(node, caller, explicit_times, &after), -EPERM);
    KEXPECT_EQ(tmpfs::tmpfs_utimens_node(node, caller, now, &after), -EACCES);
    KEXPECT_EQ(tmpfs::tmpfs_utimens_lookup(lookup, caller, mixed, &after), -EPERM);
    KEXPECT_EQ(after.uid, 12345);
    KEXPECT_EQ(tmpfs::tmpfs_utimens_lookup(lookup, caller, omit, &after), 0);
    KEXPECT_EQ(after.atime.tv_sec, before.atime.tv_sec);
    KEXPECT_EQ(after.mtime.tv_sec, before.mtime.tv_sec);
    KEXPECT_EQ(after.ctime.tv_nsec, before.ctime.tv_nsec);
    KEXPECT_EQ(tmpfs::tmpfs_chmod_node(node, root, 0666), 0);
    KEXPECT_EQ(tmpfs::tmpfs_utimens_node(node, caller, now, &after), 0);
    KEXPECT_EQ(after.atime.tv_sec, 404);
    KEXPECT_EQ(after.mtime.tv_nsec, 999);
    KEXPECT_EQ(tmpfs::tmpfs_utimens_node(node, caller, mixed, &after), -EPERM);
    KEXPECT_EQ(tmpfs::tmpfs_chown_node(node, root, 1000, 2000), 0);
    KEXPECT_EQ(ker::vfs::vfs_unlink(PATH), 0);
    KEXPECT_EQ(tmpfs::tmpfs_utimens_lookup(lookup, caller, explicit_times, &after), -EAGAIN);
    KEXPECT_EQ(tmpfs::tmpfs_utimens_node(node, caller, explicit_times, &after), 0);
    KEXPECT_EQ(after.atime.tv_sec, 101);
    KEXPECT_EQ(after.mtime.tv_nsec, 456);
    KEXPECT_EQ(after.ctime.tv_sec, 303);
    KEXPECT_EQ(ker::vfs::vfs_close_file(file), 0);

    constexpr const char* DEVICE = "pts/ktest_credential_timestamps";
    KREQUIRE_TRUE(devfs::devfs_add_device_node(DEVICE, nullptr) != nullptr);
    auto retained = devfs::devfs_acquire_path(DEVICE);
    KREQUIRE_TRUE(static_cast<bool>(retained));
    KEXPECT_EQ(devfs::devfs_chmod_node(retained.get(), root, 0644, 123), 0);
    ker::vfs::Stat dev_before{}, dev_after{};
    KEXPECT_EQ(devfs::devfs_stat_node(retained.get(), 123, &dev_before), 0);
    KEXPECT_EQ(devfs::devfs_update_times_node(retained.get(), caller, explicit_times), -EPERM);
    KEXPECT_EQ(devfs::devfs_update_times_node(retained.get(), caller, now), -EACCES);
    KEXPECT_EQ(devfs::devfs_utimens_lookup(DEVICE, retained, caller, mixed, false), -EPERM);
    KEXPECT_EQ(devfs::devfs_utimens_lookup(DEVICE, retained, caller, omit, false), 0);
    KEXPECT_EQ(devfs::devfs_stat_node(retained.get(), 123, &dev_after), 0);
    KEXPECT_EQ(dev_after.st_atim.tv_sec, dev_before.st_atim.tv_sec);
    KEXPECT_EQ(dev_after.st_mtim.tv_nsec, dev_before.st_mtim.tv_nsec);
    KEXPECT_EQ(dev_after.st_ctim.tv_nsec, dev_before.st_ctim.tv_nsec);
    KEXPECT_EQ(devfs::devfs_chmod_node(retained.get(), root, 0666, 123), 0);
    KEXPECT_EQ(devfs::devfs_utimens_lookup(DEVICE, retained, caller, now, false), 0);
    KEXPECT_TRUE(devfs::devfs_remove_node(DEVICE));
    KEXPECT_EQ(devfs::devfs_utimens_lookup(DEVICE, retained, root, explicit_times, false), -EAGAIN);
    KEXPECT_EQ(devfs::devfs_update_times_node(retained.get(), root, explicit_times), 0);
    KEXPECT_EQ(devfs::devfs_stat_node(retained.get(), 123, &dev_after), 0);
    KEXPECT_EQ(dev_after.st_atim.tv_nsec, 123);
    KEXPECT_EQ(dev_after.st_mtim.tv_sec, 202);
}

KTEST(Credentials, TmpfsMetadataDenialAndRetainedUnlinkedObject) {
    namespace tmpfs = ker::vfs::tmpfs;
    auto const root = security::Snapshot::bootstrap_root();
    auto caller = root;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_GID, .real = 2000}, caller), 0);
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    constexpr const char* PATH = "/tmp/ktest_credential_metadata";
    ker::vfs::vfs_mkdir("/tmp", 0755);
    ker::vfs::vfs_unlink(PATH);
    auto* file = ker::vfs::vfs_open_file(PATH, ker::vfs::O_CREAT | ker::vfs::O_EXCL | 1, 0644);
    KREQUIRE_TRUE(file != nullptr);
    auto* node = static_cast<tmpfs::TmpNode*>(file->private_data);
    {
        tmpfs::TmpfsLookupHandle lookup;
        KEXPECT_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(),
                                               "tmp/ktest_credential_metadata", &lookup),
                   0);
        tmpfs::TmpfsObjectMetadata before{};
        KEXPECT_EQ(tmpfs::tmpfs_lookup_metadata(lookup, false, &before), 0);
        tmpfs::TmpfsObjectMetadata output{};
        output.uid = 12345;
        KEXPECT_EQ(tmpfs::tmpfs_chmod_lookup(lookup, caller, 0777, &output), -EPERM);
        KEXPECT_EQ(tmpfs::tmpfs_chown_lookup(lookup, caller, 1000, 2000, &output), -EPERM);
        KEXPECT_EQ(tmpfs::tmpfs_chmod_node(node, caller, 0777, &output), -EPERM);
        KEXPECT_EQ(tmpfs::tmpfs_chown_node(node, caller, UINT32_MAX, UINT32_MAX, &output), -EPERM);
        KEXPECT_EQ(output.uid, 12345);
        tmpfs::TmpfsObjectMetadata after{};
        KEXPECT_EQ(tmpfs::tmpfs_lookup_metadata(lookup, false, &after), 0);
        KEXPECT_EQ(after.uid, before.uid);
        KEXPECT_EQ(after.gid, before.gid);
        KEXPECT_EQ(after.mode, before.mode);
        KEXPECT_EQ(after.ctime.tv_sec, before.ctime.tv_sec);
        KEXPECT_EQ(after.ctime.tv_nsec, before.ctime.tv_nsec);
        KEXPECT_EQ(tmpfs::tmpfs_chown_lookup(lookup, root, 1000, 3000, &output), 0);
        KEXPECT_EQ(tmpfs::tmpfs_chmod_lookup(lookup, caller, 06755, &output), 0);
        KEXPECT_EQ(output.mode, 04755);
        KEXPECT_EQ(tmpfs::tmpfs_chown_lookup(lookup, caller, UINT32_MAX, 2000, &output), 0);
        KEXPECT_EQ(output.mode, 0755);
        KEXPECT_EQ(output.gid, 2000);
        KEXPECT_EQ(ker::vfs::vfs_unlink(PATH), 0);
        KEXPECT_EQ(tmpfs::tmpfs_chmod_lookup(lookup, caller, 0600), -EAGAIN);
    }
    tmpfs::TmpfsObjectMetadata output{};
    KEXPECT_EQ(tmpfs::tmpfs_chmod_node(node, caller, 0600, &output), 0);
    KEXPECT_EQ(output.mode, 0600);
    KEXPECT_EQ(output.uid, 1000);
    ker::vfs::Stat stat{};
    KEXPECT_EQ(tmpfs::tmpfs_stat_node(node, 123, &stat), 0);
    KEXPECT_EQ(stat.st_dev, 123);
    KEXPECT_EQ(stat.st_uid, 1000);
    KEXPECT_EQ(stat.st_gid, 2000);
    KEXPECT_EQ(stat.st_mode, ker::vfs::S_IFREG | 0600);
    KEXPECT_EQ(stat.st_nlink, 0);
    KEXPECT_EQ(tmpfs::tmpfs_stat_node(node, 123, nullptr), -EINVAL);
    KEXPECT_EQ(tmpfs::tmpfs_stat_node(nullptr, 123, &stat), -EBADF);
    KEXPECT_EQ(stat.st_uid, 1000);
    ker::mod::sched::task::Task task{};
    int const FD = ker::vfs::vfs_alloc_fd(&task, file);
    KEXPECT_TRUE(FD >= 0);
    if (FD >= 0) {
        file->stat_cache_mutex.lock();
        KEXPECT_EQ(ker::vfs::vfs_fstat_snapshot_fast(&task, FD, &stat), -EAGAIN);
        file->stat_cache_mutex.unlock();
        KEXPECT_EQ(ker::vfs::vfs_release_fd(&task, FD), 0);
    }
    KEXPECT_EQ(ker::vfs::vfs_close_file(file), 0);
}

KTEST(Credentials, AtomicDropAndRetainedSnapshot) {
    security::CredentialStore store(security::Snapshot::bootstrap_root());
    auto const before = store.read();
    KEXPECT_EQ(store.update({.change = security::Change::SET_GID, .real = 2000}), 0);
    KEXPECT_EQ(store.update({.change = security::Change::SET_UID, .real = 1000}), 0);
    auto const after = store.read();
    KEXPECT_EQ(before.credentials.user().effective, 0);
    KEXPECT_EQ(after.credentials.user().real, 1000);
    KEXPECT_EQ(after.credentials.user().effective, 1000);
    KEXPECT_EQ(after.credentials.user().saved, 1000);
    KEXPECT_EQ(after.credentials.group().saved, 2000);
    KEXPECT_FALSE(after.credentials.allows(security::Authority::POWER));
    KEXPECT_EQ(store.update({.change = security::Change::SET_UID, .real = 0}), -EPERM);
    KEXPECT_EQ(store.read().generation, after.generation);
    KEXPECT_TRUE(store.read().credentials == after.credentials);
}

KTEST(Credentials, GroupsAndExecNoRegain) {
    auto value = security::Snapshot::bootstrap_root();
    std::array<uint32_t, security::MAX_GROUPS> groups{1, 2, 3, 4, 5, 6, 7, 8};
    KEXPECT_EQ(value.prepare({.change = security::Change::SET_GROUPS, .groups = groups}, value), 0);
    KEXPECT_TRUE(value.has_group(8));
    KEXPECT_FALSE(value.has_group(9));
    KEXPECT_EQ(value.prepare({.change = security::Change::NO_NEW_PRIVILEGES}, value), 0);
    KEXPECT_EQ(value.prepare({.change = security::Change::SET_UID, .real = 1000}, value), 0);
    KEXPECT_EQ(value.prepare_exec({.uid = 0, .set_uid = true}, value), 0);
    KEXPECT_EQ(value.user().effective, 1000);
    KEXPECT_EQ(value.user().saved, 1000);
    KEXPECT_EQ(value.prepare({.change = security::Change::SET_GROUPS}, value), -EPERM);
    KEXPECT_TRUE(value.has_group(8));
}

KTEST(Credentials, OrdinaryExecSavesEffectiveIdentity) {
    auto value = security::Snapshot::bootstrap_root();
    KEXPECT_EQ(value.prepare({.change = security::Change::SET_RESUID, .real = 1000, .effective = 1001, .saved = 1002}, value), 0);
    KEXPECT_EQ(value.prepare_exec({}, value), 0);
    KEXPECT_EQ(value.user().real, 1000);
    KEXPECT_EQ(value.user().effective, 1001);
    KEXPECT_EQ(value.user().saved, 1001);
}

KTEST(Credentials, ReplacedExecWorkerCannotMutatePublishedIdentity) {
    namespace resource = ker::mod::sched::resource;
    using ker::mod::sched::task::Task;
    Task process{};
    process.pid = 0xffffffe300ULL;
    process.type = ker::mod::sched::task::TaskType::PROCESS;
    Task worker{};
    worker.pid = process.pid + 1;
    worker.type = ker::mod::sched::task::TaskType::PROCESS;
    worker.is_thread = true;
    worker.owner_pid = process.pid;
    KREQUIRE_TRUE(resource::bind_thread_owner(worker, process));
    // Identity-only root tokens: the mutation/retirement path never dereferences
    // them. Clear them before Task teardown, including assertion early returns.
    int old_root = 0, replacement_root = 0;
    struct ClearRoots {
        Task& process;
        Task& worker;
        ~ClearRoots() { process.pagemap = worker.pagemap = nullptr; }
    } clear{process, worker};
    process.pagemap = worker.pagemap = reinterpret_cast<decltype(process.pagemap)>(&old_root);
    KREQUIRE_EQ(resource::change_credentials(worker, {.change = security::Change::SET_EUID, .effective = 1000}), 0);
    KEXPECT_FALSE(worker.process_exit_requested.load());
    security::Snapshot before, after;
    KREQUIRE_EQ(resource::prepare_exec_credentials(process, {.mode = 0755}, before, after), 0);
    KREQUIRE_EQ(resource::publish_exec_credentials(process, before, after), 0);
    KEXPECT_EQ(after.user().effective, 1000);
    KEXPECT_EQ(after.user().saved, 1000);
    process.pagemap = reinterpret_cast<decltype(process.pagemap)>(&replacement_root);
    for (unsigned i = 0; i <= static_cast<unsigned>(security::Change::SET_SERVICE_PRIVILEGES); ++i) {
        worker.process_exit_requested.store(false);
        KEXPECT_EQ(resource::change_credentials(worker, {.change = static_cast<security::Change>(i), .real = 0, .effective = 0}),
                   -ECANCELED);
        KEXPECT_TRUE(process.credentials() == after);
        KEXPECT_TRUE(worker.process_exit_requested.load());
        KEXPECT_EQ(worker.requested_process_exit_status.load(), 0);
        KEXPECT_EQ(worker.requested_process_exit_wait_status.load(), 0);
    }
    // A prior process-exit request retains its status. The canonical new image
    // remains eligible for ordinary credential mutations with the same errno.
    worker.requested_process_exit_status.store(73);
    worker.requested_process_exit_wait_status.store(73 << 8);
    KEXPECT_EQ(resource::change_credentials(worker, {.change = security::Change::SET_EUID, .effective = 0}), -ECANCELED);
    KEXPECT_EQ(worker.requested_process_exit_status.load(), 73);
    KEXPECT_EQ(worker.requested_process_exit_wait_status.load(), 73 << 8);
    KEXPECT_EQ(resource::change_credentials(process, {.change = security::Change::SET_EUID, .effective = 0}), 0);
    KEXPECT_EQ(process.credentials().user().effective, 0);
    KEXPECT_EQ(process.credentials().user().saved, 1000);
}

KTEST(Credentials, ProcessWideUidTransferMovesEveryThreadOrNone) {
    namespace resource = ker::mod::sched::resource;
    using ker::mod::sched::task::Task;
    constexpr uint32_t SOURCE = 0xffffffe1;
    constexpr uint32_t DESTINATION = 0xffffffe2;
    constexpr unsigned NPROC = static_cast<unsigned>(resource::Id::NPROC);
    Task process{};
    process.pid = 0xffffffe100ULL;
    process.type = ker::mod::sched::task::TaskType::PROCESS;
    KREQUIRE_EQ(resource::initialize_real_uid(process, SOURCE), 0);
    Task peer{};
    KREQUIRE_EQ(resource::initialize_real_uid(peer, DESTINATION), 0);
    resource::ProcessReservation process_seed(nullptr, SOURCE);
    resource::ProcessReservation peer_seed(nullptr, DESTINATION);
    KREQUIRE_TRUE(process_seed.attach(process));
    KREQUIRE_TRUE(peer_seed.attach(peer));
    Task thread{};
    thread.is_thread = true;
    thread.owner_pid = process.pid;
    thread.pid = process.pid + 1;
    KREQUIRE_TRUE(resource::bind_thread_owner(thread, process));
    resource::ProcessReservation thread_seed(nullptr, SOURCE);
    KREQUIRE_TRUE(thread_seed.attach(thread));
    auto const BEFORE = thread.credentials();
    KEXPECT_TRUE(BEFORE == process.credentials());
    KEXPECT_EQ(resource::initialize_real_uid(thread, DESTINATION), -EBUSY);
    KEXPECT_EQ(process.resources.set(NPROC, {2, UINT64_MAX}, true, 256), 0);
    KEXPECT_EQ(resource::change_credentials(thread, {.change = security::Change::SET_UID, .real = DESTINATION}), -EAGAIN);
    KEXPECT_TRUE(process.credentials() == BEFORE);
    KEXPECT_TRUE(thread.credentials() == BEFORE);
    KEXPECT_EQ(process.process_charge.uid, SOURCE);
    KEXPECT_EQ(thread.process_charge.uid, SOURCE);
    KEXPECT_EQ(peer.resources.entries[NPROC].current, 1);
    KEXPECT_EQ(process.resources.set(NPROC, {3, UINT64_MAX}, true, 256), 0);
    KEXPECT_EQ(resource::change_credentials(thread, {.change = security::Change::SET_UID, .real = DESTINATION}), 0);
    KEXPECT_TRUE(process.credentials() == thread.credentials());
    KEXPECT_EQ(thread.credentials().user().real, DESTINATION);
    KEXPECT_EQ(thread.credentials().user().effective, DESTINATION);
    KEXPECT_EQ(thread.credentials().user().saved, DESTINATION);
    KEXPECT_EQ(process.process_charge.uid, DESTINATION);
    KEXPECT_EQ(thread.process_charge.uid, DESTINATION);
    KEXPECT_EQ(peer.resources.entries[NPROC].current, 3);
    KEXPECT_EQ(BEFORE.user().real, SOURCE);
    KEXPECT_EQ(resource::change_credentials(process, {.change = security::Change::SET_UID, .real = 0}), -EPERM);
    KEXPECT_EQ(thread.credentials().user().effective, DESTINATION);
}

KTEST(Credentials, CreationMaskSharesRetainedProcessOwner) {
    using ker::mod::sched::task::Task;
    auto owner = std::make_unique<Task>();
    auto worker = std::make_unique<Task>();
    auto sibling = std::make_unique<Task>();
    owner->pid = 0x7FFF1021;
    worker->is_thread = true;
    sibling->is_thread = true;
    worker->owner_pid = sibling->owner_pid = owner->pid;
    KEXPECT_EQ(owner->creation_mask(), 022U);
    KEXPECT_EQ(worker->creation_mask(), 0777U);
    KEXPECT_EQ(worker->exchange_creation_mask(0), 0777U);
    KEXPECT_EQ(worker->creation_mask(), 0777U);
    KREQUIRE_TRUE(ker::mod::sched::resource::bind_thread_owner(*worker, *owner));
    KREQUIRE_TRUE(ker::mod::sched::resource::bind_thread_owner(*sibling, *worker));
    KEXPECT_EQ(owner->exchange_creation_mask(0077), 022U);
    KEXPECT_EQ(worker->creation_mask(), 0077U);
    KEXPECT_EQ(sibling->creation_mask(), 0077U);
    KEXPECT_EQ(worker->exchange_creation_mask(010027), 0077U);
    KEXPECT_EQ(owner->creation_mask(), 0027U);
    KEXPECT_EQ(sibling->creation_mask(), 0027U);
    // Main-thread retirement must not retire the process's creation policy.
    owner->main_thread_retired.store(true, std::memory_order_release);
    worker->resource_owner.finish_cpu();
    KEXPECT_EQ(sibling->exchange_creation_mask(0002), 0027U);
    KEXPECT_EQ(owner->creation_mask(), 0002U);
    KEXPECT_EQ(worker->creation_mask(), 0002U);
}

KTEST(Credentials, ReplacedExecWorkerCannotChangeCreationMask) {
    using ker::mod::sched::task::Task;
    using ker::syscall::process::set_creation_mask;
    auto owner = std::make_unique<Task>();
    auto worker = std::make_unique<Task>();
    owner->pid = 0x7FFF1022;
    owner->type = worker->type = ker::mod::sched::task::TaskType::PROCESS;
    worker->is_thread = true;
    worker->owner_pid = owner->pid;
    KEXPECT_EQ(set_creation_mask(*worker, 0), -ESRCH);
    KREQUIRE_TRUE(ker::mod::sched::resource::bind_thread_owner(*worker, *owner));
    int old_root = 0, replacement_root = 0;
    struct ClearRoots {
        Task& owner;
        Task& worker;
        ~ClearRoots() { owner.pagemap = worker.pagemap = nullptr; }
    } clear{*owner, *worker};
    owner->pagemap = worker->pagemap = reinterpret_cast<decltype(owner->pagemap)>(&old_root);
    KEXPECT_EQ(set_creation_mask(*worker, 010077), 022);
    KEXPECT_EQ(owner->creation_mask(), 0077U);
    owner->pagemap = reinterpret_cast<decltype(owner->pagemap)>(&replacement_root);
    KEXPECT_EQ(set_creation_mask(*worker, 0), -ECANCELED);
    KEXPECT_EQ(owner->creation_mask(), 0077U);
    KEXPECT_TRUE(worker->process_exit_requested.load());
    KEXPECT_EQ(set_creation_mask(*owner, 0027), 0077);
    KEXPECT_EQ(owner->creation_mask(), 0027U);
}

KTEST(Credentials, UnboundThreadFailsClosedAndSeedRejectsPublishedTask) {
    ker::mod::sched::task::Task thread{};
    thread.is_thread = true;
    KEXPECT_FALSE(thread.credentials().allows(security::Authority::SET_UID));
    KEXPECT_EQ(ker::mod::sched::resource::change_credentials(thread, {.change = security::Change::SET_GID, .real = 0}), -ESRCH);
    ker::mod::sched::task::Task process{};
    process.has_run = true;
    KEXPECT_EQ(ker::mod::sched::resource::initialize_real_uid(process, 1000), -EBUSY);
    KEXPECT_EQ(process.credentials().user().real, 0);
}

KTEST(Credentials, ExecPublicationRejectsStalePreparation) {
    namespace resource = ker::mod::sched::resource;
    ker::mod::sched::task::Task task{};
    security::Snapshot before;
    security::Snapshot after;
    KREQUIRE_EQ(resource::prepare_exec_credentials(task, {.gid = 2000, .set_gid = true}, before, after), 0);
    KEXPECT_EQ(resource::change_credentials(task, {.change = security::Change::SET_GID, .real = 1000}), 0);
    KEXPECT_EQ(resource::publish_exec_credentials(task, before, after), -EAGAIN);
    KEXPECT_EQ(task.credentials().group().effective, 1000);
    KREQUIRE_EQ(resource::prepare_exec_credentials(task, {.gid = 2000, .set_gid = true}, before, after), 0);
    KEXPECT_EQ(resource::publish_exec_credentials(task, before, after), 0);
    KEXPECT_EQ(task.credentials().group().real, 1000);
    KEXPECT_EQ(task.credentials().group().effective, 2000);
    KEXPECT_EQ(task.credentials().group().saved, 2000);
}

KTEST(Credentials, ExecAdmissionRejectsChangedSnapshotBeforePreparation) {
    namespace resource = ker::mod::sched::resource;
    ker::mod::sched::task::Task task{};
    auto const ADMITTED = task.credentials();
    KREQUIRE_EQ(resource::change_credentials(task, {.change = security::Change::NO_NEW_PRIVILEGES}), 0);
    auto const CURRENT = task.credentials();
    security::Snapshot before, after;
    auto const UNPREPARED = after;
    KEXPECT_EQ(resource::prepare_exec_credentials(task, {.uid = 1234, .set_uid = true, .mode = 04755}, before, after, &ADMITTED), -EAGAIN);
    KEXPECT_TRUE(task.credentials() == CURRENT && before == CURRENT && after == UNPREPARED);
    KREQUIRE_EQ(resource::prepare_exec_credentials(task, {.uid = 1234, .set_uid = true, .mode = 04755}, before, after, &CURRENT), 0);
    KEXPECT_TRUE(after.no_new_privileges());
    KEXPECT_EQ(after.user().effective, CURRENT.user().effective);
    KEXPECT_EQ(resource::publish_exec_credentials(task, before, after), 0);
}

KTEST(Credentials, SignalDenialAndSavedIdentityPolicy) {
    auto const ROOT = security::Snapshot::bootstrap_root();
    security::Snapshot caller;
    security::Snapshot target;
    KREQUIRE_EQ(ROOT.prepare({.change = security::Change::SET_RESUID, .real = 1000, .effective = 1001, .saved = 1002}, caller), 0);
    KREQUIRE_EQ(ROOT.prepare({.change = security::Change::SET_RESUID, .real = 2000, .effective = 1001, .saved = 2002}, target), 0);
    KEXPECT_FALSE(security::may_signal(caller, target, false, false));
    KEXPECT_FALSE(security::may_signal(caller, target, true, false));
    KEXPECT_FALSE(security::may_signal(caller, target, false, true));
    KEXPECT_TRUE(security::may_signal(caller, target, true, true));
    KEXPECT_TRUE(security::may_signal(ROOT, target, false, false));
    KREQUIRE_EQ(ROOT.prepare({.change = security::Change::SET_RESUID, .real = 2000, .effective = 2001, .saved = 1001}, target), 0);
    KEXPECT_TRUE(security::may_signal(caller, target, false, false));
}

KTEST(Credentials, PriorityDenialPreservesAuthorityRules) {
    auto const ROOT = security::Snapshot::bootstrap_root();
    security::Snapshot user;
    security::Snapshot target;
    KREQUIRE_EQ(ROOT.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    KREQUIRE_EQ(ROOT.prepare({.change = security::Change::SET_RESUID, .real = 2000, .effective = 2001, .saved = 1000}, target), 0);
    KEXPECT_EQ(security::authorize_priority(user, target, 0, 19), -EPERM);
    KEXPECT_EQ(security::authorize_priority(user, user, 0, -20), -EACCES);
    KEXPECT_EQ(security::authorize_priority(user, user, 0, 19), 0);
    KEXPECT_EQ(security::authorize_priority(user, user, 19, 100), 0);
    KEXPECT_EQ(security::authorize_priority(ROOT, target, 19, -100), 0);
    KEXPECT_EQ(security::clamp_priority(-100), -20);
    KEXPECT_EQ(security::clamp_priority(100), 19);
}

KTEST(Credentials, PtraceDumpabilityAndAuthorityCannotBeBypassed) {
    auto const ROOT = security::Snapshot::bootstrap_root();
    security::Snapshot user;
    KREQUIRE_EQ(ROOT.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    KEXPECT_FALSE(user.dumpable());
    KEXPECT_FALSE(security::may_trace(user, user));
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_DUMPABLE, .dumpable = true}, user), 0);
    KEXPECT_TRUE(security::may_trace(user, user));
    KEXPECT_FALSE(security::may_trace(user, ROOT));
    KEXPECT_TRUE(security::may_trace(ROOT, user));
    KREQUIRE_EQ(user.prepare_exec({.uid = 0, .set_uid = true, .mode = 04755}, user), 0);
    KEXPECT_FALSE(user.dumpable());
    KREQUIRE_EQ(user.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, user), 0);
    KEXPECT_FALSE(security::may_trace(user, ROOT));
}

KTEST(Credentials, IpcOwnershipAndPermissionClasses) {
    auto const ROOT = security::Snapshot::bootstrap_root();
    security::Snapshot user;
    KREQUIRE_EQ(ROOT.prepare({.change = security::Change::SET_GID, .real = 2000}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    security::IpcOwnership object{0, 0, 0, 0, 0600};
    KEXPECT_FALSE(security::may_access_ipc(user, object, 4));
    KEXPECT_FALSE(security::may_control_ipc(user, object));
    KEXPECT_TRUE(security::may_access_ipc(ROOT, object, 6));
    KEXPECT_TRUE(security::may_control_ipc(ROOT, object));
    object.gid = 2000;
    object.mode = 0040;
    KEXPECT_TRUE(security::may_access_ipc(user, object, 4));
    KEXPECT_FALSE(security::may_access_ipc(user, object, 6));
    object.creator_uid = 1000;
    KEXPECT_FALSE(security::may_access_ipc(user, object, 4));
    KEXPECT_TRUE(security::may_control_ipc(user, object));
    object.mode = 0600;
    KEXPECT_TRUE(security::may_access_ipc(user, object, 6));
}

KTEST(Credentials, ProcfsStatDirectoryErrorsPreserveOutputAndMetadataIdentity) {
    namespace vfs = ker::vfs;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    for (auto const& credentials : {ROOT, user}) {
        ker::mod::sched::task::Task caller{};
        caller.pid = 0x7B4A;
        KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(caller, credentials), 0);
        for (auto intent : {vfs::LookupIntent::STAT, vfs::LookupIntent::ACCESS}) {
            vfs::LookupHandle lookup{};
            KREQUIRE_EQ(vfs::vfs_acquire_lookup(&caller, vfs::AT_FDCWD, "/proc/version", intent, vfs::LookupFollowPolicy::NOFOLLOW_FINAL,
                                                false, true, &lookup, 0, false, &credentials),
                        0);
            KEXPECT_TRUE(lookup.mount()->fs_type == vfs::FSType::PROCFS);
            KEXPECT_TRUE(lookup.backend_parent() != nullptr);
        }
        for (const char* path : {"/proc/version", "/proc/uptime", "/proc/kperf"}) {
            vfs::Stat control{};
            KEXPECT_EQ(vfs::vfs_statat(&caller, vfs::AT_FDCWD, path, 0, &control), 0);
            KEXPECT_TRUE((control.st_mode & vfs::S_IFMT) == vfs::S_IFREG);
            std::array<char, 64> directory{};
            std::snprintf(directory.data(), directory.size(), "%s/", path);
            for (int flags : {0, vfs::AT_SYMLINK_NOFOLLOW}) {
                vfs::Stat output{};
                std::memset(&output, 0xA5, sizeof(output));
                std::array<unsigned char, sizeof(output)> before{};
                std::memcpy(before.data(), &output, sizeof(output));
                KEXPECT_EQ(vfs::vfs_statat(&caller, vfs::AT_FDCWD, directory.data(), flags, &output), -ENOTDIR);
                KEXPECT_TRUE(std::memcmp(&output, before.data(), sizeof(output)) == 0);
                KEXPECT_EQ(vfs::vfs_faccessat(&caller, vfs::AT_FDCWD, directory.data(), 0, flags), -ENOTDIR);
            }
        }
        for (int flags : {0, vfs::AT_SYMLINK_NOFOLLOW}) {
            vfs::Stat output{};
            std::memset(&output, 0xA5, sizeof(output));
            std::array<unsigned char, sizeof(output)> before{};
            std::memcpy(before.data(), &output, sizeof(output));
            KEXPECT_EQ(vfs::vfs_statat(&caller, vfs::AT_FDCWD, "/proc/goal19-no-such-metadata", flags, &output), -ENOENT);
            KEXPECT_TRUE(std::memcmp(&output, before.data(), sizeof(output)) == 0);
            KEXPECT_EQ(vfs::vfs_faccessat(&caller, vfs::AT_FDCWD, "/proc/goal19-no-such-metadata", 0, flags), -ENOENT);
            for (int mode : {0, 1, 2, 4, 6, -1, 8}) {
                for (const char* path : {"/proc/version", "/proc/kperf", "/proc/kperfctl"}) {
                    vfs::Stat metadata{};
                    KREQUIRE_EQ(vfs::vfs_statat(&caller, vfs::AT_FDCWD, path, flags, &metadata), 0);
                    int const EXPECTED =
                        security::authorize_file_access(credentials, {metadata.st_uid, metadata.st_gid, metadata.st_mode}, mode);
                    KEXPECT_EQ(vfs::vfs_faccessat(&caller, vfs::AT_FDCWD, path, mode, flags), EXPECTED);
                }
            }
        }
        KEXPECT_TRUE(caller.credentials() == credentials && caller.fd_table.empty());
    }
}

KTEST(Credentials, ProcfsMetadataUsesPinnedOwnerAndInvalidatesChangedCredentials) {
    namespace proc = ker::vfs::procfs;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto owner = ROOT;
    KREQUIRE_EQ(owner.prepare({.change = security::Change::SET_GID, .real = 2001}, owner), 0);
    KREQUIRE_EQ(owner.prepare({.change = security::Change::SET_UID, .real = 1000}, owner), 0);
    ker::mod::sched::task::Task target{};
    target.pid = 0x7B4B;
    KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(target, owner), 0);
    auto const REFS = target.ref_count.load();
    {
        proc::ProcFileData data{};
        data.target = &target;
        target.ref_count.fetch_add(1);
        // Deliberately mismatched numeric metadata proves fill_stat cannot
        // substitute a registry lookup for the retained owner.
        data.node.pid = target.pid + 1;
        ker::vfs::File file{};
        file.private_data = &data;
        for (auto type : {proc::ProcNodeType::STATUS_FILE, proc::ProcNodeType::STAT_FILE, proc::ProcNodeType::LIMITS_FILE,
                          proc::ProcNodeType::CMDLINE_FILE, proc::ProcNodeType::MAPS_FILE, proc::ProcNodeType::ROOT_LINK}) {
            data.node.type = type;
            ker::vfs::Stat metadata{};
            uint64_t generation = 0x55AA;
            KEXPECT_EQ(proc::procfs_fill_stat(&file, &metadata, 71, &generation), 0);
            KEXPECT_EQ(metadata.st_uid, owner.user().real);
            KEXPECT_EQ(metadata.st_gid, owner.group().real);
            KEXPECT_EQ(metadata.st_dev, decltype(metadata.st_dev){71});
            KEXPECT_EQ(generation, target.credential_observation().generation);
            KEXPECT_EQ(proc::procfs_revalidate_metadata_result(&file, generation), 0);
            KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(target, ROOT), 0);
            KEXPECT_EQ(proc::procfs_revalidate_metadata_result(&file, generation), -EAGAIN);
            KEXPECT_EQ(metadata.st_uid, owner.user().real);
            KEXPECT_EQ(metadata.st_gid, owner.group().real);
            KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(target, owner), 0);
            KEXPECT_EQ(proc::procfs_revalidate_metadata_result(&file, generation), -EAGAIN);
            KEXPECT_EQ(proc::procfs_fill_stat(&file, &metadata, 71, &generation), 0);
            target.state.store(ker::mod::sched::task::TaskState::DEAD);
            KEXPECT_EQ(proc::procfs_revalidate_metadata_result(&file, generation), 0);
            KEXPECT_EQ(proc::procfs_fill_stat(&file, &metadata, 71), 0);
            KEXPECT_EQ(metadata.st_uid, owner.user().real);
            KEXPECT_EQ(metadata.st_gid, owner.group().real);
            KEXPECT_EQ(target.ref_count.load(), REFS + 1);
            target.state.store(ker::mod::sched::task::TaskState::ACTIVE);
        }
    }
    KEXPECT_EQ(target.ref_count.load(), REFS);
    auto release = [](ker::vfs::File* file) {
        if (file != nullptr) {
            proc::get_procfs_fops()->vfs_close(file);
            delete file;
        }
    };
    for (const char* path : {"version", "self"}) {
        std::unique_ptr<ker::vfs::File, decltype(release)> file(proc::procfs_lookup_path_as(path, target.pid), release);
        KREQUIRE_TRUE(file != nullptr);
        ker::vfs::Stat metadata{};
        uint64_t generation = 0x55AA;
        KEXPECT_EQ(proc::procfs_fill_stat(file.get(), &metadata, 72, &generation), 0);
        KEXPECT_EQ(generation, uint64_t{0});
        KEXPECT_EQ(proc::procfs_revalidate_metadata_result(file.get(), generation), 0);
        KEXPECT_EQ(proc::procfs_revalidate_metadata_result(file.get(), 1), -EAGAIN);
    }
}

KTEST(Credentials, ProcfsOpenNoFollowUsesSelectedMetadataWithoutWorkerIdentity) {
    namespace vfs = ker::vfs;
    ker::mod::sched::task::Task caller{};
    caller.pid = 0x7B47;
    auto user = security::Snapshot::bootstrap_root();
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(caller, user), 0);
    auto const REFS = caller.ref_count.load();
    auto close = [&](int fd) {
        if (fd >= 0) {
            vfs::Stat metadata{};
            int stat_result = 0;
            KEXPECT_EQ(vfs::vfs_fstat_close_for_task(&caller, fd, &metadata, &stat_result), 0);
        }
    };
    for (int flags : {0, vfs::O_NOFOLLOW | vfs::O_CLOEXEC, vfs::O_NOTIFY_CACHE_CHANGE | vfs::O_NO_CACHE | vfs::O_CLOEXEC}) {
        int control = vfs::vfs_openat(&caller, vfs::AT_FDCWD, "/proc/version", flags, 0);
        KEXPECT_TRUE(control >= 0);
        if (control >= 0) {
            auto* file = vfs::vfs_get_file(&caller, control);
            KEXPECT_TRUE(file != nullptr && file->fs_type == vfs::FSType::PROCFS && file->fops == vfs::procfs::get_procfs_fops());
            KEXPECT_TRUE(caller.get_fd_cloexec(static_cast<unsigned>(control)) == ((flags & vfs::O_CLOEXEC) != 0));
        }
        close(control);
        KEXPECT_TRUE(caller.fd_table.empty());
        KEXPECT_EQ(caller.fd_table.reserved_count(), size_t{0});
    }
    for (int flags : {0, vfs::O_CREAT, vfs::O_CREAT | vfs::O_EXCL, vfs::O_NOFOLLOW}) {
        KEXPECT_EQ(vfs::vfs_openat(&caller, vfs::AT_FDCWD, "/proc/goal19-open-missing", flags, 0600), -ENOENT);
        KEXPECT_TRUE(caller.fd_table.empty());
        KEXPECT_EQ(caller.fd_table.reserved_count(), size_t{0});
    }
    for (int flags : {vfs::O_NOFOLLOW, vfs::O_NOFOLLOW | vfs::O_CLOEXEC}) {
        int result = vfs::vfs_openat(&caller, vfs::AT_FDCWD, "/proc/self", flags, 0);
        KEXPECT_EQ(result, -ELOOP);
        KEXPECT_TRUE(caller.fd_table.empty());
        close(result);
        KEXPECT_TRUE(caller.fd_table.empty());
        KEXPECT_EQ(caller.fd_table.reserved_count(), size_t{0});
        KEXPECT_TRUE(caller.credentials() == user);
        KEXPECT_EQ(caller.ref_count.load(), REFS);
    }
}

KTEST(Credentials, ProcfsOpenRetainedPrivateMetadataRevalidatesBeforePublication) {
    namespace proc = ker::vfs::procfs;
    auto const ROOT = security::Snapshot::bootstrap_root();
    security::Snapshot user, dumpable;
    KREQUIRE_EQ(ROOT.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_DUMPABLE, .dumpable = true}, dumpable), 0);
    ker::mod::sched::task::Task target{};
    target.pid = 0x7B48;
    auto const REFS = target.ref_count.load();
    auto release = [](ker::vfs::File* file) {
        if (file != nullptr) {
            proc::get_procfs_fops()->vfs_close(file);
            delete file;
        }
    };
    {
        proc::ProcFileData data{};
        target.ref_count.fetch_add(1);
        data.target = &target;
        data.node.pid = target.pid;
        char content[] = "private cached bytes";
        data.content = content;
        data.content_len = sizeof(content) - 1;
        ker::vfs::File retained{};
        retained.private_data = &data;
        for (auto type : {proc::ProcNodeType::CMDLINE_FILE, proc::ProcNodeType::MAPS_FILE, proc::ProcNodeType::IMAGES_FILE,
                          proc::ProcNodeType::FD_DIR}) {
            data.node.type = type;
            retained.is_directory = type == proc::ProcNodeType::FD_DIR;
            KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(target, user), 0);
            int error = 0;
            uint64_t generation = 0x55AA;
            std::unique_ptr<ker::vfs::File, decltype(release)> opened(
                proc::procfs_open_lookup_as(user, target.pid + 1, &retained, 0, false, &error, &generation), release);
            KEXPECT_TRUE(opened == nullptr);
            KEXPECT_EQ(error, -EACCES);
            KEXPECT_EQ(generation, uint64_t{0x55AA});
            KEXPECT_EQ(target.ref_count.load(), REFS + 1);
            opened.reset(proc::procfs_open_lookup_as(user, target.pid, &retained, ker::vfs::O_CLOEXEC, false, &error, &generation));
            KREQUIRE_TRUE(opened != nullptr);
            KEXPECT_EQ(error, 0);
            auto* cloned = static_cast<proc::ProcFileData*>(opened->private_data);
            KEXPECT_TRUE(cloned != &data && cloned->target == &target && cloned->node.type == data.node.type);
            KEXPECT_TRUE(cloned->content == nullptr && cloned->content_len == 0 && cloned->content_credential_generation == 0);
            KEXPECT_EQ(cloned->node.pid, target.pid);
            KEXPECT_EQ(opened->open_flags, ker::vfs::O_CLOEXEC);
            KEXPECT_EQ(target.ref_count.load(), REFS + 2);
            KEXPECT_EQ(proc::procfs_revalidate_open_result(opened.get(), generation), 0);
            KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(target, dumpable), 0);
            KEXPECT_EQ(proc::procfs_revalidate_open_result(opened.get(), generation), -EAGAIN);
            opened.reset();
            KEXPECT_EQ(target.ref_count.load(), REFS + 1);
            opened.reset(proc::procfs_open_lookup_as(user, target.pid + 1, &retained, 0, false, &error, &generation));
            KREQUIRE_TRUE(opened != nullptr);
            KEXPECT_EQ(proc::procfs_revalidate_open_result(opened.get(), generation), 0);
            KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(target, user), 0);
            KEXPECT_EQ(proc::procfs_revalidate_open_result(opened.get(), generation), -EAGAIN);
            opened.reset();
            generation = 0x55AA;
            opened.reset(proc::procfs_open_lookup_as(user, target.pid + 1, &retained, 0, false, &error, &generation));
            KEXPECT_TRUE(opened == nullptr);
            KEXPECT_EQ(error, -EACCES);
            KEXPECT_EQ(generation, uint64_t{0x55AA});
            opened.reset(proc::procfs_open_lookup_as(ROOT, target.pid + 1, &retained, 0, false, &error, &generation));
            KREQUIRE_TRUE(opened != nullptr);
            target.state.store(ker::mod::sched::task::TaskState::DEAD);
            KEXPECT_EQ(proc::procfs_revalidate_open_result(opened.get(), generation), -ESRCH);
            opened.reset();
            generation = 0x55AA;
            opened.reset(proc::procfs_open_lookup_as(ROOT, target.pid + 1, &retained, 0, false, &error, &generation));
            KEXPECT_TRUE(opened == nullptr);
            KEXPECT_EQ(error, -ESRCH);
            KEXPECT_EQ(generation, uint64_t{0x55AA});
            KEXPECT_EQ(target.ref_count.load(), REFS + 1);
            target.state.store(ker::mod::sched::task::TaskState::ACTIVE);
        }
        retained.is_directory = false;
        for (auto type : {proc::ProcNodeType::SELF_LINK, proc::ProcNodeType::ROOT_LINK, proc::ProcNodeType::CWD_LINK,
                          proc::ProcNodeType::EXE_LINK, proc::ProcNodeType::FD_LINK}) {
            data.node.type = type;
            int error = 0;
            uint64_t generation = 0x55AA;
            std::unique_ptr<ker::vfs::File, decltype(release)> opened(
                proc::procfs_open_lookup_as(user, target.pid + 1, &retained, ker::vfs::O_NOFOLLOW, false, &error, &generation), release);
            KEXPECT_TRUE(opened == nullptr);
            KEXPECT_EQ(error, -ELOOP);
            KEXPECT_EQ(generation, uint64_t{0x55AA});
            KEXPECT_EQ(target.ref_count.load(), REFS + 1);
        }
        KEXPECT_TRUE(data.content == content && data.content_len == sizeof(content) - 1);
    }
    KEXPECT_EQ(target.ref_count.load(), REFS);
}

KTEST(Credentials, ProcfsOpenFdRetainsExactFileWithoutChangingItsGrant) {
    namespace proc = ker::vfs::procfs;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    ker::mod::sched::task::Task target{};
    target.pid = 0x7B49;
    KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(target, user), 0);
    ker::vfs::File original{};
    original.refcount = 1;
    original.open_flags = 2 | ker::vfs::O_APPEND;
    original.pos = 31;
    KREQUIRE_TRUE(target.fd_table.insert(13, &original));
    struct Remove {
        ker::mod::sched::task::Task& target;
        ~Remove() { target.fd_table.remove(13); }
    } remove{target};
    proc::ProcFileData data{};
    data.target = &target;
    target.ref_count.fetch_add(1);
    data.node.type = proc::ProcNodeType::FD_LINK;
    data.node.pid = target.pid;
    data.node.fd = 13;
    ker::vfs::File retained{};
    retained.private_data = &data;
    int error = 0;
    uint64_t generation = 0x55AA;
    auto release = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
    std::unique_ptr<ker::vfs::File, decltype(release)> opened(
        proc::procfs_open_fd_lookup_as(user, target.pid + 1, &retained, &error, &generation), release);
    KEXPECT_TRUE(opened == nullptr);
    KEXPECT_EQ(error, -EACCES);
    KEXPECT_EQ(generation, uint64_t{0x55AA});
    KEXPECT_EQ(original.refcount.load(), 1);
    opened.reset(proc::procfs_open_fd_lookup_as(user, target.pid, &retained, &error, &generation));
    KREQUIRE_TRUE(opened.get() == &original);
    KEXPECT_EQ(error, 0);
    KEXPECT_EQ(original.refcount.load(), 2);
    KEXPECT_EQ(opened->open_flags, 2 | ker::vfs::O_APPEND);
    KEXPECT_EQ(opened->pos, off_t{31});
    KEXPECT_EQ(proc::procfs_revalidate_open_result(&retained, generation), 0);
    KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(target, ROOT), 0);
    KEXPECT_EQ(proc::procfs_revalidate_open_result(&retained, generation), -EAGAIN);
    opened.reset();
    KEXPECT_EQ(original.refcount.load(), 1);
    KEXPECT_TRUE(target.fd_table.lookup(13) == &original);
}

KTEST(Credentials, ProcfsReadlinkSelfUsesRequestedTaskIdentity) {
    ker::mod::sched::task::Task caller{};
    caller.pid = 0x7B45;
    auto user = security::Snapshot::bootstrap_root();
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(caller, user), 0);
    std::array<char, 32> expected{};
    std::snprintf(expected.data(), expected.size(), "/proc/%llu",
                  static_cast<unsigned long long>(ker::mod::sched::task::process_pid(caller)));
    ker::vfs::LookupHandle lookup{};
    KREQUIRE_EQ(ker::vfs::vfs_acquire_lookup(&caller, ker::vfs::AT_FDCWD, "/proc/self", ker::vfs::LookupIntent::READLINK,
                                             ker::vfs::LookupFollowPolicy::NOFOLLOW_FINAL, false, true, &lookup, 0, false, &user),
                0);
    KEXPECT_TRUE(lookup.mount()->fs_type == ker::vfs::FSType::PROCFS);
    KREQUIRE_TRUE(lookup.backend_parent() != nullptr);
    auto* retained = static_cast<ker::vfs::File*>(lookup.backend_parent());
    auto* retained_data = static_cast<ker::vfs::procfs::ProcFileData*>(retained->private_data);
    KREQUIRE_TRUE(retained_data != nullptr);
    KEXPECT_EQ(retained_data->node.pid, ker::mod::sched::task::process_pid(caller));
    lookup.reset();
    auto release = [](ker::vfs::File* file) {
        if (file != nullptr) {
            ker::vfs::procfs::get_procfs_fops()->vfs_close(file);
            delete file;
        }
    };
    std::unique_ptr<ker::vfs::File, decltype(release)> entry(ker::vfs::procfs::procfs_lookup_path("self"), release);
    KREQUIRE_TRUE(entry != nullptr);
    for (size_t capacity : {size_t{1}, size_t{4}, std::strlen(expected.data()), size_t{64}}) {
        std::array<char, 64> output{};
        output.fill('Z');
        size_t const LENGTH = std::min(capacity, std::strlen(expected.data()));
        KEXPECT_EQ(
            ker::vfs::procfs::procfs_readlink_as(entry.get(), output.data(), capacity, user, ker::mod::sched::task::process_pid(caller)),
            static_cast<ssize_t>(LENGTH));
        KEXPECT_TRUE(std::memcmp(output.data(), expected.data(), LENGTH) == 0);
        output.fill('Z');
        ssize_t const RESULT = ker::vfs::vfs_readlinkat(&caller, ker::vfs::AT_FDCWD, "/proc/self", output.data(), capacity);
        KEXPECT_EQ(RESULT, static_cast<ssize_t>(LENGTH));
        KEXPECT_TRUE(std::memcmp(output.data(), expected.data(), LENGTH) == 0);
        bool tail = true;
        for (size_t i = LENGTH; i < output.size(); ++i) tail &= output[i] == 'Z';
        KEXPECT_TRUE(tail);
        output.fill('Z');
        KEXPECT_EQ(ker::vfs::vfs_readlinkat(&caller, ker::vfs::AT_FDCWD, "/proc/goal19-no-such-proc-entry", output.data(), capacity),
                   static_cast<ssize_t>(-ENOENT));
        bool unchanged = true;
        for (char b : output) unchanged &= b == 'Z';
        KEXPECT_TRUE(unchanged);
    }
    KEXPECT_TRUE(caller.credentials() == user);
}

KTEST(Credentials, ProcfsReadlinkPrivateSnapshotAndGenerationPreserveOutput) {
    namespace proc = ker::vfs::procfs;
    auto const ROOT = security::Snapshot::bootstrap_root();
    security::Snapshot user, dumpable;
    KREQUIRE_EQ(ROOT.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_DUMPABLE, .dumpable = true}, dumpable), 0);
    ker::mod::sched::task::Task target{};
    target.pid = 0x7B46;
    target.state.store(ker::mod::sched::task::TaskState::ACTIVE);
    std::strcpy(target.root.data(), "/private-root");
    std::strcpy(target.exe_path.data(), "/private/tool");
    auto const INITIAL_REFS = target.ref_count.load();
    {
        proc::ProcFileData data{};
        data.target = &target;
        target.ref_count.fetch_add(1);
        data.node.pid = target.pid;
        ker::vfs::File file{};
        file.private_data = &data;
        for (auto type : {proc::ProcNodeType::ROOT_LINK, proc::ProcNodeType::EXE_LINK}) {
            data.node.type = type;
            KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(target, ROOT), 0);
            const char* expected = type == proc::ProcNodeType::ROOT_LINK ? "/private-root" : "/private/tool";
            for (size_t capacity : {size_t{1}, size_t{5}, std::strlen(expected), size_t{64}}) {
                std::array<char, 64> output{};
                output.fill('Z');
                uint64_t generation = 0x55AA;
                KEXPECT_EQ(proc::procfs_readlink_as(&file, output.data(), capacity, user, target.pid + 1, &generation),
                           static_cast<ssize_t>(-EACCES));
                KEXPECT_EQ(generation, uint64_t{0x55AA});
                bool unchanged = true;
                for (char b : output) unchanged &= b == 'Z';
                KEXPECT_TRUE(unchanged);
                size_t const LENGTH = std::min(capacity, std::strlen(expected));
                KEXPECT_EQ(proc::procfs_readlink_as(&file, output.data(), capacity, ROOT, target.pid + 1, &generation),
                           static_cast<ssize_t>(LENGTH));
                KEXPECT_TRUE(std::memcmp(output.data(), expected, LENGTH) == 0);
                bool tail = true;
                for (size_t i = LENGTH; i < output.size(); ++i) tail &= output[i] == 'Z';
                KEXPECT_TRUE(tail);
                KEXPECT_EQ(proc::procfs_revalidate_readlink_result(&file, generation), 0);
            }
            std::array<char, 64> output{};
            uint64_t generation = 0;
            KREQUIRE_TRUE(proc::procfs_readlink_as(&file, output.data(), output.size(), ROOT, target.pid + 1, &generation) > 0);
            KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(target, dumpable), 0);
            KEXPECT_EQ(proc::procfs_revalidate_readlink_result(&file, generation), -EAGAIN);
            KREQUIRE_TRUE(proc::procfs_readlink_as(&file, output.data(), output.size(), user, target.pid + 1, &generation) > 0);
            KEXPECT_EQ(proc::procfs_revalidate_readlink_result(&file, generation), 0);
            KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(target, user), 0);
            KEXPECT_EQ(proc::procfs_revalidate_readlink_result(&file, generation), -EAGAIN);
            output.fill('Z');
            KEXPECT_EQ(proc::procfs_readlink_as(&file, output.data(), output.size(), user, target.pid + 1, &generation),
                       static_cast<ssize_t>(-EACCES));
            bool unchanged = true;
            for (char b : output) unchanged &= b == 'Z';
            KEXPECT_TRUE(unchanged);
            KREQUIRE_TRUE(proc::procfs_readlink_as(&file, output.data(), output.size(), user, target.pid, &generation) > 0);
            target.state.store(ker::mod::sched::task::TaskState::DEAD);
            KEXPECT_EQ(proc::procfs_revalidate_readlink_result(&file, generation), -ESRCH);
            output.fill('Z');
            KEXPECT_EQ(proc::procfs_readlink_as(&file, output.data(), output.size(), ROOT, target.pid + 1, &generation),
                       static_cast<ssize_t>(-ESRCH));
            unchanged = true;
            for (char b : output) unchanged &= b == 'Z';
            KEXPECT_TRUE(unchanged);
            target.state.store(ker::mod::sched::task::TaskState::ACTIVE);
        }
    }
    KEXPECT_EQ(target.ref_count.load(), INITIAL_REFS);
}

KTEST(Credentials, ProcfsRootAndFdLinksAndMountsUseCanonicalFilesystemOwner) {
    KEXPECT_TRUE(ker::vfs::procfs::selftest_canonical_filesystem_root());
}

KTEST(Credentials, ProcfsGlobalAuthorityPrecedesCacheAndControl) { KEXPECT_TRUE(ker::vfs::procfs::selftest_global_authorization()); }

KTEST(Credentials, ProcfsLimitsKeepAdmittedCallerAndRetainedSubject) { KEXPECT_TRUE(ker::vfs::procfs::selftest_limits_authorization()); }

KTEST(Credentials, ProcfsLimitsSeekKeepsAuthorityCursorAndCanonicalOwner) {
    KEXPECT_TRUE(ker::vfs::procfs::selftest_limits_seek_authorization());
}

KTEST(Credentials, ProcfsDescriptorAuthorityPrecedesRetention) { KEXPECT_TRUE(ker::vfs::procfs::selftest_fd_authorization()); }

KTEST(Credentials, ProcfsPrivateAuthorityRevokesCachedViews) { KEXPECT_TRUE(ker::vfs::procfs::selftest_private_authorization()); }

KTEST(Credentials, ProcfsPublicSnapshotsDoNotDisclosePathsOrRetainDeadMemory) {
    KEXPECT_TRUE(ker::vfs::procfs::selftest_public_snapshots());
}

KTEST(Credentials, ProcfsPublicReadsKeepUnpublishedSubjectAndCachedExitBehavior) {
    KEXPECT_TRUE(ker::vfs::procfs::selftest_public_retained_subject());
}

KTEST(Credentials, ProcfsWkiIdentitySnapshotsRetainValuesAndRespectOutputBounds) {
    KEXPECT_TRUE(ker::vfs::procfs::selftest_wki_identity_snapshots());
}

KTEST(Credentials, CoredumpOutputNeverReusesAnExistingInode) { KEXPECT_TRUE(ker::mod::dbg::coredump::selftest_private_output_creation()); }

KTEST(Credentials, JournalRecordsPrecedeDeferredDevicePublication) { KEXPECT_TRUE(ker::mod::dbg::journal::selftest_module_publication()); }

KTEST(Credentials, JournalOpenKeepsDevfsAdmissionSnapshot) { KEXPECT_TRUE(ker::mod::dbg::journal::selftest_open_admission()); }

KTEST(Credentials, JournalReadAuthorityPrecedesRecordsCursorAndWaiters) {
    KEXPECT_TRUE(ker::mod::dbg::journal::selftest_read_authorization());
}

KTEST(Credentials, TmpfsRemovalDenialPreservesBindingAndCanonicalOwnership) {
    namespace tmpfs = ker::vfs::tmpfs;
    auto const root = security::Snapshot::bootstrap_root();
    auto user = root;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    constexpr const char* BASE = "/tmp/ktest_removal";
    KREQUIRE_EQ(ker::vfs::vfs_mkdir(BASE, 0755), 0);
    auto* file = ker::vfs::vfs_open_file("/tmp/ktest_removal/file", ker::vfs::O_CREAT | ker::vfs::O_EXCL | 2, 0600);
    KREQUIRE_TRUE(file != nullptr);
    std::array<char, 4> const ORIGINAL{'k', 'e', 'e', 'p'};
    KEXPECT_EQ(tmpfs::tmpfs_write(file, ORIGINAL.data(), ORIGINAL.size(), 0), ORIGINAL.size());
    KREQUIRE_EQ(ker::vfs::vfs_link("/tmp/ktest_removal/file", "/tmp/ktest_removal/alias"), 0);
    KREQUIRE_EQ(ker::vfs::vfs_mkdir("/tmp/ktest_removal/directory", 0700), 0);
    tmpfs::TmpfsLookupHandle alias, directory;
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(),
                                            "tmp/ktest_removal/alias", &alias),
                0);
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(),
                                            "tmp/ktest_removal/directory", &directory),
                0);
    auto const GENERATION = alias.parent()->directory_generation;
    tmpfs::TmpfsObjectMetadata before{}, after{};
    KREQUIRE_EQ(tmpfs::tmpfs_lookup_metadata(alias, false, &before), 0);
    bool changed = true;
    KEXPECT_EQ(tmpfs::tmpfs_unlink_lookup(user, alias, &changed), -EACCES);
    KEXPECT_TRUE(changed);
    KEXPECT_EQ(tmpfs::tmpfs_rmdir_lookup(user, directory), -EACCES);
    KREQUIRE_EQ(ker::vfs::vfs_chmod(BASE, 01777), 0);
    KEXPECT_EQ(tmpfs::tmpfs_unlink_lookup(user, alias, &changed), -EPERM);
    KEXPECT_TRUE(changed);
    KEXPECT_EQ(tmpfs::tmpfs_rmdir_lookup(user, directory), -EPERM);
    KEXPECT_EQ(alias.parent()->directory_generation, GENERATION);
    KEXPECT_EQ(tmpfs::tmpfs_lookup_metadata(alias, false, &after), 0);
    KEXPECT_EQ(after.size, before.size);
    KEXPECT_EQ(after.link_count, before.link_count);
    KEXPECT_EQ(after.ctime.tv_sec, before.ctime.tv_sec);
    KEXPECT_EQ(after.ctime.tv_nsec, before.ctime.tv_nsec);
    KREQUIRE_EQ(tmpfs::tmpfs_chown_lookup(alias, root, 1000, 2000, nullptr), 0);
    KEXPECT_EQ(tmpfs::tmpfs_unlink_lookup(user, alias, &changed), 0);
    KEXPECT_TRUE(changed);
    KEXPECT_EQ(tmpfs::tmpfs_unlink_lookup(user, alias, &changed), -EAGAIN);
    std::array<char, 4> output{};
    KEXPECT_EQ(tmpfs::tmpfs_read(file, output.data(), output.size(), 0), output.size());
    KEXPECT_TRUE(output == ORIGINAL);
    KEXPECT_EQ(ker::vfs::vfs_unlink("/tmp/ktest_removal/file"), 0);
    KEXPECT_EQ(ker::vfs::vfs_rmdir("/tmp/ktest_removal/directory"), 0);
    KEXPECT_EQ(ker::vfs::vfs_close_file(file), 0);
    alias = {};
    directory = {};
    KEXPECT_EQ(ker::vfs::vfs_rmdir(BASE), 0);
}

KTEST(Credentials, FatRemovalDenialPrecedesDirectoryAndClusterWrites) {
    namespace fat = ker::vfs::fat32;
    auto user = security::Snapshot::bootstrap_root();
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    FatOpenFixture fixture{};
    fat::FAT32DirectoryEntry entry{};
    std::memcpy(entry.name, "FILE    TXT", sizeof(entry.name));
    entry.attributes = 0x20;
    entry.file_size = 4;
    entry.cluster_low = 3;
    ker::dev::BlockDevice device{};
    device.block_size = 512;
    device.total_blocks = 1;
    device.read_blocks = fat_open_read;
    device.write_blocks = fat_open_write;
    device.private_data = &fixture;
    fat::FAT32MountContext context{};
    context.device = &device;
    context.root_cluster = 2;
    context.bytes_per_sector = 512;
    context.sectors_per_cluster = 1;
    context.total_sectors = 1;
    for (bool directory : {false, true}) {
        entry.attributes = directory ? 0x10 : 0x20;
        std::memcpy(fixture.directory.data(), &entry, sizeof(entry));
        auto const ORIGINAL = fixture.directory;
        KEXPECT_EQ(directory ? fat::fat32_rmdir_path(user, &context, "file.txt") : fat::fat32_unlink_path(user, &context, "file.txt"),
                   -EACCES);
        KEXPECT_EQ(fixture.writes, 0);
        KEXPECT_TRUE(fixture.directory == ORIGINAL);
    }
}

KTEST(Credentials, TmpfsDirectoryPathUsesRetainedIdentityAndPreservesFailureOutput) {
    namespace tmpfs = ker::vfs::tmpfs;
    KREQUIRE_EQ(ker::vfs::vfs_mkdir("/tmp/ktest_dirpath", 0755), 0);
    KREQUIRE_EQ(ker::vfs::vfs_mkdir("/tmp/ktest_dirpath/child", 0755), 0);
    auto* file = ker::vfs::vfs_open_file("/tmp/ktest_dirpath/child", ker::vfs::O_DIRECTORY, 0);
    KREQUIRE_TRUE(file != nullptr);
    auto* node = static_cast<tmpfs::TmpNode*>(file->private_data);
    std::array<char, 128> path{};
    KEXPECT_EQ(tmpfs::tmpfs_directory_path(tmpfs::get_root_node(), node, path.data(), path.size()), 0);
    KEXPECT_EQ(std::strcmp(path.data(), "/tmp/ktest_dirpath/child"), 0);
    std::array<char, 2> sentinel{'o', 'k'};
    KEXPECT_EQ(tmpfs::tmpfs_directory_path(tmpfs::get_root_node(), node, sentinel.data(), sentinel.size()), -ENAMETOOLONG);
    KEXPECT_TRUE((sentinel == std::array<char, 2>{'o', 'k'}));
    KREQUIRE_EQ(ker::vfs::vfs_rename("/tmp/ktest_dirpath", "/tmp/ktest_dirpath_moved"), 0);
    KREQUIRE_EQ(ker::vfs::vfs_mkdir("/tmp/ktest_dirpath", 0755), 0);
    KEXPECT_EQ(tmpfs::tmpfs_directory_path(tmpfs::get_root_node(), node, path.data(), path.size()), 0);
    KEXPECT_EQ(std::strcmp(path.data(), "/tmp/ktest_dirpath_moved/child"), 0);
    KREQUIRE_EQ(ker::vfs::vfs_rmdir("/tmp/ktest_dirpath_moved/child"), 0);
    KEXPECT_EQ(tmpfs::tmpfs_directory_path(tmpfs::get_root_node(), node, sentinel.data(), sentinel.size()), -ENOENT);
    KEXPECT_TRUE((sentinel == std::array<char, 2>{'o', 'k'}));
    KEXPECT_EQ(ker::vfs::vfs_close_file(file), 0);
    KEXPECT_EQ(ker::vfs::vfs_rmdir("/tmp/ktest_dirpath_moved"), 0);
    KEXPECT_EQ(ker::vfs::vfs_rmdir("/tmp/ktest_dirpath"), 0);
}

KTEST(Credentials, TmpfsMkdirDenialPreservesParentAndCreatesPreparedIdentity) {
    namespace tmpfs = ker::vfs::tmpfs;
    auto const root = security::Snapshot::bootstrap_root();
    auto caller = root;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_GID, .real = 2000}, caller), 0);
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    constexpr const char* BASE = "/tmp/ktest_mkdir_credentials";
    KREQUIRE_EQ(ker::vfs::vfs_mkdir(BASE, 0755), 0);
    tmpfs::TmpfsLookupHandle parent, missing;
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(), BASE, &parent), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(),
                                            "/tmp/ktest_mkdir_credentials/new", &missing),
                0);
    tmpfs::TmpfsObjectMetadata before{}, after{}, output{};
    output.uid = 12345;
    KREQUIRE_EQ(tmpfs::tmpfs_lookup_metadata(parent, true, &before), 0);
    auto const GENERATION = missing.parent()->directory_generation;
    KEXPECT_EQ(tmpfs::tmpfs_mkdir_lookup(caller, missing, 0777, &output), -EACCES);
    KEXPECT_EQ(output.uid, 12345);
    KEXPECT_EQ(tmpfs::tmpfs_mkdir_path(caller, tmpfs::get_root_node(), "/tmp/ktest_mkdir_credentials/new", 0777), -EACCES);
    KEXPECT_EQ(missing.parent()->directory_generation, GENERATION);
    KEXPECT_TRUE(tmpfs::tmpfs_lookup(missing.parent(), missing.name()) == nullptr);
    KREQUIRE_EQ(tmpfs::tmpfs_lookup_metadata(parent, true, &after), 0);
    KEXPECT_EQ(after.link_count, before.link_count);
    KEXPECT_EQ(after.ctime.tv_sec, before.ctime.tv_sec);
    KEXPECT_EQ(after.ctime.tv_nsec, before.ctime.tv_nsec);
    KEXPECT_EQ(after.mtime.tv_sec, before.mtime.tv_sec);
    KEXPECT_EQ(after.mtime.tv_nsec, before.mtime.tv_nsec);
    KREQUIRE_EQ(tmpfs::tmpfs_chown_lookup(parent, root, 0, 4000, nullptr), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_chmod_lookup(parent, root, 02777, nullptr), 0);
    KEXPECT_EQ(tmpfs::tmpfs_mkdir_lookup(caller, missing, 0000, &output), 0);
    KEXPECT_EQ(output.uid, 1000);
    KEXPECT_EQ(output.gid, 4000);
    KEXPECT_EQ(output.mode & 07777U, 02000);
    KEXPECT_EQ(tmpfs::tmpfs_mkdir_lookup(caller, missing, 0755, &output), -EAGAIN);
    missing = {};
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(),
                                            "/tmp/ktest_mkdir_credentials/new", &missing),
                0);
    KEXPECT_EQ(tmpfs::tmpfs_rmdir_lookup(root, missing), 0);
    missing = {};
    parent = {};
    KEXPECT_EQ(ker::vfs::vfs_rmdir(BASE), 0);
}

KTEST(Credentials, TmpfsSymlinkDenialPreservesStateAndPublishesCreatorIdentity) {
    namespace tmpfs = ker::vfs::tmpfs;
    auto const root = security::Snapshot::bootstrap_root();
    auto caller = root;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_GID, .real = 2000}, caller), 0);
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    constexpr const char* BASE = "/tmp/ktest_symlink_credentials";
    constexpr const char* LINK = "/tmp/ktest_symlink_credentials/new";
    constexpr const char* TARGET = "../missing/../literal";
    KREQUIRE_EQ(ker::vfs::vfs_mkdir(BASE, 0755), 0);
    tmpfs::TmpfsLookupHandle parent, missing;
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(), BASE, &parent), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(), LINK, &missing), 0);
    tmpfs::TmpfsObjectMetadata before{}, after{}, output{};
    output.uid = 12345;
    KREQUIRE_EQ(tmpfs::tmpfs_lookup_metadata(parent, true, &before), 0);
    auto const GENERATION = missing.parent()->directory_generation;
    KEXPECT_EQ(tmpfs::tmpfs_symlink_lookup(caller, missing, TARGET, &output), -EACCES);
    KEXPECT_EQ(output.uid, 12345);
    KEXPECT_EQ(missing.parent()->directory_generation, GENERATION);
    KEXPECT_TRUE(tmpfs::tmpfs_lookup(missing.parent(), missing.name()) == nullptr);
    KREQUIRE_EQ(tmpfs::tmpfs_lookup_metadata(parent, true, &after), 0);
    KEXPECT_EQ(after.link_count, before.link_count);
    KEXPECT_EQ(after.ctime.tv_sec, before.ctime.tv_sec);
    KEXPECT_EQ(after.ctime.tv_nsec, before.ctime.tv_nsec);
    KEXPECT_EQ(after.mtime.tv_sec, before.mtime.tv_sec);
    KEXPECT_EQ(after.mtime.tv_nsec, before.mtime.tv_nsec);
    KREQUIRE_EQ(tmpfs::tmpfs_chown_lookup(parent, root, 0, 4000, nullptr), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_chmod_lookup(parent, root, 03777, nullptr), 0);
    // KTEST runs before a current task exists. Supply the owner task to the
    // same already-resolved lookup path used by the confined-owner wrapper.
    ker::mod::sched::task::Task owner{};
    std::memcpy(owner.root.data(), "/", 2);
    std::memcpy(owner.cwd.data(), "/", 2);
    {
        ker::vfs::LookupHandle resolved{};
        KREQUIRE_EQ(ker::vfs::vfs_acquire_lookup(&owner, ker::vfs::AT_FDCWD, "/tmp/ktest_symlink_credentials/new/",
                                                 ker::vfs::LookupIntent::CREATE_SYMLINK, ker::vfs::LookupFollowPolicy::NOFOLLOW_FINAL,
                                                 false, false, &resolved, 0, true),
                    0);
        KEXPECT_TRUE(resolved.requires_directory());
        auto consume = [](const ker::vfs::LookupHandle& handle, void* target) -> int {
            return tmpfs::tmpfs_symlink_lookup(handle.authorization().credentials,
                                               *static_cast<tmpfs::TmpfsLookupHandle*>(handle.backend_parent()),
                                               static_cast<const char*>(target), nullptr, handle.requires_directory());
        };
        KEXPECT_EQ(ker::vfs::vfs_consume_lookup(resolved, true, consume, const_cast<char*>(TARGET)), -ENOENT);
    }
    KEXPECT_EQ(tmpfs::tmpfs_symlink_lookup(caller, missing, TARGET, &output, true), -ENOENT);
    KEXPECT_EQ(output.uid, 12345);
    KEXPECT_EQ(missing.parent()->directory_generation, GENERATION);
    KEXPECT_TRUE(tmpfs::tmpfs_lookup(missing.parent(), missing.name()) == nullptr);
    KEXPECT_EQ(tmpfs::tmpfs_symlink_lookup(caller, missing, TARGET, &output), 0);
    KEXPECT_EQ(output.uid, 1000);
    KEXPECT_EQ(output.gid, 4000);
    KEXPECT_EQ(output.mode & 07777U, 0777);
    KEXPECT_EQ(tmpfs::tmpfs_symlink_lookup(caller, missing, "replacement", &output), -EAGAIN);
    missing = {};
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(), LINK, &missing), 0);
    char text[64]{};
    KEXPECT_EQ(tmpfs::tmpfs_readlink_lookup(missing, text, sizeof(text)), static_cast<ssize_t>(std::strlen(TARGET)));
    KEXPECT_EQ(std::strcmp(text, TARGET), 0);
    KEXPECT_EQ(tmpfs::tmpfs_symlink_lookup(caller, missing, "replacement", &output), -1);
    KEXPECT_EQ(tmpfs::tmpfs_unlink_lookup(caller, missing), 0);
    missing = {};
    parent = {};
    KEXPECT_EQ(ker::vfs::vfs_rmdir(BASE), 0);
}

KTEST(Credentials, TmpfsHardlinkDenialsPreserveStateAndCanonicalIdentity) {
    namespace tmpfs = ker::vfs::tmpfs;
    auto const root = security::Snapshot::bootstrap_root();
    auto caller = root;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    constexpr const char* BASE = "/tmp/ktest_hardlink_credentials";
    constexpr const char* SOURCE = "/tmp/ktest_hardlink_credentials/source";
    constexpr const char* DESTINATION = "/tmp/ktest_hardlink_credentials/link";
    KREQUIRE_EQ(ker::vfs::vfs_mkdir(BASE, 0755), 0);
    auto* file = ker::vfs::vfs_open_file(SOURCE, ker::vfs::O_CREAT | ker::vfs::O_EXCL | 1, 0666);
    KREQUIRE_TRUE(file != nullptr);
    KREQUIRE_EQ(ker::vfs::vfs_close_file(file), 0);
    tmpfs::TmpfsLookupHandle parent, source, destination;
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(), BASE, &parent), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(), SOURCE, &source), 0);
    KREQUIRE_EQ(
        tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(), DESTINATION, &destination), 0);
    tmpfs::TmpfsObjectMetadata before{}, after{}, output{};
    output.uid = 12345;
    KREQUIRE_EQ(tmpfs::tmpfs_lookup_metadata(source, false, &before), 0);
    auto const GENERATION = destination.parent()->directory_generation;
    KEXPECT_EQ(tmpfs::tmpfs_link_lookup(caller, source, destination, &output), -EACCES);
    KEXPECT_EQ(output.uid, 12345);
    KEXPECT_EQ(destination.parent()->directory_generation, GENERATION);
    KREQUIRE_EQ(tmpfs::tmpfs_lookup_metadata(source, false, &after), 0);
    KEXPECT_EQ(after.link_count, before.link_count);
    KEXPECT_EQ(after.ctime.tv_sec, before.ctime.tv_sec);
    KEXPECT_EQ(after.ctime.tv_nsec, before.ctime.tv_nsec);
    KREQUIRE_EQ(tmpfs::tmpfs_chmod_lookup(parent, root, 01777), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_chmod_lookup(source, root, 0444), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_lookup_metadata(source, false, &before), 0);
    KEXPECT_EQ(tmpfs::tmpfs_link_lookup(caller, source, destination, &output), -EPERM);
    KEXPECT_EQ(output.uid, 12345);
    KEXPECT_EQ(destination.parent()->directory_generation, GENERATION);
    KEXPECT_TRUE(tmpfs::tmpfs_lookup(destination.parent(), destination.name()) == nullptr);
    KREQUIRE_EQ(tmpfs::tmpfs_lookup_metadata(source, false, &after), 0);
    KEXPECT_EQ(after.link_count, before.link_count);
    KEXPECT_EQ(after.ctime.tv_sec, before.ctime.tv_sec);
    KEXPECT_EQ(after.ctime.tv_nsec, before.ctime.tv_nsec);
    KREQUIRE_EQ(tmpfs::tmpfs_chown_lookup(source, root, 1000, 4000), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_chmod_lookup(source, root, 0000), 0);
    {
        ker::mod::sched::task::Task owner{};
        std::memcpy(owner.root.data(), "/", 2);
        std::memcpy(owner.cwd.data(), "/", 2);
        ker::vfs::LookupHandle first{}, second{};
        KREQUIRE_EQ(ker::vfs::vfs_acquire_lookup(&owner, ker::vfs::AT_FDCWD, SOURCE, ker::vfs::LookupIntent::LINK_SOURCE,
                                                 ker::vfs::LookupFollowPolicy::NOFOLLOW_FINAL, false, false, &first, 0, true),
                    0);
        KREQUIRE_EQ(ker::mod::sched::resource::change_credentials(owner, {.change = security::Change::SET_GID, .real = 2000}), 0);
        KREQUIRE_EQ(ker::vfs::vfs_acquire_lookup(&owner, ker::vfs::AT_FDCWD, DESTINATION, ker::vfs::LookupIntent::LINK_TARGET,
                                                 ker::vfs::LookupFollowPolicy::NOFOLLOW_FINAL, false, false, &second, 0, true),
                    0);
        unsigned calls = 0;
        auto consume = [](const ker::vfs::LookupHandle&, const ker::vfs::LookupHandle&, void* opaque) -> int {
            ++*static_cast<unsigned*>(opaque);
            return 0;
        };
        KEXPECT_EQ(ker::vfs::vfs_consume_lookup_pair(first, second, true, consume, &calls), -EAGAIN);
        KEXPECT_EQ(calls, 0);
    }
    KEXPECT_EQ(tmpfs::tmpfs_link_lookup(caller, source, destination, &output, false, true), -ENOENT);
    KEXPECT_EQ(output.uid, 12345);
    KEXPECT_EQ(destination.parent()->directory_generation, GENERATION);
    KEXPECT_EQ(tmpfs::tmpfs_link_lookup(caller, source, destination, &output), 0);
    KEXPECT_EQ(output.uid, 1000);
    KEXPECT_EQ(output.gid, 4000);
    KEXPECT_EQ(output.mode & 07777U, 0000);
    KEXPECT_EQ(output.link_count, 2);
    KEXPECT_EQ(tmpfs::tmpfs_link_lookup(caller, source, destination, &output), -EAGAIN);
    source = {};
    destination = {};
    parent = {};
    KEXPECT_EQ(ker::vfs::vfs_unlink(DESTINATION), 0);
    KEXPECT_EQ(ker::vfs::vfs_unlink(SOURCE), 0);
    KEXPECT_EQ(ker::vfs::vfs_rmdir(BASE), 0);
}

KTEST(Credentials, TmpfsLinkatRetainsAdmittedSnapshotAcrossCredentialReplacementAndRetry) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_linkat_admitted_snapshot(false));
}

KTEST(Credentials, XfsLinkatRetainsAdmittedSnapshotAcrossCredentialReplacementAndRetry) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_linkat_admitted_snapshot(true));
}

KTEST(Credentials, TmpfsRenameDenialsAndCanonicalNoopPreserveState) {
    namespace tmpfs = ker::vfs::tmpfs;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto caller = ROOT;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    constexpr const char* BASE = "/tmp/ktest_rename_credentials";
    constexpr const char* SOURCE = "/tmp/ktest_rename_credentials/source";
    constexpr const char* DESTINATION = "/tmp/ktest_rename_credentials/destination";
    KREQUIRE_EQ(ker::vfs::vfs_mkdir(BASE, 0755), 0);
    auto* file = ker::vfs::vfs_open_file(SOURCE, ker::vfs::O_CREAT | ker::vfs::O_EXCL | 1, 0000);
    KREQUIRE_TRUE(file != nullptr);
    KREQUIRE_EQ(ker::vfs::vfs_close_file(file), 0);
    tmpfs::TmpfsLookupHandle parent, source, destination;
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(), BASE, &parent), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(), SOURCE, &source), 0);
    KREQUIRE_EQ(
        tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(), DESTINATION, &destination), 0);
    tmpfs::TmpfsObjectMetadata before{}, after{}, output{};
    output.uid = 12345;
    bool changed = true;
    bool links_changed = true;
    auto const GENERATION = parent.target()->directory_generation;
    KREQUIRE_EQ(tmpfs::tmpfs_lookup_metadata(source, false, &before), 0);
    KEXPECT_EQ(tmpfs::tmpfs_rename_lookup(caller, source, destination, false, false, &output, &links_changed, &changed), -EACCES);
    KEXPECT_TRUE(changed && links_changed);
    KEXPECT_EQ(output.uid, 12345);
    KEXPECT_EQ(parent.target()->directory_generation, GENERATION);
    KREQUIRE_EQ(tmpfs::tmpfs_chmod_lookup(parent, ROOT, 01777), 0);
    KEXPECT_EQ(tmpfs::tmpfs_rename_lookup(caller, source, destination, false, false, &output, &links_changed, &changed), -EPERM);
    KEXPECT_TRUE(changed && links_changed);
    KEXPECT_EQ(output.uid, 12345);
    KEXPECT_EQ(parent.target()->directory_generation, GENERATION);
    KREQUIRE_EQ(tmpfs::tmpfs_lookup_metadata(source, false, &after), 0);
    KEXPECT_EQ(after.inode, before.inode);
    KEXPECT_EQ(after.link_count, before.link_count);
    KEXPECT_EQ(after.ctime.tv_sec, before.ctime.tv_sec);
    KEXPECT_EQ(after.ctime.tv_nsec, before.ctime.tv_nsec);
    KREQUIRE_EQ(tmpfs::tmpfs_chown_lookup(source, ROOT, 1000, 4000), 0);
    KEXPECT_EQ(tmpfs::tmpfs_rename_lookup(caller, source, destination, false, true, &output, &links_changed, &changed), -ENOENT);
    KEXPECT_EQ(tmpfs::tmpfs_rename_lookup(caller, source, destination, true, false, &output, &links_changed, &changed), -ENOTDIR);
    KEXPECT_TRUE(changed && links_changed);
    KEXPECT_EQ(output.uid, 12345);
    KEXPECT_EQ(parent.target()->directory_generation, GENERATION);
    KEXPECT_EQ(tmpfs::tmpfs_link_lookup(caller, source, destination), 0);
    source = {};
    destination = {};
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(), SOURCE, &source), 0);
    KREQUIRE_EQ(
        tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(), DESTINATION, &destination), 0);
    auto const NOOP_GENERATION = source.parent()->directory_generation;
    KREQUIRE_EQ(tmpfs::tmpfs_lookup_metadata(source, false, &before), 0);
    KEXPECT_EQ(tmpfs::tmpfs_rename_lookup(caller, source, destination, false, false, &output, &links_changed, &changed), 0);
    KEXPECT_TRUE(!changed && !links_changed);
    KEXPECT_EQ(output.link_count, 2);
    KEXPECT_EQ(output.uid, 1000);
    KEXPECT_EQ(output.gid, 4000);
    KEXPECT_EQ(output.mode, 0000);
    KEXPECT_EQ(output.ctime.tv_sec, before.ctime.tv_sec);
    KEXPECT_EQ(output.ctime.tv_nsec, before.ctime.tv_nsec);
    KEXPECT_EQ(source.parent()->directory_generation, NOOP_GENERATION);
    source = {};
    destination = {};
    parent = {};
    // Direct backend fixture mutations bypass VFS publication. Publish them
    // before warming the two path entries for the VFS no-op regression.
    ker::vfs::vfs_cache_notify_path_changed(SOURCE, DESTINATION);
    ker::vfs::Stat st{};
    KREQUIRE_EQ(ker::vfs::vfs_stat(SOURCE, &st), 0);
    KREQUIRE_EQ(st.st_nlink, 2);
    KREQUIRE_EQ(ker::vfs::vfs_stat(DESTINATION, &st), 0);
    KREQUIRE_EQ(st.st_nlink, 2);
    KEXPECT_EQ(ker::vfs::vfs_rename(SOURCE, DESTINATION), 0);
    KEXPECT_EQ(ker::vfs::vfs_stat(SOURCE, &st), 0);
    KEXPECT_EQ(st.st_nlink, 2);
    KEXPECT_EQ(ker::vfs::vfs_stat(DESTINATION, &st), 0);
    KEXPECT_EQ(st.st_nlink, 2);
    KEXPECT_EQ(ker::vfs::vfs_unlink(DESTINATION), 0);
    KEXPECT_EQ(ker::vfs::vfs_unlink(SOURCE), 0);
    KEXPECT_EQ(ker::vfs::vfs_rmdir(BASE), 0);
}

KTEST(Credentials, TmpfsRenameRejectsStructuralChangesBeforeDetachment) {
    namespace tmpfs = ker::vfs::tmpfs;
    auto const ROOT = security::Snapshot::bootstrap_root();
    constexpr const char* BASE = "/tmp/ktest_rename_structure";
    constexpr const char* DIRECTORY = "/tmp/ktest_rename_structure/directory";
    constexpr const char* CHILD = "/tmp/ktest_rename_structure/directory/child";
    constexpr const char* FILE = "/tmp/ktest_rename_structure/file";
    constexpr const char* OTHER = "/tmp/ktest_rename_structure/other";
    KREQUIRE_EQ(ker::vfs::vfs_mkdir(BASE, 0777), 0);
    KREQUIRE_EQ(ker::vfs::vfs_mkdir(DIRECTORY, 0777), 0);
    KREQUIRE_EQ(ker::vfs::vfs_mkdir(CHILD, 0777), 0);
    KREQUIRE_EQ(ker::vfs::vfs_mkdir(OTHER, 0777), 0);
    auto* file = ker::vfs::vfs_open_file(FILE, ker::vfs::O_CREAT | ker::vfs::O_EXCL | 1, 0600);
    KREQUIRE_TRUE(file != nullptr);
    KREQUIRE_EQ(ker::vfs::vfs_close_file(file), 0);
    auto reject = [&](const char* from, const char* to, int error) {
        tmpfs::TmpfsLookupHandle source, destination;
        KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(), from, &source), 0);
        KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), tmpfs::get_root_node(), to, &destination),
                    0);
        auto const SOURCE_GENERATION = source.parent()->directory_generation;
        auto const DESTINATION_GENERATION = destination.parent()->directory_generation;
        tmpfs::TmpfsObjectMetadata out{};
        out.uid = 12345;
        bool changed = true;
        bool links = true;
        KEXPECT_EQ(tmpfs::tmpfs_rename_lookup(ROOT, source, destination, false, false, &out, &links, &changed), error);
        KEXPECT_EQ(out.uid, 12345);
        KEXPECT_TRUE(changed && links);
        KEXPECT_EQ(source.parent()->directory_generation, SOURCE_GENERATION);
        KEXPECT_EQ(destination.parent()->directory_generation, DESTINATION_GENERATION);
        KEXPECT_TRUE(tmpfs::tmpfs_validate_lookup(source));
        KEXPECT_TRUE(tmpfs::tmpfs_validate_lookup(destination));
    };
    reject(FILE, OTHER, -EISDIR);
    reject(OTHER, FILE, -ENOTDIR);
    reject(OTHER, DIRECTORY, -ENOTEMPTY);
    reject(DIRECTORY, "/tmp/ktest_rename_structure/directory/child/cycle", -EINVAL);
    reject("/", OTHER, -EBUSY);
    KEXPECT_EQ(ker::vfs::vfs_rename("/tmp/ktest_rename_structure/directory/.", OTHER), -EINVAL);
    KEXPECT_EQ(ker::vfs::vfs_rename(OTHER, "/tmp/ktest_rename_structure/directory/child/.."), -EINVAL);
    KEXPECT_EQ(ker::vfs::vfs_rename(CHILD, OTHER), 0);
    ker::vfs::Stat st{};
    KEXPECT_EQ(ker::vfs::vfs_stat(CHILD, &st), -ENOENT);
    KEXPECT_EQ(ker::vfs::vfs_stat(OTHER, &st), 0);
    KEXPECT_EQ(ker::vfs::vfs_rmdir(OTHER), 0);
    KEXPECT_EQ(ker::vfs::vfs_rmdir(DIRECTORY), 0);
    KEXPECT_EQ(ker::vfs::vfs_unlink(FILE), 0);
    KEXPECT_EQ(ker::vfs::vfs_rmdir(BASE), 0);
}

KTEST(Credentials, FatExplicitRegularReadsPreserveRestrictionsAndDeniedState) {
    namespace fat = ker::vfs::fat32;
    auto fixture = std::make_unique<FatRenameFixture>();
    fat::FAT32DirectoryEntry entry{};
    std::memcpy(entry.name, "FILE    TXT", sizeof(entry.name));
    entry.cluster_low = 3;
    entry.attributes = fat::FAT32_ATTR_ARCHIVE | fat::FAT32_ATTR_READ_ONLY;
    entry.file_size = 4;
    std::memcpy(fixture->clusters[0].data(), &entry, sizeof(entry));
    std::memcpy(entry.name, "DIR        ", sizeof(entry.name));
    entry.cluster_low = 4;
    entry.attributes = fat::FAT32_ATTR_DIRECTORY;
    entry.file_size = 0;
    std::memcpy(fixture->clusters[0].data() + sizeof(entry), &entry, sizeof(entry));
    constexpr std::array<char, 4> BYTES{'f', 'a', 't', '!'};
    std::memcpy(fixture->clusters[1].data(), BYTES.data(), BYTES.size());
    auto const ORIGINAL = fixture->clusters;
    std::array<uint32_t, 128> table{};
    for (unsigned cluster : {2U, 3U, 4U}) table[cluster] = fat::FAT32_EOC;
    auto const ORIGINAL_TABLE = table;
    ker::dev::BlockDevice device{};
    device.block_size = 512;
    device.total_blocks = fixture->clusters.size();
    device.read_blocks = fat_rename_read;
    device.write_blocks = fat_rename_write;
    device.private_data = fixture.get();
    fat::FAT32MountContext context{};
    context.device = &device;
    context.root_cluster = 2;
    context.bytes_per_sector = 512;
    context.sectors_per_cluster = 1;
    context.sectors_per_fat = 1;
    context.total_sectors = fixture->clusters.size();
    context.fat_table = table.data();
    auto user = security::Snapshot::bootstrap_root();
    constexpr std::array<uint32_t, 2> GROUPS{17, 18};
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GROUPS, .groups = GROUPS}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::NO_NEW_PRIVILEGES}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GID, .real = 2000}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, user), 0);
    auto const BEFORE = user;
    int error = 0;
    auto* file = fat::fat32_open_path(user, "file.txt", 0, 0, &context, &error, false, true);
    KREQUIRE_TRUE(file != nullptr);
    KEXPECT_EQ(error, 0);
    std::array<char, 4> read{};
    KEXPECT_EQ(fat::fat32_read(file, read.data(), read.size(), 0), read.size());
    KEXPECT_TRUE(read == BYTES);
    ker::vfs::Stat stat{};
    KEXPECT_EQ(fat::fat32_fstat(file, &stat), 0);
    KEXPECT_EQ(stat.st_mode, ker::vfs::S_IFREG | 0444);
    KEXPECT_EQ(stat.st_uid, 0);
    KEXPECT_EQ(stat.st_gid, 0);
    KEXPECT_EQ(fat::fat32_close(file), 0);
    delete file;
    for (const char* path : {"", "/", "dir"}) {
        KEXPECT_TRUE(fat::fat32_open_path(user, path, 0, 0, &context, &error, false, true) == nullptr);
        KEXPECT_EQ(error, -EISDIR);
        KEXPECT_TRUE(context.open_nodes == nullptr);
    }
    KEXPECT_TRUE(fat::fat32_open_path(user, "missing", 0, 0, &context, &error, false, true) == nullptr);
    KEXPECT_EQ(error, -ENOENT);
    for (int flags : {1, ker::vfs::O_CREAT, ker::vfs::O_TRUNC, ker::vfs::O_DIRECTORY}) {
        KEXPECT_TRUE(fat::fat32_open_path(user, "file.txt", flags, 0, &context, &error, false, true) == nullptr);
        KEXPECT_EQ(error, -EINVAL);
    }
    // Ordinary directory opens retain their existing behavior.
    file = fat::fat32_open_path(user, "dir", 0, 0, &context, &error, true);
    KREQUIRE_TRUE(file != nullptr);
    KEXPECT_TRUE(file->is_directory);
    KEXPECT_EQ(fat::fat32_close(file), 0);
    delete file;
    KEXPECT_TRUE(context.open_nodes == nullptr);
    KEXPECT_TRUE(user == BEFORE && fixture->clusters == ORIGINAL && table == ORIGINAL_TABLE);
    KEXPECT_EQ(fixture->writes, 0);
}

KTEST(Credentials, FatRenamePreflightPreservesDirectoryDataAndOutputs) {
    namespace fat = ker::vfs::fat32;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto caller = ROOT;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    auto fixture = std::make_unique<FatRenameFixture>();
    auto put = [&](unsigned cluster, unsigned index, const char* name, uint32_t target, bool directory) {
        fat::FAT32DirectoryEntry entry{};
        std::memcpy(entry.name, name, sizeof(entry.name));
        entry.cluster_low = static_cast<uint16_t>(target);
        entry.attributes = directory ? fat::FAT32_ATTR_DIRECTORY : fat::FAT32_ATTR_ARCHIVE;
        entry.file_size = directory ? 0 : 4;
        std::memcpy(fixture->clusters[cluster - 2].data() + index * sizeof(entry), &entry, sizeof(entry));
    };
    put(2, 0, "FILE    TXT", 6, false);
    put(2, 1, "OTHER   TXT", 7, false);
    put(2, 2, "DIRA       ", 3, true);
    put(2, 3, "DIRB       ", 5, true);
    for (unsigned cluster : {3U, 4U, 5U}) {
        put(cluster, 0, ".          ", cluster, true);
        put(cluster, 1, "..         ", cluster == 4 ? 3 : 0, true);
    }
    put(3, 2, "CHILD      ", 4, true);
    std::array<uint32_t, 128> table{};
    for (unsigned cluster = 2; cluster < 8; ++cluster) table[cluster] = fat::FAT32_EOC;
    auto const ORIGINAL_TABLE = table;
    auto original = std::make_unique<FatRenameFixture>(*fixture);
    ker::dev::BlockDevice device{};
    device.block_size = 512;
    device.total_blocks = fixture->clusters.size();
    device.read_blocks = fat_rename_read;
    device.write_blocks = fat_rename_write;
    device.private_data = fixture.get();
    fat::FAT32MountContext context{};
    context.device = &device;
    context.root_cluster = 2;
    context.bytes_per_sector = 512;
    context.sectors_per_cluster = 1;
    context.sectors_per_fat = 1;
    context.total_sectors = fixture->clusters.size();
    context.fat_table = table.data();
    auto reject = [&](const security::Snapshot& credentials, const char* source, const char* destination, int expected) {
        bool changed = true;
        int const RET = fat::fat32_rename_path(credentials, &context, source, destination, false, false, &changed);
        if (RET != expected) ker::mod::dbg::log("[fat rename ktest] %s -> %s: result=%d expected=%d", source, destination, RET, expected);
        KEXPECT_EQ(RET, expected);
        KEXPECT_TRUE(changed);
        KEXPECT_EQ(fixture->writes, 0);
        KEXPECT_TRUE(fixture->clusters == original->clusters);
        KEXPECT_TRUE(table == ORIGINAL_TABLE);
    };
    for (const char* target : {"other.txt", "new.txt", "DIRB", "new/"}) reject(caller, "file.txt", target, -EACCES);
    reject(caller, "file.txt", "FILE.TXT", -EACCES);
    reject(ROOT, "file.txt", "DIRB", -EISDIR);
    reject(ROOT, "DIRB", "other.txt", -ENOTDIR);
    reject(ROOT, "DIRB", "DIRA", -ENOTEMPTY);
    reject(ROOT, "DIRA", "DIRA/CHILD/moved", -EINVAL);
    reject(ROOT, "file.txt/", "new.txt", -ENOTDIR);
    reject(ROOT, "file.txt", "new/", -ENOENT);
    reject(ROOT, "DIRA/.", "new", -EINVAL);
    reject(ROOT, "file.txt", "DIRA/CHILD/..", -EINVAL);
    reject(ROOT, "/", "new", -EBUSY);
    fixture->fail_read = 3;
    reject(ROOT, "DIRA", "DIRB", -EIO);
    KEXPECT_EQ(fat::fat32_rmdir_path(ROOT, &context, "DIRB"), -EIO);
    KEXPECT_EQ(fixture->writes, 0);
    KEXPECT_TRUE(fixture->clusters == original->clusters);
    KEXPECT_TRUE(table == ORIGINAL_TABLE);
    fixture->fail_read = UINT64_MAX;
    for (const char* source : {"file.txt", "DIRA"}) {
        bool changed = true;
        KEXPECT_EQ(fat::fat32_rename_path(ROOT, &context, source, source, false, false, &changed), 0);
        KEXPECT_TRUE(!changed);
        KEXPECT_EQ(fixture->writes, 0);
        KEXPECT_TRUE(fixture->clusters == original->clusters);
        KEXPECT_TRUE(table == ORIGINAL_TABLE);
    }
    bool changed = true;
    KEXPECT_EQ(fat::fat32_rename_path(ROOT, &context, "file.txt", "FILE.TXT", false, false, &changed), 0);
    KEXPECT_TRUE(!changed);
    KEXPECT_EQ(fixture->writes, 0);
    KEXPECT_TRUE(fixture->clusters == original->clusters);
    // A full deleted-entry directory with a cyclic FAT chain is corrupt,
    // not an empty replacement. The scan must terminate without freeing it.
    fixture->clusters[3].fill(0xE5);
    table[5] = 5;
    auto cyclic = std::make_unique<FatRenameFixture>(*fixture);
    changed = true;
    KEXPECT_EQ(fat::fat32_rename_path(ROOT, &context, "DIRA", "DIRB", false, false, &changed), -ELOOP);
    KEXPECT_TRUE(changed);
    KEXPECT_EQ(fixture->writes, 0);
    KEXPECT_TRUE(fixture->clusters == cyclic->clusters);
    KEXPECT_EQ(table[5], 5);
}

KTEST(Credentials, FatRenamePreparedWritesRestorePartialFailuresAndDirectoryParents) {
    namespace fat = ker::vfs::fat32;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto fixture = std::make_unique<FatRenameFixture>();
    auto put = [&](unsigned cluster, unsigned index, const char* name, uint32_t target, bool directory) {
        fat::FAT32DirectoryEntry entry{};
        std::memcpy(entry.name, name, sizeof(entry.name));
        entry.cluster_low = static_cast<uint16_t>(target);
        entry.attributes = directory ? fat::FAT32_ATTR_DIRECTORY : fat::FAT32_ATTR_ARCHIVE | fat::FAT32_ATTR_READ_ONLY;
        entry.file_size = directory ? 0 : 4;
        entry.create_date = 1234;
        entry.modify_time = 5678;
        std::memcpy(fixture->clusters[cluster].data() + index * sizeof(entry), &entry, sizeof(entry));
    };
    put(2, 0, "FILE    TXT", 6, false);
    put(2, 1, "OTHER   TXT", 7, false);
    put(2, 2, "DIRA       ", 3, true);
    put(2, 3, "DIRB       ", 5, true);
    for (unsigned cluster : {3U, 4U, 5U}) {
        put(cluster, 0, ".          ", cluster, true);
        put(cluster, 1, "..         ", cluster == 4 ? 3 : 0, true);
    }
    put(3, 2, "CHILD      ", 4, true);
    fixture->clusters[6].fill(0x61);
    fixture->clusters[7].fill(0x62);
    std::array<uint32_t, 128> table{};
    for (unsigned cluster = 2; cluster < 8; ++cluster) table[cluster] = fat::FAT32_EOC;
    for (unsigned copy : {0U, 1U}) std::memcpy(fixture->clusters[copy].data(), table.data(), 512);
    auto const ORIGINAL_TABLE = table;
    auto original = std::make_unique<FatRenameFixture>(*fixture);
    ker::dev::BlockDevice device{};
    device.block_size = 512;
    device.total_blocks = fixture->clusters.size();
    device.read_blocks = fat_rename_read;
    device.write_blocks = fat_rename_write;
    device.flush = fat_rename_flush;
    device.private_data = fixture.get();
    fat::FAT32MountContext context{};
    context.device = &device;
    context.root_cluster = 2;
    context.bytes_per_sector = 512;
    context.sectors_per_cluster = 1;
    context.sectors_per_fat = 1;
    context.num_fats = 2;
    context.data_start_sector = 2;
    context.total_sectors = fixture->clusters.size();
    context.fat_table = table.data();
    auto reset = [&] {
        *fixture = *original;
        table = ORIGINAL_TABLE;
        context.read_only = false;
    };
    auto at = [&](unsigned block, unsigned index) {
        fat::FAT32DirectoryEntry entry{};
        std::memcpy(&entry, fixture->clusters[block].data() + index * sizeof(entry), sizeof(entry));
        return entry;
    };
    for (bool directory : {false, true}) {
        const char* source = directory ? "DIRB" : "file.txt";
        const char* destination = directory ? "DIRA/moved" : "other.txt";
        for (unsigned failure = 1; failure <= 4; ++failure) {
            reset();
            if (failure < 4)
                fixture->fail_write = failure;
            else
                fixture->fail_flush = 1;
            bool changed = false;
            KEXPECT_EQ(fat::fat32_rename_path(ROOT, &context, source, destination, false, false, &changed), -EIO);
            KEXPECT_TRUE(!changed);
            KEXPECT_TRUE(fixture->clusters == original->clusters);
            KEXPECT_TRUE(table == ORIGINAL_TABLE);
            KEXPECT_TRUE(!context.read_only);
        }
        reset();
        bool changed = false;
        KEXPECT_EQ(fat::fat32_rename_path(ROOT, &context, source, destination, false, false, &changed), 0);
        KEXPECT_TRUE(changed);
        if (directory) {
            KEXPECT_EQ(static_cast<uint8_t>(at(2, 3).name[0]), 0xE5);
            KEXPECT_EQ(at(3, 4).cluster_low, 5);
            KEXPECT_EQ(at(5, 1).cluster_low, 3);
            KEXPECT_EQ(at(3, 4).create_date, 1234);
            KEXPECT_TRUE(table == ORIGINAL_TABLE);
        } else {
            KEXPECT_EQ(static_cast<uint8_t>(at(2, 0).name[0]), 0xE5);
            auto entry = at(2, 1);
            KEXPECT_EQ(entry.cluster_low, 6);
            KEXPECT_EQ(entry.file_size, 4);
            KEXPECT_EQ(entry.create_date, 1234);
            KEXPECT_EQ(entry.modify_time, 5678);
            KEXPECT_EQ(entry.attributes, fat::FAT32_ATTR_ARCHIVE | fat::FAT32_ATTR_READ_ONLY);
            KEXPECT_TRUE(std::memcmp(entry.name, "OTHER   TXT", 11) == 0);
            KEXPECT_EQ(table[7], 0);
            for (unsigned copy : {0U, 1U}) KEXPECT_TRUE(std::memcmp(fixture->clusters[copy].data(), table.data(), 512) == 0);
            KEXPECT_TRUE(fixture->clusters[6] == original->clusters[6]);
            KEXPECT_TRUE(fixture->clusters[7] == original->clusters[7]);
        }
    }
    reset();
    // Reclaiming a corrupt shared tail must leave both names and all blocks.
    table[6] = 7;
    auto const SHARED_TABLE = table;
    bool changed = false;
    KEXPECT_EQ(fat::fat32_rename_path(ROOT, &context, "file.txt", "other.txt", false, false, &changed), -EIO);
    KEXPECT_TRUE(!changed);
    KEXPECT_EQ(fixture->writes, 0);
    KEXPECT_TRUE(fixture->clusters == original->clusters);
    KEXPECT_TRUE(table == SHARED_TABLE);
    reset();
    // A full destination directory is rejected before erasing the source.
    for (unsigned index = 3; index < 16; ++index) put(3, index, "OCCUPIED   ", 6, false);
    auto full = std::make_unique<FatRenameFixture>(*fixture);
    KEXPECT_EQ(fat::fat32_rename_path(ROOT, &context, "file.txt", "DIRA/new.txt", false, false, &changed), -ENOSPC);
    KEXPECT_TRUE(!changed);
    KEXPECT_EQ(fixture->writes, 0);
    KEXPECT_TRUE(fixture->clusters == full->clusters);
    KEXPECT_TRUE(table == ORIGINAL_TABLE);
    reset();
    // Creation and later removal of a 255-byte LFN cross directory clusters.
    table[2] = 8;
    table[8] = fat::FAT32_EOC;
    fixture->clusters[8].fill(0x61);
    fixture->clusters[8][0] = 0;
    for (unsigned copy : {0U, 1U}) std::memcpy(fixture->clusters[copy].data(), table.data(), 512);
    std::array<char, 256> name{};
    name.fill('n');
    name.back() = 0;
    KEXPECT_EQ(fat::fat32_rename_path(ROOT, &context, "file.txt", name.data(), false, false, &changed), 0);
    KEXPECT_TRUE(changed);
    KEXPECT_EQ(at(8, 8).cluster_low, 6);
    KEXPECT_EQ(at(8, 9).name[0], 0);
    changed = false;
    KEXPECT_EQ(fat::fat32_rename_path(ROOT, &context, name.data(), "other.txt", false, false, &changed), 0);
    KEXPECT_TRUE(changed);
    for (unsigned index = 4; index < 16; ++index) KEXPECT_EQ(static_cast<uint8_t>(at(2, index).name[0]), 0xE5);
    for (unsigned index = 0; index <= 8; ++index) KEXPECT_EQ(static_cast<uint8_t>(at(8, index).name[0]), 0xE5);
    KEXPECT_EQ(at(2, 1).cluster_low, 6);
    reset();
    fixture->fail_write = 1;
    fixture->persistent_write_failure = true;
    changed = false;
    KEXPECT_EQ(fat::fat32_rename_path(ROOT, &context, "file.txt", "other.txt", false, false, &changed), -EIO);
    KEXPECT_TRUE(!changed);
    KEXPECT_TRUE(context.read_only);
    unsigned const WRITES = fixture->writes;
    KEXPECT_EQ(fat::fat32_rename_path(ROOT, &context, "file.txt", "other.txt", false, false, &changed), -EROFS);
    KEXPECT_EQ(fixture->writes, WRITES);
    KEXPECT_TRUE(table == ORIGINAL_TABLE);
}

KTEST(Credentials, FatRenameRetainsOpenObjectsAndRetriesFailedReclamation) {
    namespace fat = ker::vfs::fat32;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto fixture = std::make_unique<FatRenameFixture>();
    auto put = [&](unsigned cluster, unsigned index, const char* name, uint32_t target, bool directory) {
        fat::FAT32DirectoryEntry entry{};
        std::memcpy(entry.name, name, sizeof(entry.name));
        entry.cluster_low = static_cast<uint16_t>(target);
        entry.attributes = directory ? fat::FAT32_ATTR_DIRECTORY : fat::FAT32_ATTR_ARCHIVE;
        entry.file_size = directory ? 0 : 4;
        std::memcpy(fixture->clusters[cluster].data() + index * sizeof(entry), &entry, sizeof(entry));
    };
    put(2, 0, "FILE    TXT", 6, false);
    put(2, 1, "OTHER   TXT", 7, false);
    fixture->clusters[6].fill('a');
    fixture->clusters[7].fill('b');
    std::array<uint32_t, 128> table{};
    for (unsigned cluster : {2U, 6U, 7U}) table[cluster] = fat::FAT32_EOC;
    for (unsigned copy : {0U, 1U}) std::memcpy(fixture->clusters[copy].data(), table.data(), 512);
    auto const ORIGINAL_TABLE = table;
    auto original = std::make_unique<FatRenameFixture>(*fixture);
    ker::dev::BlockDevice device{};
    device.block_size = 512;
    device.total_blocks = fixture->clusters.size();
    device.read_blocks = fat_rename_read;
    device.write_blocks = fat_rename_write;
    device.flush = fat_rename_flush;
    device.private_data = fixture.get();
    fat::FAT32MountContext context{};
    context.device = &device;
    context.root_cluster = 2;
    context.bytes_per_sector = 512;
    context.sectors_per_cluster = 1;
    context.sectors_per_fat = 1;
    context.num_fats = 2;
    context.data_start_sector = 2;
    context.total_sectors = fixture->clusters.size();
    context.fat_table = table.data();
    auto* source = fat::fat32_open_path(ROOT, "file.txt", 2, 0, &context);
    auto* source_reader = fat::fat32_open_path(ROOT, "file.txt", 0, 0, &context);
    auto* displaced = fat::fat32_open_path(ROOT, "other.txt", 2, 0, &context);
    auto* displaced_reader = fat::fat32_open_path(ROOT, "other.txt", 0, 0, &context);
    KREQUIRE_TRUE(source && source_reader && displaced && displaced_reader);
    KEXPECT_EQ(fat::fat32_unmount(&context), -EBUSY);
    auto contents = [&](ker::vfs::File* file, const char* expected, unsigned links) {
        size_t const LENGTH = std::strlen(expected);
        std::array<char, 32> data{};
        ker::vfs::Stat st{};
        KEXPECT_EQ(fat::fat32_fstat(file, &st), 0);
        KEXPECT_EQ(st.st_size, static_cast<off_t>(LENGTH));
        KEXPECT_EQ(st.st_nlink, links);
        KEXPECT_EQ(fat::fat32_read(file, data.data(), data.size(), 0), static_cast<ssize_t>(LENGTH));
        KEXPECT_TRUE(std::memcmp(data.data(), expected, LENGTH) == 0);
    };
    auto close = [&](ker::vfs::File*& file, int expected = 0) {
        KEXPECT_EQ(fat::fat32_close(file), expected);
        delete file;
        file = nullptr;
    };
    bool changed = false;
    fixture->fail_write = 1;
    KEXPECT_EQ(fat::fat32_rename_path(ROOT, &context, "file.txt", "other.txt", false, false, &changed), -EIO);
    KEXPECT_TRUE(!changed);
    KEXPECT_TRUE(fixture->clusters == original->clusters);
    KEXPECT_TRUE(table == ORIGINAL_TABLE);
    contents(source, "aaaa", 1);
    contents(displaced, "bbbb", 1);
    fixture->fail_write = 0;
    KEXPECT_EQ(fat::fat32_rename_path(ROOT, &context, "file.txt", "other.txt", false, false, &changed), 0);
    KEXPECT_TRUE(changed);
    KEXPECT_EQ(table[7], fat::FAT32_EOC);
    contents(source_reader, "aaaa", 1);
    contents(displaced_reader, "bbbb", 0);
    auto* reused = fat::fat32_open_path(ROOT, "file.txt", ker::vfs::O_CREAT | ker::vfs::O_EXCL | 2, 0600, &context);
    KREQUIRE_TRUE(reused != nullptr);
    KEXPECT_EQ(fat::fat32_write(reused, "cccc", 4, 0), static_cast<ssize_t>(4));
    KEXPECT_EQ(fat::fat32_write(source, "XY", 2, 1), static_cast<ssize_t>(2));
    contents(source_reader, "aXYa", 1);
    contents(reused, "cccc", 1);
    auto* current = fat::fat32_open_path(ROOT, "other.txt", 0, 0, &context);
    KREQUIRE_TRUE(current != nullptr);
    contents(current, "aXYa", 1);
    close(current);
    KEXPECT_EQ(fat::fat32_write(displaced, "old-data", 8, 0), static_cast<ssize_t>(8));
    contents(displaced_reader, "old-data", 0);
    contents(source_reader, "aXYa", 1);
    ker::vfs::Stat path_stat{};
    KEXPECT_EQ(fat::fat32_stat("other.txt", &path_stat, &context), 0);
    KEXPECT_EQ(path_stat.st_size, 4);
    KEXPECT_EQ(path_stat.st_ino, 6);
    close(displaced);
    KEXPECT_EQ(table[7], fat::FAT32_EOC);
    auto const BEFORE_CLOSE_TABLE = table;
    auto before_close = std::make_unique<FatRenameFixture>(*fixture);
    fixture->fail_write = fixture->writes + 1;
    close(displaced_reader, -EIO);
    KEXPECT_TRUE(table == BEFORE_CLOSE_TABLE);
    KEXPECT_TRUE(fixture->clusters == before_close->clusters);
    KEXPECT_TRUE(context.open_nodes != nullptr);
    fixture->fail_write = 0;
    close(source);
    close(source_reader);
    close(reused);
    // Only the failed-close orphan remains. An unmount must not destroy its
    // context on a retryable read error (this fixture's context is stack-owned).
    fixture->fail_read = 1;
    KEXPECT_EQ(fat::fat32_sync_mount(&context), -EIO);
    KEXPECT_EQ(fat::fat32_unmount(&context), -EIO);
    KEXPECT_EQ(table[7], fat::FAT32_EOC);
    KEXPECT_TRUE(context.open_nodes != nullptr);
    fixture->fail_read = UINT64_MAX;
    KEXPECT_EQ(fat::fat32_sync_mount(&context), 0);
    KEXPECT_EQ(table[7], 0);
    KEXPECT_TRUE(context.open_nodes == nullptr);
    source = fat::fat32_open_path(ROOT, "other.txt", 2, 0, &context);
    source_reader = fat::fat32_open_path(ROOT, "other.txt", 0, 0, &context);
    KREQUIRE_TRUE(source && source_reader);
    KEXPECT_EQ(fat::fat32_unlink_path(ROOT, &context, "other.txt"), 0);
    contents(source_reader, "aXYa", 0);
    KEXPECT_EQ(table[6], fat::FAT32_EOC);
    KEXPECT_EQ(fat::fat32_write(source, "unlinked", 8, 0), static_cast<ssize_t>(8));
    contents(source_reader, "unlinked", 0);
    close(source);
    KEXPECT_EQ(table[6], fat::FAT32_EOC);
    close(source_reader);
    KEXPECT_EQ(table[6], 0);
    KEXPECT_TRUE(context.open_nodes == nullptr);
    put(2, 4, "EMPTY      ", 4, true);
    put(4, 0, ".          ", 4, true);
    put(4, 1, "..         ", 0, true);
    table[4] = fat::FAT32_EOC;
    auto* directory = fat::fat32_open_path(ROOT, "EMPTY", 0, 0, &context);
    KREQUIRE_TRUE(directory != nullptr);
    KEXPECT_EQ(fat::fat32_rmdir_path(ROOT, &context, "EMPTY"), 0);
    KEXPECT_EQ(fat::fat32_fstat(directory, &path_stat), 0);
    KEXPECT_EQ(path_stat.st_nlink, 0);
    KEXPECT_EQ(table[4], fat::FAT32_EOC);
    close(directory);
    KEXPECT_EQ(table[4], 0);
    KEXPECT_TRUE(context.open_nodes == nullptr);
    KEXPECT_EQ(fat::fat32_unlink_path(ROOT, &context, "file.txt"), 0);
    KEXPECT_TRUE(context.open_nodes == nullptr);
}

KTEST(Credentials, TmpfsSearchChecksAncestorsAndStartsAtRetainedDirectory) {
    namespace tmpfs = ker::vfs::tmpfs;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto caller = ROOT;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    constexpr const char* BASE = "/tmp/ktest_search_authority";
    constexpr const char* CHILD = "/tmp/ktest_search_authority/child";
    constexpr const char* FILE = "/tmp/ktest_search_authority/child/file";
    KREQUIRE_EQ(ker::vfs::vfs_mkdir(BASE, 0700), 0);
    KREQUIRE_EQ(ker::vfs::vfs_mkdir(CHILD, 0755), 0);
    auto* file = ker::vfs::vfs_open_file(FILE, ker::vfs::O_CREAT | ker::vfs::O_EXCL | 2, 0644);
    KREQUIRE_TRUE(file != nullptr);
    KREQUIRE_EQ(ker::vfs::vfs_close_file(file), 0);
    auto* directory = ker::vfs::vfs_open_file(CHILD, ker::vfs::O_DIRECTORY, 0);
    KREQUIRE_TRUE(directory != nullptr);
    auto* anchor = static_cast<tmpfs::TmpNode*>(directory->private_data);
    tmpfs::TmpfsLookupHandle lookup;
    uint64_t const GENERATION = tmpfs::tmpfs_namespace_generation();
    KEXPECT_EQ(tmpfs::tmpfs_acquire_lookup(caller, tmpfs::get_root_node(), FILE, &lookup), -EACCES);
    KEXPECT_TRUE(lookup.parent() == nullptr);
    KEXPECT_EQ(tmpfs::tmpfs_acquire_lookup(caller, tmpfs::get_root_node(), "/tmp/ktest_search_authority/child/missing", &lookup), -EACCES);
    KEXPECT_EQ(tmpfs::tmpfs_acquire_lookup(caller, tmpfs::get_root_node(), "/tmp/ktest_search_authority/../other", &lookup), -EACCES);
    KEXPECT_EQ(tmpfs::tmpfs_namespace_generation(), GENERATION);
    KEXPECT_EQ(tmpfs::tmpfs_acquire_lookup(caller, anchor, "file", &lookup), 0);
    KEXPECT_TRUE(lookup.target_existed());
    lookup = {};
    KEXPECT_EQ(tmpfs::tmpfs_chmod_node(anchor, ROOT, 0700), 0);
    KEXPECT_EQ(tmpfs::tmpfs_acquire_lookup(caller, anchor, "file", &lookup), -EACCES);
    KEXPECT_TRUE(lookup.parent() == nullptr);
    KEXPECT_EQ(tmpfs::tmpfs_chmod_node(anchor, ROOT, 0000), 0);
    KEXPECT_EQ(tmpfs::tmpfs_acquire_lookup(ROOT, tmpfs::get_root_node(), FILE, &lookup), 0);
    KEXPECT_TRUE(lookup.target_existed());
    lookup = {};
    KEXPECT_EQ(tmpfs::tmpfs_chmod_node(anchor, ROOT, 0755), 0);
    KEXPECT_EQ(ker::vfs::vfs_close_file(directory), 0);
    KEXPECT_EQ(ker::vfs::vfs_unlink(FILE), 0);
    KEXPECT_EQ(ker::vfs::vfs_rmdir(CHILD), 0);
    KEXPECT_EQ(ker::vfs::vfs_rmdir(BASE), 0);
}

KTEST(Credentials, MountTargetUsesAdmittedSnapshotAndRejectsMissingEntries) {
    namespace vfs = ker::vfs;
    constexpr const char* BASE = "/tmp/ktest_mount_admission";
    constexpr const char* CHILD = "/tmp/ktest_mount_admission/child";
    constexpr const char* MISSING = "/tmp/ktest_mount_admission/missing";
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0700), 0);
    KREQUIRE_EQ(vfs::vfs_mkdir(CHILD, 0755), 0);
    ker::mod::sched::task::Task task{};
    task.pid = UINT64_MAX;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    auto const GENERATION = vfs::tmpfs::tmpfs_namespace_generation();
    for (bool resolved : {false, true}) {
        vfs::LookupHandle denied{};
        KEXPECT_EQ(vfs::vfs_acquire_lookup(&task, vfs::AT_FDCWD, CHILD, vfs::LookupIntent::MOUNT_TARGET,
                                           vfs::LookupFollowPolicy::FOLLOW_FINAL, true, false, &denied, 3, resolved, &user),
                   -EACCES);
        KEXPECT_TRUE(denied.mount() == nullptr);
    }
    KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(task, user), 0);
    for (bool resolved : {false, true}) {
        vfs::LookupHandle allowed{};
        KEXPECT_EQ(vfs::vfs_acquire_lookup(&task, vfs::AT_FDCWD, CHILD, vfs::LookupIntent::MOUNT_TARGET,
                                           vfs::LookupFollowPolicy::FOLLOW_FINAL, true, false, &allowed, 3, resolved, &ROOT),
                   0);
        KEXPECT_TRUE(allowed.authorization().credentials == ROOT);
        vfs::LookupHandle missing{};
        KEXPECT_EQ(vfs::vfs_acquire_lookup(&task, vfs::AT_FDCWD, MISSING, vfs::LookupIntent::MOUNT_TARGET,
                                           vfs::LookupFollowPolicy::FOLLOW_FINAL, true, false, &missing, 3, resolved, &ROOT),
                   -ENOENT);
    }
    KEXPECT_EQ(vfs::tmpfs::tmpfs_namespace_generation(), GENERATION);
    vfs::Stat stat{};
    KEXPECT_EQ(vfs::vfs_stat(MISSING, &stat), -ENOENT);
    KEXPECT_EQ(vfs::tmpfs::tmpfs_namespace_generation(), GENERATION);
    KEXPECT_EQ(vfs::vfs_rmdir(CHILD), 0);
    KEXPECT_EQ(vfs::vfs_rmdir(BASE), 0);
}

KTEST(Credentials, MountPublicationKeepsAdmittedCredentials) { KEXPECT_TRUE(ker::vfs::vfs_selftest_mount_admitted_snapshot()); }

KTEST(Credentials, MountIdentityExhaustionPreservesPublicationAndExactRetirement) {
    KEXPECT_TRUE(ker::vfs::mount_selftest_identity_exhaustion());
}

KTEST(Credentials, MountPublicationKeepsAdmittedPhysicalTarget) { KEXPECT_TRUE(ker::vfs::vfs_selftest_mount_physical_target()); }

KTEST(Credentials, MountPublicationRefreshesOnlyUnchangedTmpfsTarget) { KEXPECT_TRUE(ker::vfs::vfs_selftest_mount_target_refresh()); }

KTEST(Credentials, MountPublicationKeepsSelectedProcfsDirectory) { KEXPECT_TRUE(ker::vfs::vfs_selftest_mount_procfs_binding()); }

KTEST(Credentials, MountPublicationKeepsSelectedDevfsDirectory) { KEXPECT_TRUE(ker::vfs::vfs_selftest_mount_devfs_binding()); }

KTEST(Credentials, DevfsMountConsumerGuardsBindingAndReservesWkiWriters) { KEXPECT_TRUE(ker::vfs::devfs::selftest_mount_target_guard()); }

KTEST(Credentials, MountPublicationReadsAdmittedFatMetadata) { KEXPECT_TRUE(ker::vfs::vfs_selftest_mount_fat_metadata()); }

KTEST(Credentials, NosuidPolicyFollowsRetainedMountAcrossOvermount) {
    namespace vfs = ker::vfs;
    constexpr const char* BASE = "/tmp/ktest_nosuid_mount";
    constexpr const char* CHILD = "/tmp/ktest_nosuid_mount/child";
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
    bool mounted = false, overmounted = false;
    vfs::File* original = nullptr;
    struct Cleanup {
        bool& mounted;
        bool& overmounted;
        vfs::File*& original;
        ~Cleanup() {
            if (original != nullptr) static_cast<void>(vfs::vfs_close_file(original));
            if (overmounted) static_cast<void>(vfs::unmount_filesystem(CHILD));
            if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
            static_cast<void>(vfs::vfs_rmdir(BASE));
        }
    } cleanup{mounted, overmounted, original};
    KREQUIRE_EQ(vfs::mount_filesystem(BASE, "tmpfs", nullptr, vfs::MOUNT_FLAG_NOSUID), 0);
    mounted = true;
    KREQUIRE_EQ(vfs::vfs_mkdir(CHILD, 0755), 0);
    original = vfs::vfs_open_file(CHILD, vfs::O_DIRECTORY, 0);
    KREQUIRE_TRUE(original != nullptr);
    KEXPECT_TRUE(original->mount_policy.nosuid);
    KEXPECT_TRUE(vfs::file_mount_nosuid(*original));
    vfs::Statvfs path{}, opened{};
    KEXPECT_EQ(vfs::vfs_statvfs(CHILD, &path), 0);
    KEXPECT_EQ(path.f_flag & vfs::ST_NOSUID, vfs::ST_NOSUID);
    bool snapshot_found = false;
    for (size_t i = 0; i < vfs::get_mount_count(); ++i) {
        vfs::MountSnapshot row{};
        if (vfs::get_mount_snapshot_at(i, &row) && std::strcmp(row.path, BASE) == 0) snapshot_found = row.nosuid;
    }
    KEXPECT_TRUE(snapshot_found);
    KREQUIRE_EQ(vfs::mount_filesystem(CHILD, "tmpfs", nullptr), 0);
    overmounted = true;
    KEXPECT_EQ(vfs::vfs_statvfs(CHILD, &path), 0);
    KEXPECT_EQ(path.f_flag & vfs::ST_NOSUID, uint64_t{0});
    KEXPECT_EQ(vfs::vfs_fstatvfs_file(original, &opened), 0);
    KEXPECT_EQ(opened.f_flag & vfs::ST_NOSUID, vfs::ST_NOSUID);
    KEXPECT_TRUE(vfs::file_mount_nosuid(*original));
    auto user = security::Snapshot::bootstrap_root();
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GID, .real = 2001}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    security::Snapshot after;
    KEXPECT_EQ(
        user.prepare_exec({.uid = 0, .gid = 0, .set_uid = true, .set_gid = true, .nosuid = vfs::file_mount_nosuid(*original), .mode = 0755},
                          after),
        0);
    KEXPECT_EQ(after.user().effective, 1000);
    KEXPECT_EQ(after.user().saved, 1000);
    KEXPECT_EQ(after.group().effective, 2001);
    KEXPECT_EQ(after.group().saved, 2001);
}

KTEST(Credentials, RetainedFileViewRestrictsWritableOwnerAndSharedDescriptors) {
    namespace vfs = ker::vfs;
    ker::mod::sched::task::Task task{};
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GID, .real = 2001}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1001}, user), 0);
    constexpr auto BASE = "/tmp/ktest_retained_view_policy";
    constexpr auto PATH = "/tmp/ktest_retained_view_policy/file";
    constexpr std::array<char, 4> DATA{'k', 'e', 'e', 'p'};
    constexpr std::array<char, 4> EDIT{'e', 'd', 'i', 't'};
    constexpr std::array<vfs::Timespec, 2> TIMES{{{123, 456}, {789, 123}}};
    constexpr std::array<vfs::Timespec, 2> OMIT{{{0, vfs::TimestampUpdate::OMIT}, {0, vfs::TimestampUpdate::OMIT}}};
    for (bool disk : {false, true}) {
        struct Cleanup {
            ker::mod::sched::task::Task& task;
            vfs::File* file{};
            vfs::File* owner_view{};
            int descriptor = -1;
            int duplicate = -1;
            bool mounted{};
            void close_descriptor(int& fd) {
                if (fd < 0) return;
                auto* retained = vfs::vfs_get_file(&task, fd);
                if (vfs::vfs_release_fd(&task, fd) == 0) vfs::vfs_put_file(retained);
                fd = -1;
            }
            ~Cleanup() {
                close_descriptor(duplicate);
                close_descriptor(descriptor);
                vfs::vfs_put_file(file);
                vfs::vfs_put_file(owner_view);
                static_cast<void>(vfs::vfs_unlink(PATH));
                if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
                static_cast<void>(vfs::vfs_rmdir(BASE));
            }
        } cleanup{task};
        KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
        if (disk) {
            auto* device = ker::dev::block_device_find_by_name("sdb2");
            KREQUIRE_TRUE(device != nullptr);
            KREQUIRE_EQ(vfs::mount_filesystem(BASE, "xfs", device), 0);
            cleanup.mounted = true;
        }
        cleanup.file = vfs::vfs_open_file(PATH, vfs::O_CREAT | vfs::O_EXCL | vfs::O_APPEND | 2, 0644);
        KREQUIRE_TRUE(cleanup.file != nullptr);
        auto* file = cleanup.file;
        KREQUIRE_TRUE(file->mount_owner != nullptr);
        KEXPECT_TRUE(!file->mount_owner->read_only && !file->mount_owner->nosuid);
        KREQUIRE_EQ(vfs::vfs_write_file(file, DATA.data(), DATA.size(), nullptr), DATA.size());
        KREQUIRE_EQ(vfs::vfs_futimens_file_as(file, ROOT, TIMES.data()), 0);
        vfs::Stat before{}, after{};
        KREQUIRE_EQ(vfs::vfs_fstat_file(file, &before), 0);
        // Model a privately staged receiver File, before any FD/anchor sharing.
        file->mount_policy = file->mount_policy.restricted_by({.read_only = true, .nosuid = true});
        cleanup.descriptor = vfs::vfs_alloc_fd(&task, file);
        KREQUIRE_TRUE(cleanup.descriptor >= 0);
        cleanup.file = nullptr;  // The descriptor now owns this reference.
        vfs::vfs_retain_file(file);
        cleanup.duplicate = vfs::vfs_alloc_fd(&task, file);
        if (cleanup.duplicate < 0) vfs::vfs_put_file(file);
        KREQUIRE_TRUE(cleanup.duplicate >= 0);
        KEXPECT_TRUE(vfs::vfs_get_file(&task, cleanup.duplicate) == file);
        cleanup.close_descriptor(cleanup.descriptor);
        auto const REFERENCES = file->refcount.load();
        auto const POSITION = file->pos;
        size_t actual = 12345;
        KEXPECT_EQ(vfs::vfs_write_file(file, EDIT.data(), EDIT.size(), &actual), static_cast<ssize_t>(-EROFS));
        KEXPECT_EQ(vfs::vfs_write_file(file, EDIT.data(), 0, &actual), static_cast<ssize_t>(-EROFS));
        KEXPECT_EQ(vfs::vfs_pwrite_file(file, EDIT.data(), EDIT.size(), 0), static_cast<ssize_t>(-EROFS));
        KEXPECT_EQ(vfs::vfs_pwrite_file(file, EDIT.data(), EDIT.size(), -1), static_cast<ssize_t>(-EINVAL));
        KEXPECT_EQ(vfs::vfs_ftruncate_file(file, 0), -EROFS);
        KEXPECT_EQ(vfs::vfs_ftruncate_file(file, -1), -EINVAL);
        KEXPECT_EQ(vfs::vfs_fchmod_file_as(file, ROOT, 0777), -EROFS);
        KEXPECT_EQ(vfs::vfs_fchown_file_as(file, ROOT, 1001, 2001), -EROFS);
        KEXPECT_EQ(vfs::vfs_futimens_file_as(file, ROOT, nullptr), -EROFS);
        KEXPECT_EQ(vfs::vfs_futimens_file_as(file, ROOT, OMIT.data()), 0);
        KEXPECT_EQ(vfs::vfs_fsetxattr_file(file, "user.retained", EDIT.data(), EDIT.size(), 0), -EROFS);
        KEXPECT_EQ(file->pos, POSITION);
        KEXPECT_EQ(actual, size_t{12345});
        KEXPECT_EQ(file->refcount.load(), REFERENCES);
        KREQUIRE_EQ(vfs::vfs_fstat_file(file, &after), 0);
        KEXPECT_EQ(after.st_ino, before.st_ino);
        KEXPECT_EQ(after.st_size, before.st_size);
        KEXPECT_EQ(after.st_mode, before.st_mode);
        KEXPECT_EQ(after.st_uid, before.st_uid);
        KEXPECT_EQ(after.st_gid, before.st_gid);
        KEXPECT_EQ(after.st_atim.tv_sec, before.st_atim.tv_sec);
        KEXPECT_EQ(after.st_atim.tv_nsec, before.st_atim.tv_nsec);
        KEXPECT_EQ(after.st_mtim.tv_sec, before.st_mtim.tv_sec);
        KEXPECT_EQ(after.st_mtim.tv_nsec, before.st_mtim.tv_nsec);
        KEXPECT_EQ(after.st_ctim.tv_sec, before.st_ctim.tv_sec);
        KEXPECT_EQ(after.st_ctim.tv_nsec, before.st_ctim.tv_nsec);
        vfs::Statvfs stats{};
        KREQUIRE_EQ(vfs::vfs_fstatvfs_file(file, &stats), 0);
        KEXPECT_EQ(stats.f_flag & (vfs::ST_RDONLY | vfs::ST_NOSUID), vfs::ST_RDONLY | vfs::ST_NOSUID);
        security::Snapshot after_exec;
        KREQUIRE_EQ(
            user.prepare_exec({.uid = 0, .gid = 0, .set_uid = true, .set_gid = true, .nosuid = vfs::file_mount_nosuid(*file), .mode = 0755},
                              after_exec),
            0);
        KEXPECT_EQ(after_exec.user().effective, 1001);
        KEXPECT_EQ(after_exec.user().saved, 1001);
        KEXPECT_EQ(after_exec.group().effective, 2001);
        KEXPECT_EQ(after_exec.group().saved, 2001);
        // An independent writable view still reaches the same owner object.
        cleanup.owner_view = vfs::vfs_open_file(PATH, vfs::O_NO_CACHE | 2, 0);
        KREQUIRE_TRUE(cleanup.owner_view != nullptr);
        KEXPECT_TRUE(!vfs::file_mount_read_only(*cleanup.owner_view) && !vfs::file_mount_nosuid(*cleanup.owner_view));
        std::array<char, DATA.size()> output{};
        KEXPECT_EQ(vfs::vfs_pread_file(cleanup.owner_view, output.data(), output.size(), 0), output.size());
        KEXPECT_TRUE(output == DATA);
        KEXPECT_EQ(vfs::vfs_write_file(cleanup.owner_view, EDIT.data(), EDIT.size(), nullptr), EDIT.size());
        KEXPECT_EQ(vfs::vfs_pread_file(file, output.data(), output.size(), 0), output.size());
        KEXPECT_TRUE(output == EDIT);
        cleanup.close_descriptor(cleanup.duplicate);
        int const CLOSE_RESULT = vfs::vfs_close_file(cleanup.owner_view);
        cleanup.owner_view = nullptr;
        KREQUIRE_EQ(CLOSE_RESULT, 0);
        KEXPECT_EQ(vfs::vfs_unlink(PATH), 0);
        if (cleanup.mounted) {
            int const UNMOUNT_RESULT = vfs::unmount_filesystem(BASE);
            cleanup.mounted = UNMOUNT_RESULT != 0;
            KREQUIRE_EQ(UNMOUNT_RESULT, 0);
        }
        KEXPECT_EQ(vfs::vfs_rmdir(BASE), 0);
    }
}

KTEST(Credentials, CommonWalkChecksOriginalComponentsAndRetainsRootedAnchors) {
    namespace vfs = ker::vfs;
    namespace tmpfs = ker::vfs::tmpfs;
    constexpr const char* BASE = "/tmp/ktest_component_search";
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
    for (const char* path :
         {"/tmp/ktest_component_search/locked", "/tmp/ktest_component_search/locked/child", "/tmp/ktest_component_search/public"})
        KREQUIRE_EQ(vfs::vfs_mkdir(path, 0755), 0);
    for (const char* path : {"/tmp/ktest_component_search/locked/child/file", "/tmp/ktest_component_search/public/file"}) {
        auto* file = vfs::vfs_open_file(path, vfs::O_CREAT | vfs::O_EXCL | 2, 0644);
        KREQUIRE_TRUE(file != nullptr);
        KREQUIRE_EQ(vfs::vfs_close_file(file), 0);
    }
    KREQUIRE_EQ(vfs::vfs_symlink("/public/file", "/tmp/ktest_component_search/locked/escape"), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("../locked/child/file", "/tmp/ktest_component_search/public/hidden"), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("/../../public/file", "/tmp/ktest_component_search/public/rooted"), 0);
    KREQUIRE_EQ(vfs::vfs_chmod("/tmp/ktest_component_search/locked", 0700), 0);
    ker::mod::sched::task::Task task{};
    task.pid = UINT64_MAX;
    std::memcpy(task.root.data(), BASE, std::strlen(BASE) + 1);
    task.root_len = static_cast<uint16_t>(std::strlen(BASE));
    std::memcpy(task.cwd.data(), "/", 2);
    task.cwd_len = 1;
    int const ANCHOR = vfs::vfs_openat(&task, vfs::AT_FDCWD, "/locked/child", vfs::O_DIRECTORY, 0);
    KREQUIRE_TRUE(ANCHOR >= 0);
    auto caller = security::Snapshot::bootstrap_root();
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(task, caller), 0);
    auto const GENERATION = tmpfs::tmpfs_namespace_generation();
    for (const char* path :
         {"/locked/child/file", "/locked/../public/file", "/locked/escape", "/public/hidden", "/locked/./child/file", "/locked/missing"}) {
        vfs::Stat stat{};
        stat.st_ino = 0xCAFE;
        KEXPECT_EQ(vfs::vfs_statat(&task, vfs::AT_FDCWD, path, 0, &stat), -EACCES);
        KEXPECT_EQ(stat.st_ino, 0xCAFEU);
        KEXPECT_EQ(vfs::vfs_openat(&task, vfs::AT_FDCWD, path, 0, 0), -EACCES);
    }
    KEXPECT_EQ(tmpfs::tmpfs_namespace_generation(), GENERATION);
    vfs::Stat file{}, observed{};
    KREQUIRE_EQ(vfs::vfs_statat(&task, ANCHOR, "file", 0, &file), 0);
    for (const char* path : {"file", "./file", ".//./file"}) {
        KEXPECT_EQ(vfs::vfs_statat(&task, ANCHOR, path, 0, &observed), 0);
        KEXPECT_EQ(observed.st_ino, file.st_ino);
    }
    KEXPECT_EQ(vfs::vfs_statat(&task, ANCHOR, ".", 0, &observed), 0);
    KEXPECT_TRUE((observed.st_mode & vfs::S_IFMT) == vfs::S_IFDIR);
    KEXPECT_EQ(vfs::vfs_statat(&task, ANCHOR, "..", 0, &observed), 0);
    KEXPECT_EQ(vfs::vfs_statat(&task, ANCHOR, "../child/file", 0, &observed), -EACCES);
    KEXPECT_EQ(vfs::vfs_statat(&task, vfs::AT_FDCWD, "/public/rooted", 0, &observed), 0);
    KEXPECT_EQ(vfs::vfs_statat(&task, vfs::AT_FDCWD, "/../../public/file", 0, &file), 0);
    KEXPECT_EQ(observed.st_ino, file.st_ino);
    std::memcpy(task.cwd.data(), "/locked/child", sizeof("/locked/child"));
    task.cwd_len = sizeof("/locked/child") - 1;
    KEXPECT_EQ(vfs::vfs_statat(&task, vfs::AT_FDCWD, "./file", 0, &observed), 0);
    KEXPECT_EQ(vfs::vfs_statat(&task, vfs::AT_FDCWD, "../child/file", 0, &observed), -EACCES);
    auto* anchor_file = vfs::vfs_get_file(&task, ANCHOR);
    KREQUIRE_TRUE(anchor_file != nullptr && anchor_file->mount_owner != nullptr);
    auto* anchor_mount = anchor_file->mount_owner;
    uint32_t const BUSY_BEFORE = anchor_mount->open_files.load(std::memory_order_acquire);
    KEXPECT_EQ(vfs::vfs_fchdir(&task, ANCHOR), 0);
    KEXPECT_EQ(anchor_mount->open_files.load(std::memory_order_acquire), BUSY_BEFORE + 1);
    KEXPECT_EQ(vfs::vfs_fchdir(&task, vfs::AT_FDCWD), -EBADF);
    KEXPECT_EQ(vfs::vfs_unlinkat(&task, ANCHOR, ".", vfs::AT_REMOVEDIR), -EINVAL);
    KREQUIRE_EQ(vfs::vfs_chmod("/tmp/ktest_component_search/locked/child", 0700), 0);
    KEXPECT_EQ(vfs::vfs_fchdir(&task, ANCHOR), -EACCES);
    KEXPECT_TRUE(std::strcmp(task.cwd.data(), "/locked/child") == 0);
    KREQUIRE_EQ(vfs::vfs_chmod("/tmp/ktest_component_search/locked/child", 0755), 0);
    KEXPECT_EQ(anchor_mount->open_files.load(std::memory_order_acquire), BUSY_BEFORE + 1);
    vfs::DirectoryAnchorRef copied_anchor = task.cwd_anchor;
    task.cwd_anchor = {};
    KEXPECT_EQ(anchor_mount->open_files.load(std::memory_order_acquire), BUSY_BEFORE + 1);
    copied_anchor = {};
    KEXPECT_EQ(anchor_mount->open_files.load(std::memory_order_acquire), BUSY_BEFORE);
    auto* retained = vfs::vfs_get_file(&task, ANCHOR);
    KREQUIRE_TRUE(retained != nullptr);
    KEXPECT_EQ(vfs::vfs_release_fd(&task, ANCHOR), 0);
    vfs::vfs_put_file(retained);
    for (const char* path : {"/tmp/ktest_component_search/locked/escape", "/tmp/ktest_component_search/public/hidden",
                             "/tmp/ktest_component_search/public/rooted", "/tmp/ktest_component_search/locked/child/file",
                             "/tmp/ktest_component_search/public/file"})
        KEXPECT_EQ(vfs::vfs_unlink(path), 0);
    for (const char* path :
         {"/tmp/ktest_component_search/locked/child", "/tmp/ktest_component_search/locked", "/tmp/ktest_component_search/public", BASE})
        KEXPECT_EQ(vfs::vfs_rmdir(path), 0);
}

KTEST(Credentials, DevfsHybridNamespaceKeepsDeviceAuthoritySeparate) {
    namespace devfs = ker::vfs::devfs;
    devfs::DevFSNode node{};
    node.type = devfs::DevFSNodeType::DEVICE;
    node.mode = 0000;
    auto caller = security::Snapshot::bootstrap_root();
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    KEXPECT_EQ(devfs::devfs_authorize_search_node(&node, caller), -ENOTDIR);
    node.child_namespace = true;
    KEXPECT_EQ(devfs::devfs_authorize_search_node(&node, caller), 0);
    ker::vfs::Stat stat{};
    KEXPECT_EQ(devfs::devfs_stat_node(&node, 1, &stat), 0);
    KEXPECT_EQ(stat.st_mode & 07777U, 0U);
    KEXPECT_EQ(security::authorize_file_access(caller, {stat.st_uid, stat.st_gid, stat.st_mode}, 4), -EACCES);
    node.type = devfs::DevFSNodeType::DIRECTORY;
    node.mode = 0700;
    KEXPECT_EQ(devfs::devfs_authorize_search_node(&node, caller), -EACCES);
    KEXPECT_EQ(devfs::devfs_authorize_search_node(&node, security::Snapshot::bootstrap_root()), 0);
    node.type = devfs::DevFSNodeType::SYMLINK;
    KEXPECT_EQ(devfs::devfs_authorize_search_node(&node, caller), -ENOTDIR);
}

KTEST(Credentials, NetworkAdministrationRequiresEffectiveRootAndItsOwnAuthority) {
    auto const ROOT = security::Snapshot::bootstrap_root();
    KEXPECT_EQ(security::authorize_network_administration(ROOT), 0);
    KEXPECT_EQ(security::authorize_network_administration(security::Snapshot{}), -EPERM);
    auto caller = ROOT;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_EUID, .effective = 1000}, caller), 0);
    KEXPECT_EQ(security::authorize_network_administration(caller), -EPERM);
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_EUID, .effective = 0}, caller), 0);
    constexpr auto BIT = uint64_t{1} << static_cast<unsigned>(security::Authority::NETWORK_ADMIN);
    KREQUIRE_EQ(caller.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = security::ALL_AUTHORITY & ~BIT}, caller),
                0);
    KEXPECT_EQ(security::authorize_network_administration(caller), -EPERM);
    caller = ROOT;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = BIT}, caller), 0);
    KEXPECT_EQ(security::authorize_network_administration(caller), 0);
}

KTEST(Credentials, ServicePrivilegeAssignmentIsBoundedAuthorizedAndSealed) {
    auto root = security::Snapshot::bootstrap_root();
    security::Snapshot next;
    KEXPECT_EQ(root.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 1}, next), -EPERM);
    KEXPECT_TRUE(next == root);
    KREQUIRE_EQ(root.prepare({.change = security::Change::NO_NEW_PRIVILEGES}, root), 0);
    for (auto invalid : std::array<uint64_t, 3>{4, 256, UINT64_MAX}) {
        KEXPECT_EQ(root.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = invalid}, next), -EINVAL);
        KEXPECT_TRUE(next == root);
    }
    for (auto authority : {security::Authority::INIT_CONTROL, security::Authority::KERNEL_DEBUG, security::Authority::NETWORK_ADMIN}) {
        auto restricted = root;
        KREQUIRE_EQ(restricted.prepare({.change = security::Change::DROP_AUTHORITY,
                                        .authority_ceiling = security::ALL_AUTHORITY & ~(uint64_t{1} << static_cast<unsigned>(authority))},
                                       restricted),
                    0);
        KEXPECT_EQ(restricted.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, next), -EPERM);
        KEXPECT_TRUE(next == restricted);
    }
    auto nonroot = root;
    KREQUIRE_EQ(nonroot.prepare({.change = security::Change::SET_UID, .real = 1000}, nonroot), 0);
    KEXPECT_EQ(nonroot.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 1}, next), -EPERM);
    KEXPECT_TRUE(next == nonroot);
    KREQUIRE_EQ(nonroot.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES}, nonroot), 0);
    KEXPECT_TRUE(nonroot.service_privileges_locked());
    KREQUIRE_EQ(root.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, root), 0);
    KEXPECT_TRUE(root.service_privileges_locked());
    KEXPECT_FALSE(root.dumpable());
    KREQUIRE_EQ(root.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, root), 0);
    KREQUIRE_EQ(root.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 1}, root), 0);
    KEXPECT_EQ(root.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, next), -EPERM);
    KEXPECT_TRUE(next == root);
    KREQUIRE_EQ(root.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES}, root), 0);
    KEXPECT_EQ(root.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 1}, next), -EPERM);
    KEXPECT_TRUE(next == root);
}

KTEST(Credentials, ServicePrivilegesSurviveIdentityAndExecButNeverBroadenOperations) {
    for (uint64_t mask : {1U, 2U, 3U}) {
        auto caller = security::Snapshot::bootstrap_root();
        KREQUIRE_EQ(caller.prepare({.change = security::Change::NO_NEW_PRIVILEGES}, caller), 0);
        KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = mask}, caller), 0);
        KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_GID, .real = 1000}, caller), 0);
        KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
        auto const CEILING = ker::abi::process::service_privilege_ceiling(mask);
        KREQUIRE_EQ(caller.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = CEILING}, caller), 0);
        KEXPECT_EQ(security::authorize_journal_read(caller), (mask & 1) != 0 ? 0 : -EACCES);
        KEXPECT_EQ(security::authorize_network_administration(caller), (mask & 2) != 0 ? 0 : -EPERM);
        KEXPECT_EQ(security::authorize_diagnostic(caller, security::DiagnosticAccess::KERNEL_READ, false), -EACCES);
        KEXPECT_EQ(security::authorize_mount_administration(caller), -EPERM);
        KEXPECT_EQ(security::authorize_legacy_remote_exec(caller), -ENOTSUP);
        for (unsigned bit = 0; bit < 256; ++bit) {
            KEXPECT_FALSE(caller.allows(static_cast<security::Authority>(bit)));
            KEXPECT_EQ(caller.allows_service(static_cast<security::ServicePrivilege>(bit)), bit < 2 && (mask & (uint64_t{1} << bit)) != 0);
        }
        auto const CHILD = caller;
        security::Snapshot executed;
        KREQUIRE_EQ(CHILD.prepare_exec({.uid = 0, .gid = 0, .set_uid = true, .set_gid = true, .mode = 0755}, executed), 0);
        KEXPECT_TRUE(executed.user() == caller.user());
        KEXPECT_TRUE(executed.group() == caller.group());
        KEXPECT_EQ(executed.service_privileges(), mask);
        KEXPECT_TRUE(executed.service_privileges_locked());
        KEXPECT_EQ(executed.effective_service_privileges(), mask);
        security::Snapshot denied;
        KEXPECT_EQ(executed.prepare({.change = security::Change::SET_UID, .real = 0}, denied), -EPERM);
        KEXPECT_TRUE(denied == executed);
    }
}

KTEST(Credentials, CeilingDropPermanentlyDisablesDelegatedPermission) {
    auto caller = security::Snapshot::bootstrap_root();
    KREQUIRE_EQ(caller.prepare({.change = security::Change::NO_NEW_PRIVILEGES}, caller), 0);
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, caller), 0);
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    KREQUIRE_EQ(
        caller.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = ker::abi::process::service_privilege_ceiling(1)},
                       caller),
        0);
    KEXPECT_EQ(security::authorize_network_administration(caller), -EPERM);
    KEXPECT_EQ(security::authorize_journal_read(caller), 0);
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, caller), 0);
    KEXPECT_EQ(security::authorize_network_administration(caller), -EPERM);
    KREQUIRE_EQ(caller.prepare({.change = security::Change::DROP_AUTHORITY}, caller), 0);
    KEXPECT_EQ(security::authorize_journal_read(caller), -EACCES);
    KEXPECT_EQ(caller.effective_service_privileges(), 0U);
    security::Snapshot denied;
    KEXPECT_EQ(caller.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = security::ALL_AUTHORITY}, denied), -EPERM);
    KEXPECT_TRUE(denied == caller);
}

KTEST(Credentials, TraceAndPrivateDescriptorsCannotRecoverServicePrivilegesOrInstaller) {
    auto base = security::Snapshot::bootstrap_root();
    KREQUIRE_EQ(base.prepare({.change = security::Change::NO_NEW_PRIVILEGES}, base), 0);
    auto installer = base;
    auto target = base;
    KREQUIRE_EQ(target.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, target), 0);
    auto caller = target;
    KEXPECT_FALSE(security::may_trace(caller, installer));
    KEXPECT_FALSE(security::may_access_process_private(caller, installer, false));
    KEXPECT_TRUE(security::may_trace(installer, caller));
    KREQUIRE_EQ(target.prepare({.change = security::Change::SET_UID, .real = 1000}, target), 0);
    KREQUIRE_EQ(target.prepare({.change = security::Change::SET_DUMPABLE, .dumpable = true}, target), 0);
    caller = target;
    KEXPECT_TRUE(security::may_trace(caller, target));
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 1}, caller), 0);
    KEXPECT_FALSE(security::may_trace(caller, target));
    KEXPECT_FALSE(security::may_access_process_private(caller, target, false));
    KEXPECT_TRUE(security::may_access_process_private(caller, target, true));
    KREQUIRE_EQ(target.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 1}, target), 0);
    KREQUIRE_EQ(target.prepare({.change = security::Change::SET_DUMPABLE, .dumpable = true}, target), 0);
    KEXPECT_TRUE(security::may_trace(caller, target));
}

KTEST(WkiAuthPolicy, OrdinaryOpenAndDescriptorAccessRequireSeparateGrants) {
    namespace wki = ker::net::wki;
    namespace policy = wki::auth_policy;
    constexpr auto READ = wki::wki_policy_bits(wki::WkiPolicy::VFS_READ);
    constexpr auto WRITE = wki::wki_policy_bits(wki::WkiPolicy::VFS_WRITE);
    constexpr auto IDENTITY = wki::wki_policy_bits(wki::WkiPolicy::IDENTITY_SAME_DOMAIN);
    constexpr auto ROOT_TRUST = wki::wki_policy_bits(wki::WkiPolicy::IDENTITY_ROOT);
    KEXPECT_EQ(policy::vfs_open_permissions(0), READ | IDENTITY);
    KEXPECT_EQ(policy::vfs_open_permissions(1), WRITE | IDENTITY);
    KEXPECT_EQ(policy::vfs_open_permissions(2), READ | WRITE | IDENTITY);
    KEXPECT_EQ(policy::vfs_open_permissions(ker::vfs::O_CREAT), READ | WRITE | IDENTITY);
    KEXPECT_EQ(policy::vfs_open_permissions(ker::vfs::O_TRUNC), READ | WRITE | IDENTITY);
    KEXPECT_EQ(policy::vfs_open_permissions(3), policy::INVALID);
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto user = ROOT;
    std::array<uint32_t, 3> groups{7, 8, 9};
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GROUPS, .groups = groups}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GID, .real = 2000}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::NO_NEW_PRIVILEGES}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, user), 0);
    auto mapped = ROOT;
    KEXPECT_EQ(policy::map_vfs_open_identity(READ | WRITE, user, 0, mapped), -EACCES);
    KEXPECT_TRUE(mapped == ROOT);
    KEXPECT_EQ(policy::map_vfs_open_identity(READ | IDENTITY, user, ker::vfs::O_TRUNC, mapped), -EACCES);
    KEXPECT_TRUE(mapped == ROOT);
    KREQUIRE_EQ(policy::map_vfs_open_identity(READ | WRITE | IDENTITY, user, 2, mapped), 0);
    KEXPECT_TRUE(mapped == user);
    KEXPECT_EQ(policy::map_vfs_open_identity(READ | WRITE | IDENTITY, ROOT, 2, mapped), -EACCES);
    KEXPECT_TRUE(mapped == user);
    KREQUIRE_EQ(policy::map_vfs_open_identity(READ | WRITE | IDENTITY | ROOT_TRUST, ROOT, 2, mapped), 0);
    KEXPECT_TRUE(mapped == ROOT);
    KEXPECT_EQ(security::authorize_file_description_access(0, false), 0);
    KEXPECT_EQ(security::authorize_file_description_access(0, true), -EBADF);
    KEXPECT_EQ(security::authorize_file_description_access(1, false), -EBADF);
    KEXPECT_EQ(security::authorize_file_description_access(1, true), 0);
    KEXPECT_EQ(security::authorize_file_description_access(2, false), 0);
    KEXPECT_EQ(security::authorize_file_description_access(2, true), 0);
    KEXPECT_EQ(security::authorize_file_description_access(3, false), -EBADF);
    KEXPECT_EQ(security::authorize_file_description_access(3, true), -EBADF);
}

KTEST(WkiAuthPolicy, ComputeIdentityNeedsExplicitTrustAndRootPermissionForEveryIdSlot) {
    namespace security = ker::mod::security;
    constexpr uint64_t SAME = ker::net::wki::wki_policy_bits(ker::net::wki::WkiPolicy::COMPUTE) |
                              ker::net::wki::wki_policy_bits(ker::net::wki::WkiPolicy::IDENTITY_SAME_DOMAIN);
    constexpr uint64_t ROOT = SAME | ker::net::wki::wki_policy_bits(ker::net::wki::WkiPolicy::IDENTITY_ROOT);
    for (unsigned mask = 0; mask < 64; ++mask) {
        std::array<uint32_t, 6> ids{};
        for (unsigned slot = 0; slot < ids.size(); ++slot) ids[slot] = (mask & (1U << slot)) != 0 ? 1000 + slot : 0;
        security::Snapshot supplied;
        KREQUIRE_EQ(security::restore_legacy_remote_exec_identity({ids[0], ids[1], ids[2]}, {ids[3], ids[4], ids[5]}, supplied), 0);
        for (uint64_t bits : {uint64_t{0}, uint64_t{1}, uint64_t{1023}, uint64_t{1024}, uint64_t{2048}, uint64_t{2049}, SAME, ROOT,
                              ROOT | (uint64_t{1} << 63)}) {
            security::Snapshot mapped;
            auto const BEFORE = mapped;
            bool const ALLOWED = bits == ROOT || (bits == SAME && mask == 63);
            KEXPECT_EQ(ker::net::wki::auth_policy::map_compute_identity(bits, supplied, mapped), ALLOWED ? 0 : -EACCES);
            KEXPECT_TRUE(mapped == (ALLOWED ? supplied : BEFORE));
        }
    }
}

KTEST(WkiAuthPolicy, MappingPreservesDroppedAuthorityAndRejectsSupplementaryRootGroup) {
    namespace security = ker::mod::security;
    constexpr uint64_t SAME = ker::net::wki::wki_policy_bits(ker::net::wki::WkiPolicy::COMPUTE) |
                              ker::net::wki::wki_policy_bits(ker::net::wki::WkiPolicy::IDENTITY_SAME_DOMAIN);
    auto supplied = security::Snapshot::bootstrap_root();
    constexpr std::array<uint32_t, 2> GROUPS{3000, 3001};
    KREQUIRE_EQ(supplied.prepare({.change = security::Change::SET_GROUPS, .groups = GROUPS}, supplied), 0);
    KREQUIRE_EQ(supplied.prepare({.change = security::Change::NO_NEW_PRIVILEGES}, supplied), 0);
    KREQUIRE_EQ(supplied.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, supplied), 0);
    KREQUIRE_EQ(supplied.prepare({.change = security::Change::SET_GID, .real = 2000}, supplied), 0);
    KREQUIRE_EQ(supplied.prepare({.change = security::Change::SET_UID, .real = 1000}, supplied), 0);
    KREQUIRE_EQ(supplied.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, supplied), 0);
    security::Snapshot mapped;
    KREQUIRE_EQ(ker::net::wki::auth_policy::map_compute_identity(SAME, supplied, mapped), 0);
    KEXPECT_TRUE(mapped == supplied);
    KEXPECT_TRUE(mapped.no_new_privileges());
    KEXPECT_TRUE(mapped.service_privileges_locked());
    KEXPECT_FALSE(mapped.dumpable());
    KEXPECT_EQ(mapped.service_privileges(), 3U);
    KEXPECT_EQ(mapped.authority_ceiling(), 0U);
    supplied = security::Snapshot::bootstrap_root();
    constexpr std::array<uint32_t, 1> ROOT_GROUP{0};
    KREQUIRE_EQ(supplied.prepare({.change = security::Change::SET_GROUPS, .groups = ROOT_GROUP}, supplied), 0);
    KREQUIRE_EQ(supplied.prepare({.change = security::Change::SET_GID, .real = 2000}, supplied), 0);
    KREQUIRE_EQ(supplied.prepare({.change = security::Change::SET_UID, .real = 1000}, supplied), 0);
    auto const BEFORE = mapped;
    KEXPECT_EQ(ker::net::wki::auth_policy::map_compute_identity(SAME, supplied, mapped), -EACCES);
    KEXPECT_TRUE(mapped == BEFORE);
    KEXPECT_EQ(ker::net::wki::auth_policy::map_compute_identity(
                   SAME | ker::net::wki::wki_policy_bits(ker::net::wki::WkiPolicy::IDENTITY_ROOT), supplied, mapped),
               0);
    KEXPECT_TRUE(mapped == supplied);
}

KTEST(Credentials, ExplicitReadAuthorityDoesNotBorrowWorkerCredentials) {
    namespace vfs = ker::vfs;
    constexpr const char* DIR = "/tmp/ktest_read_authority";
    constexpr const char* PATH = "/tmp/ktest_read_authority/file";
    constexpr const char* LINK = "/tmp/ktest_read_authority/link";
    struct Cleanup {
        vfs::File* source = nullptr;
        vfs::File* opened = nullptr;
        ~Cleanup() {
            if (opened != nullptr && opened != source) vfs::vfs_close_file(opened);
            if (source != nullptr) vfs::vfs_close_file(source);
            vfs::vfs_unlink("/tmp/ktest_read_authority/link");
            vfs::vfs_unlink("/tmp/ktest_read_authority/file");
            vfs::vfs_rmdir("/tmp/ktest_read_authority");
        }
    } cleanup;
    vfs::vfs_mkdir("/tmp", 0755);
    KREQUIRE_EQ(vfs::vfs_mkdir(DIR, 0755), 0);
    cleanup.source = vfs::vfs_open_file(PATH, vfs::O_CREAT | vfs::O_EXCL | 2, 0600);
    KREQUIRE_TRUE(cleanup.source != nullptr);
    std::array<char, 4> const BYTES{'r', 'e', 'a', 'd'};
    KREQUIRE_EQ(vfs::tmpfs::tmpfs_write(cleanup.source, BYTES.data(), BYTES.size(), 0), BYTES.size());
    ker::mod::sched::task::Task worker{};
    std::strcpy(worker.root.data(), "/");
    std::strcpy(worker.cwd.data(), "/");
    auto const WORKER_BEFORE = worker.credentials();
    auto user = security::Snapshot::bootstrap_root();
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GID, .real = 2000}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    auto const USER_BEFORE = user;
    cleanup.opened = cleanup.source;
    KEXPECT_EQ(vfs::vfs_open_read_file_as(worker, user, PATH, false, &cleanup.opened), -EACCES);
    KEXPECT_TRUE(cleanup.opened == cleanup.source);
    KEXPECT_TRUE(worker.credentials() == WORKER_BEFORE && user == USER_BEFORE);
    KREQUIRE_EQ(vfs::vfs_open_read_file_as(worker, WORKER_BEFORE, PATH, false, &cleanup.opened), 0);
    KREQUIRE_TRUE(cleanup.opened != nullptr && cleanup.opened != cleanup.source);
    KEXPECT_EQ(vfs::vfs_close_file(cleanup.opened), 0);
    cleanup.opened = cleanup.source;

    // A supplementary owner-group selects the denied group class, even though
    // the other class permits reading. Never lose groups while mapping reads.
    KREQUIRE_EQ(vfs::vfs_chmod(PATH, 0604), 0);
    auto grouped = security::Snapshot::bootstrap_root();
    std::array<uint32_t, 1> const GROUPS{0};
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_GROUPS, .groups = GROUPS}, grouped), 0);
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_GID, .real = 2000}, grouped), 0);
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_UID, .real = 1000}, grouped), 0);
    KEXPECT_EQ(vfs::vfs_open_read_file_as(worker, grouped, PATH, false, &cleanup.opened), -EACCES);
    KEXPECT_TRUE(cleanup.opened == cleanup.source);
    KREQUIRE_EQ(vfs::vfs_symlink(PATH, LINK), 0);
    KREQUIRE_EQ(vfs::vfs_open_read_file_as(worker, user, LINK, false, &cleanup.opened), 0);
    std::array<char, 4> read{};
    KEXPECT_EQ(vfs::tmpfs::tmpfs_read(cleanup.opened, read.data(), read.size(), 0), read.size());
    KEXPECT_TRUE(read == BYTES);
    KEXPECT_EQ(vfs::vfs_close_file(cleanup.opened), 0);
    cleanup.opened = cleanup.source;
    KREQUIRE_EQ(vfs::vfs_chmod(DIR, 0700), 0);
    KEXPECT_EQ(vfs::vfs_open_read_file_as(worker, user, LINK, false, &cleanup.opened), -EACCES);
    KEXPECT_TRUE(cleanup.opened == cleanup.source);
    KEXPECT_EQ(vfs::vfs_open_read_file_as(worker, WORKER_BEFORE, DIR, false, &cleanup.opened), -EISDIR);
    KEXPECT_EQ(vfs::vfs_open_read_file_as(worker, WORKER_BEFORE, "/dev/null", false, &cleanup.opened), -ENOTSUP);
    KEXPECT_TRUE(cleanup.opened == cleanup.source);
    KEXPECT_TRUE(worker.credentials() == WORKER_BEFORE && user == USER_BEFORE);
}

KTEST(Credentials, DirectExecInputUsesExplicitSnapshotWithoutDescriptors) {
    namespace vfs = ker::vfs;
    constexpr const char* DIR = "/tmp/ktest_direct_exec";
    constexpr const char* PATH = "/tmp/ktest_direct_exec/image";
    struct Cleanup {
        vfs::File* file{};
        ~Cleanup() {
            if (file) vfs::vfs_put_file(file);
            vfs::vfs_unlink("/tmp/ktest_direct_exec/image");
            vfs::vfs_rmdir("/tmp/ktest_direct_exec/private");
            vfs::vfs_rmdir("/tmp/ktest_direct_exec");
        }
    } cleanup;
    vfs::vfs_mkdir("/tmp", 0755);
    KREQUIRE_EQ(vfs::vfs_mkdir(DIR, 0755), 0);
    KREQUIRE_EQ(vfs::vfs_mkdir("/tmp/ktest_direct_exec/private", 0700), 0);
    cleanup.file = vfs::vfs_open_file(PATH, vfs::O_CREAT | vfs::O_EXCL | 2, 0600);
    KREQUIRE_TRUE(cleanup.file != nullptr);
    ker::mod::sched::task::Task worker{};
    std::strcpy(worker.root.data(), "/");
    std::strcpy(worker.cwd.data(), "/");
    auto const ROOT = worker.credentials();
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    auto* output = cleanup.file;
    KEXPECT_EQ(vfs::vfs_open_file_as(worker, user, PATH, vfs::O_LOCAL, &output), -EACCES);
    KEXPECT_TRUE(output == cleanup.file && worker.fd_table.empty());
    KREQUIRE_EQ(vfs::vfs_chmod(PATH, 0755), 0);
    KEXPECT_EQ(vfs::vfs_open_file_as(worker, user, "/tmp/ktest_direct_exec/private/../image", vfs::O_LOCAL, &output), -EACCES);
    KEXPECT_TRUE(output == cleanup.file && worker.fd_table.empty());
    KEXPECT_EQ(vfs::vfs_open_file_as(worker, ROOT, PATH, vfs::O_TRUNC, &output), -EINVAL);
    KEXPECT_TRUE(output == cleanup.file);
    KREQUIRE_EQ(vfs::vfs_open_file_as(worker, user, PATH, vfs::O_LOCAL, &output), 0);
    KEXPECT_TRUE(output != nullptr && output != cleanup.file && worker.fd_table.empty());
    KEXPECT_EQ(worker.fd_table.find_first_unset_below(0, 1), 0);
    KEXPECT_TRUE(worker.credentials() == ROOT);
    vfs::vfs_put_file(output);
}

KTEST(Credentials, FullRecordPreservesRestrictionsAndRejectsMalformedState) {
    namespace record = security::credential_record;
    auto original = security::Snapshot::bootstrap_root();
    std::array<uint32_t, security::MAX_GROUPS> groups{11, 22, 33, 44, 55, 66, 77, UINT32_MAX - 1};
    KREQUIRE_EQ(original.prepare({.change = security::Change::SET_GROUPS, .groups = groups}, original), 0);
    KREQUIRE_EQ(original.prepare({.change = security::Change::NO_NEW_PRIVILEGES}, original), 0);
    KREQUIRE_EQ(original.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, original), 0);
    KREQUIRE_EQ(original.prepare({.change = security::Change::SET_GID, .real = 2000}, original), 0);
    KREQUIRE_EQ(original.prepare({.change = security::Change::SET_UID, .real = 1000}, original), 0);
    KREQUIRE_EQ(original.prepare({.change = security::Change::DROP_AUTHORITY}, original), 0);
    auto const BYTES = record::encode(original);
    security::Snapshot decoded;
    KREQUIRE_EQ(record::decode(BYTES, decoded), 0);
    KEXPECT_TRUE(decoded == original);
    KEXPECT_EQ(decoded.prepare({.change = security::Change::SET_EUID, .effective = 0}, decoded), -EPERM);
    KEXPECT_EQ(security::authorize_journal_read(decoded), -EACCES);
    KEXPECT_EQ(security::authorize_network_administration(decoded), -EPERM);
    for (size_t length = 0; length < BYTES.size(); ++length) {
        KEXPECT_EQ(record::decode({BYTES.data(), length}, decoded), -EINVAL);
        KEXPECT_TRUE(decoded == original);
    }
    auto malformed = BYTES;
    malformed[5] = 9;
    KEXPECT_EQ(record::decode(malformed, decoded), -EINVAL);
    malformed = BYTES;
    malformed[4] &= ~record::NO_NEW_PRIVILEGES;
    KEXPECT_EQ(record::decode(malformed, decoded), -EINVAL);
    malformed = BYTES;
    malformed[6] = 1;
    KEXPECT_EQ(record::decode(malformed, decoded), -EINVAL);
    KEXPECT_TRUE(decoded == original);
    KEXPECT_EQ(security::map_remote_identity(decoded, security::RemoteIdentityTrust::DENY, decoded), -EACCES);
    KEXPECT_TRUE(decoded == original);
}

KTEST(Credentials, ExplicitOwnerOpenCreatesAndTruncatesOnlyWithAdmittedAuthority) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_owner_open";
    constexpr auto FILE = "/tmp/ktest_owner_open/file";
    constexpr auto LINK = "/tmp/ktest_owner_open/link";
    constexpr auto DENIED = "/tmp/ktest_owner_open/denied";
    struct Cleanup {
        vfs::File* opened{};
        std::array<char, 512> long_path{};
        ~Cleanup() {
            if (opened != nullptr) static_cast<void>(vfs::vfs_close_file(opened));
            if (long_path[0] != 0) static_cast<void>(vfs::vfs_unlink(long_path.data()));
            static_cast<void>(vfs::vfs_unlink(LINK));
            static_cast<void>(vfs::vfs_unlink(FILE));
            static_cast<void>(vfs::vfs_unlink(DENIED));
            static_cast<void>(vfs::vfs_rmdir(BASE));
        }
    } cleanup;
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
    ker::mod::sched::task::Task worker{};
    auto const WORKER = worker.credentials();
    auto user = WORKER;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GID, .real = 2000}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    auto other = WORKER;
    KREQUIRE_EQ(other.prepare({.change = security::Change::SET_UID, .real = 1001}, other), 0);
    auto open = [&](const security::Snapshot& caller, const char* path, int flags, int mode = 0600) {
        return vfs::vfs_open_file_resolved_beneath_as(worker, caller, BASE, path, flags, mode, &cleanup.opened);
    };
    auto close = [&] {
        int const result = vfs::vfs_close_file(cleanup.opened);
        cleanup.opened = nullptr;
        return result;
    };
    auto const INITIAL_GENERATION = vfs::tmpfs::tmpfs_namespace_generation();
    KEXPECT_EQ(open(user, DENIED, vfs::O_CREAT | 1), -EACCES);
    KEXPECT_TRUE(cleanup.opened == nullptr);
    KEXPECT_EQ(vfs::tmpfs::tmpfs_namespace_generation(), INITIAL_GENERATION);
    vfs::Stat stat{};
    KEXPECT_EQ(vfs::vfs_stat(DENIED, &stat), -ENOENT);
    KREQUIRE_EQ(vfs::vfs_chmod(BASE, 0777), 0);
    KREQUIRE_EQ(open(user, FILE, vfs::O_CREAT | vfs::O_EXCL | 2, 0660), 0);
    KEXPECT_EQ(cleanup.opened->fops->vfs_write(cleanup.opened, "secret", 6, 0), 6);
    KEXPECT_EQ(vfs::vfs_fstat_file(cleanup.opened, &stat), 0);
    KEXPECT_EQ(stat.st_uid, 1000);
    KEXPECT_EQ(stat.st_gid, 2000);
    KEXPECT_EQ(stat.st_mode & 0777, 0660);
    KEXPECT_EQ(close(), 0);
    auto const GENERATION = vfs::tmpfs::tmpfs_namespace_generation();
    KEXPECT_EQ(open(other, FILE, vfs::O_TRUNC | 1), -EACCES);
    KEXPECT_EQ(open(other, FILE, 0), -EACCES);
    KEXPECT_EQ(open(user, FILE, vfs::O_CREAT | vfs::O_EXCL | 1), -EEXIST);
    KEXPECT_EQ(open(user, DENIED, vfs::O_CREAT | vfs::O_DIRECTORY), -EINVAL);
    KEXPECT_EQ(open(user, DENIED, vfs::O_CREAT | 3), -EINVAL);
    KEXPECT_EQ(open(user, DENIED, vfs::O_CREAT | vfs::O_WOS_KNOWN_ABSENT), -EINVAL);
    KEXPECT_TRUE(cleanup.opened == nullptr);
    KEXPECT_EQ(vfs::tmpfs::tmpfs_namespace_generation(), GENERATION);
    KREQUIRE_EQ(open(user, FILE, 0), 0);
    std::array<char, 8> bytes{};
    KEXPECT_EQ(cleanup.opened->fops->vfs_read(cleanup.opened, bytes.data(), bytes.size(), 0), 6);
    KEXPECT_TRUE(std::memcmp(bytes.data(), "secret", 6) == 0);
    KEXPECT_EQ(close(), 0);
    auto grouped = WORKER;
    std::array<uint32_t, 2> groups{17, 2000};
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_GROUPS, .groups = groups}, grouped), 0);
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_UID, .real = 1001}, grouped), 0);
    KREQUIRE_EQ(open(grouped, FILE, 0), 0);
    KEXPECT_EQ(close(), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("file", LINK), 0);
    KEXPECT_EQ(open(user, LINK, vfs::O_NOFOLLOW), -ELOOP);
    KEXPECT_EQ(open(user, LINK, vfs::O_CREAT | vfs::O_EXCL | 1), -EEXIST);
    KREQUIRE_EQ(open(user, FILE, vfs::O_TRUNC | 1), 0);
    KEXPECT_EQ(vfs::vfs_fstat_file(cleanup.opened, &stat), 0);
    KEXPECT_EQ(stat.st_size, 0);
    KEXPECT_EQ(close(), 0);
    KREQUIRE_EQ(open(user, BASE, vfs::O_DIRECTORY), 0);
    KEXPECT_TRUE(cleanup.opened->is_directory);
    KEXPECT_EQ(close(), 0);
    KEXPECT_EQ(open(user, "/tmp", vfs::O_DIRECTORY), -EPERM);
    KEXPECT_TRUE(cleanup.opened == nullptr && worker.credentials() == WORKER);
    vfs::File sentinel{};
    cleanup.opened = &sentinel;
    KEXPECT_EQ(open(other, FILE, 1), -EACCES);
    KEXPECT_TRUE(cleanup.opened == &sentinel);
    cleanup.opened = nullptr;
    std::strcpy(cleanup.long_path.data(), BASE);
    size_t const PREFIX = std::strlen(BASE);
    cleanup.long_path[PREFIX] = '/';
    std::memset(cleanup.long_path.data() + PREFIX + 1, 'a', 240);
    KREQUIRE_EQ(open(user, cleanup.long_path.data(), vfs::O_CREAT | vfs::O_EXCL | 2), 0);
    KEXPECT_TRUE(cleanup.opened->vfs_path_heap_allocated);
    KEXPECT_TRUE(std::strcmp(cleanup.opened->vfs_path, cleanup.long_path.data()) == 0);
    KEXPECT_EQ(cleanup.opened->fops->vfs_write(cleanup.opened, "long", 4, 0), 4);
    KEXPECT_EQ(close(), 0);
    KREQUIRE_EQ(open(user, cleanup.long_path.data(), 0), 0);
    KEXPECT_EQ(cleanup.opened->fops->vfs_read(cleanup.opened, bytes.data(), bytes.size(), 0), 4);
    KEXPECT_TRUE(std::memcmp(bytes.data(), "long", 4) == 0);
    KEXPECT_EQ(close(), 0);
    KEXPECT_TRUE(worker.credentials() == WORKER);
}

KTEST(Credentials, LocalSubmitterRulesPreserveAncestorSearch) {
    namespace vfs = ker::vfs;
    constexpr auto DIRECTORY = "pts/ktest_route_search";
    constexpr auto LINK = "pts/ktest_route_search/link";
    constexpr auto PATH = "/dev/pts/ktest_route_search/link";
    constexpr char TARGET[] = "private target";
    struct Cleanup {
        ~Cleanup() {
            static_cast<void>(vfs::devfs::devfs_remove_node(LINK));
            static_cast<void>(vfs::devfs::devfs_remove_node(DIRECTORY));
        }
    } cleanup;
    KREQUIRE_TRUE(vfs::devfs::devfs_create_directory(DIRECTORY) != nullptr);
    KREQUIRE_TRUE(vfs::devfs::devfs_create_symlink(LINK, TARGET) != nullptr);
    KREQUIRE_EQ(vfs::vfs_chmod("/dev/pts/ktest_route_search", 0700), 0);
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    ker::mod::sched::task::Task privileged{};
    privileged.pid = UINT64_MAX;
    std::array<char, 32> output{};
    auto add_rule = [](ker::mod::sched::task::Task& task, const char* prefix, ker::mod::sched::task::WkiVfsRoute route) -> bool {
        ker::mod::sched::task::WkiVfsRule rule{};
        size_t const LENGTH = std::strlen(prefix);
        std::memcpy(rule.prefix.data(), prefix, LENGTH + 1);
        rule.prefix_len = static_cast<uint16_t>(LENGTH);
        rule.route = static_cast<uint8_t>(route);
        return task.wki_vfs_rules.push_back(rule);
    };
    using Route = ker::mod::sched::task::WkiVfsRoute;
    for (unsigned rules = 0; rules < 4; ++rules) {
        KREQUIRE_EQ(vfs::vfs_readlinkat(&privileged, vfs::AT_FDCWD, PATH, output.data(), output.size()), sizeof(TARGET) - 1);
        ker::mod::sched::task::Task task{};
        task.pid = UINT64_MAX;
        KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(task, user), 0);
        if (rules != 0) KREQUIRE_TRUE(add_rule(task, "/dev", rules == 2 ? Route::HOST : Route::LOCAL));
        if (rules == 3) {
            KREQUIRE_TRUE(add_rule(task, "/dev/pts", Route::HOST));
            KREQUIRE_TRUE(add_rule(task, "/unrelated", Route::HOST));
        }
        output.fill('Z');
        auto const SENTINEL = output;
        KEXPECT_EQ(vfs::vfs_readlinkat(&task, vfs::AT_FDCWD, PATH, output.data(), output.size()), static_cast<ssize_t>(-EACCES));
        KEXPECT_TRUE(output == SENTINEL);
        vfs::Stat metadata{};
        metadata.st_ino = 0xCAFE;
        KEXPECT_EQ(vfs::vfs_statat(&task, vfs::AT_FDCWD, PATH, 0, &metadata), -EACCES);
        KEXPECT_EQ(metadata.st_ino, 0xCAFE);
        KEXPECT_TRUE(task.credentials() == user && task.fd_table.empty());
    }
    // A different submitter must still consult its LOCAL rule and retain the
    // DEVFS fallback target. This is a privileged routing/binding control.
    std::strcpy(privileged.wki_submitter_hostname.data(), "ktest_foreign_submitter");
    KREQUIRE_TRUE(add_rule(privileged, "/dev", Route::LOCAL));
    output.fill('Z');
    KEXPECT_EQ(vfs::vfs_readlinkat(&privileged, vfs::AT_FDCWD, PATH, output.data(), output.size()), sizeof(TARGET) - 1);
    KEXPECT_TRUE(std::memcmp(output.data(), TARGET, sizeof(TARGET) - 1) == 0);
    KEXPECT_EQ(output[sizeof(TARGET) - 1], 'Z');
    KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(privileged, user), 0);
    output.fill('Z');
    auto const DENIED_OUTPUT = output;
    KEXPECT_EQ(vfs::vfs_readlinkat(&privileged, vfs::AT_FDCWD, PATH, output.data(), output.size()), static_cast<ssize_t>(-EACCES));
    KEXPECT_TRUE(output == DENIED_OUTPUT);
}

KTEST(Credentials, ForeignLocalRoutesPreservePrivateLinksAndDotAncestors) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_foreign_search";
    constexpr auto PRIVATE = "/tmp/ktest_foreign_search/private";
    constexpr auto PUBLIC = "/tmp/ktest_foreign_search/public";
    constexpr auto LINK = "/tmp/ktest_foreign_search/private/link";
    struct Cleanup {
        ~Cleanup() {
            static_cast<void>(vfs::vfs_unlink(LINK));
            static_cast<void>(vfs::vfs_unlink(PUBLIC));
            static_cast<void>(vfs::vfs_rmdir(PRIVATE));
            static_cast<void>(vfs::vfs_rmdir(BASE));
        }
    } cleanup;
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
    KREQUIRE_EQ(vfs::vfs_mkdir(PRIVATE, 0710), 0);
    KREQUIRE_EQ(vfs::vfs_chown(PRIVATE, 0, 2000), 0);
    auto* file = vfs::vfs_open_file(PUBLIC, vfs::O_CREAT | vfs::O_EXCL | 2, 0666);
    KREQUIRE_TRUE(file != nullptr);
    KREQUIRE_EQ(vfs::vfs_close_file(file), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("../public", LINK), 0);
    std::array<char, 32> bytes{};
    KREQUIRE_EQ(vfs::vfs_readlink(LINK, bytes.data(), bytes.size()), 9);
    auto user = security::Snapshot::bootstrap_root();
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    auto grouped = security::Snapshot::bootstrap_root();
    std::array<uint32_t, 1> groups{2000};
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_GROUPS, .groups = groups}, grouped), 0);
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_UID, .real = 1001}, grouped), 0);
    for (bool rooted : {false, true}) {
        ker::mod::sched::task::Task task{};
        task.pid = UINT64_MAX;
        std::strcpy(task.wki_submitter_hostname.data(), "ktest_foreign_submitter");
        if (rooted) {
            std::strcpy(task.root.data(), BASE);
            task.root_len = std::strlen(BASE);
        }
        ker::mod::sched::task::WkiVfsRule rule{};
        std::strcpy(rule.prefix.data(), rooted ? "/" : "/tmp");
        rule.prefix_len = std::strlen(rule.prefix.data());
        rule.route = static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL);
        KREQUIRE_TRUE(task.wki_vfs_rules.push_back(rule));
        KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(task, user), 0);
        for (const char* suffix : {"/private/link", "/private/../public", "/private/missing"}) {
            std::array<char, 512> path{};
            std::snprintf(path.data(), path.size(), "%s%s", rooted ? "" : BASE, suffix);
            bytes.fill('Z');
            auto const BEFORE = bytes;
            KEXPECT_EQ(vfs::vfs_readlinkat(&task, vfs::AT_FDCWD, path.data(), bytes.data(), bytes.size()), static_cast<ssize_t>(-EACCES));
            KEXPECT_TRUE(bytes == BEFORE);
            vfs::Stat metadata{};
            metadata.st_ino = 0xCAFE;
            KEXPECT_EQ(vfs::vfs_statat(&task, vfs::AT_FDCWD, path.data(), 0, &metadata), -EACCES);
            KEXPECT_EQ(metadata.st_ino, 0xCAFE);
            KEXPECT_EQ(vfs::vfs_faccessat(&task, vfs::AT_FDCWD, path.data(), 0, 0), -EACCES);
            int const FD = vfs::vfs_openat(&task, vfs::AT_FDCWD, path.data(), 0, 0);
            KEXPECT_EQ(FD, -EACCES);
            if (FD >= 0) {
                auto* opened = vfs::vfs_get_file(&task, FD);
                static_cast<void>(vfs::vfs_release_fd(&task, FD));
                vfs::vfs_put_file(opened);
            }
        }
        KEXPECT_TRUE(task.credentials() == user && task.fd_table.empty());
        KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(task, grouped), 0);
        const char* path = rooted ? "/private/link" : LINK;
        KEXPECT_EQ(vfs::vfs_readlinkat(&task, vfs::AT_FDCWD, path, bytes.data(), bytes.size()), 9);
        KEXPECT_TRUE(std::memcmp(bytes.data(), "../public", 9) == 0);
        vfs::Stat metadata{};
        KEXPECT_EQ(vfs::vfs_statat(&task, vfs::AT_FDCWD, path, 0, &metadata), 0);
        KEXPECT_EQ(vfs::vfs_faccessat(&task, vfs::AT_FDCWD, path, 0, 0), 0);
    }
}

KTEST(Credentials, LocalReadlinkRetainsDirectoryTypeAndDeviceIdentity) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_local_readlink";
    constexpr auto FILE = "/tmp/ktest_local_readlink/file";
    constexpr auto LINK = "/tmp/ktest_local_readlink/link";
    constexpr auto DIRECTORY = "/tmp/ktest_local_readlink/directory";
    constexpr auto DEVICE = "pts/ktest_readlink_device";
    DevfsOpenFixture fixture{};
    ker::dev::CharDeviceOps operations{};
    operations.open = credential_device_open;
    operations.close = credential_device_close;
    ker::dev::Device device{};
    device.char_ops = &operations;
    device.private_data = &fixture;
    struct Cleanup {
        ~Cleanup() {
            for (auto path : {LINK, DIRECTORY, FILE}) static_cast<void>(vfs::vfs_unlink(path));
            static_cast<void>(vfs::vfs_rmdir(BASE));
            static_cast<void>(vfs::devfs::devfs_remove_node(DEVICE));
        }
    } cleanup;
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0710), 0);
    KREQUIRE_EQ(vfs::vfs_chown(BASE, 0, 2000), 0);
    auto* file = vfs::vfs_open_file(FILE, vfs::O_CREAT | vfs::O_EXCL | 1, 0000);
    KREQUIRE_TRUE(file != nullptr);
    KREQUIRE_EQ(vfs::vfs_close_file(file), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("file", LINK), 0);
    KREQUIRE_EQ(vfs::vfs_symlink(".", DIRECTORY), 0);
    auto* node = vfs::devfs::devfs_add_device_node(DEVICE, &device);
    KREQUIRE_TRUE(node != nullptr);
    auto const REFS = node->pts_refs.load();
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    auto group = ROOT;
    std::array<uint32_t, 1> groups{2000};
    KREQUIRE_EQ(group.prepare({.change = security::Change::SET_GROUPS, .groups = groups}, group), 0);
    KREQUIRE_EQ(group.prepare({.change = security::Change::SET_UID, .real = 1001}, group), 0);
    for (auto const& caller : {ROOT, user, group}) {
        ker::mod::sched::task::Task task{};
        task.pid = UINT64_MAX;
        KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(task, caller), 0);
        std::array<char, 32> output{};
        output.fill('Z');
        auto const SENTINEL = output;
        auto read = [&](const char* path, size_t size = 32) {
            output = SENTINEL;
            return vfs::vfs_readlinkat(&task, vfs::AT_FDCWD, path, output.data(), size);
        };
        bool const SEARCH = caller != user;
        KEXPECT_EQ(read("/tmp/ktest_local_readlink/link/"), static_cast<ssize_t>(SEARCH ? -ENOTDIR : -EACCES));
        KEXPECT_TRUE(output == SENTINEL);
        KEXPECT_EQ(read("/tmp/ktest_local_readlink/directory/"), static_cast<ssize_t>(SEARCH ? -EINVAL : -EACCES));
        KEXPECT_TRUE(output == SENTINEL);
        KEXPECT_EQ(read(LINK, 2), static_cast<ssize_t>(SEARCH ? 2 : -EACCES));
        if (SEARCH) {
            KEXPECT_TRUE(std::memcmp(output.data(), "fi", 2) == 0);
            KEXPECT_EQ(output[2], 'Z');
        } else {
            KEXPECT_TRUE(output == SENTINEL);
        }
        KEXPECT_EQ(read("/dev/stdout", 2), 2);
        KEXPECT_TRUE(std::memcmp(output.data(), "/p", 2) == 0);
        KEXPECT_EQ(output[2], 'Z');
        KEXPECT_EQ(read("/dev/pts/ktest_readlink_device"), static_cast<ssize_t>(-EINVAL));
        KEXPECT_TRUE(output == SENTINEL);
        KEXPECT_EQ(read("/dev/pts/ktest_readlink_device/"), static_cast<ssize_t>(-ENOTDIR));
        KEXPECT_TRUE(output == SENTINEL);
        ker::mod::sched::task::WkiVfsRule rule{};
        std::memcpy(rule.prefix.data(), "/dev", 5);
        rule.prefix_len = 4;
        task.wki_vfs_rules.push_back(rule);
        KEXPECT_EQ(read("/dev/stdout", 2), 2);
        KEXPECT_TRUE(std::memcmp(output.data(), "/p", 2) == 0);
        KEXPECT_EQ(output[2], 'Z');
        KEXPECT_EQ(read("/dev/pts/ktest_readlink_device/"), static_cast<ssize_t>(-ENOTDIR));
        KEXPECT_TRUE(output == SENTINEL);
        KEXPECT_EQ(read("/dev/"), static_cast<ssize_t>(-EINVAL));
        KEXPECT_TRUE(output == SENTINEL);
        KEXPECT_TRUE(task.credentials() == caller && task.fd_table.empty());
    }
    KEXPECT_EQ(node->pts_refs.load(), REFS);
    KEXPECT_EQ(fixture.opens, 0);
    KEXPECT_EQ(fixture.closes, 0);
}

KTEST(Credentials, InitialRemoteLookupsPreserveComponentsAndAdmittedSnapshot) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_initial_components";
    constexpr auto PRIVATE = "/tmp/ktest_initial_components/private";
    constexpr auto PUBLIC = "/tmp/ktest_initial_components/public";
    constexpr auto LINK = "/tmp/ktest_initial_components/remote_link";
    constexpr auto NESTED_LINK = "/tmp/ktest_initial_components/nested_link";
    constexpr auto RELATIVE_LINK = "/tmp/ktest_initial_components/relative_link";
    constexpr auto GLOBAL = "/wki/ktest_initial_owner";
    constexpr auto ROOTED_WKI = "/tmp/ktest_initial_components/wki";
    constexpr auto ROOTED = "/tmp/ktest_initial_components/wki/ktest_initial_owner";
    constexpr auto RAW = "/wki/ktest_initial_owner/private/../public";
    constexpr auto GLOBAL_NESTED = "/wki/ktest_initial_owner/tmp";
    constexpr auto ROOTED_NESTED = "/tmp/ktest_initial_components/wki/ktest_initial_owner/tmp";
    struct Cleanup {
        std::array<char, ker::net::wki::WKI_HOSTNAME_MAX> hostname = ker::net::wki::g_wki.local_hostname;
        ~Cleanup() {
            static_cast<void>(vfs::unmount_filesystem(ROOTED_NESTED));
            static_cast<void>(vfs::unmount_filesystem(GLOBAL_NESTED));
            static_cast<void>(vfs::unmount_filesystem(ROOTED));
            static_cast<void>(vfs::unmount_filesystem(GLOBAL));
            for (auto path : {RELATIVE_LINK, NESTED_LINK, LINK, PUBLIC}) static_cast<void>(vfs::vfs_unlink(path));
            for (auto path : {ROOTED_NESTED, GLOBAL_NESTED, ROOTED, ROOTED_WKI, PRIVATE, BASE, GLOBAL})
                static_cast<void>(vfs::vfs_rmdir(path));
            ker::net::wki::g_wki.local_hostname = hostname;
        }
    } cleanup;
    // --selftest runs before WKI initialization with secondary CPUs parked.
    // Seed only the alias name; no peer, transport or WKI startup is required.
    if (ker::net::wki::g_wki.local_hostname[0] == '\0') {
        KREQUIRE_TRUE(!ker::net::wki::g_wki.initialized);
        std::strcpy(ker::net::wki::g_wki.local_hostname.data(), "ktest_initial_self");
    }
    int const WKI_DIR = vfs::vfs_mkdir("/wki", 0755);
    KREQUIRE_TRUE(WKI_DIR == 0 || WKI_DIR == -EEXIST);
    for (auto path : {BASE, PRIVATE, ROOTED_WKI, ROOTED, GLOBAL, GLOBAL_NESTED, ROOTED_NESTED}) KREQUIRE_EQ(vfs::vfs_mkdir(path, 0755), 0);
    KREQUIRE_EQ(vfs::vfs_chown(PRIVATE, 0, 2000), 0);
    KREQUIRE_EQ(vfs::vfs_chmod(PRIVATE, 0710), 0);
    auto* file = vfs::vfs_open_file(PUBLIC, vfs::O_CREAT | vfs::O_EXCL, 0644);
    KREQUIRE_TRUE(file != nullptr);
    KREQUIRE_EQ(vfs::vfs_close_file(file), 0);
    KREQUIRE_EQ(vfs::vfs_symlink(RAW, LINK), 0);
    KREQUIRE_EQ(vfs::vfs_symlink(GLOBAL_NESTED, NESTED_LINK), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("nested_link", RELATIVE_LINK), 0);
    for (auto path : {GLOBAL, ROOTED, GLOBAL_NESTED, ROOTED_NESTED}) KREQUIRE_EQ(vfs::mount_filesystem(path, "remote", nullptr), 0);
    auto user = security::Snapshot::bootstrap_root();
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GID, .real = 2001}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    auto grouped = security::Snapshot::bootstrap_root();
    std::array<uint32_t, 1> groups{2000};
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_GROUPS, .groups = groups}, grouped), 0);
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_GID, .real = 2001}, grouped), 0);
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_UID, .real = 1000}, grouped), 0);
    for (bool rooted : {false, true}) {
        for (bool foreign : {false, true}) {
            ker::mod::sched::task::Task task{};
            task.pid = UINT64_MAX;
            if (rooted) {
                std::strcpy(task.root.data(), BASE);
                task.root_len = std::strlen(BASE);
            }
            if (foreign) std::strcpy(task.wki_submitter_hostname.data(), "ktest_initial_owner");
            auto rule = [&](const char* prefix, ker::mod::sched::task::WkiVfsRoute route) {
                ker::mod::sched::task::WkiVfsRule entry{};
                std::strcpy(entry.prefix.data(), prefix);
                entry.prefix_len = std::strlen(prefix);
                entry.route = static_cast<uint8_t>(route);
                return task.wki_vfs_rules.push_back(entry);
            };
            KREQUIRE_TRUE(rule("/", ker::mod::sched::task::WkiVfsRoute::LOCAL));
            KREQUIRE_TRUE(rule("/wki", ker::mod::sched::task::WkiVfsRoute::LOCAL));
            KREQUIRE_TRUE(rule("/source-only", ker::mod::sched::task::WkiVfsRoute::HOST));
            KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(task, user), 0);
            auto acquire = [&](const char* path, vfs::LookupIntent intent, const security::Snapshot& credentials,
                               vfs::LookupHandle& lookup) {
                return vfs::vfs_acquire_lookup(&task, vfs::AT_FDCWD, path, intent, vfs::LookupFollowPolicy::FOLLOW_FINAL, false, true,
                                               &lookup, 2, false, &credentials);
            };
            for (auto intent : {vfs::LookupIntent::OPEN, vfs::LookupIntent::STAT, vfs::LookupIntent::ACCESS, vfs::LookupIntent::READLINK}) {
                vfs::LookupHandle lookup{};
                for (auto suffix : {"/public", "/private/../public"}) {
                    std::array<char, 512> nested{}, physical{};
                    std::snprintf(nested.data(), nested.size(), "%s%s", GLOBAL_NESTED, suffix);
                    std::snprintf(physical.data(), physical.size(), "%s%s", rooted ? ROOTED_NESTED : GLOBAL_NESTED, suffix);
                    KREQUIRE_EQ(acquire(nested.data(), intent, grouped, lookup), 0);
                    KEXPECT_TRUE(std::strcmp(lookup.path(), physical.data()) == 0);
                    KEXPECT_TRUE(std::strcmp(lookup.mount()->path, rooted ? ROOTED_NESTED : GLOBAL_NESTED) == 0);
                    KEXPECT_TRUE(vfs::find_mount_point(lookup.path()).get() == lookup.mount());
                    KEXPECT_TRUE(lookup.authorization().credentials == grouped);
                    lookup.reset();
                }
                for (auto alias : {rooted ? "/nested_link" : NESTED_LINK, rooted ? "/relative_link" : RELATIVE_LINK}) {
                    for (auto suffix : {"/public", "/private/../public", "//private//../public"}) {
                        for (auto admitted : {user, grouped}) {
                            std::array<char, 512> alias_path{}, physical{};
                            std::snprintf(alias_path.data(), alias_path.size(), "%s%s", alias, suffix);
                            std::snprintf(physical.data(), physical.size(), "%s%s", rooted ? ROOTED_NESTED : GLOBAL_NESTED,
                                          std::strcmp(suffix, "/public") == 0 ? "/public" : "/private/../public");
                            KREQUIRE_EQ(acquire(alias_path.data(), intent, admitted, lookup), 0);
                            KEXPECT_TRUE(std::strcmp(lookup.path(), physical.data()) == 0);
                            KEXPECT_TRUE(std::strcmp(lookup.mount()->path, rooted ? ROOTED_NESTED : GLOBAL_NESTED) == 0);
                            KEXPECT_TRUE(vfs::find_mount_point(lookup.path()).get() == lookup.mount());
                            KEXPECT_TRUE(lookup.authorization().credentials == admitted);
                            lookup.reset();
                        }
                    }
                }
                KREQUIRE_EQ(acquire(RAW, intent, grouped, lookup), 0);
                std::array<char, 512> expected{};
                std::snprintf(expected.data(), expected.size(), "%s/private/../public", rooted ? ROOTED : GLOBAL);
                KEXPECT_TRUE(lookup.mount()->fs_type == vfs::FSType::REMOTE);
                KEXPECT_TRUE(std::strcmp(lookup.path(), expected.data()) == 0);
                KEXPECT_TRUE(lookup.authorization().credentials == grouped);
                lookup.reset();
                KREQUIRE_EQ(acquire(rooted ? "/remote_link" : LINK, intent, user, lookup), 0);
                KEXPECT_TRUE(std::strcmp(lookup.path(), expected.data()) == 0);
                KEXPECT_TRUE(lookup.authorization().credentials == user);
                lookup.reset();
                if (foreign) {
                    KREQUIRE_EQ(acquire("/source-only/private/../public", intent, grouped, lookup), 0);
                    std::snprintf(expected.data(), expected.size(), "%s/source-only/private/../public", rooted ? ROOTED : GLOBAL);
                    KEXPECT_TRUE(std::strcmp(lookup.path(), expected.data()) == 0);
                    KEXPECT_TRUE(lookup.authorization().credentials == grouped);
                }
            }
            // A self alias restarts locally and must still search private/..
            // with the supplied identity, even after a privileged warmup.
            std::array<char, 512> self{};
            std::snprintf(self.data(), self.size(), "/wki/%s%s/private/../public", ker::net::wki::g_wki.local_hostname.data(),
                          rooted ? "" : BASE);
            vfs::LookupHandle lookup{};
            KREQUIRE_EQ(acquire(self.data(), vfs::LookupIntent::STAT, security::Snapshot::bootstrap_root(), lookup), 0);
            lookup.reset();
            KEXPECT_EQ(acquire(self.data(), vfs::LookupIntent::STAT, user, lookup), -EACCES);
            KREQUIRE_EQ(acquire(self.data(), vfs::LookupIntent::STAT, grouped, lookup), 0);
            KEXPECT_TRUE(lookup.mount()->fs_type == vfs::FSType::TMPFS);
            KEXPECT_TRUE(lookup.authorization().credentials == grouped);
            lookup.reset();
            std::snprintf(self.data(), self.size(), "//wki//%s/%s//private/../public", ker::net::wki::g_wki.local_hostname.data(),
                          rooted ? "" : BASE);
            KEXPECT_EQ(acquire(self.data(), vfs::LookupIntent::STAT, user, lookup), -EACCES);
            KREQUIRE_EQ(acquire(self.data(), vfs::LookupIntent::STAT, grouped, lookup), 0);
            KEXPECT_TRUE(lookup.mount()->fs_type == vfs::FSType::TMPFS);
            lookup.reset();
            KREQUIRE_EQ(acquire("/wki/ktest_initial_owner/../public", vfs::LookupIntent::STAT, user, lookup), 0);
            std::array<char, 512> expected{};
            std::snprintf(expected.data(), expected.size(), "%s/../public", rooted ? ROOTED : GLOBAL);
            KEXPECT_TRUE(std::strcmp(lookup.path(), expected.data()) == 0);
            lookup.reset();
            vfs::Stat denied{};
            denied.st_ino = 0xCAFE;
            KEXPECT_EQ(vfs::vfs_stat_resolved_beneath_as(task, user, "/", rooted ? ROOTED : GLOBAL, &denied), -EPERM);
            KEXPECT_EQ(denied.st_ino, 0xCAFE);
            KEXPECT_TRUE(task.credentials() == user);
        }
    }
}

namespace {
void check_owner_directory_grant(bool xfs) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_owner_dirfd";
    constexpr auto HIDDEN = "/tmp/ktest_owner_dirfd/hidden";
    constexpr auto ORIGINAL = "/tmp/ktest_owner_dirfd/hidden/anchor";
    constexpr auto RENAMED = "/tmp/ktest_owner_dirfd/hidden/renamed";
    struct Cleanup {
        vfs::File* directory = nullptr;
        vfs::File* opened = nullptr;
        bool mounted = false;
        bool overmounted = false;
        ~Cleanup() {
            if (overmounted) static_cast<void>(vfs::unmount_filesystem(RENAMED));
            if (opened != nullptr) vfs::vfs_put_file(opened);
            if (directory != nullptr) vfs::vfs_put_file(directory);
            for (auto base : {ORIGINAL, RENAMED}) {
                for (auto suffix : {"/private/link", "/link", "/absolute", "/escape", "/public", "/created"}) {
                    std::array<char, 512> path{};
                    std::snprintf(path.data(), path.size(), "%s%s", base, suffix);
                    static_cast<void>(vfs::vfs_unlink(path.data()));
                }
                std::array<char, 512> path{};
                std::snprintf(path.data(), path.size(), "%s/private", base);
                static_cast<void>(vfs::vfs_rmdir(path.data()));
                static_cast<void>(vfs::vfs_rmdir(base));
            }
            static_cast<void>(vfs::vfs_rmdir(HIDDEN));
            if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
            static_cast<void>(vfs::vfs_rmdir(BASE));
        }
    } cleanup;
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
    if (xfs) {
        auto* device = ker::dev::block_device_find_by_name("sdb2");
        KREQUIRE_TRUE(device != nullptr);
        KREQUIRE_EQ(vfs::mount_filesystem(BASE, "xfs", device), 0);
        cleanup.mounted = true;
    }
    for (auto path : {HIDDEN, ORIGINAL, "/tmp/ktest_owner_dirfd/hidden/anchor/private"}) KREQUIRE_EQ(vfs::vfs_mkdir(path, 0755), 0);
    constexpr auto PUBLIC = "/tmp/ktest_owner_dirfd/hidden/anchor/public";
    auto* file = vfs::vfs_open_file(PUBLIC, vfs::O_CREAT | vfs::O_EXCL | 2, 0644);
    KREQUIRE_TRUE(file != nullptr);
    cleanup.opened = file;
    KREQUIRE_EQ(file->fops->vfs_write(file, "public", 6, 0), 6);
    vfs::vfs_put_file(file);
    cleanup.opened = nullptr;
    KREQUIRE_EQ(vfs::vfs_symlink("private/../public", "/tmp/ktest_owner_dirfd/hidden/anchor/link"), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("../public", "/tmp/ktest_owner_dirfd/hidden/anchor/private/link"), 0);
    KREQUIRE_EQ(vfs::vfs_symlink(PUBLIC, "/tmp/ktest_owner_dirfd/hidden/anchor/absolute"), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("/tmp", "/tmp/ktest_owner_dirfd/hidden/anchor/escape"), 0);
    KREQUIRE_EQ(vfs::vfs_chown("/tmp/ktest_owner_dirfd/hidden/anchor/private", 0, 2000), 0);
    KREQUIRE_EQ(vfs::vfs_chmod("/tmp/ktest_owner_dirfd/hidden/anchor/private", 0710), 0);
    cleanup.directory = vfs::vfs_open_file(ORIGINAL, vfs::O_DIRECTORY, 0);
    KREQUIRE_TRUE(cleanup.directory != nullptr);
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    auto grouped = ROOT;
    std::array<uint32_t, 1> groups{2000};
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_GROUPS, .groups = groups}, grouped), 0);
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_UID, .real = 1001}, grouped), 0);
    ker::mod::sched::task::Task worker{};
    std::strcpy(worker.root.data(), "/not_the_export");
    std::strcpy(worker.cwd.data(), "/not_the_anchor");
    auto const WORKER = worker.credentials();
    auto stat = [&](const security::Snapshot& identity, const char* path, vfs::Stat& out) {
        return vfs::vfs_stat_resolved_beneath_as(worker, identity, BASE, path, &out, true, false, cleanup.directory);
    };
    auto open = [&](const security::Snapshot& identity, const char* path, int flags = 0) {
        return vfs::vfs_open_file_resolved_beneath_as(worker, identity, BASE, path, flags, 0644, &cleanup.opened, cleanup.directory);
    };
    auto close = [&]() {
        if (cleanup.opened != nullptr) vfs::vfs_put_file(cleanup.opened);
        cleanup.opened = nullptr;
    };
    std::array<char, 32> bytes{};
    auto readlink = [&](const security::Snapshot& identity, const char* path) {
        return vfs::vfs_readlink_resolved_beneath_as(worker, identity, BASE, path, bytes.data(), bytes.size(), false, cleanup.directory);
    };
    vfs::Stat original{};
    KREQUIRE_EQ(stat(user, "public", original), 0);
    KREQUIRE_EQ(vfs::vfs_chmod(HIDDEN, 0700), 0);
    auto const ANCHOR_REFS = cleanup.directory->refcount.load();
    for (bool renamed : {false, true}) {
        if (renamed) KREQUIRE_EQ(vfs::vfs_rename(ORIGINAL, RENAMED), 0);
        vfs::Stat out{};
        KREQUIRE_EQ(stat(user, "public", out), 0);
        KEXPECT_EQ(out.st_ino, original.st_ino);
        KEXPECT_EQ(out.st_size, 6);
        KREQUIRE_EQ(open(user, "public"), 0);
        KEXPECT_EQ(vfs::vfs_stat_resolved_beneath_as(worker, ROOT, BASE, "public", &out, true, false, cleanup.opened), -ENOTDIR);
        KEXPECT_EQ(cleanup.opened->fops->vfs_read(cleanup.opened, bytes.data(), bytes.size(), 0), 6);
        KEXPECT_TRUE(std::memcmp(bytes.data(), "public", 6) == 0);
        close();
        KEXPECT_EQ(readlink(user, "link"), 17);
        KEXPECT_TRUE(std::memcmp(bytes.data(), "private/../public", 17) == 0);
        for (auto path : {"private/../public", "private/missing", "private/link"}) {
            out.st_ino = 0xCAFE;
            KEXPECT_EQ(stat(user, path, out), -EACCES);
            KEXPECT_EQ(out.st_ino, 0xCAFE);
            bytes.fill('Z');
            auto const BEFORE = bytes;
            KEXPECT_EQ(readlink(user, path), static_cast<ssize_t>(-EACCES));
            KEXPECT_TRUE(bytes == BEFORE);
            KEXPECT_EQ(open(user, path), -EACCES);
            KEXPECT_TRUE(cleanup.opened == nullptr);
            close();
        }
        KEXPECT_EQ(stat(user, "link", out), -EACCES);
        KREQUIRE_EQ(stat(grouped, "private/../public", out), 0);
        KEXPECT_EQ(out.st_ino, original.st_ino);
        KREQUIRE_EQ(open(grouped, "private/link"), 0);
        close();
        KEXPECT_EQ(open(user, "private/../created", vfs::O_CREAT | vfs::O_EXCL | 1), -EACCES);
        KEXPECT_TRUE(cleanup.opened == nullptr);
        close();
        KEXPECT_EQ(stat(ROOT, "created", out), -ENOENT);
        KEXPECT_EQ(open(user, "private/../public", vfs::O_TRUNC | 1), -EACCES);
        KEXPECT_TRUE(cleanup.opened == nullptr);
        close();
        KREQUIRE_EQ(stat(ROOT, "public", out), 0);
        KEXPECT_EQ(out.st_size, 6);
        KEXPECT_EQ(stat(ROOT, "../../../", out), -EPERM);
        KEXPECT_EQ(stat(ROOT, "escape", out), -EPERM);
        KEXPECT_EQ(stat(ROOT, "absolute", out), renamed ? -ENOENT : 0);
        KEXPECT_EQ(stat(user, "absolute", out), -EACCES);
        KEXPECT_EQ(readlink(user, "escape"), 4);
        KEXPECT_TRUE(std::memcmp(bytes.data(), "/tmp", 4) == 0);
        KEXPECT_EQ(stat(user, "", out), -ENOENT);
        KEXPECT_EQ(stat(user, PUBLIC, out), -EINVAL);
        KEXPECT_TRUE(worker.credentials() == WORKER && worker.fd_table.empty());
        KEXPECT_TRUE(std::strcmp(worker.root.data(), "/not_the_export") == 0);
        KEXPECT_TRUE(std::strcmp(worker.cwd.data(), "/not_the_anchor") == 0);
        KEXPECT_EQ(cleanup.directory->refcount.load(), ANCHOR_REFS);
    }
    KREQUIRE_EQ(vfs::vfs_chmod(RENAMED, 0700), 0);
    vfs::Stat out{};
    KEXPECT_EQ(stat(user, "public", out), -EACCES);
    KEXPECT_EQ(stat(user, ".", out), -EACCES);
    KEXPECT_EQ(stat(user, "../", out), -EACCES);
    KEXPECT_EQ(vfs::vfs_stat_resolved_beneath_as(worker, ROOT, "/other_export", "public", &out, true, false, cleanup.directory), -EPERM);
    KREQUIRE_EQ(vfs::mount_filesystem(RENAMED, "tmpfs", nullptr), 0);
    cleanup.overmounted = true;
    out.st_ino = 0xCAFE;
    KEXPECT_EQ(stat(ROOT, "public", out), -EAGAIN);
    KEXPECT_EQ(out.st_ino, 0xCAFE);
    KREQUIRE_EQ(vfs::unmount_filesystem(RENAMED), 0);
    cleanup.overmounted = false;
    KREQUIRE_EQ(stat(ROOT, "public", out), 0);
    KEXPECT_EQ(out.st_ino, original.st_ino);
}
}  // namespace

KTEST(Credentials, OwnerTmpfsDirectoryGrantPreservesIdentityAndSearchBelowAnchor) { check_owner_directory_grant(false); }
KTEST(Credentials, OwnerXfsDirectoryGrantPreservesIdentityAndSearchBelowAnchor) { check_owner_directory_grant(true); }

KTEST(Credentials, OwnerComponentBoundaryRejectsEscapeAndReentryBeforeSideEffects) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_owner_component";
    constexpr auto PRIVATE = "/tmp/ktest_owner_component/private";
    constexpr auto FILE = "/tmp/ktest_owner_component/public";
    constexpr auto CREATED = "/tmp/ktest_owner_component/created";
    constexpr auto OUTSIDE = "/tmp/ktest_owner_component_outside";
    constexpr auto RELATIVE = "/tmp/ktest_owner_component/relative";
    constexpr auto ABSOLUTE = "/tmp/ktest_owner_component/absolute";
    constexpr auto REL_ESCAPE = "/tmp/ktest_owner_component/relative_escape";
    constexpr auto ABS_ESCAPE = "/tmp/ktest_owner_component/absolute_escape";
    constexpr auto REL_TARGET = "../ktest_owner_component_outside/../ktest_owner_component/public";
    constexpr auto ABS_TARGET = "/tmp/ktest_owner_component_outside/../ktest_owner_component/public";
    constexpr auto RAW_ESCAPE = "/tmp/ktest_owner_component/../ktest_owner_component_outside/../ktest_owner_component/public";
    constexpr auto RAW_CREATE = "/tmp/ktest_owner_component/../ktest_owner_component_outside/../ktest_owner_component/created";
    constexpr auto PRIVATE_DOT = "/tmp/ktest_owner_component/private/../public";
    struct Cleanup {
        vfs::File* opened{};
        ~Cleanup() {
            if (opened != nullptr) static_cast<void>(vfs::vfs_close_file(opened));
            for (auto path : {RELATIVE, ABSOLUTE, REL_ESCAPE, ABS_ESCAPE, CREATED, FILE}) static_cast<void>(vfs::vfs_unlink(path));
            for (auto path : {PRIVATE, BASE, OUTSIDE}) static_cast<void>(vfs::vfs_rmdir(path));
        }
    } cleanup;
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
    KREQUIRE_EQ(vfs::vfs_mkdir(PRIVATE, 0700), 0);
    // Reaching this directory would expose EACCES to the user and allow root
    // to leave and reenter the export. Both must stop at the export boundary.
    KREQUIRE_EQ(vfs::vfs_mkdir(OUTSIDE, 0000), 0);
    cleanup.opened = vfs::vfs_open_file(FILE, vfs::O_CREAT | vfs::O_EXCL | 2, 0644);
    KREQUIRE_TRUE(cleanup.opened != nullptr);
    KREQUIRE_EQ(cleanup.opened->fops->vfs_write(cleanup.opened, "public", 6, 0), 6);
    auto close = [&] {
        int const result = vfs::vfs_close_file(cleanup.opened);
        cleanup.opened = nullptr;
        return result;
    };
    KREQUIRE_EQ(close(), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("./public", RELATIVE), 0);
    KREQUIRE_EQ(vfs::vfs_symlink(FILE, ABSOLUTE), 0);
    KREQUIRE_EQ(vfs::vfs_symlink(REL_TARGET, REL_ESCAPE), 0);
    KREQUIRE_EQ(vfs::vfs_symlink(ABS_TARGET, ABS_ESCAPE), 0);
    ker::mod::sched::task::Task worker{};
    std::strcpy(worker.root.data(), "/not_the_export");
    std::strcpy(worker.cwd.data(), "/not_the_directory");
    auto const ROOT = worker.credentials();
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GID, .real = 2001}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    vfs::Stat output{};
    KREQUIRE_EQ(vfs::vfs_stat(FILE, &output), 0);
    auto const SENTINEL = output;
    std::array<char, 128> bytes{};
    bytes.fill('Z');
    auto const BYTE_SENTINEL = bytes;
    auto const GENERATION = vfs::tmpfs::tmpfs_namespace_generation();
    for (auto const& caller : {user, ROOT}) {
        for (auto path : {REL_ESCAPE, ABS_ESCAPE, RAW_ESCAPE}) {
            KEXPECT_EQ(vfs::vfs_stat_resolved_beneath_as(worker, caller, BASE, path, &output), -EPERM);
            KEXPECT_TRUE(std::memcmp(&output, &SENTINEL, sizeof(output)) == 0);
            KEXPECT_EQ(vfs::vfs_open_read_file_resolved_beneath_as(worker, caller, BASE, path, &cleanup.opened), -EPERM);
            KEXPECT_TRUE(cleanup.opened == nullptr);
            if (cleanup.opened != nullptr) KEXPECT_EQ(close(), 0);
            KEXPECT_EQ(vfs::vfs_open_file_resolved_beneath_as(worker, caller, BASE, path, vfs::O_TRUNC | 1, 0, &cleanup.opened), -EPERM);
            KEXPECT_TRUE(cleanup.opened == nullptr);
            if (cleanup.opened != nullptr) KEXPECT_EQ(close(), 0);
            KEXPECT_EQ(vfs::vfs_readlink_resolved_beneath_as(worker, caller, BASE, path, bytes.data(), bytes.size(), true),
                       static_cast<ssize_t>(-EPERM));
            KEXPECT_TRUE(bytes == BYTE_SENTINEL);
        }
        KEXPECT_EQ(vfs::vfs_open_file_resolved_beneath_as(worker, caller, BASE, RAW_CREATE, vfs::O_CREAT | 1, 0644, &cleanup.opened),
                   -EPERM);
        KEXPECT_TRUE(cleanup.opened == nullptr);
        if (cleanup.opened != nullptr) KEXPECT_EQ(close(), 0);
    }
    KEXPECT_EQ(vfs::tmpfs::tmpfs_namespace_generation(), GENERATION);
    KEXPECT_EQ(vfs::vfs_stat(CREATED, &output), -ENOENT);
    KREQUIRE_EQ(vfs::vfs_stat(FILE, &output), 0);
    KEXPECT_EQ(output.st_size, 6);
    // NOFOLLOW exposes link text without traversing its target.
    KREQUIRE_EQ(vfs::vfs_readlink_resolved_beneath_as(worker, user, BASE, REL_ESCAPE, bytes.data(), bytes.size()), std::strlen(REL_TARGET));
    KEXPECT_TRUE(std::memcmp(bytes.data(), REL_TARGET, std::strlen(REL_TARGET)) == 0);
    KEXPECT_EQ(bytes[std::strlen(REL_TARGET)], 'Z');
    bytes = BYTE_SENTINEL;
    KEXPECT_EQ(vfs::vfs_stat_resolved_beneath_as(worker, user, BASE, PRIVATE_DOT, &output), -EACCES);
    KEXPECT_TRUE(std::memcmp(&output, &SENTINEL, sizeof(output)) == 0);
    KEXPECT_EQ(vfs::vfs_open_read_file_resolved_beneath_as(worker, user, BASE, PRIVATE_DOT, &cleanup.opened), -EACCES);
    KEXPECT_TRUE(cleanup.opened == nullptr);
    if (cleanup.opened != nullptr) KEXPECT_EQ(close(), 0);
    KEXPECT_EQ(vfs::vfs_readlink_resolved_beneath_as(worker, user, BASE, PRIVATE_DOT, bytes.data(), bytes.size()),
               static_cast<ssize_t>(-EACCES));
    KEXPECT_TRUE(bytes == BYTE_SENTINEL);
    KREQUIRE_EQ(vfs::vfs_stat_resolved_beneath_as(worker, ROOT, BASE, PRIVATE_DOT, &output), 0);
    auto grouped = ROOT;
    std::array<uint32_t, 1> groups{2000};
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_GROUPS, .groups = groups}, grouped), 0);
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_GID, .real = 2001}, grouped), 0);
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_UID, .real = 1000}, grouped), 0);
    KREQUIRE_EQ(vfs::vfs_chown(PRIVATE, 0, 2000), 0);
    KREQUIRE_EQ(vfs::vfs_chmod(PRIVATE, 0710), 0);
    KREQUIRE_EQ(vfs::vfs_stat_resolved_beneath_as(worker, grouped, BASE, PRIVATE_DOT, &output), 0);
    for (auto path : {RELATIVE, ABSOLUTE}) {
        KREQUIRE_EQ(vfs::vfs_stat_resolved_beneath_as(worker, user, BASE, path, &output), 0);
        KEXPECT_EQ(output.st_size, 6);
        KREQUIRE_EQ(vfs::vfs_open_read_file_resolved_beneath_as(worker, user, BASE, path, &cleanup.opened), 0);
        KEXPECT_EQ(cleanup.opened->fops->vfs_read(cleanup.opened, bytes.data(), bytes.size(), 0), 6);
        KEXPECT_TRUE(std::memcmp(bytes.data(), "public", 6) == 0);
        KEXPECT_EQ(close(), 0);
    }
    KEXPECT_TRUE(worker.credentials() == ROOT);
    KEXPECT_TRUE(std::strcmp(worker.root.data(), "/not_the_export") == 0);
    KEXPECT_TRUE(std::strcmp(worker.cwd.data(), "/not_the_directory") == 0);
}

namespace {
void check_mapped_open_absolute_symlink_namespace(bool xfs) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_mapped_link_root";
    constexpr auto FILE = "/tmp/ktest_mapped_link_root/public";
    constexpr auto LINK = "/tmp/ktest_mapped_link_root/link";
    constexpr auto PRIVATE = "/tmp/ktest_mapped_link_root/private";
    constexpr auto NARROW_FILE = "/tmp/ktest_mapped_link_root/private/public";
    constexpr auto NARROW_LINK = "/tmp/ktest_mapped_link_root/private/link";
    struct Cleanup {
        vfs::File* opened{};
        vfs::File* directory{};
        bool mounted{};
        ~Cleanup() {
            if (opened != nullptr) vfs::vfs_put_file(opened);
            if (directory != nullptr) vfs::vfs_put_file(directory);
            for (auto path : {NARROW_LINK, NARROW_FILE, LINK, FILE}) static_cast<void>(vfs::vfs_unlink(path));
            static_cast<void>(vfs::vfs_rmdir(PRIVATE));
            if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
            static_cast<void>(vfs::vfs_rmdir(BASE));
        }
    } cleanup;
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
    if (xfs) {
        auto* device = ker::dev::block_device_find_by_name("sdb2");
        KREQUIRE_TRUE(device != nullptr);
        KREQUIRE_EQ(vfs::mount_filesystem(BASE, "xfs", device), 0);
        cleanup.mounted = true;
    }
    KREQUIRE_EQ(vfs::vfs_mkdir(PRIVATE, 0700), 0);
    cleanup.opened = vfs::vfs_open_file(FILE, vfs::O_CREAT | vfs::O_EXCL | 2, 0644);
    KREQUIRE_TRUE(cleanup.opened != nullptr);
    KREQUIRE_EQ(cleanup.opened->fops->vfs_write(cleanup.opened, "public", 6, 0), 6);
    auto close = [&] {
        vfs::vfs_put_file(cleanup.opened);
        cleanup.opened = nullptr;
    };
    close();
    ker::mod::sched::task::Task worker{};
    std::strcpy(worker.root.data(), "/unrelated_worker_root");
    std::strcpy(worker.cwd.data(), "/unrelated_worker_cwd");
    auto const ROOT = worker.credentials();
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    struct Case {
        const char* visible_root;
        const char* target;
        int user_result;
        int root_result;
    };
    for (auto const& test : {
             Case{"/", "/public", 0, 0},
             Case{"/", "./public", 0, 0},
             Case{"/shared", "/shared/public", 0, 0},
             Case{"//shared///", "/shared/public", 0, 0},
             Case{"/shared", "./public", 0, 0},
             Case{"/shared", "/shared/private/../public", -EACCES, 0},
             Case{"/shared", "/shared_other/public", -EPERM, -EPERM},
             Case{"/shared", "/public", -EPERM, -EPERM},
             Case{"/shared", "/shared/../shared/public", -EPERM, -EPERM},
             Case{"/", "/../public", -EPERM, -EPERM},
             Case{"/shared", "../ktest_mapped_link_root/public", -EPERM, -EPERM},
             Case{"/shared/..", "/public", -EINVAL, -EINVAL},
             Case{"/shared/.", "/shared/public", -EINVAL, -EINVAL},
             Case{"shared", "/shared/public", -EINVAL, -EINVAL},
             Case{nullptr, FILE, 0, 0},
             Case{nullptr, "/public", -EPERM, -EPERM},
         }) {
        KREQUIRE_EQ(vfs::vfs_symlink(test.target, LINK), 0);
        for (auto const& caller : {user, ROOT}) {
            int const EXPECTED = caller == user ? test.user_result : test.root_result;
            vfs::Stat metadata{};
            metadata.st_ino = 0xCAFE;
            auto const SENTINEL = metadata;
            KEXPECT_EQ(vfs::vfs_stat_resolved_beneath_as(worker, caller, BASE, LINK, &metadata, true, false, nullptr, test.visible_root),
                       EXPECTED);
            if (EXPECTED == 0)
                KEXPECT_EQ(metadata.st_size, 6);
            else
                KEXPECT_TRUE(std::memcmp(&metadata, &SENTINEL, sizeof(metadata)) == 0);
            std::array<char, 128> canonical{};
            canonical.fill('Z');
            auto const BUFFER_SENTINEL = canonical;
            size_t length = 93;
            KEXPECT_EQ(vfs::vfs_realpath_resolved_beneath_as(worker, caller, BASE, LINK, canonical.data(), canonical.size(), &length, false,
                                                             nullptr, test.visible_root),
                       EXPECTED);
            if (EXPECTED == 0) {
                KEXPECT_EQ(length, 6);
                KEXPECT_TRUE(std::strcmp(canonical.data(), "public") == 0);
            } else {
                KEXPECT_TRUE(canonical == BUFFER_SENTINEL);
                KEXPECT_EQ(length, 93);
            }
            canonical = BUFFER_SENTINEL;
            auto const READLINK_EXPECTED =
                EXPECTED == -EINVAL ? static_cast<ssize_t>(-EINVAL) : static_cast<ssize_t>(std::strlen(test.target));
            KEXPECT_EQ(vfs::vfs_readlink_resolved_beneath_as(worker, caller, BASE, LINK, canonical.data(), canonical.size(), false, nullptr,
                                                             test.visible_root),
                       READLINK_EXPECTED);
            if (READLINK_EXPECTED >= 0) {
                KEXPECT_TRUE(std::memcmp(canonical.data(), test.target, std::strlen(test.target)) == 0);
                KEXPECT_EQ(canonical[std::strlen(test.target)], 'Z');
            } else
                KEXPECT_TRUE(canonical == BUFFER_SENTINEL);
            metadata = SENTINEL;
            KEXPECT_EQ(vfs::vfs_stat_resolved_beneath_as(worker, caller, BASE, LINK, &metadata, false, false, nullptr, test.visible_root),
                       EXPECTED == -EINVAL ? -EINVAL : 0);
            if (EXPECTED != -EINVAL)
                KEXPECT_EQ(metadata.st_mode & vfs::S_IFMT, vfs::S_IFLNK);
            else
                KEXPECT_TRUE(std::memcmp(&metadata, &SENTINEL, sizeof(metadata)) == 0);
            // Both loader-only and general owner OPEN must use the same walk.
            for (bool regular_only : {true, false}) {
                int const RESULT = regular_only ? vfs::vfs_open_read_file_resolved_beneath_as(worker, caller, BASE, LINK, &cleanup.opened,
                                                                                              test.visible_root)
                                                : vfs::vfs_open_file_resolved_beneath_as(worker, caller, BASE, LINK, 0, 0, &cleanup.opened,
                                                                                         nullptr, test.visible_root);
                KEXPECT_EQ(RESULT, EXPECTED);
                if (EXPECTED == 0 && RESULT == 0) {
                    std::array<char, 8> bytes{};
                    KEXPECT_EQ(cleanup.opened->fops->vfs_read(cleanup.opened, bytes.data(), bytes.size(), 0), 6);
                    KEXPECT_TRUE(std::memcmp(bytes.data(), "public", 6) == 0);
                } else {
                    KEXPECT_TRUE(cleanup.opened == nullptr);
                }
                if (cleanup.opened != nullptr) close();
            }
            if (EXPECTED == -EPERM || EXPECTED == -EACCES) {
                KEXPECT_EQ(vfs::vfs_open_file_resolved_beneath_as(worker, caller, BASE, LINK, vfs::O_TRUNC | 1, 0, &cleanup.opened, nullptr,
                                                                  test.visible_root),
                           EXPECTED);
                KEXPECT_TRUE(cleanup.opened == nullptr);
                if (cleanup.opened != nullptr) close();
            }
        }
        KREQUIRE_EQ(vfs::vfs_unlink(LINK), 0);
        vfs::Stat stat{};
        KREQUIRE_EQ(vfs::vfs_stat(FILE, &stat), 0);
        KEXPECT_EQ(stat.st_size, 6);
    }
    KREQUIRE_EQ(vfs::vfs_symlink("/public", LINK), 0);
    KEXPECT_EQ(vfs::vfs_open_file_resolved_beneath_as(worker, user, BASE, LINK, vfs::O_NOFOLLOW, 0, &cleanup.opened, nullptr, "/"), -ELOOP);
    KEXPECT_TRUE(cleanup.opened == nullptr);
    if (cleanup.opened != nullptr) close();
    KEXPECT_EQ(vfs::vfs_open_file_resolved_beneath_as(worker, user, BASE, LINK, vfs::O_CREAT | vfs::O_EXCL | 1, 0644, &cleanup.opened,
                                                      nullptr, "/"),
               -EEXIST);
    KEXPECT_TRUE(cleanup.opened == nullptr);
    if (cleanup.opened != nullptr) close();
    KREQUIRE_EQ(vfs::vfs_unlink(LINK), 0);
    KREQUIRE_EQ(vfs::vfs_chmod(PRIVATE, 0755), 0);
    cleanup.opened = vfs::vfs_open_file(NARROW_FILE, vfs::O_CREAT | vfs::O_EXCL | 2, 0644);
    KREQUIRE_TRUE(cleanup.opened != nullptr);
    KREQUIRE_EQ(cleanup.opened->fops->vfs_write(cleanup.opened, "narrow", 6, 0), 6);
    close();
    KREQUIRE_EQ(vfs::vfs_pin_directory_resolved_beneath_as(worker, ROOT, BASE, PRIVATE, &cleanup.opened), 0);
    vfs::DirectoryViewRef narrowed;
    KREQUIRE_EQ(vfs::vfs_prepare_directory_view(ROOT, cleanup.opened, cleanup.opened, {}, narrowed), 0);
    close();
    KREQUIRE_EQ(vfs::vfs_pin_directory_view_as(worker, ROOT, narrowed, ".", &cleanup.directory), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("/shared/private", LINK), 0);
    for (bool inside : {false, true}) {
        const char* target = inside ? "/shared/private/public" : "/shared/public";
        KREQUIRE_EQ(vfs::vfs_symlink(target, NARROW_LINK), 0);
        // A complete retained view narrows authority, never the meaning of an
        // absolute export name. Outside public must not become private/public.
        for (auto const& caller : {user, ROOT}) {
            int const EXPECTED = inside ? 0 : -EPERM;
            KEXPECT_EQ(
                vfs::vfs_open_file_resolved_beneath_as(worker, caller, BASE, "link", 0, 0, &cleanup.opened, cleanup.directory, "/shared"),
                EXPECTED);
            if (cleanup.opened != nullptr) {
                std::array<char, 8> bytes{};
                KEXPECT_EQ(cleanup.opened->fops->vfs_read(cleanup.opened, bytes.data(), bytes.size(), 0), 6);
                KEXPECT_TRUE(std::memcmp(bytes.data(), "narrow", 6) == 0);
                close();
            } else
                KEXPECT_TRUE(!inside);
            vfs::Stat metadata{};
            metadata.st_ino = 0xCAFE;
            auto const SENTINEL = metadata;
            KEXPECT_EQ(
                vfs::vfs_stat_resolved_beneath_as(worker, caller, BASE, "link", &metadata, true, false, cleanup.directory, "/shared"),
                EXPECTED);
            if (!inside) KEXPECT_TRUE(std::memcmp(&metadata, &SENTINEL, sizeof(metadata)) == 0);
            std::array<char, 128> output{};
            output.fill('Z');
            auto const BYTES = output;
            size_t length = 93;
            KEXPECT_EQ(vfs::vfs_realpath_resolved_beneath_as(worker, caller, BASE, "link", output.data(), output.size(), &length, false,
                                                             cleanup.directory, "/shared"),
                       EXPECTED);
            if (inside)
                KEXPECT_TRUE(std::strcmp(output.data(), "private/public") == 0);
            else {
                KEXPECT_TRUE(output == BYTES);
                KEXPECT_EQ(length, 93);
            }
            output = BYTES;
            KEXPECT_EQ(vfs::vfs_readlink_resolved_beneath_as(worker, caller, BASE, "/tmp/ktest_mapped_link_root/link/link", output.data(),
                                                             output.size(), false, nullptr, "/shared"),
                       std::strlen(target));
            KEXPECT_TRUE(std::memcmp(output.data(), target, std::strlen(target)) == 0);
            KEXPECT_EQ(output[std::strlen(target)], 'Z');
        }
        KREQUIRE_EQ(vfs::vfs_unlink(NARROW_LINK), 0);
    }
    KEXPECT_TRUE(worker.credentials() == ROOT);
    KEXPECT_TRUE(std::strcmp(worker.root.data(), "/unrelated_worker_root") == 0);
    KEXPECT_TRUE(std::strcmp(worker.cwd.data(), "/unrelated_worker_cwd") == 0);
}
}  // namespace

KTEST(Credentials, TmpfsMappedOpenUsesCapturedAbsoluteSymlinkNamespace) { check_mapped_open_absolute_symlink_namespace(false); }
KTEST(Credentials, XfsMappedOpenUsesCapturedAbsoluteSymlinkNamespace) { check_mapped_open_absolute_symlink_namespace(true); }

namespace {
void check_mapped_mutation_symlink_namespace(bool disk) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_mapped_mutation";
    constexpr auto DIR = "/tmp/ktest_mapped_mutation/dir";
    constexpr auto PRIVATE = "/tmp/ktest_mapped_mutation/private";
    constexpr auto ALIAS = "/tmp/ktest_mapped_mutation/alias";
    constexpr auto FILE = "/tmp/ktest_mapped_mutation/dir/file";
    constexpr auto RENAMED = "/tmp/ktest_mapped_mutation/dir/renamed";
    constexpr auto NEWDIR = "/tmp/ktest_mapped_mutation/dir/newdir";
    constexpr auto NEWLINK = "/tmp/ktest_mapped_mutation/dir/newlink";
    constexpr auto REMOVE = "/tmp/ktest_mapped_mutation/dir/remove";
    constexpr auto RMDIR = "/tmp/ktest_mapped_mutation/dir/rmdir";
    struct Cleanup {
        vfs::File* directory{};
        bool mounted{};
        ~Cleanup() {
            if (directory != nullptr) vfs::vfs_put_file(directory);
            for (auto path : {ALIAS, FILE, RENAMED, NEWLINK, REMOVE}) static_cast<void>(vfs::vfs_unlink(path));
            for (auto path : {NEWDIR, RMDIR, PRIVATE, DIR}) static_cast<void>(vfs::vfs_rmdir(path));
            if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
            static_cast<void>(vfs::vfs_rmdir(BASE));
        }
    } cleanup;
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
    if (disk) {
        auto* device = ker::dev::block_device_find_by_name("sdb2");
        KREQUIRE_TRUE(device != nullptr);
        KREQUIRE_EQ(vfs::mount_filesystem(BASE, "xfs", device), 0);
        cleanup.mounted = true;
    }
    KREQUIRE_EQ(vfs::vfs_mkdir(DIR, 0755), 0);
    KREQUIRE_EQ(vfs::vfs_chown(DIR, 1000, 1000), 0);
    KREQUIRE_EQ(vfs::vfs_mkdir(PRIVATE, 0700), 0);
    ker::mod::sched::task::Task worker{};
    std::strcpy(worker.root.data(), "/unrelated_worker_root");
    std::strcpy(worker.cwd.data(), "/unrelated_worker_cwd");
    auto const ROOT = worker.credentials();
    auto user = ROOT;
    std::array<uint32_t, 1> groups{2000};
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GROUPS, .groups = groups}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GID, .real = 1000}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    auto make_file = [&](const char* path) {
        auto* file = vfs::vfs_open_file(path, vfs::O_CREAT | vfs::O_EXCL | 1, 0644);
        if (file == nullptr) return false;
        bool const WRITTEN = file->fops->vfs_write(file, "seed", 4, 0) == 4;
        vfs::vfs_put_file(file);
        return WRITTEN && vfs::vfs_chown(path, 1000, 1000) == 0;
    };
    auto stat = [&](const char* path, vfs::Stat& output) { return vfs::vfs_stat_resolved_beneath_as(worker, ROOT, BASE, path, &output); };
    struct Case {
        const char* visible;
        const char* target;
        int user_result;
        int root_result;
    };
    for (auto const& test :
         {Case{"/shared", "/shared/dir", 0, 0}, Case{"//shared///", "/shared/dir", 0, 0}, Case{"/", "/dir", 0, 0},
          Case{"/shared", "./dir", 0, 0}, Case{"/shared", "/shared/private/../dir", -EACCES, 0},
          Case{"/shared", "/shared_other/dir", -EPERM, -EPERM}, Case{"/shared", "/shared/../shared/dir", -EPERM, -EPERM},
          Case{"/shared/..", "/dir", -EINVAL, -EINVAL}, Case{nullptr, DIR, 0, 0}, Case{nullptr, "/dir", -EPERM, -EPERM}}) {
        KREQUIRE_EQ(vfs::vfs_symlink(test.target, ALIAS), 0);
        for (auto const& caller : {ROOT, user})
            for (unsigned route = 0; route < 3; ++route) {
                bool const RELATIVE = route != 0;
                bool const READONLY = route == 2;
                if (RELATIVE) {
                    vfs::File* root = nullptr;
                    KREQUIRE_EQ(vfs::vfs_pin_directory_resolved_beneath_as(worker, ROOT, BASE, BASE, &root), 0);
                    vfs::DirectoryViewRef view;
                    int const PREPARED =
                        vfs::vfs_prepare_directory_view(ROOT, root, root, {.read_only = READONLY, .nosuid = READONLY}, view);
                    vfs::vfs_put_file(root);
                    KREQUIRE_EQ(PREPARED, 0);
                    KREQUIRE_EQ(vfs::vfs_pin_directory_view_as(worker, ROOT, view, ".", &cleanup.directory), 0);
                }
                int const LOOKUP = caller == user ? test.user_result : test.root_result;
                int const EXPECTED = LOOKUP != 0 ? LOOKUP : READONLY ? -EROFS : 0;
                auto name = [&](const char* physical) { return RELATIVE ? physical + std::strlen(BASE) + 1 : physical; };
                constexpr auto ALIAS_FILE = "/tmp/ktest_mapped_mutation/alias/file";
                constexpr auto ALIAS_RENAMED = "/tmp/ktest_mapped_mutation/alias/renamed";
                KREQUIRE_TRUE(make_file(FILE) && make_file(REMOVE));
                KREQUIRE_EQ(vfs::vfs_mkdir(RMDIR, 0755), 0);
                vfs::Stat before{}, after{};
                KREQUIRE_EQ(stat(FILE, before), 0);
                KEXPECT_EQ(vfs::vfs_chmod_resolved_beneath_as(worker, caller, BASE, name(ALIAS_FILE), 0600, true, false, cleanup.directory,
                                                              test.visible),
                           EXPECTED);
                KREQUIRE_EQ(stat(FILE, after), 0);
                if (EXPECTED == 0)
                    KEXPECT_EQ(after.st_mode & 0777, 0600);
                else
                    KEXPECT_TRUE(std::memcmp(&before, &after, sizeof(before)) == 0);
                before = after;
                KEXPECT_EQ(vfs::vfs_chown_resolved_beneath_as(worker, caller, BASE, name(ALIAS_FILE), 1000, 2000, true, false,
                                                              cleanup.directory, test.visible),
                           EXPECTED);
                KREQUIRE_EQ(stat(FILE, after), 0);
                if (EXPECTED == 0)
                    KEXPECT_EQ(after.st_gid, 2000);
                else
                    KEXPECT_TRUE(std::memcmp(&before, &after, sizeof(before)) == 0);
                before = after;
                std::array<vfs::Timespec, 2> times{{{123, 0}, {456, 0}}};
                KEXPECT_EQ(vfs::vfs_utimens_resolved_beneath_as(worker, caller, BASE, name(ALIAS_FILE), times.data(), true, false,
                                                                cleanup.directory, test.visible),
                           EXPECTED);
                KREQUIRE_EQ(stat(FILE, after), 0);
                if (EXPECTED == 0)
                    KEXPECT_EQ(after.st_mtim.tv_sec, 456);
                else
                    KEXPECT_TRUE(std::memcmp(&before, &after, sizeof(before)) == 0);
                KEXPECT_EQ(vfs::vfs_mkdir_resolved_beneath_as(worker, caller, BASE, name("/tmp/ktest_mapped_mutation/alias/newdir"), 0750,
                                                              cleanup.directory, test.visible),
                           EXPECTED);
                KEXPECT_EQ(stat(NEWDIR, after), EXPECTED == 0 ? 0 : -ENOENT);
                constexpr auto LITERAL = "/literal//../target";
                KEXPECT_EQ(
                    vfs::vfs_symlink_resolved_beneath_as(worker, caller, BASE, LITERAL, name("/tmp/ktest_mapped_mutation/alias/newlink"),
                                                         false, cleanup.directory, test.visible),
                    EXPECTED);
                std::array<char, 64> text{};
                auto const LINK_LENGTH = vfs::vfs_readlink_resolved_beneath_as(worker, ROOT, BASE, NEWLINK, text.data(), text.size());
                KEXPECT_EQ(LINK_LENGTH, EXPECTED == 0 ? static_cast<ssize_t>(std::strlen(LITERAL)) : -ENOENT);
                if (EXPECTED == 0) KEXPECT_TRUE(std::memcmp(text.data(), LITERAL, std::strlen(LITERAL)) == 0);
                KEXPECT_EQ(vfs::vfs_unlink_resolved_beneath_as(worker, caller, BASE, name("/tmp/ktest_mapped_mutation/alias/remove"), false,
                                                               false, cleanup.directory, test.visible),
                           EXPECTED);
                KEXPECT_EQ(stat(REMOVE, after), EXPECTED == 0 ? -ENOENT : 0);
                KEXPECT_EQ(vfs::vfs_unlink_resolved_beneath_as(worker, caller, BASE, name("/tmp/ktest_mapped_mutation/alias/rmdir"), true,
                                                               false, cleanup.directory, test.visible),
                           EXPECTED);
                KEXPECT_EQ(stat(RMDIR, after), EXPECTED == 0 ? -ENOENT : 0);
                // Resolve the destination alias independently, then the source alias.
                KEXPECT_EQ(vfs::vfs_rename_resolved_beneath_as(worker, caller, BASE, name(FILE), name(ALIAS_RENAMED), false, false,
                                                               cleanup.directory, cleanup.directory, test.visible),
                           EXPECTED);
                KEXPECT_EQ(stat(FILE, after), EXPECTED == 0 ? -ENOENT : 0);
                KEXPECT_EQ(stat(RENAMED, after), EXPECTED == 0 ? 0 : -ENOENT);
                KEXPECT_EQ(vfs::vfs_rename_resolved_beneath_as(worker, caller, BASE, name(EXPECTED == 0 ? ALIAS_RENAMED : ALIAS_FILE),
                                                               name(EXPECTED == 0 ? FILE : RENAMED), false, false, cleanup.directory,
                                                               cleanup.directory, test.visible),
                           EXPECTED);
                KREQUIRE_EQ(stat(FILE, after), 0);
                KEXPECT_EQ(after.st_size, 4);
                KEXPECT_EQ(stat(RENAMED, after), -ENOENT);
                if (cleanup.directory != nullptr) {
                    vfs::vfs_put_file(cleanup.directory);
                    cleanup.directory = nullptr;
                }
                for (auto path : {FILE, RENAMED, REMOVE, NEWLINK}) static_cast<void>(vfs::vfs_unlink(path));
                for (auto path : {NEWDIR, RMDIR}) static_cast<void>(vfs::vfs_rmdir(path));
            }
        KREQUIRE_EQ(vfs::vfs_unlink(ALIAS), 0);
    }
    KEXPECT_TRUE(worker.credentials() == ROOT);
}
}  // namespace
KTEST(Credentials, TmpfsMappedMutationsUseCapturedSymlinkNamespace) { check_mapped_mutation_symlink_namespace(false); }
KTEST(Credentials, XfsMappedMutationsUseCapturedSymlinkNamespace) { check_mapped_mutation_symlink_namespace(true); }

KTEST(Credentials, ExplicitOwnerReadlinkPreservesSearchAuthorityAndOutput) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_owner_readlink";
    constexpr auto FILE = "/tmp/ktest_owner_readlink/file";
    constexpr auto LINK = "/tmp/ktest_owner_readlink/link";
    constexpr auto CLOSED = "/tmp/ktest_owner_readlink/closed";
    constexpr auto HIDDEN = "/tmp/ktest_owner_readlink/closed/link";
    constexpr auto ESCAPE = "/tmp/ktest_owner_readlink/escape";
    constexpr auto DIRLINK = "/tmp/ktest_owner_readlink/directory";
    constexpr auto OUTSIDE = "/tmp/ktest_owner_readlink_outside";
    struct Cleanup {
        ~Cleanup() {
            for (auto path : {HIDDEN, LINK, ESCAPE, DIRLINK, OUTSIDE, FILE}) static_cast<void>(vfs::vfs_unlink(path));
            static_cast<void>(vfs::vfs_rmdir(CLOSED));
            static_cast<void>(vfs::vfs_rmdir(BASE));
        }
    } cleanup;
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0700), 0);
    auto* file = vfs::vfs_open_file(FILE, vfs::O_CREAT | vfs::O_EXCL | 1, 0000);
    KREQUIRE_TRUE(file != nullptr);
    KREQUIRE_EQ(vfs::vfs_close_file(file), 0);
    KREQUIRE_EQ(vfs::vfs_mkdir(CLOSED, 0700), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("file", LINK), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("../file", HIDDEN), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("/tmp", ESCAPE), 0);
    KREQUIRE_EQ(vfs::vfs_symlink(".", DIRLINK), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("outside text", OUTSIDE), 0);
    ker::mod::sched::task::Task worker{};
    std::strcpy(worker.root.data(), "/not_the_export");
    std::strcpy(worker.cwd.data(), "/not_the_directory");
    auto const WORKER = worker.credentials();
    auto user = WORKER;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GID, .real = 2001}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    auto grouped = WORKER;
    std::array<uint32_t, 2> groups{17, 2000};
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_GROUPS, .groups = groups}, grouped), 0);
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_GID, .real = 2001}, grouped), 0);
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_UID, .real = 1000}, grouped), 0);
    std::array<char, 512> output{};
    output.fill('Z');
    auto const SENTINEL = output;
    auto readlink = [&](const security::Snapshot& caller, const char* path, size_t size = 512, bool directory = false) {
        return vfs::vfs_readlink_resolved_beneath_as(worker, caller, BASE, path, output.data(), size, directory);
    };
    auto const GENERATION = vfs::tmpfs::tmpfs_namespace_generation();
    KEXPECT_EQ(readlink(user, LINK), static_cast<ssize_t>(-EACCES));
    KEXPECT_TRUE(output == SENTINEL);
    KEXPECT_EQ(vfs::tmpfs::tmpfs_namespace_generation(), GENERATION);
    KREQUIRE_EQ(vfs::vfs_chown(BASE, 0, 2000), 0);
    KREQUIRE_EQ(vfs::vfs_chmod(BASE, 0710), 0);
    KEXPECT_EQ(readlink(user, LINK), static_cast<ssize_t>(-EACCES));
    KEXPECT_TRUE(output == SENTINEL);
    KREQUIRE_EQ(readlink(grouped, LINK), 4);
    KEXPECT_TRUE(std::memcmp(output.data(), "file", 4) == 0);
    KEXPECT_EQ(output[4], 'Z');
    output = SENTINEL;
    KREQUIRE_EQ(readlink(grouped, LINK, 2), 2);
    KEXPECT_TRUE(std::memcmp(output.data(), "fi", 2) == 0);
    KEXPECT_EQ(output[2], 'Z');
    output = SENTINEL;
    KEXPECT_EQ(readlink(grouped, HIDDEN), static_cast<ssize_t>(-EACCES));
    KEXPECT_TRUE(output == SENTINEL);
    KEXPECT_EQ(readlink(grouped, FILE), static_cast<ssize_t>(-EINVAL));
    KEXPECT_TRUE(output == SENTINEL);
    KEXPECT_EQ(readlink(grouped, "/tmp/ktest_owner_readlink/missing"), static_cast<ssize_t>(-ENOENT));
    KEXPECT_TRUE(output == SENTINEL);
    KEXPECT_EQ(readlink(grouped, LINK, 512, true), static_cast<ssize_t>(-ENOTDIR));
    KEXPECT_TRUE(output == SENTINEL);
    KEXPECT_EQ(readlink(grouped, DIRLINK, 512, true), static_cast<ssize_t>(-EINVAL));
    KEXPECT_TRUE(output == SENTINEL);
    KREQUIRE_EQ(readlink(grouped, ESCAPE), 4);
    KEXPECT_TRUE(std::memcmp(output.data(), "/tmp", 4) == 0);
    output = SENTINEL;
    KEXPECT_EQ(readlink(grouped, "/tmp/ktest_owner_readlink/escape/ktest_owner_readlink_outside"), static_cast<ssize_t>(-EPERM));
    KEXPECT_TRUE(output == SENTINEL);
    KEXPECT_EQ(readlink(grouped, OUTSIDE), static_cast<ssize_t>(-EPERM));
    KEXPECT_TRUE(output == SENTINEL);
    KEXPECT_EQ(readlink(grouped, LINK, 0), static_cast<ssize_t>(-EINVAL));
    KEXPECT_EQ(readlink(grouped, LINK, 513), static_cast<ssize_t>(-EMSGSIZE));
    KEXPECT_TRUE(output == SENTINEL);
    KEXPECT_EQ(vfs::vfs_readlink_resolved_beneath_as(worker, grouped, "/proc", "/proc/self", output.data(), output.size()),
               static_cast<ssize_t>(-ENOTSUP));
    KEXPECT_TRUE(output == SENTINEL);
    KREQUIRE_EQ(vfs::vfs_readlink_resolved_beneath_as(worker, user, "/dev", "/dev/stdout", output.data(), output.size()),
                sizeof("/proc/self/fd/1") - 1);
    KEXPECT_TRUE(std::memcmp(output.data(), "/proc/self/fd/1", sizeof("/proc/self/fd/1") - 1) == 0);
    output = SENTINEL;
    KEXPECT_EQ(vfs::vfs_readlink_resolved_beneath_as(worker, user, "/dev", "/dev/null", output.data(), output.size()),
               static_cast<ssize_t>(-EINVAL));
    KEXPECT_TRUE(output == SENTINEL);
    KEXPECT_TRUE(worker.credentials() == WORKER);
    KEXPECT_TRUE(std::strcmp(worker.root.data(), "/not_the_export") == 0);
    KEXPECT_TRUE(std::strcmp(worker.cwd.data(), "/not_the_directory") == 0);
}

KTEST(Credentials, ExplicitOwnerStatUsesSearchAuthorityAndPreservesDeniedOutput) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_owner_stat";
    constexpr auto FILE = "/tmp/ktest_owner_stat/file";
    constexpr auto LINK = "/tmp/ktest_owner_stat/link";
    constexpr auto ESCAPE = "/tmp/ktest_owner_stat/escape";
    constexpr auto CLOSED = "/tmp/ktest_owner_stat/closed";
    constexpr auto CLOSED_LINK = "/tmp/ktest_owner_stat/closed/link";
    struct Cleanup {
        ~Cleanup() {
            static_cast<void>(vfs::vfs_unlink(CLOSED_LINK));
            static_cast<void>(vfs::vfs_rmdir(CLOSED));
            static_cast<void>(vfs::vfs_unlink(ESCAPE));
            static_cast<void>(vfs::vfs_unlink(LINK));
            static_cast<void>(vfs::vfs_unlink(FILE));
            static_cast<void>(vfs::vfs_rmdir(BASE));
        }
    } cleanup;
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0700), 0);
    auto* file = vfs::vfs_open_file(FILE, vfs::O_CREAT | 1, 0000);
    KREQUIRE_TRUE(file != nullptr);
    KREQUIRE_EQ(vfs::vfs_close_file(file), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("file", LINK), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("/tmp", ESCAPE), 0);
    KREQUIRE_EQ(vfs::vfs_mkdir(CLOSED, 0700), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("../file", CLOSED_LINK), 0);
    ker::mod::sched::task::Task worker{};
    // Owner paths must not be prefixed with the serving task's root or cwd.
    std::strcpy(worker.root.data(), "/not_the_export");
    std::strcpy(worker.cwd.data(), "/not_the_directory");
    auto const WORKER = worker.credentials();
    auto user = WORKER;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GID, .real = 2001}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    auto grouped = WORKER;
    std::array<uint32_t, 2> groups{17, 2000};
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_GROUPS, .groups = groups}, grouped), 0);
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_GID, .real = 2001}, grouped), 0);
    KREQUIRE_EQ(grouped.prepare({.change = security::Change::SET_UID, .real = 1000}, grouped), 0);
    vfs::Stat output{};
    auto stat = [&](const security::Snapshot& caller, const char* path, bool follow = true) {
        return vfs::vfs_stat_resolved_beneath_as(worker, caller, BASE, path, &output, follow);
    };
    KREQUIRE_EQ(stat(WORKER, FILE), 0);
    KEXPECT_EQ(output.st_mode & 0777, 0);
    auto const SENTINEL = output;
    auto const GENERATION = vfs::tmpfs::tmpfs_namespace_generation();
    KEXPECT_EQ(stat(user, FILE), -EACCES);
    KEXPECT_TRUE(std::memcmp(&output, &SENTINEL, sizeof(output)) == 0);
    KEXPECT_EQ(vfs::tmpfs::tmpfs_namespace_generation(), GENERATION);
    KREQUIRE_EQ(vfs::vfs_chown(BASE, 0, 2000), 0);
    KREQUIRE_EQ(vfs::vfs_chmod(BASE, 0750), 0);
    KEXPECT_EQ(stat(user, FILE), -EACCES);
    KREQUIRE_EQ(stat(grouped, FILE), 0);
    KEXPECT_EQ(output.st_mode & 0777, 0);  // STAT needs search, not file-read access.
    auto const SEARCH_SENTINEL = output;
    KEXPECT_EQ(stat(grouped, CLOSED_LINK), -EACCES);
    KEXPECT_TRUE(std::memcmp(&output, &SEARCH_SENTINEL, sizeof(output)) == 0);
    KREQUIRE_EQ(vfs::vfs_chmod(FILE, 0644), 0);
    vfs::File* opened = nullptr;
    KEXPECT_EQ(vfs::vfs_open_file_resolved_beneath_as(worker, grouped, BASE, CLOSED_LINK, 0, 0, &opened), -EACCES);
    KEXPECT_TRUE(opened == nullptr);
    if (opened != nullptr) {
        static_cast<void>(vfs::vfs_close_file(opened));
        opened = nullptr;
    }
    KREQUIRE_EQ(vfs::vfs_open_file_resolved_beneath_as(worker, grouped, BASE, FILE, 0, 0, &opened), 0);
    KEXPECT_EQ(vfs::vfs_close_file(opened), 0);
    KREQUIRE_EQ(stat(grouped, LINK, false), 0);
    KEXPECT_EQ(output.st_mode & vfs::S_IFMT, vfs::S_IFLNK);
    KREQUIRE_EQ(stat(grouped, LINK), 0);
    KEXPECT_EQ(output.st_mode & vfs::S_IFMT, vfs::S_IFREG);
    KEXPECT_EQ(stat(grouped, "/tmp/ktest_owner_stat/file/"), -ENOTDIR);
    auto const DIRECTORY_SENTINEL = output;
    KEXPECT_EQ(vfs::vfs_stat_resolved_beneath_as(worker, grouped, BASE, FILE, &output, false, true), -ENOTDIR);
    KEXPECT_TRUE(std::memcmp(&output, &DIRECTORY_SENTINEL, sizeof(output)) == 0);
    KREQUIRE_EQ(vfs::vfs_stat_resolved_beneath_as(worker, grouped, BASE, BASE, &output, false, true), 0);
    KEXPECT_EQ(output.st_mode & vfs::S_IFMT, vfs::S_IFDIR);
    KEXPECT_EQ(stat(grouped, "/tmp/ktest_owner_stat/missing"), -ENOENT);
    KEXPECT_EQ(stat(grouped, ESCAPE), -EPERM);
    KREQUIRE_EQ(stat(grouped, ESCAPE, false), 0);
    KEXPECT_EQ(output.st_mode & vfs::S_IFMT, vfs::S_IFLNK);
    auto const LAST = output;
    KEXPECT_EQ(vfs::vfs_stat_resolved_beneath_as(worker, user, nullptr, FILE, &output), -EINVAL);
    KEXPECT_TRUE(std::memcmp(&output, &LAST, sizeof(output)) == 0);
    KEXPECT_EQ(vfs::vfs_stat_resolved_beneath_as(worker, user, "/proc", "/proc", &output), -ENOTSUP);
    KEXPECT_TRUE(std::memcmp(&output, &LAST, sizeof(output)) == 0);
    KREQUIRE_EQ(vfs::vfs_stat_resolved_beneath_as(worker, user, "/dev", "/dev/null", &output), 0);
    KEXPECT_EQ(output.st_mode & vfs::S_IFMT, vfs::S_IFCHR);
    KEXPECT_TRUE(worker.credentials() == WORKER);
    KEXPECT_TRUE(std::strcmp(worker.root.data(), "/not_the_export") == 0);
    KEXPECT_TRUE(std::strcmp(worker.cwd.data(), "/not_the_directory") == 0);
}

KTEST(Credentials, ExplicitOwnerXfsOpenCompletesTruncationAfterAuthorization) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_owner_xfs";
    constexpr auto FILE = "/tmp/ktest_owner_xfs/owned";
    constexpr auto LINK = "/tmp/ktest_owner_xfs/link";
    auto* device = ker::dev::block_device_find_by_name("sdb2");
    KREQUIRE_TRUE(device != nullptr);
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
    struct Cleanup {
        vfs::File* opened{};
        bool mounted{};
        ~Cleanup() {
            if (opened != nullptr) static_cast<void>(vfs::vfs_close_file(opened));
            if (mounted) {
                static_cast<void>(vfs::vfs_unlink(LINK));
                static_cast<void>(vfs::vfs_unlink(FILE));
                static_cast<void>(vfs::unmount_filesystem(BASE));
            }
            static_cast<void>(vfs::vfs_rmdir(BASE));
        }
    } cleanup;
    KREQUIRE_EQ(vfs::mount_filesystem(BASE, "xfs", device), 0);
    cleanup.mounted = true;
    ker::mod::sched::task::Task worker{};
    auto const ROOT = worker.credentials();
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    auto open = [&](const security::Snapshot& caller, int flags) {
        return vfs::vfs_open_file_resolved_beneath_as(worker, caller, BASE, FILE, flags, 0600, &cleanup.opened);
    };
    auto close = [&] {
        int const result = vfs::vfs_close_file(cleanup.opened);
        cleanup.opened = nullptr;
        return result;
    };
    KREQUIRE_EQ(open(ROOT, vfs::O_CREAT | vfs::O_EXCL | 2), 0);
    KEXPECT_EQ(cleanup.opened->fops->vfs_write(cleanup.opened, "preserve", 8, 0), 8);
    KEXPECT_EQ(close(), 0);
    KEXPECT_EQ(open(user, vfs::O_TRUNC | 1), -EACCES);
    KEXPECT_TRUE(cleanup.opened == nullptr);
    vfs::Stat stat{};
    KEXPECT_EQ(vfs::vfs_stat(FILE, &stat), 0);
    KEXPECT_EQ(stat.st_size, 8);
    KREQUIRE_EQ(vfs::vfs_stat_resolved_beneath_as(worker, user, BASE, FILE, &stat), 0);
    KEXPECT_EQ(stat.st_size, 8);
    KEXPECT_EQ(stat.st_mode & 0777, 0600);
    KREQUIRE_EQ(open(ROOT, vfs::O_TRUNC | 1), 0);
    KEXPECT_EQ(vfs::vfs_fstat_file(cleanup.opened, &stat), 0);
    KEXPECT_EQ(stat.st_size, 0);
    KEXPECT_EQ(close(), 0);
    KEXPECT_EQ(vfs::vfs_stat(FILE, &stat), 0);
    KEXPECT_EQ(stat.st_size, 0);
    KREQUIRE_EQ(vfs::vfs_symlink("owned", LINK), 0);
    std::array<char, 16> text{};
    text.fill('Z');
    KREQUIRE_EQ(vfs::vfs_readlink_resolved_beneath_as(worker, user, BASE, LINK, text.data(), text.size()), 5);
    KEXPECT_TRUE(std::memcmp(text.data(), "owned", 5) == 0);
    KEXPECT_EQ(text[5], 'Z');
    auto const BEFORE = text;
    KEXPECT_EQ(vfs::vfs_readlink_resolved_beneath_as(worker, user, BASE, FILE, text.data(), text.size()), static_cast<ssize_t>(-EINVAL));
    KEXPECT_TRUE(text == BEFORE);
    KEXPECT_TRUE(worker.credentials() == ROOT);
}

KTEST(Credentials, ExplicitOwnerStatReadsReadonlyFatMetadata) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_owner_fat_stat";
    constexpr auto FILE = "/tmp/ktest_owner_fat_stat/wos";
    auto* device = ker::dev::block_device_find_by_name("sda1");
    KREQUIRE_TRUE(device != nullptr);
    // Mount flags are not the device read-only contract in WOS. Use an
    // unregistered read-only view; no write can reach the isolated boot disk.
    ker::dev::BlockDevice view{};
    view.block_size = device->block_size;
    view.total_blocks = device->total_blocks;
    view.is_partition = true;
    view.read_only = true;
    view.private_data = device;
    view.read_blocks = [](ker::dev::BlockDevice* self, uint64_t block, size_t count, void* buffer) -> int {
        return ker::dev::block_read(static_cast<ker::dev::BlockDevice*>(self->private_data), block, count, buffer);
    };
    view.write_blocks = [](ker::dev::BlockDevice*, uint64_t, size_t, const void*) -> int { return -EROFS; };
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
    struct Cleanup {
        bool mounted = false;
        ~Cleanup() {
            if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
            static_cast<void>(vfs::vfs_rmdir(BASE));
        }
    } cleanup;
    KREQUIRE_EQ(vfs::mount_filesystem(BASE, "fat32", &view), 0);
    cleanup.mounted = true;
    ker::mod::sched::task::Task worker{};
    auto const WORKER = worker.credentials();
    auto user = WORKER;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GID, .real = 2000}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    vfs::Stat result{};
    KREQUIRE_EQ(vfs::vfs_stat_resolved_beneath_as(worker, user, BASE, FILE, &result), 0);
    KEXPECT_EQ(result.st_mode & vfs::S_IFMT, vfs::S_IFREG);
    KEXPECT_EQ(result.st_uid, 0);
    KEXPECT_EQ(result.st_gid, 0);
    KEXPECT_TRUE(result.st_size > 0);
    auto const BEFORE = result;
    KEXPECT_EQ(vfs::vfs_stat_resolved_beneath_as(worker, user, BASE, "/tmp/ktest_owner_fat_stat/wos/", &result), -ENOTDIR);
    KEXPECT_TRUE(std::memcmp(&result, &BEFORE, sizeof(result)) == 0);
    KEXPECT_EQ(vfs::vfs_stat_resolved_beneath_as(worker, user, BASE, "/tmp/ktest_owner_fat_stat/absent", &result), -ENOENT);
    KEXPECT_TRUE(std::memcmp(&result, &BEFORE, sizeof(result)) == 0);
    std::array<char, 16> text{};
    text.fill('Z');
    auto const TEXT_BEFORE = text;
    KEXPECT_EQ(vfs::vfs_readlink_resolved_beneath_as(worker, user, BASE, FILE, text.data(), text.size()), static_cast<ssize_t>(-ENOSYS));
    KEXPECT_TRUE(text == TEXT_BEFORE);
    KEXPECT_TRUE(worker.credentials() == WORKER);
}

KTEST(Credentials, ExplicitOwnerReadCannotEscapeExport) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_read_export";
    constexpr auto EXPORT = "/tmp/ktest_read_export/export";
    constexpr auto FILE = "/tmp/ktest_read_export/export/file";
    constexpr auto OUTSIDE = "/tmp/ktest_read_export/outside";
    constexpr auto ESCAPE = "/tmp/ktest_read_export/export/escape";
    struct Cleanup {
        vfs::File* opened{};
        ~Cleanup() {
            if (opened != nullptr) static_cast<void>(vfs::vfs_close_file(opened));
            static_cast<void>(vfs::vfs_unlink(ESCAPE));
            static_cast<void>(vfs::vfs_unlink(FILE));
            static_cast<void>(vfs::vfs_unlink(OUTSIDE));
            static_cast<void>(vfs::vfs_rmdir(EXPORT));
            static_cast<void>(vfs::vfs_rmdir(BASE));
        }
    } cleanup;
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
    KREQUIRE_EQ(vfs::vfs_mkdir(EXPORT, 0755), 0);
    for (auto path : {FILE, OUTSIDE}) {
        auto* file = vfs::vfs_open_file(path, vfs::O_CREAT, 0644);
        KREQUIRE_TRUE(file != nullptr);
        KEXPECT_EQ(vfs::vfs_close_file(file), 0);
    }
    ker::mod::sched::task::Task worker{};
    // The owner receives a physical path, even after its worker pivots root.
    std::strcpy(worker.root.data(), "/tmp");
    std::strcpy(worker.cwd.data(), "/");
    auto const WORKER_CREDENTIALS = worker.credentials();
    auto user = WORKER_CREDENTIALS;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GID, .real = 1000}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    KREQUIRE_EQ(vfs::vfs_open_read_file_resolved_beneath_as(worker, user, EXPORT, FILE, &cleanup.opened), 0);
    KEXPECT_TRUE(cleanup.opened != nullptr);
    KEXPECT_EQ(vfs::vfs_close_file(cleanup.opened), 0);
    cleanup.opened = nullptr;
    KREQUIRE_EQ(vfs::vfs_chmod(FILE, 0600), 0);
    KEXPECT_EQ(vfs::vfs_open_read_file_resolved_beneath_as(worker, user, EXPORT, FILE, &cleanup.opened), -EACCES);
    KEXPECT_TRUE(cleanup.opened == nullptr);
    KREQUIRE_EQ(vfs::vfs_symlink("../outside", ESCAPE), 0);
    for (auto const& credentials : {user, WORKER_CREDENTIALS}) {
        KEXPECT_EQ(vfs::vfs_open_read_file_resolved_beneath_as(worker, credentials, EXPORT, ESCAPE, &cleanup.opened), -EPERM);
        KEXPECT_EQ(vfs::vfs_open_read_file_resolved_beneath_as(worker, credentials, EXPORT, OUTSIDE, &cleanup.opened), -EPERM);
        KEXPECT_TRUE(cleanup.opened == nullptr);
    }
    KEXPECT_EQ(vfs::vfs_open_read_file_resolved_beneath_as(worker, user, nullptr, FILE, &cleanup.opened), -EINVAL);
    KEXPECT_TRUE(worker.credentials() == WORKER_CREDENTIALS && cleanup.opened == nullptr);
}

KTEST(ExecContent, IdentityLifetimeAndDirectWriterExclusion) {
    ker::vfs::ExecContentState state;
    ker::vfs::ExecReadView first, second, after;
    KREQUIRE_EQ(state.acquire(first), 0);
    KEXPECT_TRUE(first.content_id != 0 && !state.can_mutate());
    KREQUIRE_EQ(state.acquire(second), 0);
    KEXPECT_EQ(first.content_id, second.content_id);
    state.release();
    KEXPECT_TRUE(!state.can_mutate());
    state.release();
    KEXPECT_TRUE(state.can_mutate());
    state.direct_writers = 1;
    KEXPECT_EQ(state.acquire(after), -ETXTBSY);
    KEXPECT_EQ(after.content_id, 0);
    state.direct_writers = 0;
    KREQUIRE_EQ(state.acquire(after), 0);
    KEXPECT_TRUE(after.content_id != first.content_id);
    state.release();
}

KTEST(ExecContent, TmpfsPrivateViewRetainsBytesMetadataAndReleasesIndependently) {
    namespace vfs = ker::vfs;
    constexpr const char* PATH = "/tmp/ktest_exec_content";
    struct Cleanup {
        vfs::File* source{};
        vfs::File* first{};
        vfs::File* second{};
        ~Cleanup() {
            vfs::vfs_put_file(first);
            vfs::vfs_put_file(second);
            vfs::vfs_put_file(source);
            vfs::vfs_unlink("/tmp/ktest_exec_content");
        }
    } cleanup;
    cleanup.source = vfs::vfs_open_file(PATH, vfs::O_CREAT | vfs::O_EXCL | vfs::O_LOCAL | 2, 0755);
    KREQUIRE_TRUE(cleanup.source != nullptr);
    auto* ops = cleanup.source->fops;
    KREQUIRE_EQ(ops->vfs_write(cleanup.source, "old", 3, 0), 3);
    cleanup.first = cleanup.source;
    vfs::vfs_retain_file(cleanup.first);
    KREQUIRE_EQ(vfs::vfs_stabilize_exec_file(&cleanup.first), 0);
    KREQUIRE_TRUE(cleanup.first != cleanup.source && cleanup.first->exec_read_view != nullptr);
    KEXPECT_TRUE(cleanup.source->exec_read_view == nullptr);
    auto const ID = cleanup.first->exec_read_view->content_id;
    KEXPECT_EQ(ops->vfs_write(cleanup.source, "new", 3, 0), static_cast<ssize_t>(-ETXTBSY));
    KEXPECT_EQ(ops->vfs_truncate(cleanup.source, 0), -ETXTBSY);
    KEXPECT_EQ(ops->vfs_truncate(cleanup.source, 3), -ETXTBSY);
    KEXPECT_EQ(vfs::tmpfs::tmpfs_write_append(cleanup.source, "more", 4, nullptr), static_cast<ssize_t>(-ETXTBSY));
    KEXPECT_EQ(vfs::vfs_chmod(PATH, 0700), 0);
    vfs::Stat frozen{};
    KREQUIRE_EQ(vfs::vfs_fstat_file(cleanup.first, &frozen), 0);
    KEXPECT_EQ(frozen.st_mode & 0777, 0755);
    char bytes[4]{};
    KEXPECT_EQ(cleanup.first->fops->vfs_read(cleanup.first, bytes, 3, 0), 3);
    KEXPECT_TRUE(std::memcmp(bytes, "old", 3) == 0);
    cleanup.second = cleanup.source;
    vfs::vfs_retain_file(cleanup.second);
    KREQUIRE_EQ(vfs::vfs_stabilize_exec_file(&cleanup.second), 0);
    KEXPECT_EQ(cleanup.second->exec_read_view->content_id, ID);
    vfs::vfs_put_file(cleanup.first);
    cleanup.first = nullptr;
    KEXPECT_EQ(ops->vfs_write(cleanup.source, "new", 3, 0), static_cast<ssize_t>(-ETXTBSY));
    vfs::vfs_put_file(cleanup.second);
    cleanup.second = nullptr;
    KEXPECT_EQ(ops->vfs_write(cleanup.source, "new", 3, 0), 3);
    cleanup.first = cleanup.source;
    vfs::vfs_retain_file(cleanup.first);
    KREQUIRE_EQ(vfs::vfs_stabilize_exec_file(&cleanup.first), 0);
    KEXPECT_TRUE(cleanup.first->exec_read_view->content_id != ID);
}

KTEST(ExecContent, ReplacementPreservesSourceViewPolicyAndIndependentLifetime) {
    namespace vfs = ker::vfs;
    constexpr const char* BASE = "/tmp/ktest_exec_view_policy";
    constexpr const char* PATH = "/tmp/ktest_exec_view_policy/file";
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GID, .real = 100}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 100}, user), 0);
    for (bool disk : {false, true}) {
        struct Cleanup {
            vfs::File* source{};
            vfs::File* first{};
            vfs::File* second{};
            bool mounted{};
            ~Cleanup() {
                vfs::vfs_put_file(second);
                vfs::vfs_put_file(first);
                vfs::vfs_put_file(source);
                static_cast<void>(vfs::vfs_unlink(PATH));
                if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
                static_cast<void>(vfs::vfs_rmdir(BASE));
            }
        } cleanup;
        KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
        if (disk) {
            auto* device = ker::dev::block_device_find_by_name("sdb2");
            KREQUIRE_TRUE(device != nullptr);
            KREQUIRE_EQ(vfs::mount_filesystem(BASE, "xfs", device), 0);
            cleanup.mounted = true;
        }
        for (unsigned policy = 0; policy < 4; ++policy) {
            bool const READONLY = (policy & 1U) != 0;
            bool const NOSUID = (policy & 2U) != 0;
            cleanup.source = vfs::vfs_open_file(PATH, vfs::O_CREAT | vfs::O_NO_CACHE | 2, 06755);
            KREQUIRE_TRUE(cleanup.source != nullptr && cleanup.source->mount_owner != nullptr);
            KEXPECT_TRUE(!vfs::file_mount_read_only(*cleanup.source) && !vfs::file_mount_nosuid(*cleanup.source));
            KREQUIRE_EQ(vfs::vfs_pwrite_file(cleanup.source, "seed", 4, 0), static_cast<ssize_t>(4));
            KREQUIRE_EQ(vfs::vfs_fchmod_file_as(cleanup.source, ROOT, 06755), 0);
            // Model privately admitted policy before this description is shared.
            cleanup.source->mount_policy = {.read_only = READONLY, .nosuid = NOSUID};
            cleanup.first = cleanup.source;
            vfs::vfs_retain_file(cleanup.first);
            KREQUIRE_EQ(vfs::vfs_stabilize_exec_file(&cleanup.first), 0);
            KREQUIRE_TRUE(cleanup.first != cleanup.source && cleanup.first->exec_read_view != nullptr);
            KEXPECT_TRUE(cleanup.source->exec_read_view == nullptr && cleanup.source->refcount.load() == 1);
            KEXPECT_TRUE(vfs::file_mount_read_only(*cleanup.source) == READONLY && vfs::file_mount_nosuid(*cleanup.source) == NOSUID);
            cleanup.second = cleanup.first;
            vfs::vfs_retain_file(cleanup.second);
            KREQUIRE_EQ(vfs::vfs_stabilize_exec_file(&cleanup.second), 0);
            KREQUIRE_TRUE(cleanup.second != cleanup.first && cleanup.second->exec_read_view != nullptr);
            KEXPECT_EQ(cleanup.first->exec_read_view->content_id, cleanup.second->exec_read_view->content_id);
            for (auto* file : {cleanup.first, cleanup.second}) {
                KEXPECT_TRUE(file->mount_owner == cleanup.source->mount_owner);
                KEXPECT_TRUE(vfs::file_mount_read_only(*file) == READONLY && vfs::file_mount_nosuid(*file) == NOSUID);
                vfs::Stat stat{};
                KREQUIRE_EQ(vfs::vfs_fstat_file(file, &stat), 0);
                KEXPECT_EQ(stat.st_mode & 07777, 06755);
                vfs::Statvfs filesystem{};
                KREQUIRE_EQ(vfs::vfs_fstatvfs_file(file, &filesystem), 0);
                KEXPECT_TRUE(((filesystem.f_flag & vfs::ST_RDONLY) != 0) == READONLY);
                KEXPECT_TRUE(((filesystem.f_flag & vfs::ST_NOSUID) != 0) == NOSUID);
                security::Snapshot executed;
                KREQUIRE_EQ(user.prepare_exec({.uid = stat.st_uid,
                                               .gid = stat.st_gid,
                                               .set_uid = true,
                                               .set_gid = true,
                                               .nosuid = vfs::file_mount_nosuid(*file),
                                               .mode = static_cast<uint32_t>(stat.st_mode)},
                                              executed),
                            0);
                KEXPECT_EQ(executed.user().real, 100U);
                KEXPECT_EQ(executed.group().real, 100U);
                KEXPECT_EQ(executed.user().effective, NOSUID ? 100U : 0U);
                KEXPECT_EQ(executed.user().saved, NOSUID ? 100U : 0U);
                KEXPECT_EQ(executed.group().effective, NOSUID ? 100U : 0U);
                KEXPECT_EQ(executed.group().saved, NOSUID ? 100U : 0U);
                std::array<char, 4> bytes{};
                KEXPECT_EQ(vfs::vfs_pread_file(file, bytes.data(), bytes.size(), 0), static_cast<ssize_t>(bytes.size()));
                KEXPECT_TRUE(std::memcmp(bytes.data(), "seed", bytes.size()) == 0);
            }
            KEXPECT_EQ(vfs::vfs_fchmod_file_as(cleanup.second, ROOT, 0600), READONLY ? -EROFS : 0);
            KEXPECT_EQ(vfs::vfs_pwrite_file(cleanup.source, "edit", 4, 0), static_cast<ssize_t>(READONLY ? -EROFS : -ETXTBSY));
            vfs::vfs_put_file(cleanup.source);
            cleanup.source = nullptr;
            vfs::vfs_put_file(cleanup.first);
            cleanup.first = nullptr;
            std::array<char, 4> bytes{};
            KEXPECT_EQ(vfs::vfs_pread_file(cleanup.second, bytes.data(), bytes.size(), 0), static_cast<ssize_t>(bytes.size()));
            KEXPECT_TRUE(std::memcmp(bytes.data(), "seed", bytes.size()) == 0);
            vfs::vfs_put_file(cleanup.second);
            cleanup.second = nullptr;
            cleanup.source = vfs::vfs_open_file(PATH, vfs::O_NO_CACHE | 2, 0);
            KREQUIRE_TRUE(cleanup.source != nullptr);
            KEXPECT_EQ(vfs::vfs_pwrite_file(cleanup.source, "edit", 4, 0), static_cast<ssize_t>(4));
            vfs::vfs_put_file(cleanup.source);
            cleanup.source = nullptr;
        }
    }
}

KTEST(Credentials, ExplicitOwnerXattrUsesOneCallerForPathAndFile) {
    namespace vfs = ker::vfs;
    using Operation = vfs::XattrOperation;
    constexpr auto BASE = "/tmp/ktest_xattr_owner";
    constexpr auto FILE = "/tmp/ktest_xattr_owner/goal19-object";
    constexpr auto MOVED = "/tmp/ktest_xattr_owner/goal19-moved";
    constexpr auto PRIVATE = "/tmp/ktest_xattr_owner/goal19-private";
    constexpr auto LINK = "/tmp/ktest_xattr_owner/goal19-link";
    struct Cleanup {
        vfs::File* file{};
        bool mounted{};
        ~Cleanup() {
            vfs::vfs_put_file(file);
            if (mounted) {
                for (auto path : {FILE, MOVED, LINK}) static_cast<void>(vfs::vfs_unlink(path));
                static_cast<void>(vfs::vfs_rmdir(PRIVATE));
                static_cast<void>(vfs::unmount_filesystem(BASE));
            }
            static_cast<void>(vfs::vfs_rmdir(BASE));
        }
    } cleanup;
    auto* device = ker::dev::block_device_find_by_name("sdb2");
    KREQUIRE_TRUE(device != nullptr);
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
    KREQUIRE_EQ(vfs::mount_filesystem(BASE, "xfs", device), 0);
    cleanup.mounted = true;
    cleanup.file = vfs::vfs_open_file(FILE, vfs::O_CREAT | vfs::O_EXCL, 0644);
    KREQUIRE_TRUE(cleanup.file != nullptr);
    KREQUIRE_EQ(vfs::vfs_chown(FILE, 1001, 2002), 0);
    ker::mod::sched::task::Task worker{};
    std::strcpy(worker.root.data(), "/not_the_export");
    std::strcpy(worker.cwd.data(), "/not_the_directory");
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto unrelated = ROOT;
    KREQUIRE_EQ(unrelated.prepare({.change = security::Change::SET_UID, .real = 1003}, unrelated), 0);
    KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(worker, unrelated), 0);
    auto path_call = [&](const security::Snapshot& caller, const char* path, Operation op, void* output, size_t size, bool follow = true,
                         bool directory = false) {
        return vfs::vfs_xattr_resolved_beneath_as(worker, caller, BASE, path, op, op == Operation::LIST ? nullptr : "user.matrix", output,
                                                  size, op == Operation::SET ? 2 : 0, follow, directory);
    };
    for (unsigned profile = 0; profile < 6; ++profile) {
        uint32_t const UID = profile < 2 ? 0 : profile == 2 ? 1001 : 1002;
        uint32_t const GROUP = 2002;
        security::SnapshotFields fields{.user = {UID, UID, UID},
                                        .group = {profile == 3 ? GROUP : 2001, profile == 3 ? GROUP : 2001, profile == 3 ? GROUP : 2001},
                                        .authority_ceiling = profile == 1 ? 0 : security::ALL_AUTHORITY};
        if (profile == 4) fields.groups = {&GROUP, 1};
        security::Snapshot admitted;
        KREQUIRE_EQ(security::Snapshot::restore_fields(fields, admitted), 0);
        for (int mode : {0600, 0060, 0006, 0000, 0444, 0222}) {
            KREQUIRE_EQ(vfs::vfs_chmod(FILE, mode), 0);
            unsigned const SHIFT = profile == 2 ? 6 : profile == 3 || profile == 4 ? 3 : 0;
            unsigned const PERMISSIONS = (static_cast<unsigned>(mode) >> SHIFT) & 7U;
            for (bool descriptor : {false, true}) {
                for (Operation operation : {Operation::SET, Operation::GET, Operation::LIST, Operation::REMOVE}) {
                    KREQUIRE_EQ(vfs::vfs_setxattr(FILE, "user.matrix", "seed", 4, 0, true), 0);
                    vfs::Stat before{}, after{};
                    KREQUIRE_EQ(vfs::vfs_stat(FILE, &before), 0);
                    std::array<char, 32> output{};
                    output.fill('!');
                    bool const WRITE = operation == Operation::SET || operation == Operation::REMOVE;
                    bool const ALLOWED = profile == 0 || (PERMISSIONS & (WRITE ? 2U : 4U)) != 0;
                    void* value = operation == Operation::SET ? const_cast<char*>("next") : output.data();
                    size_t const SIZE = operation == Operation::SET ? 4 : operation == Operation::REMOVE ? 0 : output.size();
                    ssize_t const RESULT = descriptor ? vfs::vfs_xattr_file_as(admitted, cleanup.file, operation,
                                                                               operation == Operation::LIST ? nullptr : "user.matrix",
                                                                               value, SIZE, operation == Operation::SET ? 2 : 0)
                                                      : path_call(admitted, FILE, operation, value, SIZE);
                    ssize_t const EXPECTED = !ALLOWED ? -EACCES : operation == Operation::GET ? 4 : operation == Operation::LIST ? 12 : 0;
                    KEXPECT_EQ(RESULT, EXPECTED);
                    KREQUIRE_EQ(vfs::vfs_stat(FILE, &after), 0);
                    KEXPECT_EQ(after.st_ino, before.st_ino);
                    KEXPECT_EQ(after.st_mode, before.st_mode);
                    KEXPECT_EQ(after.st_uid, before.st_uid);
                    KEXPECT_EQ(after.st_gid, before.st_gid);
                    KEXPECT_EQ(after.st_size, before.st_size);
                    if (!ALLOWED) {
                        KEXPECT_EQ(after.st_ctim.tv_sec, before.st_ctim.tv_sec);
                        KEXPECT_EQ(after.st_ctim.tv_nsec, before.st_ctim.tv_nsec);
                        for (char c : output) KEXPECT_EQ(c, '!');
                        std::array<char, 4> preserved{};
                        KEXPECT_EQ(vfs::vfs_getxattr(FILE, "user.matrix", preserved.data(), preserved.size(), true), 4);
                        KEXPECT_TRUE(std::memcmp(preserved.data(), "seed", 4) == 0);
                    } else if (operation == Operation::GET) {
                        KEXPECT_TRUE(std::memcmp(output.data(), "seed", 4) == 0);
                    } else if (operation == Operation::LIST) {
                        KEXPECT_TRUE(std::strcmp(output.data(), "user.matrix") == 0);
                    }
                    KEXPECT_TRUE(worker.credentials() == unrelated);
                }
            }
        }
    }
    KREQUIRE_EQ(vfs::vfs_chmod(FILE, 0666), 0);
    KREQUIRE_EQ(vfs::vfs_setxattr(FILE, "user.matrix", "seed", 4, 0, true), 0);
    KREQUIRE_EQ(vfs::vfs_mkdir(PRIVATE, 0700), 0);
    KREQUIRE_EQ(vfs::vfs_symlink("goal19-object", LINK), 0);
    std::array<char, 32> output{};
    output.fill('!');
    KEXPECT_EQ(path_call(unrelated, "/tmp/ktest_xattr_owner/goal19-private/../goal19-object", Operation::GET, output.data(), output.size()),
               static_cast<ssize_t>(-EACCES));
    KEXPECT_EQ(output[0], '!');
    KEXPECT_EQ(path_call(ROOT, "/tmp", Operation::GET, output.data(), output.size()), static_cast<ssize_t>(-EPERM));
    KEXPECT_EQ(path_call(ROOT, FILE, Operation::GET, output.data(), output.size(), true, true), static_cast<ssize_t>(-ENOTDIR));
    KEXPECT_EQ(path_call(unrelated, LINK, Operation::GET, output.data(), output.size()), 4);
    KEXPECT_EQ(vfs::vfs_xattr_file_as(ROOT, nullptr, Operation::GET, "user.matrix", output.data(), output.size(), 0),
               static_cast<ssize_t>(-EBADF));
    KEXPECT_EQ(vfs::vfs_xattr_file_as(ROOT, cleanup.file, static_cast<Operation>(255), "user.matrix", output.data(), output.size(), 0),
               static_cast<ssize_t>(-EINVAL));
    vfs::File remote{};
    remote.fs_type = vfs::FSType::REMOTE;
    KEXPECT_EQ(vfs::vfs_xattr_file_as(ROOT, &remote, Operation::GET, "user.matrix", output.data(), output.size(), 0),
               static_cast<ssize_t>(-EXDEV));
    KREQUIRE_EQ(vfs::vfs_rename(FILE, MOVED), 0);
    auto* decoy = vfs::vfs_open_file(FILE, vfs::O_CREAT | vfs::O_EXCL, 0600);
    KREQUIRE_TRUE(decoy != nullptr);
    vfs::vfs_put_file(decoy);
    KREQUIRE_EQ(vfs::vfs_setxattr(FILE, "user.matrix", "else", 4, 0, true), 0);
    KEXPECT_EQ(vfs::vfs_xattr_file_as(unrelated, cleanup.file, Operation::GET, "user.matrix", output.data(), output.size(), 0), 4);
    KEXPECT_TRUE(std::memcmp(output.data(), "seed", 4) == 0);
    KREQUIRE_EQ(vfs::vfs_unlink(MOVED), 0);
    KEXPECT_EQ(vfs::vfs_xattr_file_as(unrelated, cleanup.file, Operation::SET, "user.matrix", const_cast<char*>("kept"), 4, 2), 0);
    KEXPECT_EQ(vfs::vfs_xattr_file_as(unrelated, cleanup.file, Operation::GET, "user.matrix", output.data(), output.size(), 0), 4);
    KEXPECT_TRUE(std::memcmp(output.data(), "kept", 4) == 0);
    KEXPECT_EQ(vfs::vfs_getxattr(FILE, "user.matrix", output.data(), output.size(), true), 4);
    KEXPECT_TRUE(std::memcmp(output.data(), "else", 4) == 0);
}

KTEST(Credentials, SearchOnlyDirectoryPinsPreserveAuthorityAndIdentity) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_directory_pin";
    constexpr auto DIR = "/tmp/ktest_directory_pin/anchor";
    constexpr auto MOVED = "/tmp/ktest_directory_pin/moved";
    constexpr auto FILE = "/tmp/ktest_directory_pin/anchor/file";
    constexpr auto MOVED_FILE = "/tmp/ktest_directory_pin/moved/file";
    for (bool disk : {false, true}) {
        struct Cleanup {
            vfs::File* pin{};
            bool mounted{};
            ~Cleanup() {
                vfs::vfs_put_file(pin);
                for (auto path : {FILE, MOVED_FILE}) static_cast<void>(vfs::vfs_unlink(path));
                for (auto path : {DIR, MOVED}) static_cast<void>(vfs::vfs_rmdir(path));
                if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
                static_cast<void>(vfs::vfs_rmdir(BASE));
            }
        } cleanup;
        KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
        if (disk) {
            auto* device = ker::dev::block_device_find_by_name("sdb2");
            KREQUIRE_TRUE(device != nullptr);
            KREQUIRE_EQ(vfs::mount_filesystem(BASE, "xfs", device), 0);
            cleanup.mounted = true;
        }
        KREQUIRE_EQ(vfs::vfs_mkdir(DIR, 0755), 0);
        KREQUIRE_EQ(vfs::vfs_chown(DIR, 1001, 2002), 0);
        ker::mod::sched::task::Task worker{};
        std::strcpy(worker.root.data(), "/unrelated-worker-root");
        std::strcpy(worker.cwd.data(), "/unrelated-worker-cwd");
        auto const ROOT = security::Snapshot::bootstrap_root();
        auto unrelated = ROOT;
        KREQUIRE_EQ(unrelated.prepare({.change = security::Change::SET_UID, .real = 1003}, unrelated), 0);
        KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(worker, unrelated), 0);
        for (unsigned profile = 0; profile < 6; ++profile) {
            uint32_t const UID = profile < 2 ? 0 : profile == 2 ? 1001 : 1002;
            uint32_t const GROUP = 2002;
            security::SnapshotFields fields{
                .user = {UID, UID, UID},
                .group = {profile == 3 ? GROUP : 2001, profile == 3 ? GROUP : 2001, profile == 3 ? GROUP : 2001},
                .authority_ceiling = profile == 1 ? 0 : security::ALL_AUTHORITY};
            if (profile == 4) fields.groups = {&GROUP, 1};
            security::Snapshot caller;
            KREQUIRE_EQ(security::Snapshot::restore_fields(fields, caller), 0);
            for (unsigned mode : {0100U, 0010U, 0001U, 0444U, 0222U, 0000U, 0111U, 0777U}) {
                KREQUIRE_EQ(vfs::vfs_chmod(DIR, mode), 0);
                vfs::Stat before{}, after{};
                KREQUIRE_EQ(vfs::vfs_stat(DIR, &before), 0);
                unsigned const SHIFT = profile == 2 ? 6 : profile == 3 || profile == 4 ? 3 : 0;
                bool const ALLOWED = profile == 0 || ((mode >> SHIFT) & 1U) != 0;
                vfs::File sentinel{};
                vfs::File* pin = &sentinel;
                int const RESULT = vfs::vfs_pin_directory_resolved_beneath_as(worker, caller, BASE, DIR, &pin);
                KEXPECT_EQ(RESULT, ALLOWED ? 0 : -EACCES);
                if (RESULT == 0) {
                    KEXPECT_TRUE(pin != nullptr && pin != &sentinel && pin->is_directory);
                    KEXPECT_TRUE(pin->vfs_path == nullptr);
                    KEXPECT_EQ(static_cast<unsigned>(pin->open_flags) & 3U, vfs::FILE_ACCESS_SEARCH_ONLY);
                    vfs::Stat retained{};
                    KEXPECT_EQ(vfs::vfs_fstat_file(pin, &retained), 0);
                    KEXPECT_EQ(retained.st_ino, before.st_ino);
                    std::array<char, 32> bytes{};
                    bytes.fill('!');
                    KEXPECT_EQ(pin->fops->vfs_read(pin, bytes.data(), bytes.size(), 0), static_cast<ssize_t>(-EBADF));
                    KEXPECT_EQ(pin->fops->vfs_write(pin, "bad", 3, 0), static_cast<ssize_t>(-EBADF));
                    KEXPECT_EQ(pin->fops->vfs_truncate(pin, 0), -EBADF);
                    for (char c : bytes) KEXPECT_EQ(c, '!');
                    vfs::DirEntry entry{};
                    entry.d_ino = 0xCAFE;
                    KEXPECT_EQ(pin->fops->vfs_readdir(pin, &entry, 0), -EBADF);
                    KEXPECT_EQ(entry.d_ino, static_cast<uint64_t>(0xCAFE));
                    auto* mount = pin->mount_owner;
                    uint32_t const BUSY = mount->open_files.load(std::memory_order_acquire);
                    vfs::vfs_retain_file(pin);
                    vfs::vfs_put_file(pin);
                    KEXPECT_EQ(mount->open_files.load(std::memory_order_acquire), BUSY);
                    vfs::vfs_put_file(pin);
                    KEXPECT_EQ(mount->open_files.load(std::memory_order_acquire), BUSY - 1);
                } else
                    KEXPECT_TRUE(pin == &sentinel);
                KREQUIRE_EQ(vfs::vfs_stat(DIR, &after), 0);
                KEXPECT_EQ(after.st_ino, before.st_ino);
                KEXPECT_EQ(after.st_mode, before.st_mode);
                KEXPECT_EQ(after.st_uid, before.st_uid);
                KEXPECT_EQ(after.st_gid, before.st_gid);
                KEXPECT_EQ(after.st_ctim.tv_sec, before.st_ctim.tv_sec);
                KEXPECT_EQ(after.st_ctim.tv_nsec, before.st_ctim.tv_nsec);
                KEXPECT_TRUE(worker.credentials() == unrelated);
            }
        }
        KREQUIRE_EQ(vfs::vfs_chmod(DIR, 0711), 0);
        auto make_file = [&](const char* path, const char* contents) {
            auto* file = vfs::vfs_open_file(path, vfs::O_CREAT | vfs::O_EXCL | 1, 0644);
            if (file == nullptr) return false;
            bool const OK = file->fops->vfs_write(file, contents, 4, 0) == 4;
            vfs::vfs_put_file(file);
            return OK;
        };
        KREQUIRE_TRUE(make_file(FILE, "seed"));
        KREQUIRE_EQ(vfs::vfs_pin_directory_resolved_beneath_as(worker, unrelated, BASE, DIR, &cleanup.pin), 0);
        vfs::vfs_retain_file(cleanup.pin);
        int const PIN_FD = vfs::vfs_alloc_fd(&worker, cleanup.pin);
        if (PIN_FD < 0) vfs::vfs_put_file(cleanup.pin);
        KREQUIRE_TRUE(PIN_FD >= 0);
        std::array<char, 512> entries{};
        entries.fill('!');
        KEXPECT_EQ(vfs::vfs_read_dir_entries(PIN_FD, entries.data(), entries.size(), &worker), static_cast<ssize_t>(-EBADF));
        for (char c : entries) KEXPECT_EQ(c, '!');
        KEXPECT_EQ(vfs::vfs_release_fd(&worker, PIN_FD), 0);
        vfs::vfs_put_file(cleanup.pin);
        auto* mount = cleanup.pin->mount_owner;
        mount->read_only = true;
        vfs::File* read_only_pin = nullptr;
        int const READ_ONLY = vfs::vfs_pin_directory_resolved_beneath_as(worker, unrelated, BASE, DIR, &read_only_pin);
        mount->read_only = false;
        KEXPECT_EQ(READ_ONLY, 0);
        vfs::vfs_put_file(read_only_pin);
        KREQUIRE_EQ(vfs::vfs_rename(DIR, MOVED), 0);
        KREQUIRE_EQ(vfs::vfs_mkdir(DIR, 0755), 0);
        KREQUIRE_TRUE(make_file(FILE, "fake"));
        vfs::File* opened = nullptr;
        KREQUIRE_EQ(vfs::vfs_open_file_resolved_beneath_as(worker, unrelated, BASE, "file", 0, 0, &opened, cleanup.pin), 0);
        std::array<char, 4> contents{};
        KEXPECT_EQ(opened->fops->vfs_read(opened, contents.data(), contents.size(), 0), 4);
        KEXPECT_TRUE(std::memcmp(contents.data(), "seed", 4) == 0);
        vfs::vfs_put_file(opened);
        vfs::File sentinel{};
        opened = &sentinel;
        KREQUIRE_EQ(vfs::vfs_chmod(MOVED, 0700), 0);
        KEXPECT_EQ(vfs::vfs_open_file_resolved_beneath_as(worker, unrelated, BASE, "file", 0, 0, &opened, cleanup.pin), -EACCES);
        KEXPECT_TRUE(opened == &sentinel);
        KEXPECT_EQ(vfs::vfs_pin_directory_resolved_beneath_as(worker, unrelated, BASE, ".", &opened, cleanup.pin), -EACCES);
        KEXPECT_TRUE(opened == &sentinel);
        KEXPECT_EQ(vfs::vfs_pin_directory_resolved_beneath_as(worker, ROOT, BASE, "../..", &opened, cleanup.pin), -EPERM);
        KEXPECT_TRUE(opened == &sentinel);
        KEXPECT_EQ(vfs::vfs_pin_directory_resolved_beneath_as(worker, ROOT, BASE, FILE, &opened), -ENOTDIR);
        KEXPECT_TRUE(opened == &sentinel);
        KEXPECT_EQ(vfs::vfs_open_file_resolved_beneath_as(worker, ROOT, BASE, DIR, 3, 0, &opened), -EINVAL);
        KEXPECT_TRUE(opened == &sentinel);
        KEXPECT_EQ(vfs::vfs_pin_directory_resolved_beneath_as(worker, ROOT, BASE, DIR, nullptr), -EINVAL);
        vfs::vfs_put_file(cleanup.pin);
        cleanup.pin = nullptr;
        KEXPECT_EQ(vfs::vfs_unlink(FILE), 0);
        KEXPECT_EQ(vfs::vfs_unlink(MOVED_FILE), 0);
        KEXPECT_EQ(vfs::vfs_rmdir(DIR), 0);
        KEXPECT_EQ(vfs::vfs_rmdir(MOVED), 0);
        if (cleanup.mounted) {
            KEXPECT_EQ(vfs::unmount_filesystem(BASE), 0);
            cleanup.mounted = false;
        }
        KEXPECT_EQ(vfs::vfs_rmdir(BASE), 0);
    }
}

KTEST(Credentials, RetainedDirectoryNamesStayConfinedWithoutNewAccess) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_directory_recovery";
    constexpr auto EXPORT = "/tmp/ktest_directory_recovery/scope";
    constexpr auto SIBLING = "/tmp/ktest_directory_recovery/scope2";
    constexpr auto ORIGINAL = "/tmp/ktest_directory_recovery/scope/original";
    constexpr auto MOVED = "/tmp/ktest_directory_recovery/scope/moved";
    constexpr auto OUTSIDE = "/tmp/ktest_directory_recovery/scope2/moved";
    for (bool disk : {false, true}) {
        struct Cleanup {
            vfs::File* pin{};
            vfs::File* ordinary{};
            vfs::File* root{};
            bool mounted{};
            ~Cleanup() {
                for (auto* file : {pin, ordinary, root}) vfs::vfs_put_file(file);
                for (auto path : {ORIGINAL, MOVED, OUTSIDE, EXPORT, SIBLING}) static_cast<void>(vfs::vfs_rmdir(path));
                if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
                static_cast<void>(vfs::vfs_rmdir(BASE));
            }
        } cleanup;
        KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
        if (disk) {
            auto* device = ker::dev::block_device_find_by_name("sdb2");
            KREQUIRE_TRUE(device != nullptr);
            KREQUIRE_EQ(vfs::mount_filesystem(BASE, "xfs", device), 0);
            cleanup.mounted = true;
        }
        for (auto path : {EXPORT, SIBLING, ORIGINAL}) KREQUIRE_EQ(vfs::vfs_mkdir(path, 0755), 0);
        ker::mod::sched::task::Task worker{};
        std::strcpy(worker.root.data(), "/unrelated-worker-root");
        std::strcpy(worker.cwd.data(), "/unrelated-worker-cwd");
        auto const ROOT = security::Snapshot::bootstrap_root();
        auto caller = ROOT;
        KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1003}, caller), 0);
        KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(worker, caller), 0);
        KREQUIRE_EQ(vfs::vfs_pin_directory_resolved_beneath_as(worker, caller, EXPORT, ORIGINAL, &cleanup.pin), 0);
        KREQUIRE_EQ(vfs::vfs_pin_directory_resolved_beneath_as(worker, caller, EXPORT, EXPORT, &cleanup.root), 0);
        KREQUIRE_EQ(vfs::vfs_open_file_resolved_beneath_as(worker, ROOT, EXPORT, ORIGINAL, vfs::O_DIRECTORY, 0, &cleanup.ordinary), 0);
        auto check = [&](vfs::File* file, const char* root, const char* expected) {
            std::array<char, 512> output{};
            output.fill('!');
            size_t length = 999;
            KREQUIRE_EQ(vfs::vfs_directory_path_beneath(file, root, output.data(), output.size(), &length), 0);
            KEXPECT_TRUE(std::memcmp(output.data(), expected, std::strlen(expected) + 1) == 0);
            KEXPECT_EQ(length, std::strlen(expected));
            KEXPECT_EQ(output.at(std::strlen(expected) + 1), '!');
        };
        auto deny = [&](vfs::File* file, const char* root, size_t capacity, int expected) {
            std::array<char, 512> output{};
            output.fill('!');
            size_t length = 999;
            KEXPECT_EQ(vfs::vfs_directory_path_beneath(file, root, output.data(), capacity, &length), expected);
            KEXPECT_EQ(length, static_cast<size_t>(999));
            for (char byte : output) KEXPECT_EQ(byte, '!');
        };
        auto canonical = [&](const security::Snapshot& credentials, const char* path, int expected, const char* name = "",
                             size_t capacity = 512) {
            std::array<char, 512> output{};
            output.fill('!');
            size_t length = 999;
            int const RESULT = vfs::vfs_realpath_resolved_beneath_as(worker, credentials, EXPORT, path, output.data(), capacity, &length,
                                                                     false, cleanup.pin);
            KEXPECT_EQ(RESULT, expected);
            if (expected == 0) {
                KEXPECT_EQ(length, std::strlen(name));
                KEXPECT_EQ(std::strcmp(output.data(), name), 0);
            } else {
                KEXPECT_EQ(length, static_cast<size_t>(999));
                for (char byte : output) KEXPECT_EQ(byte, '!');
            }
        };
        canonical(caller, ".", 0, "original");
        canonical(caller, ".", -ERANGE, "", 2);
        canonical(caller, "/", -EINVAL);
        canonical(ROOT, "../..", -EPERM);
        check(cleanup.pin, EXPORT, "/original");
        check(cleanup.ordinary, EXPORT, "/original");
        check(cleanup.root, EXPORT, "/");
        check(cleanup.pin, "/", ORIGINAL);
        deny(cleanup.pin, EXPORT, std::strlen("/original"), -ERANGE);
        deny(cleanup.pin, EXPORT, 0, -EINVAL);
        deny(cleanup.pin, "relative", 512, -EINVAL);
        deny(cleanup.pin, nullptr, 512, -EINVAL);
        deny(nullptr, EXPORT, 512, -EBADF);
        size_t length = 999;
        KEXPECT_EQ(vfs::vfs_directory_path_beneath(cleanup.pin, EXPORT, nullptr, 512, &length), -EINVAL);
        KEXPECT_EQ(length, static_cast<size_t>(999));
        vfs::File invalid{};
        invalid.private_data = &invalid;
        deny(&invalid, EXPORT, 512, -ENOTDIR);
        invalid.is_directory = true;
        invalid.fs_type = vfs::FSType::REMOTE;
        deny(&invalid, EXPORT, 512, -EXDEV);
        invalid.fs_type = vfs::FSType::DEVFS;
        deny(&invalid, EXPORT, 512, -ENOTSUP);
        invalid.fs_type = vfs::FSType::TMPFS;
        deny(&invalid, EXPORT, 512, -EBADF);
        KREQUIRE_EQ(vfs::vfs_rename(ORIGINAL, MOVED), 0);
        KREQUIRE_EQ(vfs::vfs_mkdir(ORIGINAL, 0755), 0);
        check(cleanup.pin, EXPORT, "/moved");
        check(cleanup.ordinary, EXPORT, "/moved");
        KREQUIRE_EQ(vfs::vfs_chmod(EXPORT, 0000), 0);
        canonical(caller, ".", 0, "moved");
        canonical(caller, "./.", 0, "moved");
        KREQUIRE_EQ(vfs::vfs_chmod(MOVED, 0000), 0);
        canonical(caller, ".", -EACCES);
        canonical(ROOT, ".", 0, "moved");
        KREQUIRE_EQ(vfs::vfs_chmod(EXPORT, 0000), 0);
        vfs::Stat before{}, after{};
        KREQUIRE_EQ(vfs::vfs_fstat_file(cleanup.pin, &before), 0);
        auto* mount = cleanup.pin->mount_owner;
        auto const BUSY = mount->open_files.load(std::memory_order_acquire);
        auto const REFS = cleanup.pin->refcount.load(std::memory_order_acquire);
        check(cleanup.pin, EXPORT, "/moved");
        mount->read_only = true;
        check(cleanup.pin, EXPORT, "/moved");
        mount->read_only = false;
        KREQUIRE_EQ(vfs::vfs_fstat_file(cleanup.pin, &after), 0);
        KEXPECT_EQ(before.st_mode, after.st_mode);
        KEXPECT_EQ(before.st_ctim.tv_sec, after.st_ctim.tv_sec);
        KEXPECT_EQ(before.st_ctim.tv_nsec, after.st_ctim.tv_nsec);
        KEXPECT_EQ(mount->open_files.load(std::memory_order_acquire), BUSY);
        KEXPECT_EQ(cleanup.pin->refcount.load(std::memory_order_acquire), REFS);
        KEXPECT_TRUE(worker.credentials() == caller);
        // Name recovery grants no child access after search is withdrawn.
        vfs::File sentinel{};
        vfs::File* repin = &sentinel;
        KEXPECT_EQ(vfs::vfs_pin_directory_resolved_beneath_as(worker, caller, EXPORT, ".", &repin, cleanup.pin), -EACCES);
        KEXPECT_TRUE(repin == &sentinel);
        KREQUIRE_EQ(vfs::vfs_rename(MOVED, OUTSIDE), 0);
        canonical(ROOT, ".", -EPERM);
        deny(cleanup.pin, EXPORT, 512, -EPERM);
        deny(cleanup.ordinary, EXPORT, 512, -EPERM);
        check(cleanup.pin, SIBLING, "/moved");
        KREQUIRE_EQ(vfs::vfs_rmdir(OUTSIDE), 0);
        canonical(ROOT, ".", -ENOENT);
        deny(cleanup.pin, "/", 512, -ENOENT);
        deny(cleanup.ordinary, "/", 512, -ENOENT);
        for (auto** file : {&cleanup.pin, &cleanup.ordinary, &cleanup.root}) {
            vfs::vfs_put_file(*file);
            *file = nullptr;
        }
        for (auto path : {ORIGINAL, EXPORT, SIBLING}) KEXPECT_EQ(vfs::vfs_rmdir(path), 0);
        if (cleanup.mounted) {
            KEXPECT_EQ(vfs::unmount_filesystem(BASE), 0);
            cleanup.mounted = false;
        }
        KEXPECT_EQ(vfs::vfs_rmdir(BASE), 0);
    }
}

KTEST(Credentials, RetainedFileMetadataUsesExplicitCallerAndOriginalInode) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_fd_metadata";
    constexpr auto PATH = "/tmp/ktest_fd_metadata/original";
    constexpr auto MOVED = "/tmp/ktest_fd_metadata/moved";
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto owner = ROOT;
    KREQUIRE_EQ(owner.prepare({.change = security::Change::SET_GID, .real = 2001}, owner), 0);
    KREQUIRE_EQ(owner.prepare({.change = security::Change::SET_UID, .real = 1001}, owner), 0);
    auto unrelated = ROOT;
    KREQUIRE_EQ(unrelated.prepare({.change = security::Change::SET_UID, .real = 1003}, unrelated), 0);
    auto restricted_root = ROOT;
    KREQUIRE_EQ(restricted_root.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, restricted_root), 0);
    auto grouped_owner = ROOT;
    constexpr std::array<uint32_t, 1> GROUPS{2002};
    KREQUIRE_EQ(grouped_owner.prepare({.change = security::Change::SET_GROUPS, .groups = GROUPS}, grouped_owner), 0);
    KREQUIRE_EQ(grouped_owner.prepare({.change = security::Change::SET_GID, .real = 2001}, grouped_owner), 0);
    KREQUIRE_EQ(grouped_owner.prepare({.change = security::Change::SET_UID, .real = 1001}, grouped_owner), 0);
    constexpr std::array<vfs::Timespec, 2> TIMES{{{123, 456}, {789, 123}}};
    constexpr std::array<vfs::Timespec, 2> OMIT{{{0, vfs::TimestampUpdate::OMIT}, {0, vfs::TimestampUpdate::OMIT}}};
    constexpr std::array<vfs::Timespec, 2> MIXED{{{0, vfs::TimestampUpdate::NOW}, {0, vfs::TimestampUpdate::OMIT}}};
    constexpr std::array<vfs::Timespec, 2> INVALID{{{123, 1000000000}, {789, 123}}};
    auto unchanged = [](const vfs::Stat& before, const vfs::Stat& after) {
        return before.st_ino == after.st_ino && before.st_mode == after.st_mode && before.st_uid == after.st_uid &&
               before.st_gid == after.st_gid && before.st_size == after.st_size && before.st_nlink == after.st_nlink &&
               before.st_atim.tv_sec == after.st_atim.tv_sec && before.st_atim.tv_nsec == after.st_atim.tv_nsec &&
               before.st_mtim.tv_sec == after.st_mtim.tv_sec && before.st_mtim.tv_nsec == after.st_mtim.tv_nsec &&
               before.st_ctim.tv_sec == after.st_ctim.tv_sec && before.st_ctim.tv_nsec == after.st_ctim.tv_nsec;
    };
    for (bool disk : {false, true}) {
        struct Cleanup {
            vfs::File* file{};
            vfs::File* replacement{};
            bool mounted{};
            ~Cleanup() {
                vfs::vfs_put_file(file);
                vfs::vfs_put_file(replacement);
                for (auto path : {PATH, MOVED}) static_cast<void>(vfs::vfs_unlink(path));
                if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
                static_cast<void>(vfs::vfs_rmdir(BASE));
            }
        } cleanup;
        KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
        if (disk) {
            auto* device = ker::dev::block_device_find_by_name("sdb2");
            KREQUIRE_TRUE(device != nullptr);
            KREQUIRE_EQ(vfs::mount_filesystem(BASE, "xfs", device), 0);
            cleanup.mounted = true;
        }
        cleanup.file = vfs::vfs_open_file(PATH, vfs::O_CREAT | vfs::O_EXCL, 0640);
        KREQUIRE_TRUE(cleanup.file != nullptr);
        auto* file = cleanup.file;
        auto const REFERENCES = file->refcount.load();
        auto const OPEN_FLAGS = file->open_flags;
        vfs::Stat replacement_before{};
        for (unsigned stage = 0; stage < 3; ++stage) {
            if (stage == 1) {
                ker::mod::sched::task::Task task{};
                KREQUIRE_EQ(vfs::vfs_renameat(&task, vfs::AT_FDCWD, PATH, vfs::AT_FDCWD, MOVED), 0);
                cleanup.replacement = vfs::vfs_open_file(PATH, vfs::O_CREAT | vfs::O_EXCL, 0600);
                KREQUIRE_TRUE(cleanup.replacement != nullptr);
                KREQUIRE_EQ(vfs::vfs_fstat_file(cleanup.replacement, &replacement_before), 0);
            }
            if (stage == 2) KREQUIRE_EQ(vfs::vfs_unlink(MOVED), 0);
            KREQUIRE_EQ(vfs::vfs_fchown_file_as(file, ROOT, 1001, 2002), 0);
            KREQUIRE_EQ(vfs::vfs_fchmod_file_as(file, ROOT, 0640), 0);
            KREQUIRE_EQ(vfs::vfs_futimens_file_as(file, ROOT, TIMES.data()), 0);
            vfs::Stat before{}, after{};
            KREQUIRE_EQ(vfs::vfs_fstat_file(file, &before), 0);
            for (auto const& denied : {unrelated, restricted_root}) {
                KEXPECT_EQ(vfs::vfs_fchmod_file_as(file, denied, 0777), -EPERM);
                KEXPECT_EQ(vfs::vfs_fchown_file_as(file, denied, 0, 0), -EPERM);
                KEXPECT_EQ(vfs::vfs_futimens_file_as(file, denied, TIMES.data()), -EPERM);
                KEXPECT_EQ(vfs::vfs_futimens_file_as(file, denied, nullptr), -EACCES);
                KEXPECT_EQ(vfs::vfs_futimens_file_as(file, denied, MIXED.data()), -EPERM);
                KEXPECT_EQ(vfs::vfs_futimens_file_as(file, denied, OMIT.data()), 0);
                KREQUIRE_EQ(vfs::vfs_fstat_file(file, &after), 0);
                KEXPECT_TRUE(unchanged(before, after));
            }
            KEXPECT_EQ(vfs::vfs_futimens_file_as(file, owner, INVALID.data()), -EINVAL);
            KREQUIRE_EQ(vfs::vfs_fstat_file(file, &after), 0);
            KEXPECT_TRUE(unchanged(before, after));
            // Simulate an immutable read-only mount policy on this private File,
            // without modifying a published/shared MountPoint.
            {
                vfs::MountPoint read_only{};
                read_only.read_only = true;
                struct RestoreMount {
                    vfs::File* file;
                    vfs::MountPoint* saved;
                    ~RestoreMount() { file->mount_owner = saved; }
                } restore{file, file->mount_owner};
                file->mount_owner = &read_only;
                KEXPECT_EQ(vfs::vfs_fchmod_file_as(file, ROOT, 0777), -EROFS);
                KEXPECT_EQ(vfs::vfs_fchown_file_as(file, ROOT, 0, 0), -EROFS);
                KEXPECT_EQ(vfs::vfs_futimens_file_as(file, ROOT, TIMES.data()), -EROFS);
                KEXPECT_EQ(vfs::vfs_futimens_file_as(file, ROOT, INVALID.data()), -EINVAL);
            }
            KREQUIRE_EQ(vfs::vfs_fstat_file(file, &after), 0);
            KEXPECT_TRUE(unchanged(before, after));
            KEXPECT_EQ(vfs::vfs_fchmod_file_as(file, owner, 02750), 0);
            KREQUIRE_EQ(vfs::vfs_fstat_file(file, &after), 0);
            KEXPECT_EQ(after.st_mode & 07777U, 0750U);
            KEXPECT_EQ(vfs::vfs_fchmod_file_as(file, grouped_owner, 02750), 0);
            KREQUIRE_EQ(vfs::vfs_fstat_file(file, &after), 0);
            KEXPECT_EQ(after.st_mode & 07777U, 02750U);
            KEXPECT_EQ(vfs::vfs_fchown_file_as(file, owner, UINT32_MAX, 2003), -EPERM);
            KEXPECT_EQ(vfs::vfs_fchown_file_as(file, owner, UINT32_MAX, 2001), 0);
            KEXPECT_EQ(vfs::vfs_fchown_file_as(file, grouped_owner, UINT32_MAX, 2002), 0);
            KREQUIRE_EQ(vfs::vfs_fstat_file(file, &after), 0);
            KEXPECT_EQ(after.st_uid, 1001U);
            KEXPECT_EQ(after.st_gid, 2002U);
            KEXPECT_EQ(after.st_mode & 06000U, 0U);
            KEXPECT_EQ(vfs::vfs_futimens_file_as(file, owner, TIMES.data()), 0);
            KREQUIRE_EQ(vfs::vfs_fstat_file(file, &after), 0);
            KEXPECT_EQ(after.st_atim.tv_sec, 123);
            KEXPECT_EQ(after.st_atim.tv_nsec, 456);
            KEXPECT_EQ(after.st_mtim.tv_sec, 789);
            KEXPECT_EQ(after.st_mtim.tv_nsec, 123);
            KEXPECT_EQ(vfs::vfs_fchmod_file_as(file, owner, 0666), 0);
            KEXPECT_EQ(vfs::vfs_futimens_file_as(file, unrelated, nullptr), 0);
            KEXPECT_EQ(vfs::vfs_futimens_file_as(file, unrelated, TIMES.data()), -EPERM);
            KEXPECT_EQ(file->open_flags, OPEN_FLAGS);
            KEXPECT_EQ(file->refcount.load(), REFERENCES);
            if (stage != 0) {
                KREQUIRE_EQ(vfs::vfs_fstat_file(cleanup.replacement, &after), 0);
                KEXPECT_TRUE(unchanged(replacement_before, after));
                KEXPECT_TRUE(after.st_ino != before.st_ino);
            }
        }
    }
}

KTEST(Credentials, RetainedMetadataRejectsInvalidFilesWithoutBackendAccess) {
    namespace vfs = ker::vfs;
    auto const ROOT = security::Snapshot::bootstrap_root();
    KEXPECT_EQ(vfs::vfs_fchmod_file_as(nullptr, ROOT, 0777), -EBADF);
    KEXPECT_EQ(vfs::vfs_fchown_file_as(nullptr, ROOT, 0, 0), -EBADF);
    KEXPECT_EQ(vfs::vfs_futimens_file_as(nullptr, ROOT, nullptr), -EBADF);
    vfs::File invalid;
    invalid.fs_type = vfs::FSType::TMPFS;
    // A non-TMPFS operations table must not let metadata cast private_data to TmpNode.
    KEXPECT_EQ(vfs::vfs_fchmod_file_as(&invalid, ROOT, 0777), -ENOSYS);
    KEXPECT_EQ(vfs::vfs_fchown_file_as(&invalid, ROOT, 0, 0), -ENOSYS);
    KEXPECT_EQ(vfs::vfs_futimens_file_as(&invalid, ROOT, nullptr), -ENOSYS);
    for (auto type : {vfs::FSType::REMOTE, vfs::FSType::PROCFS, vfs::FSType::SOCKET}) {
        invalid.fs_type = type;
        KEXPECT_EQ(vfs::vfs_fchmod_file_as(&invalid, ROOT, 0777), -ENOSYS);
        KEXPECT_EQ(vfs::vfs_fchown_file_as(&invalid, ROOT, 0, 0), type == vfs::FSType::REMOTE ? -EOPNOTSUPP : -ENOSYS);
        KEXPECT_EQ(vfs::vfs_futimens_file_as(&invalid, ROOT, nullptr), -ENOSYS);
    }
}

KTEST(Credentials, PipeMetadataDenialPreservesBufferAndDescriptorLifetime) {
    namespace vfs = ker::vfs;
    ker::mod::sched::task::Task task{};
    struct Cleanup {
        ker::mod::sched::task::Task& task;
        std::array<int, 2> fds{-1, -1};
        ~Cleanup() {
            for (int fd : fds) {
                if (fd < 0) continue;
                auto* file = static_cast<vfs::File*>(task.fd_table.lookup(static_cast<uint64_t>(fd)));
                if (file != nullptr && vfs::vfs_release_fd(&task, fd) == 0) vfs::vfs_put_file(file);
            }
        }
    } cleanup{task};
    KREQUIRE_EQ(vfs::vfs_selftest_create_pipe(task, cleanup.fds.data()), 0);
    auto const CALLER = task.credentials();
    auto* reader = static_cast<vfs::File*>(task.fd_table.lookup(static_cast<uint64_t>(cleanup.fds[0])));
    auto* writer = static_cast<vfs::File*>(task.fd_table.lookup(static_cast<uint64_t>(cleanup.fds[1])));
    KREQUIRE_TRUE(reader != nullptr && writer != nullptr);
    constexpr std::array<char, 4> DATA{'k', 'e', 'e', 'p'};
    KREQUIRE_EQ(writer->fops->vfs_write(writer, DATA.data(), DATA.size(), 0), DATA.size());
    for (int fd : cleanup.fds) {
        auto* file = static_cast<vfs::File*>(task.fd_table.lookup(static_cast<uint64_t>(fd)));
        auto const REFERENCES = file->refcount.load();
        KEXPECT_EQ(vfs::vfs_fchmodat(&task, fd, "", 0777, vfs::AT_EMPTY_PATH), -ENOSYS);
        KEXPECT_EQ(vfs::vfs_fchownat(&task, fd, "", 1001, 2001, vfs::AT_EMPTY_PATH), -ENOSYS);
        KEXPECT_EQ(vfs::vfs_futimens_file_as(file, CALLER, nullptr), -ENOSYS);
        KEXPECT_EQ(file->refcount.load(), REFERENCES);
    }
    std::array<char, DATA.size()> output{};
    KEXPECT_EQ(reader->fops->vfs_read(reader, output.data(), output.size(), 0), output.size());
    KEXPECT_TRUE(output == DATA);
}

KTEST(Credentials, DevfsRetainedMetadataUsesCallerWithoutDeviceCallbacks) {
    namespace vfs = ker::vfs;
    constexpr auto NAME = "pts/ktest_fd_metadata_device";
    constexpr auto PATH = "/dev/pts/ktest_fd_metadata_device";
    DevfsOpenFixture fixture;
    ker::dev::CharDeviceOps operations{};
    operations.open = credential_device_open;
    operations.close = credential_device_close;
    ker::dev::Device device{};
    device.char_ops = &operations;
    device.private_data = &fixture;
    struct Cleanup {
        vfs::File* file{};
        ~Cleanup() {
            vfs::vfs_put_file(file);
            static_cast<void>(vfs::devfs::devfs_remove_node(NAME));
        }
    } cleanup;
    KREQUIRE_TRUE(vfs::devfs::devfs_add_device_node(NAME, &device) != nullptr);
    cleanup.file = vfs::vfs_open_file(PATH, 0, 0);
    KREQUIRE_TRUE(cleanup.file != nullptr);
    auto* file = cleanup.file;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto owner = ROOT;
    KREQUIRE_EQ(owner.prepare({.change = security::Change::SET_UID, .real = 1001}, owner), 0);
    constexpr std::array<vfs::Timespec, 2> TIMES{{{123, 456}, {789, 123}}};
    constexpr std::array<vfs::Timespec, 2> OMIT{{{0, vfs::TimestampUpdate::OMIT}, {0, vfs::TimestampUpdate::OMIT}}};
    KEXPECT_EQ(vfs::vfs_fchmod_file_as(file, owner, 0600), -EPERM);
    KEXPECT_EQ(vfs::vfs_fchown_file_as(file, owner, 1001, 2001), -EPERM);
    KEXPECT_EQ(vfs::vfs_futimens_file_as(file, owner, TIMES.data()), -EPERM);
    KEXPECT_EQ(vfs::vfs_fchown_file_as(file, ROOT, 1001, 2001), 0);
    KEXPECT_EQ(vfs::vfs_fchmod_file_as(file, owner, 0600), 0);
    KEXPECT_EQ(vfs::vfs_futimens_file_as(file, owner, TIMES.data()), 0);
    vfs::Stat before{}, after{};
    KREQUIRE_EQ(vfs::vfs_fstat_file(file, &before), 0);
    {
        vfs::MountPoint read_only{};
        read_only.read_only = true;
        struct RestoreMount {
            vfs::File* file;
            vfs::MountPoint* saved;
            ~RestoreMount() { file->mount_owner = saved; }
        } restore{file, file->mount_owner};
        file->mount_owner = &read_only;
        KEXPECT_EQ(vfs::vfs_fchmod_file_as(file, ROOT, 0777), -EROFS);
        KEXPECT_EQ(vfs::vfs_fchown_file_as(file, ROOT, 0, 0), -EROFS);
        KEXPECT_EQ(vfs::vfs_futimens_file_as(file, ROOT, nullptr), -EROFS);
        KEXPECT_EQ(vfs::vfs_futimens_file_as(file, owner, OMIT.data()), 0);
    }
    KREQUIRE_EQ(vfs::vfs_fstat_file(file, &after), 0);
    KEXPECT_EQ(after.st_mode, before.st_mode);
    KEXPECT_EQ(after.st_uid, before.st_uid);
    KEXPECT_EQ(after.st_gid, before.st_gid);
    KEXPECT_EQ(after.st_atim.tv_sec, 123);
    KEXPECT_EQ(after.st_atim.tv_nsec, 456);
    KEXPECT_EQ(after.st_mtim.tv_sec, 789);
    KEXPECT_EQ(after.st_mtim.tv_nsec, 123);
    KEXPECT_EQ(after.st_ctim.tv_sec, before.st_ctim.tv_sec);
    KEXPECT_EQ(after.st_ctim.tv_nsec, before.st_ctim.tv_nsec);
    KEXPECT_EQ(fixture.opens, 1U);
    KEXPECT_EQ(fixture.closes, 0U);
}

KTEST(Credentials, ConfinedChownUsesSuppliedCallerAndRetainedDirectory) {
    namespace vfs = ker::vfs;
    constexpr auto BASE = "/tmp/ktest_path_chown";
    constexpr auto DIR = "/tmp/ktest_path_chown/dir";
    constexpr auto MOVED = "/tmp/ktest_path_chown/moved";
    constexpr auto PATH = "/tmp/ktest_path_chown/dir/file";
    constexpr auto LINK = "/tmp/ktest_path_chown/dir/link";
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto owner = ROOT;
    KREQUIRE_EQ(owner.prepare({.change = security::Change::SET_GID, .real = 2001}, owner), 0);
    KREQUIRE_EQ(owner.prepare({.change = security::Change::SET_UID, .real = 1001}, owner), 0);
    auto other = ROOT;
    KREQUIRE_EQ(other.prepare({.change = security::Change::SET_UID, .real = 1003}, other), 0);
    auto limited = ROOT;
    KREQUIRE_EQ(limited.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, limited), 0);
    for (bool disk : {false, true}) {
        struct Cleanup {
            vfs::File* directory{};
            vfs::File* file{};
            bool mounted{};
            ~Cleanup() {
                vfs::vfs_put_file(file);
                vfs::vfs_put_file(directory);
                for (auto p : {PATH, LINK, "/tmp/ktest_path_chown/moved/file", "/tmp/ktest_path_chown/moved/link"})
                    static_cast<void>(vfs::vfs_unlink(p));
                for (auto p : {DIR, MOVED}) static_cast<void>(vfs::vfs_rmdir(p));
                if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
                static_cast<void>(vfs::vfs_rmdir(BASE));
            }
        } cleanup;
        KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
        if (disk) {
            auto* device = ker::dev::block_device_find_by_name("sdb2");
            KREQUIRE_TRUE(device != nullptr);
            KREQUIRE_EQ(vfs::mount_filesystem(BASE, "xfs", device), 0);
            cleanup.mounted = true;
        }
        KREQUIRE_EQ(vfs::vfs_mkdir(DIR, 0755), 0);
        cleanup.file = vfs::vfs_open_file(PATH, vfs::O_CREAT | vfs::O_EXCL, 06755);
        KREQUIRE_TRUE(cleanup.file != nullptr);
        KREQUIRE_EQ(vfs::vfs_symlink("file", LINK), 0);
        ker::mod::sched::task::Task worker{};
        KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(worker, other), 0);
        KREQUIRE_EQ(vfs::vfs_pin_directory_resolved_beneath_as(worker, ROOT, BASE, DIR, &cleanup.directory), 0);
        auto change = [&](const security::Snapshot& caller, const char* path, uint32_t uid, uint32_t gid, bool follow = true) {
            return vfs::vfs_chown_resolved_beneath_as(worker, caller, BASE, path, uid, gid, follow, false, cleanup.directory);
        };
        for (unsigned stage = 0; stage < 2; ++stage) {
            if (stage) {
                ker::mod::sched::task::Task root_task{};
                KREQUIRE_EQ(vfs::vfs_renameat(&root_task, vfs::AT_FDCWD, DIR, vfs::AT_FDCWD, MOVED), 0);
                KREQUIRE_EQ(vfs::vfs_mkdir(DIR, 0755), 0);
                auto* replacement = vfs::vfs_open_file(PATH, vfs::O_CREAT | vfs::O_EXCL, 0644);
                KREQUIRE_TRUE(replacement != nullptr);
                vfs::vfs_put_file(replacement);
            }
            KEXPECT_EQ(change(ROOT, "file", 1001, 2002), 0);
            vfs::Stat before{}, after{};
            KREQUIRE_EQ(vfs::vfs_fstat_file(cleanup.file, &before), 0);
            KEXPECT_EQ(before.st_uid, 1001U);
            KEXPECT_EQ(before.st_gid, 2002U);
            KEXPECT_EQ(before.st_mode & 06000U, 0U);
            for (const auto& caller : {other, limited}) {
                KEXPECT_EQ(change(caller, "file", 0, 0), -EPERM);
                KREQUIRE_EQ(vfs::vfs_fstat_file(cleanup.file, &after), 0);
                KEXPECT_EQ(after.st_uid, before.st_uid);
                KEXPECT_EQ(after.st_gid, before.st_gid);
                KEXPECT_EQ(after.st_mode, before.st_mode);
                KEXPECT_EQ(after.st_ctim.tv_sec, before.st_ctim.tv_sec);
                KEXPECT_EQ(after.st_ctim.tv_nsec, before.st_ctim.tv_nsec);
            }
            KEXPECT_EQ(change(owner, "file", UINT32_MAX, 2003), -EPERM);
            KEXPECT_EQ(change(owner, "file", UINT32_MAX, 2001), 0);
            KEXPECT_EQ(change(ROOT, "link", 1003, 3003, false), 0);
            KREQUIRE_EQ(vfs::vfs_fstat_file(cleanup.file, &after), 0);
            KEXPECT_EQ(after.st_uid, 1001U);
            KEXPECT_EQ(after.st_gid, 2001U);
            KEXPECT_EQ(change(ROOT, "file/", 0, 0), -ENOTDIR);
            KEXPECT_EQ(change(ROOT, "../../outside", 0, 0), -EPERM);
            if (stage) {
                KREQUIRE_EQ(vfs::vfs_stat(PATH, &after), 0);
                KEXPECT_EQ(after.st_uid, 0U);
                KEXPECT_EQ(after.st_gid, 0U);
            }
        }
    }
}

KTEST(Credentials, MountSourceKeepsAdmittedContextAndProvider) { KEXPECT_TRUE(ker::vfs::vfs_selftest_mount_source_binding()); }

KTEST(Credentials, RequestedReadonlyFatMountPreservesWritableDevice) {
    namespace vfs = ker::vfs;
    namespace fat = ker::vfs::fat32;
    struct Media {
        std::array<std::array<uint8_t, 512>, 4> sectors{};
        unsigned writes{};
        unsigned flushes{};
    };
    auto media = std::make_unique<Media>();
    KREQUIRE_TRUE(media != nullptr);
    fat::FAT32BootSector boot{};
    boot.bytes_per_sector = 512;
    boot.sectors_per_cluster = 1;
    boot.reserved_sectors = 1;
    boot.num_fats = 1;
    boot.total_sectors_32 = 4;
    boot.sectors_per_fat_32 = 1;
    boot.root_cluster = 2;
    std::memcpy(media->sectors[0].data(), &boot, sizeof(boot));
    media->sectors[0][510] = 0x55;
    media->sectors[0][511] = 0xaa;
    std::array<uint32_t, 128> table{};
    table[0] = 0x0ffffff8;
    table[1] = table[2] = table[3] = fat::FAT32_EOC;
    std::memcpy(media->sectors[1].data(), table.data(), 512);
    ker::dev::BlockDevice device{};
    device.block_size = 512;
    device.total_blocks = 4;
    device.is_partition = true;
    device.private_data = media.get();
    device.read_blocks = [](ker::dev::BlockDevice* block, uint64_t sector, size_t count, void* out) -> int {
        auto& state = *static_cast<Media*>(block->private_data);
        if (sector >= state.sectors.size() || count != 1) return -EIO;
        std::memcpy(out, state.sectors[sector].data(), 512);
        return 0;
    };
    device.write_blocks = [](ker::dev::BlockDevice* block, uint64_t, size_t, const void*) -> int {
        ++static_cast<Media*>(block->private_data)->writes;
        return -EIO;
    };
    device.flush = [](ker::dev::BlockDevice* block) -> int {
        ++static_cast<Media*>(block->private_data)->flushes;
        return -EIO;
    };
    constexpr const char* TARGET = "/tmp/ktest_requested_readonly";
    vfs::PreparedMount prepared{};
    bool published = false;
    struct Cleanup {
        vfs::PreparedMount& prepared;
        bool& published;
        ~Cleanup() {
            vfs::finish_prepared_mount(&prepared);
            if (published) static_cast<void>(vfs::unmount_filesystem(TARGET));
        }
    } cleanup{prepared, published};
    ker::dev::BlockWriterLease remote_writer;
    KREQUIRE_TRUE(remote_writer.try_acquire(&device, ker::dev::BlockWriterLeaseOwner::REMOTE_BINDING));
    auto prepare = [&](unsigned long flags) {
        return vfs::prepare_mount_filesystem(TARGET, "fat32", &device, flags, nullptr, nullptr, nullptr, &prepared,
                                             vfs::MountPathKind::RETAINED_PHYSICAL);
    };
    KEXPECT_EQ(prepare(0), -EBUSY);
    KEXPECT_TRUE(prepared.state == nullptr);
    KREQUIRE_EQ(prepare(vfs::MOUNT_FLAG_RDONLY), 0);
    vfs::finish_prepared_mount(&prepared);  // Unpublished cancellation must not write/flush.
    KEXPECT_EQ(media->writes, 0U);
    KEXPECT_EQ(media->flushes, 0U);
    KREQUIRE_EQ(prepare(vfs::MOUNT_FLAG_RDONLY | vfs::MOUNT_FLAG_NOSUID), 0);
    KREQUIRE_EQ(vfs::publish_prepared_mount(&prepared, TARGET), 0);
    published = true;
    vfs::finish_prepared_mount(&prepared);
    auto mount = vfs::find_mount_point(TARGET);
    KREQUIRE_TRUE(mount && mount->read_only && mount->nosuid && !mount->block_writer_lease.active());
    auto* ctx = static_cast<fat::FAT32MountContext*>(mount->private_data);
    KREQUIRE_TRUE(ctx != nullptr && ctx->read_only);
    int result = 0;
    auto* file = fat::fat32_open_path(security::Snapshot::bootstrap_root(), "new", vfs::O_CREAT | 1, 0600, ctx, &result);
    KEXPECT_TRUE(file == nullptr);
    KEXPECT_EQ(result, -EROFS);
    if (file) {
        fat::fat32_close(file);
        delete file;
    }
    vfs::Statvfs policy{};
    KEXPECT_EQ(fat::fat32_statvfs(ctx, &policy), 0);
    KEXPECT_TRUE((policy.f_flag & vfs::ST_RDONLY) != 0);
    KEXPECT_EQ(fat::fat32_sync_mount(ctx), 0);
    KEXPECT_TRUE(!device.read_only && !ker::dev::block_device_is_read_only(&device) && remote_writer.active());
    mount.reset();
    KEXPECT_EQ(vfs::unmount_filesystem(TARGET), 0);
    published = false;
    KEXPECT_EQ(media->writes, 0U);
    KEXPECT_EQ(media->flushes, 0U);
}

KTEST(Credentials, ReadonlyTmpfsNamespacePreservesBindingsAndMountTargets) {
    namespace vfs = ker::vfs;
    namespace tmp = ker::vfs::tmpfs;
    constexpr const char* BASE = "/tmp/ktest_readonly_namespace";
    constexpr const char* FILE = "/tmp/ktest_readonly_namespace/file";
    constexpr const char* ALIAS = "/tmp/ktest_readonly_namespace/alias";
    constexpr const char* EMPTY = "/tmp/ktest_readonly_namespace/empty";
    constexpr const char* NEW = "/tmp/ktest_readonly_namespace/new";
    constexpr const char* OTHER = "/tmp/ktest_readonly_namespace/other";
    auto const ROOT = security::Snapshot::bootstrap_root();
    ker::mod::sched::task::Task task{};
    task.pid = UINT64_MAX;
    std::memcpy(task.root.data(), "/", 2);
    std::memcpy(task.cwd.data(), "/", 2);
    task.root_len = task.cwd_len = 1;
    bool mounted = false, overmounted = false;
    int descriptor = -1;
    struct Cleanup {
        ker::mod::sched::task::Task& task;
        bool& mounted;
        bool& overmounted;
        int& descriptor;
        ~Cleanup() {
            if (descriptor >= 0) {
                auto* file = static_cast<vfs::File*>(task.fd_table.lookup(static_cast<uint64_t>(descriptor)));
                if (vfs::vfs_release_fd(&task, descriptor) == 0) vfs::vfs_put_file(file);
            }
            if (overmounted) static_cast<void>(vfs::unmount_filesystem(EMPTY));
            if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
            static_cast<void>(vfs::vfs_rmdir(BASE));
        }
    } cleanup{task, mounted, overmounted, descriptor};
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
    KREQUIRE_EQ(vfs::mount_filesystem(BASE, "tmpfs", nullptr, vfs::MOUNT_FLAG_RDONLY), 0);
    mounted = true;
    auto mount = vfs::find_mount_point(BASE);
    KREQUIRE_TRUE(mount && mount->read_only && mount->fs_type == vfs::FSType::TMPFS);
    auto* root = tmp::mount_root(static_cast<tmp::TmpfsMount*>(mount->private_data));
    KREQUIRE_TRUE(root != nullptr);
    // An empty mount root has no parent membership to detach. Readonly must
    // preserve that error before the normal creation/removal denial matrix.
    KEXPECT_EQ(vfs::vfs_unlinkat(&task, vfs::AT_FDCWD, BASE, vfs::AT_REMOVEDIR), -ENOENT);

    // Seed only this disposable tree through kernel construction APIs. The
    // published mount policy stays readonly throughout the VFS operations.
    root->mode = 0777;
    auto* file = tmp::tmpfs_create_file(root, "file", 0666);
    auto* empty = tmp::tmpfs_mkdir(root, "empty");
    auto* other = tmp::tmpfs_create_file(root, "other", 0666);
    auto* locked = tmp::tmpfs_mkdir(root, "locked");
    KREQUIRE_TRUE(file && empty && other && locked);
    empty->mode = 0777;
    locked->mode = 0700;
    KREQUIRE_TRUE(tmp::tmpfs_create_hardlink(root, "alias", file) != nullptr);
    descriptor = vfs::vfs_openat(&task, vfs::AT_FDCWD, BASE, vfs::O_DIRECTORY, 0);
    KREQUIRE_TRUE(descriptor >= 0);
    auto* directory = static_cast<vfs::File*>(task.fd_table.lookup(static_cast<uint64_t>(descriptor)));
    KREQUIRE_TRUE(directory != nullptr && directory->mount_owner == mount.get());
    auto const GENERATION = tmp::tmpfs_namespace_generation();
    auto const ROOT_GENERATION = root->directory_generation;
    auto const CAPACITY = root->children_capacity;
    auto const CHILDREN = root->children_live_count;
    auto const LINKS = file->link_count.load();
    auto const CTIME = file->ctime;
    auto const MTIME = root->mtime;
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    for (const auto& caller : {ROOT, user}) {
        KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(task, caller), 0);
        for (bool relative : {false, true}) {
            int const FD = relative ? descriptor : vfs::AT_FDCWD;
            const char* const source = relative ? "file" : FILE;
            const char* const target = relative ? "new" : NEW;
            const char* const child = relative ? "empty" : EMPTY;
            KEXPECT_EQ(vfs::vfs_mkdirat(&task, FD, target, 0755), -EROFS);
            KEXPECT_EQ(vfs::vfs_symlinkat(&task, "file", FD, target), -EROFS);
            KEXPECT_EQ(vfs::vfs_unlinkat(&task, FD, source, 0), -EROFS);
            KEXPECT_EQ(vfs::vfs_unlinkat(&task, FD, child, vfs::AT_REMOVEDIR), -EROFS);
            KEXPECT_EQ(vfs::vfs_renameat(&task, FD, source, FD, target), -EROFS);
            KEXPECT_EQ(vfs::vfs_linkat(&task, FD, source, FD, target, 0), -EROFS);
            KEXPECT_EQ(vfs::vfs_mkdirat(&task, FD, child, 0755), -EEXIST);
            KEXPECT_EQ(vfs::vfs_unlinkat(&task, FD, child, 0), -EISDIR);
            KEXPECT_EQ(vfs::vfs_unlinkat(&task, FD, source, vfs::AT_REMOVEDIR), -ENOTDIR);
            KEXPECT_EQ(vfs::vfs_unlinkat(&task, FD, target, 0), -ENOENT);
        }
        KEXPECT_EQ(vfs::vfs_renameat(&task, descriptor, "file", descriptor, "other"), -EROFS);
        KEXPECT_EQ(vfs::vfs_renameat(&task, descriptor, "file", descriptor, "alias"), 0);
        KEXPECT_EQ(vfs::vfs_renameat(&task, descriptor, "file", descriptor, "file"), 0);
        KEXPECT_EQ(vfs::vfs_linkat(&task, descriptor, "file", descriptor, "alias", 0), -EEXIST);
        KEXPECT_EQ(vfs::vfs_renameat(&task, descriptor, "file", vfs::AT_FDCWD, "/tmp/ktest_ro_cross_mount"), -EXDEV);
        KEXPECT_EQ(vfs::vfs_linkat(&task, descriptor, "file", vfs::AT_FDCWD, "/tmp/ktest_ro_cross_mount", 0), -EXDEV);
        KEXPECT_TRUE(task.credentials() == caller);
    }
    KEXPECT_EQ(vfs::vfs_mkdirat(&task, descriptor, "locked/new", 0700), -EACCES);
    // Owner execution must use its explicit admitted Snapshot even when its
    // Task has since dropped authority; the retained readonly policy still wins.
    KEXPECT_EQ(vfs::vfs_mkdir_resolved_beneath_as(task, ROOT, BASE, NEW, 0755), -EROFS);
    KEXPECT_EQ(vfs::vfs_symlink_resolved_beneath_as(task, ROOT, BASE, "file", NEW, false), -EROFS);
    KEXPECT_EQ(vfs::vfs_unlink_resolved_beneath_as(task, ROOT, BASE, FILE, false, false), -EROFS);
    KEXPECT_EQ(vfs::vfs_unlink_resolved_beneath_as(task, ROOT, BASE, EMPTY, true, true), -EROFS);
    KEXPECT_EQ(vfs::vfs_rename_resolved_beneath_as(task, ROOT, BASE, FILE, NEW, false, false), -EROFS);
    KEXPECT_EQ(vfs::vfs_mkdir_resolved_beneath_as(task, ROOT, BASE, "new", 0755, directory), -EROFS);
    KEXPECT_EQ(vfs::vfs_symlink_resolved_beneath_as(task, ROOT, BASE, "file", "new", false, directory), -EROFS);
    KEXPECT_EQ(vfs::vfs_unlink_resolved_beneath_as(task, ROOT, BASE, "file", false, false, directory), -EROFS);
    KEXPECT_EQ(vfs::vfs_rename_resolved_beneath_as(task, ROOT, BASE, "file", "new", false, false, directory, directory), -EROFS);
    // Boot KTEST has no ambient task. The legacy owner wrappers must preserve
    // ESRCH there; the explicit owner APIs above qualify actual backend denial.
    auto* ambient = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : nullptr;
    int const AMBIENT_RESULT = ambient == nullptr ? -ESRCH : -EROFS;
    KEXPECT_EQ(vfs::vfs_mkdir_resolved_beneath(BASE, NEW, 0755), AMBIENT_RESULT);
    KEXPECT_EQ(vfs::vfs_symlink_resolved_beneath(BASE, "file", NEW), AMBIENT_RESULT);
    KEXPECT_EQ(vfs::vfs_unlink_resolved_beneath(BASE, FILE, false), AMBIENT_RESULT);
    KEXPECT_EQ(vfs::vfs_unlink_resolved_beneath(BASE, EMPTY, true), AMBIENT_RESULT);
    KEXPECT_EQ(vfs::vfs_rename_resolved_beneath(BASE, FILE, NEW), AMBIENT_RESULT);
    KEXPECT_EQ(vfs::vfs_symlink_resolved("file", NEW), -EROFS);
    KEXPECT_EQ(vfs::vfs_unlink_resolved(FILE), -EROFS);
    KEXPECT_EQ(vfs::vfs_rename_resolved(FILE, NEW), -EROFS);
    KEXPECT_EQ(tmp::tmpfs_namespace_generation(), GENERATION);
    KEXPECT_EQ(root->directory_generation, ROOT_GENERATION);
    KEXPECT_EQ(root->children_capacity, CAPACITY);
    KEXPECT_EQ(root->children_live_count, CHILDREN);
    KEXPECT_EQ(file->link_count.load(), LINKS);
    KEXPECT_EQ(file->ctime.tv_sec, CTIME.tv_sec);
    KEXPECT_EQ(file->ctime.tv_nsec, CTIME.tv_nsec);
    KEXPECT_EQ(root->mtime.tv_sec, MTIME.tv_sec);
    KEXPECT_EQ(root->mtime.tv_nsec, MTIME.tv_nsec);
    KEXPECT_TRUE(tmp::tmpfs_lookup(root, "new") == nullptr);
    KEXPECT_TRUE(tmp::tmpfs_lookup(root, "file") == file && tmp::tmpfs_lookup(root, "other") == other);
    vfs::Stat original{}, alias{}, replacement{};
    KEXPECT_EQ(vfs::vfs_stat(FILE, &original), 0);
    KEXPECT_EQ(vfs::vfs_stat(ALIAS, &alias), 0);
    KEXPECT_EQ(vfs::vfs_stat(OTHER, &replacement), 0);
    KEXPECT_EQ(original.st_ino, alias.st_ino);
    KEXPECT_TRUE(original.st_ino != replacement.st_ino);
    KEXPECT_TRUE(task.credentials() == user && mount->read_only);
    // Mount publication changes the mount table, not the readonly directory.
    {
        vfs::LookupHandle denied{}, target{};
        KREQUIRE_EQ(vfs::vfs_acquire_lookup(&task, vfs::AT_FDCWD, EMPTY, vfs::LookupIntent::MOUNT_TARGET,
                                            vfs::LookupFollowPolicy::FOLLOW_FINAL, true, true, &denied, 3, false, &user),
                    0);
        KREQUIRE_EQ(vfs::vfs_acquire_lookup(&task, vfs::AT_FDCWD, EMPTY, vfs::LookupIntent::MOUNT_TARGET,
                                            vfs::LookupFollowPolicy::FOLLOW_FINAL, true, true, &target, 3, false, &ROOT),
                    0);
        KEXPECT_EQ(vfs::vfs_selftest_mount_readonly_target(task, user, denied), -EPERM);
        KREQUIRE_EQ(vfs::vfs_selftest_mount_readonly_target(task, ROOT, target), 0);
        overmounted = true;
    }
    KEXPECT_EQ(vfs::vfs_mkdir("/tmp/ktest_readonly_namespace/empty/writable", 0755), 0);
    KREQUIRE_EQ(vfs::unmount_filesystem(EMPTY), 0);
    overmounted = false;
    KEXPECT_TRUE(tmp::tmpfs_directory_is_empty(empty));
    KEXPECT_TRUE(mount->read_only);
    KEXPECT_EQ(vfs::vfs_release_fd(&task, descriptor), 0);
    descriptor = -1;
    vfs::vfs_put_file(directory);
    mount.reset();
    KREQUIRE_EQ(vfs::unmount_filesystem(BASE), 0);
    mounted = false;
}

KTEST(Credentials, NativeLookupPolicyFollowsSelectedMountAcrossDirectoryAnchors) {
    namespace vfs = ker::vfs;
    namespace tmp = ker::vfs::tmpfs;
    constexpr const char* BASE = "/tmp/ktest_native_view_boundary";
    constexpr const char* RO = "/tmp/ktest_native_view_boundary/ro";
    constexpr const char* NESTED = "/tmp/ktest_native_view_boundary/ro/nested";
    constexpr const char* WRITABLE = "/tmp/ktest_native_view_boundary/rw/file";
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GID, .real = 100}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 100}, user), 0);
    ker::mod::sched::task::Task task{};
    task.pid = UINT64_MAX;
    std::memcpy(task.root.data(), "/", 2);
    std::memcpy(task.cwd.data(), "/", 2);
    task.root_len = task.cwd_len = 1;
    struct Cleanup {
        ker::mod::sched::task::Task& task;
        bool base{}, ro{}, nested{};
        int descriptor{-1};
        ~Cleanup() {
            task.cwd_anchor = {};
            if (descriptor >= 0) {
                auto* file = static_cast<vfs::File*>(task.fd_table.lookup(static_cast<uint64_t>(descriptor)));
                if (vfs::vfs_release_fd(&task, descriptor) == 0) vfs::vfs_put_file(file);
            }
            if (nested) static_cast<void>(vfs::unmount_filesystem(NESTED));
            if (ro) static_cast<void>(vfs::unmount_filesystem(RO));
            if (base) static_cast<void>(vfs::unmount_filesystem(BASE));
            static_cast<void>(vfs::vfs_rmdir(BASE));
        }
    } cleanup{task};
    KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(task, ROOT), 0);
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
    KREQUIRE_EQ(vfs::mount_filesystem(BASE, "tmpfs", nullptr, 0), 0);
    cleanup.base = true;
    auto base_mount = vfs::find_mount_point(BASE);
    KREQUIRE_TRUE(base_mount && base_mount->fs_type == vfs::FSType::TMPFS);
    auto* base_root = tmp::mount_root(static_cast<tmp::TmpfsMount*>(base_mount->private_data));
    KREQUIRE_TRUE(base_root != nullptr);
    base_root->mode = 0777;
    auto* rw = tmp::tmpfs_mkdir(base_root, "rw");
    KREQUIRE_TRUE(rw != nullptr && tmp::tmpfs_mkdir(base_root, "ro") != nullptr);
    rw->mode = 0777;
    KREQUIRE_TRUE(tmp::tmpfs_create_file(rw, "file", 0666) != nullptr);
    KREQUIRE_EQ(vfs::mount_filesystem(RO, "tmpfs", nullptr, vfs::MOUNT_FLAG_RDONLY | vfs::MOUNT_FLAG_NOSUID), 0);
    cleanup.ro = true;
    auto ro_mount = vfs::find_mount_point(RO);
    KREQUIRE_TRUE(ro_mount && ro_mount->fs_type == vfs::FSType::TMPFS && ro_mount->read_only && ro_mount->nosuid);
    auto* ro_root = tmp::mount_root(static_cast<tmp::TmpfsMount*>(ro_mount->private_data));
    KREQUIRE_TRUE(ro_root != nullptr);
    // Seed this disposable tree through construction APIs. Published native
    // policy stays immutable throughout the actual Task lookup operations.
    ro_root->mode = 0777;
    KREQUIRE_TRUE(tmp::tmpfs_create_file(ro_root, "inside", 0666) != nullptr);
    KREQUIRE_TRUE(tmp::tmpfs_mkdir(ro_root, "nested") != nullptr);
    KREQUIRE_TRUE(tmp::tmpfs_create_symlink(ro_root, "absolute", WRITABLE) != nullptr);
    KREQUIRE_EQ(vfs::mount_filesystem(NESTED, "tmpfs", nullptr, 0), 0);
    cleanup.nested = true;
    auto nested_mount = vfs::find_mount_point(NESTED);
    KREQUIRE_TRUE(nested_mount && nested_mount->fs_type == vfs::FSType::TMPFS);
    auto* nested_root = tmp::mount_root(static_cast<tmp::TmpfsMount*>(nested_mount->private_data));
    KREQUIRE_TRUE(nested_root != nullptr);
    nested_root->mode = 0777;
    KREQUIRE_TRUE(tmp::tmpfs_create_file(nested_root, "file", 0666) != nullptr);
    cleanup.descriptor = vfs::vfs_openat(&task, vfs::AT_FDCWD, RO, vfs::O_DIRECTORY, 0);
    KREQUIRE_TRUE(cleanup.descriptor >= 0);
    auto* directory = static_cast<vfs::File*>(task.fd_table.lookup(static_cast<uint64_t>(cleanup.descriptor)));
    KREQUIRE_TRUE(directory && vfs::file_mount_read_only(*directory) && vfs::file_mount_nosuid(*directory));
    for (const auto& caller : {ROOT, user}) {
        KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(task, caller), 0);
        for (bool cwd_relative : {false, true}) {
            if (cwd_relative) KREQUIRE_EQ(vfs::vfs_fchdir(&task, cleanup.descriptor), 0);
            int const FD = cwd_relative ? vfs::AT_FDCWD : cleanup.descriptor;
            for (const char* path : {"inside", "../rw/file", "absolute", "nested/file"}) {
                bool const RESTRICTED = std::strcmp(path, "inside") == 0;
                auto* selected =
                    RESTRICTED ? ro_mount.get() : (std::strcmp(path, "nested/file") == 0 ? nested_mount.get() : base_mount.get());
                vfs::LookupHandle lookup{};
                KREQUIRE_EQ(vfs::vfs_acquire_lookup(&task, FD, path, vfs::LookupIntent::OPEN, vfs::LookupFollowPolicy::FOLLOW_FINAL, false,
                                                    true, &lookup),
                            0);
                KEXPECT_TRUE(lookup.mount() == selected && lookup.authorization().credentials == caller);
                KEXPECT_TRUE(lookup.mount_policy().read_only == RESTRICTED && lookup.mount_policy().nosuid == RESTRICTED);
                vfs::LookupHandle moved{std::move(lookup)};
                KEXPECT_TRUE(lookup.mount() == nullptr && !lookup.mount_policy().read_only && !lookup.mount_policy().nosuid);
                KEXPECT_TRUE(moved.mount() == selected && moved.mount_policy().read_only == RESTRICTED);
                moved.reset();
                KEXPECT_TRUE(moved.mount() == nullptr && !moved.mount_policy().read_only && !moved.mount_policy().nosuid);
                int const OPENED = vfs::vfs_openat(&task, FD, path, RESTRICTED ? 0 : 2, 0);
                KREQUIRE_TRUE(OPENED >= 0);
                auto* file = static_cast<vfs::File*>(task.fd_table.lookup(static_cast<uint64_t>(OPENED)));
                KREQUIRE_TRUE(file != nullptr);
                KEXPECT_TRUE(file->mount_owner == selected && vfs::file_mount_read_only(*file) == RESTRICTED &&
                             vfs::file_mount_nosuid(*file) == RESTRICTED);
                if (!RESTRICTED) {
                    char const BYTE = 'w';
                    auto const POSITION = file->pos;
                    KEXPECT_EQ(vfs::vfs_pwrite_file(file, &BYTE, 1, 0), static_cast<ssize_t>(1));
                    KEXPECT_EQ(file->pos, POSITION);
                }
                KEXPECT_EQ(vfs::vfs_release_fd(&task, OPENED), 0);
                vfs::vfs_put_file(file);
                if (RESTRICTED) KEXPECT_EQ(vfs::vfs_openat(&task, FD, path, 1, 0), -EROFS);
            }
            KEXPECT_EQ(vfs::vfs_mkdirat(&task, FD, "new", 0700), -EROFS);
            for (const char* path : {"../rw/new", "nested/new"}) {
                KREQUIRE_EQ(vfs::vfs_mkdirat(&task, FD, path, 0700), 0);
                auto* expected_parent = path[0] == '.' ? rw : nested_root;
                KEXPECT_TRUE(tmp::tmpfs_lookup(expected_parent, "new") != nullptr);
                vfs::LookupHandle removal{};
                int const ACQUIRE = vfs::vfs_acquire_lookup(&task, FD, path, vfs::LookupIntent::REMOVE,
                                                            vfs::LookupFollowPolicy::NOFOLLOW_FINAL, true, true, &removal);
                KEXPECT_EQ(ACQUIRE, 0);
                if (ACQUIRE == 0) {
                    auto* binding = static_cast<tmp::TmpfsLookupHandle*>(removal.backend_parent());
                    KEXPECT_TRUE(binding && binding->parent() == expected_parent);
                    KEXPECT_TRUE(binding && !binding->root_target());
                    KEXPECT_TRUE(binding && binding->target() == tmp::tmpfs_lookup(expected_parent, "new"));
                }
                removal.reset();
                int const REMOVE = vfs::vfs_unlinkat(&task, FD, path, vfs::AT_REMOVEDIR);
                if (REMOVE != 0)
                    ker::mod::dbg::logger<"ktest">::warn("native mount control cwd=%d path=%s acquire=%d remove=%d", cwd_relative, path,
                                                         ACQUIRE, REMOVE);
                KREQUIRE_EQ(REMOVE, 0);
            }
            KEXPECT_TRUE(task.credentials() == caller && vfs::file_mount_read_only(*directory) && vfs::file_mount_nosuid(*directory));
        }
    }
    // Drop every retained anchor before verifying ordinary unmount drains.
    task.cwd_anchor = {};
    KREQUIRE_EQ(vfs::vfs_release_fd(&task, cleanup.descriptor), 0);
    cleanup.descriptor = -1;
    vfs::vfs_put_file(directory);
    nested_mount.reset();
    ro_mount.reset();
    base_mount.reset();
    KREQUIRE_EQ(vfs::unmount_filesystem(NESTED), 0);
    cleanup.nested = false;
    KREQUIRE_EQ(vfs::unmount_filesystem(RO), 0);
    cleanup.ro = false;
    KREQUIRE_EQ(vfs::unmount_filesystem(BASE), 0);
    cleanup.base = false;
}

KTEST(Credentials, TmpfsOwnedDirectoryViewNarrowsAndRetiresOutsideLocks) { KEXPECT_TRUE(ker::vfs::vfs_selftest_directory_view(false)); }

KTEST(Credentials, XfsOwnedDirectoryViewNarrowsAndRetiresOutsideLocks) { KEXPECT_TRUE(ker::vfs::vfs_selftest_directory_view(true)); }

KTEST(Credentials, RetainedDirectoryBoundarySurvivesRenameAndRejectsOutsideObjects) {
    namespace vfs = ker::vfs;
    constexpr const char* BASE = "/tmp/ktest_directory_boundary";
    constexpr const char* BOUNDARY = "/tmp/ktest_directory_boundary/root";
    constexpr const char* CHILD = "/tmp/ktest_directory_boundary/root/child";
    constexpr const char* MOVED = "/tmp/ktest_directory_boundary/moved";
    constexpr const char* MOVED_CHILD = "/tmp/ktest_directory_boundary/moved/child";
    constexpr const char* RENAMED = "/tmp/ktest_directory_boundary/moved/renamed";
    constexpr const char* SIBLING = "/tmp/ktest_directory_boundary/outside";
    constexpr const char* OUTSIDE = "/tmp/ktest_directory_boundary/outside/child";
    constexpr const char* REGULAR = "/tmp/ktest_directory_boundary/file";
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_GID, .real = 100}, user), 0);
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 100}, user), 0);
    for (bool disk : {false, true}) {
        struct Cleanup {
            std::array<vfs::File*, 5> files{};
            bool mounted = false;
            ~Cleanup() {
                for (auto* file : files) vfs::vfs_put_file(file);
                static_cast<void>(vfs::vfs_unlink(REGULAR));
                for (auto* path : {CHILD, MOVED_CHILD, RENAMED, OUTSIDE, BOUNDARY, MOVED, SIBLING}) static_cast<void>(vfs::vfs_rmdir(path));
                if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
                static_cast<void>(vfs::vfs_rmdir(BASE));
            }
        } cleanup;
        ker::mod::sched::task::Task task{};
        KREQUIRE_EQ(vfs::vfs_pin_directory_resolved_beneath_as(task, ROOT, "/", "/tmp", &cleanup.files[0]), 0);
        KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
        auto* device = disk ? ker::dev::block_device_find_by_name("sdb2") : nullptr;
        KREQUIRE_TRUE(!disk || device != nullptr);
        KREQUIRE_EQ(vfs::mount_filesystem(BASE, disk ? "xfs" : "tmpfs", device), 0);
        cleanup.mounted = true;
        KREQUIRE_EQ(vfs::vfs_chmod(BASE, 0755), 0);
        for (auto* path : {BOUNDARY, CHILD, SIBLING}) KREQUIRE_EQ(vfs::vfs_mkdir(path, 0755), 0);
        KREQUIRE_EQ(vfs::vfs_pin_directory_resolved_beneath_as(task, ROOT, BASE, BOUNDARY, &cleanup.files[1]), 0);
        KREQUIRE_EQ(vfs::vfs_pin_directory_resolved_beneath_as(task, ROOT, BASE, CHILD, &cleanup.files[2]), 0);
        auto* root = cleanup.files[1];
        auto* child = cleanup.files[2];
        KEXPECT_TRUE(root->vfs_path == nullptr && child->vfs_path == nullptr);
        auto const ROOT_REFS = root->refcount.load();
        auto const CHILD_REFS = child->refcount.load();
        auto expect = [&](vfs::File* directory, vfs::File* boundary, int error, const char* expected, size_t capacity = 128) {
            std::array<char, 128> output{};
            output.fill('!');
            auto const BEFORE = output;
            size_t length = 0x1234;
            int const RESULT = vfs::vfs_directory_path_beneath_directory(directory, boundary, output.data(), capacity, &length);
            KEXPECT_EQ(RESULT, error);
            if (error != 0) {
                KEXPECT_TRUE(output == BEFORE);
                KEXPECT_EQ(length, static_cast<size_t>(0x1234));
            } else {
                KEXPECT_TRUE(std::strcmp(output.data(), expected) == 0);
                KEXPECT_EQ(length, std::strlen(expected));
            }
        };
        expect(root, root, 0, "/");
        expect(child, root, 0, "/child");
        expect(root, cleanup.files[0], -ENOTSUP, nullptr);
        expect(child, nullptr, -EBADF, nullptr);
        expect(child, root, -EINVAL, nullptr, 0);
        expect(child, root, -ERANGE, nullptr, 2);
        cleanup.files[3] = vfs::vfs_open_file(REGULAR, vfs::O_CREAT | vfs::O_EXCL, 0644);
        KREQUIRE_TRUE(cleanup.files[3] != nullptr);
        expect(cleanup.files[3], root, -ENOTDIR, nullptr);
        expect(child, cleanup.files[3], -ENOTDIR, nullptr);
        // Preserve bad-handle precedence over type when both inputs are invalid.
        expect(cleanup.files[3], nullptr, -EBADF, nullptr);
        // Membership is not a search grant. Existing operation admission still
        // checks the current caller while identity recovery ignores ancestor DAC.
        KREQUIRE_EQ(vfs::vfs_chmod(BOUNDARY, 0000), 0);
        expect(child, root, 0, "/child");
        vfs::Stat denied{};
        KEXPECT_EQ(vfs::vfs_stat_resolved_beneath_as(task, user, BASE, CHILD, &denied, true, true), -EACCES);
        KREQUIRE_EQ(vfs::vfs_chmod(BOUNDARY, 0755), 0);
        KREQUIRE_EQ(vfs::vfs_renameat(&task, vfs::AT_FDCWD, BOUNDARY, vfs::AT_FDCWD, MOVED), 0);
        KREQUIRE_EQ(vfs::vfs_mkdir(BOUNDARY, 0755), 0);
        KREQUIRE_EQ(vfs::vfs_mkdir(CHILD, 0755), 0);
        KREQUIRE_EQ(vfs::vfs_pin_directory_resolved_beneath_as(task, ROOT, BASE, CHILD, &cleanup.files[4]), 0);
        expect(child, root, 0, "/child");
        expect(cleanup.files[4], root, -EPERM, nullptr);
        KREQUIRE_EQ(vfs::vfs_renameat(&task, vfs::AT_FDCWD, MOVED_CHILD, vfs::AT_FDCWD, RENAMED), 0);
        expect(child, root, 0, "/renamed");
        KREQUIRE_EQ(vfs::vfs_renameat(&task, vfs::AT_FDCWD, RENAMED, vfs::AT_FDCWD, OUTSIDE), 0);
        expect(child, root, -EPERM, nullptr);
        KREQUIRE_EQ(vfs::vfs_renameat(&task, vfs::AT_FDCWD, OUTSIDE, vfs::AT_FDCWD, RENAMED), 0);
        expect(child, root, 0, "/renamed");
        KREQUIRE_EQ(vfs::vfs_rmdir(RENAMED), 0);
        expect(child, root, -ENOENT, nullptr);
        expect(child, child, -ENOENT, nullptr);
        KREQUIRE_EQ(vfs::vfs_rmdir(MOVED), 0);
        expect(root, root, -ENOENT, nullptr);
        expect(cleanup.files[4], root, -ENOENT, nullptr);
        KEXPECT_EQ(root->refcount.load(), ROOT_REFS);
        KEXPECT_EQ(child->refcount.load(), CHILD_REFS);
        for (auto*& file : cleanup.files) {
            vfs::vfs_put_file(file);
            file = nullptr;
        }
        KREQUIRE_EQ(vfs::vfs_unlink(REGULAR), 0);
        for (auto* path : {CHILD, BOUNDARY, SIBLING}) KREQUIRE_EQ(vfs::vfs_rmdir(path), 0);
        KREQUIRE_EQ(vfs::unmount_filesystem(BASE), 0);
        cleanup.mounted = false;
        KREQUIRE_EQ(vfs::vfs_rmdir(BASE), 0);
    }
}

KTEST(Credentials, XfsLookupViewDenialsPreserveWritableBackendAndOutputs) {
    namespace vfs = ker::vfs;
    namespace xfs = ker::vfs::xfs;
    constexpr const char* BASE = "/tmp/ktest_xfs_lookup_view";
    constexpr const char* PATH = "/tmp/ktest_xfs_lookup_view/file";
    constexpr const char* EMPTY = "/tmp/ktest_xfs_lookup_view/empty";
    constexpr const char* NEW = "/tmp/ktest_xfs_lookup_view/new";
    struct Cleanup {
        vfs::File* file{};
        bool mounted{};
        ~Cleanup() {
            vfs::vfs_put_file(file);
            for (auto* path : {PATH, NEW}) static_cast<void>(vfs::vfs_unlink(path));
            for (auto* path : {EMPTY, NEW}) static_cast<void>(vfs::vfs_rmdir(path));
            if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
            static_cast<void>(vfs::vfs_rmdir(BASE));
        }
    } cleanup;
    auto* device = ker::dev::block_device_find_by_name("sdb2");
    KREQUIRE_TRUE(device != nullptr);
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
    KREQUIRE_EQ(vfs::mount_filesystem(BASE, "xfs", device), 0);
    cleanup.mounted = true;
    KREQUIRE_EQ(vfs::vfs_chmod(BASE, 0777), 0);
    KREQUIRE_EQ(vfs::vfs_mkdir(EMPTY, 0777), 0);
    cleanup.file = vfs::vfs_open_file(PATH, vfs::O_CREAT | vfs::O_EXCL | vfs::O_NO_CACHE | 2, 0666);
    KREQUIRE_TRUE(cleanup.file != nullptr);
    KREQUIRE_EQ(vfs::vfs_pwrite_file(cleanup.file, "seed", 4, 0), static_cast<ssize_t>(4));
    KREQUIRE_EQ(vfs::vfs_fsetxattr_file(cleanup.file, "user.view", "seed", 4, 0), 0);
    auto* mount = cleanup.file->mount_owner;
    KREQUIRE_TRUE(mount && mount->fs_type == vfs::FSType::XFS);
    auto* context = static_cast<xfs::XfsMountContext*>(mount->private_data);
    KREQUIRE_TRUE(context && !context->read_only && !mount->read_only);
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto user = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 100}, user), 0);
    vfs::TimestampUpdate times{}, omit{};
    constexpr std::array<vfs::Timespec, 2> TIMES{{{123, 456}, {789, 123}}};
    constexpr std::array<vfs::Timespec, 2> OMIT{{{0, vfs::TimestampUpdate::OMIT}, {0, vfs::TimestampUpdate::OMIT}}};
    KREQUIRE_EQ(vfs::TimestampUpdate::prepare(TIMES.data(), {}, times), 0);
    KREQUIRE_EQ(vfs::TimestampUpdate::prepare(OMIT.data(), {}, omit), 0);
    vfs::Stat before{}, parent_before{}, after{}, parent_after{};
    KREQUIRE_EQ(vfs::vfs_fstat_file(cleanup.file, &before), 0);
    KREQUIRE_EQ(vfs::vfs_stat(BASE, &parent_before), 0);
    {
        xfs::XfsLookupHandle file{}, missing{}, directory{}, invalid{};
        KREQUIRE_EQ(xfs::xfs_acquire_lookup(ROOT, "file", context, &file), 0);
        KREQUIRE_EQ(xfs::xfs_acquire_lookup(ROOT, "new", context, &missing), 0);
        KREQUIRE_EQ(xfs::xfs_acquire_lookup(ROOT, "empty", context, &directory), 0);
        auto const MUTATIONS = context->mutation_sequence.load();
        auto const LOOKUPS = context->lookup_sequence.load();
        for (const auto& caller : {ROOT, user}) {
            vfs::Stat output{};
            std::memset(&output, 0x5a, sizeof(output));
            auto const SENTINEL = output;
            bool changed = true, displaced = true;
            KEXPECT_EQ(xfs::xfs_chmod_lookup(file, caller, 0600, &output, true), -EROFS);
            KEXPECT_EQ(xfs::xfs_chown_lookup(file, caller, 100, 100, &output, true), -EROFS);
            KEXPECT_EQ(xfs::xfs_utimens_lookup(file, caller, times, &output, true), -EROFS);
            KEXPECT_EQ(xfs::xfs_setxattr_lookup(caller, file, "user.view", "edit", 4, 2, true), -EROFS);
            KEXPECT_EQ(xfs::xfs_removexattr_lookup(caller, file, "user.view", true), -EROFS);
            KEXPECT_EQ(xfs::xfs_mkdir_lookup(caller, missing, 0700, &output, true), -EROFS);
            KEXPECT_EQ(xfs::xfs_symlink_lookup(caller, missing, "file", &output, false, true), -EROFS);
            KEXPECT_EQ(xfs::xfs_unlink_lookup(caller, file, true), -EROFS);
            KEXPECT_EQ(xfs::xfs_rmdir_lookup(caller, directory, true), -EROFS);
            KEXPECT_EQ(xfs::xfs_link_lookup(caller, file, missing, &output, false, false, true), -EROFS);
            KEXPECT_EQ(xfs::xfs_rename_lookup(caller, file, missing, &output, false, false, &changed, &displaced, true), -EROFS);
            // Match native XFS's existing readonly precedence, including no-ops
            // and absent targets. Do not mutate shared mount policy for a view.
            KEXPECT_EQ(xfs::xfs_rename_lookup(caller, file, file, &output, false, false, &changed, &displaced, true), -EROFS);
            KEXPECT_EQ(xfs::xfs_utimens_lookup(file, caller, omit, &output, true), -EROFS);
            KEXPECT_EQ(xfs::xfs_unlink_lookup(caller, missing, true), -EROFS);
            KEXPECT_EQ(xfs::xfs_link_lookup(caller, file, invalid, &output, false, false, true), -EINVAL);
            KEXPECT_TRUE(changed && displaced && std::memcmp(&output, &SENTINEL, sizeof(output)) == 0);
            KEXPECT_EQ(context->mutation_sequence.load(), MUTATIONS);
            KEXPECT_EQ(context->lookup_sequence.load(), LOOKUPS);
            KEXPECT_TRUE(!context->read_only && !mount->read_only);
        }
        KEXPECT_EQ(xfs::xfs_validate_lookup(file), 0);
        KEXPECT_EQ(xfs::xfs_validate_lookup(missing), 0);
        KEXPECT_EQ(xfs::xfs_validate_lookup(directory), 0);
    }
    KREQUIRE_EQ(vfs::vfs_fstat_file(cleanup.file, &after), 0);
    KREQUIRE_EQ(vfs::vfs_stat(BASE, &parent_after), 0);
    KEXPECT_EQ(after.st_ino, before.st_ino);
    KEXPECT_EQ(after.st_mode, before.st_mode);
    KEXPECT_EQ(after.st_uid, before.st_uid);
    KEXPECT_EQ(after.st_gid, before.st_gid);
    KEXPECT_EQ(after.st_nlink, before.st_nlink);
    KEXPECT_EQ(after.st_size, before.st_size);
    KEXPECT_EQ(after.st_mtim.tv_sec, before.st_mtim.tv_sec);
    KEXPECT_EQ(after.st_mtim.tv_nsec, before.st_mtim.tv_nsec);
    KEXPECT_EQ(after.st_ctim.tv_sec, before.st_ctim.tv_sec);
    KEXPECT_EQ(after.st_ctim.tv_nsec, before.st_ctim.tv_nsec);
    KEXPECT_EQ(parent_after.st_nlink, parent_before.st_nlink);
    KEXPECT_EQ(parent_after.st_mtim.tv_sec, parent_before.st_mtim.tv_sec);
    KEXPECT_EQ(parent_after.st_mtim.tv_nsec, parent_before.st_mtim.tv_nsec);
    std::array<char, 4> bytes{};
    KEXPECT_EQ(vfs::vfs_fgetxattr_file(cleanup.file, "user.view", bytes.data(), bytes.size()), static_cast<ssize_t>(4));
    KEXPECT_TRUE(std::memcmp(bytes.data(), "seed", bytes.size()) == 0);
    KEXPECT_EQ(vfs::vfs_pread_file(cleanup.file, bytes.data(), bytes.size(), 0), static_cast<ssize_t>(4));
    KEXPECT_TRUE(std::memcmp(bytes.data(), "seed", bytes.size()) == 0);
    KEXPECT_EQ(vfs::vfs_stat(NEW, &after), -ENOENT);
    KREQUIRE_EQ(vfs::vfs_stat(EMPTY, &after), 0);
    // Independent unconfined/native authority still uses the writable backend.
    KEXPECT_EQ(vfs::vfs_fchmod_file_as(cleanup.file, ROOT, 0600), 0);
    KEXPECT_EQ(vfs::vfs_pwrite_file(cleanup.file, "edit", 4, 0), static_cast<ssize_t>(4));
    KREQUIRE_EQ(vfs::vfs_mkdir(NEW, 0700), 0);
    KREQUIRE_EQ(vfs::vfs_rmdir(NEW), 0);
}

KTEST(Credentials, ReadonlyPathMetadataPreservesIdentityTimesAndNoops) {
    namespace vfs = ker::vfs;
    namespace tmp = ker::vfs::tmpfs;
    namespace dev = ker::vfs::devfs;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto owner = ROOT, other = ROOT;
    KREQUIRE_EQ(owner.prepare({.change = security::Change::SET_UID, .real = 1000}, owner), 0);
    KREQUIRE_EQ(other.prepare({.change = security::Change::SET_UID, .real = 2000}, other), 0);
    constexpr std::array<vfs::Timespec, 2> TIMES{{{123, 456}, {789, 123}}};
    constexpr std::array<vfs::Timespec, 2> OMIT{{{0, vfs::TimestampUpdate::OMIT}, {0, vfs::TimestampUpdate::OMIT}}};
    constexpr std::array<vfs::Timespec, 2> MIXED{{{0, vfs::TimestampUpdate::NOW}, {0, vfs::TimestampUpdate::OMIT}}};
    constexpr std::array<vfs::Timespec, 2> INVALID{{{0, 1000000000}, {0, 0}}};
    constexpr const char* DEVICE_NAME = "pts/ktest_readonly_metadata";
    for (bool device_fs : {false, true}) {
        const char* const BASE = device_fs ? "/tmp/ktest_ro_meta_dev" : "/tmp/ktest_ro_meta_tmp";
        const char* const PATH = device_fs ? "/tmp/ktest_ro_meta_dev/pts/ktest_readonly_metadata" : "/tmp/ktest_ro_meta_tmp/owned";
        const char* const DIRECTORY = device_fs ? "/tmp/ktest_ro_meta_dev/pts" : BASE;
        const char* const NAME = device_fs ? "ktest_readonly_metadata" : "owned";
        DevfsOpenFixture device_state{};
        ker::dev::CharDeviceOps operations{};
        operations.open = credential_device_open;
        operations.close = credential_device_close;
        ker::dev::Device device{};
        device.char_ops = &operations;
        device.private_data = &device_state;
        ker::mod::sched::task::Task task{};
        task.pid = UINT64_MAX;
        std::memcpy(task.root.data(), "/", 2);
        std::memcpy(task.cwd.data(), "/", 2);
        task.root_len = task.cwd_len = 1;
        struct Cleanup {
            ker::mod::sched::task::Task& task;
            const char* base;
            vfs::File* file{};
            vfs::File* directory{};
            int descriptor{-1};
            bool mounted{};
            bool device_added{};
            ~Cleanup() {
                if (descriptor >= 0) static_cast<void>(vfs::vfs_release_fd(&task, descriptor));
                vfs::vfs_put_file(directory);
                vfs::vfs_put_file(file);
                if (mounted) static_cast<void>(vfs::unmount_filesystem(base));
                if (device_added) static_cast<void>(dev::devfs_remove_node(DEVICE_NAME));
                static_cast<void>(vfs::vfs_rmdir(base));
            }
        } cleanup{task, BASE};
        KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
        KREQUIRE_EQ(vfs::mount_filesystem(BASE, device_fs ? "devfs" : "tmpfs", nullptr, vfs::MOUNT_FLAG_RDONLY), 0);
        cleanup.mounted = true;
        auto mount = vfs::find_mount_point(BASE);
        KREQUIRE_TRUE(mount && mount->read_only);
        if (device_fs) {
            auto* node = dev::devfs_add_device_node(DEVICE_NAME, &device);
            KREQUIRE_TRUE(node != nullptr);
            cleanup.device_added = true;
            KREQUIRE_EQ(dev::devfs_chown_node(node, ROOT, 1000, 0, mount->dev_id), 0);
            KREQUIRE_EQ(dev::devfs_chmod_node(node, ROOT, 0644, mount->dev_id), 0);
        } else {
            auto* root = tmp::mount_root(static_cast<tmp::TmpfsMount*>(mount->private_data));
            auto* node = tmp::tmpfs_create_file(root, NAME, 0644);
            KREQUIRE_TRUE(node != nullptr);
            KREQUIRE_EQ(tmp::tmpfs_chown_node(node, ROOT, 1000, 0), 0);
        }
        cleanup.file = vfs::vfs_open_file(PATH, 0, 0);
        KREQUIRE_TRUE(cleanup.file != nullptr && cleanup.file->mount_owner == mount.get());
        if (device_fs) {
            KEXPECT_EQ(vfs::vfs_pin_directory_resolved_beneath_as(task, ROOT, BASE, DIRECTORY, &cleanup.directory), -ENOTSUP);
            cleanup.directory = vfs::vfs_open_file(DIRECTORY, vfs::O_DIRECTORY, 0);
            KREQUIRE_TRUE(cleanup.directory != nullptr);
        } else {
            KREQUIRE_EQ(vfs::vfs_pin_directory_resolved_beneath_as(task, ROOT, BASE, DIRECTORY, &cleanup.directory), 0);
        }
        cleanup.descriptor = vfs::vfs_alloc_fd(&task, cleanup.directory);
        KREQUIRE_TRUE(cleanup.descriptor >= 0);
        vfs::Stat before{}, after{};
        KREQUIRE_EQ(vfs::vfs_stat(PATH, &before), 0);
        for (const auto& caller : {ROOT, owner, other}) {
            KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(task, caller), 0);
            int const EXPECTED = caller == other ? -EPERM : -EROFS;
            int const RELATIVE_EXPECTED = device_fs ? -ENOTSUP : EXPECTED;
            KEXPECT_EQ(vfs::vfs_fchmodat(&task, vfs::AT_FDCWD, PATH, 0777, 0), EXPECTED);
            KEXPECT_EQ(vfs::vfs_fchmodat(&task, cleanup.descriptor, NAME, 0777, 0), EXPECTED);
            KEXPECT_EQ(vfs::vfs_chmod_resolved_beneath_as(task, caller, BASE, PATH, 0777, true, false), EXPECTED);
            KEXPECT_EQ(vfs::vfs_chmod_resolved_beneath_as(task, caller, BASE, NAME, 0777, true, false, cleanup.directory),
                       RELATIVE_EXPECTED);
            KEXPECT_EQ(vfs::vfs_utimens_resolved_beneath_as(task, caller, BASE, PATH, TIMES.data(), true, false), EXPECTED);
            KEXPECT_EQ(vfs::vfs_utimens_resolved_beneath_as(task, caller, BASE, NAME, TIMES.data(), true, false, cleanup.directory),
                       RELATIVE_EXPECTED);
            KEXPECT_EQ(vfs::vfs_utimens_resolved_beneath_as(task, caller, BASE, NAME, MIXED.data(), true, false, cleanup.directory),
                       RELATIVE_EXPECTED);
            KEXPECT_EQ(vfs::vfs_utimens_resolved_beneath_as(task, caller, BASE, NAME, nullptr, true, false, cleanup.directory),
                       device_fs         ? -ENOTSUP
                       : caller == other ? -EACCES
                                         : -EROFS);
            KEXPECT_EQ(vfs::vfs_utimens_resolved_beneath_as(task, caller, BASE, NAME, OMIT.data(), true, false, cleanup.directory),
                       device_fs ? -ENOTSUP : 0);
            KEXPECT_EQ(vfs::vfs_utimens_resolved_beneath_as(task, caller, BASE, NAME, INVALID.data(), true, false, cleanup.directory),
                       -EINVAL);
            KEXPECT_EQ(vfs::vfs_fchmodat(&task, cleanup.descriptor, "missing", 0777, 0), -ENOENT);
            KEXPECT_EQ(vfs::vfs_utimens_resolved_beneath_as(task, caller, BASE, "missing", TIMES.data(), true, false, cleanup.directory),
                       device_fs ? -ENOTSUP : -ENOENT);
            KEXPECT_TRUE(task.credentials() == caller);
        }
        // The worker is now unprivileged; explicit admitted root still cannot
        // alter this mount's metadata. Descriptor gates retain their own order.
        KEXPECT_EQ(vfs::vfs_chmod_resolved_beneath_as(task, ROOT, BASE, PATH, 0777, true, false), -EROFS);
        KEXPECT_EQ(vfs::vfs_utimens_resolved_beneath_as(task, ROOT, BASE, PATH, TIMES.data(), true, false), -EROFS);
        KEXPECT_EQ(vfs::vfs_chmod_resolved(PATH, 0777, true), -EROFS);
        KEXPECT_EQ(vfs::vfs_utimensat(vfs::AT_FDCWD, PATH, TIMES.data(), 0), -EROFS);
        KEXPECT_EQ(vfs::vfs_utimens_resolved_beneath(BASE, PATH, TIMES.data(), true), -EROFS);
        KEXPECT_EQ(vfs::vfs_utimens_resolved_beneath(BASE, PATH, OMIT.data(), true), 0);
        KEXPECT_EQ(vfs::vfs_fchmod_file_as(cleanup.file, ROOT, 0777), -EROFS);
        KEXPECT_EQ(vfs::vfs_futimens_file_as(cleanup.file, ROOT, TIMES.data()), -EROFS);
        KEXPECT_EQ(vfs::vfs_futimens_file_as(cleanup.file, other, OMIT.data()), 0);
        KREQUIRE_EQ(vfs::vfs_stat(PATH, &after), 0);
        KEXPECT_EQ(after.st_ino, before.st_ino);
        KEXPECT_EQ(after.st_mode, before.st_mode);
        KEXPECT_EQ(after.st_uid, before.st_uid);
        KEXPECT_EQ(after.st_gid, before.st_gid);
        KEXPECT_EQ(after.st_atim.tv_sec, before.st_atim.tv_sec);
        KEXPECT_EQ(after.st_atim.tv_nsec, before.st_atim.tv_nsec);
        KEXPECT_EQ(after.st_mtim.tv_sec, before.st_mtim.tv_sec);
        KEXPECT_EQ(after.st_mtim.tv_nsec, before.st_mtim.tv_nsec);
        KEXPECT_EQ(after.st_ctim.tv_sec, before.st_ctim.tv_sec);
        KEXPECT_EQ(after.st_ctim.tv_nsec, before.st_ctim.tv_nsec);
        KEXPECT_TRUE(task.credentials() == other && mount->read_only);
        if (device_fs) {
            KEXPECT_EQ(device_state.opens, 1U);
            KEXPECT_EQ(device_state.closes, 0U);
        }
    }
}

KTEST(Credentials, ReadonlyRemotePathMutationsPreserveClientState) {
    namespace vfs = ker::vfs;
    namespace wki = ker::net::wki;
    constexpr auto RO = "/tmp/ktest_remote_readonly";
    constexpr auto RW = "/tmp/ktest_remote_writable";
    constexpr auto RO_PATH = "/tmp/ktest_remote_readonly/child";
    constexpr auto RW_PATH = "/tmp/ktest_remote_writable/child";
    auto proxy = std::make_unique<wki::ProxyVfsState>();
    struct Cleanup {
        ~Cleanup() {
            static_cast<void>(vfs::unmount_filesystem(RO));
            static_cast<void>(vfs::unmount_filesystem(RW));
            static_cast<void>(vfs::vfs_rmdir(RO));
            static_cast<void>(vfs::vfs_rmdir(RW));
        }
    } cleanup;
    for (auto path : {RO, RW}) {
        KREQUIRE_EQ(vfs::vfs_mkdir(path, 0755), 0);
        KREQUIRE_EQ(vfs::mount_filesystem(path, "remote", nullptr, path == RO ? vfs::MOUNT_FLAG_RDONLY : 0, nullptr, proxy.get(),
                                          wki::wki_remote_vfs_get_fops()),
                    0);
    }
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto user = ROOT, restricted = ROOT;
    KREQUIRE_EQ(user.prepare({.change = security::Change::SET_UID, .real = 1000}, user), 0);
    KREQUIRE_EQ(restricted.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, restricted), 0);
    for (auto credentials : {ROOT, user, restricted}) {
        ker::mod::sched::task::Task task{};
        task.pid = UINT64_MAX;
        std::strcpy(task.root.data(), "/");
        std::strcpy(task.cwd.data(), "/");
        task.root_len = task.cwd_len = 1;
        KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(task, credentials), 0);
        auto mount = vfs::find_mount_point(RO);
        KREQUIRE_TRUE(mount && mount->read_only);
        wki::RemoteFileContext context{};
        context.proxy = proxy.get();
        context.remote_fd = 17;
        context.stat_cache_valid = true;
        vfs::File directory{};
        directory.fs_type = vfs::FSType::REMOTE;
        directory.fops = wki::wki_remote_vfs_get_fops();
        directory.private_data = &context;
        directory.is_directory = true;
        directory.mount_owner = mount.get();
        directory.vfs_path = RO;
        directory.vfs_path_len = std::strlen(RO);
        directory.refcount.store(1);
        KREQUIRE_TRUE(task.fd_table.insert(17, &directory));
        struct RemoveBorrowedFd {
            ker::mod::sched::task::Task& task;
            ~RemoveBorrowedFd() { task.fd_table.remove(17); }
        } remove_fd{task};
        auto const GENERATION = vfs::mount_table_generation_snapshot();
        auto const REFS = vfs::mount_point_ref_count_for_test(mount.get());
        auto const OPEN_FILES = vfs::mount_point_open_file_count_for_test(mount.get());
        for (bool relative : {false, true}) {
            int const FD = relative ? 17 : vfs::AT_FDCWD;
            const char* const PATH = relative ? "child" : RO_PATH;
            KEXPECT_EQ(vfs::vfs_mkdirat(&task, FD, PATH, 0700), -EROFS);
            KEXPECT_EQ(vfs::vfs_symlinkat(&task, "target", FD, PATH), -EROFS);
            KEXPECT_EQ(vfs::vfs_unlinkat(&task, FD, PATH, 0), -EROFS);
            KEXPECT_EQ(vfs::vfs_unlinkat(&task, FD, PATH, vfs::AT_REMOVEDIR), -EROFS);
            KEXPECT_EQ(vfs::vfs_fchmodat(&task, FD, PATH, 0600, 0), -EROFS);
            KEXPECT_EQ(vfs::vfs_fchownat(&task, FD, PATH, 1000, 1000, 0), -EROFS);
            KEXPECT_EQ(vfs::vfs_renameat(&task, FD, PATH, FD, PATH), -EROFS);
            KEXPECT_EQ(vfs::vfs_renameat(&task, FD, PATH, FD, relative ? "other" : "/tmp/ktest_remote_readonly/other"), -EROFS);
            KEXPECT_EQ(vfs::vfs_renameat(&task, FD, PATH, vfs::AT_FDCWD, RW_PATH), -EXDEV);
            KEXPECT_EQ(vfs::vfs_fchmodat(&task, FD, PATH, 0600, 0x40000000), -EINVAL);
            KEXPECT_EQ(vfs::vfs_unlinkat(&task, FD, PATH, 0x40000000), -EINVAL);
        }
        KEXPECT_EQ(vfs::vfs_mkdirat(&task, 9999, "child", 0700), -EBADF);
        KEXPECT_EQ(vfs::vfs_mkdirat(&task, 17, "", 0700), -ENOENT);
        KEXPECT_EQ(vfs::vfs_symlinkat(&task, nullptr, 17, "child"), -EINVAL);
        KEXPECT_EQ(vfs::vfs_mkdirat(&task, vfs::AT_FDCWD, RW_PATH, 0700), -ENOTCONN);
        KEXPECT_EQ(vfs::vfs_fchmodat(&task, vfs::AT_FDCWD, RW_PATH, 0600, 0), -ENOTCONN);
        KEXPECT_EQ(vfs::vfs_selftest_remote_readonly_resolved(credentials, RO_PATH, -EROFS), 0);
        KEXPECT_EQ(vfs::vfs_selftest_remote_readonly_resolved(credentials, RW_PATH, -ENOTCONN), 0);
        for (auto operation :
             {vfs::MetadataBatchOperation::CREATE_CLOSE, vfs::MetadataBatchOperation::UNLINK, vfs::MetadataBatchOperation::RENAME}) {
            bool const RENAME = operation == vfs::MetadataBatchOperation::RENAME;
            std::array<vfs::MetadataBatchEntry, 2> entries{
                {{RO_PATH, RENAME ? RO_PATH : nullptr}, {"/tmp/ktest_remote_readonly/other", RENAME ? RO_PATH : nullptr}}};
            std::array<vfs::MetadataBatchResult, 2> results{};
            KEXPECT_EQ(vfs::vfs_metadata_batch(&task, operation, 0, entries.data(), entries.size(), results.data()), -EROFS);
            for (auto const& result : results) {
                KEXPECT_EQ(result.status, -EROFS);
                KEXPECT_EQ(result.stat.st_ino, 0U);
            }
            entries[1].path = nullptr;
            KEXPECT_EQ(vfs::vfs_metadata_batch(&task, operation, 0, entries.data(), entries.size(), results.data()), -EINVAL);
        }
        vfs::MetadataBatchEntry const read_entry{RO_PATH, nullptr};
        vfs::MetadataBatchResult read_result{};
        KEXPECT_EQ(vfs::vfs_metadata_batch(&task, vfs::MetadataBatchOperation::STAT_FOLLOW, 0, &read_entry, 1, &read_result), -ENOTSUP);
        KEXPECT_TRUE(task.credentials() == credentials);
        KEXPECT_EQ(directory.refcount.load(), 1);
        KEXPECT_TRUE(context.stat_cache_valid);
        KEXPECT_FALSE(context.cache_invalidation_pending.load());
        KEXPECT_EQ(vfs::mount_table_generation_snapshot(), GENERATION);
        KEXPECT_EQ(vfs::mount_point_ref_count_for_test(mount.get()), REFS);
        KEXPECT_EQ(vfs::mount_point_open_file_count_for_test(mount.get()), OPEN_FILES);
        KEXPECT_FALSE(proxy->op_pending.load());
        KEXPECT_EQ(proxy->op_generation, 0U);
        KEXPECT_EQ(proxy->op_slot_waiter_count, 0U);
    }
    KEXPECT_EQ(vfs::unmount_filesystem(RO), 0);
    KEXPECT_EQ(vfs::unmount_filesystem(RW), 0);
}

KTEST(Credentials, TmpfsSerializedLookupRetiresDescriptorsOutsideLocks) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_serialized_lookup_retirement(false));
}

KTEST(Credentials, XfsSerializedLookupRetiresDescriptorsOutsideLocks) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_serialized_lookup_retirement(true));
}

KTEST(Credentials, TmpfsDirectoryFilesAndCwdPreserveOwnedViews) { KEXPECT_TRUE(ker::vfs::vfs_selftest_directory_view_publication(false)); }

KTEST(Credentials, XfsDirectoryFilesAndCwdPreserveOwnedViews) { KEXPECT_TRUE(ker::vfs::vfs_selftest_directory_view_publication(true)); }

KTEST(Credentials, TmpfsProcfsMagicLinksPreserveOwnedViews) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_directory_view_publication(false, true));
}

KTEST(Credentials, XfsProcfsMagicLinksPreserveOwnedViews) { KEXPECT_TRUE(ker::vfs::vfs_selftest_directory_view_publication(true, true)); }

KTEST(Credentials, TmpfsPreparesOwnedRootAndCwdTogether) { KEXPECT_TRUE(ker::vfs::vfs_selftest_prepared_filesystem_context(false)); }

KTEST(Credentials, XfsPreparesOwnedRootAndCwdTogether) { KEXPECT_TRUE(ker::vfs::vfs_selftest_prepared_filesystem_context(true)); }

KTEST(Credentials, TmpfsMountDescendantsKeepParentAlive) { KEXPECT_TRUE(ker::vfs::mount_selftest_descendant_lifetime(false)); }

KTEST(Credentials, XfsMountDescendantsKeepParentAlive) { KEXPECT_TRUE(ker::vfs::mount_selftest_descendant_lifetime(true)); }

KTEST(Credentials, TmpfsMountRetainsExactCoveredDirectory) { KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_cover(false)); }

KTEST(Credentials, XfsMountRetainsExactCoveredDirectory) { KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_cover(true)); }

KTEST(Credentials, TmpfsDirectoryIdentityRetentionDoesNotGrantAccess) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_directory_view_admission(false));
}

KTEST(Credentials, XfsDirectoryIdentityRetentionDoesNotGrantAccess) { KEXPECT_TRUE(ker::vfs::vfs_selftest_directory_view_admission(true)); }

KTEST(Credentials, TmpfsDirectoryViewRebaseBoundsOwnershipAndRechecksTarget) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_directory_view_rebase(false));
}

KTEST(Credentials, XfsDirectoryViewRebaseBoundsOwnershipAndRechecksTarget) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_directory_view_rebase(true));
}

KTEST(Credentials, TmpfsLookupRetainsDirectoryViewBoundaryAndPolicy) { KEXPECT_TRUE(ker::vfs::vfs_selftest_directory_view_lookup(false)); }

KTEST(Credentials, XfsLookupRetainsDirectoryViewBoundaryAndPolicy) { KEXPECT_TRUE(ker::vfs::vfs_selftest_directory_view_lookup(true)); }

KTEST(Credentials, XfsSearchAuthorityAcrossMounts) { KEXPECT_TRUE(ker::vfs::vfs_selftest_xfs_search_authority()); }

KTEST(Credentials, TmpfsRootMembershipAcrossNativeMounts) { KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(false)); }

KTEST(Credentials, XfsRootMembershipAcrossNativeMounts) { KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(true)); }

KTEST(Credentials, TmpfsCrossMountContextOwnsTypedAnchors) { KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(false, true)); }

KTEST(Credentials, XfsCrossMountContextOwnsTypedAnchors) { KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(true, true)); }

KTEST(Credentials, TmpfsMountMembershipReusesEpochOwnedReferences) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(false, false, true));
}

KTEST(Credentials, XfsMountMembershipReusesEpochOwnedReferences) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(true, false, true));
}

KTEST(Credentials, TmpfsLookupOwnsReadOnlyMountMembership) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(false, false, false, true));
}

KTEST(Credentials, XfsLookupOwnsReadOnlyMountMembership) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(true, false, false, true));
}

KTEST(Credentials, TmpfsNativeMountCrossingUsesCoveredObjects) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(false, false, false, false, true));
}

KTEST(Credentials, XfsNativeMountCrossingUsesCoveredObjects) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(true, false, false, false, true));
}

KTEST(Credentials, TmpfsTypedAnchorPathAcrossNativeMounts) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(false, false, false, false, false, true));
}
KTEST(Credentials, XfsTypedAnchorPathAcrossNativeMounts) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(true, false, false, false, false, true));
}

KTEST(Credentials, TmpfsCwdRecoveryOwnsCrossMountProof) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(false, false, false, false, false, false, true));
}
KTEST(Credentials, XfsCwdRecoveryOwnsCrossMountProof) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(true, false, false, false, false, false, true));
}

KTEST(Credentials, TmpfsScopedWalkProvesSelectedObjectsAcrossNativeMounts) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(false, false, false, false, false, false, false, true));
}
KTEST(Credentials, XfsScopedWalkProvesSelectedObjectsAcrossNativeMounts) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(true, false, false, false, false, false, false, true));
}

KTEST(Credentials, TmpfsStatvfsUsesCallerAndPreservesDeniedOutput) { KEXPECT_TRUE(ker::vfs::vfs_selftest_statvfs_authorization(false)); }
KTEST(Credentials, XfsStatvfsUsesCallerAndPreservesDeniedOutput) { KEXPECT_TRUE(ker::vfs::vfs_selftest_statvfs_authorization(true)); }
KTEST(Credentials, TmpfsStatvfsUsesScopedNativeCwd) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(false, false, false, false, false, false, false, true, true));
}
KTEST(Credentials, XfsStatvfsUsesScopedNativeCwd) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_native_mount_membership(true, false, false, false, false, false, false, true, true));
}

namespace {
void check_access_identity(bool disk) {
    namespace vfs = ker::vfs;
    constexpr const char* BASE = "/tmp/ktest_access_identity";
    constexpr const char* DIRECT = "/tmp/ktest_access_identity/direct";
    constexpr const char* HIDDEN = "/tmp/ktest_access_identity/hidden";
    constexpr const char* LEAF = "/tmp/ktest_access_identity/hidden/leaf";
    constexpr const char* GROUP = "/tmp/ktest_access_identity/group";
    constexpr const char* GROUP_LEAF = "/tmp/ktest_access_identity/group/leaf";
    struct Cleanup {
        bool mounted{};
        ~Cleanup() {
            for (const char* path : {DIRECT, LEAF, GROUP_LEAF}) static_cast<void>(vfs::vfs_unlink(path));
            for (const char* path : {HIDDEN, GROUP}) static_cast<void>(vfs::vfs_rmdir(path));
            if (mounted) static_cast<void>(vfs::unmount_filesystem(BASE));
            static_cast<void>(vfs::vfs_rmdir(BASE));
        }
    } cleanup;
    KREQUIRE_EQ(vfs::vfs_mkdir(BASE, 0755), 0);
    auto* device = disk ? ker::dev::block_device_find_by_name("sdb2") : nullptr;
    KREQUIRE_TRUE(!disk || device != nullptr);
    KREQUIRE_EQ(vfs::mount_filesystem(BASE, disk ? "xfs" : "tmpfs", device), 0);
    cleanup.mounted = true;
    KREQUIRE_EQ(vfs::vfs_mkdir(HIDDEN, 0700), 0);
    KREQUIRE_EQ(vfs::vfs_mkdir(GROUP, 0755), 0);
    for (const char* path : {DIRECT, LEAF, GROUP_LEAF}) {
        auto* file = vfs::vfs_open_file(path, vfs::O_CREAT | vfs::O_EXCL | 1, 0644);
        KREQUIRE_TRUE(file != nullptr);
        KREQUIRE_EQ(vfs::vfs_close_file(file), 0);
    }
    KREQUIRE_EQ(vfs::vfs_chmod(DIRECT, 0000), 0);
    KREQUIRE_EQ(vfs::vfs_chown(GROUP, 4000, 2000), 0);
    KREQUIRE_EQ(vfs::vfs_chmod(GROUP, 0010), 0);
    struct Profile {
        security::Ids user, group;
        bool real_root, effective_root, real_group, effective_group;
    };
    std::array const profiles{Profile{{1000, 0, 1001}, {2001, 2001, 2002}, false, true, false, false},
                              Profile{{0, 1000, 1001}, {2001, 2001, 2002}, true, false, false, false},
                              Profile{{1000, 1000, 1001}, {2000, 2001, 2002}, false, false, true, false},
                              Profile{{1000, 1000, 1001}, {2001, 2000, 2002}, false, false, false, true}};
    for (auto const& profile : profiles) {
        for (bool restricted : {false, true}) {
            for (bool supplementary : {false, true}) {
                std::array<uint32_t, 1> groups{2000};
                security::Snapshot caller;
                KREQUIRE_EQ(security::Snapshot::restore_fields(
                                {.user = profile.user,
                                 .group = profile.group,
                                 .groups = supplementary ? std::span<const uint32_t>(groups) : std::span<const uint32_t>{},
                                 .authority_ceiling = restricted ? 0 : security::ALL_AUTHORITY,
                                 .service_privileges = 3,
                                 .no_new_privileges = true,
                                 .service_privileges_locked = true},
                                caller),
                            0);
                ker::mod::sched::task::Task task{};
                task.pid = UINT64_MAX;
                auto* opened = vfs::vfs_open_file(DIRECT, 0, 0);
                KREQUIRE_TRUE(opened != nullptr);
                int const FD = vfs::vfs_alloc_fd(&task, opened);
                KREQUIRE_TRUE(FD >= 0);
                KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(task, caller), 0);
                for (bool effective : {false, true}) {
                    int const FLAGS = effective ? vfs::AT_EACCESS : 0;
                    bool const ROOT = effective ? profile.effective_root : profile.real_root;
                    bool const GROUP_SEARCH =
                        (ROOT && !restricted) || supplementary || (effective ? profile.effective_group : profile.real_group);
                    KEXPECT_EQ(vfs::vfs_faccessat(&task, vfs::AT_FDCWD, DIRECT, 4, FLAGS), ROOT && !restricted ? 0 : -EACCES);
                    KEXPECT_EQ(vfs::vfs_faccessat(&task, vfs::AT_FDCWD, LEAF, 4, FLAGS), ROOT ? 0 : -EACCES);
                    KEXPECT_EQ(vfs::vfs_faccessat(&task, vfs::AT_FDCWD, GROUP_LEAF, 4, FLAGS), GROUP_SEARCH ? 0 : -EACCES);
                    KEXPECT_EQ(vfs::vfs_faccessat(&task, FD, "", 4, FLAGS | vfs::AT_EMPTY_PATH), ROOT && !restricted ? 0 : -EACCES);
                    KEXPECT_EQ(vfs::vfs_faccessat(&task, -99, DIRECT, 8, FLAGS), 0);
                    KEXPECT_EQ(vfs::vfs_faccessat(&task, -99, DIRECT, -1, FLAGS), -EACCES);
                    KEXPECT_EQ(vfs::vfs_faccessat(&task, vfs::AT_FDCWD, DIRECT, 0, FLAGS | 0x40000000), -EINVAL);
                    KEXPECT_TRUE(task.credentials() == caller);
                }
                KREQUIRE_EQ(vfs::vfs_release_fd(&task, FD), 0);
                vfs::vfs_put_file(opened);
            }
        }
    }
}
}  // namespace
KTEST(Credentials, TmpfsAccessUsesOneRealOrEffectiveIdentity) { check_access_identity(false); }
KTEST(Credentials, XfsAccessUsesOneRealOrEffectiveIdentity) { check_access_identity(true); }
