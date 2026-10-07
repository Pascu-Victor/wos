#include <cstdint>
#include <cstring>
#include <dev/block_device.hpp>
#include <memory>
#include <net/wki/remote_vfs.hpp>
#include <platform/security/credential_record.hpp>
#include <test/ktest.hpp>
#include <vfs/coherency.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>
#include <vfs/fs/xfs/xfs_vfs.hpp>
#include <vfs/vfs.hpp>

namespace coherence = ker::vfs::coherency;

KTEST(VfsCoherency, CacheFillCannotAcquireAuthorityAcrossMutationOrObjectReplacement) {
    coherence::CacheValidation cache;
    coherence::Stamp const BEFORE{1, 2, 3, 1};
    coherence::Stamp const AFTER{1, 4, 4, 1};
    coherence::Stamp const REPLACEMENT{5, 5, 5, 5};
    KREQUIRE_TRUE(cache.publish(BEFORE, BEFORE, coherence::Domain::CONTENT));
    KEXPECT_TRUE(cache.matches(BEFORE, coherence::Domain::CONTENT));
    KEXPECT_FALSE(cache.publish(BEFORE, AFTER, coherence::Domain::CONTENT));
    KEXPECT_FALSE(cache.matches(BEFORE, coherence::Domain::CONTENT));
    KEXPECT_FALSE(cache.matches(AFTER, coherence::Domain::CONTENT));
    KREQUIRE_TRUE(cache.publish(AFTER, AFTER, coherence::Domain::CONTENT));
    KEXPECT_FALSE(cache.matches(REPLACEMENT, coherence::Domain::CONTENT));
    cache.invalidate();
    KEXPECT_FALSE(cache.matches(AFTER, coherence::Domain::CONTENT));
    KEXPECT_FALSE(cache.publish(AFTER, {}, coherence::Domain::CONTENT));
}

KTEST(VfsCoherency, CommitPublishesContentAndMetadataButNotDirectory) {
    coherence::Counter counter;
    coherence::State state;
    KREQUIRE_TRUE(state.initialize(counter));
    coherence::Stamp old{};
    KREQUIRE_TRUE(state.snapshot(old));
    coherence::Ticket ticket{};
    KREQUIRE_TRUE(state.prepare(coherence::Domain::CONTENT, ticket));
    coherence::Stamp output{7, 7, 7, 7};
    KEXPECT_FALSE(state.snapshot(output));
    KEXPECT_EQ(output.object, uint64_t{7});
    KREQUIRE_TRUE(state.publish(ticket));
    KREQUIRE_TRUE(state.snapshot(output));
    KEXPECT_FALSE(coherence::cache_matches(old, output, coherence::Domain::CONTENT));
    KEXPECT_FALSE(coherence::cache_matches(old, output, coherence::Domain::METADATA));
    KEXPECT_TRUE(coherence::cache_matches(old, output, coherence::Domain::DIRECTORY));
}

KTEST(VfsCoherency, CancelCannotReuseTicketOrClosePendingEffect) {
    coherence::Counter counter;
    coherence::State state;
    KREQUIRE_TRUE(state.initialize(counter));
    coherence::Ticket old{}, current{};
    KREQUIRE_TRUE(state.prepare(coherence::Domain::ALL, old));
    KREQUIRE_TRUE(state.cancel_no_effect(old));
    KREQUIRE_TRUE(state.prepare(coherence::Domain::ALL, current));
    KEXPECT_TRUE(current.serial > old.serial);
    KEXPECT_FALSE(state.publish(old));
    KEXPECT_FALSE(state.cancel_no_effect(old));
    KEXPECT_FALSE(state.close_admission());
    KEXPECT_FALSE(state.prepare(coherence::Domain::CONTENT, old));
    KREQUIRE_TRUE(state.publish(current));
    KEXPECT_TRUE(state.quiescent());
    KEXPECT_TRUE(state.close_admission());
    coherence::Stamp stamp{};
    KEXPECT_FALSE(state.snapshot(stamp));
    KEXPECT_FALSE(state.initialize(counter));
}

KTEST(VfsCoherency, ExhaustionAndObjectReuseNeverGrantFreshness) {
    coherence::Counter counter(UINT64_MAX - 2);
    coherence::State first, second;
    KREQUIRE_TRUE(first.initialize(counter));
    KREQUIRE_TRUE(second.initialize(counter));
    coherence::Stamp one{}, two{};
    KREQUIRE_TRUE(first.snapshot(one));
    KREQUIRE_TRUE(second.snapshot(two));
    KEXPECT_FALSE(coherence::cache_matches(one, two, coherence::Domain::ALL));
    coherence::Ticket last{};
    KREQUIRE_TRUE(first.prepare(coherence::Domain::ALL, last));
    KEXPECT_EQ(last.serial, UINT64_MAX);
    KREQUIRE_TRUE(first.publish(last));
    KEXPECT_FALSE(second.prepare(coherence::Domain::CONTENT, last));
    KEXPECT_EQ(last.serial, UINT64_MAX);
    KEXPECT_EQ(counter.take(), uint64_t{0});
    KEXPECT_FALSE(coherence::cache_matches({}, {}, coherence::Domain::ALL));
}

namespace {

struct TmpfsVersionFixture {
    static constexpr const char* DIRECTORY = "/tmp/ktest_owner_versions26";
    static constexpr const char* ORIGINAL = "/tmp/ktest_owner_versions26/original";
    static constexpr const char* ALIAS = "/tmp/ktest_owner_versions26/alias";
    static constexpr const char* MOVED = "/tmp/ktest_owner_versions26/moved";

    ~TmpfsVersionFixture() {
        static_cast<void>(ker::vfs::vfs_unlink(ORIGINAL));
        static_cast<void>(ker::vfs::vfs_unlink(ALIAS));
        static_cast<void>(ker::vfs::vfs_unlink(MOVED));
        static_cast<void>(ker::vfs::vfs_rmdir(DIRECTORY));
    }
};

struct FileCloser {
    void operator()(ker::vfs::File* file) const { static_cast<void>(ker::vfs::vfs_close_file(file)); }
};
using OwnedFile = std::unique_ptr<ker::vfs::File, FileCloser>;

struct XfsOwnerFixture {
    ker::vfs::xfs::XfsMountContext* mount = nullptr;
    ~XfsOwnerFixture() {
        if (mount == nullptr) return;
        auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
        for (const char* path : {"/ktest_owner_xfs26/a", "/ktest_owner_xfs26/b", "/ktest_owner_xfs26/c", "/ktest_owner_xfs26/moved"})
            static_cast<void>(ker::vfs::xfs::xfs_unlink_path(ROOT, path, mount));
        static_cast<void>(ker::vfs::xfs::xfs_rmdir_path(ROOT, "/ktest_owner_xfs26", mount));
        int const STATUS = ker::vfs::xfs::xfs_unmount(mount);
        KEXPECT_EQ(STATUS, 0);
        if (STATUS != 0) ker::vfs::xfs::xfs_unmount_force(mount);
    }
};

}  // namespace

KTEST(VfsCoherency, MountedXfsNativeOwnerObservationsVersionsScopesAndRetirement) {
    namespace vfs = ker::vfs;
    namespace xfs = ker::vfs::xfs;
    XfsOwnerFixture fixture;
    auto* device = ker::dev::block_device_find_by_name("sdb2");
    KREQUIRE_NE(device, nullptr);
    KREQUIRE_EQ(xfs::xfs_mount(device, false, &fixture.mount), 0);
    KREQUIRE_NE(fixture.mount, nullptr);
    auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
    KREQUIRE_EQ(xfs::xfs_mkdir_path(ROOT, "/ktest_owner_xfs26", 0700, fixture.mount), 0);
    OwnedFile directory(xfs::xfs_open_path(ROOT, "/ktest_owner_xfs26", vfs::O_DIRECTORY, 0, fixture.mount));
    KREQUIRE_NE(directory.get(), nullptr);
    coherence::Stamp directory_before{}, directory_after{};
    KREQUIRE_EQ(vfs::vfs_file_coherency_snapshot(directory.get(), directory_before), 0);
    OwnedFile owner(xfs::xfs_open_path(ROOT, "/ktest_owner_xfs26/a", vfs::O_CREAT | vfs::O_EXCL | 2, 0600, fixture.mount));
    OwnedFile hole(xfs::xfs_open_path(ROOT, "/ktest_owner_xfs26/b", vfs::O_CREAT | vfs::O_EXCL | 2, 0600, fixture.mount));
    KREQUIRE_NE(owner.get(), nullptr);
    KREQUIRE_NE(hole.get(), nullptr);
    KREQUIRE_EQ(xfs::xfs_link_path(ROOT, "/ktest_owner_xfs26/a", "/ktest_owner_xfs26/c", fixture.mount), 0);
    OwnedFile alias(xfs::xfs_open_path(ROOT, "/ktest_owner_xfs26/c", 2, 0, fixture.mount));
    OwnedFile probe(xfs::xfs_open_path(ROOT, "/ktest_owner_xfs26/a", 2, 0, fixture.mount));
    KREQUIRE_NE(alias.get(), nullptr);
    KREQUIRE_NE(probe.get(), nullptr);
    KREQUIRE_EQ(xfs::xfs_unlink_path(ROOT, "/ktest_owner_xfs26/b", fixture.mount), 0);
    hole.reset();
    KREQUIRE_EQ(vfs::vfs_file_coherency_snapshot(directory.get(), directory_after), 0);
    KEXPECT_EQ(directory_after.object, directory_before.object);
    KEXPECT_TRUE(directory_after.directory > directory_before.directory);
    coherence::Stamp before{}, after{}, linked{};
    KREQUIRE_EQ(vfs::vfs_file_coherency_snapshot(owner.get(), before), 0);
    KREQUIRE_EQ(vfs::vfs_file_coherency_snapshot(alias.get(), linked), 0);
    KEXPECT_EQ(linked.object, before.object);
    KREQUIRE_EQ(vfs::vfs_pwrite_file(owner.get(), "abc", 3, 0), ssize_t{3});
    KREQUIRE_EQ(vfs::vfs_file_coherency_snapshot(alias.get(), after), 0);
    KEXPECT_EQ(after.object, before.object);
    KEXPECT_TRUE(after.content > before.content);
    KEXPECT_TRUE(after.metadata > before.metadata);
    before = after;
    size_t offset = 99;
    KREQUIRE_EQ(vfs::vfs_append_file_native(alias.get(), "de", 2, &offset), ssize_t{2});
    KEXPECT_EQ(offset, size_t{3});
    KREQUIRE_EQ(vfs::vfs_ftruncate_file(owner.get(), 2), 0);
    KREQUIRE_EQ(vfs::vfs_pwrite_file(owner.get(), "cde", 3, 2), ssize_t{3});
    KREQUIRE_EQ(vfs::vfs_file_coherency_snapshot(owner.get(), after), 0);
    KEXPECT_TRUE(after.content > before.content);
    before = after;
    KREQUIRE_EQ(xfs::xfs_fchmod(owner.get(), ROOT, 0640), 0);
    KREQUIRE_EQ(xfs::xfs_fchown(owner.get(), ROOT, 17, 19), 0);
    constexpr char VALUE[] = "revision";
    KREQUIRE_EQ(xfs::xfs_setxattr_path(ROOT, "/ktest_owner_xfs26/a", "user.owner26", VALUE, sizeof(VALUE) - 1, 0, fixture.mount), 0);
    KREQUIRE_EQ(vfs::vfs_file_coherency_snapshot(alias.get(), after), 0);
    KEXPECT_EQ(after.object, before.object);
    KEXPECT_TRUE(after.metadata > before.metadata);
    // A generic mount view must not replace the retained inode mount's dev key.
    owner->mount_dev_id = 0xFEEEBEEF;
    alias->mount_dev_id = 0xFEEEBEEE;
    owner->pos = 91;
    KREQUIRE_TRUE(ker::net::wki::wki_remote_vfs_selftest_xfs_owner_files(owner.get(), alias.get(), directory.get(), probe.get()));
    KEXPECT_EQ(owner->pos, off_t{91});
    KREQUIRE_EQ(xfs::xfs_rename_path(ROOT, "/ktest_owner_xfs26/a", "/ktest_owner_xfs26/moved", fixture.mount), 0);
    KREQUIRE_EQ(xfs::xfs_unlink_path(ROOT, "/ktest_owner_xfs26/c", fixture.mount), 0);
    KREQUIRE_EQ(xfs::xfs_unlink_path(ROOT, "/ktest_owner_xfs26/moved", fixture.mount), 0);
    vfs::Stat stat{};
    KREQUIRE_EQ(vfs::vfs_fstat_file_native(owner.get(), &stat), 0);
    KEXPECT_EQ(stat.st_dev, uint64_t{fixture.mount->dev_id});
    KEXPECT_EQ(stat.st_size, off_t{5});
    KEXPECT_EQ(stat.st_nlink, uint64_t{0});
    KEXPECT_EQ(stat.st_uid, uint32_t{17});
    KEXPECT_EQ(stat.st_gid, uint32_t{19});
    KREQUIRE_EQ(vfs::vfs_file_coherency_snapshot(owner.get(), linked), 0);
    KEXPECT_EQ(linked.object, after.object);
    KEXPECT_TRUE(linked.metadata > after.metadata);
    KREQUIRE_EQ(vfs::vfs_file_coherency_snapshot(directory.get(), linked), 0);
    KEXPECT_EQ(linked.object, directory_after.object);
    KEXPECT_TRUE(linked.directory > directory_after.directory);
}

KTEST(VfsCoherency, NativeTmpfsVersionsFollowHardlinksAppendRenameAndUnlinkedHandles) {
    TmpfsVersionFixture cleanup;
    static_cast<void>(ker::vfs::vfs_mkdir("/tmp", 0755));
    KREQUIRE_EQ(ker::vfs::vfs_mkdir(TmpfsVersionFixture::DIRECTORY, 0755), 0);
    OwnedFile directory(ker::vfs::vfs_open_file(TmpfsVersionFixture::DIRECTORY, ker::vfs::O_DIRECTORY, 0));
    KREQUIRE_NE(directory.get(), nullptr);
    KREQUIRE_EQ(directory->fs_type, ker::vfs::FSType::TMPFS);
    auto* parent = static_cast<ker::vfs::tmpfs::TmpNode*>(directory->private_data);
    coherence::Stamp dir_before{}, dir_after{};
    KREQUIRE_TRUE(parent->coherency.snapshot(dir_before));
    OwnedFile first(ker::vfs::vfs_open_file(TmpfsVersionFixture::ORIGINAL, ker::vfs::O_CREAT | ker::vfs::O_EXCL | 2, 0644));
    KREQUIRE_NE(first.get(), nullptr);
    auto* node = ker::vfs::tmpfs::tmpfs_canonical_node(static_cast<ker::vfs::tmpfs::TmpNode*>(first->private_data));
    KREQUIRE_TRUE(parent->coherency.snapshot(dir_after));
    KEXPECT_TRUE(dir_after.directory > dir_before.directory);
    KEXPECT_EQ(dir_after.content, dir_before.content);
    coherence::Stamp before{}, after{};
    KREQUIRE_TRUE(node->coherency.snapshot(before));
    constexpr char DATA[] = "owner";
    KREQUIRE_EQ(ker::vfs::tmpfs::tmpfs_write(first.get(), DATA, sizeof(DATA) - 1, 0), static_cast<ssize_t>(sizeof(DATA) - 1));
    KREQUIRE_TRUE(node->coherency.snapshot(after));
    KEXPECT_EQ(after.object, before.object);
    KEXPECT_TRUE(after.content > before.content);
    KEXPECT_TRUE(after.metadata > before.metadata);
    KEXPECT_EQ(after.directory, before.directory);

    before = after;
    KREQUIRE_EQ(ker::vfs::vfs_link(TmpfsVersionFixture::ORIGINAL, TmpfsVersionFixture::ALIAS), 0);
    OwnedFile alias(ker::vfs::vfs_open_file(TmpfsVersionFixture::ALIAS, 2, 0));
    KREQUIRE_NE(alias.get(), nullptr);
    auto* alias_node = ker::vfs::tmpfs::tmpfs_canonical_node(static_cast<ker::vfs::tmpfs::TmpNode*>(alias->private_data));
    KEXPECT_EQ(alias_node, node);
    KREQUIRE_TRUE(node->coherency.snapshot(after));
    KEXPECT_EQ(after.object, before.object);
    KEXPECT_EQ(after.content, before.content);
    KEXPECT_TRUE(after.metadata > before.metadata);

    before = after;
    size_t offset = 999;
    KREQUIRE_EQ(ker::vfs::tmpfs::tmpfs_write_append(alias.get(), "!", 1, &offset), static_cast<ssize_t>(1));
    KEXPECT_EQ(offset, sizeof(DATA) - 1);
    KREQUIRE_TRUE(node->coherency.snapshot(after));
    KEXPECT_TRUE(after.content > before.content);
    KREQUIRE_EQ(ker::vfs::vfs_rename(TmpfsVersionFixture::ORIGINAL, TmpfsVersionFixture::MOVED), 0);
    KREQUIRE_EQ(ker::vfs::vfs_unlink(TmpfsVersionFixture::MOVED), 0);
    KREQUIRE_EQ(ker::vfs::vfs_unlink(TmpfsVersionFixture::ALIAS), 0);
    KREQUIRE_TRUE(node->coherency.snapshot(after));
    KEXPECT_EQ(after.object, before.object);
    KEXPECT_TRUE(after.metadata > before.metadata);
    before = after;
    KREQUIRE_EQ(ker::vfs::vfs_ftruncate_file(first.get(), 2), 0);
    KREQUIRE_TRUE(node->coherency.snapshot(after));
    KEXPECT_EQ(after.object, before.object);
    KEXPECT_TRUE(after.content > before.content);
    char readback[2]{};
    KREQUIRE_EQ(ker::vfs::tmpfs::tmpfs_read(alias.get(), readback, sizeof(readback), 0), static_cast<ssize_t>(2));
    KEXPECT_EQ(readback[0], 'o');
    KEXPECT_EQ(readback[1], 'w');
}
