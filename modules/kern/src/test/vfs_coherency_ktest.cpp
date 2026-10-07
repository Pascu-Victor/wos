#include <cstdint>
#include <cstring>
#include <dev/block_device.hpp>
#include <memory>
#include <net/wki/remote_vfs.hpp>
#include <platform/security/credential_record.hpp>
#include <test/ktest.hpp>
#include <utility>
#include <vfs/coherency.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>
#include <vfs/fs/xfs/xfs_vfs.hpp>
#include <vfs/vfs.hpp>

namespace coherence = ker::vfs::coherency;

KTEST(VfsCoherency, RemoteDirectoryErrorsPreserveRetryCursorAndInitializedPrefix) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_remote_readdir_errors());
}

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

struct TmpfsDirectoryFixture {
    static constexpr const char* BASE = "/tmp/ktest_removed_dirs26";
    static constexpr const char* REMOVED = "/tmp/ktest_removed_dirs26/removed";
    static constexpr const char* SOURCE = "/tmp/ktest_removed_dirs26/source";
    static constexpr const char* HOLDER = "/tmp/ktest_removed_dirs26/holder";
    static constexpr const char* REPLACED = "/tmp/ktest_removed_dirs26/holder/replaced";
    static constexpr const char* NONEMPTY = "/tmp/ktest_removed_dirs26/nonempty";
    static constexpr const char* CHILD = "/tmp/ktest_removed_dirs26/nonempty/child";
    ~TmpfsDirectoryFixture() {
        for (auto* path : {CHILD, NONEMPTY, REPLACED, SOURCE, REMOVED, HOLDER, BASE}) static_cast<void>(ker::vfs::vfs_rmdir(path));
    }
};

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

KTEST(VfsCoherency, RetainedTmpfsDirectoryVersionsCoverRemovalReplacementAndParentMovement) {
    namespace vfs = ker::vfs;
    namespace tmpfs = ker::vfs::tmpfs;
    TmpfsDirectoryFixture cleanup;
    auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
    auto denied = ROOT;
    KREQUIRE_EQ(denied.prepare({.change = ker::mod::security::Change::SET_UID, .real = 1000}, denied), 0);
    for (auto* path : {cleanup.BASE, cleanup.REMOVED, cleanup.SOURCE, cleanup.HOLDER, cleanup.REPLACED, cleanup.NONEMPTY, cleanup.CHILD})
        KREQUIRE_EQ(vfs::vfs_mkdir(path, 0700), 0);
    OwnedFile parent(vfs::vfs_open_file(cleanup.BASE, vfs::O_DIRECTORY, 0));
    OwnedFile removed(vfs::vfs_open_file(cleanup.REMOVED, vfs::O_DIRECTORY, 0));
    OwnedFile source(vfs::vfs_open_file(cleanup.SOURCE, vfs::O_DIRECTORY, 0));
    OwnedFile holder(vfs::vfs_open_file(cleanup.HOLDER, vfs::O_DIRECTORY, 0));
    OwnedFile replaced(vfs::vfs_open_file(cleanup.REPLACED, vfs::O_DIRECTORY, 0));
    KREQUIRE_NE(parent.get(), nullptr);
    KREQUIRE_NE(removed.get(), nullptr);
    KREQUIRE_NE(source.get(), nullptr);
    KREQUIRE_NE(holder.get(), nullptr);
    KREQUIRE_NE(replaced.get(), nullptr);
    auto* parent_node = static_cast<tmpfs::TmpNode*>(parent->private_data);
    auto* removed_node = static_cast<tmpfs::TmpNode*>(removed->private_data);
    auto* source_node = static_cast<tmpfs::TmpNode*>(source->private_data);
    auto* holder_node = static_cast<tmpfs::TmpNode*>(holder->private_data);
    auto* replaced_node = static_cast<tmpfs::TmpNode*>(replaced->private_data);
    vfs::DirEntry entry{};
    KREQUIRE_EQ(tmpfs::tmpfs_readdir_file_native(removed.get(), &entry, 1), 0);
    KEXPECT_EQ(entry.d_ino, reinterpret_cast<uint64_t>(parent_node));
    KEXPECT_EQ(entry.d_off, uint64_t{2});
    coherence::Stamp before{}, after{}, parent_before{}, parent_after{};
    KREQUIRE_TRUE(removed_node->coherency.snapshot(before));
    KREQUIRE_TRUE(parent_node->coherency.snapshot(parent_before));
    coherence::CacheValidation cache;
    KREQUIRE_TRUE(cache.publish(before, before, coherence::Domain::DIRECTORY));
    tmpfs::TmpfsLookupHandle removed_lookup, nonempty_lookup;
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ROOT, tmpfs::get_root_node(), cleanup.REMOVED, &removed_lookup), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ROOT, tmpfs::get_root_node(), cleanup.NONEMPTY, &nonempty_lookup), 0);
    KEXPECT_EQ(tmpfs::tmpfs_rmdir_lookup(ROOT, removed_lookup, true), -EROFS);
    KEXPECT_EQ(tmpfs::tmpfs_rmdir_lookup(denied, removed_lookup), -EACCES);
    KEXPECT_EQ(tmpfs::tmpfs_rmdir_lookup(ROOT, nonempty_lookup), -ENOTEMPTY);
    KREQUIRE_TRUE(removed_node->coherency.snapshot(after));
    KEXPECT_TRUE(coherence::cache_matches(before, after, coherence::Domain::ALL));
    KREQUIRE_TRUE(parent_node->coherency.snapshot(parent_after));
    KEXPECT_TRUE(coherence::cache_matches(parent_before, parent_after, coherence::Domain::ALL));
    KREQUIRE_EQ(vfs::vfs_rmdir(cleanup.REMOVED), 0);
    KREQUIRE_TRUE(removed_node->coherency.snapshot(after));
    KEXPECT_EQ(after.object, before.object);
    KEXPECT_EQ(after.content, before.content);
    KEXPECT_TRUE(after.metadata > before.metadata);
    KEXPECT_TRUE(after.directory > before.directory);
    KEXPECT_FALSE(cache.matches(after, coherence::Domain::DIRECTORY));
    KEXPECT_FALSE(cache.publish(before, after, coherence::Domain::DIRECTORY));
    KREQUIRE_EQ(tmpfs::tmpfs_readdir_file_native(removed.get(), &entry, 1), 0);
    KEXPECT_EQ(entry.d_ino, reinterpret_cast<uint64_t>(removed_node));
    KEXPECT_EQ(entry.d_off, uint64_t{2});
    KREQUIRE_TRUE(parent_node->coherency.snapshot(parent_after));
    KEXPECT_TRUE(parent_after.directory > parent_before.directory);
    before = after;
    KEXPECT_EQ(tmpfs::tmpfs_rmdir_lookup(ROOT, removed_lookup), -EAGAIN);
    KREQUIRE_TRUE(removed_node->coherency.snapshot(after));
    KEXPECT_TRUE(coherence::cache_matches(before, after, coherence::Domain::ALL));

    // Both the moved directory and the displaced retained directory change
    // their '..' entries, independently of the two parents' membership lists.
    coherence::Stamp source_before{}, source_after{}, replaced_before{}, replaced_after{}, holder_before{}, holder_after{};
    KREQUIRE_TRUE(source_node->coherency.snapshot(source_before));
    KREQUIRE_TRUE(replaced_node->coherency.snapshot(replaced_before));
    KREQUIRE_TRUE(holder_node->coherency.snapshot(holder_before));
    KREQUIRE_EQ(tmpfs::tmpfs_readdir_file_native(source.get(), &entry, 1), 0);
    KEXPECT_EQ(entry.d_ino, reinterpret_cast<uint64_t>(parent_node));
    KREQUIRE_EQ(tmpfs::tmpfs_readdir_file_native(replaced.get(), &entry, 1), 0);
    KEXPECT_EQ(entry.d_ino, reinterpret_cast<uint64_t>(holder_node));
    tmpfs::TmpfsLookupHandle from, to;
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ROOT, tmpfs::get_root_node(), cleanup.SOURCE, &from), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ROOT, tmpfs::get_root_node(), cleanup.REPLACED, &to), 0);
    KEXPECT_EQ(tmpfs::tmpfs_rename_lookup(ROOT, from, to, false, false, nullptr, nullptr, nullptr, true), -EROFS);
    KREQUIRE_TRUE(source_node->coherency.snapshot(source_after));
    KREQUIRE_TRUE(replaced_node->coherency.snapshot(replaced_after));
    KEXPECT_TRUE(coherence::cache_matches(source_before, source_after, coherence::Domain::ALL));
    KEXPECT_TRUE(coherence::cache_matches(replaced_before, replaced_after, coherence::Domain::ALL));
    KREQUIRE_EQ(vfs::vfs_rename(cleanup.SOURCE, cleanup.REPLACED), 0);
    KREQUIRE_TRUE(source_node->coherency.snapshot(source_after));
    KREQUIRE_TRUE(replaced_node->coherency.snapshot(replaced_after));
    for (auto const& stamps : {std::pair{source_before, source_after}, std::pair{replaced_before, replaced_after}}) {
        KEXPECT_EQ(stamps.second.object, stamps.first.object);
        KEXPECT_EQ(stamps.second.content, stamps.first.content);
        KEXPECT_TRUE(stamps.second.metadata > stamps.first.metadata);
        KEXPECT_TRUE(stamps.second.directory > stamps.first.directory);
        KEXPECT_FALSE(coherence::cache_matches(stamps.first, stamps.second, coherence::Domain::DIRECTORY));
    }
    KREQUIRE_EQ(tmpfs::tmpfs_readdir_file_native(source.get(), &entry, 1), 0);
    KEXPECT_EQ(entry.d_ino, reinterpret_cast<uint64_t>(holder_node));
    KREQUIRE_EQ(tmpfs::tmpfs_readdir_file_native(replaced.get(), &entry, 1), 0);
    KEXPECT_EQ(entry.d_ino, reinterpret_cast<uint64_t>(replaced_node));
    KREQUIRE_TRUE(holder_node->coherency.snapshot(holder_after));
    KEXPECT_TRUE(holder_after.directory > holder_before.directory);
    tmpfs::TmpfsLookupHandle root;
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ROOT, tmpfs::get_root_node(), "/", &root), 0);
    KREQUIRE_TRUE(root.target()->coherency.snapshot(before));
    // Root is nonempty in this boot; semantic validation precedes root denial.
    KEXPECT_EQ(tmpfs::tmpfs_rmdir_lookup(ROOT, root), -ENOTEMPTY);
    KREQUIRE_TRUE(root.target()->coherency.snapshot(after));
    KEXPECT_TRUE(coherence::cache_matches(before, after, coherence::Domain::ALL));
}

KTEST(VfsCoherency, TmpfsTimestampCommitAndDenialsPreserveExactVersionDomains) {
    namespace vfs = ker::vfs;
    namespace tmpfs = ker::vfs::tmpfs;
    TmpfsVersionFixture cleanup;
    auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
    auto denied = ROOT;
    KREQUIRE_EQ(denied.prepare({.change = ker::mod::security::Change::SET_UID, .real = 1000}, denied), 0);
    KREQUIRE_EQ(vfs::vfs_mkdir(cleanup.DIRECTORY, 0700), 0);
    OwnedFile file(vfs::vfs_open_file(cleanup.ORIGINAL, vfs::O_CREAT | vfs::O_EXCL | 2, 0600));
    KREQUIRE_NE(file.get(), nullptr);
    auto* node = static_cast<tmpfs::TmpNode*>(file->private_data);
    tmpfs::TmpfsLookupHandle lookup;
    KREQUIRE_EQ(tmpfs::tmpfs_acquire_lookup(ROOT, tmpfs::get_root_node(), cleanup.ORIGINAL, &lookup), 0);
    std::array<vfs::Timespec, 2> requested{{{101, 123}, {202, 456}}};
    vfs::TimestampUpdate explicit_times, omit;
    KREQUIRE_EQ(vfs::TimestampUpdate::prepare(requested.data(), {303, 789}, explicit_times), 0);
    requested = {{{0, vfs::TimestampUpdate::OMIT}, {0, vfs::TimestampUpdate::OMIT}}};
    KREQUIRE_EQ(vfs::TimestampUpdate::prepare(requested.data(), {404, 999}, omit), 0);
    coherence::Stamp before{}, after{};
    KREQUIRE_TRUE(node->coherency.snapshot(before));
    tmpfs::TmpfsObjectMetadata output{};
    output.uid = 12345;
    KEXPECT_EQ(tmpfs::tmpfs_utimens_lookup(lookup, denied, explicit_times, &output), -EPERM);
    KEXPECT_EQ(output.uid, uint32_t{12345});
    KEXPECT_EQ(tmpfs::tmpfs_utimens_lookup(lookup, ROOT, explicit_times, &output, true), -EROFS);
    KEXPECT_EQ(output.uid, uint32_t{12345});
    KEXPECT_EQ(tmpfs::tmpfs_utimens_lookup(lookup, denied, omit, &output, true), 0);
    KREQUIRE_TRUE(node->coherency.snapshot(after));
    KEXPECT_TRUE(coherence::cache_matches(before, after, coherence::Domain::ALL));
    KREQUIRE_EQ(tmpfs::tmpfs_utimens_lookup(lookup, ROOT, explicit_times, &output), 0);
    KREQUIRE_TRUE(node->coherency.snapshot(after));
    KEXPECT_EQ(after.object, before.object);
    KEXPECT_EQ(after.content, before.content);
    KEXPECT_EQ(after.directory, before.directory);
    KEXPECT_TRUE(after.metadata > before.metadata);
    vfs::Stat stat{};
    KREQUIRE_EQ(vfs::vfs_fstat_file_native(file.get(), &stat), 0);
    KEXPECT_EQ(stat.st_atim.tv_sec, int64_t{101});
    KEXPECT_EQ(stat.st_mtim.tv_nsec, int64_t{456});
    KEXPECT_EQ(stat.st_ctim.tv_sec, int64_t{303});
    KREQUIRE_EQ(vfs::vfs_unlink(cleanup.ORIGINAL), 0);
    KREQUIRE_TRUE(node->coherency.snapshot(before));
    KEXPECT_EQ(tmpfs::tmpfs_utimens_lookup(lookup, ROOT, explicit_times, &output), -EAGAIN);
    KREQUIRE_TRUE(node->coherency.snapshot(after));
    KEXPECT_TRUE(coherence::cache_matches(before, after, coherence::Domain::ALL));
    KREQUIRE_EQ(tmpfs::tmpfs_utimens_node(node, ROOT, explicit_times, &output), 0);
    KREQUIRE_TRUE(node->coherency.snapshot(after));
    KEXPECT_EQ(after.object, before.object);
    KEXPECT_TRUE(after.metadata > before.metadata);
}

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
    before = after;
    // Timestamp transactions participate in the same retained canonical
    // authority; policy/readonly/OMIT denials must not publish a new version.
    std::array<vfs::Timespec, 2> requested{{{101, 123}, {202, 456}}};
    vfs::TimestampUpdate explicit_times, omit;
    KREQUIRE_EQ(vfs::TimestampUpdate::prepare(requested.data(), {303, 789}, explicit_times), 0);
    requested = {{{0, vfs::TimestampUpdate::OMIT}, {0, vfs::TimestampUpdate::OMIT}}};
    KREQUIRE_EQ(vfs::TimestampUpdate::prepare(requested.data(), {404, 999}, omit), 0);
    auto denied = ROOT;
    KREQUIRE_EQ(denied.prepare({.change = ker::mod::security::Change::SET_UID, .real = 1000}, denied), 0);
    xfs::XfsLookupHandle lookup;
    KREQUIRE_EQ(xfs::xfs_acquire_lookup(ROOT, "/ktest_owner_xfs26/a", fixture.mount, &lookup), 0);
    vfs::Stat timestamp_output{};
    timestamp_output.st_uid = 12345;
    KEXPECT_EQ(xfs::xfs_set_times_file(owner.get(), denied, explicit_times, &timestamp_output), -EPERM);
    KEXPECT_EQ(timestamp_output.st_uid, uint32_t{12345});
    KEXPECT_EQ(xfs::xfs_utimens_lookup(lookup, ROOT, explicit_times, &timestamp_output, true), -EROFS);
    KEXPECT_EQ(xfs::xfs_utimens_lookup(lookup, ROOT, omit, &timestamp_output, true), -EROFS);
    KEXPECT_EQ(timestamp_output.st_uid, uint32_t{12345});
    KREQUIRE_EQ(xfs::xfs_set_times_file(owner.get(), denied, omit, &timestamp_output), 0);
    KREQUIRE_EQ(vfs::vfs_file_coherency_snapshot(alias.get(), after), 0);
    KEXPECT_TRUE(coherence::cache_matches(before, after, coherence::Domain::ALL));
    KREQUIRE_EQ(xfs::xfs_set_times_file(owner.get(), ROOT, explicit_times, &timestamp_output), 0);
    KREQUIRE_EQ(vfs::vfs_file_coherency_snapshot(alias.get(), after), 0);
    KEXPECT_EQ(after.object, before.object);
    KEXPECT_TRUE(after.metadata > before.metadata);
    KREQUIRE_EQ(vfs::vfs_fstat_file_native(alias.get(), &timestamp_output), 0);
    KEXPECT_EQ(timestamp_output.st_atim.tv_sec, int64_t{101});
    KEXPECT_EQ(timestamp_output.st_atim.tv_nsec, int64_t{123});
    KEXPECT_EQ(timestamp_output.st_mtim.tv_sec, int64_t{202});
    KEXPECT_EQ(timestamp_output.st_mtim.tv_nsec, int64_t{456});
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
