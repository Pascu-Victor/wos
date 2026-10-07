#include <cstdint>
#include <memory>
#include <test/ktest.hpp>
#include <vfs/coherency.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/tmpfs.hpp>
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

}  // namespace

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
