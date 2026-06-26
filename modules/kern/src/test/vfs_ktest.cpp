#include <bits/ssize_t.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <test/ktest.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/mount.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

// vfs_open() requires get_current_task() != nullptr (it allocates a fd in the
// task's fd table), unavailable at selftest time.  Use vfs_open_file() which
// returns a File* without touching the task fd table, and call tmpfs helpers
// directly for read/write.  vfs_mkdir / vfs_stat / vfs_unlink work without a
// current task.
//
// All test paths are under /tmp/ktest_* to avoid colliding with real content.

KTEST(VFS, OpenFdInstallFailureClosesFile) { KEXPECT_TRUE(ker::vfs::vfs_selftest_fd_install_failure_closes_file()); }

KTEST(VFS, Dup2ReplacePreservesNewfdOnFailure) { KEXPECT_TRUE(ker::vfs::vfs_selftest_dup2_replace_preserves_newfd_on_failure()); }

KTEST(VFS, PipeFailureUnwindsAllStages) { KEXPECT_TRUE(ker::vfs::vfs_selftest_pipe_failure_unwinds()); }

KTEST(VFS, PipeFlags) { KEXPECT_TRUE(ker::vfs::vfs_selftest_pipe_flags()); }

KTEST(VFS, AnonymousFstatSnapshotHits) { KEXPECT_TRUE(ker::vfs::vfs_selftest_anonymous_fstat_snapshot_hits()); }

KTEST(VFS, RemoteFstatSnapshotCacheable) { KEXPECT_TRUE(ker::vfs::vfs_selftest_remote_fstat_snapshot_cacheable()); }

KTEST(VFS, FilePathStorage) { KEXPECT_TRUE(ker::vfs::vfs_selftest_file_path_storage()); }

KTEST(VFS, FileDataWriteInvalidatesPathStat) { KEXPECT_TRUE(ker::vfs::vfs_selftest_file_data_write_invalidates_path_stat()); }

KTEST(VFS, FileDataWriteSkipsUncachedPathInvalidation) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_file_data_write_skips_uncached_path_invalidation());
}

KTEST(VFS, FileMetadataChangeInvalidatesPathStat) { KEXPECT_TRUE(ker::vfs::vfs_selftest_file_metadata_change_invalidates_path_stat()); }

KTEST(VFS, OpenCreateMetadataHint) { KEXPECT_TRUE(ker::vfs::vfs_selftest_open_create_metadata_hint()); }

KTEST(VFS, FcntlSetflPreservesOpenPolicyFlags) { KEXPECT_TRUE(ker::vfs::vfs_selftest_fcntl_setfl_preserves_open_policy_flags()); }

KTEST(VFS, StreamCacheReadEligibility) { KEXPECT_TRUE(ker::vfs::vfs_selftest_stream_cache_read_eligibility()); }

KTEST(VFS, StreamCacheLocalDetachedTtl) { KEXPECT_TRUE(ker::vfs::vfs_selftest_stream_cache_local_detached_ttl()); }

KTEST(VFS, FdAllocationCapsCloexecRange) { KEXPECT_TRUE(ker::vfs::vfs_selftest_fd_allocation_caps_cloexec_range()); }

KTEST(VFS, CreateAndStat) {
    ker::vfs::vfs_mkdir("/tmp", 0755);  // idempotent if /tmp already exists

    ker::vfs::File* f = ker::vfs::vfs_open_file("/tmp/ktest_create", ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(f, nullptr);
    ker::vfs::tmpfs::tmpfs_fops_close(f);

    ker::vfs::Stat st{};
    KEXPECT_EQ(ker::vfs::vfs_stat("/tmp/ktest_create", &st), 0);
    KEXPECT_TRUE((st.st_mode & ker::vfs::S_IFREG) != 0U);

    ker::vfs::vfs_unlink("/tmp/ktest_create");
}

KTEST(VFS, MetadataCacheInvalidatesPathMutation) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    constexpr const char* PATH = "/tmp/ktest_meta_cache";
    constexpr const char* RENAMED = "/tmp/ktest_meta_cache_renamed";
    ker::vfs::vfs_unlink(PATH);
    ker::vfs::vfs_unlink(RENAMED);

    ker::vfs::Stat st{};
    KEXPECT_NE(ker::vfs::vfs_stat(PATH, &st), 0);
    KEXPECT_NE(ker::vfs::vfs_stat(PATH, &st), 0);

    ker::vfs::File* f = ker::vfs::vfs_open_file(PATH, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(f, nullptr);
    ker::vfs::tmpfs::tmpfs_fops_close(f);

    KEXPECT_EQ(ker::vfs::vfs_stat(PATH, &st), 0);
    KEXPECT_TRUE((st.st_mode & ker::vfs::S_IFREG) != 0U);
    KEXPECT_EQ(ker::vfs::vfs_stat(PATH, &st), 0);

    KEXPECT_EQ(ker::vfs::vfs_rename(PATH, RENAMED), 0);
    KEXPECT_NE(ker::vfs::vfs_stat(PATH, &st), 0);
    KEXPECT_EQ(ker::vfs::vfs_stat(RENAMED, &st), 0);

    KEXPECT_EQ(ker::vfs::vfs_unlink(RENAMED), 0);
    KEXPECT_NE(ker::vfs::vfs_stat(RENAMED, &st), 0);
}

KTEST(VFS, MetadataCacheRepeatedStatRecordsHit) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    constexpr const char* PATH = "/tmp/ktest_meta_counter";
    ker::vfs::vfs_unlink(PATH);

    ker::vfs::File* f = ker::vfs::vfs_open_file(PATH, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(f, nullptr);
    ker::vfs::vfs_put_file(f);

    ker::vfs::VfsCachePerfSnapshot before{};
    ker::vfs::vfs_get_cache_perf_snapshot(before);

    ker::vfs::Stat st{};
    KEXPECT_EQ(ker::vfs::vfs_stat(PATH, &st), 0);

    ker::vfs::VfsCachePerfSnapshot after_first{};
    ker::vfs::vfs_get_cache_perf_snapshot(after_first);

    KEXPECT_EQ(ker::vfs::vfs_stat(PATH, &st), 0);

    ker::vfs::VfsCachePerfSnapshot after{};
    ker::vfs::vfs_get_cache_perf_snapshot(after);
    KEXPECT_TRUE(after.metadata_stores > before.metadata_stores);
    KEXPECT_TRUE(after.metadata_hits > before.metadata_hits);
    KEXPECT_EQ(after.symlink_hits, after_first.symlink_hits);

    KEXPECT_EQ(ker::vfs::vfs_unlink(PATH), 0);
}

KTEST(VFS, MetadataCachePathMutationKeepsSiblingStatHot) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    constexpr const char* HOT_PATH = "/tmp/ktest_meta_hot_sibling";
    constexpr const char* CHURN_PATH = "/tmp/ktest_meta_churn_sibling";
    ker::vfs::vfs_unlink(HOT_PATH);
    ker::vfs::vfs_unlink(CHURN_PATH);

    ker::vfs::File* hot = ker::vfs::vfs_open_file(HOT_PATH, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(hot, nullptr);
    ker::vfs::vfs_put_file(hot);

    ker::vfs::Stat st{};
    KEXPECT_EQ(ker::vfs::vfs_stat(HOT_PATH, &st), 0);
    KEXPECT_EQ(ker::vfs::vfs_stat(HOT_PATH, &st), 0);

    ker::vfs::VfsCachePerfSnapshot before{};
    ker::vfs::vfs_get_cache_perf_snapshot(before);

    ker::vfs::File* churn = ker::vfs::vfs_open_file(CHURN_PATH, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(churn, nullptr);
    ker::vfs::vfs_put_file(churn);

    KEXPECT_EQ(ker::vfs::vfs_stat(HOT_PATH, &st), 0);

    ker::vfs::VfsCachePerfSnapshot after{};
    ker::vfs::vfs_get_cache_perf_snapshot(after);
    KEXPECT_TRUE(after.metadata_hits >= before.metadata_hits + 1);

    KEXPECT_EQ(ker::vfs::vfs_unlink(CHURN_PATH), 0);
    KEXPECT_EQ(ker::vfs::vfs_unlink(HOT_PATH), 0);
}

KTEST(VFS, OpenFileFstatSnapshotHitsAndInvalidates) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    constexpr const char* PATH = "/tmp/ktest_fstat_snapshot";
    constexpr char DATA[] = "snapshot";
    ker::vfs::vfs_unlink(PATH);

    ker::vfs::File* wf = ker::vfs::vfs_open_file(PATH, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(wf, nullptr);
    KREQUIRE_NE(wf->fops, nullptr);
    KREQUIRE_NE(wf->fops->vfs_write, nullptr);
    KEXPECT_EQ(wf->fops->vfs_write(wf, DATA, sizeof(DATA) - 1, 0), static_cast<ssize_t>(sizeof(DATA) - 1));
    ker::vfs::vfs_put_file(wf);

    ker::vfs::VfsCachePerfSnapshot before{};
    ker::vfs::vfs_get_cache_perf_snapshot(before);

    ker::vfs::File* rf = ker::vfs::vfs_open_file(PATH, 0, 0);
    KREQUIRE_NE(rf, nullptr);

    ker::vfs::Stat st{};
    KEXPECT_EQ(ker::vfs::vfs_fstat_file(rf, &st), 0);
    KEXPECT_EQ(ker::vfs::vfs_fstat_file(rf, &st), 0);
    KEXPECT_EQ(st.st_size, static_cast<off_t>(sizeof(DATA) - 1));

    ker::vfs::VfsCachePerfSnapshot after_hits{};
    ker::vfs::vfs_get_cache_perf_snapshot(after_hits);
    KEXPECT_TRUE(after_hits.fstat_snapshot_stores > before.fstat_snapshot_stores);
    KEXPECT_TRUE(after_hits.fstat_snapshot_hits >= before.fstat_snapshot_hits + 2);

    ker::vfs::vfs_cache_notify_file_changed(rf);
    KEXPECT_EQ(ker::vfs::vfs_fstat_file(rf, &st), 0);

    ker::vfs::VfsCachePerfSnapshot after_invalidate{};
    ker::vfs::vfs_get_cache_perf_snapshot(after_invalidate);
    KEXPECT_TRUE(after_invalidate.fstat_snapshot_misses > after_hits.fstat_snapshot_misses);

    ker::vfs::vfs_put_file(rf);
    KEXPECT_EQ(ker::vfs::vfs_unlink(PATH), 0);
}

KTEST(VFS, DevfsFstatSnapshotHits) {
    constexpr const char PATH[] = "/dev/ktest-devfs";
    ker::vfs::File file{};
    file.fs_type = ker::vfs::FSType::DEVFS;
    file.vfs_path = PATH;

    ker::vfs::VfsCachePerfSnapshot before{};
    ker::vfs::VfsCachePerfSnapshot after_first{};
    ker::vfs::VfsCachePerfSnapshot after_second{};
    ker::vfs::Stat st{};
    ker::vfs::vfs_get_cache_perf_snapshot(before);

    KEXPECT_EQ(ker::vfs::vfs_fstat_file(&file, &st), 0);
    KEXPECT_EQ(st.st_mode & static_cast<mode_t>(ker::vfs::S_IFMT), static_cast<mode_t>(ker::vfs::S_IFCHR));
    ker::vfs::vfs_get_cache_perf_snapshot(after_first);

    KEXPECT_EQ(ker::vfs::vfs_fstat_file(&file, &st), 0);
    ker::vfs::vfs_get_cache_perf_snapshot(after_second);

    KEXPECT_TRUE(after_first.fstat_snapshot_stores > before.fstat_snapshot_stores);
    KEXPECT_EQ(after_second.fstat_snapshot_miss_uncacheable, before.fstat_snapshot_miss_uncacheable);
    KEXPECT_TRUE(after_second.fstat_snapshot_hits > after_first.fstat_snapshot_hits);
}

KTEST(VFS, PreadBypassesStreamCacheAndPreservesOffset) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    constexpr const char* PATH = "/tmp/ktest_stream_counter";
    constexpr char DATA[] = "abcdefghijklmnopqrstuvwxyz";
    ker::vfs::vfs_unlink(PATH);

    ker::vfs::File* wf = ker::vfs::vfs_open_file(PATH, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(wf, nullptr);
    KREQUIRE_NE(wf->fops, nullptr);
    KREQUIRE_NE(wf->fops->vfs_write, nullptr);
    KEXPECT_EQ(wf->fops->vfs_write(wf, DATA, sizeof(DATA) - 1, 0), static_cast<ssize_t>(sizeof(DATA) - 1));
    ker::vfs::vfs_put_file(wf);

    ker::vfs::File* rf = ker::vfs::vfs_open_file(PATH, 0, 0);
    KREQUIRE_NE(rf, nullptr);

    ker::vfs::VfsCachePerfSnapshot before{};
    ker::vfs::vfs_get_cache_perf_snapshot(before);

    char buf[8] = {};
    KEXPECT_EQ(ker::vfs::vfs_pread_file(rf, buf, sizeof(buf), 0), static_cast<ssize_t>(sizeof(buf)));
    KEXPECT_EQ(ker::vfs::vfs_pread_file(rf, buf, sizeof(buf), 0), static_cast<ssize_t>(sizeof(buf)));
    KEXPECT_TRUE(std::memcmp(buf, DATA, sizeof(buf)) == 0);
    KEXPECT_EQ(rf->pos, 0);

    ker::vfs::VfsCachePerfSnapshot after{};
    ker::vfs::vfs_get_cache_perf_snapshot(after);
    KEXPECT_EQ(after.stream_backend_reads, before.stream_backend_reads);
    KEXPECT_EQ(after.stream_hits, before.stream_hits);
    KEXPECT_EQ(after.stream_copied_bytes, before.stream_copied_bytes);

    ker::vfs::vfs_put_file(rf);
    KEXPECT_EQ(ker::vfs::vfs_unlink(PATH), 0);
}

KTEST(VFS, SymlinkCacheRepeatedReadlinkRecordsHit) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    constexpr const char* TARGET = "/tmp/ktest_symlink_counter_target";
    constexpr const char* LINK = "/tmp/ktest_symlink_counter_link";
    ker::vfs::vfs_unlink(LINK);
    ker::vfs::vfs_unlink(TARGET);

    ker::vfs::File* f = ker::vfs::vfs_open_file(TARGET, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(f, nullptr);
    ker::vfs::vfs_put_file(f);
    KEXPECT_EQ(ker::vfs::vfs_symlink(TARGET, LINK), 0);

    ker::vfs::VfsCachePerfSnapshot before{};
    ker::vfs::vfs_get_cache_perf_snapshot(before);

    char buf[128] = {};
    KEXPECT_EQ(ker::vfs::vfs_readlink_resolved(LINK, buf, sizeof(buf)), static_cast<ssize_t>(std::strlen(TARGET)));
    KEXPECT_TRUE(std::strcmp(buf, TARGET) == 0);
    std::memset(buf, 0, sizeof(buf));
    KEXPECT_EQ(ker::vfs::vfs_readlink_resolved(LINK, buf, sizeof(buf)), static_cast<ssize_t>(std::strlen(TARGET)));
    KEXPECT_TRUE(std::strcmp(buf, TARGET) == 0);

    ker::vfs::VfsCachePerfSnapshot after{};
    ker::vfs::vfs_get_cache_perf_snapshot(after);
    KEXPECT_TRUE(after.symlink_stores > before.symlink_stores);
    KEXPECT_TRUE(after.symlink_hits > before.symlink_hits);

    KEXPECT_EQ(ker::vfs::vfs_unlink(LINK), 0);
    KEXPECT_EQ(ker::vfs::vfs_unlink(TARGET), 0);
}

KTEST(VFS, WriteRead) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    ker::vfs::File* wf = ker::vfs::vfs_open_file("/tmp/ktest_wr", ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(wf, nullptr);

    uint8_t wbuf[128];
    for (int i = 0; i < 128; ++i) {
        wbuf[i] = static_cast<uint8_t>(i);
    }
    ssize_t const NW = ker::vfs::tmpfs::tmpfs_write(wf, static_cast<const void*>(wbuf), 128, 0);
    KEXPECT_EQ(NW, static_cast<ssize_t>(128));
    ker::vfs::tmpfs::tmpfs_fops_close(wf);

    ker::vfs::File* rf = ker::vfs::vfs_open_file("/tmp/ktest_wr", 0, 0);
    KREQUIRE_NE(rf, nullptr);

    uint8_t rbuf[128] = {};
    ssize_t const NR = ker::vfs::tmpfs::tmpfs_read(rf, static_cast<void*>(rbuf), 128, 0);
    KEXPECT_EQ(NR, static_cast<ssize_t>(128));
    ker::vfs::tmpfs::tmpfs_fops_close(rf);

    bool match = true;
    for (int i = 0; i < 128; ++i) {
        if (rbuf[i] != static_cast<uint8_t>(i)) {
            match = false;
            break;
        }
    }
    KEXPECT_TRUE(match);

    ker::vfs::vfs_unlink("/tmp/ktest_wr");
}

KTEST(VFS, TmpfsRejectedWritePreservesExistingData) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    constexpr const char* PATH = "/tmp/ktest_rejected_write";
    constexpr char DATA[] = "stable";
    constexpr size_t DATA_LEN = sizeof(DATA) - 1;

    ker::vfs::File* f = ker::vfs::vfs_open_file(PATH, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(f, nullptr);

    ssize_t const FIRST_WRITE = ker::vfs::tmpfs::tmpfs_write(f, static_cast<const void*>(DATA), DATA_LEN, 0);
    KEXPECT_EQ(FIRST_WRITE, static_cast<ssize_t>(DATA_LEN));

    char other = 'x';
    ssize_t const REJECTED_WRITE = ker::vfs::tmpfs::tmpfs_write(f, static_cast<const void*>(&other), 1, static_cast<size_t>(-1));
    KEXPECT_EQ(REJECTED_WRITE, static_cast<ssize_t>(-EFBIG));

    char rbuf[sizeof(DATA)] = {};
    ssize_t const READ = ker::vfs::tmpfs::tmpfs_read(f, static_cast<void*>(rbuf), DATA_LEN, 0);
    KEXPECT_EQ(READ, static_cast<ssize_t>(DATA_LEN));
    KEXPECT_TRUE(memcmp(static_cast<const void*>(rbuf), static_cast<const void*>(DATA), DATA_LEN) == 0);

    ker::vfs::tmpfs::tmpfs_fops_close(f);
    ker::vfs::vfs_unlink(PATH);
}

KTEST(VFS, TmpfsOpenTruncatesExistingFile) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    constexpr const char* PATH = "/tmp/ktest_trunc";
    constexpr char OLD[] = "old content\n";
    constexpr char NEW[] = "new\n";

    ker::vfs::File* f = ker::vfs::vfs_open_file(PATH, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(f, nullptr);
    ssize_t const OLD_WRITE = ker::vfs::tmpfs::tmpfs_write(f, static_cast<const void*>(OLD), sizeof(OLD) - 1, 0);
    KEXPECT_EQ(OLD_WRITE, static_cast<ssize_t>(sizeof(OLD) - 1));
    ker::vfs::tmpfs::tmpfs_fops_close(f);

    f = ker::vfs::vfs_open_file(PATH, ker::vfs::O_CREAT | ker::vfs::O_TRUNC | 1, 0644);
    KREQUIRE_NE(f, nullptr);
    ssize_t const NEW_WRITE = ker::vfs::tmpfs::tmpfs_write(f, static_cast<const void*>(NEW), sizeof(NEW) - 1, 0);
    KEXPECT_EQ(NEW_WRITE, static_cast<ssize_t>(sizeof(NEW) - 1));
    ker::vfs::tmpfs::tmpfs_fops_close(f);

    f = ker::vfs::vfs_open_file(PATH, 0, 0);
    KREQUIRE_NE(f, nullptr);
    char rbuf[sizeof(OLD)] = {};
    ssize_t const READ = ker::vfs::tmpfs::tmpfs_read(f, static_cast<void*>(rbuf), sizeof(rbuf), 0);
    KEXPECT_EQ(READ, static_cast<ssize_t>(sizeof(NEW) - 1));
    KEXPECT_TRUE(memcmp(static_cast<const void*>(rbuf), static_cast<const void*>(NEW), sizeof(NEW) - 1) == 0);
    ker::vfs::tmpfs::tmpfs_fops_close(f);

    f = ker::vfs::vfs_open_file(PATH, ker::vfs::O_TRUNC | 1, 0644);
    KREQUIRE_NE(f, nullptr);
    ker::vfs::tmpfs::tmpfs_fops_close(f);

    f = ker::vfs::vfs_open_file(PATH, 0, 0);
    KREQUIRE_NE(f, nullptr);
    KEXPECT_EQ(ker::vfs::tmpfs::tmpfs_read(f, static_cast<void*>(rbuf), sizeof(rbuf), 0), static_cast<ssize_t>(0));
    ker::vfs::tmpfs::tmpfs_fops_close(f);

    ker::vfs::vfs_unlink(PATH);
}

KTEST(VFS, Unlink) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    ker::vfs::File* f = ker::vfs::vfs_open_file("/tmp/ktest_unlink", ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(f, nullptr);
    ker::vfs::tmpfs::tmpfs_fops_close(f);

    KEXPECT_EQ(ker::vfs::vfs_unlink("/tmp/ktest_unlink"), 0);

    // After unlink the file is gone from the directory
    ker::vfs::File* f2 = ker::vfs::vfs_open_file("/tmp/ktest_unlink", 0, 0);
    KEXPECT_EQ(f2, nullptr);
}

KTEST(VFS, TmpfsReaddirKeepsStableOffsetsAcrossDeletes) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    constexpr const char* DIR = "/tmp/ktest_readdir_delete";
    constexpr const char* FILES[] = {
        "/tmp/ktest_readdir_delete/f0", "/tmp/ktest_readdir_delete/f1", "/tmp/ktest_readdir_delete/f2", "/tmp/ktest_readdir_delete/f3",
        "/tmp/ktest_readdir_delete/f4", "/tmp/ktest_readdir_delete/f5", "/tmp/ktest_readdir_delete/f6", "/tmp/ktest_readdir_delete/f7",
    };

    for (const char* path : FILES) {
        ker::vfs::vfs_unlink(path);
    }
    ker::vfs::vfs_rmdir(DIR);
    KEXPECT_EQ(ker::vfs::vfs_mkdir(DIR, 0755), 0);

    for (const char* path : FILES) {
        ker::vfs::File* f = ker::vfs::vfs_open_file(path, ker::vfs::O_CREAT | 1, 0644);
        KREQUIRE_NE(f, nullptr);
        ker::vfs::vfs_put_file(f);
    }

    ker::vfs::File* dir = ker::vfs::vfs_open_file(DIR, 0, 0);
    KREQUIRE_NE(dir, nullptr);
    KREQUIRE_NE(dir->fops, nullptr);
    KREQUIRE_NE(dir->fops->vfs_readdir, nullptr);

    for (size_t i = 0; i < 5; ++i) {
        KEXPECT_EQ(ker::vfs::vfs_unlink(FILES[i]), 0);
    }

    ker::vfs::DirEntry entry{};
    KEXPECT_EQ(dir->fops->vfs_readdir(dir, &entry, 7), 0);
    KEXPECT_TRUE(std::strcmp(entry.d_name.data(), "f5") == 0);
    ker::vfs::vfs_put_file(dir);

    for (size_t i = 5; i < 8; ++i) {
        ker::vfs::vfs_unlink(FILES[i]);
    }
    KEXPECT_EQ(ker::vfs::vfs_rmdir(DIR), 0);
}

KTEST(VFS, LstatDoesNotFollowFinalTmpfsSymlink) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    constexpr const char* TARGET = "/tmp/ktest_lstat_target";
    constexpr const char* LINK = "/tmp/ktest_lstat_link";

    ker::vfs::vfs_unlink(LINK);
    ker::vfs::vfs_rmdir(TARGET);
    KEXPECT_EQ(ker::vfs::vfs_mkdir(TARGET, 0755), 0);
    KEXPECT_EQ(ker::vfs::vfs_symlink(TARGET, LINK), 0);

    ker::vfs::Stat st{};
    KEXPECT_EQ(ker::vfs::vfs_lstat(LINK, &st), 0);
    KEXPECT_TRUE((st.st_mode & ker::vfs::S_IFMT) == ker::vfs::S_IFLNK);
    KEXPECT_EQ(ker::vfs::vfs_stat(LINK, &st), 0);
    KEXPECT_TRUE((st.st_mode & ker::vfs::S_IFMT) == ker::vfs::S_IFDIR);

    KEXPECT_EQ(ker::vfs::vfs_rmdir(TARGET), 0);
    KEXPECT_EQ(ker::vfs::vfs_lstat(LINK, &st), 0);
    KEXPECT_TRUE((st.st_mode & ker::vfs::S_IFMT) == ker::vfs::S_IFLNK);
    KEXPECT_NE(ker::vfs::vfs_stat(LINK, &st), 0);

    KEXPECT_EQ(ker::vfs::vfs_unlink(LINK), 0);
}

KTEST(VFS, LstatTrailingSlashFollowsTmpfsSymlinkToDirectory) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    constexpr const char* TARGET = "/tmp/ktest_lstat_slash_target";
    constexpr const char* LINK = "/tmp/ktest_lstat_slash_link";
    constexpr const char* LINK_WITH_SLASH = "/tmp/ktest_lstat_slash_link/";

    ker::vfs::vfs_unlink(LINK);
    ker::vfs::vfs_rmdir(TARGET);
    KEXPECT_EQ(ker::vfs::vfs_mkdir(TARGET, 0755), 0);
    KEXPECT_EQ(ker::vfs::vfs_symlink(TARGET, LINK), 0);

    ker::vfs::Stat st{};
    KEXPECT_EQ(ker::vfs::vfs_lstat(LINK_WITH_SLASH, &st), 0);
    KEXPECT_TRUE((st.st_mode & ker::vfs::S_IFMT) == ker::vfs::S_IFDIR);

    std::array<char, 128> linkbuf{};
    KEXPECT_EQ(ker::vfs::vfs_readlink(LINK_WITH_SLASH, linkbuf.data(), linkbuf.size()), static_cast<ssize_t>(-EINVAL));

    KEXPECT_EQ(ker::vfs::vfs_unlink(LINK), 0);
    KEXPECT_EQ(ker::vfs::vfs_rmdir(TARGET), 0);
}

KTEST(VFS, RealpathFastPathFollowsSimpleSymlink) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    constexpr const char* TARGET = "/tmp/ktest_realpath_target";
    constexpr const char* LINK = "/tmp/ktest_realpath_link";
    constexpr const char* MISSING = "/tmp/ktest_realpath_missing";
    constexpr const char* FILE_PATH = "/tmp/ktest_realpath_file";

    ker::vfs::vfs_unlink(LINK);
    ker::vfs::vfs_unlink(FILE_PATH);
    ker::vfs::vfs_rmdir(TARGET);
    KEXPECT_EQ(ker::vfs::vfs_mkdir(TARGET, 0755), 0);
    KEXPECT_EQ(ker::vfs::vfs_symlink(TARGET, LINK), 0);
    ker::vfs::File* file = ker::vfs::vfs_open_file(FILE_PATH, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(file, nullptr);
    ker::vfs::tmpfs::tmpfs_fops_close(file);

    char buf[512]{};
    KEXPECT_EQ(ker::vfs::vfs_realpath(LINK, buf, sizeof(buf)), 0);
    KEXPECT_TRUE(std::strcmp(buf, TARGET) == 0);

    KEXPECT_NE(ker::vfs::vfs_realpath(MISSING, buf, sizeof(buf)), 0);
    KEXPECT_EQ(ker::vfs::vfs_realpath("/tmp/ktest_realpath_link/..", buf, sizeof(buf)), 0);
    KEXPECT_TRUE(std::strcmp(buf, "/tmp") == 0);
    KEXPECT_EQ(ker::vfs::vfs_realpath("/tmp/ktest_realpath_target/../ktest_realpath_target", buf, sizeof(buf)), 0);
    KEXPECT_TRUE(std::strcmp(buf, TARGET) == 0);
    KEXPECT_EQ(ker::vfs::vfs_realpath("/tmp/ktest_realpath_file/..", buf, sizeof(buf)), -ENOTDIR);

    KEXPECT_EQ(ker::vfs::vfs_unlink(FILE_PATH), 0);
    KEXPECT_EQ(ker::vfs::vfs_unlink(LINK), 0);
    KEXPECT_EQ(ker::vfs::vfs_rmdir(TARGET), 0);
}

KTEST(VFS, Mkdir) {
    KEXPECT_EQ(ker::vfs::vfs_mkdir("/tmp/ktest_dir", 0755), 0);

    ker::vfs::Stat st{};
    KEXPECT_EQ(ker::vfs::vfs_stat("/tmp/ktest_dir", &st), 0);
    KEXPECT_TRUE((st.st_mode & ker::vfs::S_IFDIR) != 0U);
}

KTEST(VFS, TmpfsMountHasSeparateRoot) {
    constexpr const char* MOUNTPOINT = "/tmp/ktest_tmpfs_mount";
    constexpr const char* ROOT_ONLY_FILE = "/tmp/ktest_tmpfs_root_only";
    constexpr const char* MOUNTED_TMP_DIR = "/tmp/ktest_tmpfs_mount/tmp";
    constexpr const char* MOUNT_FILE = "/tmp/ktest_tmpfs_mount/ktest_tmpfs_mount_file";

    ker::vfs::vfs_mkdir("/tmp", 0755);

    ker::vfs::File* root_file = ker::vfs::vfs_open_file(ROOT_ONLY_FILE, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(root_file, nullptr);
    ker::vfs::tmpfs::tmpfs_fops_close(root_file);

    KEXPECT_EQ(ker::vfs::vfs_mkdir(MOUNTPOINT, 0755), 0);
    KEXPECT_EQ(ker::vfs::mount_filesystem(MOUNTPOINT, "tmpfs", nullptr), 0);

    ker::vfs::File* inherited_tmp = ker::vfs::vfs_open_file(MOUNTED_TMP_DIR, 0, 0);
    KEXPECT_EQ(inherited_tmp, nullptr);
    if (inherited_tmp != nullptr) {
        ker::vfs::tmpfs::tmpfs_fops_close(inherited_tmp);
    }

    ker::vfs::File* mount_file = ker::vfs::vfs_open_file(MOUNT_FILE, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(mount_file, nullptr);
    ker::vfs::tmpfs::tmpfs_fops_close(mount_file);

    ker::vfs::Stat st{};
    KEXPECT_EQ(ker::vfs::vfs_stat(MOUNT_FILE, &st), 0);

    KEXPECT_EQ(ker::vfs::unmount_filesystem(MOUNTPOINT), 0);
    KEXPECT_NE(ker::vfs::vfs_stat(MOUNT_FILE, &st), 0);

    ker::vfs::vfs_unlink(ROOT_ONLY_FILE);
    ker::vfs::vfs_rmdir(MOUNTPOINT);
}

KTEST(VFS, MountLookupRefsFencePathMutation) {
    constexpr const char* MOUNTPOINT = "/tmp/ktest_mount_ref";
    constexpr const char* PUT_OLD = "/tmp/ktest_mount_ref_oldroot";

    ker::vfs::vfs_mkdir("/tmp", 0755);
    ker::vfs::unmount_filesystem(MOUNTPOINT);
    ker::vfs::vfs_rmdir(MOUNTPOINT);
    KEXPECT_EQ(ker::vfs::vfs_mkdir(MOUNTPOINT, 0755), 0);
    KREQUIRE_EQ(ker::vfs::mount_filesystem(MOUNTPOINT, "tmpfs", nullptr), 0);

    ker::vfs::MountRef mount_ref = ker::vfs::find_mount_point(MOUNTPOINT);
    KREQUIRE_TRUE(static_cast<bool>(mount_ref));
    ker::vfs::MountPoint* mount = mount_ref.get();
    KEXPECT_EQ(mount->fs_type, ker::vfs::FSType::TMPFS);
    KEXPECT_EQ(ker::vfs::mount_point_ref_count_for_test(mount), static_cast<uint32_t>(1));

    bool saw_snapshot = false;
    for (size_t i = 0; i < ker::vfs::get_mount_count(); ++i) {
        ker::vfs::MountSnapshot snapshot{};
        if (!ker::vfs::get_mount_snapshot_at(i, &snapshot)) {
            continue;
        }
        if (std::strcmp(snapshot.path, MOUNTPOINT) == 0) {
            saw_snapshot = snapshot.fs_type == ker::vfs::FSType::TMPFS && std::strcmp(snapshot.fstype, "tmpfs") == 0;
            break;
        }
    }
    KEXPECT_TRUE(saw_snapshot);

    {
        ker::vfs::MountRef root_ref = ker::vfs::find_mount_point("/");
        KREQUIRE_TRUE(static_cast<bool>(root_ref));
        KEXPECT_EQ(ker::vfs::remap_mounts_for_pivot(MOUNTPOINT, PUT_OLD), -EBUSY);
    }

    mount_ref.reset();
    KEXPECT_EQ(ker::vfs::mount_point_ref_count_for_test(mount), static_cast<uint32_t>(0));
    KEXPECT_EQ(ker::vfs::unmount_filesystem(MOUNTPOINT), 0);
    ker::vfs::vfs_rmdir(MOUNTPOINT);
}

KTEST(VFS, AppendMode) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    ker::vfs::File* f = ker::vfs::vfs_open_file("/tmp/ktest_append", ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(f, nullptr);

    constexpr char CHUNK1[] = "Hello, ";
    constexpr char CHUNK2[] = "World!";
    constexpr size_t L1 = sizeof(CHUNK1) - 1;
    constexpr size_t L2 = sizeof(CHUNK2) - 1;

    ssize_t const NW1 = ker::vfs::tmpfs::tmpfs_write(f, static_cast<const void*>(CHUNK1), L1, 0);
    KEXPECT_EQ(NW1, static_cast<ssize_t>(L1));
    ssize_t const NW2 = ker::vfs::tmpfs::tmpfs_write(f, static_cast<const void*>(CHUNK2), L2, L1);
    KEXPECT_EQ(NW2, static_cast<ssize_t>(L2));
    ker::vfs::tmpfs::tmpfs_fops_close(f);

    ker::vfs::File* rf = ker::vfs::vfs_open_file("/tmp/ktest_append", 0, 0);
    KREQUIRE_NE(rf, nullptr);

    char rbuf[32] = {};
    ssize_t const NR = ker::vfs::tmpfs::tmpfs_read(rf, static_cast<void*>(rbuf), L1 + L2, 0);
    KEXPECT_EQ(NR, static_cast<ssize_t>(L1 + L2));
    ker::vfs::tmpfs::tmpfs_fops_close(rf);

    KEXPECT_TRUE(memcmp(static_cast<const void*>(rbuf), "Hello, World!", L1 + L2) == 0);

    ker::vfs::vfs_unlink("/tmp/ktest_append");
}

KTEST(VFS, TmpfsAppendUsesCurrentEnd) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    ker::vfs::File* first = ker::vfs::vfs_open_file("/tmp/ktest_append_current_end", ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(first, nullptr);
    ker::vfs::File* second = ker::vfs::vfs_open_file("/tmp/ktest_append_current_end", 1, 0644);
    KREQUIRE_NE(second, nullptr);

    constexpr char FIRST[] = "one\n";
    constexpr char SECOND[] = "two\n";
    constexpr size_t FIRST_LEN = sizeof(FIRST) - 1;
    constexpr size_t SECOND_LEN = sizeof(SECOND) - 1;

    size_t first_offset = 99;
    ssize_t const FIRST_WRITE = ker::vfs::tmpfs::tmpfs_write_append(first, static_cast<const void*>(FIRST), FIRST_LEN, &first_offset);
    KEXPECT_EQ(FIRST_WRITE, static_cast<ssize_t>(FIRST_LEN));
    KEXPECT_EQ(first_offset, static_cast<size_t>(0));

    size_t second_offset = 99;
    ssize_t const SECOND_WRITE = ker::vfs::tmpfs::tmpfs_write_append(second, static_cast<const void*>(SECOND), SECOND_LEN, &second_offset);
    KEXPECT_EQ(SECOND_WRITE, static_cast<ssize_t>(SECOND_LEN));
    KEXPECT_EQ(second_offset, FIRST_LEN);

    char rbuf[16] = {};
    ssize_t const READ = ker::vfs::tmpfs::tmpfs_read(first, static_cast<void*>(rbuf), FIRST_LEN + SECOND_LEN, 0);
    KEXPECT_EQ(READ, static_cast<ssize_t>(FIRST_LEN + SECOND_LEN));
    KEXPECT_TRUE(memcmp(static_cast<const void*>(rbuf), "one\ntwo\n", FIRST_LEN + SECOND_LEN) == 0);

    ker::vfs::tmpfs::tmpfs_fops_close(second);
    ker::vfs::tmpfs::tmpfs_fops_close(first);
    ker::vfs::vfs_unlink("/tmp/ktest_append_current_end");
}

KTEST(VFS, WriteReadAligned4K) {
    // Write exactly 4 KB then read it back from the same file.
    // Exercises the aligned write->read path (same as mmap_file in testd):
    // the filesystem must bridge the write's dirty buffer and the read's
    // block-cache lookup without going to disk.
    constexpr size_t SIZE = 4096;
    ker::vfs::vfs_mkdir("/tmp", 0755);

    ker::vfs::File* f = ker::vfs::vfs_open_file("/tmp/ktest_aligned4k", ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(f, nullptr);
    KREQUIRE_NE(f->fops, nullptr);
    KREQUIRE_NE(f->fops->vfs_write, nullptr);
    KREQUIRE_NE(f->fops->vfs_read, nullptr);

    uint8_t wbuf[SIZE];
    for (size_t i = 0; i < SIZE; i++) {
        wbuf[i] = static_cast<uint8_t>(i & 0xFF);
    }
    ssize_t const NW = f->fops->vfs_write(f, static_cast<const void*>(wbuf), SIZE, 0);
    KEXPECT_EQ(NW, static_cast<ssize_t>(SIZE));

    uint8_t rbuf[SIZE] = {};
    ssize_t const NR = f->fops->vfs_read(f, static_cast<void*>(rbuf), SIZE, 0);
    KEXPECT_EQ(NR, static_cast<ssize_t>(SIZE));

    bool ok = true;
    for (size_t i = 0; i < SIZE; i++) {
        if (rbuf[i] != static_cast<uint8_t>(i & 0xFF)) {
            ok = false;
            break;
        }
    }
    KEXPECT_TRUE(ok);

    if (f->fops->vfs_close != nullptr) {
        f->fops->vfs_close(f);
    }
    ker::vfs::vfs_unlink("/tmp/ktest_aligned4k");
}
