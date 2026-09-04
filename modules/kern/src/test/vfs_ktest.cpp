#include <bits/ssize_t.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/task.hpp>
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

namespace {

auto create_empty_vfs_file(const char* path) -> bool {
    ker::vfs::File* file = ker::vfs::vfs_open_file(path, ker::vfs::O_CREAT | ker::vfs::O_EXCL | 1, 0644);
    if (file == nullptr) {
        return false;
    }
    return ker::vfs::vfs_close_file(file) == 0;
}

auto close_task_vfs_fd(ker::mod::sched::task::Task& task, int fd) -> bool {
    ker::vfs::File* file = ker::vfs::vfs_get_file(&task, fd);
    if (file == nullptr || ker::vfs::vfs_release_fd(&task, fd) != 0) {
        return false;
    }
    ker::vfs::vfs_put_file(file);
    return true;
}

auto renamed_dirfd_keeps_directory_identity_after_path_reuse() -> bool {
    constexpr const char* OLD_DIR = "/tmp/ktest_stable_dirfd_old";
    constexpr const char* MOVED_DIR = "/tmp/ktest_stable_dirfd_moved";
    constexpr const char* OLD_CHILD = "/tmp/ktest_stable_dirfd_old/child";
    constexpr const char* MOVED_CHILD = "/tmp/ktest_stable_dirfd_moved/child";
    constexpr const char* OLD_NESTED_DIR = "/tmp/ktest_stable_dirfd_old/nested";
    constexpr const char* MOVED_NESTED_DIR = "/tmp/ktest_stable_dirfd_moved/nested";
    constexpr const char* OLD_NESTED_CHILD = "/tmp/ktest_stable_dirfd_old/nested/child";
    constexpr const char* MOVED_NESTED_CHILD = "/tmp/ktest_stable_dirfd_moved/nested/child";
    constexpr const char* MOVED_CREATED = "/tmp/ktest_stable_dirfd_moved/created";
    constexpr const char* MOVED_RENAMED = "/tmp/ktest_stable_dirfd_moved/renamed";
    constexpr const char* MOVED_LINK = "/tmp/ktest_stable_dirfd_moved/link";

    ker::vfs::vfs_unlink(MOVED_LINK);
    ker::vfs::vfs_unlink(MOVED_RENAMED);
    ker::vfs::vfs_unlink(MOVED_CREATED);
    ker::vfs::vfs_unlink(MOVED_CHILD);
    ker::vfs::vfs_unlink(OLD_CHILD);
    ker::vfs::vfs_unlink(MOVED_NESTED_CHILD);
    ker::vfs::vfs_unlink(OLD_NESTED_CHILD);
    ker::vfs::vfs_rmdir(MOVED_NESTED_DIR);
    ker::vfs::vfs_rmdir(OLD_NESTED_DIR);
    ker::vfs::vfs_rmdir(MOVED_DIR);
    ker::vfs::vfs_rmdir(OLD_DIR);
    ker::vfs::vfs_mkdir("/tmp", 0755);

    unsigned stage = 1;
    bool ok = ker::vfs::vfs_mkdir(OLD_DIR, 0755) == 0 && create_empty_vfs_file(OLD_CHILD) &&
              ker::vfs::vfs_mkdir(OLD_NESTED_DIR, 0755) == 0 && create_empty_vfs_file(OLD_NESTED_CHILD);
    ker::mod::sched::task::Task task{};
    std::memcpy(task.root.data(), "/", 2);
    std::memcpy(task.cwd.data(), "/", 2);
    ker::vfs::File* directory = ok ? ker::vfs::vfs_open_file(OLD_DIR, ker::vfs::O_DIRECTORY, 0) : nullptr;
    int const DIR_FD = directory != nullptr ? ker::vfs::vfs_alloc_fd(&task, directory) : -1;
    ok = ok && directory != nullptr && DIR_FD >= 0;

    // Rename the opened directory, then reuse its former pathname for a
    // different directory containing a same-named child. Relative operations
    // must stay attached to the opened object, not its historical path text.
    if (ok) {
        stage = 2;
        ok = ker::vfs::vfs_rename(OLD_DIR, MOVED_DIR) == 0 && ker::vfs::vfs_mkdir(OLD_DIR, 0755) == 0 && create_empty_vfs_file(OLD_CHILD) &&
             ker::vfs::vfs_mkdir(OLD_NESTED_DIR, 0755) == 0 && create_empty_vfs_file(OLD_NESTED_CHILD);
    }

    ker::vfs::Stat retained{};
    ker::vfs::Stat moved{};
    ker::vfs::Stat replacement{};
    if (ok) {
        stage = 3;
        ok = ker::vfs::vfs_statat(&task, DIR_FD, "child", 0, &retained) == 0 && ker::vfs::vfs_stat(MOVED_CHILD, &moved) == 0 &&
             ker::vfs::vfs_stat(OLD_CHILD, &replacement) == 0 && retained.st_ino == moved.st_ino && retained.st_ino != replacement.st_ino;
    }

    int child_fd = -1;
    if (ok) {
        stage = 4;
        child_fd = ker::vfs::vfs_openat(&task, DIR_FD, "child", 0, 0);
        ok = child_fd >= 0;
    }
    if (child_fd >= 0) {
        ok = close_task_vfs_fd(task, child_fd) && ok;
    }

    ker::vfs::Stat retained_nested{};
    ker::vfs::Stat moved_nested{};
    ker::vfs::Stat replacement_nested{};
    if (ok) {
        stage = 5;
        ok = ker::vfs::vfs_statat(&task, DIR_FD, "nested/child", 0, &retained_nested) == 0 &&
             ker::vfs::vfs_stat(MOVED_NESTED_CHILD, &moved_nested) == 0 && ker::vfs::vfs_stat(OLD_NESTED_CHILD, &replacement_nested) == 0 &&
             retained_nested.st_ino == moved_nested.st_ino && retained_nested.st_ino != replacement_nested.st_ino;
    }

    int created_fd = -1;
    if (ok) {
        stage = 6;
        created_fd = ker::vfs::vfs_openat(&task, DIR_FD, "created", ker::vfs::O_CREAT | ker::vfs::O_EXCL | 1, 0600);
        ok = created_fd >= 0;
    }
    if (created_fd >= 0) {
        ok = close_task_vfs_fd(task, created_fd) && ok;
    }
    if (ok) {
        stage = 7;
        ker::vfs::Stat created{};
        int const MOVED_CREATED_STAT = ker::vfs::vfs_stat(MOVED_CREATED, &created);
        int const OLD_CREATED_STAT = ker::vfs::vfs_stat("/tmp/ktest_stable_dirfd_old/created", &created);
        ok = MOVED_CREATED_STAT == 0 && OLD_CREATED_STAT == -ENOENT;
        if (!ok) {
            ker::mod::dbg::log("stable dirfd created placement failed moved=%d old=%d", MOVED_CREATED_STAT, OLD_CREATED_STAT);
        }
    }
    if (ok) {
        stage = 8;
        ok = ker::vfs::vfs_renameat(&task, DIR_FD, "created", DIR_FD, "renamed") == 0;
    }
    if (ok) {
        stage = 9;
        ok = ker::vfs::vfs_symlinkat(&task, "nested/child", DIR_FD, "link") == 0;
    }
    if (ok) {
        stage = 10;
        std::array<char, 16> target{};
        ssize_t const READ = ker::vfs::vfs_readlinkat(&task, DIR_FD, "link", target.data(), target.size());
        ok = READ == static_cast<ssize_t>(sizeof("nested/child") - 1) &&
             std::memcmp(target.data(), "nested/child", sizeof("nested/child") - 1) == 0;
    }
    int followed_link_fd = -1;
    if (ok) {
        stage = 11;
        followed_link_fd = ker::vfs::vfs_openat(&task, DIR_FD, "link", 0, 0);
        ok = followed_link_fd >= 0;
    }
    if (followed_link_fd >= 0) {
        ok = close_task_vfs_fd(task, followed_link_fd) && ok;
    }
    if (ok) {
        stage = 12;
        ok = ker::vfs::vfs_unlinkat(&task, DIR_FD, "link", 0) == 0 && ker::vfs::vfs_unlinkat(&task, DIR_FD, "renamed", 0) == 0;
    }

    if (DIR_FD >= 0) {
        ok = ker::vfs::vfs_release_fd(&task, DIR_FD) == 0 && ok;
    }
    if (directory != nullptr) {
        ker::vfs::vfs_put_file(directory);
    }
    ok = task.fd_table.empty() && ok;

    ker::vfs::vfs_unlink(MOVED_LINK);
    ker::vfs::vfs_unlink(MOVED_RENAMED);
    ker::vfs::vfs_unlink(MOVED_CREATED);
    int const MOVED_CHILD_CLEANUP = ker::vfs::vfs_unlink(MOVED_CHILD);
    int const OLD_CHILD_CLEANUP = ker::vfs::vfs_unlink(OLD_CHILD);
    int const MOVED_NESTED_CHILD_CLEANUP = ker::vfs::vfs_unlink(MOVED_NESTED_CHILD);
    int const OLD_NESTED_CHILD_CLEANUP = ker::vfs::vfs_unlink(OLD_NESTED_CHILD);
    int const MOVED_NESTED_DIR_CLEANUP = ker::vfs::vfs_rmdir(MOVED_NESTED_DIR);
    int const OLD_NESTED_DIR_CLEANUP = ker::vfs::vfs_rmdir(OLD_NESTED_DIR);
    int const MOVED_DIR_CLEANUP = ker::vfs::vfs_rmdir(MOVED_DIR);
    int const OLD_DIR_CLEANUP = ker::vfs::vfs_rmdir(OLD_DIR);
    bool const CLEANUP_OK = MOVED_CHILD_CLEANUP == 0 && OLD_CHILD_CLEANUP == 0 && MOVED_NESTED_CHILD_CLEANUP == 0 &&
                            OLD_NESTED_CHILD_CLEANUP == 0 && MOVED_NESTED_DIR_CLEANUP == 0 && OLD_NESTED_DIR_CLEANUP == 0 &&
                            MOVED_DIR_CLEANUP == 0 && OLD_DIR_CLEANUP == 0;
    if (!ok || !CLEANUP_OK) {
        ker::mod::dbg::log("stable dirfd selftest failed stage=%u cleanup=%d,%d,%d,%d,%d,%d,%d,%d", stage, MOVED_CHILD_CLEANUP,
                           OLD_CHILD_CLEANUP, MOVED_NESTED_CHILD_CLEANUP, OLD_NESTED_CHILD_CLEANUP, MOVED_NESTED_DIR_CLEANUP,
                           OLD_NESTED_DIR_CLEANUP, MOVED_DIR_CLEANUP, OLD_DIR_CLEANUP);
    }
    return ok && CLEANUP_OK;
}

auto symlink_rename_replacement_publishes_whole_binding() -> bool {
    constexpr const char* TARGET_A = "/tmp/ktest_lookup_target_a";
    constexpr const char* TARGET_B = "/tmp/ktest_lookup_target_b";
    constexpr const char* LIVE_LINK = "/tmp/ktest_lookup_live_link";
    constexpr const char* NEXT_LINK = "/tmp/ktest_lookup_next_link";
    constexpr unsigned ITERATIONS = 16;

    ker::vfs::vfs_unlink(NEXT_LINK);
    ker::vfs::vfs_unlink(LIVE_LINK);
    ker::vfs::vfs_unlink(TARGET_B);
    ker::vfs::vfs_unlink(TARGET_A);
    ker::vfs::vfs_mkdir("/tmp", 0755);

    bool ok = create_empty_vfs_file(TARGET_A) && create_empty_vfs_file(TARGET_B) && ker::vfs::vfs_symlink(TARGET_A, LIVE_LINK) == 0;
    std::array<ker::vfs::Stat, 2> targets{};
    ok = ok && ker::vfs::vfs_stat(TARGET_A, &targets[0]) == 0 && ker::vfs::vfs_stat(TARGET_B, &targets[1]) == 0;

    for (unsigned iteration = 0; ok && iteration < ITERATIONS; ++iteration) {
        unsigned const TARGET_INDEX = iteration & 1U;
        const char* target = TARGET_INDEX == 0 ? TARGET_A : TARGET_B;
        ok = ker::vfs::vfs_symlink(target, NEXT_LINK) == 0 && ker::vfs::vfs_rename(NEXT_LINK, LIVE_LINK) == 0;

        std::array<char, 64> link_text{};
        ssize_t const READ = ker::vfs::vfs_readlink(LIVE_LINK, link_text.data(), link_text.size());
        size_t const TARGET_LENGTH = std::strlen(target);
        ker::vfs::Stat followed{};
        ker::vfs::Stat nofollow{};
        ok = ok && READ == static_cast<ssize_t>(TARGET_LENGTH) && std::memcmp(link_text.data(), target, TARGET_LENGTH) == 0 &&
             ker::vfs::vfs_stat(LIVE_LINK, &followed) == 0 && followed.st_ino == targets[TARGET_INDEX].st_ino &&
             ker::vfs::vfs_lstat(LIVE_LINK, &nofollow) == 0 &&
             (nofollow.st_mode & static_cast<mode_t>(ker::vfs::S_IFMT)) == static_cast<mode_t>(ker::vfs::S_IFLNK) &&
             ker::vfs::vfs_lstat(NEXT_LINK, &nofollow) == -ENOENT;
    }

    int const NEXT_CLEANUP = ker::vfs::vfs_unlink(NEXT_LINK);
    int const LIVE_CLEANUP = ker::vfs::vfs_unlink(LIVE_LINK);
    int const TARGET_B_CLEANUP = ker::vfs::vfs_unlink(TARGET_B);
    int const TARGET_A_CLEANUP = ker::vfs::vfs_unlink(TARGET_A);
    return ok && (NEXT_CLEANUP == 0 || NEXT_CLEANUP == -ENOENT) && LIVE_CLEANUP == 0 && TARGET_B_CLEANUP == 0 && TARGET_A_CLEANUP == 0;
}

}  // namespace

KTEST(VFS, OpenFdInstallFailureClosesFile) { KEXPECT_TRUE(ker::vfs::vfs_selftest_fd_install_failure_closes_file()); }

KTEST(VFS, Dup2ReplacePreservesNewfdOnFailure) { KEXPECT_TRUE(ker::vfs::vfs_selftest_dup2_replace_preserves_newfd_on_failure()); }

KTEST(VFS, PipeFailureUnwindsAllStages) { KEXPECT_TRUE(ker::vfs::vfs_selftest_pipe_failure_unwinds()); }

KTEST(VFS, PipeFlags) { KEXPECT_TRUE(ker::vfs::vfs_selftest_pipe_flags()); }

KTEST(VFS, AnonymousFstatSnapshotHits) { KEXPECT_TRUE(ker::vfs::vfs_selftest_anonymous_fstat_snapshot_hits()); }

KTEST(VFS, FstatSnapshotFastPathHits) { KEXPECT_TRUE(ker::vfs::vfs_selftest_fstat_snapshot_fast_path_hits()); }

KTEST(VFS, FstatCloseCombinesFdRemoval) { KEXPECT_TRUE(ker::vfs::vfs_selftest_fstat_close_combines_fd_removal()); }

KTEST(VFS, RemoteFstatSnapshotCacheable) { KEXPECT_TRUE(ker::vfs::vfs_selftest_remote_fstat_snapshot_cacheable()); }

KTEST(VFS, FstatSeedsPathMetadataCache) { KEXPECT_TRUE(ker::vfs::vfs_selftest_fstat_seeds_path_metadata_cache()); }

KTEST(VFS, FilePathStorage) { KEXPECT_TRUE(ker::vfs::vfs_selftest_file_path_storage()); }

KTEST(VFS, FileDataWriteInvalidatesPathStat) { KEXPECT_TRUE(ker::vfs::vfs_selftest_file_data_write_invalidates_path_stat()); }

KTEST(VFS, FileDataWriteSkipsUncachedPathInvalidation) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_file_data_write_skips_uncached_path_invalidation());
}

KTEST(VFS, FileDataCloseRefreshesCreatedPathStat) { KEXPECT_TRUE(ker::vfs::vfs_selftest_file_data_close_refreshes_created_path_stat()); }

KTEST(VFS, FileDataCloseDoesNotResurrectRemovedPath) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_file_data_close_does_not_resurrect_removed_path());
}

KTEST(VFS, CreatedOpenPrefillSeedsPathStat) { KEXPECT_TRUE(ker::vfs::vfs_selftest_created_open_prefill_seeds_path_stat()); }

KTEST(VFS, FileMetadataChangeInvalidatesPathStat) { KEXPECT_TRUE(ker::vfs::vfs_selftest_file_metadata_change_invalidates_path_stat()); }

KTEST(VFS, OpenCreateMetadataHint) { KEXPECT_TRUE(ker::vfs::vfs_selftest_open_create_metadata_hint()); }

KTEST(VFS, OpenMissingUsesMetadataCache) { KEXPECT_TRUE(ker::vfs::vfs_selftest_open_missing_uses_metadata_cache()); }

KTEST(VFS, OpenSuccessSeedsMetadataCache) { KEXPECT_TRUE(ker::vfs::vfs_selftest_open_success_seeds_metadata_cache()); }

KTEST(VFS, OpenWriteSuccessSeedsMetadataHints) { KEXPECT_TRUE(ker::vfs::vfs_selftest_open_write_success_seeds_metadata_hints()); }

KTEST(VFS, MetadataCacheStoresEnotdir) { KEXPECT_TRUE(ker::vfs::vfs_selftest_metadata_cache_stores_enotdir()); }

KTEST(VFS, MkdirSeedsMetadataCache) { KEXPECT_TRUE(ker::vfs::vfs_selftest_mkdir_seeds_metadata_cache()); }

KTEST(VFS, RemovedPathsSeedMissingMetadataCache) { KEXPECT_TRUE(ker::vfs::vfs_selftest_removed_paths_seed_missing_metadata_cache()); }

KTEST(VFS, OpenatDirfdInstallsOpenFile) { KEXPECT_TRUE(ker::vfs::vfs_selftest_openat_dirfd_installs_open_file()); }

KTEST(VFS, OpenatAtFdcwdUsesSuppliedTask) { KEXPECT_TRUE(ker::vfs::vfs_selftest_openat_at_fdcwd_uses_supplied_task()); }

KTEST(VFS, FchdirChangesSuppliedTaskCwd) { KEXPECT_TRUE(ker::vfs::vfs_selftest_fchdir_changes_supplied_task_cwd()); }

KTEST(VFS, UnlinkatRenameatDirfdMutations) { KEXPECT_TRUE(ker::vfs::vfs_selftest_unlinkat_renameat_dirfd_mutations()); }

KTEST(VFS, RenamedDirfdKeepsDirectoryIdentityAfterPathReuse) { KEXPECT_TRUE(renamed_dirfd_keeps_directory_identity_after_path_reuse()); }

KTEST(VFS, RenameSeedsMetadataCache) { KEXPECT_TRUE(ker::vfs::vfs_selftest_rename_seeds_metadata_cache()); }

KTEST(VFS, MetadataCacheRejectsStaleNegativeStore) { KEXPECT_TRUE(ker::vfs::vfs_selftest_metadata_cache_rejects_stale_negative_store()); }

KTEST(VFS, ResolvedStatCacheRejectsMountGenerationChange) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_resolved_stat_cache_rejects_mount_generation_change());
}

KTEST(VFS, PathTextScanMatchesHelpers) { KEXPECT_TRUE(ker::vfs::vfs_selftest_path_text_scan_matches_helpers()); }

KTEST(VFS, WkiHostAliasOverlap) { KEXPECT_TRUE(ker::vfs::vfs_selftest_wki_host_alias_overlap()); }

KTEST(VFS, WkiHostRootMountGateMatchesTaskRoot) { KEXPECT_TRUE(ker::vfs::vfs_selftest_wki_host_root_mount_gate_matches_task_root()); }

KTEST(VFS, ResolvedWkiEntryUsesTaskRootView) { KEXPECT_TRUE(ker::vfs::vfs_selftest_resolved_wki_entry_uses_task_root_view()); }

KTEST(VFS, AbsoluteLocalStatFastPathGate) { KEXPECT_TRUE(ker::vfs::vfs_selftest_absolute_local_stat_fast_path_gate()); }

KTEST(VFS, CommonLocalRelativeResolverFastPath) { KEXPECT_TRUE(ker::vfs::vfs_selftest_common_local_relative_resolver_fast_path()); }

KTEST(VFS, StatatDirfdMetadataCache) { KEXPECT_TRUE(ker::vfs::vfs_selftest_statat_dirfd_metadata_cache()); }

KTEST(VFS, StatatAtFdcwdUsesSuppliedTask) { KEXPECT_TRUE(ker::vfs::vfs_selftest_statat_at_fdcwd_uses_supplied_task()); }

KTEST(VFS, StatatRootCwdRelativePaths) { KEXPECT_TRUE(ker::vfs::vfs_selftest_statat_root_cwd_relative_paths()); }

KTEST(VFS, FaccessatDirfdMetadataCache) { KEXPECT_TRUE(ker::vfs::vfs_selftest_faccessat_dirfd_metadata_cache()); }

KTEST(VFS, FaccessatAtFdcwdUsesSuppliedTask) { KEXPECT_TRUE(ker::vfs::vfs_selftest_faccessat_at_fdcwd_uses_supplied_task()); }

KTEST(VFS, FaccessatFOkExistenceCacheInvalidates) { KEXPECT_TRUE(ker::vfs::vfs_selftest_faccessat_f_ok_existence_cache_invalidates()); }

KTEST(VFS, FaccessatFOkSkipsKnownNonSymlinkProbe) { KEXPECT_TRUE(ker::vfs::vfs_selftest_faccessat_f_ok_skips_known_non_symlink_probe()); }

KTEST(VFS, FaccessatFlags) { KEXPECT_TRUE(ker::vfs::vfs_selftest_faccessat_flags()); }

KTEST(VFS, MkdiratDirfdCreatesRelativeDirectory) { KEXPECT_TRUE(ker::vfs::vfs_selftest_mkdirat_dirfd_creates_relative_directory()); }

KTEST(VFS, ReadlinkatDirfdReadsRelativeSymlink) { KEXPECT_TRUE(ker::vfs::vfs_selftest_readlinkat_dirfd_reads_relative_symlink()); }

KTEST(VFS, ReadlinkFollowsIntermediateSymlink) { KEXPECT_TRUE(ker::vfs::vfs_selftest_readlink_follows_intermediate_symlink()); }

KTEST(VFS, SymlinkatDirfdCreatesRelativeSymlink) { KEXPECT_TRUE(ker::vfs::vfs_selftest_symlinkat_dirfd_creates_relative_symlink()); }

KTEST(VFS, SymlinkRenameReplacementPublishesWholeBinding) { KEXPECT_TRUE(symlink_rename_replacement_publishes_whole_binding()); }

KTEST(VFS, LinkatDirfdCreatesRelativeHardlink) { KEXPECT_TRUE(ker::vfs::vfs_selftest_linkat_dirfd_creates_relative_hardlink()); }

KTEST(VFS, ChdirCommonLocalFastPathUsesMetadataCache) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_chdir_common_local_fast_path_uses_metadata_cache());
}

KTEST(VFS, FchmodatDirfdChangesRelativeFileMode) { KEXPECT_TRUE(ker::vfs::vfs_selftest_fchmodat_dirfd_changes_relative_file_mode()); }

KTEST(VFS, FchownatDirfdChangesRelativeFileOwner) { KEXPECT_TRUE(ker::vfs::vfs_selftest_fchownat_dirfd_changes_relative_file_owner()); }

KTEST(VFS, StatLstatShareNonSymlinkCache) { KEXPECT_TRUE(ker::vfs::vfs_selftest_stat_lstat_share_non_symlink_cache()); }

KTEST(VFS, ReaddirSeedsNonSymlinkHints) { KEXPECT_TRUE(ker::vfs::vfs_selftest_readdir_seeds_non_symlink_hints()); }

KTEST(VFS, ReadlinkUsesMetadataNegativeCache) { KEXPECT_TRUE(ker::vfs::vfs_selftest_readlink_uses_metadata_negative_cache()); }

KTEST(VFS, MissingPrefixShortCircuitsSymlinkWalk) { KEXPECT_TRUE(ker::vfs::vfs_selftest_missing_prefix_short_circuits_symlink_walk()); }

KTEST(VFS, SymlinkPrefixCacheSkipsKnownParent) { KEXPECT_TRUE(ker::vfs::vfs_selftest_symlink_prefix_cache_skips_known_parent()); }

KTEST(VFS, ProcfsFdLinkProbeGate) { KEXPECT_TRUE(ker::vfs::vfs_selftest_procfs_fd_link_probe_gate()); }

KTEST(VFS, PackedDirentRecords) { KEXPECT_TRUE(ker::vfs::vfs_selftest_packed_dirent_records()); }

KTEST(VFS, FcntlSetflPreservesOpenPolicyFlags) { KEXPECT_TRUE(ker::vfs::vfs_selftest_fcntl_setfl_preserves_open_policy_flags()); }

KTEST(VFS, StreamCacheReadEligibility) { KEXPECT_TRUE(ker::vfs::vfs_selftest_stream_cache_read_eligibility()); }

KTEST(VFS, RemoteReadBounceMatchesBulkWindow) { KEXPECT_TRUE(ker::vfs::vfs_selftest_remote_read_bounce_window()); }

KTEST(VFS, StreamCacheLocalDetachedTtl) { KEXPECT_TRUE(ker::vfs::vfs_selftest_stream_cache_local_detached_ttl()); }

KTEST(VFS, FdAllocationCapsCloexecRange) { KEXPECT_TRUE(ker::vfs::vfs_selftest_fd_allocation_caps_cloexec_range()); }

KTEST(VFS, CreateAndStat) {
    ker::vfs::vfs_mkdir("/tmp", 0755);  // idempotent if /tmp already exists

    ker::vfs::File* f = ker::vfs::vfs_open_file("/tmp/ktest_create", ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(f, nullptr);
    static_cast<void>(ker::vfs::vfs_close_file(f));

    ker::vfs::Stat st{};
    KEXPECT_EQ(ker::vfs::vfs_stat("/tmp/ktest_create", &st), 0);
    KEXPECT_TRUE((st.st_mode & ker::vfs::S_IFREG) != 0U);

    ker::vfs::vfs_unlink("/tmp/ktest_create");
}

KTEST(VFS, StatvfsTmpfsPath) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    ker::vfs::Statvfs st{};
    KEXPECT_EQ(ker::vfs::vfs_statvfs("/tmp", &st), 0);
    KEXPECT_EQ(st.f_bsize, static_cast<unsigned long>(4096));
    KEXPECT_EQ(st.f_frsize, static_cast<unsigned long>(4096));
}

KTEST(VFS, XattrBeneathRejectsSymlinkAndRemoteEscape) {
    constexpr const char* EXPORT_ROOT = "/tmp/ktest_xattr_export";
    constexpr const char* FINAL_ABSOLUTE = "/tmp/ktest_xattr_export/final_absolute";
    constexpr const char* FINAL_RELATIVE = "/tmp/ktest_xattr_export/final_relative";
    constexpr const char* INTERMEDIATE = "/tmp/ktest_xattr_export/intermediate";
    constexpr const char* REMOTE_MOUNT = "/tmp/ktest_xattr_export/remote";
    constexpr const char* NAME = "user.ktest";

    static_cast<void>(ker::vfs::unmount_filesystem(REMOTE_MOUNT));
    static_cast<void>(ker::vfs::vfs_unlink(FINAL_ABSOLUTE));
    static_cast<void>(ker::vfs::vfs_unlink(FINAL_RELATIVE));
    static_cast<void>(ker::vfs::vfs_unlink(INTERMEDIATE));
    static_cast<void>(ker::vfs::vfs_rmdir(REMOTE_MOUNT));
    static_cast<void>(ker::vfs::vfs_rmdir(EXPORT_ROOT));
    static_cast<void>(ker::vfs::vfs_mkdir("/tmp", 0755));
    KREQUIRE_EQ(ker::vfs::vfs_mkdir(EXPORT_ROOT, 0755), 0);

    KREQUIRE_EQ(ker::vfs::vfs_symlink("/tmp", FINAL_ABSOLUTE), 0);
    KREQUIRE_EQ(ker::vfs::vfs_symlink("../ktest_xattr_outside", FINAL_RELATIVE), 0);
    KREQUIRE_EQ(ker::vfs::vfs_symlink("/tmp", INTERMEDIATE), 0);

    KEXPECT_EQ(ker::vfs::vfs_getxattr_beneath(EXPORT_ROOT, "final_absolute", NAME, nullptr, 0, true), static_cast<ssize_t>(-EPERM));
    KEXPECT_EQ(ker::vfs::vfs_getxattr_beneath(EXPORT_ROOT, "final_relative", NAME, nullptr, 0, true), static_cast<ssize_t>(-EPERM));
    KEXPECT_EQ(ker::vfs::vfs_getxattr_beneath(EXPORT_ROOT, "intermediate/child", NAME, nullptr, 0, true), static_cast<ssize_t>(-EPERM));
    KEXPECT_EQ(ker::vfs::vfs_getxattr_beneath(EXPORT_ROOT, "/absolute", NAME, nullptr, 0, true), static_cast<ssize_t>(-EINVAL));

    KREQUIRE_EQ(ker::vfs::vfs_mkdir(REMOTE_MOUNT, 0755), 0);
    KREQUIRE_EQ(ker::vfs::mount_filesystem(REMOTE_MOUNT, "remote", nullptr, 0, nullptr, nullptr, nullptr), 0);
    KEXPECT_EQ(ker::vfs::vfs_getxattr_beneath(EXPORT_ROOT, "remote/child", NAME, nullptr, 0, true), static_cast<ssize_t>(-EPERM));

    KEXPECT_EQ(ker::vfs::unmount_filesystem(REMOTE_MOUNT), 0);
    KEXPECT_EQ(ker::vfs::vfs_unlink(FINAL_ABSOLUTE), 0);
    KEXPECT_EQ(ker::vfs::vfs_unlink(FINAL_RELATIVE), 0);
    KEXPECT_EQ(ker::vfs::vfs_unlink(INTERMEDIATE), 0);
    KEXPECT_EQ(ker::vfs::vfs_rmdir(REMOTE_MOUNT), 0);
    KEXPECT_EQ(ker::vfs::vfs_rmdir(EXPORT_ROOT), 0);
}

KTEST(VFS, UtimensatUpdatesTmpfsTimestamps) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    constexpr const char* PATH = "/tmp/ktest_utimensat";
    constexpr int64_t UTIME_OMIT_VALUE = (1LL << 30) - 2;
    ker::vfs::vfs_unlink(PATH);

    ker::vfs::File* f = ker::vfs::vfs_open_file(PATH, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(f, nullptr);
    static_cast<void>(ker::vfs::vfs_close_file(f));

    ker::vfs::Timespec times[2] = {
        {.tv_sec = 1234, .tv_nsec = 567},
        {.tv_sec = 2345, .tv_nsec = 678},
    };
    KEXPECT_EQ(ker::vfs::vfs_utimensat(ker::vfs::AT_FDCWD, PATH, times, 0), 0);

    ker::vfs::VfsCachePerfSnapshot before_first_stat{};
    ker::vfs::VfsCachePerfSnapshot after_first_stat{};
    ker::vfs::Stat st{};
    ker::vfs::vfs_get_cache_perf_snapshot(before_first_stat);
    KEXPECT_EQ(ker::vfs::vfs_stat(PATH, &st), 0);
    ker::vfs::vfs_get_cache_perf_snapshot(after_first_stat);
    KEXPECT_TRUE(after_first_stat.metadata_hits > before_first_stat.metadata_hits);
    KEXPECT_EQ(st.st_atim.tv_sec, static_cast<int64_t>(1234));
    KEXPECT_EQ(st.st_atim.tv_nsec, static_cast<int64_t>(567));
    KEXPECT_EQ(st.st_mtim.tv_sec, static_cast<int64_t>(2345));
    KEXPECT_EQ(st.st_mtim.tv_nsec, static_cast<int64_t>(678));

    ker::vfs::Timespec omit_atime[2] = {
        {.tv_sec = 0, .tv_nsec = UTIME_OMIT_VALUE},
        {.tv_sec = 3456, .tv_nsec = 789},
    };
    KEXPECT_EQ(ker::vfs::vfs_utimensat(ker::vfs::AT_FDCWD, PATH, omit_atime, 0), 0);

    ker::vfs::VfsCachePerfSnapshot before_omit_stat{};
    ker::vfs::VfsCachePerfSnapshot after_omit_stat{};
    ker::vfs::vfs_get_cache_perf_snapshot(before_omit_stat);
    KEXPECT_EQ(ker::vfs::vfs_stat(PATH, &st), 0);
    ker::vfs::vfs_get_cache_perf_snapshot(after_omit_stat);
    KEXPECT_TRUE(after_omit_stat.metadata_hits > before_omit_stat.metadata_hits);
    KEXPECT_EQ(st.st_atim.tv_sec, static_cast<int64_t>(1234));
    KEXPECT_EQ(st.st_atim.tv_nsec, static_cast<int64_t>(567));
    KEXPECT_EQ(st.st_mtim.tv_sec, static_cast<int64_t>(3456));
    KEXPECT_EQ(st.st_mtim.tv_nsec, static_cast<int64_t>(789));

    ker::vfs::Timespec abs_times[2] = {
        {.tv_sec = 4567, .tv_nsec = 890},
        {.tv_sec = 5678, .tv_nsec = 901},
    };
    constexpr int BOGUS_DIRFD = 9999;
    KEXPECT_EQ(ker::vfs::vfs_utimensat(BOGUS_DIRFD, PATH, abs_times, 0), 0);

    ker::vfs::VfsCachePerfSnapshot before_abs_stat{};
    ker::vfs::VfsCachePerfSnapshot after_abs_stat{};
    ker::vfs::vfs_get_cache_perf_snapshot(before_abs_stat);
    KEXPECT_EQ(ker::vfs::vfs_stat(PATH, &st), 0);
    ker::vfs::vfs_get_cache_perf_snapshot(after_abs_stat);
    KEXPECT_TRUE(after_abs_stat.metadata_hits > before_abs_stat.metadata_hits);
    KEXPECT_EQ(st.st_atim.tv_sec, static_cast<int64_t>(4567));
    KEXPECT_EQ(st.st_atim.tv_nsec, static_cast<int64_t>(890));
    KEXPECT_EQ(st.st_mtim.tv_sec, static_cast<int64_t>(5678));
    KEXPECT_EQ(st.st_mtim.tv_nsec, static_cast<int64_t>(901));

    ker::vfs::vfs_unlink(PATH);
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
    static_cast<void>(ker::vfs::vfs_close_file(f));

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
    ker::vfs::vfs_cache_notify_path_changed(PATH, nullptr);

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
    ker::vfs::vfs_cache_notify_path_changed(LINK, nullptr);

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

KTEST(VFS, SymlinkCacheRepeatedNegativeReadlinkRecordsHit) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    constexpr const char* PATH = "/tmp/ktest_symlink_negative_counter";
    ker::vfs::vfs_unlink(PATH);

    ker::vfs::File* f = ker::vfs::vfs_open_file(PATH, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(f, nullptr);
    ker::vfs::vfs_put_file(f);
    ker::vfs::vfs_cache_notify_path_changed(PATH, nullptr);

    ker::vfs::VfsCachePerfSnapshot before{};
    ker::vfs::vfs_get_cache_perf_snapshot(before);

    char buf[128] = {};
    KEXPECT_EQ(ker::vfs::vfs_readlink_resolved(PATH, buf, sizeof(buf)), static_cast<ssize_t>(-EINVAL));

    ker::vfs::VfsCachePerfSnapshot after_first{};
    ker::vfs::vfs_get_cache_perf_snapshot(after_first);

    KEXPECT_EQ(ker::vfs::vfs_readlink_resolved(PATH, buf, sizeof(buf)), static_cast<ssize_t>(-EINVAL));

    ker::vfs::VfsCachePerfSnapshot after{};
    ker::vfs::vfs_get_cache_perf_snapshot(after);
    KEXPECT_TRUE(after.symlink_stores > before.symlink_stores);
    KEXPECT_TRUE(after.symlink_hits > after_first.symlink_hits);

    KEXPECT_EQ(ker::vfs::vfs_unlink(PATH), 0);
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
    static_cast<void>(ker::vfs::vfs_close_file(wf));

    ker::vfs::File* rf = ker::vfs::vfs_open_file("/tmp/ktest_wr", 0, 0);
    KREQUIRE_NE(rf, nullptr);

    uint8_t rbuf[128] = {};
    ssize_t const NR = ker::vfs::tmpfs::tmpfs_read(rf, static_cast<void*>(rbuf), 128, 0);
    KEXPECT_EQ(NR, static_cast<ssize_t>(128));
    static_cast<void>(ker::vfs::vfs_close_file(rf));

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

    static_cast<void>(ker::vfs::vfs_close_file(f));
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
    static_cast<void>(ker::vfs::vfs_close_file(f));

    f = ker::vfs::vfs_open_file(PATH, ker::vfs::O_CREAT | ker::vfs::O_TRUNC | 1, 0644);
    KREQUIRE_NE(f, nullptr);
    ssize_t const NEW_WRITE = ker::vfs::tmpfs::tmpfs_write(f, static_cast<const void*>(NEW), sizeof(NEW) - 1, 0);
    KEXPECT_EQ(NEW_WRITE, static_cast<ssize_t>(sizeof(NEW) - 1));
    static_cast<void>(ker::vfs::vfs_close_file(f));

    f = ker::vfs::vfs_open_file(PATH, 0, 0);
    KREQUIRE_NE(f, nullptr);
    char rbuf[sizeof(OLD)] = {};
    ssize_t const READ = ker::vfs::tmpfs::tmpfs_read(f, static_cast<void*>(rbuf), sizeof(rbuf), 0);
    KEXPECT_EQ(READ, static_cast<ssize_t>(sizeof(NEW) - 1));
    KEXPECT_TRUE(memcmp(static_cast<const void*>(rbuf), static_cast<const void*>(NEW), sizeof(NEW) - 1) == 0);
    static_cast<void>(ker::vfs::vfs_close_file(f));

    f = ker::vfs::vfs_open_file(PATH, ker::vfs::O_TRUNC | 1, 0644);
    KREQUIRE_NE(f, nullptr);
    static_cast<void>(ker::vfs::vfs_close_file(f));

    f = ker::vfs::vfs_open_file(PATH, 0, 0);
    KREQUIRE_NE(f, nullptr);
    KEXPECT_EQ(ker::vfs::tmpfs::tmpfs_read(f, static_cast<void*>(rbuf), sizeof(rbuf), 0), static_cast<ssize_t>(0));
    static_cast<void>(ker::vfs::vfs_close_file(f));

    ker::vfs::vfs_unlink(PATH);
}

KTEST(VFS, Unlink) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    ker::vfs::File* f = ker::vfs::vfs_open_file("/tmp/ktest_unlink", ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(f, nullptr);
    static_cast<void>(ker::vfs::vfs_close_file(f));

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
    static_cast<void>(ker::vfs::vfs_close_file(file));

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

KTEST(VFS, MkdirExclusiveFinalComponent) {
    constexpr const char* DIR = "/tmp/ktest_dir";
    constexpr const char* MISSING_CHILD = "/tmp/ktest_missing_parent/child";
    constexpr const char* FILE_PATH = "/tmp/ktest_mkdir_existing_file";
    static_cast<void>(ker::vfs::vfs_rmdir(DIR));
    static_cast<void>(ker::vfs::vfs_rmdir("/tmp/ktest_missing_parent"));
    static_cast<void>(ker::vfs::vfs_unlink(FILE_PATH));

    KEXPECT_EQ(ker::vfs::vfs_mkdir(DIR, 0755), 0);
    KEXPECT_EQ(ker::vfs::vfs_mkdir(DIR, 0755), -EEXIST);
    KEXPECT_EQ(ker::vfs::vfs_mkdir(MISSING_CHILD, 0755), -ENOENT);

    ker::vfs::Stat st{};
    KEXPECT_EQ(ker::vfs::vfs_stat(DIR, &st), 0);
    KEXPECT_TRUE((st.st_mode & ker::vfs::S_IFDIR) != 0U);

    ker::vfs::File* file = ker::vfs::vfs_open_file(FILE_PATH, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(file, nullptr);
    ker::vfs::vfs_put_file(file);
    KEXPECT_EQ(ker::vfs::vfs_mkdir(FILE_PATH, 0755), -EEXIST);

    KEXPECT_EQ(ker::vfs::vfs_unlink(FILE_PATH), 0);
    KEXPECT_EQ(ker::vfs::vfs_rmdir(DIR), 0);
}

KTEST(VFS, TmpfsMountHasSeparateRoot) {
    constexpr const char* MOUNTPOINT = "/tmp/ktest_tmpfs_mount";
    constexpr const char* ROOT_ONLY_FILE = "/tmp/ktest_tmpfs_root_only";
    constexpr const char* MOUNTED_TMP_DIR = "/tmp/ktest_tmpfs_mount/tmp";
    constexpr const char* MOUNT_FILE = "/tmp/ktest_tmpfs_mount/ktest_tmpfs_mount_file";

    ker::vfs::vfs_mkdir("/tmp", 0755);

    ker::vfs::File* root_file = ker::vfs::vfs_open_file(ROOT_ONLY_FILE, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(root_file, nullptr);
    static_cast<void>(ker::vfs::vfs_close_file(root_file));

    KEXPECT_EQ(ker::vfs::vfs_mkdir(MOUNTPOINT, 0755), 0);
    KEXPECT_EQ(ker::vfs::mount_filesystem(MOUNTPOINT, "tmpfs", nullptr), 0);

    ker::vfs::File* inherited_tmp = ker::vfs::vfs_open_file(MOUNTED_TMP_DIR, 0, 0);
    KEXPECT_EQ(inherited_tmp, nullptr);
    if (inherited_tmp != nullptr) {
        static_cast<void>(ker::vfs::vfs_close_file(inherited_tmp));
    }

    ker::vfs::File* mount_file = ker::vfs::vfs_open_file(MOUNT_FILE, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(mount_file, nullptr);
    static_cast<void>(ker::vfs::vfs_close_file(mount_file));

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

KTEST(VFS, OpenFilePinsMountUntilBackendClose) {
    constexpr const char* MOUNTPOINT = "/tmp/ktest_mount_open_file";
    constexpr const char* FILE_PATH = "/tmp/ktest_mount_open_file/file";

    ker::vfs::vfs_mkdir("/tmp", 0755);
    ker::vfs::unmount_filesystem(MOUNTPOINT);
    ker::vfs::vfs_rmdir(MOUNTPOINT);
    KEXPECT_EQ(ker::vfs::vfs_mkdir(MOUNTPOINT, 0755), 0);
    KREQUIRE_EQ(ker::vfs::mount_filesystem(MOUNTPOINT, "tmpfs", nullptr), 0);
    KEXPECT_EQ(ker::vfs::mount_filesystem(MOUNTPOINT, "tmpfs", nullptr), -EBUSY);

    ker::vfs::File* file = ker::vfs::vfs_open_file(FILE_PATH, ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(file, nullptr);

    ker::vfs::MountRef mount_ref = ker::vfs::find_mount_point(FILE_PATH);
    KREQUIRE_TRUE(static_cast<bool>(mount_ref));
    ker::vfs::MountPoint* mount = mount_ref.get();
    uint32_t const MOUNT_DEV_ID = mount->dev_id;
    KEXPECT_EQ(file->mount_owner, mount);
    KEXPECT_EQ(ker::vfs::mount_point_open_file_count_for_test(mount), static_cast<uint32_t>(1));
    mount_ref.reset();

    KEXPECT_EQ(ker::vfs::unmount_filesystem(MOUNTPOINT), -EBUSY);
    ker::vfs::MountRef after_busy = ker::vfs::find_mount_point(FILE_PATH);
    KREQUIRE_TRUE(static_cast<bool>(after_busy));
    KEXPECT_EQ(after_busy->dev_id, MOUNT_DEV_ID);
    KEXPECT_FALSE(after_busy->retiring.load(std::memory_order_acquire));
    KEXPECT_EQ(ker::vfs::mount_point_open_file_count_for_test(after_busy.get()), static_cast<uint32_t>(1));
    after_busy.reset();

    KEXPECT_EQ(ker::vfs::vfs_close_file(file), 0);
    ker::vfs::MountRef after_close = ker::vfs::find_mount_point(MOUNTPOINT);
    KREQUIRE_TRUE(static_cast<bool>(after_close));
    KEXPECT_EQ(ker::vfs::mount_point_open_file_count_for_test(after_close.get()), static_cast<uint32_t>(0));
    after_close.reset();

    KEXPECT_EQ(ker::vfs::unmount_filesystem(MOUNTPOINT), 0);
    ker::vfs::vfs_rmdir(MOUNTPOINT);
}

KTEST(VFS, MountLookupCacheExactPathRetainsAndInvalidates) {
    constexpr const char* MOUNTPOINT = "/tmp/ktest_mount_lookup_cache";
    constexpr const char* CHILD = "/tmp/ktest_mount_lookup_cache/child";
    constexpr size_t CHILD_LEN = sizeof("/tmp/ktest_mount_lookup_cache/child") - 1;

    ker::vfs::vfs_mkdir("/tmp", 0755);
    ker::vfs::unmount_filesystem(MOUNTPOINT);
    ker::vfs::vfs_rmdir(MOUNTPOINT);
    KEXPECT_EQ(ker::vfs::vfs_mkdir(MOUNTPOINT, 0755), 0);
    KREQUIRE_EQ(ker::vfs::mount_filesystem(MOUNTPOINT, "tmpfs", nullptr), 0);

    ker::vfs::mount_lookup_cache_reset_for_test();

    ker::vfs::MountRef first = ker::vfs::find_mount_point(CHILD, CHILD_LEN);
    KREQUIRE_TRUE(static_cast<bool>(first));
    KEXPECT_EQ(first->fs_type, ker::vfs::FSType::TMPFS);
    uint32_t const mounted_dev_id = first->dev_id;
    KEXPECT_EQ(ker::vfs::mount_lookup_cache_hits_for_test(), static_cast<uint64_t>(0));
    first.reset();

    ker::vfs::MountRef second = ker::vfs::find_mount_point(CHILD, CHILD_LEN);
    KREQUIRE_TRUE(static_cast<bool>(second));
    KEXPECT_EQ(second->dev_id, mounted_dev_id);
    KEXPECT_EQ(ker::vfs::mount_point_ref_count_for_test(second.get()), static_cast<uint32_t>(1));
    KEXPECT_TRUE(ker::vfs::mount_lookup_cache_hits_for_test() >= static_cast<uint64_t>(1));
    second.reset();

    uint64_t const hits_after_cached_lookup = ker::vfs::mount_lookup_cache_hits_for_test();
    KEXPECT_EQ(ker::vfs::unmount_filesystem(MOUNTPOINT), 0);

    ker::vfs::MountRef after_unmount = ker::vfs::find_mount_point(CHILD, CHILD_LEN);
    KREQUIRE_TRUE(static_cast<bool>(after_unmount));
    KEXPECT_NE(after_unmount->dev_id, mounted_dev_id);
    KEXPECT_EQ(ker::vfs::mount_lookup_cache_hits_for_test(), hits_after_cached_lookup);
    after_unmount.reset();

    ker::vfs::mount_lookup_cache_reset_for_test();
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
    static_cast<void>(ker::vfs::vfs_close_file(f));

    ker::vfs::File* rf = ker::vfs::vfs_open_file("/tmp/ktest_append", 0, 0);
    KREQUIRE_NE(rf, nullptr);

    char rbuf[32] = {};
    ssize_t const NR = ker::vfs::tmpfs::tmpfs_read(rf, static_cast<void*>(rbuf), L1 + L2, 0);
    KEXPECT_EQ(NR, static_cast<ssize_t>(L1 + L2));
    static_cast<void>(ker::vfs::vfs_close_file(rf));

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

    static_cast<void>(ker::vfs::vfs_close_file(second));
    static_cast<void>(ker::vfs::vfs_close_file(first));
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

    static_cast<void>(ker::vfs::vfs_close_file(f));
    ker::vfs::vfs_unlink("/tmp/ktest_aligned4k");
}
