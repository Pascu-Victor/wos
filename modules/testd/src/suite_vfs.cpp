#include "testd.hpp"

// ---------------------------------------------------------------------------
// B2: VFS syscall coverage
// ---------------------------------------------------------------------------

TESTD_RUN(test_vfs_open_write_read_close) {
    const char* path = "/tmp/testd_rw.txt";

    int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("vfs_open_write", "open failed");
        return;
    }

    std::string_view const PAYLOAD = "hello testd\n";
    ssize_t const NW = write(fd, PAYLOAD.data(), PAYLOAD.size());
    if (std::cmp_not_equal(NW, PAYLOAD.size())) {
        close(fd);
        fail("vfs_write", "short write");
        return;
    }
    close(fd);
    TESTD_PASS("vfs_open_write");

    fd = open(path, O_RDONLY);
    if (fd < 0) {
        fail("vfs_open_read", "open failed");
        return;
    }

    std::array<char, 64> rbuf{};
    ssize_t const NR = read(fd, rbuf.data(), rbuf.size());
    close(fd);

    if (NR != NW || std::string_view(rbuf.data(), static_cast<size_t>(NR)) != PAYLOAD) {
        fail("vfs_read_verify", "data mismatch");
        return;
    }
    TESTD_PASS("vfs_open_read");
    TESTD_PASS("vfs_read_verify");
}
TESTD_RUN_END(test_vfs_open_write_read_close)

TESTD_RUN(test_vfs_stat) {
    const char* path = "/tmp/testd_stat.txt";
    int const FD = open(path, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (FD < 0) {
        fail("vfs_stat_create", "open failed");
        return;
    }
    write(FD, "x", 1);
    close(FD);

    struct stat st{};
    if (stat(path, &st) != 0) {
        fail("vfs_stat", "stat failed");
        return;
    }
    if (!S_ISREG(st.st_mode)) {
        fail("vfs_stat_mode", "not a regular file");
        return;
    }
    if (st.st_size != 1) {
        fail("vfs_stat_size", "wrong size");
        return;
    }
    TESTD_PASS("vfs_stat");

    if (fstat(FD, &st) == 0) {
        // fd is closed - fstat on closed fd should fail
        fail("vfs_fstat_closed", "expected error on closed fd");
    } else {
        TESTD_PASS("vfs_fstat_closed_fd");
    }
    unlink(path);
}
TESTD_RUN_END(test_vfs_stat)

TESTD_RUN(test_vfs_lseek) {
    const char* path = "/tmp/testd_seek.txt";
    int const FD = open(path, O_CREAT | O_RDWR | O_TRUNC, MODE_0644);
    if (FD < 0) {
        fail("vfs_lseek_open", "open failed");
        return;
    }
    std::string_view const PAYLOAD = "ABCDEF";
    write(FD, PAYLOAD.data(), PAYLOAD.size());

    off_t pos = lseek(FD, 2, SEEK_SET);
    if (pos != 2) {
        close(FD);
        unlink(path);
        fail("vfs_lseek_set", "wrong position");
        return;
    }

    char c = 0;
    read(FD, &c, 1);
    if (c != 'C') {
        close(FD);
        unlink(path);
        fail("vfs_lseek_verify", "wrong byte after seek");
        return;
    }

    pos = lseek(FD, 0, SEEK_END);
    if (std::cmp_not_equal(pos, PAYLOAD.size())) {
        close(FD);
        unlink(path);
        fail("vfs_lseek_end", "wrong end position");
        return;
    }

    close(FD);
    unlink(path);
    TESTD_PASS("vfs_lseek");
}
TESTD_RUN_END(test_vfs_lseek)

TESTD_RUN(test_vfs_mkdir_rmdir) {
    const char* dir = "/tmp/testd_dir";
    if (mkdir(dir, MODE_0755) != 0) {
        fail("vfs_mkdir", "mkdir failed");
        return;
    }

    struct stat st{};
    if (stat(dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
        rmdir(dir);
        fail("vfs_mkdir_stat", "not a directory");
        return;
    }
    TESTD_PASS("vfs_mkdir");

    if (rmdir(dir) != 0) {
        fail("vfs_rmdir", "rmdir failed");
        return;
    }
    TESTD_PASS("vfs_rmdir");
}
TESTD_RUN_END(test_vfs_mkdir_rmdir)

TESTD_RUN(test_vfs_unlink_rename) {
    const char* src = "/tmp/testd_src.txt";
    const char* dst = "/tmp/testd_dst.txt";

    int const FD = open(src, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (FD < 0) {
        fail("vfs_rename_create", "open failed");
        return;
    }
    std::string_view const PAYLOAD = "rename test";
    write(FD, PAYLOAD.data(), PAYLOAD.size());
    close(FD);

    if (rename(src, dst) != 0) {
        unlink(src);
        fail("vfs_rename", "rename failed");
        return;
    }
    TESTD_PASS("vfs_rename");

    struct stat st{};
    if (stat(src, &st) == 0) {
        unlink(dst);
        fail("vfs_rename_src_gone", "src still exists");
        return;
    }
    if (stat(dst, &st) != 0) {
        fail("vfs_rename_dst_exists", "dst missing");
        return;
    }
    TESTD_PASS("vfs_rename_src_gone");

    if (unlink(dst) != 0) {
        fail("vfs_unlink", "unlink failed");
        return;
    }
    TESTD_PASS("vfs_unlink");
}
TESTD_RUN_END(test_vfs_unlink_rename)

TESTD_RUN(test_vfs_rename_cross_mount_exdev) {
    const char* mountpoint = "/tmp/testd_rename_devfs_mount";
    const char* src = "/tmp/testd_rename_cross_src.txt";
    const char* dst = "/tmp/testd_rename_devfs_mount/cross_dst";

    ker::abi::vfs::umount(mountpoint);
    rmdir(mountpoint);
    unlink(src);

    int fd = open(src, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("vfs_rename_cross_create", "open source failed");
        return;
    }
    close(fd);

    if (mkdir(mountpoint, MODE_0755) != 0) {
        unlink(src);
        fail("vfs_rename_cross_mkdir", "mkdir mountpoint failed");
        return;
    }

    int const MOUNT_RET = ker::abi::vfs::mount(nullptr, mountpoint, "devfs");
    if (MOUNT_RET != 0) {
        rmdir(mountpoint);
        unlink(src);
        fail("vfs_rename_cross_mount", "devfs mount failed");
        return;
    }

    errno = 0;
    if (rename(src, dst) == 0 || errno != EXDEV) {
        int const SAVED_ERRNO = errno;
        ker::abi::vfs::umount(mountpoint);
        rmdir(mountpoint);
        unlink(src);
        errno = SAVED_ERRNO;
        fail("vfs_rename_cross_exdev", "expected EXDEV");
        return;
    }
    TESTD_PASS("vfs_rename_cross_exdev");

    struct stat st{};
    if (stat(src, &st) != 0) {
        ker::abi::vfs::umount(mountpoint);
        rmdir(mountpoint);
        fail("vfs_rename_cross_src_preserved", "source missing after EXDEV");
        return;
    }
    TESTD_PASS("vfs_rename_cross_src_preserved");

    ker::abi::vfs::umount(mountpoint);
    rmdir(mountpoint);
    unlink(src);
}
TESTD_RUN_END(test_vfs_rename_cross_mount_exdev)

TESTD_RUN(test_vfs_lstat_symlink) {
    const char* target = "/tmp/testd_lstat_target";
    const char* link = "/tmp/testd_lstat_link";
    const char* link_slash = "/tmp/testd_lstat_link/";

    unlink(link);
    rmdir(target);
    if (mkdir(target, MODE_0755) != 0) {
        fail("vfs_lstat_mkdir", "mkdir target failed");
        return;
    }
    if (symlink(target, link) != 0) {
        rmdir(target);
        fail("vfs_lstat_symlink_create", "symlink failed");
        return;
    }

    struct stat st{};
    if (lstat(link, &st) != 0 || !S_ISLNK(st.st_mode)) {
        unlink(link);
        rmdir(target);
        fail("vfs_lstat_symlink", "lstat did not report symlink");
        return;
    }
    if (stat(link, &st) != 0 || !S_ISDIR(st.st_mode)) {
        unlink(link);
        rmdir(target);
        fail("vfs_lstat_stat_follow", "stat did not follow symlink");
        return;
    }
    if (lstat(link_slash, &st) != 0 || !S_ISDIR(st.st_mode)) {
        unlink(link);
        rmdir(target);
        fail("vfs_lstat_trailing_slash", "lstat with trailing slash did not follow directory symlink");
        return;
    }
    std::array<char, 128> link_buf{};
    errno = 0;
    if (readlink(link_slash, link_buf.data(), link_buf.size()) >= 0 || errno != EINVAL) {
        int const SAVED_ERRNO = errno;
        unlink(link);
        rmdir(target);
        errno = SAVED_ERRNO;
        fail("vfs_readlink_trailing_slash", "readlink with trailing slash returned symlink target");
        return;
    }
    if (rmdir(target) != 0) {
        unlink(link);
        fail("vfs_lstat_target_remove", "rmdir target failed");
        return;
    }
    if (lstat(link, &st) != 0 || !S_ISLNK(st.st_mode)) {
        unlink(link);
        fail("vfs_lstat_dangling", "lstat failed on dangling symlink");
        return;
    }

    unlink(link);
    TESTD_PASS("vfs_lstat_symlink");
}
TESTD_RUN_END(test_vfs_lstat_symlink)

TESTD_RUN(test_vfs_shell_fsops_shape) {
    const char* base = "/tmp/testd_fsops_shape";
    const char* d1 = "/tmp/testd_fsops_shape/d1";
    const char* d2 = "/tmp/testd_fsops_shape/d1/d2";
    const char* src = "/tmp/testd_fsops_shape/f1_cp";
    const char* dst = "/tmp/testd_fsops_shape/f1_mv";
    constexpr std::array<const char*, 7> FILES = {
        "/tmp/testd_fsops_shape/f1", "/tmp/testd_fsops_shape/f2",        "/tmp/testd_fsops_shape/f3",  "/tmp/testd_fsops_shape/f4",
        "/tmp/testd_fsops_shape/f5", "/tmp/testd_fsops_shape/stdin_src", "/tmp/testd_fsops_shape/cap",
    };

    unlink(dst);
    unlink(src);
    for (const char* path : FILES) {
        unlink(path);
    }
    rmdir(d2);
    rmdir(d1);
    rmdir(base);

    if (mkdir(base, MODE_0755) != 0) {
        fail("vfs_shell_fsops_base", "mkdir base failed");
        return;
    }

    for (const char* path : FILES) {
        int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
        if (fd < 0) {
            fail("vfs_shell_fsops_seed", "open seed file failed");
            return;
        }
        close(fd);
    }

    if (mkdir(d1, MODE_0755) != 0 || mkdir(d2, MODE_0755) != 0) {
        fail("vfs_shell_nested_mkdir", "nested mkdir failed");
        return;
    }

    struct stat st{};
    if (stat(d2, &st) != 0 || !S_ISDIR(st.st_mode)) {
        fail("vfs_shell_nested_mkdir_stat", "nested directory missing");
        return;
    }
    TESTD_PASS("vfs_shell_nested_mkdir_stat");

    int fd = open(src, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("vfs_shell_rename_create", "open source failed");
        return;
    }
    close(fd);

    if (rename(src, dst) != 0) {
        fail("vfs_shell_rename", "rename failed");
        return;
    }
    if (stat(dst, &st) != 0 || !S_ISREG(st.st_mode)) {
        fail("vfs_shell_rename_dst", "destination missing");
        return;
    }
    if (stat(src, &st) == 0) {
        fail("vfs_shell_rename_src", "source still exists");
        return;
    }
    TESTD_PASS("vfs_shell_rename");

    unlink(dst);
    for (const char* path : FILES) {
        unlink(path);
    }
    rmdir(d2);
    rmdir(d1);
    rmdir(base);
}
TESTD_RUN_END(test_vfs_shell_fsops_shape)

TESTD_RUN(test_vfs_dup) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("vfs_dup_pipe", "pipe failed");
        return;
    }

    int const FD_DUP = dup(fds[1]);
    if (FD_DUP < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("vfs_dup", "dup failed");
        return;
    }

    write(fds[1], "A", 1);
    write(FD_DUP, "B", 1);
    close(fds[1]);
    close(FD_DUP);

    std::array<char, 4> buf{};
    ssize_t const NR = read_expected_bytes_timeout(fds[0], buf.data(), 2, REMOTE_IPC_TIMEOUT_MS);
    close(fds[0]);

    if (NR != 2 || buf[0] != 'A' || buf[1] != 'B') {
        fail("vfs_dup_verify", "data mismatch");
        return;
    }
    TESTD_PASS("vfs_dup");
}
TESTD_RUN_END(test_vfs_dup)

TESTD_RUN(test_vfs_dup2) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("vfs_dup2_pipe", "pipe failed");
        return;
    }

    // dup write-end to a specific fd number
    const int TARGET = 50;
    if (dup2(fds[1], TARGET) != TARGET) {
        close(fds[0]);
        close(fds[1]);
        fail("vfs_dup2", "dup2 failed");
        return;
    }
    close(fds[1]);

    write(TARGET, "XY", 2);
    close(TARGET);

    std::array<char, 4> buf{};
    ssize_t const NR = read_expected_bytes_timeout(fds[0], buf.data(), 2, REMOTE_IPC_TIMEOUT_MS);
    close(fds[0]);

    if (NR != 2 || buf[0] != 'X' || buf[1] != 'Y') {
        fail("vfs_dup2_verify", "data mismatch");
        return;
    }
    TESTD_PASS("vfs_dup2");
}
TESTD_RUN_END(test_vfs_dup2)

TESTD_RUN(test_vfs_readdir) {
    // /tmp should exist and be readable
    DIR* dir = opendir("/tmp");
    if (dir == nullptr) {
        fail("vfs_readdir_open", "opendir /tmp failed");
        return;
    }

    int count = 0;
    while (readdir(dir) != nullptr) {
        count++;
    }
    closedir(dir);

    // /tmp has at least . and ..
    if (count < 2) {
        fail("vfs_readdir_count", "too few entries");
        return;
    }
    TESTD_PASS("vfs_readdir");
}
TESTD_RUN_END(test_vfs_readdir)

bool run_readdir_unlink_progress_case(const char* dir_path, const char* fail_name) {
    for (int i = 0; i < 8; ++i) {
        std::array<char, 64> path{};
        (void)testd_format_to_array(path, "%s/f%d", dir_path, i);
        unlink(path.data());
    }
    rmdir(dir_path);
    if (mkdir(dir_path, MODE_0755) != 0) {
        fail(fail_name, "mkdir failed");
        return false;
    }

    for (int i = 0; i < 8; ++i) {
        std::array<char, 64> path{};
        (void)testd_format_to_array(path, "%s/f%d", dir_path, i);
        int fd = open(path.data(), O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
        if (fd < 0) {
            fail(fail_name, "open seed failed");
            return false;
        }
        close(fd);
    }

    DIR* dir = opendir(dir_path);
    if (dir == nullptr) {
        fail(fail_name, "opendir failed");
        return false;
    }

    struct dirent* ent = nullptr;
    while ((ent = readdir(dir)) != nullptr) {
        bool const IS_DOT = ent->d_name[0] == '.' && ent->d_name[1] == '\0';
        bool const IS_DOTDOT = ent->d_name[0] == '.' && ent->d_name[1] == '.' && ent->d_name[2] == '\0';
        if (IS_DOT || IS_DOTDOT) {
            continue;
        }
        std::array<char, 64> path{};
        (void)testd_format_to_array(path, "%s/%s", dir_path, ent->d_name);
        if (unlink(path.data()) != 0) {
            closedir(dir);
            fail(fail_name, "unlink during readdir failed");
            return false;
        }
    }
    closedir(dir);

    if (rmdir(dir_path) != 0) {
        fail(fail_name, "directory not empty after streamed unlink");
        return false;
    }
    return true;
}

TESTD_RUN(test_vfs_readdir_unlink_progress) {
    if (!run_readdir_unlink_progress_case("/tmp/testd_readdir_unlink", "vfs_readdir_unlink_progress")) {
        return;
    }
    TESTD_PASS("vfs_readdir_unlink_progress");
}
TESTD_RUN_END(test_vfs_readdir_unlink_progress)

TESTD_RUN(test_vfs_readdir_unlink_progress_rootfs) {
    if (!run_readdir_unlink_progress_case("/testd_readdir_unlink_rootfs", "vfs_readdir_unlink_progress_rootfs")) {
        return;
    }
    TESTD_PASS("vfs_readdir_unlink_progress_rootfs");
}
TESTD_RUN_END(test_vfs_readdir_unlink_progress_rootfs)

TESTD_RUN(test_vfs_directory_requirements) {
    const char* file = "/tmp/testd_not_dir.txt";
    unlink(file);

    int fd = open(file, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("vfs_dirreq_create", "open failed");
        return;
    }
    close(fd);

    errno = 0;
    DIR* dir = opendir(file);
    if (dir != nullptr) {
        closedir(dir);
        unlink(file);
        fail("vfs_opendir_regular_file", "opendir regular file succeeded");
        return;
    }
    if (errno != ENOTDIR) {
        int const SAVED_ERRNO = errno;
        unlink(file);
        errno = SAVED_ERRNO;
        fail("vfs_opendir_regular_file_errno", "expected ENOTDIR");
        return;
    }
    TESTD_PASS("vfs_opendir_regular_file");

    struct stat st{};
    errno = 0;
    if (stat("/tmp/testd_not_dir.txt/", &st) == 0 || errno != ENOTDIR) {
        int const SAVED_ERRNO = errno;
        unlink(file);
        errno = SAVED_ERRNO;
        fail("vfs_stat_trailing_slash", "expected ENOTDIR");
        return;
    }
    TESTD_PASS("vfs_stat_trailing_slash");

    unlink(file);
}
TESTD_RUN_END(test_vfs_directory_requirements)

TESTD_RUN(test_vfs_rename_file_parent_enotdir) {
    const char* src = "/tmp/testd_rename_src.txt";
    const char* parent_file = "/tmp/testd_rename_parent.txt";
    const char* nested_dst = "/tmp/testd_rename_parent.txt/src.txt";
    unlink(src);
    unlink(parent_file);

    int fd = open(src, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("vfs_rename_file_parent_create_src", "open src failed");
        return;
    }
    close(fd);

    fd = open(parent_file, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (fd < 0) {
        unlink(src);
        fail("vfs_rename_file_parent_create_parent", "open parent failed");
        return;
    }
    close(fd);

    errno = 0;
    if (rename(src, "/tmp/testd_rename_parent.txt/") == 0 || errno != ENOTDIR) {
        int const SAVED_ERRNO = errno;
        unlink(src);
        unlink(parent_file);
        errno = SAVED_ERRNO;
        fail("vfs_rename_trailing_slash_file", "expected ENOTDIR");
        return;
    }
    TESTD_PASS("vfs_rename_trailing_slash_file");

    errno = 0;
    if (rename(src, nested_dst) == 0 || errno != ENOTDIR) {
        int const SAVED_ERRNO = errno;
        unlink(src);
        unlink(parent_file);
        errno = SAVED_ERRNO;
        fail("vfs_rename_file_parent", "expected ENOTDIR");
        return;
    }

    struct stat st{};
    if (stat(src, &st) != 0 || !S_ISREG(st.st_mode)) {
        unlink(src);
        unlink(parent_file);
        fail("vfs_rename_file_parent_src", "source was not preserved");
        return;
    }
    if (stat(parent_file, &st) != 0 || !S_ISREG(st.st_mode)) {
        unlink(src);
        unlink(parent_file);
        fail("vfs_rename_file_parent_parent", "parent file was not preserved");
        return;
    }

    unlink(src);
    unlink(parent_file);
    TESTD_PASS("vfs_rename_file_parent");
}
TESTD_RUN_END(test_vfs_rename_file_parent_enotdir)

TESTD_RUN(test_vfs_access) {
    int const FD = open("/tmp/testd_access.txt", O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (FD < 0) {
        fail("vfs_access_create", "open failed");
        return;
    }
    close(FD);

    if (access("/tmp/testd_access.txt", F_OK) != 0) {
        unlink("/tmp/testd_access.txt");
        fail("vfs_access_exists", "F_OK failed");
        return;
    }
    if (access("/tmp/testd_access.txt", R_OK | W_OK) != 0) {
        unlink("/tmp/testd_access.txt");
        fail("vfs_access_rw", "R_OK|W_OK failed");
        return;
    }
    // Non-existent path must fail
    if (access("/tmp/testd_nonexistent_xyz", F_OK) == 0) {
        unlink("/tmp/testd_access.txt");
        fail("vfs_access_noexist", "should have failed");
        return;
    }
    unlink("/tmp/testd_access.txt");
    TESTD_PASS("vfs_access");
}
TESTD_RUN_END(test_vfs_access)
