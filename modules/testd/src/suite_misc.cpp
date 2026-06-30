#include "testd.hpp"

// ---------------------------------------------------------------------------
// B2: getpid / getppid
// ---------------------------------------------------------------------------

TESTD_RUN(test_getpid_getppid) {
    pid_t const MYPID = getpid();
    if (MYPID <= 0) {
        fail("getpid", "getpid returned bad value");
        return;
    }
    TESTD_PASS("getpid");

    pid_t const PPID = getppid();
    if (PPID <= 0) {
        fail("getppid", "getppid returned bad value");
        return;
    }
    TESTD_PASS("getppid");
}
TESTD_RUN_END(test_getpid_getppid)

// ---------------------------------------------------------------------------
// B2: getcwd / chdir
// ---------------------------------------------------------------------------

TESTD_RUN(test_getcwd_chdir) {
    std::array<char, PATH_MAX> cwd{};
    if (getcwd(cwd.data(), cwd.size()) == nullptr) {
        fail("getcwd", "getcwd failed");
        return;
    }
    TESTD_PASS("getcwd");

    if (chdir("/tmp") != 0) {
        fail("chdir", "chdir /tmp failed");
        return;
    }

    std::array<char, PATH_MAX> cwd2{};
    if (getcwd(cwd2.data(), cwd2.size()) == nullptr) {
        chdir(cwd.data());
        fail("getcwd_after_chdir", "failed");
        return;
    }
    if (strncmp(cwd2.data(), "/tmp", 4) != 0) {
        chdir(cwd.data());
        fail("chdir_verify", "cwd not /tmp");
        return;
    }
    chdir(cwd.data());  // restore
    TESTD_PASS("chdir");
}
TESTD_RUN_END(test_getcwd_chdir)

// ---------------------------------------------------------------------------
// B2: chmod / fchmod via stat verification
// ---------------------------------------------------------------------------

TESTD_RUN(test_chmod) {
    const char* path = "/tmp/testd_chmod.txt";
    int const FD = open(path, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (FD < 0) {
        fail("chmod_create", "open failed");
        return;
    }
    close(FD);

    if (chmod(path, MODE_0600) != 0) {
        unlink(path);
        fail("chmod", "chmod failed");
        return;
    }

    struct stat st{};
    if (stat(path, &st) != 0) {
        unlink(path);
        fail("chmod_stat", "stat failed");
        return;
    }
    if ((st.st_mode & MODE_MASK) != MODE_0600) {
        unlink(path);
        fail("chmod_verify", "mode not 0600");
        return;
    }
    unlink(path);
    TESTD_PASS("chmod");
}
TESTD_RUN_END(test_chmod)

// ---------------------------------------------------------------------------
// B2: truncate / ftruncate
// ---------------------------------------------------------------------------

TESTD_RUN(test_truncate) {
    const char* path = "/tmp/testd_trunc.txt";
    int const FD = open(path, O_CREAT | O_RDWR | O_TRUNC, MODE_0644);
    if (FD < 0) {
        fail("truncate_create", "open failed");
        return;
    }
    std::string_view const PAYLOAD = "0123456789ABCDE";
    write(FD, PAYLOAD.data(), PAYLOAD.size());
    close(FD);

    if (truncate(path, static_cast<off_t>(PAYLOAD.size() / 2)) != 0) {
        unlink(path);
        fail("truncate", "truncate failed");
        return;
    }

    struct stat st{};
    stat(path, &st);
    if (std::cmp_not_equal(st.st_size, PAYLOAD.size() / 2)) {
        unlink(path);
        fail("truncate_size", "wrong size");
        return;
    }

    unlink(path);
    TESTD_PASS("truncate");
}

TESTD_RUN_END(test_truncate)
