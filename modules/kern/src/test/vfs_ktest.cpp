#include <cstddef>
#include <cstdint>
#include <cstring>
#include <test/ktest.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

// vfs_open() requires get_current_task() != nullptr (it allocates a fd in the
// task's fd table), unavailable at selftest time.  Use vfs_open_file() which
// returns a File* without touching the task fd table, and call tmpfs helpers
// directly for read/write.  vfs_mkdir / vfs_stat / vfs_unlink work without a
// current task.
//
// All test paths are under /tmp/ktest_* to avoid colliding with real content.

KTEST(VFS, CreateAndStat) {
    ker::vfs::vfs_mkdir("/tmp", 0755);  // idempotent if /tmp already exists

    ker::vfs::File* f = ker::vfs::vfs_open_file("/tmp/ktest_create", ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(f, nullptr);
    ker::vfs::tmpfs::tmpfs_fops_close(f);

    ker::vfs::stat st{};
    KEXPECT_EQ(ker::vfs::vfs_stat("/tmp/ktest_create", &st), 0);
    KEXPECT_TRUE((st.st_mode & ker::vfs::S_IFREG) != 0U);

    ker::vfs::vfs_unlink("/tmp/ktest_create");
}

KTEST(VFS, WriteRead) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    ker::vfs::File* wf = ker::vfs::vfs_open_file("/tmp/ktest_wr", ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(wf, nullptr);

    uint8_t wbuf[128];
    for (int i = 0; i < 128; ++i) {
        wbuf[i] = static_cast<uint8_t>(i);
    }
    ssize_t nw = ker::vfs::tmpfs::tmpfs_write(wf, static_cast<const void*>(wbuf), 128, 0);
    KEXPECT_EQ(nw, static_cast<ssize_t>(128));
    ker::vfs::tmpfs::tmpfs_fops_close(wf);

    ker::vfs::File* rf = ker::vfs::vfs_open_file("/tmp/ktest_wr", 0, 0);
    KREQUIRE_NE(rf, nullptr);

    uint8_t rbuf[128] = {};
    ssize_t nr = ker::vfs::tmpfs::tmpfs_read(rf, static_cast<void*>(rbuf), 128, 0);
    KEXPECT_EQ(nr, static_cast<ssize_t>(128));
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

KTEST(VFS, Mkdir) {
    KEXPECT_EQ(ker::vfs::vfs_mkdir("/tmp/ktest_dir", 0755), 0);

    ker::vfs::stat st{};
    KEXPECT_EQ(ker::vfs::vfs_stat("/tmp/ktest_dir", &st), 0);
    KEXPECT_TRUE((st.st_mode & ker::vfs::S_IFDIR) != 0U);
}

KTEST(VFS, AppendMode) {
    ker::vfs::vfs_mkdir("/tmp", 0755);

    ker::vfs::File* f = ker::vfs::vfs_open_file("/tmp/ktest_append", ker::vfs::O_CREAT | 1, 0644);
    KREQUIRE_NE(f, nullptr);

    constexpr char CHUNK1[] = "Hello, ";
    constexpr char CHUNK2[] = "World!";
    constexpr size_t L1 = sizeof(CHUNK1) - 1;
    constexpr size_t L2 = sizeof(CHUNK2) - 1;

    ssize_t nw1 = ker::vfs::tmpfs::tmpfs_write(f, static_cast<const void*>(CHUNK1), L1, 0);
    KEXPECT_EQ(nw1, static_cast<ssize_t>(L1));
    ssize_t nw2 = ker::vfs::tmpfs::tmpfs_write(f, static_cast<const void*>(CHUNK2), L2, L1);
    KEXPECT_EQ(nw2, static_cast<ssize_t>(L2));
    ker::vfs::tmpfs::tmpfs_fops_close(f);

    ker::vfs::File* rf = ker::vfs::vfs_open_file("/tmp/ktest_append", 0, 0);
    KREQUIRE_NE(rf, nullptr);

    char rbuf[32] = {};
    ssize_t nr = ker::vfs::tmpfs::tmpfs_read(rf, static_cast<void*>(rbuf), L1 + L2, 0);
    KEXPECT_EQ(nr, static_cast<ssize_t>(L1 + L2));
    ker::vfs::tmpfs::tmpfs_fops_close(rf);

    KEXPECT_TRUE(memcmp(static_cast<const void*>(rbuf), "Hello, World!", L1 + L2) == 0);

    ker::vfs::vfs_unlink("/tmp/ktest_append");
}

KTEST(VFS, WriteReadAligned4K) {
    // Write exactly 4 KB then read it back from the same file.
    // Exercises the aligned write→read path (same as mmap_file in testd):
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
    ssize_t nw = f->fops->vfs_write(f, static_cast<const void*>(wbuf), SIZE, 0);
    KEXPECT_EQ(nw, static_cast<ssize_t>(SIZE));

    uint8_t rbuf[SIZE] = {};
    ssize_t nr = f->fops->vfs_read(f, static_cast<void*>(rbuf), SIZE, 0);
    KEXPECT_EQ(nr, static_cast<ssize_t>(SIZE));

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
