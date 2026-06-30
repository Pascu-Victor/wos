#include "testd.hpp"

// ---------------------------------------------------------------------------
// B4: Memory management
// ---------------------------------------------------------------------------

TESTD_RUN(test_mmap_anon) {
    constexpr size_t SIZE = 4096;
    void* ptr = mmap(nullptr, SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) {
        fail("mmap_anon", "mmap failed");
        return;
    }
    constexpr uint8_t PATTERN_MASK = 0xFF;
    // Write and read back a pattern
    auto* p = static_cast<uint8_t*>(ptr);
    for (size_t i = 0; i < SIZE; ++i) {
        p[i] = static_cast<uint8_t>(i & PATTERN_MASK);
    }
    bool ok = true;
    for (size_t i = 0; i < SIZE; ++i) {
        if (p[i] != static_cast<uint8_t>(i & PATTERN_MASK)) {
            ok = false;
            break;
        }
    }

    munmap(ptr, SIZE);

    if (!ok) {
        fail("mmap_anon_verify", "pattern mismatch");
        return;
    }
    TESTD_PASS("mmap_anon");
}
TESTD_RUN_END(test_mmap_anon)

// Verify write-then-read on a regular file (no mmap).
// If this fails the filesystem write/read path is broken independent of mmap.
TESTD_RUN(test_file_write_read) {
    const char* path = "/tmp/testd_filewr.bin";
    constexpr size_t SIZE = 4096;

    int const FD = open(path, O_CREAT | O_RDWR | O_TRUNC, MODE_0644);
    if (FD < 0) {
        fail("file_write_read_open", "open failed");
        return;
    }

    constexpr uint8_t PATTERN_MASK = 0xFF;
    std::array<char, SIZE> data{};
    for (size_t i = 0; i < SIZE; ++i) {
        data[i] = static_cast<char>(i & PATTERN_MASK);
    }
    ssize_t const NW = write(FD, data.data(), data.size());
    if (std::cmp_not_equal(NW, SIZE)) {
        close(FD);
        unlink(path);
        fail("file_write_read_write", "short write");
        return;
    }

    lseek(FD, 0, SEEK_SET);

    std::array<char, SIZE> rbuf{};
    ssize_t const NR = read(FD, rbuf.data(), rbuf.size());
    close(FD);
    unlink(path);

    if (std::cmp_not_equal(NR, SIZE)) {
        fail("file_write_read_count", "short read");
        return;
    }
    bool ok = true;
    for (size_t i = 0; i < SIZE; ++i) {
        if (rbuf[i] != static_cast<char>(i & PATTERN_MASK)) {
            ok = false;
            break;
        }
    }
    if (!ok) {
        fail("file_write_read_data", "data mismatch");
        return;
    }
    TESTD_PASS("file_write_read");
}
TESTD_RUN_END(test_file_write_read)

TESTD_RUN(test_mmap_file) {
    const char* path = "/tmp/testd_mmap.bin";

    // Write known data
    int const FD = open(path, O_CREAT | O_RDWR | O_TRUNC, MODE_0644);
    if (FD < 0) {
        fail("mmap_file_create", "open failed");
        return;
    }

    constexpr size_t SIZE = 4096;
    constexpr uint8_t PATTERN_MASK = 0xFF;
    std::array<char, SIZE> data{};
    for (size_t i = 0; i < SIZE; ++i) {
        data[i] = static_cast<char>(i & PATTERN_MASK);
    }
    write(FD, data.data(), data.size());

    // Map file read-only
    void* ptr = mmap(nullptr, SIZE, PROT_READ, MAP_PRIVATE, FD, 0);
    close(FD);
    unlink(path);

    if (ptr == MAP_FAILED) {
        fail("mmap_file", "mmap failed");
        return;
    }

    bool ok = true;
    auto* p = static_cast<uint8_t*>(ptr);
    for (size_t i = 0; i < SIZE; ++i) {
        if (p[i] != static_cast<uint8_t>(i & PATTERN_MASK)) {
            ok = false;
            break;
        }
    }
    munmap(ptr, SIZE);

    if (!ok) {
        fail("mmap_file_verify", "content mismatch");
        return;
    }
    TESTD_PASS("mmap_file");
}
TESTD_RUN_END(test_mmap_file)
