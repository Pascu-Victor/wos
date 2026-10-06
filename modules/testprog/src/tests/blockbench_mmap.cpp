#include "blockbench_mmap.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <limits>

namespace {
constexpr size_t PAGE_BYTES = 4096;
constexpr size_t DEFAULT_BYTES = 1024 * 1024;
constexpr size_t MAX_BYTES = 16 * 1024 * 1024;

auto number(const char* text, uint64_t& result) -> bool {
    if (text == nullptr || *text == '\0' || *text == '-' || *text == '+') return false;
    errno = 0;
    char* end = nullptr;
    auto const VALUE = std::strtoull(text, &end, 0);
    if (errno != 0 || end == text || *end != '\0') return false;
    result = VALUE;
    return true;
}

struct Mapping {
    void* data{MAP_FAILED};
    size_t bytes{};
    ~Mapping() {
        if (data != MAP_FAILED) static_cast<void>(munmap(data, bytes));
    }
    auto release() -> bool {
        if (munmap(data, bytes) != 0) return false;
        data = MAP_FAILED;
        return true;
    }
};

auto map_closed_file(const char* path, size_t bytes, int protection, Mapping& mapping) -> bool {
    int const FD = open(path, O_RDONLY);
    if (FD < 0) return false;
    struct stat metadata{};
    if (fstat(FD, &metadata) != 0 || !S_ISREG(metadata.st_mode) || metadata.st_size < 0 ||
        static_cast<uint64_t>(metadata.st_size) < bytes) {
        int const ERROR = errno != 0 ? errno : EINVAL;
        static_cast<void>(close(FD));
        errno = ERROR;
        return false;
    }
    mapping.data = mmap(nullptr, bytes, protection, MAP_PRIVATE, FD, 0);
    mapping.bytes = bytes;
    int const MAP_ERROR = errno;
    int const CLOSED = close(FD);
    if (mapping.data == MAP_FAILED) {
        errno = MAP_ERROR;
        return false;
    }
    // The mapping is the only retained file handle before any data access.
    // A cold lazy page therefore exercises deferred File lifetime ownership.
    return CLOSED == 0;
}

struct ScratchFile {
    std::array<char, 4096> path{};
    int fd{-1};
    bool created{};
    ~ScratchFile() {
        if (fd >= 0) static_cast<void>(close(fd));
        if (created) static_cast<void>(unlink(path.data()));
    }
    auto release() -> bool {
        int const CLOSED = close(fd);
        int const CLOSE_ERROR = errno;
        fd = -1;
        int const UNLINKED = unlink(path.data());
        if (UNLINKED == 0) created = false;
        if (CLOSED != 0) errno = CLOSE_ERROR;
        return CLOSED == 0 && UNLINKED == 0;
    }
};

auto positioned_page_io(int fd, void* data, bool write) -> bool {
    size_t done = 0;
    constexpr size_t MAX_ATTEMPTS = 64;
    for (size_t attempt = 0; attempt < MAX_ATTEMPTS && done < PAGE_BYTES; ++attempt) {
        auto* cursor = static_cast<uint8_t*>(data) + done;
        ssize_t const RESULT = write ? pwrite(fd, cursor, PAGE_BYTES - done, static_cast<off_t>(done))
                                     : pread(fd, cursor, PAGE_BYTES - done, static_cast<off_t>(done));
        if (RESULT < 0 && errno == EINTR) continue;
        if (RESULT <= 0 || static_cast<size_t>(RESULT) > PAGE_BYTES - done) {
            if (RESULT >= 0) errno = EIO;
            return false;
        }
        done += static_cast<size_t>(RESULT);
    }
    if (done == PAGE_BYTES) return true;
    errno = EIO;
    return false;
}

auto cold_mapped_usercopy(const char* path, uint8_t expected) -> bool {
    Mapping source;
    Mapping destination;
    if (!map_closed_file(path, PAGE_BYTES, PROT_READ, source) || !map_closed_file(path, PAGE_BYTES, PROT_READ | PROT_WRITE, destination)) {
        std::fprintf(stderr, "blockbench_mmap status=fail phase=cold-usercopy-map errno=%d\n", errno);
        return false;
    }
    ScratchFile scratch;
    size_t path_length = 0;
    while (path_length < scratch.path.size() && path[path_length] != '\0') ++path_length;
    int const LENGTH = path_length < scratch.path.size() ? std::snprintf(scratch.path.data(), scratch.path.size(), "%s.goal30-mmap-%ld",
                                                                         path, static_cast<long>(getpid()))
                                                         : -1;
    if (LENGTH < 0 || static_cast<size_t>(LENGTH) >= scratch.path.size()) {
        errno = ENAMETOOLONG;
        std::fprintf(stderr, "blockbench_mmap status=fail phase=cold-usercopy-path errno=%d\n", errno);
        return false;
    }
    scratch.fd = open(scratch.path.data(), O_CREAT | O_EXCL | O_RDWR, 0600);
    if (scratch.fd < 0) {
        std::fprintf(stderr, "blockbench_mmap status=fail phase=cold-usercopy-scratch errno=%d\n", errno);
        return false;
    }
    scratch.created = true;
    // Neither fresh mapping has been dereferenced. Ordinary VFS usercopy must
    // prefault the closed-FD FILE source and writable private destination.
    if (!positioned_page_io(scratch.fd, source.data, true)) {
        std::fprintf(stderr, "blockbench_mmap status=fail phase=cold-pwrite errno=%d\n", errno);
        return false;
    }
    if (fsync(scratch.fd) != 0) {
        std::fprintf(stderr, "blockbench_mmap status=fail phase=cold-fsync errno=%d\n", errno);
        return false;
    }
    std::array<uint8_t, PAGE_BYTES> checked{};
    if (!positioned_page_io(scratch.fd, checked.data(), false)) {
        std::fprintf(stderr, "blockbench_mmap status=fail phase=cold-scratch-read errno=%d\n", errno);
        return false;
    }
    for (size_t i = 0; i < checked.size(); ++i) {
        if (checked[i] != expected) {
            std::fprintf(stderr, "blockbench_mmap status=fail phase=cold-scratch-verify offset=%zu\n", i);
            return false;
        }
        checked[i] ^= 0xffU;
    }
    if (!positioned_page_io(scratch.fd, checked.data(), true) || fsync(scratch.fd) != 0) {
        std::fprintf(stderr, "blockbench_mmap status=fail phase=cold-scratch-rewrite errno=%d\n", errno);
        return false;
    }
    if (!positioned_page_io(scratch.fd, destination.data, false)) {
        std::fprintf(stderr, "blockbench_mmap status=fail phase=cold-pread errno=%d\n", errno);
        return false;
    }
    auto const* contents = static_cast<const volatile uint8_t*>(destination.data);
    for (size_t i = 0; i < PAGE_BYTES; ++i) {
        if (contents[i] != static_cast<uint8_t>(expected ^ 0xffU)) {
            std::fprintf(stderr, "blockbench_mmap status=fail phase=cold-usercopy-verify offset=%zu\n", i);
            return false;
        }
    }
    if (!scratch.release() || !destination.release() || !source.release()) {
        std::fprintf(stderr, "blockbench_mmap status=fail phase=cold-usercopy-cleanup errno=%d\n", errno);
        return false;
    }
    return true;
}
}  // namespace

auto run_blockbench_mmap(int argc, char** argv) -> int {
    uint64_t bytes = DEFAULT_BYTES;
    uint64_t pattern = 0x5a;
    if (argc < 2 || argc > 4 || (argc >= 3 && !number(argv[2], bytes)) || (argc >= 4 && !number(argv[3], pattern)) || bytes < PAGE_BYTES ||
        bytes > MAX_BYTES || bytes % PAGE_BYTES != 0 || bytes > std::numeric_limits<size_t>::max() || pattern > 255) {
        std::fprintf(stderr, "usage: testprog blockbench-mmap PATH [BYTES:4096..16777216] [PATTERN:0..255]\n");
        return 2;
    }
    auto const SIZE = static_cast<size_t>(bytes);
    auto const EXPECTED = static_cast<uint8_t>(pattern);
    Mapping private_mapping;
    if (!map_closed_file(argv[1], SIZE, PROT_READ | PROT_WRITE, private_mapping)) {
        std::fprintf(stderr, "blockbench_mmap status=fail phase=private-map errno=%d\n", errno);
        return 1;
    }
    auto* contents = static_cast<volatile uint8_t*>(private_mapping.data);
    uint64_t checksum = 0;
    for (size_t i = 0; i < SIZE; ++i) {
        uint8_t const ACTUAL = contents[i];
        if (ACTUAL != EXPECTED) {
            std::fprintf(stderr, "blockbench_mmap status=fail phase=private-read offset=%zu expected=%u actual=%u\n", i,
                         static_cast<unsigned>(EXPECTED), static_cast<unsigned>(ACTUAL));
            return 1;
        }
        checksum += ACTUAL;
    }
    contents[0] = static_cast<uint8_t>(EXPECTED ^ 0xffU);
    contents[SIZE - 1] = static_cast<uint8_t>(EXPECTED ^ 0xffU);

    Mapping original_mapping;
    if (!map_closed_file(argv[1], SIZE, PROT_READ, original_mapping)) {
        std::fprintf(stderr, "blockbench_mmap status=fail phase=original-map errno=%d\n", errno);
        return 1;
    }
    auto const* original = static_cast<const volatile uint8_t*>(original_mapping.data);
    for (size_t i = 0; i < SIZE; ++i) {
        if (original[i] != EXPECTED) {
            std::fprintf(stderr, "blockbench_mmap status=fail phase=private-isolation offset=%zu\n", i);
            return 1;
        }
    }
    if (contents[0] != static_cast<uint8_t>(EXPECTED ^ 0xffU) || contents[SIZE - 1] != static_cast<uint8_t>(EXPECTED ^ 0xffU)) {
        std::fprintf(stderr, "blockbench_mmap status=fail phase=private-write\n");
        return 1;
    }
    if (!original_mapping.release() || !private_mapping.release()) {
        std::fprintf(stderr, "blockbench_mmap status=fail phase=unmap errno=%d\n", errno);
        return 1;
    }
    if (!cold_mapped_usercopy(argv[1], EXPECTED)) return 1;
    std::printf(
        "blockbench_mmap status=pass bytes=%zu pages=%zu pattern=%u checksum=%llu private_isolation=pass fd_closed=pass "
        "cold_pwrite=pass cold_pread=pass usercopy_bytes=%zu cold_pread_pattern=%u scratch_cleanup=pass\n",
        SIZE, SIZE / PAGE_BYTES, static_cast<unsigned>(EXPECTED), static_cast<unsigned long long>(checksum), PAGE_BYTES,
        static_cast<unsigned>(EXPECTED ^ 0xffU));
    return 0;
}
