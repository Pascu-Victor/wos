#include "memory_pressure.hpp"

#include <sys/mman.h>
#include <time.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <print>

namespace {

constexpr size_t PAGE_SIZE = 4096;
constexpr uint64_t FNV_OFFSET = 1469598103934665603ULL;
constexpr uint64_t FNV_PRIME = 1099511628211ULL;

auto pressure_byte(size_t offset) -> uint8_t {
    uint64_t const PAGE = offset / PAGE_SIZE;
    uint64_t const IN_PAGE = offset % PAGE_SIZE;
    return static_cast<uint8_t>((PAGE * 131U) ^ (IN_PAGE * 17U) ^ (PAGE >> 8U) ^ 0xA5U);
}

void hash_byte(uint64_t& hash, uint8_t byte) {
    hash ^= byte;
    hash *= FNV_PRIME;
}

auto parse_size(const char* text, uint64_t& out) -> bool {
    if (text == nullptr || *text == '\0') {
        return false;
    }

    errno = 0;
    char* end = nullptr;
    unsigned long long const PARSED = std::strtoull(text, &end, 0);
    if (end == text || errno != 0) {
        return false;
    }

    uint64_t multiplier = 1;
    if (*end != '\0') {
        if (end[1] != '\0') {
            return false;
        }
        switch (*end) {
            case 'k':
            case 'K':
                multiplier = 1024ULL;
                break;
            case 'm':
            case 'M':
                multiplier = 1024ULL * 1024ULL;
                break;
            case 'g':
            case 'G':
                multiplier = 1024ULL * 1024ULL * 1024ULL;
                break;
            default:
                return false;
        }
    }
    if (PARSED == 0 || PARSED > UINT64_MAX / multiplier) {
        return false;
    }
    out = static_cast<uint64_t>(PARSED) * multiplier;
    return true;
}

auto parse_seconds(const char* text, uint32_t& out) -> bool {
    if (text == nullptr || *text == '\0') {
        return false;
    }
    errno = 0;
    char* end = nullptr;
    unsigned long const PARSED = std::strtoul(text, &end, 0);
    if (end == text || *end != '\0' || errno != 0 || PARSED == 0 || PARSED > UINT32_MAX) {
        return false;
    }
    out = static_cast<uint32_t>(PARSED);
    return true;
}

void usage() { std::println("usage: testprog memory-pressure --bytes N[k|m|g] [--hold-seconds N]"); }

}  // namespace

auto run_memory_pressure(int argc, char** argv) -> int {
    uint64_t requested_bytes = 0;
    uint32_t hold_seconds = 60;
    for (int i = 0; i < argc; ++i) {
        if (std::strcmp(argv[i], "--bytes") == 0 && i + 1 < argc) {
            if (!parse_size(argv[++i], requested_bytes)) {
                usage();
                return 2;
            }
        } else if (std::strcmp(argv[i], "--hold-seconds") == 0 && i + 1 < argc) {
            if (!parse_seconds(argv[++i], hold_seconds)) {
                usage();
                return 2;
            }
        } else {
            usage();
            return 2;
        }
    }
    if (requested_bytes == 0 || requested_bytes > static_cast<uint64_t>(SIZE_MAX) - (PAGE_SIZE - 1)) {
        usage();
        return 2;
    }

    size_t const bytes = static_cast<size_t>((requested_bytes + PAGE_SIZE - 1) & ~(static_cast<uint64_t>(PAGE_SIZE) - 1));
    void* const mapping = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mapping == MAP_FAILED) {
        std::println(stderr, "memory-pressure: mmap failed bytes={} errno={}", static_cast<uint64_t>(bytes), errno);
        return 1;
    }

    auto* region = static_cast<volatile uint8_t*>(mapping);
    uint64_t expected_hash = FNV_OFFSET;
    for (size_t offset = 0; offset < bytes; ++offset) {
        uint8_t const VALUE = pressure_byte(offset);
        region[offset] = VALUE;
        hash_byte(expected_hash, VALUE);
    }
    std::println(R"({{"event":"memory_pressure_ready","bytes":{},"pages":{},"hold_seconds":{},"hash":"{:016x}"}})",
                 static_cast<uint64_t>(bytes), static_cast<uint64_t>(bytes / PAGE_SIZE), hold_seconds, expected_hash);
    std::fflush(stdout);

    timespec remaining{.tv_sec = static_cast<time_t>(hold_seconds), .tv_nsec = 0};
    while (nanosleep(&remaining, &remaining) != 0 && errno == EINTR) {
    }

    uint64_t actual_hash = FNV_OFFSET;
    size_t mismatches = 0;
    for (size_t offset = 0; offset < bytes; ++offset) {
        uint8_t const VALUE = region[offset];
        hash_byte(actual_hash, VALUE);
        if (VALUE != pressure_byte(offset)) {
            ++mismatches;
        }
    }
    std::println(R"({{"event":"memory_pressure_verified","bytes":{},"pages":{},"hash":"{:016x}","mismatches":{}}})",
                 static_cast<uint64_t>(bytes), static_cast<uint64_t>(bytes / PAGE_SIZE), actual_hash, static_cast<uint64_t>(mismatches));
    std::fflush(stdout);

    if (munmap(mapping, bytes) != 0) {
        std::println(stderr, "memory-pressure: munmap failed errno={}", errno);
        return 1;
    }
    std::println(R"({{"event":"memory_pressure_released","bytes":{},"pages":{}}})", static_cast<uint64_t>(bytes),
                 static_cast<uint64_t>(bytes / PAGE_SIZE));
    return mismatches == 0 && actual_hash == expected_hash ? 0 : 1;
}
