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

    auto* region = static_cast<uint8_t*>(mapping);
    for (size_t offset = 0; offset < bytes; offset += PAGE_SIZE) {
        region[offset] = static_cast<uint8_t>((offset / PAGE_SIZE) & 0xffU);
    }
    std::println(R"({{"event":"memory_pressure_ready","bytes":{},"pages":{},"hold_seconds":{}}})", static_cast<uint64_t>(bytes),
                 static_cast<uint64_t>(bytes / PAGE_SIZE), hold_seconds);
    std::fflush(stdout);

    timespec remaining{.tv_sec = static_cast<time_t>(hold_seconds), .tv_nsec = 0};
    while (nanosleep(&remaining, &remaining) != 0 && errno == EINTR) {
    }

    if (munmap(mapping, bytes) != 0) {
        std::println(stderr, "memory-pressure: munmap failed errno={}", errno);
        return 1;
    }
    std::println(R"({{"event":"memory_pressure_released","bytes":{},"pages":{}}})", static_cast<uint64_t>(bytes),
                 static_cast<uint64_t>(bytes / PAGE_SIZE));
    return 0;
}
