#include "cowbench.hpp"

#include <signal.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): WOS POSIX clock declarations live here.
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <print>

namespace {

constexpr size_t PAGE_SIZE = 4096;
constexpr uint32_t DEFAULT_CHILDREN = 4;
constexpr uint32_t DEFAULT_ITERATIONS = 5;
constexpr uint64_t DEFAULT_BYTES = 32ULL * 1024ULL * 1024ULL;
constexpr uint32_t DEFAULT_WRITE_PAGES = 256;
constexpr uint32_t DEFAULT_CHILD_TIMEOUT_MS = 30000;
constexpr uint32_t CHILD_WAIT_POLL_US = 1000;
constexpr uint64_t NSEC_PER_MSEC = 1000000ULL;

struct CowOptions {
    uint64_t bytes = DEFAULT_BYTES;
    uint32_t children = DEFAULT_CHILDREN;
    uint32_t iterations = DEFAULT_ITERATIONS;
    uint32_t write_pages = DEFAULT_WRITE_PAGES;
    uint32_t child_timeout_ms = DEFAULT_CHILD_TIMEOUT_MS;
    bool verbose = false;
};

auto monotonic_ns() -> uint64_t {
    struct timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL) + static_cast<uint64_t>(ts.tv_nsec);
}

auto parse_u32(const char* text, uint32_t& out, bool allow_zero) -> bool {
    if (text == nullptr || *text == '\0') {
        return false;
    }

    errno = 0;
    char* end = nullptr;
    unsigned long const PARSED = std::strtoul(text, &end, 0);
    if (end == text || *end != '\0' || errno != 0 || PARSED > UINT32_MAX || (!allow_zero && PARSED == 0)) {
        return false;
    }

    out = static_cast<uint32_t>(PARSED);
    return true;
}

auto parse_size(const char* text, uint64_t& out, bool allow_zero) -> bool {
    if (text == nullptr || *text == '\0') {
        return false;
    }

    errno = 0;
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(text, &end, 0);
    if (end == text || errno != 0) {
        return false;
    }

    uint64_t multiplier = 1;
    if (*end != '\0') {
        char suffix = *end;
        if (end[1] != '\0') {
            return false;
        }
        if (suffix == 'k' || suffix == 'K') {
            multiplier = 1024ULL;
        } else if (suffix == 'm' || suffix == 'M') {
            multiplier = 1024ULL * 1024ULL;
        } else if (suffix == 'g' || suffix == 'G') {
            multiplier = 1024ULL * 1024ULL * 1024ULL;
        } else {
            return false;
        }
    }

    if (parsed > UINT64_MAX / multiplier) {
        return false;
    }

    out = static_cast<uint64_t>(parsed) * multiplier;
    return allow_zero || out != 0;
}

void print_usage() {
    std::println("Usage:");
    std::println(
        "  testprog fork-cow [--bytes N[k|m|g]] [--children N] [--iterations N] [--write-pages N] [--child-timeout-ms N] [--verbose]");
    std::println(
        "  testprog cow      [--bytes N[k|m|g]] [--children N] [--iterations N] [--write-pages N] [--child-timeout-ms N] [--verbose]");
}

auto parse_options(int argc, char** argv, CowOptions& options) -> bool {
    for (int i = 0; i < argc; ++i) {
        if (std::strcmp(argv[i], "--bytes") == 0 && i + 1 < argc) {
            if (!parse_size(argv[++i], options.bytes, false)) {
                std::println(stderr, "fork-cow: invalid --bytes '{}'", argv[i]);
                return false;
            }
            continue;
        }
        if (std::strcmp(argv[i], "--children") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], options.children, false)) {
                std::println(stderr, "fork-cow: invalid --children '{}'", argv[i]);
                return false;
            }
            continue;
        }
        if (std::strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], options.iterations, false)) {
                std::println(stderr, "fork-cow: invalid --iterations '{}'", argv[i]);
                return false;
            }
            continue;
        }
        if (std::strcmp(argv[i], "--write-pages") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], options.write_pages, true)) {
                std::println(stderr, "fork-cow: invalid --write-pages '{}'", argv[i]);
                return false;
            }
            continue;
        }
        if (std::strcmp(argv[i], "--child-timeout-ms") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], options.child_timeout_ms, false)) {
                std::println(stderr, "fork-cow: invalid --child-timeout-ms '{}'", argv[i]);
                return false;
            }
            continue;
        }
        if (std::strcmp(argv[i], "--verbose") == 0) {
            options.verbose = true;
            continue;
        }

        std::println(stderr, "fork-cow: unknown option '{}'", argv[i]);
        return false;
    }

    return true;
}

auto align_up(uint64_t value, uint64_t alignment) -> uint64_t {
    uint64_t const REMAINDER = value % alignment;
    return REMAINDER == 0 ? value : value + (alignment - REMAINDER);
}

auto base_value(size_t page_index) -> uint8_t { return static_cast<uint8_t>(((page_index * 131ULL) + 0x5dULL) & 0xffULL); }

auto tail_value(size_t page_index) -> uint8_t { return static_cast<uint8_t>(base_value(page_index) ^ 0xa5U); }

auto cow_value(uint32_t iteration, uint32_t child_index, uint32_t write_index) -> uint8_t {
    return static_cast<uint8_t>(((iteration * 37U) + (child_index * 17U) + (write_index * 11U) + 0x31U) & 0xffU);
}

void initialize_region(uint8_t* region, size_t page_count) {
    for (size_t page = 0; page < page_count; ++page) {
        uint8_t* const BASE = region + (page * PAGE_SIZE);
        BASE[0] = base_value(page);
        BASE[PAGE_SIZE - 1] = tail_value(page);
    }
}

auto verify_region(const uint8_t* region, size_t page_count) -> bool {
    for (size_t page = 0; page < page_count; ++page) {
        const uint8_t* const BASE = region + (page * PAGE_SIZE);
        if (BASE[0] != base_value(page) || BASE[PAGE_SIZE - 1] != tail_value(page)) {
            return false;
        }
    }
    return true;
}

auto child_start_page(uint32_t iteration, uint32_t child_index, size_t page_count) -> size_t {
    return ((static_cast<size_t>(iteration) * 4099ULL) + (static_cast<size_t>(child_index) * 257ULL)) % page_count;
}

auto run_child(uint8_t* region, size_t page_count, uint32_t write_pages, uint32_t iteration, uint32_t child_index) -> int {
    if (!verify_region(region, page_count)) {
        return 10;
    }

    uint32_t const WRITES = std::min<uint32_t>(write_pages, static_cast<uint32_t>(page_count));
    size_t const START_PAGE = child_start_page(iteration, child_index, page_count);
    for (uint32_t write_index = 0; write_index < WRITES; ++write_index) {
        size_t const PAGE = (START_PAGE + write_index) % page_count;
        uint8_t* const SLOT = region + (PAGE * PAGE_SIZE);
        uint8_t const VALUE = cow_value(iteration, child_index, write_index);
        SLOT[0] = VALUE;
        if (SLOT[0] != VALUE) {
            return 11;
        }
        if (SLOT[PAGE_SIZE - 1] != tail_value(PAGE)) {
            return 12;
        }
    }

    return 0;
}

auto reap_child_after_timeout(pid_t pid) -> void {
    (void)kill(pid, SIGKILL);
    for (uint32_t retry = 0; retry < 1000; ++retry) {
        int reap_status = 0;
        pid_t const REAPED = waitpid(pid, &reap_status, WNOHANG);
        if (REAPED == pid || (REAPED < 0 && errno != EINTR)) {
            return;
        }
        usleep(CHILD_WAIT_POLL_US);
    }
}

auto wait_for_child(pid_t pid, uint32_t timeout_ms) -> bool {
    uint64_t const START_NS = monotonic_ns();
    uint64_t const TIMEOUT_NS = static_cast<uint64_t>(timeout_ms) * NSEC_PER_MSEC;
    int status = 0;

    while (monotonic_ns() - START_NS <= TIMEOUT_NS) {
        errno = 0;
        pid_t const WAITED = waitpid(pid, &status, WNOHANG);
        if (WAITED < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::println(stderr, "fork-cow: waitpid({}) failed ret={} errno={}", static_cast<int>(pid), static_cast<int>(WAITED), errno);
            return false;
        }
        if (WAITED == 0) {
            usleep(CHILD_WAIT_POLL_US);
            continue;
        }
        if (WAITED != pid) {
            std::println(stderr, "fork-cow: waitpid({}) reaped unexpected child {}", static_cast<int>(pid), static_cast<int>(WAITED));
            return false;
        }

        if (!WIFEXITED(status)) {
            std::println(stderr, "fork-cow: child {} did not exit normally status={}", static_cast<int>(pid), status);
            return false;
        }
        if (WEXITSTATUS(status) != 0) {
            std::println(stderr, "fork-cow: child {} reported data mismatch exit={}", static_cast<int>(pid), WEXITSTATUS(status));
            return false;
        }
        return true;
    }

    std::println(stderr, "fork-cow: child {} timed out after {}ms", static_cast<int>(pid), timeout_ms);
    reap_child_after_timeout(pid);
    return false;
}

auto run_stress(const CowOptions& options) -> int {
    uint64_t const MAP_BYTES_U64 = align_up(options.bytes, PAGE_SIZE);
    if (MAP_BYTES_U64 > static_cast<uint64_t>(SIZE_MAX)) {
        std::println(stderr, "fork-cow: --bytes too large for this userspace");
        return 1;
    }

    auto const MAP_BYTES = static_cast<size_t>(MAP_BYTES_U64);
    size_t const PAGE_COUNT = MAP_BYTES / PAGE_SIZE;
    if (PAGE_COUNT == 0 || PAGE_COUNT > static_cast<size_t>(UINT32_MAX)) {
        std::println(stderr, "fork-cow: page count out of supported range");
        return 1;
    }

    void* const MAPPING = mmap(nullptr, MAP_BYTES, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (MAPPING == MAP_FAILED) {
        std::println(stderr, "fork-cow: mmap failed bytes={} errno={}", static_cast<uint64_t>(MAP_BYTES), errno);
        return 1;
    }

    auto* const REGION = static_cast<uint8_t*>(MAPPING);
    initialize_region(REGION, PAGE_COUNT);

    if (options.verbose) {
        std::println(stderr, "fork-cow: bytes={} pages={} children={} iterations={} write_pages={} child_timeout_ms={}",
                     static_cast<uint64_t>(MAP_BYTES), static_cast<uint64_t>(PAGE_COUNT), options.children, options.iterations,
                     options.write_pages, options.child_timeout_ms);
    }

    uint64_t const START_NS = monotonic_ns();
    uint64_t forks = 0;
    uint64_t cow_writes = 0;
    uint32_t const WRITES_PER_CHILD = std::min<uint32_t>(options.write_pages, static_cast<uint32_t>(PAGE_COUNT));

    for (uint32_t iteration = 0; iteration < options.iterations; ++iteration) {
        for (uint32_t child = 0; child < options.children; ++child) {
            pid_t const PID = fork();
            if (PID < 0) {
                std::println(stderr, "fork-cow: fork failed iteration={} child={} errno={}", iteration, child, errno);
                munmap(REGION, MAP_BYTES);
                return 1;
            }
            if (PID == 0) {
                _exit(run_child(REGION, PAGE_COUNT, options.write_pages, iteration, child));
            }
            ++forks;
            if (!wait_for_child(PID, options.child_timeout_ms)) {
                munmap(REGION, MAP_BYTES);
                return 1;
            }
            cow_writes += WRITES_PER_CHILD;
        }

        if (!verify_region(REGION, PAGE_COUNT)) {
            std::println(stderr, "fork-cow: parent mapping changed after iteration {}", iteration);
            munmap(REGION, MAP_BYTES);
            return 1;
        }
    }

    uint64_t const ELAPSED_NS = monotonic_ns() - START_NS;
    munmap(REGION, MAP_BYTES);

    double const ELAPSED_S = static_cast<double>(ELAPSED_NS) / 1000000000.0;
    double const FORKS_PER_S = static_cast<double>(forks) / (ELAPSED_S > 0.0 ? ELAPSED_S : 1.0);
    double const COW_WRITES_PER_S = static_cast<double>(cow_writes) / (ELAPSED_S > 0.0 ? ELAPSED_S : 1.0);

    std::println(
        R"({{"benchmark":"wos_fork_cow","wait_mode":"serial","bytes":{},"pages":{},"children":{},"iterations":{},"forks":{},"write_pages_per_child":{},"cow_writes":{},"elapsed_ns":{},"forks_per_s":{},"cow_writes_per_s":{}}})",
        static_cast<uint64_t>(MAP_BYTES), static_cast<uint64_t>(PAGE_COUNT), options.children, options.iterations, forks,
        options.write_pages, cow_writes, ELAPSED_NS, FORKS_PER_S, COW_WRITES_PER_S);
    return 0;
}

}  // namespace

auto run_cowbench(int argc, char** argv) -> int {
    CowOptions options;
    if (!parse_options(argc, argv, options)) {
        print_usage();
        return 2;
    }
    return run_stress(options);
}
