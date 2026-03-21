#define _DEFAULT_SOURCE 1

#include "fsbench.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <print>

namespace {

struct ReadOptions {
    const char* path = nullptr;
    uint32_t read_size = 65536;
    uint32_t iterations = 1;
};

struct StatOptions {
    const char* path = nullptr;
    uint32_t iterations = 1000;
};

auto monotonic_ns() -> uint64_t {
    struct timespec ts{};
    clock_gettime(CLOCK_REALTIME, &ts);
    return (static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL) + static_cast<uint64_t>(ts.tv_nsec);
}

auto parse_u32(const char* value, uint32_t* out) -> bool {
    if (value == nullptr || out == nullptr || *value == '\0') {
        return false;
    }
    char* end = nullptr;
    unsigned long parsed = std::strtoul(value, &end, 10);
    if (end == value || *end != '\0') {
        return false;
    }
    *out = static_cast<uint32_t>(parsed);
    return true;
}

auto fnv1a_update(uint64_t hash, const uint8_t* data, size_t size) -> uint64_t {
    constexpr uint64_t FNV_OFFSET = 1469598103934665603ULL;
    constexpr uint64_t FNV_PRIME = 1099511628211ULL;

    if (hash == 0) {
        hash = FNV_OFFSET;
    }
    for (size_t i = 0; i < size; ++i) {
        hash ^= static_cast<uint64_t>(data[i]);
        hash *= FNV_PRIME;
    }
    return hash;
}

void print_usage() {
    std::println("Usage:");
    std::println("  testprog vfsbench-read --path <path> [--read-size N] [--iterations N]");
    std::println("  testprog vfsbench-stat --path <path> [--iterations N]");
}

auto run_read(int argc, char** argv) -> int {
    ReadOptions options;
    for (int i = 0; i < argc; ++i) {
        if (std::strcmp(argv[i], "--path") == 0 && i + 1 < argc) {
            options.path = argv[++i];
        } else if (std::strcmp(argv[i], "--read-size") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], &options.read_size) || options.read_size == 0) {
                print_usage();
                return 1;
            }
        } else if (std::strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], &options.iterations) || options.iterations == 0) {
                print_usage();
                return 1;
            }
        } else {
            print_usage();
            return 1;
        }
    }

    if (options.path == nullptr) {
        print_usage();
        return 1;
    }

    auto* buffer = static_cast<uint8_t*>(std::malloc(options.read_size));
    if (buffer == nullptr) {
        std::println("vfsbench-read: allocation failed");
        return 1;
    }

    uint64_t checksum = 0;
    uint64_t total_bytes = 0;
    uint64_t started_ns = monotonic_ns();

    for (uint32_t iteration = 0; iteration < options.iterations; ++iteration) {
        int fd = open(options.path, O_RDONLY);
        if (fd < 0) {
            std::free(buffer);
            std::println("vfsbench-read: failed to open '{}'", options.path);
            return 1;
        }

        for (;;) {
            ssize_t bytes_read = read(fd, buffer, options.read_size);
            if (bytes_read < 0) {
                close(fd);
                std::free(buffer);
                std::println("vfsbench-read: read failed for '{}'", options.path);
                return 1;
            }
            if (bytes_read == 0) {
                break;
            }
            checksum = fnv1a_update(checksum, buffer, static_cast<size_t>(bytes_read));
            total_bytes += static_cast<uint64_t>(bytes_read);
        }

        close(fd);
    }

    uint64_t elapsed_ns = monotonic_ns() - started_ns;
    std::free(buffer);

    const double ELAPSED_S = static_cast<double>(elapsed_ns) / 1000000000.0;
    const double THROUGHPUT_MIB_PER_S = (static_cast<double>(total_bytes) / (1024.0 * 1024.0)) / (ELAPSED_S > 0.0 ? ELAPSED_S : 1.0);

    std::println(
        R"({{"benchmark":"wos_vfsbench_read","path":"{}","iterations":{},"read_size":{},"bytes":{},"checksum":{},"throughput_mib_per_s":{}}})",
        options.path, options.iterations, options.read_size, total_bytes, checksum, THROUGHPUT_MIB_PER_S);
    return 0;
}

auto run_stat(int argc, char** argv) -> int {
    StatOptions options;
    for (int i = 0; i < argc; ++i) {
        if (std::strcmp(argv[i], "--path") == 0 && i + 1 < argc) {
            options.path = argv[++i];
        } else if (std::strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], &options.iterations) || options.iterations == 0) {
                print_usage();
                return 1;
            }
        } else {
            print_usage();
            return 1;
        }
    }

    if (options.path == nullptr) {
        print_usage();
        return 1;
    }

    struct stat path_stat{};
    uint64_t started_ns = monotonic_ns();
    for (uint32_t iteration = 0; iteration < options.iterations; ++iteration) {
        if (stat(options.path, &path_stat) != 0) {
            std::println("vfsbench-stat: stat failed for '{}'", options.path);
            return 1;
        }
    }
    uint64_t elapsed_ns = monotonic_ns() - started_ns;

    const double AVERAGE_LATENCY_US = (static_cast<double>(elapsed_ns) / 1000.0) / static_cast<double>(options.iterations);

    std::println(R"({{"benchmark":"wos_vfsbench_stat","path":"{}","iterations":{},"size":{},"avg_latency_us":{}}})", options.path,
                 options.iterations, static_cast<uint64_t>(path_stat.st_size), AVERAGE_LATENCY_US);
    return 0;
}

}  // namespace

int run_fsbench(int argc, char** argv) {
    if (argc <= 0 || argv == nullptr) {
        print_usage();
        return 1;
    }

    if (std::strcmp(argv[0], "vfsbench-read") == 0) {
        return run_read(argc - 1, argv + 1);
    }
    if (std::strcmp(argv[0], "vfsbench-stat") == 0) {
        return run_stat(argc - 1, argv + 1);
    }

    print_usage();
    return 1;
}
