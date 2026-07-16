#include "fsbench.hpp"

#include <abi-bits/fcntl.h>
#include <abi-bits/stat.h>
#include <bits/ssize_t.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): WOS POSIX clock declarations live here.
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <limits>
#include <print>
#include <string>
#include <utility>
#include <vector>

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

struct MetadataOptions {
    const char* path = nullptr;
    uint32_t iterations = 1000;
};

struct MetadataWorkerOptions {
    const char* create_path = nullptr;
    const char* rename_path = nullptr;
    uint32_t iterations = 0;
};

auto monotonic_ns() -> uint64_t {
    struct timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL) + static_cast<uint64_t>(ts.tv_nsec);
}

auto parse_u32(const char* value, uint32_t* out) -> bool {
    if (value == nullptr || out == nullptr || *value == '\0') {
        return false;
    }
    char* end = nullptr;
    unsigned long const PARSED = std::strtoul(value, &end, 10);
    if (end == value || *end != '\0' || PARSED > std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    *out = static_cast<uint32_t>(PARSED);
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
    std::println("  testprog vfsbench-create --path <prefix> [--iterations N]");
    std::println("  testprog vfsbench-rename --path <prefix> [--iterations N]");
    std::println("  testprog vfsbench-metadata-worker --create-path <prefix> --rename-path <prefix> --iterations N");
}

auto parse_metadata_options(int argc, char** argv, MetadataOptions* options) -> bool {
    if (options == nullptr) {
        return false;
    }
    for (int i = 0; i < argc; ++i) {
        if (std::strcmp(argv[i], "--path") == 0 && i + 1 < argc) {
            options->path = argv[++i];
        } else if (std::strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], &options->iterations) || options->iterations == 0) {
                return false;
            }
        } else {
            return false;
        }
    }
    return options->path != nullptr;
}

auto parse_metadata_worker_options(int argc, char** argv, MetadataWorkerOptions* options) -> bool {
    if (options == nullptr) {
        return false;
    }
    for (int i = 0; i < argc; ++i) {
        if (std::strcmp(argv[i], "--create-path") == 0 && i + 1 < argc) {
            options->create_path = argv[++i];
        } else if (std::strcmp(argv[i], "--rename-path") == 0 && i + 1 < argc) {
            options->rename_path = argv[++i];
        } else if (std::strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], &options->iterations) || options->iterations == 0) {
                return false;
            }
        } else {
            return false;
        }
    }
    return options->create_path != nullptr && options->rename_path != nullptr && options->iterations != 0;
}

auto iteration_paths(const char* prefix, const char* suffix, uint32_t iterations) -> std::vector<std::string> {
    std::vector<std::string> paths;
    paths.reserve(iterations);
    for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
        std::string path(prefix);
        path += suffix;
        path += std::to_string(iteration);
        paths.push_back(std::move(path));
    }
    return paths;
}

auto cleanup_paths(const std::vector<std::string>& paths) -> bool {
    bool cleaned = true;
    for (const auto& path : paths) {
        if (unlink(path.c_str()) != 0 && errno != ENOENT) {
            cleaned = false;
        }
    }
    return cleaned;
}

auto verify_empty_files(const std::vector<std::string>& paths) -> bool {
    for (const auto& path : paths) {
        struct stat file_stat{};
        if (stat(path.c_str(), &file_stat) != 0 || file_stat.st_size != 0) {
            return false;
        }
    }
    return true;
}

auto paths_are_absent(const std::vector<std::string>& paths) -> bool {
    for (const auto& path : paths) {
        struct stat file_stat{};
        if (stat(path.c_str(), &file_stat) == 0 || errno != ENOENT) {
            return false;
        }
    }
    return true;
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

    std::vector<uint8_t> buffer(options.read_size);

    uint64_t checksum = 0;
    uint64_t total_bytes = 0;
    uint64_t const STARTED_NS = monotonic_ns();

    for (uint32_t iteration = 0; iteration < options.iterations; ++iteration) {
        int const FD = open(options.path, O_RDONLY);
        if (FD < 0) {
            std::println("vfsbench-read: failed to open '{}'", options.path);
            return 1;
        }

        for (;;) {
            ssize_t const BYTES_READ = read(FD, buffer.data(), buffer.size());
            if (BYTES_READ < 0) {
                close(FD);
                std::println("vfsbench-read: read failed for '{}'", options.path);
                return 1;
            }
            if (BYTES_READ == 0) {
                break;
            }
            checksum = fnv1a_update(checksum, buffer.data(), static_cast<size_t>(BYTES_READ));
            total_bytes += static_cast<uint64_t>(BYTES_READ);
        }

        close(FD);
    }

    uint64_t const ELAPSED_NS = monotonic_ns() - STARTED_NS;

    const double ELAPSED_S = static_cast<double>(ELAPSED_NS) / 1000000000.0;
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
    uint64_t const STARTED_NS = monotonic_ns();
    for (uint32_t iteration = 0; iteration < options.iterations; ++iteration) {
        if (stat(options.path, &path_stat) != 0) {
            std::println("vfsbench-stat: stat failed for '{}'", options.path);
            return 1;
        }
    }
    uint64_t const ELAPSED_NS = monotonic_ns() - STARTED_NS;

    const double AVERAGE_LATENCY_US = (static_cast<double>(ELAPSED_NS) / 1000.0) / static_cast<double>(options.iterations);

    std::println(R"({{"benchmark":"wos_vfsbench_stat","path":"{}","iterations":{},"size":{},"avg_latency_us":{}}})", options.path,
                 options.iterations, static_cast<uint64_t>(path_stat.st_size), AVERAGE_LATENCY_US);
    return 0;
}

auto run_create(const MetadataOptions& options) -> int {
    auto paths = iteration_paths(options.path, ".create.", options.iterations);
    if (!cleanup_paths(paths)) {
        std::println("vfsbench-create: failed to remove stale files for '{}'", options.path);
        return 1;
    }

    uint64_t const STARTED_NS = monotonic_ns();
    for (const auto& path : paths) {
        int const FD = open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600);
        if (FD < 0) {
            cleanup_paths(paths);
            std::println("vfsbench-create: failed to create '{}'", path);
            return 1;
        }
        if (close(FD) != 0) {
            cleanup_paths(paths);
            std::println("vfsbench-create: failed to close '{}'", path);
            return 1;
        }
    }
    uint64_t const ELAPSED_NS = monotonic_ns() - STARTED_NS;

    bool const VERIFIED = verify_empty_files(paths);
    bool const CLEANED = cleanup_paths(paths);
    if (!VERIFIED || !CLEANED) {
        std::println("vfsbench-create: verification or cleanup failed for '{}'", options.path);
        return 1;
    }

    double const AVERAGE_LATENCY_US = (static_cast<double>(ELAPSED_NS) / 1000.0) / static_cast<double>(options.iterations);
    std::println(R"({{"benchmark":"wos_vfsbench_create","path":"{}","iterations":{},"elapsed_seconds":{},"avg_latency_us":{}}})",
                 options.path, options.iterations, static_cast<double>(ELAPSED_NS) / 1000000000.0, AVERAGE_LATENCY_US);
    return 0;
}

auto run_create(int argc, char** argv) -> int {
    MetadataOptions options;
    if (!parse_metadata_options(argc, argv, &options)) {
        print_usage();
        return 1;
    }
    return run_create(options);
}

auto run_rename(const MetadataOptions& options) -> int {
    auto source_paths = iteration_paths(options.path, ".rename-source.", options.iterations);
    auto destination_paths = iteration_paths(options.path, ".rename-destination.", options.iterations);
    bool const SOURCES_CLEAN = cleanup_paths(source_paths);
    bool const DESTINATIONS_CLEAN = cleanup_paths(destination_paths);
    if (!SOURCES_CLEAN || !DESTINATIONS_CLEAN) {
        std::println("vfsbench-rename: failed to remove stale files for '{}'", options.path);
        return 1;
    }
    for (const auto& path : source_paths) {
        int const FD = open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600);
        if (FD < 0) {
            cleanup_paths(source_paths);
            std::println("vfsbench-rename: setup failed for '{}'", path);
            return 1;
        }
        if (close(FD) != 0) {
            cleanup_paths(source_paths);
            std::println("vfsbench-rename: setup close failed for '{}'", path);
            return 1;
        }
    }

    uint64_t const STARTED_NS = monotonic_ns();
    for (size_t index = 0; index < source_paths.size(); ++index) {
        if (rename(source_paths.at(index).c_str(), destination_paths.at(index).c_str()) != 0) {
            cleanup_paths(source_paths);
            cleanup_paths(destination_paths);
            std::println("vfsbench-rename: failed to rename '{}'", source_paths.at(index));
            return 1;
        }
    }
    uint64_t const ELAPSED_NS = monotonic_ns() - STARTED_NS;

    bool const VERIFIED = verify_empty_files(destination_paths);
    bool const SOURCES_ABSENT = paths_are_absent(source_paths);
    bool const SOURCES_CLEANED = cleanup_paths(source_paths);
    bool const CLEANED = cleanup_paths(destination_paths);
    if (!VERIFIED || !SOURCES_ABSENT || !SOURCES_CLEANED || !CLEANED) {
        std::println("vfsbench-rename: verification or cleanup failed for '{}'", options.path);
        return 1;
    }

    double const AVERAGE_LATENCY_US = (static_cast<double>(ELAPSED_NS) / 1000.0) / static_cast<double>(options.iterations);
    std::println(R"({{"benchmark":"wos_vfsbench_rename","path":"{}","iterations":{},"elapsed_seconds":{},"avg_latency_us":{}}})",
                 options.path, options.iterations, static_cast<double>(ELAPSED_NS) / 1000000000.0, AVERAGE_LATENCY_US);
    return 0;
}

auto run_rename(int argc, char** argv) -> int {
    MetadataOptions options;
    if (!parse_metadata_options(argc, argv, &options)) {
        print_usage();
        return 1;
    }
    return run_rename(options);
}

auto run_metadata_worker_operation(const char* operation, const char* path, uint32_t iterations) -> int {
    MetadataOptions const OPTIONS{.path = path, .iterations = iterations};
    int result = 1;
    if (std::strcmp(operation, "create") == 0) {
        result = run_create(OPTIONS);
    } else if (std::strcmp(operation, "rename") == 0) {
        result = run_rename(OPTIONS);
    }
    if (result != 0) {
        return result;
    }
    std::println("metadata-worker-done-v1 {}", operation);
    if (std::fflush(stdout) != 0) {
        std::println(stderr, "vfsbench-metadata-worker: failed to flush {} completion", operation);
        return 1;
    }
    return 0;
}

auto run_metadata_worker(int argc, char** argv) -> int {
    MetadataWorkerOptions options;
    if (!parse_metadata_worker_options(argc, argv, &options)) {
        print_usage();
        return 1;
    }

    std::println("metadata-worker-ready-v1");
    if (std::fflush(stdout) != 0) {
        std::println(stderr, "vfsbench-metadata-worker: failed to flush readiness");
        return 1;
    }

    std::array<char, 32> command{};
    while (std::fgets(command.data(), static_cast<int>(command.size()), stdin) != nullptr) {
        size_t length = std::strlen(command.data());
        while (length > 0 && (command.at(length - 1) == '\n' || command.at(length - 1) == '\r')) {
            command.at(--length) = '\0';
        }

        const char* path = nullptr;
        if (std::strcmp(command.data(), "create") == 0) {
            path = options.create_path;
        } else if (std::strcmp(command.data(), "rename") == 0) {
            path = options.rename_path;
        } else {
            std::println(stderr, "vfsbench-metadata-worker: invalid operation '{}'", command.data());
            return 1;
        }
        int const RESULT = run_metadata_worker_operation(command.data(), path, options.iterations);
        if (RESULT != 0) {
            return RESULT;
        }
        command.fill('\0');
    }
    if (std::ferror(stdin) != 0) {
        std::println(stderr, "vfsbench-metadata-worker: failed to read control input");
        return 1;
    }
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
    if (std::strcmp(argv[0], "vfsbench-create") == 0) {
        return run_create(argc - 1, argv + 1);
    }
    if (std::strcmp(argv[0], "vfsbench-rename") == 0) {
        return run_rename(argc - 1, argv + 1);
    }
    if (std::strcmp(argv[0], "vfsbench-metadata-worker") == 0) {
        return run_metadata_worker(argc - 1, argv + 1);
    }

    print_usage();
    return 1;
}
