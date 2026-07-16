#include "fsbench.hpp"

#include <abi-bits/fcntl.h>
#include <abi-bits/stat.h>
#include <bits/ssize_t.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): WOS POSIX clock declarations live here.
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
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

#include "mandelbench/tinycthread.hpp"

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
    uint32_t workers = 1;
};

struct MetadataWorkerOptions {
    const char* create_path = nullptr;
    const char* rename_path = nullptr;
    uint32_t iterations = 0;
    uint32_t workers = 0;
};

enum class MetadataPathAction : uint8_t {
    CLEANUP,
    VERIFY_EMPTY,
    VERIFY_ABSENT,
    CREATE,
    RENAME,
};

struct MetadataPathWorkers;

struct MetadataPathWorkerArg {
    MetadataPathWorkers* pool = nullptr;
    uint32_t worker_index = 0;
    bool success = false;
};

struct MetadataPathWorkers {
    uint32_t worker_count = 0;
    bool initialized = false;
    bool stopping = false;
    uint64_t generation = 0;
    uint32_t started = 0;
    uint32_t completed = 0;
    const std::vector<std::string>* primary_paths = nullptr;
    const std::vector<std::string>* secondary_paths = nullptr;
    MetadataPathAction action = MetadataPathAction::CLEANUP;
    mtx_t mutex{};
    cnd_t work_available{};
    cnd_t work_complete{};
    std::vector<thrd_t> threads;
    std::vector<MetadataPathWorkerArg> args;
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
    std::println("  testprog vfsbench-create --path <prefix> [--iterations N] [--workers N]");
    std::println("  testprog vfsbench-rename --path <prefix> [--iterations N] [--workers N]");
    std::println("  testprog vfsbench-metadata-worker --create-path <prefix> --rename-path <prefix> --iterations N --workers N");
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
        } else if (std::strcmp(argv[i], "--workers") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], &options->workers) || options->workers == 0) {
                return false;
            }
        } else {
            return false;
        }
    }
    return options->path != nullptr && options->workers <= options->iterations;
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
        } else if (std::strcmp(argv[i], "--workers") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], &options->workers) || options->workers == 0) {
                return false;
            }
        } else {
            return false;
        }
    }
    return options->create_path != nullptr && options->rename_path != nullptr && options->iterations != 0 &&
           options->workers <= options->iterations;
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

auto apply_metadata_path_action(const MetadataPathWorkers& pool, size_t index) -> bool {
    const auto& primary = pool.primary_paths->at(index);
    switch (pool.action) {
        case MetadataPathAction::CLEANUP:
            return unlink(primary.c_str()) == 0 || errno == ENOENT;
        case MetadataPathAction::VERIFY_EMPTY: {
            struct stat file_stat{};
            return stat(primary.c_str(), &file_stat) == 0 && file_stat.st_size == 0;
        }
        case MetadataPathAction::VERIFY_ABSENT: {
            struct stat file_stat{};
            return stat(primary.c_str(), &file_stat) != 0 && errno == ENOENT;
        }
        case MetadataPathAction::CREATE: {
            int const FD = open(primary.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600);
            return FD >= 0 && close(FD) == 0;
        }
        case MetadataPathAction::RENAME:
            return pool.secondary_paths != nullptr && rename(primary.c_str(), pool.secondary_paths->at(index).c_str()) == 0;
    }
    return false;
}

auto metadata_batch_operation_for_action(MetadataPathAction action, ker::abi::vfs::metadata_batch_operation* operation) -> bool {
    if (operation == nullptr) {
        return false;
    }
    switch (action) {
        case MetadataPathAction::CLEANUP:
            *operation = ker::abi::vfs::metadata_batch_operation::unlink;
            return true;
        case MetadataPathAction::VERIFY_EMPTY:
        case MetadataPathAction::VERIFY_ABSENT:
            *operation = ker::abi::vfs::metadata_batch_operation::stat_follow;
            return true;
        case MetadataPathAction::CREATE:
            *operation = ker::abi::vfs::metadata_batch_operation::create_close;
            return true;
        case MetadataPathAction::RENAME:
            *operation = ker::abi::vfs::metadata_batch_operation::rename;
            return true;
    }
    return false;
}

auto metadata_batch_result_succeeded(MetadataPathAction action, const ker::abi::vfs::metadata_batch_result& result) -> bool {
    switch (action) {
        case MetadataPathAction::CLEANUP:
            return result.status == 0 || result.status == -ENOENT;
        case MetadataPathAction::VERIFY_EMPTY:
            return result.status == 0 && result.statbuf.st_size == 0;
        case MetadataPathAction::VERIFY_ABSENT:
            return result.status == -ENOENT;
        case MetadataPathAction::CREATE:
        case MetadataPathAction::RENAME:
            return result.status == 0;
    }
    return false;
}

auto execute_metadata_path_range(const MetadataPathWorkers& pool, uint32_t worker_index) -> bool {
    size_t const PATH_COUNT = pool.primary_paths->size();
    size_t const BASE = PATH_COUNT / pool.worker_count;
    size_t const EXTRA = PATH_COUNT % pool.worker_count;
    size_t const BEGIN = (static_cast<size_t>(worker_index) * BASE) + std::min(static_cast<size_t>(worker_index), EXTRA);
    size_t const COUNT = BASE + (static_cast<size_t>(worker_index) < EXTRA ? 1 : 0);

    auto operation = ker::abi::vfs::metadata_batch_operation::create_close;
    if (!metadata_batch_operation_for_action(pool.action, &operation)) {
        return false;
    }

    std::array<ker::abi::vfs::metadata_batch_entry, ker::abi::vfs::METADATA_BATCH_MAX_ITEMS> entries{};
    std::array<ker::abi::vfs::metadata_batch_result, ker::abi::vfs::METADATA_BATCH_MAX_ITEMS> results{};
    bool success = true;
    for (size_t chunk_begin = BEGIN; chunk_begin < BEGIN + COUNT; chunk_begin += ker::abi::vfs::METADATA_BATCH_MAX_ITEMS) {
        size_t const CHUNK_COUNT = std::min(static_cast<size_t>(ker::abi::vfs::METADATA_BATCH_MAX_ITEMS), (BEGIN + COUNT) - chunk_begin);
        for (size_t offset = 0; offset < CHUNK_COUNT; ++offset) {
            size_t const INDEX = chunk_begin + offset;
            entries.at(offset) = {
                .path = pool.primary_paths->at(INDEX).c_str(),
                .second_path = pool.secondary_paths == nullptr ? nullptr : pool.secondary_paths->at(INDEX).c_str(),
            };
            results.at(offset) = {};
            results.at(offset).status = -EIO;
        }

        ker::abi::vfs::metadata_batch_header const HEADER{
            .version = ker::abi::vfs::METADATA_BATCH_VERSION,
            .operation = operation,
            .count = static_cast<uint8_t>(CHUNK_COUNT),
            .mode = pool.action == MetadataPathAction::CREATE ? 0600U : 0U,
        };
        int const BATCH_STATUS = ker::abi::vfs::metadata_batch(&HEADER, entries.data(), results.data());
        if (BATCH_STATUS == -EOPNOTSUPP && chunk_begin == BEGIN) {
            bool scalar_success = true;
            for (size_t index = BEGIN; index < BEGIN + COUNT; ++index) {
                bool const ITEM_SUCCEEDED = apply_metadata_path_action(pool, index);
                scalar_success = ITEM_SUCCEEDED && scalar_success;
            }
            return scalar_success;
        }
        if (BATCH_STATUS != 0) {
            return false;
        }

        for (size_t offset = 0; offset < CHUNK_COUNT; ++offset) {
            bool const ITEM_SUCCEEDED = metadata_batch_result_succeeded(pool.action, results.at(offset));
            success = ITEM_SUCCEEDED && success;
        }
    }
    return success;
}

auto metadata_path_worker_main(void* raw_arg) -> int {
    auto* arg = static_cast<MetadataPathWorkerArg*>(raw_arg);
    auto& pool = *arg->pool;
    mtx_lock(&pool.mutex);
    uint64_t observed_generation = pool.generation;
    ++pool.started;
    cnd_broadcast(&pool.work_complete);

    for (;;) {
        while (!pool.stopping && pool.generation == observed_generation) {
            if (cnd_wait(&pool.work_available, &pool.mutex) != THRD_SUCCESS) {
                mtx_unlock(&pool.mutex);
                thrd_yield();
                mtx_lock(&pool.mutex);
            }
        }
        if (pool.stopping) {
            mtx_unlock(&pool.mutex);
            return 0;
        }

        observed_generation = pool.generation;
        mtx_unlock(&pool.mutex);
        bool const SUCCESS = execute_metadata_path_range(pool, arg->worker_index);
        mtx_lock(&pool.mutex);
        arg->success = SUCCESS;
        ++pool.completed;
        cnd_broadcast(&pool.work_complete);
    }
}

void destroy_metadata_path_worker_sync(MetadataPathWorkers& pool) {
    cnd_destroy(&pool.work_complete);
    cnd_destroy(&pool.work_available);
    mtx_destroy(&pool.mutex);
}

auto initialize_metadata_path_workers(MetadataPathWorkers& pool, uint32_t worker_count) -> bool {
    if (worker_count == 0) {
        return false;
    }
    pool.worker_count = worker_count;
    if (worker_count == 1) {
        pool.initialized = true;
        return true;
    }
    if (mtx_init(&pool.mutex, MTX_PLAIN) != THRD_SUCCESS) {
        return false;
    }
    if (cnd_init(&pool.work_available) != THRD_SUCCESS) {
        mtx_destroy(&pool.mutex);
        return false;
    }
    if (cnd_init(&pool.work_complete) != THRD_SUCCESS) {
        cnd_destroy(&pool.work_available);
        mtx_destroy(&pool.mutex);
        return false;
    }

    auto const BACKGROUND_COUNT = static_cast<size_t>(worker_count - 1);
    pool.threads.resize(BACKGROUND_COUNT, nullptr);
    pool.args.resize(BACKGROUND_COUNT);
    mtx_lock(&pool.mutex);
    size_t created = 0;
    for (; created < BACKGROUND_COUNT; ++created) {
        auto& arg = pool.args.at(created);
        arg.pool = &pool;
        arg.worker_index = static_cast<uint32_t>(created + 1);
        if (thrd_create(&pool.threads.at(created), metadata_path_worker_main, &arg) != THRD_SUCCESS) {
            break;
        }
    }
    if (created != BACKGROUND_COUNT) {
        pool.stopping = true;
        cnd_broadcast(&pool.work_available);
        mtx_unlock(&pool.mutex);
        for (size_t index = 0; index < created; ++index) {
            thrd_join(pool.threads.at(index), nullptr);
        }
        destroy_metadata_path_worker_sync(pool);
        std::println(stderr, "vfsbench metadata: failed to create worker {}/{}", created + 1, worker_count);
        return false;
    }

    bool wait_succeeded = true;
    while (pool.started != BACKGROUND_COUNT) {
        if (cnd_wait(&pool.work_complete, &pool.mutex) != THRD_SUCCESS) {
            wait_succeeded = false;
            break;
        }
    }
    if (!wait_succeeded) {
        pool.stopping = true;
        cnd_broadcast(&pool.work_available);
        mtx_unlock(&pool.mutex);
        for (auto* thread : pool.threads) {
            thrd_join(thread, nullptr);
        }
        destroy_metadata_path_worker_sync(pool);
        std::println(stderr, "vfsbench metadata: failed while waiting for worker readiness");
        return false;
    }
    mtx_unlock(&pool.mutex);
    pool.initialized = true;
    return true;
}

auto stop_metadata_path_workers(MetadataPathWorkers& pool) -> bool {
    if (!pool.initialized) {
        return true;
    }
    bool success = true;
    if (pool.worker_count > 1) {
        mtx_lock(&pool.mutex);
        pool.stopping = true;
        cnd_broadcast(&pool.work_available);
        mtx_unlock(&pool.mutex);
        for (auto* thread : pool.threads) {
            if (thrd_join(thread, nullptr) != THRD_SUCCESS) {
                success = false;
            }
        }
        destroy_metadata_path_worker_sync(pool);
    }
    pool.initialized = false;
    return success;
}

auto run_metadata_path_action(MetadataPathWorkers& pool, const std::vector<std::string>& primary_paths,
                              const std::vector<std::string>* secondary_paths, MetadataPathAction action) -> bool {
    if (!pool.initialized || primary_paths.empty() || pool.worker_count > primary_paths.size() ||
        (secondary_paths != nullptr && secondary_paths->size() != primary_paths.size())) {
        return false;
    }
    pool.primary_paths = &primary_paths;
    pool.secondary_paths = secondary_paths;
    pool.action = action;
    if (pool.worker_count == 1) {
        bool const SUCCESS = execute_metadata_path_range(pool, 0);
        pool.primary_paths = nullptr;
        pool.secondary_paths = nullptr;
        return SUCCESS;
    }

    mtx_lock(&pool.mutex);
    pool.completed = 0;
    for (auto& arg : pool.args) {
        arg.success = false;
    }
    ++pool.generation;
    cnd_broadcast(&pool.work_available);
    mtx_unlock(&pool.mutex);

    bool success = execute_metadata_path_range(pool, 0);
    mtx_lock(&pool.mutex);
    bool wait_succeeded = true;
    while (pool.completed != pool.threads.size()) {
        if (cnd_wait(&pool.work_complete, &pool.mutex) != THRD_SUCCESS) {
            wait_succeeded = false;
            mtx_unlock(&pool.mutex);
            thrd_yield();
            mtx_lock(&pool.mutex);
        }
    }
    for (const auto& arg : pool.args) {
        success = arg.success && success;
    }
    pool.primary_paths = nullptr;
    pool.secondary_paths = nullptr;
    mtx_unlock(&pool.mutex);
    return wait_succeeded && success;
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

auto run_create(const MetadataOptions& options, MetadataPathWorkers& workers) -> int {
    auto paths = iteration_paths(options.path, ".create.", options.iterations);
    if (!run_metadata_path_action(workers, paths, nullptr, MetadataPathAction::CLEANUP)) {
        std::println("vfsbench-create: failed to remove stale files for '{}'", options.path);
        return 1;
    }

    uint64_t const STARTED_NS = monotonic_ns();
    bool const CREATED = run_metadata_path_action(workers, paths, nullptr, MetadataPathAction::CREATE);
    uint64_t const ELAPSED_NS = monotonic_ns() - STARTED_NS;
    if (!CREATED) {
        run_metadata_path_action(workers, paths, nullptr, MetadataPathAction::CLEANUP);
        std::println("vfsbench-create: failed to create files for '{}'", options.path);
        return 1;
    }

    bool const VERIFIED = run_metadata_path_action(workers, paths, nullptr, MetadataPathAction::VERIFY_EMPTY);
    bool const CLEANED = run_metadata_path_action(workers, paths, nullptr, MetadataPathAction::CLEANUP);
    if (!VERIFIED || !CLEANED) {
        std::println("vfsbench-create: verification or cleanup failed for '{}'", options.path);
        return 1;
    }

    double const AVERAGE_LATENCY_US = (static_cast<double>(ELAPSED_NS) / 1000.0) / static_cast<double>(options.iterations);
    std::println(
        R"({{"benchmark":"wos_vfsbench_create","path":"{}","iterations":{},"workers":{},"elapsed_seconds":{},"avg_latency_us":{}}})",
        options.path, options.iterations, options.workers, static_cast<double>(ELAPSED_NS) / 1000000000.0, AVERAGE_LATENCY_US);
    return 0;
}

auto run_create(int argc, char** argv) -> int {
    MetadataOptions options;
    if (!parse_metadata_options(argc, argv, &options)) {
        print_usage();
        return 1;
    }
    MetadataPathWorkers workers;
    if (!initialize_metadata_path_workers(workers, options.workers)) {
        return 1;
    }
    int const RESULT = run_create(options, workers);
    return stop_metadata_path_workers(workers) ? RESULT : 1;
}

auto run_rename(const MetadataOptions& options, MetadataPathWorkers& workers) -> int {
    auto source_paths = iteration_paths(options.path, ".rename-source.", options.iterations);
    auto destination_paths = iteration_paths(options.path, ".rename-destination.", options.iterations);
    bool const SOURCES_CLEAN = run_metadata_path_action(workers, source_paths, nullptr, MetadataPathAction::CLEANUP);
    bool const DESTINATIONS_CLEAN = run_metadata_path_action(workers, destination_paths, nullptr, MetadataPathAction::CLEANUP);
    if (!SOURCES_CLEAN || !DESTINATIONS_CLEAN) {
        std::println("vfsbench-rename: failed to remove stale files for '{}'", options.path);
        return 1;
    }
    if (!run_metadata_path_action(workers, source_paths, nullptr, MetadataPathAction::CREATE)) {
        run_metadata_path_action(workers, source_paths, nullptr, MetadataPathAction::CLEANUP);
        std::println("vfsbench-rename: setup failed for '{}'", options.path);
        return 1;
    }

    uint64_t const STARTED_NS = monotonic_ns();
    bool const RENAMED = run_metadata_path_action(workers, source_paths, &destination_paths, MetadataPathAction::RENAME);
    uint64_t const ELAPSED_NS = monotonic_ns() - STARTED_NS;
    if (!RENAMED) {
        run_metadata_path_action(workers, source_paths, nullptr, MetadataPathAction::CLEANUP);
        run_metadata_path_action(workers, destination_paths, nullptr, MetadataPathAction::CLEANUP);
        std::println("vfsbench-rename: failed to rename files for '{}'", options.path);
        return 1;
    }

    bool const VERIFIED = run_metadata_path_action(workers, destination_paths, nullptr, MetadataPathAction::VERIFY_EMPTY);
    bool const SOURCES_ABSENT = run_metadata_path_action(workers, source_paths, nullptr, MetadataPathAction::VERIFY_ABSENT);
    bool const SOURCES_CLEANED = run_metadata_path_action(workers, source_paths, nullptr, MetadataPathAction::CLEANUP);
    bool const CLEANED = run_metadata_path_action(workers, destination_paths, nullptr, MetadataPathAction::CLEANUP);
    if (!VERIFIED || !SOURCES_ABSENT || !SOURCES_CLEANED || !CLEANED) {
        std::println("vfsbench-rename: verification or cleanup failed for '{}'", options.path);
        return 1;
    }

    double const AVERAGE_LATENCY_US = (static_cast<double>(ELAPSED_NS) / 1000.0) / static_cast<double>(options.iterations);
    std::println(
        R"({{"benchmark":"wos_vfsbench_rename","path":"{}","iterations":{},"workers":{},"elapsed_seconds":{},"avg_latency_us":{}}})",
        options.path, options.iterations, options.workers, static_cast<double>(ELAPSED_NS) / 1000000000.0, AVERAGE_LATENCY_US);
    return 0;
}

auto run_rename(int argc, char** argv) -> int {
    MetadataOptions options;
    if (!parse_metadata_options(argc, argv, &options)) {
        print_usage();
        return 1;
    }
    MetadataPathWorkers workers;
    if (!initialize_metadata_path_workers(workers, options.workers)) {
        return 1;
    }
    int const RESULT = run_rename(options, workers);
    return stop_metadata_path_workers(workers) ? RESULT : 1;
}

auto run_metadata_worker_operation(const char* operation, const char* path, uint32_t iterations, MetadataPathWorkers& workers) -> int {
    MetadataOptions const OPTIONS{.path = path, .iterations = iterations, .workers = workers.worker_count};
    int result = 1;
    if (std::strcmp(operation, "create") == 0) {
        result = run_create(OPTIONS, workers);
    } else if (std::strcmp(operation, "rename") == 0) {
        result = run_rename(OPTIONS, workers);
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

    MetadataPathWorkers workers;
    if (!initialize_metadata_path_workers(workers, options.workers)) {
        return 1;
    }

    std::println("metadata-worker-ready-v1");
    if (std::fflush(stdout) != 0) {
        std::println(stderr, "vfsbench-metadata-worker: failed to flush readiness");
        stop_metadata_path_workers(workers);
        return 1;
    }

    int result = 0;
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
            result = 1;
            break;
        }
        result = run_metadata_worker_operation(command.data(), path, options.iterations, workers);
        if (result != 0) {
            break;
        }
        command.fill('\0');
    }
    if (std::ferror(stdin) != 0) {
        std::println(stderr, "vfsbench-metadata-worker: failed to read control input");
        result = 1;
    }
    if (!stop_metadata_path_workers(workers)) {
        std::println(stderr, "vfsbench-metadata-worker: failed to join metadata workers");
        result = 1;
    }
    return result;
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
