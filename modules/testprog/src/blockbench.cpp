#include "blockbench.hpp"

#include <fcntl.h>
#include <pthread.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

namespace {
constexpr size_t MAX_WORKERS = 8;
constexpr size_t FSYNC_BYTES = 4096;
constexpr size_t MAX_OPERATIONS = 1000;

struct Options {
    const char* path{};
    size_t workers{1};
    size_t block_bytes{128 * 1024};
    uint64_t bytes{64 * 1024 * 1024};
    size_t operations{100};
    unsigned pattern{0x5a};
    bool prefix{};
};

auto number(const char* text, uint64_t& value) -> bool {
    if (text == nullptr || *text == '\0' || *text == '-' || *text == '+') {
        return false;
    }
    errno = 0;
    char* end{};
    auto parsed = std::strtoull(text, &end, 10);
    if (errno != 0 || *end != '\0') {
        return false;
    }
    value = parsed;
    return true;
}

auto parse(int argc, char** argv, Options& options) -> bool {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--path-prefix") == 0) {
            options.prefix = true;
        } else if (std::strcmp(argv[i], "--path") == 0 && i + 1 < argc) {
            options.path = argv[++i];
        } else {
            if (i + 1 >= argc) {
                return false;
            }
            const char* key = argv[i++];
            uint64_t value{};
            if (!number(argv[i], value)) {
                return false;
            }
            if (std::strcmp(key, "--workers") == 0) {
                if (value == 0 || value > MAX_WORKERS) return false;
                options.workers = static_cast<size_t>(value);
            } else if (std::strcmp(key, "--block-bytes") == 0) {
                if (value == 0 || value > 2 * 1024 * 1024) return false;
                options.block_bytes = static_cast<size_t>(value);
            } else if (std::strcmp(key, "--bytes") == 0) {
                if (value == 0 || value > 1024ULL * 1024 * 1024) return false;
                options.bytes = value;
            } else if (std::strcmp(key, "--operations") == 0) {
                if (value == 0 || value > MAX_OPERATIONS) return false;
                options.operations = static_cast<size_t>(value);
            } else if (std::strcmp(key, "--pattern") == 0) {
                if (value > 255) return false;
                options.pattern = static_cast<unsigned>(value);
            } else {
                return false;
            }
        }
    }
    return options.path != nullptr && *options.path != '\0' && options.bytes % options.block_bytes == 0 &&
           (options.workers == 1 || options.prefix);
}

auto now_ns() -> uint64_t {
    timespec time{};
    if (clock_gettime(CLOCK_MONOTONIC, &time) != 0 || time.tv_sec < 0 || time.tv_nsec < 0) {
        return 0;
    }
    return static_cast<uint64_t>(time.tv_sec) * 1'000'000'000 + static_cast<uint64_t>(time.tv_nsec);
}

auto transfer(int fd, uint8_t* data, size_t bytes, bool write_data) -> int {
    size_t done = 0;
    while (done < bytes) {
        ssize_t const RC = write_data ? write(fd, data + done, bytes - done) : read(fd, data + done, bytes - done);
        if (RC < 0 && errno == EINTR) continue;
        if (RC <= 0) return RC == 0 ? EIO : errno;
        done += static_cast<size_t>(RC);
    }
    return 0;
}

struct ReadPool;
struct ReadWorker {
    ReadPool* pool{};
    size_t index{};
    uint64_t bytes{};
    uint64_t elapsed_ns{};
    int error{};
};
struct ReadPool {
    Options options{};
    pthread_mutex_t mutex{};
    pthread_cond_t condition{};
    size_t ready{};
    bool start{};
    bool abort{};
    std::array<ReadWorker, MAX_WORKERS> workers{};
    std::array<pthread_t, MAX_WORKERS> threads{};
};

auto read_worker(void* argument) -> void* {
    auto& worker = *static_cast<ReadWorker*>(argument);
    ReadPool& pool = *worker.pool;
    std::string path = pool.options.path;
    if (pool.options.prefix) path += "." + std::to_string(worker.index);
    // This flag bypasses per-open/detached VFS caches. Cold buffer-cache media
    // is supplied by the fresh-VM fixture; this flag alone does not evict it.
    int const FD = open(path.c_str(), O_RDONLY | O_NO_CACHE);
    std::vector<uint8_t> data(pool.options.block_bytes);
    std::vector<uint8_t> expected(pool.options.block_bytes, static_cast<uint8_t>(pool.options.pattern));
    if (FD < 0) worker.error = errno;
    pthread_mutex_lock(&pool.mutex);
    ++pool.ready;
    pthread_cond_broadcast(&pool.condition);
    while (!pool.start) pthread_cond_wait(&pool.condition, &pool.mutex);
    bool const ABORT = pool.abort;
    pthread_mutex_unlock(&pool.mutex);
    uint64_t const START = now_ns();
    while (!ABORT && worker.error == 0 && worker.bytes < pool.options.bytes) {
        worker.error = transfer(FD, data.data(), data.size(), false);
        if (worker.error == 0 && std::memcmp(data.data(), expected.data(), data.size()) != 0) worker.error = EILSEQ;
        if (worker.error == 0) worker.bytes += data.size();
    }
    worker.elapsed_ns = now_ns() - START;
    if (FD >= 0 && close(FD) != 0 && worker.error == 0) worker.error = errno;
    return nullptr;
}

auto run_read(const Options& options) -> int {
    ReadPool pool{};
    pool.options = options;
    if (pthread_mutex_init(&pool.mutex, nullptr) != 0) return 1;
    if (pthread_cond_init(&pool.condition, nullptr) != 0) {
        pthread_mutex_destroy(&pool.mutex);
        return 1;
    }
    size_t created = 0;
    int error = 0;
    for (; created < options.workers; ++created) {
        pool.workers[created].pool = &pool;
        pool.workers[created].index = created;
        error = pthread_create(&pool.threads[created], nullptr, read_worker, &pool.workers[created]);
        if (error != 0) break;
    }
    pthread_mutex_lock(&pool.mutex);
    while (pool.ready != created) pthread_cond_wait(&pool.condition, &pool.mutex);
    pool.abort = error != 0;
    uint64_t const START = now_ns();
    pool.start = true;
    pthread_cond_broadcast(&pool.condition);
    pthread_mutex_unlock(&pool.mutex);
    uint64_t bytes = 0;
    for (size_t i = 0; i < created; ++i) {
        int const RC = pthread_join(pool.threads[i], nullptr);
        if (error == 0) error = RC == 0 ? pool.workers[i].error : RC;
        bytes += pool.workers[i].bytes;
    }
    uint64_t const ELAPSED = now_ns() - START;
    pthread_cond_destroy(&pool.condition);
    pthread_mutex_destroy(&pool.mutex);
    if (ELAPSED == 0 && error == 0) error = EIO;
    std::printf(
        "{\"mode\":\"read\",\"workers\":%zu,\"block_bytes\":%zu,\"expected_bytes\":%llu,\"bytes\":%llu,"
        "\"elapsed_ns\":%llu,\"mib_per_second\":%.6f,\"pattern\":%u,\"error\":%d,\"pass\":%s}\n",
        options.workers, options.block_bytes, static_cast<unsigned long long>(options.bytes * options.workers),
        static_cast<unsigned long long>(bytes), static_cast<unsigned long long>(ELAPSED),
        ELAPSED == 0 ? 0.0 : static_cast<double>(bytes) * 1e9 / static_cast<double>(ELAPSED) / (1024 * 1024), options.pattern, error,
        error == 0 && bytes == options.bytes * options.workers ? "true" : "false");
    return error == 0 && bytes == options.bytes * options.workers ? 0 : 1;
}

auto run_fsync(const Options& options) -> int {
    // Mutations belong to a regular private fixture file. DEVFS fsync in the
    // original kernel is a no-op, and cannot provide durability measurements.
    struct stat existing{};
    if ((stat(options.path, &existing) == 0 && !S_ISREG(existing.st_mode)) || std::strncmp(options.path, "/dev/", 5) == 0) {
        std::fprintf(stderr, "blockbench-fsync requires a regular private file\n");
        return 2;
    }
    int const FD = open(options.path, O_RDWR | O_CREAT | O_TRUNC | O_NO_CACHE, 0600);
    if (FD < 0) return 1;
    std::array<uint8_t, FSYNC_BYTES> data{};
    std::array<uint64_t, MAX_OPERATIONS> latencies{};
    int error = 0;
    size_t completed = 0;
    for (; completed < options.operations; ++completed) {
        data.fill(static_cast<uint8_t>((options.pattern + completed) & 0xffU));
        uint64_t const START = now_ns();
        error = transfer(FD, data.data(), data.size(), true);
        if (error == 0 && fsync(FD) != 0) error = errno;
        latencies[completed] = now_ns() - START;
        if (error != 0) break;
    }
    if (close(FD) != 0 && error == 0) error = errno;
    if (error == 0) {
        int const VERIFY = open(options.path, O_RDONLY | O_NO_CACHE);
        if (VERIFY < 0) {
            error = errno;
        } else {
            std::array<uint8_t, FSYNC_BYTES> expected{};
            for (size_t i = 0; i < completed && error == 0; ++i) {
                expected.fill(static_cast<uint8_t>((options.pattern + i) & 0xffU));
                error = transfer(VERIFY, data.data(), data.size(), false);
                if (error == 0 && data != expected) error = EILSEQ;
            }
            if (close(VERIFY) != 0 && error == 0) error = errno;
        }
    }
    auto sorted = latencies;
    std::sort(sorted.begin(), sorted.begin() + static_cast<ptrdiff_t>(completed));
    size_t const P95_INDEX = completed == 0 ? 0 : (completed * 95 + 99) / 100 - 1;
    std::printf(
        "{\"mode\":\"write_fsync\",\"block_bytes\":%zu,\"operations\":%zu,\"expected_operations\":%zu,"
        "\"p95_ns\":%llu,\"error\":%d,\"pass\":%s,\"latencies_ns\":[",
        FSYNC_BYTES, completed, options.operations, static_cast<unsigned long long>(sorted[P95_INDEX]), error,
        error == 0 && completed == options.operations ? "true" : "false");
    for (size_t i = 0; i < completed; ++i) {
        std::printf("%s%llu", i == 0 ? "" : ",", static_cast<unsigned long long>(latencies[i]));
    }
    std::printf("]}\n");
    return error == 0 && completed == options.operations ? 0 : 1;
}
}  // namespace

auto run_blockbench(int argc, char** argv) -> int {
    Options options{};
    if (argc == 0 || !parse(argc, argv, options)) {
        std::fprintf(stderr,
                     "blockbench-read|blockbench-fsync --path PATH [--path-prefix] [--workers 1..8] "
                     "[--bytes N] [--block-bytes N] [--pattern 0..255] [--operations 1..1000]\n");
        return 2;
    }
    if (std::strcmp(argv[0], "blockbench-read") == 0) return run_read(options);
    if (std::strcmp(argv[0], "blockbench-fsync") == 0 && options.workers == 1 && !options.prefix) return run_fsync(options);
    return 2;
}
