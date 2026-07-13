#include "mandelbench_wki.hpp"

#include <abi-bits/fcntl.h>
#include <abi-bits/wait.h>
#include <bits/ssize_t.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/process.h>
#include <sys/vfs.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <format>
#include <iterator>
#include <limits>
#include <print>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "config.hpp"
#include "tinycthread.hpp"
#include "util.hpp"

#define MANDELBENCH_TRACE(...)                \
    do {                                     \
        if (MANDELBENCH_DEBUG_ENABLED) {     \
            std::println(stderr, __VA_ARGS__); \
        }                                    \
    } while (false)

namespace {

constexpr int EINTR_NEG = -4;  // WOS returns -errno from syscalls.
constexpr const char* DEVICE_NAME = "process";
constexpr uint64_t PIPE_READ_IDLE_TIMEOUT_US = 60'000'000;
constexpr uint64_t PIPE_PAYLOAD_IDLE_TIMEOUT_US = 300'000'000;
constexpr uint64_t WORKER_WAIT_TIMEOUT_US = 60'000'000;
constexpr uint64_t STATUS_LOG_INTERVAL_US = 1'000'000;
constexpr int WORKER_EVENT_POLL_TIMEOUT_MS = 10;
constexpr int WORKER_OUTPUT_FD = 3;
constexpr int WORKER_RELEASE_FD = 4;
constexpr uint32_t WORKER_OUTPUT_MAGIC = 0x31424D57;  // WMB1
constexpr uint16_t WORKER_OUTPUT_VERSION = 1;
constexpr uint16_t WORKER_OUTPUT_FLAG_PAYLOAD = 0;
constexpr size_t WORKER_OUTPUT_HEADER_SIZE = 16;
constexpr unsigned char WORKER_CONTROL_START = 1;
constexpr unsigned char WORKER_CONTROL_RELEASE_PAYLOAD = 2;
constexpr int MANDELBENCH_ROW_BAND_ROWS = 8;
constexpr size_t MANDELBENCH_PROFILE_WORKER_LIMIT = 8;
constexpr uint64_t USEC_PER_SEC = 1'000'000;
constexpr int64_t NSEC_PER_SEC = 1'000'000'000;
constexpr uint64_t NSEC_PER_USEC = 1'000;

auto now_us() -> uint64_t {
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    if (ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= NSEC_PER_SEC) {
        return 0;
    }

    uint64_t const NSEC_US = static_cast<uint64_t>(ts.tv_nsec) / NSEC_PER_USEC;
    auto const SEC = static_cast<uint64_t>(ts.tv_sec);
    if (SEC > (std::numeric_limits<uint64_t>::max() - NSEC_US) / USEC_PER_SEC) {
        return std::numeric_limits<uint64_t>::max();
    }

    return (SEC * USEC_PER_SEC) + NSEC_US;
}

auto elapsed_us(uint64_t start_us, uint64_t end_us) -> uint64_t {
    if (end_us <= start_us) {
        return 0;
    }
    return end_us - start_us;
}

auto elapsed_ms(uint64_t start_us, uint64_t end_us) -> double { return static_cast<double>(elapsed_us(start_us, end_us)) / 1000.0; }

auto repeat_prefix(int repeat_index) -> std::string {
    if (repeat_index < 0) {
        return "final";
    }
    return std::format("repeat {}", repeat_index);
}

auto mandelbench_profile_enabled() -> bool {
    const char* value = std::getenv("MANDELBENCH_PROFILE");
    return value != nullptr && value[0] != '\0' && value[0] != '0';
}

auto mandelbench_save_images_enabled() -> bool {
    const char* value = std::getenv("MANDELBENCH_SAVE_IMAGES");
    return value == nullptr || value[0] == '\0' || value[0] != '0';
}

auto mandelbench_payload_release_enabled() -> bool {
    const char* value = std::getenv("MANDELBENCH_PAYLOAD_RELEASE");
    return value != nullptr && value[0] != '\0' && value[0] != '0';
}

auto normalize_node_name(std::string_view node) -> std::string_view {
    constexpr std::string_view WOS_DNS_SUFFIX = ".wos";
    if (node.size() > WOS_DNS_SUFFIX.size() && node.ends_with(WOS_DNS_SUFFIX)) {
        return node.substr(0, node.size() - WOS_DNS_SUFFIX.size());
    }
    return node;
}

auto parse_node_list(const char* nodes) -> std::vector<std::string> {
    if (nodes == nullptr || nodes[0] == '\0') {
        return {};
    }

    std::vector<std::string> parsed;
    std::string_view text(nodes);
    size_t start = 0;
    while (start <= text.size()) {
        size_t const END = text.find(',', start);
        std::string_view token = END == std::string_view::npos ? text.substr(start) : text.substr(start, END - start);
        if (!token.empty()) {
            parsed.emplace_back(normalize_node_name(token));
        }
        if (END == std::string_view::npos) {
            break;
        }
        start = END + 1;
    }
    return parsed;
}

auto join_node_list(std::span<const std::string> nodes) -> std::string {
    if (nodes.empty()) {
        return "auto";
    }

    std::string out;
    for (const auto& node : nodes) {
        if (!out.empty()) {
            out += ',';
        }
        out += node;
    }
    return out;
}

auto parse_int_arg(const char* text, int& value) -> bool {
    char* end = nullptr;
    errno = 0;
    const long PARSED = std::strtol(text, &end, 10);
    if (text == end || *end != '\0' || errno == ERANGE || PARSED < std::numeric_limits<int>::min() ||
        PARSED > std::numeric_limits<int>::max()) {
        return false;
    }

    value = static_cast<int>(PARSED);
    return true;
}

auto write_all(int fd, std::span<const unsigned char> bytes, ssize_t* failed_ret = nullptr, int* failed_errno = nullptr) -> bool {
    size_t written_total = 0;
    bool const CONTROL_DIAG = MANDELBENCH_DEBUG_ENABLED && bytes.size() == 1;
    while (written_total < bytes.size()) {
        if (CONTROL_DIAG) {
            MANDELBENCH_TRACE("mandelbench: write_all control fd={} offset={} size={} syscall begin", fd, written_total, bytes.size());
        }
        errno = 0;
        ssize_t const WRITTEN = write(fd, bytes.data() + written_total, bytes.size() - written_total);
        if (CONTROL_DIAG) {
            MANDELBENCH_TRACE("mandelbench: write_all control fd={} offset={} ret={} errno={} syscall end", fd, written_total, WRITTEN,
                              errno);
        }
        if (WRITTEN <= 0) {
            if (failed_ret != nullptr) {
                *failed_ret = WRITTEN;
            }
            if (failed_errno != nullptr) {
                *failed_errno = errno;
            }
            return false;
        }
        written_total += static_cast<size_t>(WRITTEN);
    }
    return true;
}

auto write_text_fd(int fd, std::string_view text) -> bool {
    auto const* raw_bytes = reinterpret_cast<const unsigned char*>(text.data());
    return write_all(fd, std::span(raw_bytes, text.size()));
}

auto fd_is_open(int fd) -> bool {
    errno = 0;
    if (fcntl(fd, F_GETFD, 0) >= 0) {
        return true;
    }
    return errno != EBADF;
}

auto wait_for_control_byte(int fd, int worker_id, const char* label) -> bool {
    if (fd < 0) {
        return true;
    }

    MANDELBENCH_TRACE("mandelbench-worker[{}]: waiting for {} on fd {}", worker_id, label, fd);
    unsigned char control_byte = 0;
    while (true) {
        errno = 0;
        ssize_t const BYTES_READ = read(fd, &control_byte, 1);
        if (BYTES_READ == 1) {
            MANDELBENCH_TRACE("mandelbench-worker[{}]: received {} byte={} fd={}", worker_id, label, static_cast<unsigned>(control_byte),
                              fd);
            return true;
        }
        if (BYTES_READ == EINTR_NEG || errno == EINTR) {
            continue;
        }
        if (BYTES_READ == 0) {
            std::println(stderr, "mandelbench-worker[{}]: control pipe closed while waiting for {}", worker_id, label);
        } else {
            std::println(stderr, "mandelbench-worker[{}]: control read failed while waiting for {} ret={} errno={} ({})", worker_id,
                         label, BYTES_READ, errno, std::strerror(errno));
        }
        return false;
    }
}

void store_u16(std::span<unsigned char> out, size_t offset, uint16_t value) { std::memcpy(out.data() + offset, &value, sizeof(value)); }

void store_u32(std::span<unsigned char> out, size_t offset, uint32_t value) { std::memcpy(out.data() + offset, &value, sizeof(value)); }

auto load_u16(std::span<const unsigned char> in, size_t offset) -> uint16_t {
    uint16_t value = 0;
    std::memcpy(&value, in.data() + offset, sizeof(value));
    return value;
}

auto load_u32(std::span<const unsigned char> in, size_t offset) -> uint32_t {
    uint32_t value = 0;
    std::memcpy(&value, in.data() + offset, sizeof(value));
    return value;
}

auto make_worker_header(int worker_id, size_t payload_size, uint16_t flags, std::array<unsigned char, WORKER_OUTPUT_HEADER_SIZE>& out)
    -> bool {
    if (worker_id < 0 || payload_size > std::numeric_limits<uint32_t>::max()) {
        return false;
    }

    out.fill(0);
    store_u32(out, 0, WORKER_OUTPUT_MAGIC);
    store_u16(out, 4, WORKER_OUTPUT_VERSION);
    store_u16(out, 6, flags);
    store_u32(out, 8, static_cast<uint32_t>(worker_id));
    store_u32(out, 12, static_cast<uint32_t>(payload_size));
    return true;
}

auto make_worker_output_header(int worker_id, size_t payload_size, std::array<unsigned char, WORKER_OUTPUT_HEADER_SIZE>& out) -> bool {
    return make_worker_header(worker_id, payload_size, WORKER_OUTPUT_FLAG_PAYLOAD, out);
}

auto get_testprog_path() -> const char* { return "/usr/bin/testprog"; }

auto wki_launcher_node() -> std::string {
    std::array<char, 64> node{};
    ker::process::wki_launcher_node(node.data(), node.size());
    return node.data();
}

auto wki_runner_node() -> std::string {
    std::array<char, 64> node{};
    ker::process::wki_runner_node(node.data(), node.size());
    return node.data();
}

struct WorkerThreadArg {
    unsigned char* image;
    unsigned char* colormap;
    int width;
    int height;
    int max_iteration;
    int start_row;
    int row_count;
    int local_thread_id;
    int local_thread_count;
    int rows_done;
    int diag_worker_id;
    int diag_repeat_index;
    int diag_progress_stride;
    uint64_t diag_compute_start_us;
};

struct WorkerChunk {
    int worker_id;
    int start_row;
    int row_count;
};

struct WorkerBandThreadArg {
    unsigned char* image;
    unsigned char* colormap;
    int width;
    int height;
    int max_iteration;
    const std::vector<WorkerChunk>* chunks;
    const std::vector<size_t>* chunk_offsets;
    std::atomic<int>* next_chunk;
    std::atomic<int>* total_rows_done;
    int rows_done;
    int local_thread_id;
    int local_thread_count;
    int diag_worker_id;
    int diag_repeat_index;
    int diag_progress_stride;
    uint64_t diag_compute_start_us;
};

auto generate_rows(void* param) -> int {
    auto* arg = static_cast<WorkerThreadArg*>(param);
    int thread_rows_done = 0;
    int const THREAD_ROW_TOTAL =
        arg->local_thread_id < arg->row_count
            ? ((arg->row_count - 1 - arg->local_thread_id) / std::max(arg->local_thread_count, 1)) + 1
            : 0;
    int const PROGRESS_STRIDE = std::max(arg->diag_progress_stride, 1);

    for (int local_row = arg->local_thread_id; local_row < arg->row_count; local_row += arg->local_thread_count) {
        int const ROW = arg->start_row + local_row;
        for (int col = 0; col < arg->width; col++) {
            double const C_RE = (col - (arg->width / 2.0)) * 4.0 / arg->width;
            double const C_IM = (ROW - (arg->height / 2.0)) * 4.0 / arg->width;
            double x = 0;
            double y = 0;
            int iteration = 0;
            while ((x * x) + (y * y) <= 4 && iteration < arg->max_iteration) {
                double const X_NEW = (x * x) - (y * y) + C_RE;
                y = (2 * x * y) + C_IM;
                x = X_NEW;
                iteration++;
            }
            iteration = std::min(iteration, arg->max_iteration);
            set_pixel(arg->image, arg->width, col, local_row, &arg->colormap[static_cast<size_t>(iteration * 3)]);
        }
        arg->rows_done++;
        thread_rows_done++;
        if (arg->diag_worker_id >= 0 &&
            (thread_rows_done == 1 || (thread_rows_done % PROGRESS_STRIDE) == 0 || thread_rows_done == THREAD_ROW_TOTAL)) {
            MANDELBENCH_TRACE(
                "mandelbench-worker[{}]: repeat {} compute progress thread={}/{} rows={}/{} last_row={} elapsed_ms={:.3f}",
                arg->diag_worker_id, arg->diag_repeat_index, arg->local_thread_id, arg->local_thread_count, thread_rows_done,
                THREAD_ROW_TOTAL, ROW, elapsed_ms(arg->diag_compute_start_us, now_us()));
        }
    }
    return 0;
}

auto generate_dynamic_chunk_rows(void* param) -> int {
    auto* arg = static_cast<WorkerBandThreadArg*>(param);
    arg->rows_done = 0;

    while (true) {
        int const CHUNK_INDEX = arg->next_chunk->fetch_add(1, std::memory_order_relaxed);
        if (CHUNK_INDEX >= static_cast<int>(arg->chunks->size())) {
            break;
        }

        auto const CHUNK_OFFSET = static_cast<size_t>(CHUNK_INDEX);
        const auto& chunk = arg->chunks->at(CHUNK_OFFSET);
        WorkerThreadArg row_arg{
            .image = arg->image + arg->chunk_offsets->at(CHUNK_OFFSET),
            .colormap = arg->colormap,
            .width = arg->width,
            .height = arg->height,
            .max_iteration = arg->max_iteration,
            .start_row = chunk.start_row,
            .row_count = chunk.row_count,
            .local_thread_id = 0,
            .local_thread_count = 1,
            .rows_done = 0,
            .diag_worker_id = arg->diag_worker_id,
            .diag_repeat_index = arg->diag_repeat_index,
            .diag_progress_stride = arg->diag_progress_stride,
            .diag_compute_start_us = arg->diag_compute_start_us,
        };
        (void)generate_rows(&row_arg);
        arg->rows_done += row_arg.rows_done;
        arg->total_rows_done->fetch_add(row_arg.rows_done, std::memory_order_relaxed);
    }
    return 0;
}

struct WorkerLaunch {
    int output_slot;
    int thread_count;
    int start_row;
    int row_count;
    std::vector<WorkerChunk> chunks;
    int64_t child_pid;
    uint64_t fork_begin_us;
    uint64_t fork_return_us;
    int pipe_read_fd;
    int pipe_write_fd;
    int release_read_fd;
    int release_write_fd;
    bool read_ok;
    bool wait_done;
    bool wait_ok;
    int32_t wait_status;
    uint64_t wait_end_us;
    bool header_ok;
    bool payload_released;
    uint64_t header_begin_us;
    uint64_t header_end_us;
    uint64_t header_last_progress_us;
    uint64_t header_last_status_us;
    std::array<unsigned char, WORKER_OUTPUT_HEADER_SIZE> header;
    size_t header_offset;
    uint64_t read_begin_us;
    uint64_t read_end_us;
    uint64_t read_last_progress_us;
    uint64_t read_last_status_us;
    unsigned char* read_dest;
    size_t read_target;
    size_t read_offset;
    std::vector<unsigned char> read_buffer;
    bool local;
    std::string target_node;
};

struct LocalComputeThread {
    WorkerBandThreadArg arg{};
    thrd_t thread{};
    int worker_id = -1;
};

auto worker_chunk_bytes(const WorkerChunk& chunk, size_t row_size) -> size_t { return static_cast<size_t>(chunk.row_count) * row_size; }

auto worker_chunks_bytes(std::span<const WorkerChunk> chunks, size_t row_size) -> size_t {
    size_t bytes = 0;
    for (const auto& chunk : chunks) {
        bytes += worker_chunk_bytes(chunk, row_size);
    }
    return bytes;
}

auto parse_nonnegative_int(std::string_view text, int& value) -> bool {
    if (text.empty()) {
        return false;
    }

    int parsed = 0;
    for (char const CH : text) {
        if (CH < '0' || CH > '9') {
            return false;
        }
        int const DIGIT = CH - '0';
        if (parsed > (std::numeric_limits<int>::max() - DIGIT) / 10) {
            return false;
        }
        parsed = (parsed * 10) + DIGIT;
    }
    value = parsed;
    return true;
}

auto parse_worker_chunk(std::string_view text, WorkerChunk& chunk) -> bool {
    size_t const FIRST = text.find(':');
    if (FIRST == std::string_view::npos) {
        return false;
    }
    size_t const SECOND = text.find(':', FIRST + 1);
    if (SECOND == std::string_view::npos || text.find(':', SECOND + 1) != std::string_view::npos) {
        return false;
    }

    WorkerChunk parsed{};
    if (!parse_nonnegative_int(text.substr(0, FIRST), parsed.worker_id) ||
        !parse_nonnegative_int(text.substr(FIRST + 1, SECOND - FIRST - 1), parsed.start_row) ||
        !parse_nonnegative_int(text.substr(SECOND + 1), parsed.row_count) || parsed.row_count <= 0) {
        return false;
    }

    chunk = parsed;
    return true;
}

auto parse_worker_chunks(std::string_view text, std::vector<WorkerChunk>& chunks) -> bool {
    chunks.clear();
    if (text.empty()) {
        return false;
    }

    size_t start = 0;
    while (start <= text.size()) {
        size_t const END = text.find(';', start);
        std::string_view const TOKEN = END == std::string_view::npos ? text.substr(start) : text.substr(start, END - start);
        WorkerChunk chunk{};
        if (!parse_worker_chunk(TOKEN, chunk)) {
            chunks.clear();
            return false;
        }
        chunks.push_back(chunk);
        if (END == std::string_view::npos) {
            break;
        }
        start = END + 1;
    }
    return !chunks.empty();
}

auto format_worker_chunks(std::span<const WorkerChunk> chunks) -> std::string {
    std::string out;
    for (const auto& chunk : chunks) {
        if (!out.empty()) {
            out += ';';
        }
        out += std::format("{}:{}:{}", chunk.worker_id, chunk.start_row, chunk.row_count);
    }
    return out;
}

auto make_worker_launch(int output_slot, std::string target_node, int thread_count = 1, bool local = false) -> WorkerLaunch {
    return WorkerLaunch{
        .output_slot = output_slot,
        .thread_count = std::max(1, thread_count),
        .start_row = 0,
        .row_count = 0,
        .chunks = {},
        .child_pid = -1,
        .fork_begin_us = 0,
        .fork_return_us = 0,
        .pipe_read_fd = -1,
        .pipe_write_fd = -1,
        .release_read_fd = -1,
        .release_write_fd = -1,
        .read_ok = false,
        .wait_done = false,
        .wait_ok = false,
        .wait_status = 0,
        .wait_end_us = 0,
        .header_ok = false,
        .payload_released = false,
        .header_begin_us = 0,
        .header_end_us = 0,
        .header_last_progress_us = 0,
        .header_last_status_us = 0,
        .header = {},
        .header_offset = 0,
        .read_begin_us = 0,
        .read_end_us = 0,
        .read_last_progress_us = 0,
        .read_last_status_us = 0,
        .read_dest = nullptr,
        .read_target = 0,
        .read_offset = 0,
        .read_buffer = {},
        .local = local,
        .target_node = std::move(target_node),
    };
}

void add_chunk_to_launch(WorkerLaunch& launch, const WorkerChunk& chunk) {
    if (launch.chunks.empty()) {
        launch.start_row = chunk.start_row;
    }
    launch.row_count += chunk.row_count;
    launch.chunks.push_back(chunk);
}

struct WorkerSlot {
    int worker_id;
    std::string target_node;
    bool local;
};

auto count_slots_for_target(std::span<const WorkerSlot> slots, std::string_view target_node, bool local) -> int {
    return static_cast<int>(std::count_if(slots.begin(), slots.end(), [&](const WorkerSlot& slot) {
        return slot.local == local && slot.target_node == target_node;
    }));
}

auto find_launch_for_target(std::vector<WorkerLaunch>& launches, std::string_view target_node) -> std::vector<WorkerLaunch>::iterator {
    return std::ranges::find_if(launches, [&](const WorkerLaunch& launch) { return launch.target_node == target_node; });
}

auto find_launch_for_output_slot(std::vector<WorkerLaunch>& launches, int output_slot) -> std::vector<WorkerLaunch>::iterator {
    return std::ranges::find_if(launches, [&](const WorkerLaunch& launch) { return launch.output_slot == output_slot; });
}

auto make_chunk_offsets(std::span<const WorkerChunk> chunks, size_t row_size, bool compact_payload) -> std::vector<size_t> {
    std::vector<size_t> offsets(chunks.size());
    size_t next_offset = 0;
    for (size_t chunk_index = 0; chunk_index < chunks.size(); ++chunk_index) {
        const auto& chunk = chunks[chunk_index];
        offsets[chunk_index] = compact_payload ? next_offset : static_cast<size_t>(chunk.start_row) * row_size;
        next_offset += worker_chunk_bytes(chunk, row_size);
    }
    return offsets;
}

void close_fd(int& fd) {
    if (fd >= 0) {
        close(fd);
        fd = -1;
    }
}

void close_child_pipe_fds(std::span<const WorkerLaunch> launches, int keep_output_write_fd, int keep_release_read_fd) {
    for (const auto& launch : launches) {
        if (launch.pipe_read_fd >= 0) {
            close(launch.pipe_read_fd);
        }
        if (launch.pipe_write_fd >= 0 && launch.pipe_write_fd != keep_output_write_fd) {
            close(launch.pipe_write_fd);
        }
        if (launch.release_read_fd >= 0 && launch.release_read_fd != keep_release_read_fd) {
            close(launch.release_read_fd);
        }
        if (launch.release_write_fd >= 0) {
            close(launch.release_write_fd);
        }
    }
}

auto create_worker_pipe(WorkerLaunch& launch) -> bool {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        std::println(stderr, "mandelbench: pipe failed for worker {} errno={} ({})", launch.output_slot, errno, std::strerror(errno));
        return false;
    }
    launch.pipe_read_fd = fds.at(0);
    launch.pipe_write_fd = fds.at(1);
    return true;
}

auto create_worker_release_pipe(WorkerLaunch& launch) -> bool {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        std::println(stderr, "mandelbench: release pipe failed for worker {} errno={} ({})", launch.output_slot, errno,
                     std::strerror(errno));
        return false;
    }
    launch.release_read_fd = fds.at(0);
    launch.release_write_fd = fds.at(1);
    return true;
}

auto move_child_output_fd(int pipe_write_fd) -> bool {
    if (pipe_write_fd < 0) {
        return false;
    }
    if (pipe_write_fd == WORKER_OUTPUT_FD) {
        return true;
    }
    if (dup2(pipe_write_fd, WORKER_OUTPUT_FD) < 0) {
        return false;
    }
    close(pipe_write_fd);
    return true;
}

auto move_child_release_fd(int release_read_fd) -> bool {
    if (release_read_fd < 0) {
        return false;
    }
    if (release_read_fd == WORKER_RELEASE_FD) {
        return true;
    }
    if (dup2(release_read_fd, WORKER_RELEASE_FD) < 0) {
        return false;
    }
    close(release_read_fd);
    return true;
}

void close_standard_fds_for_worker_child() {
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
}

void install_worker_vfs_policy(int worker_id, const std::string& target_node) {
    auto add_local_rule = [&](const char* path, const char* purpose) {
        int const RC = ker::abi::vfs::wki_rule_add_vfs(path, ker::abi::vfs::WKI_VFS_ROUTE_LOCAL);
        if (RC < 0) {
            std::println(stderr,
                         "mandelbench: worker {} on {} failed to keep {} local at {} (rc={}); continuing with inherited VFS policy",
                         worker_id, target_node, purpose, path, RC);
        }
    };

    add_local_rule("/usr", "worker runtime");
    add_local_rule("/bin", "worker runtime fallback");
    add_local_rule("/lib", "worker runtime");
    add_local_rule("/lib64", "worker runtime");
    add_local_rule("/usr/bin/testprog", "worker executable");
    add_local_rule("/bin/testprog", "worker executable fallback");
}

auto set_fd_nonblocking(int fd, bool enabled) -> bool {
    int const FLAGS = fcntl(fd, F_GETFL, 0);
    if (FLAGS < 0) {
        return false;
    }

    int const NEXT_FLAGS = enabled ? (FLAGS | O_NONBLOCK) : (FLAGS & ~O_NONBLOCK);
    return fcntl(fd, F_SETFL, NEXT_FLAGS) == 0;
}

auto read_is_pending(const WorkerLaunch& launch) -> bool { return launch.pipe_read_fd >= 0 && !launch.read_ok; }
auto header_is_pending(const WorkerLaunch& launch) -> bool { return launch.pipe_read_fd >= 0 && !launch.header_ok; }

auto validate_worker_output_header(WorkerLaunch& launch, uint16_t expected_flags) -> bool {
    std::span<const unsigned char> const HEADER(launch.header.data(), launch.header.size());
    uint32_t const MAGIC = load_u32(HEADER, 0);
    uint16_t const VERSION = load_u16(HEADER, 4);
    uint16_t const FLAGS = load_u16(HEADER, 6);
    uint32_t const WORKER_ID = load_u32(HEADER, 8);
    uint32_t const PAYLOAD_SIZE = load_u32(HEADER, 12);
    auto const EXPECTED_WORKER_ID = static_cast<uint32_t>(launch.output_slot);
    if (MAGIC != WORKER_OUTPUT_MAGIC || VERSION != WORKER_OUTPUT_VERSION || FLAGS != expected_flags || WORKER_ID != EXPECTED_WORKER_ID ||
        static_cast<size_t>(PAYLOAD_SIZE) != launch.read_target) {
        std::println(stderr,
                     "mandelbench: bad output header for worker {} magic=0x{:08x} version={} flags={} expected_flags={} id={} "
                     "payload={} expected_payload={}",
                     launch.output_slot, MAGIC, VERSION, FLAGS, expected_flags, WORKER_ID, PAYLOAD_SIZE, launch.read_target);
        return false;
    }
    return true;
}

auto drain_worker_header_final(WorkerLaunch& launch, uint16_t expected_flags) -> bool {
    if (launch.pipe_read_fd < 0) {
        return launch.header_ok;
    }
    if (launch.header_begin_us == 0) {
        launch.header_begin_us = now_us();
        launch.header_last_progress_us = launch.header_begin_us;
    }

    while (launch.header_offset < launch.header.size()) {
        errno = 0;
        ssize_t const BYTES_READ =
            read(launch.pipe_read_fd, launch.header.data() + launch.header_offset, launch.header.size() - launch.header_offset);
        if (BYTES_READ > 0) {
            launch.header_offset += static_cast<size_t>(BYTES_READ);
            launch.header_last_progress_us = now_us();
            continue;
        }
        if (BYTES_READ == 0) {
            launch.header_end_us = now_us();
            std::println(stderr, "mandelbench: short output header for worker {} read={}/{}", launch.output_slot, launch.header_offset,
                         launch.header.size());
            close_fd(launch.pipe_read_fd);
            return false;
        }
        if (BYTES_READ == EINTR_NEG || errno == EINTR) {
            continue;
        }
        if (BYTES_READ == -EAGAIN || BYTES_READ == -EWOULDBLOCK || errno == EAGAIN || errno == EWOULDBLOCK) {
            return true;
        }

        launch.header_end_us = now_us();
        std::println(stderr, "mandelbench: output header read failed for worker {} ret={} errno={} ({})", launch.output_slot, BYTES_READ,
                     errno, std::strerror(errno));
        close_fd(launch.pipe_read_fd);
        return false;
    }

    if (!validate_worker_output_header(launch, expected_flags)) {
        launch.header_end_us = now_us();
        close_fd(launch.pipe_read_fd);
        return false;
    }

    launch.header_ok = true;
    launch.header_end_us = now_us();
    return true;
}

auto drain_worker_output_final(WorkerLaunch& launch) -> bool {
    if (launch.pipe_read_fd < 0) {
        return launch.read_ok;
    }
    if (launch.read_dest == nullptr || launch.read_offset > launch.read_target) {
        std::println(stderr, "mandelbench: invalid read target for worker {}", launch.output_slot);
        launch.read_end_us = now_us();
        close_fd(launch.pipe_read_fd);
        return false;
    }
    if (launch.read_begin_us == 0) {
        launch.read_begin_us = now_us();
        launch.read_last_progress_us = launch.read_begin_us;
    }

    while (launch.read_offset < launch.read_target) {
        errno = 0;
        ssize_t const BYTES_READ =
            read(launch.pipe_read_fd, launch.read_dest + launch.read_offset, launch.read_target - launch.read_offset);
        if (BYTES_READ > 0) {
            launch.read_offset += static_cast<size_t>(BYTES_READ);
            launch.read_last_progress_us = now_us();
            continue;
        }
        if (BYTES_READ == 0) {
            launch.read_end_us = now_us();
            std::println(stderr, "mandelbench: short pipe read for worker {} read={}/{}", launch.output_slot, launch.read_offset,
                         launch.read_target);
            close_fd(launch.pipe_read_fd);
            return false;
        }
        if (BYTES_READ == EINTR_NEG || errno == EINTR) {
            continue;
        }
        if (BYTES_READ == -EAGAIN || BYTES_READ == -EWOULDBLOCK || errno == EAGAIN || errno == EWOULDBLOCK) {
            return true;
        }

        launch.read_end_us = now_us();
        std::println(stderr, "mandelbench: pipe read failed for worker {} ret={} errno={} ({})", launch.output_slot, BYTES_READ, errno,
                     std::strerror(errno));
        close_fd(launch.pipe_read_fd);
        return false;
    }

    launch.read_ok = true;
    launch.read_end_us = now_us();
    return true;
}

void close_worker_pipes(std::span<WorkerLaunch> launches) {
    for (auto& launch : launches) {
        close_fd(launch.pipe_read_fd);
        close_fd(launch.pipe_write_fd);
        close_fd(launch.release_read_fd);
        close_fd(launch.release_write_fd);
    }
}

auto signal_workers(std::span<WorkerLaunch> launches, unsigned char signal_byte, std::string_view label, bool close_after_signal) -> bool {
    bool ok = true;
    for (auto& launch : launches) {
        if (launch.release_write_fd < 0) {
            continue;
        }
        MANDELBENCH_TRACE("mandelbench: signal {} worker={} fd={} begin", label, launch.output_slot, launch.release_write_fd);
        ssize_t write_fail_ret = 0;
        int write_fail_errno = 0;
        if (!write_all(launch.release_write_fd, std::span(&signal_byte, size_t{1}), &write_fail_ret, &write_fail_errno)) {
            std::println(stderr, "mandelbench: failed to signal worker {} {} ret={} errno={} ({})", launch.output_slot, label,
                         write_fail_ret, write_fail_errno, std::strerror(write_fail_errno));
            ok = false;
        }
        MANDELBENCH_TRACE("mandelbench: signal {} worker={} fd={} end ok={}", label, launch.output_slot, launch.release_write_fd, ok);
        if (close_after_signal) {
            close_fd(launch.release_write_fd);
        }
    }
    return ok;
}

auto signal_worker_starts(std::span<WorkerLaunch> launches) -> bool {
    return signal_workers(launches, WORKER_CONTROL_START, "start", false);
}

auto release_worker_payload(WorkerLaunch& launch, int repeat_index) -> bool {
    if (launch.payload_released) {
        return true;
    }

    unsigned char const SIGNAL = WORKER_CONTROL_RELEASE_PAYLOAD;
    ssize_t write_fail_ret = 0;
    int write_fail_errno = 0;
    MANDELBENCH_TRACE("mandelbench: repeat {} payload-release worker={} fd={} begin", repeat_index, launch.output_slot,
                      launch.release_write_fd);
    if (!write_all(launch.release_write_fd, std::span(&SIGNAL, size_t{1}), &write_fail_ret, &write_fail_errno)) {
        std::println(stderr, "mandelbench: failed to release worker {} payload ret={} errno={} ({})", launch.output_slot, write_fail_ret,
                     write_fail_errno, std::strerror(write_fail_errno));
        return false;
    }
    launch.payload_released = true;
    MANDELBENCH_TRACE("mandelbench: repeat {} payload-release worker={} end", repeat_index, launch.output_slot);
    return true;
}

auto poll_worker_exit(WorkerLaunch& launch) -> bool {
    if (launch.wait_done || launch.child_pid < 0) {
        return true;
    }

    int32_t status = 0;
    int64_t ret = 0;
    while (true) {
        ret = ker::process::waitpid(launch.child_pid, &status, WNOHANG, nullptr);
        if (ret != EINTR_NEG) {
            break;
        }
    }

    if (ret == 0) {
        return true;
    }

    launch.wait_done = true;
    launch.wait_status = status;
    launch.wait_end_us = now_us();

    if (ret < 0) {
        std::println(stderr, "mandelbench: waitpid({}) failed for worker {} target='{}': {}", launch.child_pid, launch.output_slot,
                     launch.target_node, ret);
        launch.wait_ok = false;
        return false;
    }
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        launch.wait_ok = true;
        return true;
    }
    if (WIFEXITED(status)) {
        std::println(stderr, "mandelbench: worker {} exited with code {}", launch.output_slot, WEXITSTATUS(status));
    } else {
        std::println(stderr, "mandelbench: worker {} terminated with signal {}", launch.output_slot, WTERMSIG(status));
    }
    launch.wait_ok = false;
    return false;
}

auto set_child_target(const std::string& target_node, int worker_id) -> bool {
    uint32_t const INHERIT_FLAGS = ker::process::WKI_TARGET_FLAG_STRICT | ker::process::WKI_TARGET_FLAG_NOINHERIT;
    if (target_node.empty()) {
        uint32_t const FLAGS = INHERIT_FLAGS | ker::process::WKI_TARGET_FLAG_REMOTE;
        int64_t const RC = ker::process::setwkitarget(nullptr, 0, FLAGS);
        if (RC < 0) {
            std::println(stderr, "mandelbench: failed to auto-target worker {} to remote node: {}", worker_id, static_cast<long>(RC));
            return false;
        }
        return true;
    }

    uint32_t const FLAGS = INHERIT_FLAGS;
    int64_t const RC = ker::process::setwkitarget(target_node.c_str(), static_cast<uint64_t>(target_node.size()), FLAGS);
    if (RC < 0) {
        std::println(stderr, "mandelbench: failed to target worker {} to '{}': {}", worker_id, target_node, static_cast<long>(RC));
        return false;
    }
    return true;
}

void reset_worker_transfer(WorkerLaunch& launch, unsigned char* read_dest, size_t read_target) {
    launch.read_dest = read_dest;
    launch.read_target = read_target;
    launch.read_offset = 0;
    launch.header_ok = false;
    launch.payload_released = false;
    launch.header_begin_us = 0;
    launch.header_end_us = 0;
    launch.header_last_progress_us = 0;
    launch.header_last_status_us = 0;
    launch.header.fill(0);
    launch.header_offset = 0;
    launch.read_ok = false;
    launch.read_begin_us = 0;
    launch.read_end_us = 0;
    launch.read_last_progress_us = 0;
    launch.read_last_status_us = 0;
}

void configure_worker_read_targets(std::span<WorkerLaunch> launches, unsigned char* /*image*/, size_t row_size) {
    for (auto& launch : launches) {
        size_t const PAYLOAD_SIZE = worker_chunks_bytes(launch.chunks, row_size);
        launch.read_buffer.assign(PAYLOAD_SIZE, 0);
        reset_worker_transfer(launch, launch.read_buffer.data(), PAYLOAD_SIZE);
    }
}

auto scatter_worker_outputs(std::span<const WorkerLaunch> launches, unsigned char* image, size_t row_size) -> bool {
    for (const auto& launch : launches) {
        if (!launch.read_ok || launch.read_offset != launch.read_target) {
            std::println(stderr, "mandelbench: remote worker {} output incomplete read={}/{}", launch.output_slot, launch.read_offset,
                         launch.read_target);
            return false;
        }

        size_t payload_offset = 0;
        for (const auto& chunk : launch.chunks) {
            size_t const BYTES = worker_chunk_bytes(chunk, row_size);
            size_t const IMAGE_OFFSET = static_cast<size_t>(chunk.start_row) * row_size;
            if (payload_offset + BYTES > launch.read_buffer.size()) {
                std::println(stderr, "mandelbench: remote worker {} chunk {} overruns payload offset={} bytes={} payload={}",
                             launch.output_slot, chunk.worker_id, payload_offset, BYTES, launch.read_buffer.size());
                return false;
            }
            std::memcpy(image + IMAGE_OFFSET, launch.read_buffer.data() + payload_offset, BYTES);
            payload_offset += BYTES;
        }
        if (payload_offset != launch.read_buffer.size()) {
            std::println(stderr, "mandelbench: remote worker {} payload size mismatch scattered={} payload={}", launch.output_slot,
                         payload_offset, launch.read_buffer.size());
            return false;
        }
    }
    return true;
}

auto worker_profile_done_us(const WorkerLaunch& launch) -> uint64_t { return std::max(launch.header_end_us, launch.read_end_us); }

void print_slowest_worker_profiles(std::span<const WorkerLaunch> launches, int repeat_index, uint64_t repeat_start_us) {
    std::vector<const WorkerLaunch*> sorted;
    sorted.reserve(launches.size());
    for (const auto& launch : launches) {
        sorted.push_back(&launch);
    }
    std::ranges::sort(sorted, [](const WorkerLaunch* lhs, const WorkerLaunch* rhs) {
        return worker_profile_done_us(*lhs) > worker_profile_done_us(*rhs);
    });

    size_t const COUNT = std::min(MANDELBENCH_PROFILE_WORKER_LIMIT, sorted.size());
    for (size_t rank = 0; rank < COUNT; ++rank) {
        const auto& launch = *sorted.at(rank);
        uint64_t const HEADER_END_US = std::max(launch.header_end_us, repeat_start_us);
        uint64_t const READ_END_US = std::max(launch.read_end_us, HEADER_END_US);
        std::println(stderr,
                     "mandelbench-profile-worker repeat={} rank={} worker={} target='{}' pid={} total_ms={:.3f} header_ms={:.3f} "
                     "payload_ms={:.3f} bytes={} read={}/{} header_ok={} read_ok={} chunks='{}'",
                     repeat_index, rank, launch.output_slot, launch.target_node, launch.child_pid,
                     elapsed_ms(repeat_start_us, READ_END_US), elapsed_ms(repeat_start_us, HEADER_END_US),
                     elapsed_ms(HEADER_END_US, READ_END_US), launch.read_target, launch.read_offset, launch.read_target,
                     launch.header_ok, launch.read_ok, format_worker_chunks(launch.chunks));
    }
}

[[maybe_unused]] auto compute_local_launches(std::span<const WorkerLaunch> launches, unsigned char* image, unsigned char* colormap,
                                             int width, int height, int max_iteration, size_t row_size, int repeat_index) -> bool {
    if (launches.empty()) {
        return true;
    }

    size_t thread_count = 0;
    for (const auto& launch : launches) {
        thread_count += static_cast<size_t>(std::clamp(launch.thread_count, 1, static_cast<int>(launch.chunks.size())));
    }
    std::vector<LocalComputeThread> compute_threads(thread_count);
    std::vector<std::vector<size_t>> chunk_offsets;
    chunk_offsets.reserve(launches.size());
    for (const auto& launch : launches) {
        chunk_offsets.push_back(make_chunk_offsets(launch.chunks, row_size, false));
    }
    std::vector<std::atomic<int>> next_chunks(launches.size());
    std::vector<std::atomic<int>> rows_done_by_launch(launches.size());
    for (size_t launch_index = 0; launch_index < launches.size(); ++launch_index) {
        next_chunks.at(launch_index).store(0, std::memory_order_relaxed);
        rows_done_by_launch.at(launch_index).store(0, std::memory_order_relaxed);
    }

    bool ok = true;
    size_t created_threads = 0;
    for (size_t launch_index = 0; launch_index < launches.size(); ++launch_index) {
        const auto& launch = launches[launch_index];
        int const THREADS = std::clamp(launch.thread_count, 1, static_cast<int>(launch.chunks.size()));
        uint64_t const COMPUTE_START_US = now_us();
        for (int thread_index = 0; thread_index < THREADS; ++thread_index) {
            auto& compute_thread = compute_threads.at(created_threads);
            compute_thread.worker_id = launch.output_slot;
            compute_thread.arg = {
                .image = image,
                .colormap = colormap,
                .width = width,
                .height = height,
                .max_iteration = max_iteration,
                .chunks = &launch.chunks,
                .chunk_offsets = &chunk_offsets.at(launch_index),
                .next_chunk = &next_chunks.at(launch_index),
                .total_rows_done = &rows_done_by_launch.at(launch_index),
                .rows_done = 0,
                .local_thread_id = thread_index,
                .local_thread_count = THREADS,
                .diag_worker_id = launch.output_slot,
                .diag_repeat_index = repeat_index,
                .diag_progress_stride = std::max(launch.row_count / 8, 1),
                .diag_compute_start_us = COMPUTE_START_US,
            };
            if (thrd_create(&compute_thread.thread, generate_dynamic_chunk_rows, &compute_thread.arg) != THRD_SUCCESS) {
                std::println(stderr, "mandelbench: failed to create local compute thread {} for worker {}", thread_index,
                             launch.output_slot);
                ok = false;
                break;
            }
            ++created_threads;
        }
        if (!ok) {
            break;
        }
    }

    for (size_t i = 0; i < created_threads; ++i) {
        auto& compute_thread = compute_threads.at(i);
        if (thrd_join(compute_thread.thread, nullptr) != THRD_SUCCESS) {
            std::println(stderr, "mandelbench: failed to join local compute thread for worker {}", compute_thread.worker_id);
            ok = false;
        }
    }
    for (size_t launch_index = 0; launch_index < launches.size(); ++launch_index) {
        const auto& launch = launches[launch_index];
        int const rows_done = rows_done_by_launch.at(launch_index).load(std::memory_order_relaxed);
        if (rows_done != launch.row_count) {
            std::println(stderr, "mandelbench: local worker {} completed {}/{} rows", launch.output_slot, rows_done, launch.row_count);
            ok = false;
        }
    }
    return ok;
}

struct LocalComputeTask {
    std::span<const WorkerLaunch> launches;
    unsigned char* image;
    unsigned char* colormap;
    int width;
    int height;
    int max_iteration;
    size_t row_size;
    int repeat_index;
    bool ok = false;
    uint64_t end_us = 0;
    std::atomic<bool> finished{false};
};

auto execute_local_compute_task(LocalComputeTask& task) -> bool {
    bool const OK = compute_local_launches(task.launches, task.image, task.colormap, task.width, task.height, task.max_iteration,
                                           task.row_size, task.repeat_index);
    task.ok = OK;
    task.end_us = now_us();
    task.finished.store(true, std::memory_order_release);
    return OK;
}

auto run_local_compute_task(void* raw_task) -> int {
    auto* task = static_cast<LocalComputeTask*>(raw_task);
    if (task == nullptr) {
        return EXIT_FAILURE;
    }
    return execute_local_compute_task(*task) ? EXIT_SUCCESS : EXIT_FAILURE;
}

auto join_local_compute_task(thrd_t thread, LocalComputeTask& task) -> bool {
    int result = EXIT_FAILURE;
    if (thrd_join(thread, &result) != THRD_SUCCESS) {
        std::println(stderr, "mandelbench: failed to join local compute supervisor");
        static_cast<void>(thrd_detach(thread));
        while (!task.finished.load(std::memory_order_acquire)) {
            thrd_yield();
        }
        return false;
    }
    return result == EXIT_SUCCESS && task.ok;
}

struct WorkerEventResult {
    bool ok;
    uint64_t wait_end_us;
    uint64_t read_end_us;
    uint64_t loop_end_us;
};

struct WorkerHeaderResult {
    bool ok;
    uint64_t header_end_us;
    uint64_t loop_end_us;
};

struct WorkerPayloadResult {
    bool ok;
    uint64_t header_end_us;
    uint64_t release_end_us;
    uint64_t wait_end_us;
    uint64_t read_end_us;
    uint64_t loop_end_us;
};

[[maybe_unused]] auto complete_worker_headers(std::span<WorkerLaunch> launches, uint16_t expected_flags, int repeat_index)
    -> WorkerHeaderResult {
    uint64_t const START_US = now_us();
    uint64_t header_end_us = START_US;
    bool ok = true;
    std::string const PHASE = repeat_prefix(repeat_index);

    MANDELBENCH_TRACE("mandelbench: {} header-wait begin workers={} expected_flags={}", PHASE, launches.size(), expected_flags);

    for (auto& launch : launches) {
        if (launch.pipe_read_fd < 0) {
            std::println(stderr, "mandelbench: failed to prepare output header reader for worker {}", launch.output_slot);
            ok = false;
            continue;
        }
        launch.header_begin_us = START_US;
        launch.header_last_progress_us = START_US;
        launch.header_last_status_us = START_US;
        MANDELBENCH_TRACE("mandelbench: {} header-wait worker={} pid={} target='{}' fd={} payload_bytes={} chunks='{}'", PHASE,
                          launch.output_slot, launch.child_pid, launch.target_node, launch.pipe_read_fd, launch.read_target,
                          format_worker_chunks(launch.chunks));
    }

    std::vector<pollfd> pollfds;
    pollfds.reserve(launches.size());
    while (true) {
        for (auto& launch : launches) {
            bool const WAS_PENDING = header_is_pending(launch);
            if (WAS_PENDING && !drain_worker_header_final(launch, expected_flags)) {
                ok = false;
            }
            if (WAS_PENDING && !header_is_pending(launch)) {
                header_end_us = std::max(header_end_us, launch.header_end_us);
            }
        }

        for (auto& launch : launches) {
            if (!launch.header_ok && !poll_worker_exit(launch)) {
                ok = false;
            }
        }

        size_t const PENDING_HEADERS = std::count_if(launches.begin(), launches.end(), header_is_pending);
        if (PENDING_HEADERS == 0) {
            break;
        }

        uint64_t const NOW_US = now_us();
        for (auto& launch : launches) {
            if (header_is_pending(launch) && elapsed_us(launch.header_last_status_us, NOW_US) >= STATUS_LOG_INTERVAL_US) {
                MANDELBENCH_TRACE(
                    "mandelbench: {} header-wait pending worker={} pid={} target='{}' read={}/{} fd={} idle_ms={:.3f} "
                    "elapsed_ms={:.3f} wait_done={} wait_ok={}",
                    PHASE, launch.output_slot, launch.child_pid, launch.target_node, launch.header_offset, launch.header.size(),
                    launch.pipe_read_fd, elapsed_ms(launch.header_last_progress_us, NOW_US), elapsed_ms(START_US, NOW_US),
                    launch.wait_done, launch.wait_ok);
                launch.header_last_status_us = NOW_US;
            }
        }
        for (auto& launch : launches) {
            if (header_is_pending(launch) && elapsed_us(launch.header_last_progress_us, NOW_US) > PIPE_READ_IDLE_TIMEOUT_US) {
                std::println(stderr, "mandelbench: output header timeout for worker {} read={}/{} pipe_fd={} elapsed_ms={:.3f}",
                             launch.output_slot, launch.header_offset, launch.header.size(), launch.pipe_read_fd,
                             elapsed_ms(launch.header_last_progress_us, NOW_US));
                launch.header_end_us = NOW_US;
                header_end_us = std::max(header_end_us, launch.header_end_us);
                close_fd(launch.pipe_read_fd);
                ok = false;
            }
        }

        pollfds.clear();
        for (const auto& launch : launches) {
            if (header_is_pending(launch)) {
                pollfds.push_back(pollfd{
                    .fd = launch.pipe_read_fd,
                    .events = POLLIN | POLLHUP | POLLERR,
                    .revents = 0,
                });
            }
        }

        pollfd* const FDS = pollfds.empty() ? nullptr : pollfds.data();
        int const READY = poll(FDS, pollfds.size(), WORKER_EVENT_POLL_TIMEOUT_MS);
        if (READY < 0 && READY != EINTR_NEG && errno != EINTR) {
            std::println(stderr, "mandelbench: poll failed while waiting for worker output headers errno={} ({})", errno,
                         std::strerror(errno));
            ok = false;
            break;
        }
    }

    uint64_t const LOOP_END_US = now_us();
    MANDELBENCH_TRACE("mandelbench: {} header-wait end ok={} header_ms={:.3f} loop_ms={:.3f}", PHASE, ok,
                      elapsed_ms(START_US, std::max(header_end_us, START_US)), elapsed_ms(START_US, LOOP_END_US));
    return WorkerHeaderResult{.ok = ok, .header_end_us = std::max(header_end_us, START_US), .loop_end_us = LOOP_END_US};
}

auto complete_workers_and_outputs(std::span<WorkerLaunch> launches, bool require_worker_exit, int repeat_index) -> WorkerEventResult {
    uint64_t const START_US = now_us();
    uint64_t wait_end_us = START_US;
    uint64_t read_end_us = START_US;
    bool ok = true;
    std::string const PHASE = repeat_prefix(repeat_index);

    MANDELBENCH_TRACE("mandelbench: {} output-wait begin workers={} require_exit={}", PHASE, launches.size(), require_worker_exit);

    for (auto& launch : launches) {
        if (launch.read_offset == launch.read_target) {
            launch.read_ok = true;
            launch.read_end_us = START_US;
            continue;
        }
        if (launch.pipe_read_fd < 0) {
            std::println(stderr, "mandelbench: failed to prepare output reader for worker {}", launch.output_slot);
            ok = false;
            continue;
        }
        launch.read_begin_us = START_US;
        launch.read_last_progress_us = START_US;
        launch.read_last_status_us = START_US;
        MANDELBENCH_TRACE("mandelbench: {} output-wait worker={} pid={} target='{}' fd={} bytes={}/{}", PHASE, launch.output_slot,
                          launch.child_pid, launch.target_node, launch.pipe_read_fd, launch.read_offset, launch.read_target);
    }

    std::vector<pollfd> pollfds;
    pollfds.reserve(launches.size());
    while (true) {
        for (auto& launch : launches) {
            bool const WAS_PENDING = read_is_pending(launch);
            if (WAS_PENDING && !drain_worker_output_final(launch)) {
                ok = false;
            }
            if (WAS_PENDING && !read_is_pending(launch)) {
                read_end_us = std::max(read_end_us, launch.read_end_us);
            }
        }

        for (auto& launch : launches) {
            bool const WAS_DONE = launch.wait_done;
            if (!poll_worker_exit(launch)) {
                ok = false;
            }
            if (!WAS_DONE && launch.wait_done) {
                wait_end_us = std::max(wait_end_us, launch.wait_end_us);
            }
        }

        size_t const PENDING_READS = std::count_if(launches.begin(), launches.end(), read_is_pending);
        size_t const PENDING_WAITS =
            require_worker_exit
                ? std::count_if(launches.begin(), launches.end(),
                                [](const WorkerLaunch& launch) { return launch.child_pid >= 0 && !launch.wait_done; })
                : 0;
        if (PENDING_READS == 0 && PENDING_WAITS == 0) {
            break;
        }

        uint64_t const NOW_US = now_us();
        for (auto& launch : launches) {
            if (read_is_pending(launch) && elapsed_us(launch.read_last_status_us, NOW_US) >= STATUS_LOG_INTERVAL_US) {
                MANDELBENCH_TRACE(
                    "mandelbench: {} output-wait pending worker={} pid={} target='{}' read={}/{} fd={} idle_ms={:.3f} "
                    "elapsed_ms={:.3f} wait_done={} wait_ok={}",
                    PHASE, launch.output_slot, launch.child_pid, launch.target_node, launch.read_offset, launch.read_target,
                    launch.pipe_read_fd, elapsed_ms(launch.read_last_progress_us, NOW_US), elapsed_ms(START_US, NOW_US),
                    launch.wait_done, launch.wait_ok);
                launch.read_last_status_us = NOW_US;
            }
        }
        if (PENDING_WAITS > 0 && elapsed_us(START_US, NOW_US) > WORKER_WAIT_TIMEOUT_US) {
            for (const auto& launch : launches) {
                if (!launch.wait_done && launch.child_pid >= 0) {
                    std::println(stderr, "mandelbench: worker {} wait timeout pid={} target='{}' read={}/{} pipe_fd={} elapsed_ms={:.3f}",
                                 launch.output_slot, launch.child_pid, launch.target_node, launch.read_offset, launch.read_target,
                                 launch.pipe_read_fd, elapsed_ms(START_US, NOW_US));
                }
            }
            ok = false;
            break;
        }
        for (auto& launch : launches) {
            if (read_is_pending(launch) && elapsed_us(launch.read_last_progress_us, NOW_US) > PIPE_PAYLOAD_IDLE_TIMEOUT_US) {
                std::println(stderr, "mandelbench: pipe read timeout for worker {} read={}/{} pipe_fd={} elapsed_ms={:.3f}",
                             launch.output_slot, launch.read_offset, launch.read_target, launch.pipe_read_fd,
                             elapsed_ms(launch.read_last_progress_us, NOW_US));
                launch.read_end_us = NOW_US;
                read_end_us = std::max(read_end_us, launch.read_end_us);
                close_fd(launch.pipe_read_fd);
                ok = false;
            }
        }

        pollfds.clear();
        for (const auto& launch : launches) {
            if (read_is_pending(launch)) {
                pollfds.push_back(pollfd{
                    .fd = launch.pipe_read_fd,
                    .events = POLLIN | POLLHUP | POLLERR,
                    .revents = 0,
                });
            }
        }

        pollfd* const FDS = pollfds.empty() ? nullptr : pollfds.data();
        int const READY = poll(FDS, pollfds.size(), WORKER_EVENT_POLL_TIMEOUT_MS);
        if (READY < 0 && READY != EINTR_NEG && errno != EINTR) {
            std::println(stderr, "mandelbench: poll failed while waiting for worker output errno={} ({})", errno, std::strerror(errno));
            ok = false;
            break;
        }
    }

    uint64_t const LOOP_END_US = now_us();
    wait_end_us = std::max(wait_end_us, START_US);
    read_end_us = std::max(read_end_us, START_US);
    MANDELBENCH_TRACE("mandelbench: {} output-wait end ok={} read_ms={:.3f} wait_ms={:.3f} loop_ms={:.3f}", PHASE, ok,
                      elapsed_ms(START_US, read_end_us), elapsed_ms(START_US, wait_end_us), elapsed_ms(START_US, LOOP_END_US));
    return WorkerEventResult{.ok = ok, .wait_end_us = wait_end_us, .read_end_us = read_end_us, .loop_end_us = LOOP_END_US};
}

auto complete_worker_payloads_streaming(std::span<WorkerLaunch> launches, uint16_t expected_flags, int repeat_index,
                                        bool payload_release_enabled) -> WorkerPayloadResult {
    uint64_t const START_US = now_us();
    uint64_t header_end_us = START_US;
    uint64_t release_end_us = START_US;
    uint64_t wait_end_us = START_US;
    uint64_t read_end_us = START_US;
    bool ok = true;
    std::string const PHASE = repeat_prefix(repeat_index);

    MANDELBENCH_TRACE("mandelbench: {} streaming-output begin workers={} expected_flags={}", PHASE, launches.size(), expected_flags);

    for (auto& launch : launches) {
        if (launch.pipe_read_fd < 0) {
            std::println(stderr, "mandelbench: failed to prepare output reader for worker {}", launch.output_slot);
            ok = false;
            continue;
        }
        launch.header_begin_us = START_US;
        launch.header_last_progress_us = START_US;
        launch.header_last_status_us = START_US;
        launch.read_begin_us = START_US;
        launch.read_last_progress_us = START_US;
        launch.read_last_status_us = START_US;
        MANDELBENCH_TRACE("mandelbench: {} streaming worker={} pid={} target='{}' fd={} payload_bytes={} chunks='{}'", PHASE,
                          launch.output_slot, launch.child_pid, launch.target_node, launch.pipe_read_fd, launch.read_target,
                          format_worker_chunks(launch.chunks));
    }

    std::vector<pollfd> pollfds;
    pollfds.reserve(launches.size());
    while (true) {
        for (auto& launch : launches) {
            bool const HEADER_WAS_PENDING = header_is_pending(launch);
            if (HEADER_WAS_PENDING && !drain_worker_header_final(launch, expected_flags)) {
                ok = false;
            }
            if (HEADER_WAS_PENDING && !header_is_pending(launch)) {
                header_end_us = std::max(header_end_us, launch.header_end_us);
            }
            if (launch.header_ok && !launch.payload_released) {
                if (payload_release_enabled) {
                    if (!release_worker_payload(launch, repeat_index)) {
                        ok = false;
                    }
                } else {
                    launch.payload_released = true;
                }
                release_end_us = std::max(release_end_us, now_us());
            }
            bool const READ_WAS_PENDING = read_is_pending(launch);
            if (launch.header_ok && READ_WAS_PENDING && !drain_worker_output_final(launch)) {
                ok = false;
            }
            if (READ_WAS_PENDING && !read_is_pending(launch)) {
                read_end_us = std::max(read_end_us, launch.read_end_us);
            }
        }

        for (auto& launch : launches) {
            bool const WAS_DONE = launch.wait_done;
            if (!poll_worker_exit(launch)) {
                ok = false;
            }
            if (!WAS_DONE && launch.wait_done) {
                wait_end_us = std::max(wait_end_us, launch.wait_end_us);
            }
        }

        size_t const PENDING_HEADERS = std::count_if(launches.begin(), launches.end(), header_is_pending);
        size_t const PENDING_READS = std::count_if(launches.begin(), launches.end(), read_is_pending);
        if (PENDING_HEADERS == 0 && PENDING_READS == 0) {
            break;
        }

        uint64_t const NOW_US = now_us();
        for (auto& launch : launches) {
            if (header_is_pending(launch) && elapsed_us(launch.header_last_status_us, NOW_US) >= STATUS_LOG_INTERVAL_US) {
                MANDELBENCH_TRACE(
                    "mandelbench: {} streaming header pending worker={} pid={} target='{}' read={}/{} fd={} idle_ms={:.3f} "
                    "elapsed_ms={:.3f} wait_done={} wait_ok={}",
                    PHASE, launch.output_slot, launch.child_pid, launch.target_node, launch.header_offset, launch.header.size(),
                    launch.pipe_read_fd, elapsed_ms(launch.header_last_progress_us, NOW_US), elapsed_ms(START_US, NOW_US),
                    launch.wait_done, launch.wait_ok);
                launch.header_last_status_us = NOW_US;
            }
            if (launch.header_ok && read_is_pending(launch) &&
                elapsed_us(launch.read_last_status_us, NOW_US) >= STATUS_LOG_INTERVAL_US) {
                MANDELBENCH_TRACE(
                    "mandelbench: {} streaming payload pending worker={} pid={} target='{}' read={}/{} fd={} idle_ms={:.3f} "
                    "elapsed_ms={:.3f} wait_done={} wait_ok={}",
                    PHASE, launch.output_slot, launch.child_pid, launch.target_node, launch.read_offset, launch.read_target,
                    launch.pipe_read_fd, elapsed_ms(launch.read_last_progress_us, NOW_US), elapsed_ms(START_US, NOW_US), launch.wait_done,
                    launch.wait_ok);
                launch.read_last_status_us = NOW_US;
            }
        }
        for (auto& launch : launches) {
            if (header_is_pending(launch) && elapsed_us(launch.header_last_progress_us, NOW_US) > PIPE_READ_IDLE_TIMEOUT_US) {
                std::println(stderr, "mandelbench: output header timeout for worker {} read={}/{} pipe_fd={} elapsed_ms={:.3f}",
                             launch.output_slot, launch.header_offset, launch.header.size(), launch.pipe_read_fd,
                             elapsed_ms(launch.header_last_progress_us, NOW_US));
                launch.header_end_us = NOW_US;
                header_end_us = std::max(header_end_us, launch.header_end_us);
                close_fd(launch.pipe_read_fd);
                ok = false;
            }
            if (launch.header_ok && read_is_pending(launch) &&
                elapsed_us(launch.read_last_progress_us, NOW_US) > PIPE_PAYLOAD_IDLE_TIMEOUT_US) {
                std::println(stderr, "mandelbench: pipe read timeout for worker {} read={}/{} pipe_fd={} elapsed_ms={:.3f}",
                             launch.output_slot, launch.read_offset, launch.read_target, launch.pipe_read_fd,
                             elapsed_ms(launch.read_last_progress_us, NOW_US));
                launch.read_end_us = NOW_US;
                read_end_us = std::max(read_end_us, launch.read_end_us);
                close_fd(launch.pipe_read_fd);
                ok = false;
            }
        }

        pollfds.clear();
        for (const auto& launch : launches) {
            if (header_is_pending(launch) || (launch.header_ok && read_is_pending(launch))) {
                pollfds.push_back(pollfd{
                    .fd = launch.pipe_read_fd,
                    .events = POLLIN | POLLHUP | POLLERR,
                    .revents = 0,
                });
            }
        }

        pollfd* const FDS = pollfds.empty() ? nullptr : pollfds.data();
        int const READY = poll(FDS, pollfds.size(), WORKER_EVENT_POLL_TIMEOUT_MS);
        if (READY < 0 && READY != EINTR_NEG && errno != EINTR) {
            std::println(stderr, "mandelbench: poll failed while streaming worker output errno={} ({})", errno, std::strerror(errno));
            ok = false;
            break;
        }
    }

    uint64_t const LOOP_END_US = now_us();
    MANDELBENCH_TRACE("mandelbench: {} streaming-output end ok={} header_ms={:.3f} release_ms={:.3f} read_ms={:.3f} loop_ms={:.3f}",
                      PHASE, ok, elapsed_ms(START_US, std::max(header_end_us, START_US)),
                      elapsed_ms(START_US, std::max(release_end_us, START_US)), elapsed_ms(START_US, std::max(read_end_us, START_US)),
                      elapsed_ms(START_US, LOOP_END_US));
    return WorkerPayloadResult{
        .ok = ok,
        .header_end_us = std::max(header_end_us, START_US),
        .release_end_us = std::max(release_end_us, START_US),
        .wait_end_us = std::max(wait_end_us, START_US),
        .read_end_us = std::max(read_end_us, START_US),
        .loop_end_us = LOOP_END_US,
    };
}

}  // namespace

auto mandelbench_wki(int width, int height, int max_iteration, int workers, int repeat, const char* nodes) -> int {
    bool const PROFILE = mandelbench_profile_enabled() && fd_is_open(STDERR_FILENO);
    bool const SAVE_IMAGES = mandelbench_save_images_enabled();
    bool const PAYLOAD_RELEASE = mandelbench_payload_release_enabled();
    const char* node_text = nodes;
    if (node_text == nullptr || node_text[0] == '\0') {
        node_text = std::getenv("MANDELBENCH_NODES");
    }
    std::vector<std::string> const TARGET_NODES = parse_node_list(node_text);
    std::string const TARGET_NODE_TEXT = join_node_list(TARGET_NODES);
    std::string const RAW_LAUNCHER_NODE = TARGET_NODES.empty() ? std::string{} : wki_runner_node();
    std::string const LAUNCHER_NODE = TARGET_NODES.empty() ? std::string{} : std::string(normalize_node_name(RAW_LAUNCHER_NODE));

    if (workers <= 0) {
        std::println(stderr, "mandelbench: invalid worker count {}", workers);
        return 1;
    }
    if (height <= 0 || width <= 0 || max_iteration <= 0 || repeat <= 0) {
        std::println(stderr, "mandelbench: invalid dimensions or repeat count");
        return 1;
    }

    int const ACTIVE_WORKERS = std::min(workers, height);
    std::vector<unsigned char> image(static_cast<size_t>(width) * static_cast<size_t>(height) * 4U);
    std::vector<unsigned char> local_colormap(static_cast<size_t>((max_iteration + 1) * 3));
    init_colormap(max_iteration + 1, local_colormap.data());
    std::vector<double> times(repeat);

    std::vector<WorkerLaunch> remote_launches;
    std::vector<WorkerLaunch> local_launches;
    remote_launches.reserve(static_cast<size_t>(TARGET_NODES.empty() ? ACTIVE_WORKERS : TARGET_NODES.size()));
    local_launches.reserve(TARGET_NODES.empty() ? 0U : 1U);

    std::vector<WorkerSlot> worker_slots;
    worker_slots.reserve(static_cast<size_t>(ACTIVE_WORKERS));
    for (int worker_id = 0; worker_id < ACTIVE_WORKERS; ++worker_id) {
        std::string target_node =
            TARGET_NODES.empty() ? std::string{} : TARGET_NODES.at(static_cast<size_t>(worker_id) % TARGET_NODES.size());
        bool const LOCAL = !TARGET_NODES.empty() && target_node == LAUNCHER_NODE;
        worker_slots.push_back({
            .worker_id = worker_id,
            .target_node = std::move(target_node),
            .local = LOCAL,
        });
    }

    for (int start_row = 0, band_index = 0; start_row < height; start_row += MANDELBENCH_ROW_BAND_ROWS, ++band_index) {
        int const ROW_COUNT = std::min(MANDELBENCH_ROW_BAND_ROWS, height - start_row);
        const auto& slot = worker_slots.at(static_cast<size_t>(band_index % ACTIVE_WORKERS));
        WorkerChunk const CHUNK{
            .worker_id = slot.worker_id,
            .start_row = start_row,
            .row_count = ROW_COUNT,
        };
        int const THREAD_COUNT = count_slots_for_target(worker_slots, slot.target_node, slot.local);

        if (TARGET_NODES.empty()) {
            auto launch_it = find_launch_for_output_slot(remote_launches, slot.worker_id);
            if (launch_it == remote_launches.end()) {
                remote_launches.push_back(make_worker_launch(slot.worker_id, {}, 1, false));
                launch_it = std::prev(remote_launches.end());
            }
            add_chunk_to_launch(*launch_it, CHUNK);
        } else if (!slot.local) {
            auto launch_it = find_launch_for_target(remote_launches, slot.target_node);
            if (launch_it == remote_launches.end()) {
                remote_launches.push_back(make_worker_launch(slot.worker_id, slot.target_node, THREAD_COUNT, false));
                launch_it = std::prev(remote_launches.end());
            }
            add_chunk_to_launch(*launch_it, CHUNK);
        } else {
            auto launch_it = find_launch_for_target(local_launches, slot.target_node);
            if (launch_it == local_launches.end()) {
                local_launches.push_back(make_worker_launch(slot.worker_id, slot.target_node, THREAD_COUNT, true));
                launch_it = std::prev(local_launches.end());
            }
            add_chunk_to_launch(*launch_it, CHUNK);
        }
    }

    size_t const ROW_SIZE = static_cast<size_t>(width) * 4;
    size_t const REMOTE_WORKER_COUNT = remote_launches.size();
    size_t const LOCAL_WORKER_COUNT = local_launches.size();
    MANDELBENCH_TRACE(
        "mandelbench: setup begin pid={} width={} height={} max_iter={} requested_workers={} active_workers={} repeat={} "
        "remote_workers={} local_workers={} nodes={} launcher='{}' runner='{}'",
        static_cast<int>(ker::process::getpid()), width, height, max_iteration, workers, ACTIVE_WORKERS, repeat, REMOTE_WORKER_COUNT,
        LOCAL_WORKER_COUNT, TARGET_NODE_TEXT, wki_launcher_node(), wki_runner_node());
    for (const auto& launch : remote_launches) {
        MANDELBENCH_TRACE("mandelbench: remote plan worker={} target='{}' start_row={} row_count={} threads={} payload_bytes={} chunks='{}'",
                          launch.output_slot, launch.target_node, launch.start_row, launch.row_count, launch.thread_count,
                          worker_chunks_bytes(launch.chunks, ROW_SIZE), format_worker_chunks(launch.chunks));
    }
    for (const auto& launch : local_launches) {
        MANDELBENCH_TRACE("mandelbench: local plan worker={} target='{}' start_row={} row_count={} chunks='{}'", launch.output_slot,
                          launch.target_node, launch.start_row, launch.row_count, format_worker_chunks(launch.chunks));
    }

    uint64_t const SETUP_START_US = now_us();
    for (auto& launch : remote_launches) {
        MANDELBENCH_TRACE("mandelbench: worker {} setup pipes target='{}'", launch.output_slot, launch.target_node);
        if (!create_worker_pipe(launch)) {
            close_worker_pipes(remote_launches);
            return 1;
        }
        if (!create_worker_release_pipe(launch)) {
            close_worker_pipes(remote_launches);
            return 1;
        }
        if (!set_fd_nonblocking(launch.pipe_read_fd, true)) {
            std::println(stderr, "mandelbench: failed to make output pipe nonblocking for worker {}", launch.output_slot);
            close_worker_pipes(remote_launches);
            return 1;
        }

        std::string const CHUNK_SPEC = format_worker_chunks(launch.chunks);
        std::vector<std::string> arg_storage{
            "testprog",       "--mandelbench-worker",
            "--id",           std::to_string(launch.output_slot),
            "--threads",      std::to_string(std::max(1, launch.thread_count)),
            "--start-row",    std::to_string(launch.start_row),
            "--row-count",    std::to_string(launch.row_count),
            "--chunks",       CHUNK_SPEC,
            "--width",        std::to_string(width),
            "--height",       std::to_string(height),
            "--max-iter",     std::to_string(max_iteration),
            "--output-fd",    std::to_string(WORKER_OUTPUT_FD),
            "--control-fd",   std::to_string(WORKER_RELEASE_FD),
            "--repeat-count", std::to_string(repeat),
            "--repeat-index", std::to_string(0),
        };
        std::vector<char*> argv;
        argv.reserve(arg_storage.size() + 1);
        for (auto& arg : arg_storage) {
            argv.push_back(arg.data());
        }
        argv.push_back(nullptr);

        launch.fork_begin_us = now_us();
        MANDELBENCH_TRACE("mandelbench: worker {} fork begin target='{}' chunks='{}'", launch.output_slot, launch.target_node,
                          CHUNK_SPEC);
        int64_t child_pid = ker::process::fork();
        launch.fork_return_us = now_us();
        if (child_pid < 0) {
            std::println(stderr, "mandelbench: fork failed for worker {}: {}", launch.output_slot, child_pid);
            close_worker_pipes(remote_launches);
            return 1;
        }
        if (child_pid == 0) {
            MANDELBENCH_TRACE("mandelbench-child[{}]: forked target='{}' output_fd={} control_fd={} exec='{}'", launch.output_slot,
                              launch.target_node, launch.pipe_write_fd, launch.release_read_fd, get_testprog_path());
            close_child_pipe_fds(remote_launches, launch.pipe_write_fd, launch.release_read_fd);
            if (!move_child_output_fd(launch.pipe_write_fd)) {
                std::println(stderr, "mandelbench: failed to move output fd for worker {} from {} to {} errno={} ({})",
                             launch.output_slot, launch.pipe_write_fd, WORKER_OUTPUT_FD, errno, std::strerror(errno));
                _exit(1);
            }
            if (!move_child_release_fd(launch.release_read_fd)) {
                std::println(stderr, "mandelbench: failed to move control fd for worker {} from {} to {} errno={} ({})",
                             launch.output_slot, launch.release_read_fd, WORKER_RELEASE_FD, errno, std::strerror(errno));
                _exit(1);
            }
            install_worker_vfs_policy(launch.output_slot, launch.target_node);
            close_standard_fds_for_worker_child();
            if (!set_child_target(launch.target_node, launch.output_slot)) {
                _exit(1);
            }
            execv(get_testprog_path(), argv.data());
            std::println(stderr, "mandelbench: execv failed for worker {} path='{}' target='{}' errno={} ({})", launch.output_slot,
                         get_testprog_path(), launch.target_node, errno, std::strerror(errno));
            _exit(1);
        }
        launch.child_pid = child_pid;
        MANDELBENCH_TRACE("mandelbench: worker {} forked pid={} target='{}' fork_ms={:.3f}", launch.output_slot, launch.child_pid,
                          launch.target_node, elapsed_ms(launch.fork_begin_us, launch.fork_return_us));
        close_fd(launch.pipe_write_fd);
        close_fd(launch.release_read_fd);
    }
    uint64_t const AFTER_LAUNCH_US = now_us();
    MANDELBENCH_TRACE("mandelbench: setup launched remote_workers={} local_workers={} setup_ms={:.3f}", REMOTE_WORKER_COUNT,
                      LOCAL_WORKER_COUNT, elapsed_ms(SETUP_START_US, AFTER_LAUNCH_US));

    if (PROFILE) {
        uint64_t fork_sum_us = 0;
        for (const auto& launch : remote_launches) {
            fork_sum_us += elapsed_us(launch.fork_begin_us, launch.fork_return_us);
        }
        std::println(stderr,
                     "mandelbench-profile setup launch_wall_ms={:.3f} fork_sum_ms={:.3f} workers={} remote_workers={} local_workers={} "
                     "nodes={}",
                     elapsed_ms(SETUP_START_US, AFTER_LAUNCH_US), static_cast<double>(fork_sum_us) / 1000.0,
                     ACTIVE_WORKERS, REMOTE_WORKER_COUNT, LOCAL_WORKER_COUNT, TARGET_NODE_TEXT);
    }

    int repeat_index = 0;
    for (auto& elapsed_seconds : times) {
        MANDELBENCH_TRACE("mandelbench: repeat {} begin", repeat_index);
        std::ranges::fill(image, 0);
        configure_worker_read_targets(remote_launches, image.data(), ROW_SIZE);

        uint64_t const START_US = now_us();
        MANDELBENCH_TRACE("mandelbench: repeat {} start-signal begin remote_workers={}", repeat_index, remote_launches.size());
        if (!signal_worker_starts(remote_launches)) {
            close_worker_pipes(remote_launches);
            return 1;
        }
        uint64_t const AFTER_START_SIGNAL_US = now_us();
        uint64_t const LOCAL_COMPUTE_START_US = now_us();
        MANDELBENCH_TRACE("mandelbench: repeat {} local-compute begin local_workers={}", repeat_index, local_launches.size());

        LocalComputeTask local_compute_task{
            .launches = local_launches,
            .image = image.data(),
            .colormap = local_colormap.data(),
            .width = width,
            .height = height,
            .max_iteration = max_iteration,
            .row_size = ROW_SIZE,
            .repeat_index = repeat_index,
        };
        thrd_t local_compute_thread{};
        bool local_compute_started = false;
        bool const OVERLAP_LOCAL_AND_REMOTE = !local_launches.empty() && !remote_launches.empty();
        if (OVERLAP_LOCAL_AND_REMOTE) {
            local_compute_started = thrd_create(&local_compute_thread, run_local_compute_task, &local_compute_task) == THRD_SUCCESS;
            if (!local_compute_started) {
                std::println(stderr, "mandelbench: failed to create local compute supervisor; using synchronous fallback");
            }
        }

        bool local_compute_ok = true;
        if (!local_compute_started) {
            local_compute_ok = execute_local_compute_task(local_compute_task);
            if (!local_compute_ok) {
                close_worker_pipes(remote_launches);
                return 1;
            }
        }

        uint64_t const PAYLOAD_DRAIN_START_US = now_us();
        WorkerPayloadResult const WORKER_RESULT =
            complete_worker_payloads_streaming(remote_launches, WORKER_OUTPUT_FLAG_PAYLOAD, repeat_index, PAYLOAD_RELEASE);
        if (local_compute_started) {
            local_compute_ok = join_local_compute_task(local_compute_thread, local_compute_task);
        }
        uint64_t const AFTER_LOCAL_COMPUTE_US = local_compute_task.end_us;
        MANDELBENCH_TRACE("mandelbench: repeat {} local-compute end ms={:.3f}", repeat_index,
                          elapsed_ms(LOCAL_COMPUTE_START_US, AFTER_LOCAL_COMPUTE_US));

        uint64_t const AFTER_HEADER_US = WORKER_RESULT.header_end_us;
        uint64_t const AFTER_PAYLOAD_RELEASE_US = WORKER_RESULT.release_end_us;
        uint64_t const AFTER_WAIT_US = WORKER_RESULT.wait_end_us;
        uint64_t const AFTER_READ_US = WORKER_RESULT.read_end_us;
        if (!local_compute_ok || !WORKER_RESULT.ok) {
            close_worker_pipes(remote_launches);
            return 1;
        }
        if (!scatter_worker_outputs(remote_launches, image.data(), ROW_SIZE)) {
            close_worker_pipes(remote_launches);
            return 1;
        }

        uint64_t const AFTER_MERGE_US = now_us();
        MANDELBENCH_TRACE("mandelbench: repeat {} merge end total_ms={:.3f}", repeat_index, elapsed_ms(START_US, AFTER_MERGE_US));
        elapsed_seconds = static_cast<double>(elapsed_us(START_US, AFTER_MERGE_US)) / 1000000.0;

        if (PROFILE) {
            std::println(stderr,
                         "mandelbench-profile repeat={} total_ms={:.3f} start_signal_ms={:.3f} local_compute_ms={:.3f} "
                         "header_ms={:.3f} release_ms={:.3f} wait_ms={:.3f} payload_ms={:.3f} merge_ms={:.3f} workers={} "
                         "remote_workers={} local_workers={} nodes={}",
                         repeat_index, elapsed_ms(START_US, AFTER_MERGE_US), elapsed_ms(START_US, AFTER_START_SIGNAL_US),
                         elapsed_ms(LOCAL_COMPUTE_START_US, AFTER_LOCAL_COMPUTE_US), elapsed_ms(PAYLOAD_DRAIN_START_US, AFTER_HEADER_US),
                         elapsed_ms(PAYLOAD_DRAIN_START_US, AFTER_PAYLOAD_RELEASE_US),
                         elapsed_ms(AFTER_PAYLOAD_RELEASE_US, AFTER_WAIT_US), elapsed_ms(AFTER_PAYLOAD_RELEASE_US, AFTER_READ_US),
                         elapsed_ms(AFTER_PAYLOAD_RELEASE_US, AFTER_MERGE_US), ACTIVE_WORKERS, REMOTE_WORKER_COUNT, LOCAL_WORKER_COUNT,
                         TARGET_NODE_TEXT);
            print_slowest_worker_profiles(remote_launches, repeat_index, START_US);
        }

        progress(DEVICE_NAME, width, height, max_iteration, workers, repeat, repeat_index, elapsed_seconds);
        if (SAVE_IMAGES) {
            uint64_t const SAVE_START_US = now_us();
            std::string const IMG_PATH = std::format(IMAGE, DEVICE_NAME, repeat_index);
            MANDELBENCH_TRACE("mandelbench: repeat {} save begin path='{}'", repeat_index, IMG_PATH);
            save_image(IMG_PATH.c_str(), image.data(), static_cast<unsigned>(width), static_cast<unsigned>(height));
            MANDELBENCH_TRACE("mandelbench: repeat {} save end", repeat_index);
            if (PROFILE) {
                std::println(stderr, "mandelbench-profile repeat={} save_ms={:.3f} path='{}'", repeat_index,
                             elapsed_ms(SAVE_START_US, now_us()), IMG_PATH);
            }
        } else if (PROFILE) {
            std::println(stderr, "mandelbench-profile repeat={} save_skipped=1", repeat_index);
        }
        repeat_index++;
    }

    MANDELBENCH_TRACE("mandelbench: final worker-exit wait begin remote_workers={}", remote_launches.size());
    WorkerEventResult const EXIT_RESULT = complete_workers_and_outputs(remote_launches, true, -1);
    close_worker_pipes(remote_launches);
    if (!EXIT_RESULT.ok) {
        return 1;
    }

    MANDELBENCH_TRACE("mandelbench: report begin");
    report(DEVICE_NAME, width, height, max_iteration, workers, repeat, times);
    MANDELBENCH_TRACE("mandelbench: complete");
    return 0;
}

auto mandelbench_worker(int argc, char** argv) -> int {
    int id = -1;
    int thread_count = 1;
    int start_row = -1;
    int row_count = -1;
    int width = -1;
    int height = -1;
    int max_iter = -1;
    const char* output = nullptr;
    const char* chunk_spec = nullptr;
    int output_fd = -1;
    int release_fd = -1;
    int control_fd = -1;
    int repeat_index = 0;
    int repeat_count = 1;
    uint64_t const ENTRY_US = now_us();

    for (int i = 0; i < argc; i++) {
        const std::string_view ARG = argv[i];
        if (ARG == "--id" && i + 1 < argc) {
            parse_int_arg(argv[++i], id);
        } else if (ARG == "--threads" && i + 1 < argc) {
            parse_int_arg(argv[++i], thread_count);
        } else if (ARG == "--start-row" && i + 1 < argc) {
            parse_int_arg(argv[++i], start_row);
        } else if (ARG == "--row-count" && i + 1 < argc) {
            parse_int_arg(argv[++i], row_count);
        } else if (ARG == "--chunks" && i + 1 < argc) {
            chunk_spec = argv[++i];
        } else if (ARG == "--width" && i + 1 < argc) {
            parse_int_arg(argv[++i], width);
        } else if (ARG == "--height" && i + 1 < argc) {
            parse_int_arg(argv[++i], height);
        } else if (ARG == "--max-iter" && i + 1 < argc) {
            parse_int_arg(argv[++i], max_iter);
        } else if (ARG == "--output" && i + 1 < argc) {
            output = argv[++i];
        } else if (ARG == "--output-fd" && i + 1 < argc) {
            parse_int_arg(argv[++i], output_fd);
        } else if (ARG == "--release-fd" && i + 1 < argc) {
            parse_int_arg(argv[++i], release_fd);
        } else if (ARG == "--control-fd" && i + 1 < argc) {
            parse_int_arg(argv[++i], control_fd);
        } else if (ARG == "--repeat-index" && i + 1 < argc) {
            parse_int_arg(argv[++i], repeat_index);
        } else if (ARG == "--repeat-count" && i + 1 < argc) {
            parse_int_arg(argv[++i], repeat_count);
        }
    }

    std::vector<WorkerChunk> chunks;
    bool const GROUPED_CHUNKS = chunk_spec != nullptr && chunk_spec[0] != '\0';
    if (GROUPED_CHUNKS) {
        if (!parse_worker_chunks(chunk_spec, chunks)) {
            std::println(stderr, "mandelbench-worker: invalid chunk list '{}'", chunk_spec);
            return 1;
        }
    } else if (start_row >= 0 && row_count > 0) {
        chunks.push_back(WorkerChunk{
            .worker_id = id,
            .start_row = start_row,
            .row_count = row_count,
        });
    }

    int total_rows = 0;
    for (const auto& chunk : chunks) {
        if (chunk.start_row < 0 || chunk.row_count <= 0 || chunk.start_row > height || chunk.row_count > height - chunk.start_row) {
            std::println(stderr, "mandelbench-worker[{}]: invalid chunk worker={} start_row={} row_count={} height={}", id,
                         chunk.worker_id, chunk.start_row, chunk.row_count, height);
            return 1;
        }
        total_rows += chunk.row_count;
    }

    if (id < 0 || thread_count <= 0 || chunks.empty() || total_rows <= 0 || width <= 0 || height <= 0 || max_iter <= 0 ||
        repeat_count <= 0 || (output == nullptr && output_fd < 0)) {
        std::println(stderr, "mandelbench-worker: missing required arguments");
        return 1;
    }
    if (GROUPED_CHUNKS) {
        thread_count = std::clamp(thread_count, 1, static_cast<int>(chunks.size()));
        start_row = chunks.front().start_row;
        row_count = total_rows;
    }
    uint64_t const AFTER_PARSE_US = now_us();
    MANDELBENCH_TRACE(
        "mandelbench-worker[{}]: entry pid={} launcher='{}' runner='{}' width={} height={} max_iter={} threads={} "
        "repeat_index={} repeat_count={} start_row={} row_count={} total_rows={} output_fd={} control_fd={} chunks='{}'",
        id, static_cast<int>(ker::process::getpid()), wki_launcher_node(), wki_runner_node(), width, height, max_iter, thread_count,
        repeat_index, repeat_count, start_row, row_count, total_rows, output_fd, control_fd, GROUPED_CHUNKS ? chunk_spec : "");

    std::vector<unsigned char> colormap(static_cast<size_t>((max_iter + 1) * 3));
    init_colormap(max_iter + 1, colormap.data());

    size_t const ROW_SIZE = static_cast<size_t>(width) * 4;
    std::vector<unsigned char> image(static_cast<size_t>(total_rows) * ROW_SIZE);
    struct WorkerThread {
        WorkerThreadArg arg{};
        WorkerBandThreadArg band_arg{};
        thrd_t thread{};
    };
    std::vector<WorkerThread> worker_threads(static_cast<size_t>(thread_count));
    std::vector<size_t> chunk_offsets = make_chunk_offsets(chunks, ROW_SIZE, true);

    uint64_t const AFTER_ALLOC_US = now_us();
    bool const PROFILE = mandelbench_profile_enabled() && fd_is_open(STDERR_FILENO);
    bool const PAYLOAD_RELEASE = mandelbench_payload_release_enabled();

    int const CONTROL_FD = control_fd >= 0 ? control_fd : release_fd;
    int const OUTPUT_STREAM_FD = output_fd;
    ssize_t write_fail_ret = 0;
    int write_fail_errno = 0;

    for (int repeat_offset = 0; repeat_offset < repeat_count; repeat_offset++) {
        int const CURRENT_REPEAT = repeat_index + repeat_offset;
        uint64_t const REPEAT_ENTRY_US = now_us();
        MANDELBENCH_TRACE("mandelbench-worker[{}]: repeat {} begin", id, CURRENT_REPEAT);
        if (control_fd >= 0 && !wait_for_control_byte(control_fd, id, "start")) {
            close(OUTPUT_STREAM_FD);
            close(CONTROL_FD);
            return 1;
        }
        uint64_t const AFTER_START_US = now_us();

        uint64_t const COMPUTE_START_US = now_us();
        MANDELBENCH_TRACE("mandelbench-worker[{}]: repeat {} compute begin threads={} chunks={} bytes={}", id, CURRENT_REPEAT,
                          thread_count, chunks.size(), image.size());
        int const PROGRESS_STRIDE = std::max(total_rows / 8, 1);
        std::atomic<int> next_chunk{0};
        std::atomic<int> grouped_rows_done{0};
        auto prepare_thread_arg = [&](WorkerThread& worker_thread, int thread_index) {
            if (GROUPED_CHUNKS) {
                worker_thread.band_arg = {
                    .image = image.data(),
                    .colormap = colormap.data(),
                    .width = width,
                    .height = height,
                    .max_iteration = max_iter,
                    .chunks = &chunks,
                    .chunk_offsets = &chunk_offsets,
                    .next_chunk = &next_chunk,
                    .total_rows_done = &grouped_rows_done,
                    .rows_done = 0,
                    .local_thread_id = thread_index,
                    .local_thread_count = thread_count,
                    .diag_worker_id = id,
                    .diag_repeat_index = CURRENT_REPEAT,
                    .diag_progress_stride = PROGRESS_STRIDE,
                    .diag_compute_start_us = COMPUTE_START_US,
                };
                return;
            }

            auto& thread_arg = worker_thread.arg;
            WorkerChunk const& chunk = chunks.front();
            thread_arg.image = image.data();
            thread_arg.colormap = colormap.data();
            thread_arg.width = width;
            thread_arg.height = height;
            thread_arg.max_iteration = max_iter;
            thread_arg.start_row = chunk.start_row;
            thread_arg.row_count = chunk.row_count;
            thread_arg.local_thread_id = thread_index;
            thread_arg.local_thread_count = thread_count;
            thread_arg.rows_done = 0;
            thread_arg.diag_worker_id = id;
            thread_arg.diag_repeat_index = CURRENT_REPEAT;
            thread_arg.diag_progress_stride = PROGRESS_STRIDE;
            thread_arg.diag_compute_start_us = COMPUTE_START_US;
        };

        int rows_done = 0;
        if (thread_count == 1) {
            auto& worker_thread = worker_threads.front();
            prepare_thread_arg(worker_thread, 0);
            MANDELBENCH_TRACE("mandelbench-worker[{}]: repeat {} inline compute begin", id, CURRENT_REPEAT);
            if (GROUPED_CHUNKS) {
                (void)generate_dynamic_chunk_rows(&worker_thread.band_arg);
                rows_done = grouped_rows_done.load(std::memory_order_relaxed);
            } else {
                (void)generate_rows(&worker_thread.arg);
                rows_done = worker_thread.arg.rows_done;
            }
            MANDELBENCH_TRACE("mandelbench-worker[{}]: repeat {} inline compute end", id, CURRENT_REPEAT);
        } else {
            int thread_index = 0;
            for (auto& worker_thread : worker_threads) {
                prepare_thread_arg(worker_thread, thread_index);
                MANDELBENCH_TRACE("mandelbench-worker[{}]: repeat {} thread {} create begin", id, CURRENT_REPEAT, thread_index);
                auto* thread_arg = GROUPED_CHUNKS ? static_cast<void*>(&worker_thread.band_arg) : static_cast<void*>(&worker_thread.arg);
                auto thread_func = GROUPED_CHUNKS ? generate_dynamic_chunk_rows : generate_rows;
                if (thrd_create(&worker_thread.thread, thread_func, thread_arg) != THRD_SUCCESS) {
                    std::println(stderr, "mandelbench-worker[{}]: failed to create thread {}", id, thread_index);
                    close(OUTPUT_STREAM_FD);
                    close(CONTROL_FD);
                    return 1;
                }
                MANDELBENCH_TRACE("mandelbench-worker[{}]: repeat {} thread {} create end", id, CURRENT_REPEAT, thread_index);
                thread_index++;
            }

            int joined_thread_index = 0;
            for (auto& worker_thread : worker_threads) {
                MANDELBENCH_TRACE("mandelbench-worker[{}]: repeat {} thread {} join begin", id, CURRENT_REPEAT, joined_thread_index);
                if (thrd_join(worker_thread.thread, nullptr) != THRD_SUCCESS) {
                    std::println(stderr, "mandelbench-worker[{}]: failed to join thread {}", id, joined_thread_index);
                    close(OUTPUT_STREAM_FD);
                    close(CONTROL_FD);
                    return 1;
                }
                MANDELBENCH_TRACE("mandelbench-worker[{}]: repeat {} thread {} join end", id, CURRENT_REPEAT, joined_thread_index);
                if (!GROUPED_CHUNKS) {
                    rows_done += worker_thread.arg.rows_done;
                }
                joined_thread_index++;
            }
            if (GROUPED_CHUNKS) {
                rows_done = grouped_rows_done.load(std::memory_order_relaxed);
            }
        }
        uint64_t const COMPUTE_END_US = now_us();
        MANDELBENCH_TRACE("mandelbench-worker[{}]: repeat {} compute end rows_done={} expected_rows={} ms={:.3f}", id, CURRENT_REPEAT,
                          rows_done, total_rows, elapsed_ms(COMPUTE_START_US, COMPUTE_END_US));

        uint64_t const WRITE_START_US = now_us();
        MANDELBENCH_TRACE("mandelbench-worker[{}]: repeat {} open/write begin output_fd={} output='{}'", id, CURRENT_REPEAT,
                          OUTPUT_STREAM_FD, output != nullptr ? output : "");
        int const FD = OUTPUT_STREAM_FD >= 0 ? OUTPUT_STREAM_FD : open(output, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        uint64_t const OPEN_END_US = now_us();
        if (FD < 0) {
            std::println(stderr, "mandelbench-worker[{}]: failed to open output '{}'", id, output);
            close(OUTPUT_STREAM_FD);
            close(CONTROL_FD);
            return 1;
        }

        if (OUTPUT_STREAM_FD >= 0) {
            std::array<unsigned char, WORKER_OUTPUT_HEADER_SIZE> header{};
            if (!make_worker_output_header(id, image.size(), header)) {
                close(FD);
                close(CONTROL_FD);
                std::println(stderr, "mandelbench-worker[{}]: failed to build output header bytes={}", id, image.size());
                return 1;
            }
            MANDELBENCH_TRACE("mandelbench-worker[{}]: repeat {} header write begin fd={} bytes={}", id, CURRENT_REPEAT, FD,
                              header.size());
            if (!write_all(FD, header, &write_fail_ret, &write_fail_errno)) {
                close(FD);
                close(CONTROL_FD);
                std::println(stderr, "mandelbench-worker[{}]: failed while writing output header ret={} errno={} ({})", id,
                             write_fail_ret, write_fail_errno, std::strerror(write_fail_errno));
                return 1;
            }
            MANDELBENCH_TRACE("mandelbench-worker[{}]: repeat {} header write end fd={} bytes={}", id, CURRENT_REPEAT, FD,
                              header.size());
            if (PAYLOAD_RELEASE && !wait_for_control_byte(CONTROL_FD, id, "payload release")) {
                close(FD);
                close(CONTROL_FD);
                return 1;
            }
        }

        uint64_t const WRITE_BODY_START_US = now_us();
        MANDELBENCH_TRACE("mandelbench-worker[{}]: repeat {} payload write begin fd={} bytes={}", id, CURRENT_REPEAT, FD, image.size());
        if (!write_all(FD, image, &write_fail_ret, &write_fail_errno)) {
            close(FD);
            close(CONTROL_FD);
            std::println(stderr, "mandelbench-worker[{}]: failed while writing output ret={} errno={} ({})", id, write_fail_ret,
                         write_fail_errno, std::strerror(write_fail_errno));
            return 1;
        }
        uint64_t const WRITE_BODY_END_US = now_us();
        MANDELBENCH_TRACE("mandelbench-worker[{}]: repeat {} payload write end fd={} bytes={} ms={:.3f}", id, CURRENT_REPEAT, FD,
                          image.size(), elapsed_ms(WRITE_BODY_START_US, WRITE_BODY_END_US));

        if (OUTPUT_STREAM_FD < 0 && close(FD) != 0) {
            std::println(stderr, "mandelbench-worker[{}]: close failed for output", id);
            close(CONTROL_FD);
            return 1;
        }
        uint64_t const WRITE_END_US = now_us();

        if (PROFILE) {
            std::string const LAUNCHER = wki_launcher_node();
            std::string const RUNNER = wki_runner_node();
            size_t const BYTES = image.size();
            std::string const PROFILE_LINE = std::format(
                "repeat={} id={} pid={} launcher={} runner={} start_row={} row_count={} rows_done={} bytes={} parse_ms={:.3f} "
                "alloc_ms={:.3f} start_wait_ms={:.3f} compute_ms={:.3f} open_ms={:.3f} write_body_ms={:.3f} write_ms={:.3f} "
                "total_ms={:.3f}\n",
                CURRENT_REPEAT, id, static_cast<int>(ker::process::getpid()), LAUNCHER, RUNNER, start_row, row_count, rows_done, BYTES,
                elapsed_ms(ENTRY_US, AFTER_PARSE_US), elapsed_ms(AFTER_PARSE_US, AFTER_ALLOC_US),
                elapsed_ms(REPEAT_ENTRY_US, AFTER_START_US), elapsed_ms(COMPUTE_START_US, COMPUTE_END_US),
                elapsed_ms(WRITE_START_US, OPEN_END_US), elapsed_ms(WRITE_BODY_START_US, WRITE_BODY_END_US),
                elapsed_ms(WRITE_START_US, WRITE_END_US), elapsed_ms(REPEAT_ENTRY_US, WRITE_END_US));
            if (!write_text_fd(STDERR_FILENO, PROFILE_LINE)) {
                std::println(stderr, "mandelbench-worker[{}]: failed to write profile line", id);
            }
        }

        if (MANDELBENCH_DEBUG_ENABLED) {
            std::println(stderr, "mandelbench-worker[{}]: computed {} rows using {} threads", id, rows_done, thread_count);
        }
        MANDELBENCH_TRACE("mandelbench-worker[{}]: repeat {} end total_ms={:.3f}", id, CURRENT_REPEAT,
                          elapsed_ms(REPEAT_ENTRY_US, WRITE_END_US));
    }

    if (OUTPUT_STREAM_FD >= 0 && close(OUTPUT_STREAM_FD) != 0) {
        std::println(stderr, "mandelbench-worker[{}]: close failed for output", id);
        close(CONTROL_FD);
        return 1;
    }
    if (CONTROL_FD >= 0) {
        close(CONTROL_FD);
    }
    MANDELBENCH_TRACE("mandelbench-worker[{}]: exit ok", id);
    return 0;
}
