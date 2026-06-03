#include "mandelbench_wki.hpp"

#include <abi-bits/fcntl.h>
#include <abi-bits/wait.h>
#include <bits/ssize_t.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/process.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
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

namespace {

constexpr int EINTR_NEG = -4;  // WOS returns -errno from syscalls.
constexpr const char* DEVICE_NAME = "process";
constexpr uint64_t PIPE_READ_IDLE_TIMEOUT_US = 60'000'000;
constexpr uint64_t WORKER_WAIT_TIMEOUT_US = 60'000'000;
constexpr int WORKER_EVENT_POLL_TIMEOUT_MS = 10;
constexpr int WORKER_OUTPUT_FD = 3;
constexpr int WORKER_RELEASE_FD = 4;
constexpr uint32_t WORKER_OUTPUT_MAGIC = 0x31424D57;  // WMB1
constexpr uint16_t WORKER_OUTPUT_VERSION = 1;
constexpr uint16_t WORKER_OUTPUT_FLAG_PAYLOAD = 0;
constexpr size_t WORKER_OUTPUT_HEADER_SIZE = 16;
constexpr unsigned char WORKER_CONTROL_START = 1;
constexpr unsigned char WORKER_CONTROL_RELEASE_PAYLOAD = 2;

auto now_us() -> uint64_t {
    timeval tv{};
    gettimeofday(&tv, nullptr);
    return (static_cast<uint64_t>(tv.tv_sec) * 1000000) + tv.tv_usec;
}

auto elapsed_ms(uint64_t start_us, uint64_t end_us) -> double { return static_cast<double>(end_us - start_us) / 1000.0; }

auto mandelbench_profile_enabled() -> bool {
    const char* value = std::getenv("MANDELBENCH_PROFILE");
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
    while (written_total < bytes.size()) {
        ssize_t const WRITTEN = write(fd, bytes.data() + written_total, bytes.size() - written_total);
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

auto wait_for_control_byte(int fd, int worker_id, const char* label) -> bool {
    if (fd < 0) {
        return true;
    }

    unsigned char control_byte = 0;
    while (true) {
        errno = 0;
        ssize_t const BYTES_READ = read(fd, &control_byte, 1);
        if (BYTES_READ == 1) {
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

auto get_testprog_path() -> const char* { return "/bin/testprog"; }

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
};

struct WorkerChunk {
    int worker_id;
    int start_row;
    int row_count;
};

auto generate_rows(void* param) -> int {
    auto* arg = static_cast<WorkerThreadArg*>(param);

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
    }
    return 0;
}

struct WorkerLaunch {
    int output_slot;
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
    uint64_t header_begin_us;
    uint64_t header_end_us;
    uint64_t header_last_progress_us;
    std::array<unsigned char, WORKER_OUTPUT_HEADER_SIZE> header;
    size_t header_offset;
    uint64_t read_begin_us;
    uint64_t read_end_us;
    uint64_t read_last_progress_us;
    unsigned char* read_dest;
    size_t read_target;
    size_t read_offset;
    std::vector<unsigned char> read_buffer;
    std::string target_node;
};

struct LocalComputeThread {
    WorkerThreadArg arg{};
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

auto make_worker_launch(int output_slot, std::string target_node) -> WorkerLaunch {
    return WorkerLaunch{
        .output_slot = output_slot,
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
        .header_begin_us = 0,
        .header_end_us = 0,
        .header_last_progress_us = 0,
        .header = {},
        .header_offset = 0,
        .read_begin_us = 0,
        .read_end_us = 0,
        .read_last_progress_us = 0,
        .read_dest = nullptr,
        .read_target = 0,
        .read_offset = 0,
        .read_buffer = {},
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
        ssize_t write_fail_ret = 0;
        int write_fail_errno = 0;
        if (!write_all(launch.release_write_fd, std::span(&signal_byte, size_t{1}), &write_fail_ret, &write_fail_errno)) {
            std::println(stderr, "mandelbench: failed to signal worker {} {} ret={} errno={} ({})", launch.output_slot, label,
                         write_fail_ret, write_fail_errno, std::strerror(write_fail_errno));
            ok = false;
        }
        if (close_after_signal) {
            close_fd(launch.release_write_fd);
        }
    }
    return ok;
}

auto signal_worker_starts(std::span<WorkerLaunch> launches) -> bool {
    return signal_workers(launches, WORKER_CONTROL_START, "start", false);
}

auto release_worker_payloads(std::span<WorkerLaunch> launches) -> bool {
    return signal_workers(launches, WORKER_CONTROL_RELEASE_PAYLOAD, "payload release", false);
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
    if (target_node.empty()) {
        return true;
    }

    uint32_t const FLAGS = ker::process::WKI_TARGET_FLAG_STRICT | ker::process::WKI_TARGET_FLAG_NOINHERIT;
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
    launch.header_begin_us = 0;
    launch.header_end_us = 0;
    launch.header_last_progress_us = 0;
    launch.header.fill(0);
    launch.header_offset = 0;
    launch.read_ok = false;
    launch.read_begin_us = 0;
    launch.read_end_us = 0;
    launch.read_last_progress_us = 0;
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

auto compute_local_launches(std::span<const WorkerLaunch> launches, unsigned char* image, unsigned char* colormap, int width, int height,
                            int max_iteration, size_t row_size) -> bool {
    if (launches.empty()) {
        return true;
    }

    std::vector<LocalComputeThread> compute_threads(launches.size());
    size_t created_threads = 0;
    auto launch_it = launches.begin();
    for (auto& compute_thread : compute_threads) {
        const auto& launch = *launch_it;
        compute_thread.worker_id = launch.output_slot;
        compute_thread.arg.image = image + (static_cast<size_t>(launch.start_row) * row_size);
        compute_thread.arg.colormap = colormap;
        compute_thread.arg.width = width;
        compute_thread.arg.height = height;
        compute_thread.arg.max_iteration = max_iteration;
        compute_thread.arg.start_row = launch.start_row;
        compute_thread.arg.row_count = launch.row_count;
        compute_thread.arg.local_thread_id = 0;
        compute_thread.arg.local_thread_count = 1;
        compute_thread.arg.rows_done = 0;

        if (thrd_create(&compute_thread.thread, generate_rows, &compute_thread.arg) != THRD_SUCCESS) {
            std::println(stderr, "mandelbench: failed to create local compute thread for worker {}", launch.output_slot);
            for (auto join_it = compute_threads.begin(); join_it != compute_threads.begin() + static_cast<ptrdiff_t>(created_threads);
                 ++join_it) {
                thrd_join(join_it->thread, nullptr);
            }
            return false;
        }
        created_threads++;
        ++launch_it;
    }

    bool ok = true;
    for (auto join_it = compute_threads.begin(); join_it != compute_threads.begin() + static_cast<ptrdiff_t>(created_threads); ++join_it) {
        auto& compute_thread = *join_it;
        if (thrd_join(compute_thread.thread, nullptr) != THRD_SUCCESS) {
            std::println(stderr, "mandelbench: failed to join local compute thread for worker {}", compute_thread.worker_id);
            ok = false;
        }
        if (compute_thread.arg.rows_done != compute_thread.arg.row_count) {
            std::println(stderr, "mandelbench: local worker {} completed {}/{} rows", compute_thread.worker_id,
                         compute_thread.arg.rows_done, compute_thread.arg.row_count);
            ok = false;
        }
    }
    return ok;
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

auto complete_worker_headers(std::span<WorkerLaunch> launches, uint16_t expected_flags) -> WorkerHeaderResult {
    uint64_t const START_US = now_us();
    uint64_t header_end_us = START_US;
    bool ok = true;

    for (auto& launch : launches) {
        if (launch.pipe_read_fd < 0) {
            std::println(stderr, "mandelbench: failed to prepare output header reader for worker {}", launch.output_slot);
            ok = false;
            continue;
        }
        launch.header_begin_us = START_US;
        launch.header_last_progress_us = START_US;
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
            if (header_is_pending(launch) && NOW_US - launch.header_last_progress_us > PIPE_READ_IDLE_TIMEOUT_US) {
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
    return WorkerHeaderResult{.ok = ok, .header_end_us = std::max(header_end_us, START_US), .loop_end_us = LOOP_END_US};
}

auto complete_workers_and_outputs(std::span<WorkerLaunch> launches, bool require_worker_exit) -> WorkerEventResult {
    uint64_t const START_US = now_us();
    uint64_t wait_end_us = START_US;
    uint64_t read_end_us = START_US;
    bool ok = true;

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
        if (PENDING_WAITS > 0 && NOW_US - START_US > WORKER_WAIT_TIMEOUT_US) {
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
            if (read_is_pending(launch) && NOW_US - launch.read_last_progress_us > PIPE_READ_IDLE_TIMEOUT_US) {
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
    return WorkerEventResult{.ok = ok, .wait_end_us = wait_end_us, .read_end_us = read_end_us, .loop_end_us = LOOP_END_US};
}

}  // namespace

auto mandelbench_wki(int width, int height, int max_iteration, int workers, int repeat, const char* nodes) -> int {
    bool const PROFILE = mandelbench_profile_enabled();
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
    local_launches.reserve(static_cast<size_t>(ACTIVE_WORKERS));

    int next_start_row = 0;
    int const BASE_ROWS = height / ACTIVE_WORKERS;
    int const EXTRA_ROWS = height % ACTIVE_WORKERS;
    for (int worker_id = 0; worker_id < ACTIVE_WORKERS; worker_id++) {
        int const ROW_COUNT = BASE_ROWS + (worker_id < EXTRA_ROWS ? 1 : 0);
        std::string const TARGET_NODE =
            TARGET_NODES.empty() ? std::string{} : TARGET_NODES.at(static_cast<size_t>(worker_id) % TARGET_NODES.size());
        WorkerChunk const CHUNK{
            .worker_id = worker_id,
            .start_row = next_start_row,
            .row_count = ROW_COUNT,
        };

        if (TARGET_NODES.empty()) {
            remote_launches.push_back(make_worker_launch(worker_id, {}));
            add_chunk_to_launch(remote_launches.back(), CHUNK);
        } else if (TARGET_NODE != LAUNCHER_NODE) {
            auto launch_it =
                std::ranges::find_if(remote_launches, [&](const WorkerLaunch& launch) { return launch.target_node == TARGET_NODE; });
            if (launch_it == remote_launches.end()) {
                remote_launches.push_back(make_worker_launch(worker_id, TARGET_NODE));
                launch_it = std::prev(remote_launches.end());
            }
            add_chunk_to_launch(*launch_it, CHUNK);
        } else {
            local_launches.push_back(make_worker_launch(worker_id, TARGET_NODE));
            add_chunk_to_launch(local_launches.back(), CHUNK);
        }
        next_start_row += ROW_COUNT;
    }

    size_t const ROW_SIZE = static_cast<size_t>(width) * 4;

    uint64_t const SETUP_START_US = now_us();
    for (auto& launch : remote_launches) {
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
            "--threads",      std::to_string(std::max<size_t>(size_t{1}, launch.chunks.size())),
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
        int64_t child_pid = ker::process::fork();
        launch.fork_return_us = now_us();
        if (child_pid < 0) {
            std::println(stderr, "mandelbench: fork failed for worker {}: {}", launch.output_slot, child_pid);
            close_worker_pipes(remote_launches);
            return 1;
        }
        if (child_pid == 0) {
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
            if (!set_child_target(launch.target_node, launch.output_slot)) {
                _exit(1);
            }
            execv(get_testprog_path(), argv.data());
            std::println(stderr, "mandelbench: execv failed for worker {} path='{}' target='{}' errno={} ({})", launch.output_slot,
                         get_testprog_path(), launch.target_node, errno, std::strerror(errno));
            _exit(1);
        }
        launch.child_pid = child_pid;
        close_fd(launch.pipe_write_fd);
        close_fd(launch.release_read_fd);
    }
    uint64_t const AFTER_LAUNCH_US = now_us();

    if (PROFILE) {
        uint64_t fork_sum_us = 0;
        for (const auto& launch : remote_launches) {
            fork_sum_us += launch.fork_return_us - launch.fork_begin_us;
        }
        std::println(stderr,
                     "mandelbench-profile setup launch_wall_ms={:.3f} fork_sum_ms={:.3f} workers={} remote_workers={} local_workers={} "
                     "nodes={}",
                     elapsed_ms(SETUP_START_US, AFTER_LAUNCH_US), static_cast<double>(fork_sum_us) / 1000.0,
                     ACTIVE_WORKERS, remote_launches.size(), local_launches.size(), TARGET_NODE_TEXT);
    }

    int repeat_index = 0;
    for (auto& elapsed_seconds : times) {
        std::ranges::fill(image, 0);
        configure_worker_read_targets(remote_launches, image.data(), ROW_SIZE);

        uint64_t const START_US = now_us();
        if (!signal_worker_starts(remote_launches)) {
            close_worker_pipes(remote_launches);
            return 1;
        }
        uint64_t const AFTER_START_SIGNAL_US = now_us();
        uint64_t const LOCAL_COMPUTE_START_US = now_us();
        if (!compute_local_launches(local_launches, image.data(), local_colormap.data(), width, height, max_iteration, ROW_SIZE)) {
            close_worker_pipes(remote_launches);
            return 1;
        }
        uint64_t const AFTER_LOCAL_COMPUTE_US = now_us();

        WorkerHeaderResult const HEADER_RESULT = complete_worker_headers(remote_launches, WORKER_OUTPUT_FLAG_PAYLOAD);
        uint64_t const AFTER_HEADER_US = HEADER_RESULT.header_end_us;
        if (!HEADER_RESULT.ok) {
            close_worker_pipes(remote_launches);
            return 1;
        }
        uint64_t const PAYLOAD_RELEASE_START_US = now_us();
        if (!release_worker_payloads(remote_launches)) {
            close_worker_pipes(remote_launches);
            return 1;
        }
        uint64_t const AFTER_PAYLOAD_RELEASE_US = now_us();
        WorkerEventResult const WORKER_RESULT = complete_workers_and_outputs(remote_launches, false);
        uint64_t const AFTER_WAIT_US = WORKER_RESULT.wait_end_us;
        uint64_t const AFTER_READ_US = WORKER_RESULT.read_end_us;
        if (!WORKER_RESULT.ok) {
            close_worker_pipes(remote_launches);
            return 1;
        }
        if (!scatter_worker_outputs(remote_launches, image.data(), ROW_SIZE)) {
            close_worker_pipes(remote_launches);
            return 1;
        }

        uint64_t const AFTER_MERGE_US = now_us();
        elapsed_seconds = static_cast<double>(AFTER_MERGE_US - START_US) / 1000000.0;

        if (PROFILE) {
            std::println(stderr,
                         "mandelbench-profile repeat={} total_ms={:.3f} start_signal_ms={:.3f} local_compute_ms={:.3f} "
                         "header_ms={:.3f} release_ms={:.3f} wait_ms={:.3f} payload_ms={:.3f} merge_ms={:.3f} workers={} "
                         "remote_workers={} local_workers={} nodes={}",
                         repeat_index, elapsed_ms(START_US, AFTER_MERGE_US), elapsed_ms(START_US, AFTER_START_SIGNAL_US),
                         elapsed_ms(LOCAL_COMPUTE_START_US, AFTER_LOCAL_COMPUTE_US), elapsed_ms(AFTER_LOCAL_COMPUTE_US, AFTER_HEADER_US),
                         elapsed_ms(PAYLOAD_RELEASE_START_US, AFTER_PAYLOAD_RELEASE_US),
                         elapsed_ms(AFTER_PAYLOAD_RELEASE_US, AFTER_WAIT_US), elapsed_ms(AFTER_PAYLOAD_RELEASE_US, AFTER_READ_US),
                         elapsed_ms(AFTER_PAYLOAD_RELEASE_US, AFTER_MERGE_US), ACTIVE_WORKERS, remote_launches.size(),
                         local_launches.size(), TARGET_NODE_TEXT);
        }

        std::string const IMG_PATH = std::format(IMAGE, DEVICE_NAME, repeat_index);
        save_image(IMG_PATH.c_str(), image.data(), static_cast<unsigned>(width), static_cast<unsigned>(height));
        progress(DEVICE_NAME, width, height, max_iteration, workers, repeat, repeat_index, elapsed_seconds);
        repeat_index++;
    }

    WorkerEventResult const EXIT_RESULT = complete_workers_and_outputs(remote_launches, true);
    close_worker_pipes(remote_launches);
    if (!EXIT_RESULT.ok) {
        return 1;
    }

    report(DEVICE_NAME, width, height, max_iteration, workers, repeat, times);
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
        thread_count = static_cast<int>(chunks.size());
        start_row = chunks.front().start_row;
        row_count = total_rows;
    }
    uint64_t const AFTER_PARSE_US = now_us();

    std::vector<unsigned char> colormap(static_cast<size_t>((max_iter + 1) * 3));
    init_colormap(max_iter + 1, colormap.data());

    size_t const ROW_SIZE = static_cast<size_t>(width) * 4;
    std::vector<unsigned char> image(static_cast<size_t>(total_rows) * ROW_SIZE);
    struct WorkerThread {
        WorkerThreadArg arg{};
        thrd_t thread{};
    };
    std::vector<WorkerThread> worker_threads(static_cast<size_t>(thread_count));
    std::vector<size_t> chunk_offsets(chunks.size());
    size_t next_chunk_offset = 0;
    for (size_t chunk_index = 0; chunk_index < chunks.size(); chunk_index++) {
        chunk_offsets.at(chunk_index) = next_chunk_offset;
        next_chunk_offset += worker_chunk_bytes(chunks.at(chunk_index), ROW_SIZE);
    }

    uint64_t const AFTER_ALLOC_US = now_us();
    bool const PROFILE = mandelbench_profile_enabled();

    int const CONTROL_FD = control_fd >= 0 ? control_fd : release_fd;
    int const OUTPUT_STREAM_FD = output_fd;
    ssize_t write_fail_ret = 0;
    int write_fail_errno = 0;

    for (int repeat_offset = 0; repeat_offset < repeat_count; repeat_offset++) {
        int const CURRENT_REPEAT = repeat_index + repeat_offset;
        uint64_t const REPEAT_ENTRY_US = now_us();
        if (control_fd >= 0 && !wait_for_control_byte(control_fd, id, "start")) {
            close(OUTPUT_STREAM_FD);
            close(CONTROL_FD);
            return 1;
        }
        uint64_t const AFTER_START_US = now_us();

        uint64_t const COMPUTE_START_US = now_us();
        int thread_index = 0;
        for (auto& worker_thread : worker_threads) {
            auto& thread_arg = worker_thread.arg;
            WorkerChunk const& chunk = GROUPED_CHUNKS ? chunks.at(static_cast<size_t>(thread_index)) : chunks.front();
            thread_arg.image = GROUPED_CHUNKS ? image.data() + chunk_offsets.at(static_cast<size_t>(thread_index)) : image.data();
            thread_arg.colormap = colormap.data();
            thread_arg.width = width;
            thread_arg.height = height;
            thread_arg.max_iteration = max_iter;
            thread_arg.start_row = chunk.start_row;
            thread_arg.row_count = chunk.row_count;
            thread_arg.local_thread_id = GROUPED_CHUNKS ? 0 : thread_index;
            thread_arg.local_thread_count = GROUPED_CHUNKS ? 1 : thread_count;
            thread_arg.rows_done = 0;

            if (thrd_create(&worker_thread.thread, generate_rows, &thread_arg) != THRD_SUCCESS) {
                std::println(stderr, "mandelbench-worker[{}]: failed to create thread {}", id, thread_index);
                close(OUTPUT_STREAM_FD);
                close(CONTROL_FD);
                return 1;
            }
            thread_index++;
        }

        int rows_done = 0;
        int joined_thread_index = 0;
        for (auto& worker_thread : worker_threads) {
            if (thrd_join(worker_thread.thread, nullptr) != THRD_SUCCESS) {
                std::println(stderr, "mandelbench-worker[{}]: failed to join thread {}", id, joined_thread_index);
                close(OUTPUT_STREAM_FD);
                close(CONTROL_FD);
                return 1;
            }
            rows_done += worker_thread.arg.rows_done;
            joined_thread_index++;
        }
        uint64_t const COMPUTE_END_US = now_us();

        uint64_t const WRITE_START_US = now_us();
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
            if (!write_all(FD, header, &write_fail_ret, &write_fail_errno)) {
                close(FD);
                close(CONTROL_FD);
                std::println(stderr, "mandelbench-worker[{}]: failed while writing output header ret={} errno={} ({})", id, write_fail_ret,
                             write_fail_errno, std::strerror(write_fail_errno));
                return 1;
            }
            if (!wait_for_control_byte(CONTROL_FD, id, "payload release")) {
                close(FD);
                close(CONTROL_FD);
                return 1;
            }
        }

        uint64_t const WRITE_BODY_START_US = now_us();
        if (!write_all(FD, image, &write_fail_ret, &write_fail_errno)) {
            close(FD);
            close(CONTROL_FD);
            std::println(stderr, "mandelbench-worker[{}]: failed while writing output ret={} errno={} ({})", id, write_fail_ret,
                         write_fail_errno, std::strerror(write_fail_errno));
            return 1;
        }
        uint64_t const WRITE_BODY_END_US = now_us();

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
    }

    if (OUTPUT_STREAM_FD >= 0 && close(OUTPUT_STREAM_FD) != 0) {
        std::println(stderr, "mandelbench-worker[{}]: close failed for output", id);
        close(CONTROL_FD);
        return 1;
    }
    if (CONTROL_FD >= 0) {
        close(CONTROL_FD);
    }
    return 0;
}
