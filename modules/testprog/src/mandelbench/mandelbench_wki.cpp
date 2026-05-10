#include "mandelbench_wki.hpp"

#include <abi-bits/fcntl.h>
#include <abi-bits/wait.h>
#include <bits/ssize_t.h>
#include <bits/timeval.h>
#include <fcntl.h>
#include <sys/process.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <format>
#include <limits>
#include <print>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "config.hpp"
#include "tinycthread.hpp"
#include "util.hpp"

namespace {

constexpr int EINTR_NEG = -4;  // WOS returns -errno from syscalls.
constexpr const char* DEVICE_NAME = "process";

auto now_ms() -> uint64_t {
    timeval tv{};
    gettimeofday(&tv, nullptr);
    return (static_cast<uint64_t>(tv.tv_sec) * 1000) + (tv.tv_usec / 1000);
}

auto worker_output_path(int pid, int repeat_index, int worker_id) -> std::string {
    return std::format("/tmp/mandelbench_{}_{}_{}.raw", pid, repeat_index, worker_id);
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

auto write_all(int fd, std::span<const unsigned char> bytes) -> bool {
    size_t written_total = 0;
    while (written_total < bytes.size()) {
        ssize_t const WRITTEN = write(fd, bytes.data() + written_total, bytes.size() - written_total);
        if (WRITTEN <= 0) {
            return false;
        }
        written_total += static_cast<size_t>(WRITTEN);
    }
    return true;
}

auto get_testprog_path() -> const char* { return "/bin/testprog"; }

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
    int64_t child_pid;
};

auto wait_for_worker(const WorkerLaunch& launch) -> bool {
    int32_t status = 0;
    int64_t ret = 0;
    while (true) {
        ret = static_cast<int64_t>(ker::process::waitpid(launch.child_pid, &status, 0, nullptr));
        if (ret != EINTR_NEG) {
            break;
        }
    }

    if (ret < 0) {
        std::println(stderr, "mandelbench: waitpid({}) failed: {}", launch.child_pid, ret);
        return false;
    }
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        return true;
    }
    if (WIFEXITED(status)) {
        std::println(stderr, "mandelbench: worker {} exited with code {}", launch.output_slot, WEXITSTATUS(status));
    } else {
        std::println(stderr, "mandelbench: worker {} terminated with signal {}", launch.output_slot, WTERMSIG(status));
    }
    return false;
}

}  // namespace

auto mandelbench_wki(int width, int height, int max_iteration, int workers, int repeat, const char* nodes) -> int {
    (void)nodes;
    int const PID = static_cast<int>(ker::process::getpid());

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
    std::vector<double> times(repeat);

    int repeat_index = 0;
    for (auto& elapsed_seconds : times) {
        std::ranges::fill(image, 0);
        uint64_t const START_TIME = now_ms();

        std::vector<WorkerLaunch> launches;
        launches.reserve(static_cast<size_t>(ACTIVE_WORKERS));

        int next_start_row = 0;
        int const BASE_ROWS = height / ACTIVE_WORKERS;
        int const EXTRA_ROWS = height % ACTIVE_WORKERS;
        for (int worker_id = 0; worker_id < ACTIVE_WORKERS; worker_id++) {
            int const ROW_COUNT = BASE_ROWS + (worker_id < EXTRA_ROWS ? 1 : 0);
            launches.push_back(WorkerLaunch{
                .output_slot = worker_id,
                .start_row = next_start_row,
                .row_count = ROW_COUNT,
                .child_pid = -1,
            });
            next_start_row += ROW_COUNT;
        }

        for (auto& launch : launches) {
            auto output = worker_output_path(PID, repeat_index, launch.output_slot);
            // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
            std::array<std::string, 17> arg_storage = {
                "testprog",    "--mandelbench-worker",
                "--id",        std::to_string(launch.output_slot),
                "--threads",   std::to_string(1),
                "--start-row", std::to_string(launch.start_row),
                "--row-count", std::to_string(launch.row_count),
                "--width",     std::to_string(width),
                "--height",    std::to_string(height),
                "--max-iter",  std::to_string(max_iteration),
                "--output",
            };
            std::array<char*, arg_storage.size() + 2> argv{};
            auto* argv_out = argv.begin();
            for (auto& arg : arg_storage) {
                *argv_out++ = arg.data();
            }
            *argv_out++ = output.data();
            *argv_out = nullptr;

            int64_t child_pid = ker::process::fork();
            if (child_pid < 0) {
                std::println(stderr, "mandelbench: fork failed for worker {}: {}", launch.output_slot, child_pid);
                return 1;
            }
            if (child_pid == 0) {
                execv(get_testprog_path(), argv.data());
                std::println(stderr, "mandelbench: execv failed for worker {}", launch.output_slot);
                _exit(1);
            }
            launch.child_pid = child_pid;
        }

        const auto FAILED_WORKERS =
            std::count_if(launches.begin(), launches.end(), [](const auto& launch) { return !wait_for_worker(launch); });
        if (FAILED_WORKERS != 0) {
            for (const auto& launch : launches) {
                auto path = worker_output_path(PID, repeat_index, launch.output_slot);
                unlink(path.c_str());
            }
            return 1;
        }

        size_t const ROW_SIZE = static_cast<size_t>(width) * 4;
        for (const auto& launch : launches) {
            auto path = worker_output_path(PID, repeat_index, launch.output_slot);
            FILE* f = fopen(path.c_str(), "rb");
            if (f == nullptr) {
                std::println(stderr, "mandelbench: failed to open worker output {}", path);
                return 1;
            }

            size_t const CHUNK_SIZE = static_cast<size_t>(launch.row_count) * ROW_SIZE;
            size_t const IMAGE_OFFSET = static_cast<size_t>(launch.start_row) * ROW_SIZE;
            size_t const BYTES_READ = fread(image.data() + IMAGE_OFFSET, 1, CHUNK_SIZE, f);
            fclose(f);
            unlink(path.c_str());
            if (BYTES_READ != CHUNK_SIZE) {
                std::println(stderr, "mandelbench: short read for worker {} from {}", launch.output_slot, path);
                return 1;
            }
        }

        uint64_t const END_TIME = now_ms();
        elapsed_seconds = static_cast<double>(END_TIME - START_TIME) / 1000.0;

        std::string const IMG_PATH = std::format(IMAGE, DEVICE_NAME, repeat_index);
        save_image(IMG_PATH.c_str(), image.data(), static_cast<unsigned>(width), static_cast<unsigned>(height));
        progress(DEVICE_NAME, width, height, max_iteration, workers, repeat, repeat_index, elapsed_seconds);
        repeat_index++;
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
        } else if (ARG == "--width" && i + 1 < argc) {
            parse_int_arg(argv[++i], width);
        } else if (ARG == "--height" && i + 1 < argc) {
            parse_int_arg(argv[++i], height);
        } else if (ARG == "--max-iter" && i + 1 < argc) {
            parse_int_arg(argv[++i], max_iter);
        } else if (ARG == "--output" && i + 1 < argc) {
            output = argv[++i];
        }
    }

    if (id < 0 || thread_count <= 0 || start_row < 0 || row_count <= 0 || width <= 0 || height <= 0 || max_iter <= 0 || output == nullptr) {
        std::println(stderr, "mandelbench-worker: missing required arguments");
        return 1;
    }

    std::vector<unsigned char> colormap(static_cast<size_t>((max_iter + 1) * 3));
    init_colormap(max_iter + 1, colormap.data());

    size_t const ROW_SIZE = static_cast<size_t>(width) * 4;
    std::vector<unsigned char> image(static_cast<size_t>(row_count) * ROW_SIZE);
    struct WorkerThread {
        WorkerThreadArg arg{};
        thrd_t thread{};
    };
    std::vector<WorkerThread> worker_threads(static_cast<size_t>(thread_count));

    int thread_index = 0;
    for (auto& worker_thread : worker_threads) {
        auto& thread_arg = worker_thread.arg;
        thread_arg.image = image.data();
        thread_arg.colormap = colormap.data();
        thread_arg.width = width;
        thread_arg.height = height;
        thread_arg.max_iteration = max_iter;
        thread_arg.start_row = start_row;
        thread_arg.row_count = row_count;
        thread_arg.local_thread_id = thread_index;
        thread_arg.local_thread_count = thread_count;
        thread_arg.rows_done = 0;

        if (thrd_create(&worker_thread.thread, generate_rows, &thread_arg) != THRD_SUCCESS) {
            std::println(stderr, "mandelbench-worker[{}]: failed to create thread {}", id, thread_index);
            return 1;
        }
        thread_index++;
    }

    int rows_done = 0;
    int joined_thread_index = 0;
    for (auto& worker_thread : worker_threads) {
        if (thrd_join(worker_thread.thread, nullptr) != THRD_SUCCESS) {
            std::println(stderr, "mandelbench-worker[{}]: failed to join thread {}", id, joined_thread_index);
            return 1;
        }
        rows_done += worker_thread.arg.rows_done;
        joined_thread_index++;
    }

    int const FD = open(output, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (FD < 0) {
        std::println(stderr, "mandelbench-worker[{}]: failed to open output '{}'", id, output);
        return 1;
    }

    if (!write_all(FD, image)) {
        close(FD);
        std::println(stderr, "mandelbench-worker[{}]: failed while writing '{}'", id, output);
        return 1;
    }

    if (close(FD) != 0) {
        std::println(stderr, "mandelbench-worker[{}]: close failed for '{}'", id, output);
        return 1;
    }

    if (MANDELBENCH_DEBUG_ENABLED) {
        std::println(stderr, "mandelbench-worker[{}]: computed {} rows using {} threads", id, rows_done, thread_count);
    }
    return 0;
}
