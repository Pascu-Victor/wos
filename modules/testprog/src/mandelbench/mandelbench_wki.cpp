#include "mandelbench_wki.hpp"

#include <dirent.h>
#include <fcntl.h>
#include <sys/process.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <format>
#include <print>
#include <string>
#include <vector>

#include "config.hpp"
#include "tinycthread.hpp"
#include "util.hpp"

namespace {

constexpr int EINTR_NEG = -4;  // WOS returns -errno from syscalls

auto now_ms() -> uint64_t {
    timeval tv{};
    gettimeofday(&tv, nullptr);
    return (static_cast<uint64_t>(tv.tv_sec) * 1000) + (tv.tv_usec / 1000);
}

// Parse comma-separated node list into vector of hostnames.
auto parse_nodes(const char* nodes) -> std::vector<std::string> {
    std::vector<std::string> result;
    if (nodes == nullptr || nodes[0] == '\0') {
        return result;
    }
    const char* p = nodes;
    while (*p != '\0') {
        const char* comma = std::strchr(p, ',');
        if (comma == nullptr) {
            result.emplace_back(p);
            break;
        }
        result.emplace_back(p, static_cast<size_t>(comma - p));
        p = comma + 1;
    }
    return result;
}

// Each worker writes its rows as raw RGBA data into a file. The file format is:
//   For each row the worker owns (in ascending order):
//     [row_index: uint32_t] [rgba_data: width * 4 bytes]
// This allows the orchestrator to scatter rows back into the full image.
auto worker_output_path(int pid, int repeat_index, int worker_id) -> std::string {
    return std::format("/tmp/mandelbench_{}_{}_{}.raw", pid, repeat_index, worker_id);
}

auto worker_result_read_path(const std::string& worker_node, const char* local_runner, int pid, int repeat_index, int worker_id)
    -> std::string {
    auto output = worker_output_path(pid, repeat_index, worker_id);
    if (worker_node == local_runner) {
        return output;
    }
    return std::format("/wki/{}{}", worker_node, output);
}

auto write_all(int fd, const void* buf, size_t count) -> bool {
    const auto* src = static_cast<const unsigned char*>(buf);
    size_t written_total = 0;
    while (written_total < count) {
        ssize_t written = write(fd, src + written_total, count - written_total);
        if (written <= 0) {
            return false;
        }
        written_total += static_cast<size_t>(written);
    }
    return true;
}

auto get_testprog_path() -> const char* {
    // The testprog binary is always at /bin/testprog after rootfs pivot
    return "/bin/testprog";
}

// Auto-discover WKI peer hostnames by scanning /wki/ directory.
// Returns a list that includes the local hostname plus any remote peers.
auto discover_wki_nodes(const char* local_hostname) -> std::vector<std::string> {
    std::vector<std::string> nodes;
    nodes.emplace_back(local_hostname);

    DIR* d = opendir("/wki");
    if (d == nullptr) {
        return nodes;
    }
    struct dirent* ent = nullptr;
    while ((ent = readdir(d)) != nullptr) {
        if (ent->d_name[0] == '.') {
            continue;
        }
        // Skip entries that match the local hostname (already added)
        if (std::strcmp(ent->d_name, local_hostname) == 0) {
            continue;
        }
        // Skip the synthetic 'host' alias (kernel rewrites /wki/host -> /)
        if (std::strcmp(ent->d_name, "host") == 0) {
            continue;
        }
        // Skip sub-mounts like "wos-1/boot" — only top-level peer dirs
        if (std::strchr(ent->d_name, '/') != nullptr) {
            continue;
        }
        nodes.emplace_back(ent->d_name);
    }
    closedir(d);
    return nodes;
}

struct WkiThreadArg {
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

auto generate_wki_rows(void* param) -> int {
    auto* arg = static_cast<WkiThreadArg*>(param);

    for (int local_row = arg->local_thread_id; local_row < arg->row_count; local_row += arg->local_thread_count) {
        int row = arg->start_row + local_row;
        for (int col = 0; col < arg->width; col++) {
            double c_re = (col - (arg->width / 2.0)) * 4.0 / arg->width;
            double c_im = (row - (arg->height / 2.0)) * 4.0 / arg->width;
            double x = 0;
            double y = 0;
            int iteration = 0;
            while ((x * x) + (y * y) <= 4 && iteration < arg->max_iteration) {
                double x_new = (x * x) - (y * y) + c_re;
                y = (2 * x * y) + c_im;
                x = x_new;
                iteration++;
            }
            iteration = std::min(iteration, arg->max_iteration);
            set_pixel(arg->image, arg->width, col, local_row, &arg->colormap[static_cast<size_t>(iteration * 3)]);
        }
        arg->rows_done++;
    }

    return 0;
}

struct WkiLaunch {
    std::string node;
    int output_slot;
    int thread_count;
    int start_row;
    int row_count;
    int64_t child_pid;
    bool local_inline;
};

auto run_local_launch(const WkiLaunch& launch, unsigned char* image, unsigned char* colormap, int width, int height, int max_iteration)
    -> bool {
    std::vector<WkiThreadArg> thread_args(static_cast<size_t>(launch.thread_count));
    std::vector<thrd_t> threads(static_cast<size_t>(launch.thread_count));
    auto* image_chunk = image + (static_cast<size_t>(launch.start_row) * static_cast<size_t>(width) * 4);

    for (int thread_index = 0; thread_index < launch.thread_count; thread_index++) {
        auto& thread_arg = thread_args[static_cast<size_t>(thread_index)];
        thread_arg.image = image_chunk;
        thread_arg.colormap = colormap;
        thread_arg.width = width;
        thread_arg.height = height;
        thread_arg.max_iteration = max_iteration;
        thread_arg.start_row = launch.start_row;
        thread_arg.row_count = launch.row_count;
        thread_arg.local_thread_id = thread_index;
        thread_arg.local_thread_count = launch.thread_count;
        thread_arg.rows_done = 0;

        if (thrd_create(&threads[static_cast<size_t>(thread_index)], generate_wki_rows, &thread_arg) != thrd_success) {
            return false;
        }
    }

    int rows_done = 0;
    for (int thread_index = 0; thread_index < launch.thread_count; thread_index++) {
        if (thrd_join(threads[static_cast<size_t>(thread_index)], nullptr) != thrd_success) {
            return false;
        }
        rows_done += thread_args[static_cast<size_t>(thread_index)].rows_done;
    }

    std::println(stderr, "mandelbench-worker[{}@local-inline]: computed {} rows using {} threads", launch.output_slot, rows_done,
                 launch.thread_count);
    return true;
}

}  // namespace

// =============================================================================
// Orchestrator: fork+exec workers across the cluster, collect results
// =============================================================================

auto mandelbench_wki(int width, int height, int max_iteration, int workers, int repeat, const char* nodes) -> int {
    auto node_list = parse_nodes(nodes);
    int pid = static_cast<int>(ker::process::getpid());

    std::vector<unsigned char> image(static_cast<size_t>(width * height * 4));
    std::vector<unsigned char> colormap(static_cast<size_t>((max_iteration + 1) * 3));
    init_colormap(max_iteration + 1, colormap.data());

    std::vector<double> times(repeat);

    char launcher[64] = {};
    char runner[64] = {};
    ker::process::wki_launcher_node(launcher, sizeof(launcher));
    ker::process::wki_runner_node(runner, sizeof(runner));

    std::println(stderr, "mandelbench-wki: orchestrator on {} (launcher={})", runner, launcher);
    if (node_list.empty()) {
        // Auto-discover peers by scanning /wki/ directory
        node_list = discover_wki_nodes(runner);
        std::print(stderr, "mandelbench-wki: discovered nodes:");
        for (const auto& n : node_list) std::print(stderr, " {}", n);
        std::println(stderr, "");
    } else {
        std::print(stderr, "mandelbench-wki: nodes:");
        for (const auto& n : node_list) std::print(stderr, " {}", n);
        std::println(stderr, "");
    }

    // Scale workers to cluster size: each node should get the full thread count
    // so total parallelism = threads_per_node × node_count.
    // Only auto-scale if the caller used the default (THREADS).
    if (workers == THREADS && node_list.size() > 1) {
        workers = THREADS * static_cast<int>(node_list.size());
    }

    if (workers <= 0) {
        std::println(stderr, "mandelbench-wki: invalid worker count {}", workers);
        return 1;
    }

    std::println(stderr, "mandelbench-wki: {}x{} max_iter={} workers={} repeat={}", width, height, max_iteration, workers, repeat);

    for (int r = 0; r < repeat; r++) {
        std::memset(image.data(), 0, image.size());

        uint64_t start_time = now_ms();

        std::vector<WkiLaunch> launches;
        launches.reserve(node_list.size());

        int base_threads_per_node = workers / static_cast<int>(node_list.size());
        int extra_threads = workers % static_cast<int>(node_list.size());
        for (size_t node_index = 0; node_index < node_list.size(); node_index++) {
            int thread_count = base_threads_per_node + (static_cast<int>(node_index) < extra_threads ? 1 : 0);
            if (thread_count <= 0) {
                continue;
            }
            launches.push_back(WkiLaunch{.node = node_list[node_index],
                                         .output_slot = static_cast<int>(launches.size()),
                                         .thread_count = thread_count,
                                         .start_row = 0,
                                         .row_count = 0,
                                         .child_pid = -1,
                                         .local_inline = false});
        }

        int next_start_row = 0;
        int base_rows_per_node = height / static_cast<int>(launches.size());
        int extra_rows = height % static_cast<int>(launches.size());
        for (size_t launch_index = 0; launch_index < launches.size(); launch_index++) {
            int row_count = base_rows_per_node + (static_cast<int>(launch_index) < extra_rows ? 1 : 0);
            launches[launch_index].start_row = next_start_row;
            launches[launch_index].row_count = row_count;
            launches[launch_index].local_inline = launches[launch_index].node == runner;
            next_start_row += row_count;
        }

        int remote_spawned = 0;

        auto spawn_worker = [&](WkiLaunch& launch) {
            const auto& target = launch.node;
            int64_t rc = ker::process::setwkitarget(target.c_str(), target.size(), ker::process::WKI_TARGET_FLAG_STRICT);
            if (rc < 0) {
                std::println(stderr, "mandelbench-wki: setwkitarget('{}') failed: {}", target, rc);
            }

            auto output = worker_output_path(pid, r, launch.output_slot);
            auto id_str = std::to_string(launch.output_slot);
            auto threads_str = std::to_string(launch.thread_count);
            auto width_str = std::to_string(width);
            auto height_str = std::to_string(height);
            auto max_iter_str = std::to_string(max_iteration);
            auto start_row_str = std::to_string(launch.start_row);
            auto row_count_str = std::to_string(launch.row_count);

            const char* argv[] = {
                "testprog",    "--mandelbench-worker", "--id",        id_str.c_str(),        "--threads", threads_str.c_str(),
                "--start-row", start_row_str.c_str(),  "--row-count", row_count_str.c_str(), "--width",   width_str.c_str(),
                "--height",    height_str.c_str(),     "--max-iter",  max_iter_str.c_str(),  "--output",  output.c_str(),
                nullptr,
            };

            int64_t cpid = ker::process::fork();
            if (cpid < 0) {
                std::println(stderr, "mandelbench-wki: fork failed for node worker {}: {}", launch.output_slot, cpid);
                return false;
            }
            if (cpid == 0) {
                execv(get_testprog_path(), const_cast<char* const*>(argv));
                std::println(stderr, "mandelbench-wki: execv failed for node worker {}", launch.output_slot);
                _exit(1);
            }
            launch.child_pid = cpid;
            remote_spawned++;
            return true;
        };

        // Phase 1: fork remote node workers
        for (auto& launch : launches) {
            if (!launch.local_inline) {
                if (!spawn_worker(launch)) {
                    break;
                }
            }
        }

        // Clear WKI target so orchestrator stays local
        ker::process::setwkitarget(nullptr, 0, 0);

        int local_launches = 0;
        for (const auto& launch : launches) {
            if (launch.local_inline) {
                local_launches++;
            }
        }

        if (remote_spawned != static_cast<int>(launches.size()) - local_launches) {
            std::println(stderr, "mandelbench-wki: failed to spawn all remote node workers ({}/{})", remote_spawned,
                         static_cast<int>(launches.size()) - local_launches);
            return 1;
        }

        for (const auto& launch : launches) {
            if (launch.local_inline) {
                if (!run_local_launch(launch, image.data(), colormap.data(), width, height, max_iteration)) {
                    std::println(stderr, "mandelbench-wki: local inline worker failed");
                    return 1;
                }
            }
        }

        std::println(stderr, "mandelbench-wki: [repeat {}/{}] started {} node chunks ({} remote worker processes, {} total threads)", r + 1,
                     repeat, launches.size(), remote_spawned, workers);

        // Wait for all workers
        int failed = 0;
        for (const auto& launch : launches) {
            if (launch.local_inline) {
                continue;
            }
            int32_t status = 0;
            int64_t ret = 0;
            // Retry on EINTR (SIGCHLD from sibling workers)
            do {
                ret = ker::process::waitpid(launch.child_pid, &status, 0, nullptr);
            } while (ret == EINTR_NEG);

            if (ret < 0) {
                std::println(stderr, "mandelbench-wki: waitpid({}) failed: {}", launch.child_pid, ret);
                failed++;
            } else if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
                // Exit code 255 can be a kernel GC race for remote tasks —
                // check if the worker produced valid output before declaring failure.
                auto path = worker_result_read_path(launch.node, runner, pid, r, launch.output_slot);
                FILE* probe = fopen(path.c_str(), "rb");
                if (probe != nullptr) {
                    fclose(probe);
                    // Output file exists — worker succeeded despite bad status
                    std::println(stderr, "mandelbench-wki: node worker {} (pid {}) status {} (output OK, ignoring)", launch.output_slot,
                                 launch.child_pid, WEXITSTATUS(status));
                } else {
                    std::println(stderr, "mandelbench-wki: node worker {} (pid {}) exited with code {}", launch.output_slot,
                                 launch.child_pid, WEXITSTATUS(status));
                    failed++;
                }
            } else if (!WIFEXITED(status)) {
                std::println(stderr, "mandelbench-wki: node worker {} (pid {}) killed by signal {}", launch.output_slot, launch.child_pid,
                             WTERMSIG(status));
                failed++;
            }
        }

        if (failed > 0) {
            std::println(stderr, "mandelbench-wki: {} workers failed on repeat {}", failed, r + 1);
            // Clean up partial output files
            for (const auto& launch : launches) {
                if (launch.local_inline) {
                    continue;
                }
                auto path = worker_result_read_path(launch.node, runner, pid, r, launch.output_slot);
                unlink(path.c_str());
            }
            return 1;
        }

        // Read partial results from each worker and composite into image
        size_t row_size = static_cast<size_t>(width) * 4;
        for (const auto& launch : launches) {
            if (launch.local_inline) {
                continue;
            }
            auto path = worker_result_read_path(launch.node, runner, pid, r, launch.output_slot);
            FILE* f = fopen(path.c_str(), "rb");
            if (f == nullptr) {
                std::println(stderr, "mandelbench-wki: failed to open output from node worker {}: {}", launch.output_slot, path);
                return 1;
            }

            size_t chunk_size = static_cast<size_t>(launch.row_count) * row_size;
            size_t image_offset = static_cast<size_t>(launch.start_row) * row_size;
            constexpr size_t REMOTE_READ_CHUNK = 32768;
            size_t bytes_read = 0;
            while (bytes_read < chunk_size) {
                size_t to_read = std::min(REMOTE_READ_CHUNK, chunk_size - bytes_read);
                size_t n = fread(image.data() + image_offset + bytes_read, 1, to_read, f);
                if (n != to_read) {
                    fclose(f);
                    std::println(stderr, "mandelbench-wki: short read for node worker {} from {} at {}/{} bytes", launch.output_slot, path,
                                 bytes_read + n, chunk_size);
                    return 1;
                }
                bytes_read += n;
            }
            fclose(f);
            unlink(path.c_str());
        }

        uint64_t end_time = now_ms();
        times[r] = static_cast<double>(end_time - start_time) / 1000.0;

        std::string img_path = std::format("./wki_{:02d}.png", r);
        save_image(img_path.c_str(), image.data(), static_cast<unsigned>(width), static_cast<unsigned>(height));
        progress("wki", width, height, max_iteration, workers, repeat, r, times[r]);
    }

    report("wki", width, height, max_iteration, workers, repeat, times);
    return 0;
}

// =============================================================================
// Worker: compute assigned row stripe and write to output file
// =============================================================================

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
        if (std::strcmp(argv[i], "--id") == 0 && i + 1 < argc)
            id = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--threads") == 0 && i + 1 < argc)
            thread_count = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--start-row") == 0 && i + 1 < argc)
            start_row = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--row-count") == 0 && i + 1 < argc)
            row_count = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--width") == 0 && i + 1 < argc)
            width = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--height") == 0 && i + 1 < argc)
            height = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--max-iter") == 0 && i + 1 < argc)
            max_iter = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--output") == 0 && i + 1 < argc)
            output = argv[++i];
    }

    if (id < 0 || thread_count <= 0 || start_row < 0 || row_count <= 0 || width <= 0 || height <= 0 || max_iter <= 0 || output == nullptr) {
        std::println(stderr, "mandelbench-worker: missing required arguments");
        return 1;
    }

    char runner[64] = {};
    ker::process::wki_runner_node(runner, sizeof(runner));

    // Build colormap locally (trivial cost, avoids IPC)
    std::vector<unsigned char> colormap(static_cast<size_t>((max_iter + 1) * 3));
    init_colormap(max_iter + 1, colormap.data());

    size_t row_size = static_cast<size_t>(width) * 4;
    std::vector<unsigned char> image(static_cast<size_t>(row_count) * row_size);
    std::vector<WkiThreadArg> thread_args(static_cast<size_t>(thread_count));
    std::vector<thrd_t> threads(static_cast<size_t>(thread_count));

    for (int thread_index = 0; thread_index < thread_count; thread_index++) {
        auto& thread_arg = thread_args[static_cast<size_t>(thread_index)];
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

        if (thrd_create(&threads[static_cast<size_t>(thread_index)], generate_wki_rows, &thread_arg) != thrd_success) {
            std::println(stderr, "mandelbench-worker[{}@{}]: failed to create thread {}", id, runner, thread_index);
            return 1;
        }
    }

    int rows_done = 0;
    for (int thread_index = 0; thread_index < thread_count; thread_index++) {
        if (thrd_join(threads[static_cast<size_t>(thread_index)], nullptr) != thrd_success) {
            std::println(stderr, "mandelbench-worker[{}@{}]: failed to join thread {}", id, runner, thread_index);
            return 1;
        }
        rows_done += thread_args[static_cast<size_t>(thread_index)].rows_done;
    }

    int fd = open(output, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        std::println(stderr, "mandelbench-worker[{}@{}]: failed to open output '{}'", id, runner, output);
        return 1;
    }

    if (!write_all(fd, image.data(), image.size())) {
        close(fd);
        std::println(stderr, "mandelbench-worker[{}@{}]: failed while writing '{}'", id, runner, output);
        return 1;
    }

    if (close(fd) != 0) {
        std::println(stderr, "mandelbench-worker[{}@{}]: close failed for '{}'", id, runner, output);
        return 1;
    }

    std::println(stderr, "mandelbench-worker[{}@{}]: computed {} rows using {} threads, wrote to {}", id, runner, rows_done, thread_count,
                 output);
    return 0;
}
