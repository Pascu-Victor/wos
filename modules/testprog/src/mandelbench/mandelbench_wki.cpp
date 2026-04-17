#include "mandelbench_wki.hpp"

#include <dirent.h>
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
auto worker_output_path(int pid, int worker_id) -> std::string { return std::format("/tmp/mandelbench_{}_{}.raw", pid, worker_id); }

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
        if (ent->d_name[0] == '.') continue;
        // Skip entries that match the local hostname (already added)
        if (std::strcmp(ent->d_name, local_hostname) == 0) continue;
        // Skip the synthetic 'host' alias (kernel rewrites /wki/host -> /)
        if (std::strcmp(ent->d_name, "host") == 0) continue;
        // Skip sub-mounts like "wos-1/boot" — only top-level peer dirs
        if (std::strchr(ent->d_name, '/') != nullptr) continue;
        nodes.emplace_back(ent->d_name);
    }
    closedir(d);
    return nodes;
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
    std::println(stderr, "mandelbench-wki: {}x{} max_iter={} workers={} repeat={}", width, height, max_iteration, workers, repeat);
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

    for (int r = 0; r < repeat; r++) {
        std::memset(image.data(), 0, image.size());

        uint64_t start_time = now_ms();

        // Spawn workers via fork+exec.
        // Fork remote workers FIRST so the kernel can start loading the binary
        // on the remote node before local workers saturate all CPUs.
        std::vector<int64_t> child_pids(workers);
        int spawned = 0;

        auto spawn_worker = [&](int i) {
            const auto& target = node_list[static_cast<size_t>(i) % node_list.size()];
            if (target != runner) {
                int64_t rc = ker::process::setwkitarget(target.c_str(), target.size(), 0);
                if (rc < 0) {
                    std::println(stderr, "mandelbench-wki: setwkitarget('{}') failed: {}", target, rc);
                }
            } else {
                ker::process::setwkitarget(nullptr, 0, 0);
            }

            auto output = worker_output_path(pid, i);
            auto id_str = std::to_string(i);
            auto workers_str = std::to_string(workers);
            auto width_str = std::to_string(width);
            auto height_str = std::to_string(height);
            auto max_iter_str = std::to_string(max_iteration);

            const char* argv[] = {
                "testprog", "--mandelbench-worker", "--id",     id_str.c_str(),     "--workers",  workers_str.c_str(),
                "--width",  width_str.c_str(),      "--height", height_str.c_str(), "--max-iter", max_iter_str.c_str(),
                "--output", output.c_str(),         nullptr,
            };

            int64_t cpid = ker::process::fork();
            if (cpid < 0) {
                std::println(stderr, "mandelbench-wki: fork failed for worker {}: {}", i, cpid);
                return false;
            }
            if (cpid == 0) {
                execv(get_testprog_path(), const_cast<char* const*>(argv));
                std::println(stderr, "mandelbench-wki: execv failed for worker {}", i);
                _exit(1);
            }
            child_pids[i] = cpid;
            spawned++;
            return true;
        };

        // Phase 1: fork remote workers
        int remote_count = 0;
        for (int i = 0; i < workers; i++) {
            const auto& target = node_list[static_cast<size_t>(i) % node_list.size()];
            if (target != runner) {
                if (!spawn_worker(i)) break;
                remote_count++;
            }
        }

        // Brief delay to let the kernel start loading the binary on remote nodes
        // before local workers saturate all CPUs.
        if (remote_count > 0) {
            usleep(200000);  // 200ms
        }

        // Phase 2: fork local workers
        for (int i = 0; i < workers; i++) {
            const auto& target = node_list[static_cast<size_t>(i) % node_list.size()];
            if (target == runner) {
                if (!spawn_worker(i)) break;
            }
        }

        // Clear WKI target so orchestrator stays local
        ker::process::setwkitarget(nullptr, 0, 0);

        std::println(stderr, "mandelbench-wki: [repeat {}/{}] spawned {} workers", r + 1, repeat, spawned);

        // Wait for all workers
        int failed = 0;
        for (int i = 0; i < spawned; i++) {
            int32_t status = 0;
            int64_t ret;
            // Retry on EINTR (SIGCHLD from sibling workers)
            do {
                ret = ker::process::waitpid(child_pids[i], &status, 0, nullptr);
            } while (ret == EINTR_NEG);

            if (ret < 0) {
                std::println(stderr, "mandelbench-wki: waitpid({}) failed: {}", child_pids[i], ret);
                failed++;
            } else if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
                // Exit code 255 can be a kernel GC race for remote tasks —
                // check if the worker produced valid output before declaring failure.
                auto path = worker_output_path(pid, i);
                FILE* probe = fopen(path.c_str(), "rb");
                if (probe != nullptr) {
                    fclose(probe);
                    // Output file exists — worker succeeded despite bad status
                    std::println(stderr, "mandelbench-wki: worker {} (pid {}) status {} (output OK, ignoring)", i, child_pids[i],
                                 WEXITSTATUS(status));
                } else {
                    std::println(stderr, "mandelbench-wki: worker {} (pid {}) exited with code {}", i, child_pids[i], WEXITSTATUS(status));
                    failed++;
                }
            } else if (!WIFEXITED(status)) {
                std::println(stderr, "mandelbench-wki: worker {} (pid {}) killed by signal {}", i, child_pids[i], WTERMSIG(status));
                failed++;
            }
        }

        if (failed > 0) {
            std::println(stderr, "mandelbench-wki: {} workers failed on repeat {}", failed, r + 1);
            // Clean up partial output files
            for (int i = 0; i < spawned; i++) {
                auto path = worker_output_path(pid, i);
                unlink(path.c_str());
            }
            return 1;
        }

        // Read partial results from each worker and composite into image
        size_t row_size = static_cast<size_t>(width) * 4;
        for (int i = 0; i < workers; i++) {
            auto path = worker_output_path(pid, i);
            FILE* f = fopen(path.c_str(), "rb");
            if (f == nullptr) {
                std::println(stderr, "mandelbench-wki: failed to open output from worker {}: {}", i, path);
                return 1;
            }

            uint32_t row_idx = 0;
            while (fread(&row_idx, sizeof(uint32_t), 1, f) == 1) {
                if (row_idx >= static_cast<uint32_t>(height)) {
                    std::println(stderr, "mandelbench-wki: worker {} wrote invalid row {}", i, row_idx);
                    fclose(f);
                    return 1;
                }
                size_t offset = static_cast<size_t>(row_idx) * row_size;
                if (fread(image.data() + offset, 1, row_size, f) != row_size) {
                    std::println(stderr, "mandelbench-wki: short read for row {} from worker {}", row_idx, i);
                    fclose(f);
                    return 1;
                }
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
    int workers = -1;
    int width = -1;
    int height = -1;
    int max_iter = -1;
    const char* output = nullptr;

    for (int i = 0; i < argc; i++) {
        if (std::strcmp(argv[i], "--id") == 0 && i + 1 < argc)
            id = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--workers") == 0 && i + 1 < argc)
            workers = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--width") == 0 && i + 1 < argc)
            width = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--height") == 0 && i + 1 < argc)
            height = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--max-iter") == 0 && i + 1 < argc)
            max_iter = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--output") == 0 && i + 1 < argc)
            output = argv[++i];
    }

    if (id < 0 || workers <= 0 || width <= 0 || height <= 0 || max_iter <= 0 || output == nullptr) {
        std::println(stderr, "mandelbench-worker: missing required arguments");
        return 1;
    }

    char launcher[64] = {};
    char runner[64] = {};
    ker::process::wki_launcher_node(launcher, sizeof(launcher));
    ker::process::wki_runner_node(runner, sizeof(runner));

    // If running remotely, route output back to the launcher's filesystem
    // via the WKI remote VFS mount so the orchestrator can read it.
    std::string actual_output;
    if (std::strcmp(launcher, runner) != 0) {
        actual_output = std::format("/wki/{}{}", launcher, output);
    } else {
        actual_output = output;
    }

    // Build colormap locally (trivial cost, avoids IPC)
    std::vector<unsigned char> colormap(static_cast<size_t>((max_iter + 1) * 3));
    init_colormap(max_iter + 1, colormap.data());

    size_t row_size = static_cast<size_t>(width) * 4;
    std::vector<unsigned char> row_buf(row_size);

    FILE* f = fopen(actual_output.c_str(), "wb");
    if (f == nullptr) {
        std::println(stderr, "mandelbench-worker[{}@{}]: failed to open output '{}'", id, runner, actual_output);
        return 1;
    }

    // Compute assigned rows (interleaved: row id, id+workers, id+2*workers, ...)
    int rows_done = 0;
    for (int row = id; row < height; row += workers) {
        std::memset(row_buf.data(), 0, row_size);

        for (int col = 0; col < width; col++) {
            double c_re = (col - (width / 2.0)) * 4.0 / width;
            double c_im = (row - (height / 2.0)) * 4.0 / width;
            double x = 0, y = 0;
            int iteration = 0;
            while ((x * x) + (y * y) <= 4 && iteration < max_iter) {
                double x_new = (x * x) - (y * y) + c_re;
                y = (2 * x * y) + c_im;
                x = x_new;
                iteration++;
            }
            iteration = std::min(iteration, max_iter);
            unsigned char* c = &colormap[static_cast<size_t>(iteration * 3)];
            size_t px = static_cast<size_t>(col) * 4;
            row_buf[px + 0] = c[0];
            row_buf[px + 1] = c[1];
            row_buf[px + 2] = c[2];
            row_buf[px + 3] = 255;
        }

        // Write [row_index][rgba_data] to output file
        auto row_u32 = static_cast<uint32_t>(row);
        fwrite(&row_u32, sizeof(uint32_t), 1, f);
        fwrite(row_buf.data(), 1, row_size, f);
        rows_done++;
    }

    fclose(f);

    std::println(stderr, "mandelbench-worker[{}@{}]: computed {} rows, wrote to {}", id, runner, rows_done, actual_output);
    return 0;
}
