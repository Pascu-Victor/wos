#include <fcntl.h>
#include <signal.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <mandelbench/tinycthread.hpp>
#include <print>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "render_core.hpp"

namespace {

constexpr std::array<char, 4> TILE_PACKET_MAGIC = {'R', 'B', 'T', 'L'};
constexpr int WORKER_STDOUT_FD = 1;
volatile sig_atomic_t g_cancel_signal = 0;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

struct WkiPeerInfo {
    std::string hostname;
    int node_id = 0;
    int cpus = 1;
    bool local = false;
};

struct WorkerInvocation {
    bool enabled = false;
    int worker_id = 0;
    int worker_count = 1;
    int worker_threads = 1;
};

struct IpcWorkerSpec {
    int worker_id = 0;
    int worker_count = 1;
    int worker_threads = 1;
    std::string hostname;
};

struct TilePacketHeader {
    std::array<char, 4> magic = TILE_PACKET_MAGIC;
    uint32_t tile_index = 0;
    uint32_t x0 = 0;
    uint32_t y0 = 0;
    uint32_t x1 = 0;
    uint32_t y1 = 0;
    uint32_t float_count = 0;
};

struct ChildWorker {
    pid_t pid = -1;
    int read_fd = -1;
    std::string hostname;
    std::vector<unsigned char> buffer;
    bool pipe_open = true;
};

struct ThreadState {
    const tracebench::Scene* scene = nullptr;
    tracebench::FilmView film;
    const tracebench::Options* options = nullptr;
    const std::vector<tracebench::Tile>* tiles = nullptr;
    std::atomic<int>* next_tile = nullptr;
    std::atomic<uint64_t>* tiles_done = nullptr;
    uint64_t seed = 0;
};

struct WorkerThreadState {
    const tracebench::Scene* scene = nullptr;
    tracebench::FilmView film;
    const tracebench::Options* options = nullptr;
    const std::vector<tracebench::Tile>* tiles = nullptr;
    std::atomic<int>* next_tile = nullptr;
    std::atomic<bool>* failed = nullptr;
    mtx_t* output_lock = nullptr;
};

void handle_cancel_signal(int signum) { g_cancel_signal = signum; }

void install_cancel_signal_handlers() {
    struct sigaction action = {};
    action.sa_handler = handle_cancel_signal;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    (void)::sigaction(SIGINT, &action, nullptr);
    (void)::sigaction(SIGTERM, &action, nullptr);
    (void)::sigaction(SIGHUP, &action, nullptr);
    (void)::sigaction(SIGQUIT, &action, nullptr);
}

auto cancel_requested() -> bool { return g_cancel_signal != 0; }

auto cancel_exit_code() -> int {
    int const SIGNAL = g_cancel_signal != 0 ? static_cast<int>(g_cancel_signal) : SIGTERM;
    return 128 + SIGNAL;
}

auto fallback_hostname() -> std::string {
    std::array<char, 64> hostname = {};
    if (::gethostname(hostname.data(), hostname.size() - 1) == 0 && hostname.front() != '\0') {
        return hostname.data();
    }
    return "localhost";
}

auto read_wki_peers() -> std::vector<WkiPeerInfo> {
    std::vector<WkiPeerInfo> peers_out;
    std::ifstream peers("/proc/wki/peers");
    std::string header;
    if (std::getline(peers, header)) {
        std::string hostname;
        int node = 0;
        int connected = 0;
        int cpus = 0;
        int load = 0;
        uint64_t last_update = 0;
        int local = 0;
        while (peers >> hostname >> node >> connected >> cpus >> load >> last_update >> local) {
            if (local == 0 && connected == 0) {
                continue;
            }
            peers_out.push_back({
                .hostname = hostname,
                .node_id = node,
                .cpus = std::max(1, cpus),
                .local = local != 0,
            });
        }
    }

    if (peers_out.empty()) {
        peers_out.push_back({
            .hostname = fallback_hostname(),
            .node_id = 0,
            .cpus = 1,
            .local = true,
        });
    }

    std::stable_sort(peers_out.begin(), peers_out.end(), [](const WkiPeerInfo& left, const WkiPeerInfo& right) {
        if (left.local != right.local) {
            return !left.local;
        }
        return left.node_id < right.node_id;
    });
    return peers_out;
}

auto local_cpu_count() -> int {
    auto peers = read_wki_peers();
    auto local = std::find_if(peers.begin(), peers.end(), [](const WkiPeerInfo& peer) { return peer.local; });
    if (local != peers.end()) {
        return local->cpus;
    }
    long const ONLINE = ::sysconf(_SC_NPROCESSORS_ONLN);
    return ONLINE > 0 ? static_cast<int>(ONLINE) : 1;
}

auto make_progress(const tracebench::Options& options, uint64_t tiles_done, uint64_t total_tiles, double started, bool done)
    -> tracebench::Progress {
    uint64_t const TOTAL_SAMPLES =
        static_cast<uint64_t>(options.width) * static_cast<uint64_t>(options.height) * static_cast<uint64_t>(options.spp);
    uint64_t const SAMPLES_DONE = total_tiles != 0 ? (TOTAL_SAMPLES * std::min(tiles_done, total_tiles)) / total_tiles : 0;
    return {
        .tiles_done = tiles_done,
        .total_tiles = total_tiles,
        .samples_done = SAMPLES_DONE,
        .total_samples = TOTAL_SAMPLES,
        .elapsed_seconds = tracebench::monotonic_seconds() - started,
        .done = done,
    };
}

auto thread_worker(void* raw) -> int {
    auto* state = static_cast<ThreadState*>(raw);
    for (;;) {
        int const INDEX = state->next_tile->fetch_add(1);
        if (INDEX >= static_cast<int>(state->tiles->size())) {
            break;
        }
        tracebench::render_tile(*state->scene, state->film, *state->options, state->tiles->at(static_cast<size_t>(INDEX)),
                                state->seed + static_cast<uint64_t>(INDEX));
        state->tiles_done->fetch_add(1);
    }
    return 0;
}

auto tile_float_count(const tracebench::Tile& tile) -> size_t {
    int const WIDTH = std::max(0, tile.x1 - tile.x0);
    int const HEIGHT = std::max(0, tile.y1 - tile.y0);
    return static_cast<size_t>(WIDTH) * static_cast<size_t>(HEIGHT) * 3U;
}

auto extract_tile_payload(tracebench::FilmView film, const tracebench::Tile& tile) -> std::vector<float> {
    std::vector<float> payload(tile_float_count(tile));
    size_t out = 0;
    for (int y = tile.y0; y < tile.y1; ++y) {
        for (int x = tile.x0; x < tile.x1; ++x) {
            size_t const SOURCE = ((static_cast<size_t>(y) * static_cast<size_t>(film.width)) + static_cast<size_t>(x)) * 3U;
            payload.at(out++) = film.rgb[SOURCE];
            payload.at(out++) = film.rgb[SOURCE + 1U];
            payload.at(out++) = film.rgb[SOURCE + 2U];
        }
    }
    return payload;
}

auto write_all(int fd, std::span<const unsigned char> bytes) -> bool {
    while (!bytes.empty()) {
        ssize_t const WRITTEN = ::write(fd, bytes.data(), bytes.size());
        if (WRITTEN < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (WRITTEN == 0) {
            return false;
        }
        bytes = bytes.subspan(static_cast<size_t>(WRITTEN));
    }
    return true;
}

auto send_tile_packet(int fd, tracebench::FilmView film, const tracebench::Tile& tile) -> bool {
    auto payload = extract_tile_payload(film, tile);
    TilePacketHeader const HEADER = {
        .magic = TILE_PACKET_MAGIC,
        .tile_index = static_cast<uint32_t>(tile.index),
        .x0 = static_cast<uint32_t>(tile.x0),
        .y0 = static_cast<uint32_t>(tile.y0),
        .x1 = static_cast<uint32_t>(tile.x1),
        .y1 = static_cast<uint32_t>(tile.y1),
        .float_count = static_cast<uint32_t>(payload.size()),
    };
    auto const* header_bytes = reinterpret_cast<const unsigned char*>(&HEADER);
    auto const* payload_bytes = reinterpret_cast<const unsigned char*>(payload.data());
    return write_all(fd, std::span<const unsigned char>(header_bytes, sizeof(HEADER))) &&
           write_all(fd, std::span<const unsigned char>(payload_bytes, payload.size() * sizeof(float)));
}

auto worker_thread(void* raw) -> int {
    auto* state = static_cast<WorkerThreadState*>(raw);
    for (;;) {
        if (state->failed->load(std::memory_order_relaxed)) {
            break;
        }
        int const INDEX = state->next_tile->fetch_add(1);
        if (INDEX >= static_cast<int>(state->tiles->size())) {
            break;
        }
        const auto& tile = state->tiles->at(static_cast<size_t>(INDEX));
        tracebench::render_tile(*state->scene, state->film, *state->options, tile, 0xBADC0DEULL + static_cast<uint64_t>(tile.index));
        mtx_lock(state->output_lock);
        bool const SENT = send_tile_packet(WORKER_STDOUT_FD, state->film, tile);
        mtx_unlock(state->output_lock);
        if (!SENT) {
            state->failed->store(true, std::memory_order_relaxed);
            break;
        }
    }
    return 0;
}

auto apply_tile_payload(tracebench::FilmView film, const TilePacketHeader& header, std::span<const float> payload) -> bool {
    if (header.magic != TILE_PACKET_MAGIC) {
        return false;
    }
    if (header.x1 > static_cast<uint32_t>(film.width) || header.y1 > static_cast<uint32_t>(film.height) || header.x0 >= header.x1 ||
        header.y0 >= header.y1) {
        return false;
    }
    tracebench::Tile const tile{
        .x0 = static_cast<int>(header.x0),
        .y0 = static_cast<int>(header.y0),
        .x1 = static_cast<int>(header.x1),
        .y1 = static_cast<int>(header.y1),
        .index = static_cast<int>(header.tile_index),
    };
    if (payload.size() != tile_float_count(tile)) {
        return false;
    }

    size_t in = 0;
    for (int y = tile.y0; y < tile.y1; ++y) {
        for (int x = tile.x0; x < tile.x1; ++x) {
            size_t const TARGET = ((static_cast<size_t>(y) * static_cast<size_t>(film.width)) + static_cast<size_t>(x)) * 3U;
            film.rgb[TARGET] = payload[in++];
            film.rgb[TARGET + 1U] = payload[in++];
            film.rgb[TARGET + 2U] = payload[in++];
        }
    }
    return true;
}

auto parse_worker_packets(ChildWorker& worker, tracebench::FilmView film, uint64_t& tiles_done) -> bool {
    size_t consumed = 0;
    while (worker.buffer.size() - consumed >= sizeof(TilePacketHeader)) {
        TilePacketHeader header = {};
        std::memcpy(&header, worker.buffer.data() + consumed, sizeof(header));
        size_t const PAYLOAD_BYTES = static_cast<size_t>(header.float_count) * sizeof(float);
        size_t const PACKET_BYTES = sizeof(TilePacketHeader) + PAYLOAD_BYTES;
        if (worker.buffer.size() - consumed < PACKET_BYTES) {
            break;
        }

        std::vector<float> payload(header.float_count);
        std::memcpy(payload.data(), worker.buffer.data() + consumed + sizeof(TilePacketHeader), PAYLOAD_BYTES);
        if (!apply_tile_payload(film, header, std::span<const float>(payload.data(), payload.size()))) {
            std::println(stderr, "renderbench: invalid tile packet from {}", worker.hostname);
            return false;
        }
        ++tiles_done;
        consumed += PACKET_BYTES;
    }

    if (consumed != 0) {
        worker.buffer.erase(worker.buffer.begin(), worker.buffer.begin() + static_cast<ptrdiff_t>(consumed));
    }
    return true;
}

auto drain_worker_pipe(ChildWorker& worker, tracebench::FilmView film, uint64_t& tiles_done) -> bool {
    std::array<unsigned char, 16384> chunk = {};
    for (;;) {
        ssize_t const READ = ::read(worker.read_fd, chunk.data(), chunk.size());
        if (READ > 0) {
            worker.buffer.insert(worker.buffer.end(), chunk.begin(), chunk.begin() + static_cast<ptrdiff_t>(READ));
            continue;
        }
        if (READ == 0) {
            ::close(worker.read_fd);
            worker.read_fd = -1;
            worker.pipe_open = false;
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }
        std::perror("renderbench: read worker pipe");
        return false;
    }
    return parse_worker_packets(worker, film, tiles_done);
}

void set_nonblocking(int fd) {
    int const FLAGS = ::fcntl(fd, F_GETFL, 0);
    if (FLAGS >= 0) {
        (void)::fcntl(fd, F_SETFL, FLAGS | O_NONBLOCK);
    }
}

auto renderbench_program_path(const char* argv0) -> std::string {
    if (argv0 != nullptr && std::strchr(argv0, '/') != nullptr) {
        return argv0;
    }
    return "/usr/bin/renderbench";
}

auto make_worker_args(const std::string& program_path, const tracebench::Options& options, const IpcWorkerSpec& spec)
    -> std::vector<std::string> {
    return {
        program_path,
        "--tracebench-worker",
        "--worker-id",
        std::to_string(spec.worker_id),
        "--worker-count",
        std::to_string(spec.worker_count),
        "--worker-threads",
        std::to_string(spec.worker_threads),
        "--scene",
        options.scene_path,
        "--backend",
        tracebench::backend_name(options.backend),
        "--placement",
        tracebench::placement_name(options.placement),
        "--width",
        std::to_string(options.width),
        "--height",
        std::to_string(options.height),
        "--spp",
        std::to_string(options.spp),
        "--max-depth",
        std::to_string(options.max_depth),
        "--tile-size",
        std::to_string(options.tile_size),
        "--output-root",
        options.output_root,
        "--run-id",
        options.run_id,
    };
}

[[noreturn]] void exec_worker_child(const std::string& program_path, const tracebench::Options& options, const IpcWorkerSpec& spec,
                                    int write_fd) {
    if (::dup2(write_fd, WORKER_STDOUT_FD) < 0) {
        std::perror("renderbench: dup2 worker pipe");
        ::_exit(126);
    }
    if (write_fd != WORKER_STDOUT_FD) {
        ::close(write_fd);
    }

    int64_t const TARGET_RC = ker::process::setwkitarget(spec.hostname.c_str(), spec.hostname.size(), ker::process::WKI_TARGET_FLAG_STRICT);
    if (TARGET_RC < 0) {
        std::println(stderr, "renderbench: failed to target worker {} to {}: {}", spec.worker_id, spec.hostname,
                     static_cast<long>(TARGET_RC));
        ::_exit(126);
    }

    auto args = make_worker_args(program_path, options, spec);
    std::vector<char*> argv;
    argv.reserve(args.size() + 1U);
    for (auto& arg : args) {
        argv.push_back(arg.data());
    }
    argv.push_back(nullptr);

    ::execv(args.front().c_str(), argv.data());
    if (args.front() != "/usr/bin/renderbench") {
        args.front() = "/usr/bin/renderbench";
        argv.front() = args.front().data();
        ::execv(args.front().c_str(), argv.data());
    }
    if (args.front() != "/bin/renderbench") {
        args.front() = "/bin/renderbench";
        argv.front() = args.front().data();
        ::execv(args.front().c_str(), argv.data());
    }
    std::perror("renderbench: exec worker");
    ::_exit(127);
}

auto launch_worker(const std::string& program_path, const tracebench::Options& options, const IpcWorkerSpec& spec, ChildWorker& out)
    -> bool {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (::pipe(pipe_fds.data()) != 0) {
        std::perror("renderbench: pipe");
        return false;
    }

    pid_t const PID = ::fork();
    if (PID < 0) {
        std::perror("renderbench: fork worker");
        ::close(pipe_fds[0]);
        ::close(pipe_fds[1]);
        return false;
    }

    if (PID == 0) {
        ::close(pipe_fds[0]);
        exec_worker_child(program_path, options, spec, pipe_fds[1]);
    }

    ::close(pipe_fds[1]);
    set_nonblocking(pipe_fds[0]);
    out = {
        .pid = PID,
        .read_fd = pipe_fds[0],
        .hostname = spec.hostname,
        .buffer = {},
        .pipe_open = true,
    };
    return true;
}

void signal_workers(std::span<ChildWorker> workers, int signum) {
    for (auto& worker : workers) {
        if (worker.pid > 0) {
            (void)::kill(worker.pid, signum);
        }
    }
}

void close_worker_pipes(std::span<ChildWorker> workers) {
    for (auto& worker : workers) {
        if (worker.read_fd >= 0) {
            ::close(worker.read_fd);
            worker.read_fd = -1;
        }
        worker.pipe_open = false;
    }
}

auto make_node_thread_specs(const tracebench::Options& options, const std::vector<WkiPeerInfo>& peers) -> std::vector<IpcWorkerSpec> {
    std::vector<IpcWorkerSpec> specs;
    specs.reserve(peers.size());
    int const WORKER_COUNT = static_cast<int>(peers.size());
    for (int id = 0; id < WORKER_COUNT; ++id) {
        const auto& peer = peers.at(static_cast<size_t>(id));
        specs.push_back({
            .worker_id = id,
            .worker_count = WORKER_COUNT,
            .worker_threads = std::max(1, options.threads > 0 ? options.threads : peer.cpus),
            .hostname = peer.hostname,
        });
    }
    return specs;
}

auto make_process_specs(const tracebench::Options& options, const std::vector<WkiPeerInfo>& peers) -> std::vector<IpcWorkerSpec> {
    int max_cpus = 1;
    for (const auto& peer : peers) {
        max_cpus = std::max(max_cpus, peer.cpus);
    }

    std::vector<std::string> worker_hosts;
    for (int cpu = 0; cpu < max_cpus; ++cpu) {
        for (const auto& peer : peers) {
            if (cpu < peer.cpus) {
                worker_hosts.push_back(peer.hostname);
            }
        }
    }
    if (options.threads > 0 && options.threads < static_cast<int>(worker_hosts.size())) {
        worker_hosts.resize(static_cast<size_t>(options.threads));
    }

    std::vector<IpcWorkerSpec> specs;
    specs.reserve(worker_hosts.size());
    int const WORKER_COUNT = static_cast<int>(worker_hosts.size());
    for (int id = 0; id < WORKER_COUNT; ++id) {
        specs.push_back({
            .worker_id = id,
            .worker_count = WORKER_COUNT,
            .worker_threads = 1,
            .hostname = worker_hosts.at(static_cast<size_t>(id)),
        });
    }
    return specs;
}

auto run_node_threads(const tracebench::Options& options) -> int {
    auto scene = tracebench::load_scene(options.scene_path);
    if (!scene) {
        return 2;
    }
    auto tiles = tracebench::make_tiles(options.width, options.height, options.tile_size);
    auto storage = tracebench::make_film_storage(options.width, options.height);
    tracebench::FilmView film{.width = options.width, .height = options.height, .rgb = std::span<float>(storage.data(), storage.size())};

    int const THREADS = std::max(1, options.threads > 0 ? options.threads : local_cpu_count());
    std::vector<thrd_t> workers(static_cast<size_t>(THREADS));
    std::vector<ThreadState> states(static_cast<size_t>(THREADS));
    std::atomic<int> next_tile{0};
    std::atomic<uint64_t> tiles_done{0};
    double const STARTED = tracebench::monotonic_seconds();
    double next_update = STARTED;

    (void)tracebench::write_status(options, make_progress(options, 0, tiles.size(), STARTED, false));
    for (int i = 0; i < THREADS; ++i) {
        states[static_cast<size_t>(i)] = {
            .scene = scene.get(),
            .film = film,
            .options = &options,
            .tiles = &tiles,
            .next_tile = &next_tile,
            .tiles_done = &tiles_done,
            .seed = 0xC0FFEEULL + (static_cast<uint64_t>(i) * 0x100000001b3ULL),
        };
        if (thrd_create(&workers[static_cast<size_t>(i)], thread_worker, &states[static_cast<size_t>(i)]) != THRD_SUCCESS) {
            std::println(stderr, "renderbench: failed to start worker thread {}", i);
            return 2;
        }
    }

    bool running = true;
    while (running) {
        running = tiles_done.load() < tiles.size();
        double const NOW = tracebench::monotonic_seconds();
        if (NOW >= next_update || !running) {
            auto progress = make_progress(options, tiles_done.load(), tiles.size(), STARTED, !running);
            (void)tracebench::write_status(options, progress);
            (void)tracebench::write_preview_png(options, film);
            next_update = NOW + 0.75;
        }
        if (running) {
            timespec delay{.tv_sec = 0, .tv_nsec = 100000000L};
            (void)thrd_sleep(&delay, nullptr);
        }
    }

    for (auto& worker : workers) {
        thrd_join(worker, nullptr);
    }

    auto progress = make_progress(options, tiles.size(), tiles.size(), STARTED, true);
    double const RAYS_PER_SECOND =
        progress.elapsed_seconds > 0.0 ? static_cast<double>(progress.total_samples) / progress.elapsed_seconds : 0.0;
    (void)tracebench::write_status(options, progress);
    (void)tracebench::write_metrics(options, progress, RAYS_PER_SECOND);
    (void)tracebench::write_final_png(options, film);
    (void)tracebench::write_preview_png(options, film);
    return 0;
}

auto run_ipc_worker(const tracebench::Options& options, const WorkerInvocation& worker) -> int {
    auto scene = tracebench::load_scene(options.scene_path);
    if (!scene) {
        return 2;
    }
    auto all_tiles = tracebench::make_tiles(options.width, options.height, options.tile_size);
    std::vector<tracebench::Tile> assigned_tiles;
    for (size_t index = static_cast<size_t>(worker.worker_id); index < all_tiles.size();
         index += static_cast<size_t>(worker.worker_count)) {
        assigned_tiles.push_back(all_tiles[index]);
    }

    auto storage = tracebench::make_film_storage(options.width, options.height);
    tracebench::FilmView film{.width = options.width, .height = options.height, .rgb = std::span<float>(storage.data(), storage.size())};
    int const THREADS = std::max(1, worker.worker_threads);
    std::vector<thrd_t> threads(static_cast<size_t>(THREADS));
    std::vector<WorkerThreadState> states(static_cast<size_t>(THREADS));
    std::atomic<int> next_tile{0};
    std::atomic<bool> failed{false};
    mtx_t output_lock{};
    if (mtx_init(&output_lock, MTX_PLAIN) != THRD_SUCCESS) {
        std::println(stderr, "renderbench: failed to initialize worker output lock");
        return 2;
    }

    int created_threads = 0;
    for (int i = 0; i < THREADS; ++i) {
        states[static_cast<size_t>(i)] = {
            .scene = scene.get(),
            .film = film,
            .options = &options,
            .tiles = &assigned_tiles,
            .next_tile = &next_tile,
            .failed = &failed,
            .output_lock = &output_lock,
        };
        if (thrd_create(&threads[static_cast<size_t>(i)], worker_thread, &states[static_cast<size_t>(i)]) != THRD_SUCCESS) {
            std::println(stderr, "renderbench: failed to start IPC worker thread {}", i);
            failed.store(true, std::memory_order_relaxed);
            break;
        }
        ++created_threads;
    }

    for (int i = 0; i < created_threads; ++i) {
        thrd_join(threads[static_cast<size_t>(i)], nullptr);
    }
    mtx_destroy(&output_lock);
    return failed.load(std::memory_order_relaxed) ? 2 : 0;
}

auto wait_for_children(std::span<ChildWorker> workers, bool cancellation_expected) -> bool {
    bool ok = true;
    for (auto& worker : workers) {
        int status = 0;
        while (::waitpid(worker.pid, &status, 0) < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::perror("renderbench: waitpid");
            ok = false;
            break;
        }
        if (cancellation_expected) {
            continue;
        }
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            std::println(stderr, "renderbench: worker on {} exited with status {}", worker.hostname, status);
            ok = false;
        }
    }
    return ok;
}

auto run_distributed_ipc(const tracebench::Options& options, std::vector<WkiPeerInfo> peers, const char* argv0) -> int {
    auto specs = options.placement == tracebench::Placement::NodeThreads ? make_node_thread_specs(options, peers)
                                                                         : make_process_specs(options, peers);
    if (specs.empty()) {
        return run_node_threads(options);
    }

    auto tiles = tracebench::make_tiles(options.width, options.height, options.tile_size);
    auto storage = tracebench::make_film_storage(options.width, options.height);
    tracebench::FilmView film{.width = options.width, .height = options.height, .rgb = std::span<float>(storage.data(), storage.size())};
    std::vector<ChildWorker> workers(specs.size());
    std::string const PROGRAM_PATH = renderbench_program_path(argv0);

    double const STARTED = tracebench::monotonic_seconds();
    double next_update = STARTED;
    uint64_t tiles_done = 0;
    (void)tracebench::write_status(options, make_progress(options, 0, tiles.size(), STARTED, false));

    for (size_t i = 0; i < specs.size(); ++i) {
        if (!launch_worker(PROGRAM_PATH, options, specs[i], workers[i])) {
            signal_workers(std::span<ChildWorker>(workers.data(), i), SIGTERM);
            (void)wait_for_children(std::span<ChildWorker>(workers.data(), i), true);
            return 2;
        }
        if (cancel_requested()) {
            signal_workers(std::span<ChildWorker>(workers.data(), i + 1U), static_cast<int>(g_cancel_signal));
            (void)wait_for_children(std::span<ChildWorker>(workers.data(), i + 1U), true);
            return cancel_exit_code();
        }
    }

    bool ok = true;
    bool cancellation_sent = false;
    bool kill_escalated = false;
    double cancel_started = 0.0;
    size_t open_pipes = workers.size();
    while (open_pipes != 0) {
        if (cancel_requested() && !cancellation_sent) {
            cancellation_sent = true;
            cancel_started = tracebench::monotonic_seconds();
            ok = false;
            signal_workers(std::span<ChildWorker>(workers.data(), workers.size()), static_cast<int>(g_cancel_signal));
        }

        for (auto& worker : workers) {
            if (!worker.pipe_open) {
                continue;
            }
            if (!drain_worker_pipe(worker, film, tiles_done)) {
                ok = false;
                worker.pipe_open = false;
            }
            if (!worker.pipe_open) {
                --open_pipes;
                if (!worker.buffer.empty()) {
                    std::println(stderr, "renderbench: partial tile packet left by {}", worker.hostname);
                    ok = false;
                }
            }
        }

        double const NOW = tracebench::monotonic_seconds();
        if (cancellation_sent && !kill_escalated && NOW - cancel_started >= 2.0) {
            kill_escalated = true;
            signal_workers(std::span<ChildWorker>(workers.data(), workers.size()), SIGKILL);
        }
        if (NOW >= next_update || open_pipes == 0) {
            bool const DONE = open_pipes == 0 && tiles_done >= tiles.size();
            auto progress = make_progress(options, tiles_done, tiles.size(), STARTED, DONE);
            (void)tracebench::write_status(options, progress);
            (void)tracebench::write_preview_png(options, film);
            next_update = NOW + 0.75;
        }
        if (open_pipes != 0) {
            ::usleep(50000);
        }
    }

    ok = wait_for_children(std::span<ChildWorker>(workers.data(), workers.size()), cancellation_sent) && ok;
    if (tiles_done != tiles.size()) {
        std::println(stderr, "renderbench: completed {} of {} tiles", tiles_done, tiles.size());
        ok = false;
    }
    if (!ok) {
        close_worker_pipes(std::span<ChildWorker>(workers.data(), workers.size()));
        return cancellation_sent ? cancel_exit_code() : 2;
    }

    auto progress = make_progress(options, tiles.size(), tiles.size(), STARTED, true);
    double const RAYS_PER_SECOND =
        progress.elapsed_seconds > 0.0 ? static_cast<double>(progress.total_samples) / progress.elapsed_seconds : 0.0;
    (void)tracebench::write_status(options, progress);
    (void)tracebench::write_metrics(options, progress, RAYS_PER_SECOND);
    (void)tracebench::write_final_png(options, film);
    (void)tracebench::write_preview_png(options, film);
    return 0;
}

auto parse_worker_invocation(int argc, char** argv) -> WorkerInvocation {
    WorkerInvocation worker;
    for (int i = 1; i < argc; ++i) {
        std::string_view const ARG = argv[i];
        auto positive_value = [&](int& out) {
            if (i + 1 < argc) {
                out = std::max(1, std::atoi(argv[++i]));
            }
        };
        auto nonnegative_value = [&](int& out) {
            if (i + 1 < argc) {
                out = std::max(0, std::atoi(argv[++i]));
            }
        };
        if (ARG == "--tracebench-worker") {
            worker.enabled = true;
        } else if (ARG == "--worker-id") {
            nonnegative_value(worker.worker_id);
        } else if (ARG == "--worker-count") {
            positive_value(worker.worker_count);
        } else if (ARG == "--worker-threads") {
            positive_value(worker.worker_threads);
        }
    }
    if (worker.worker_id >= worker.worker_count) {
        worker.worker_id = worker.worker_count - 1;
    }
    return worker;
}

}  // namespace

auto main(int argc, char** argv) -> int {
    auto worker = parse_worker_invocation(argc, argv);
    auto options = tracebench::parse_options(argc, argv, tracebench::Backend::Ipc);
    if (options.backend != tracebench::Backend::Ipc) {
        std::fprintf(stderr, "renderbench: WOS module supports --backend ipc\n");
        return 2;
    }
    if (worker.enabled) {
        return run_ipc_worker(options, worker);
    }
    install_cancel_signal_handlers();
    if (!tracebench::ensure_output_tree(options)) {
        std::fprintf(stderr, "renderbench: unable to create output tree under %s\n", options.output_root.c_str());
        return 2;
    }

    auto peers = read_wki_peers();
    if (peers.size() <= 1 && options.placement == tracebench::Placement::NodeThreads) {
        return run_node_threads(options);
    }
    return run_distributed_ipc(options, std::move(peers), argv[0]);
}
