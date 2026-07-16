#ifndef TRACEBENCH_ENABLE_MPI
#define TRACEBENCH_ENABLE_MPI 0
#endif

#if TRACEBENCH_ENABLE_MPI
#include <mpi.h>
#else
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/multiproc.h>
#include <sys/process.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

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
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#if TRACEBENCH_ENABLE_MPI
#include <thread>
#else
#include <mandelbench/tinycthread.hpp>
#include <print>
#endif
#include <utility>
#include <vector>

#include "render_core.hpp"
#include "render_protocol.hpp"

namespace {

#if !TRACEBENCH_ENABLE_MPI
constexpr std::array<char, 4> TILE_PACKET_MAGIC = {'R', 'B', 'T', 'L'};
constexpr std::array<char, 4> DONE_PACKET_MAGIC = {'R', 'B', 'T', 'D'};
constexpr std::array<char, 4> BATCH_DONE_PACKET_MAGIC = {'R', 'B', 'T', 'B'};
constexpr std::array<char, 4> BATCH_COMMAND_MAGIC = {'R', 'B', 'T', 'C'};
constexpr std::array<char, 4> PHASE_PACKET_MAGIC = {'R', 'B', 'T', 'P'};
constexpr size_t WORKER_PIPE_BUFFER_RESERVE = static_cast<size_t>(256U) * 1024U;
constexpr size_t WORKER_PIPE_DRAIN_BYTE_BUDGET = static_cast<size_t>(8U) * 1024U * 1024U;
// Match the local pipe read ceiling so each user read stays on the VFS stack bounce.
constexpr size_t WORKER_PIPE_READ_CHUNK = 4096;
constexpr int WORKER_PIPE_IDLE_POLL_TIMEOUT_MS = 5;
constexpr double COORDINATOR_STALL_REPORT_SECONDS = 15.0;
constexpr double STATUS_UPDATE_INTERVAL_SECONDS = 0.75;
constexpr double WORKER_CANCEL_KILL_AFTER_SECONDS = 2.0;
constexpr double WORKER_CANCEL_GIVE_UP_AFTER_SECONDS = 10.0;
constexpr double WORKER_EXIT_KILL_AFTER_SECONDS = 2.0;
constexpr double WORKER_EXIT_GIVE_UP_AFTER_SECONDS = 10.0;
constexpr long WORKER_WAIT_POLL_NS = 50'000'000L;
constexpr size_t LIVE_OUTPUT_QUEUE_MIN_PACKETS = 1;
constexpr size_t WORKER_TILE_WRITE_BATCH_BYTES = static_cast<size_t>(1U) * 1024U * 1024U;
constexpr int WORKER_STDOUT_FD = 1;
constexpr int WORKER_COMMAND_FD = 0;
constexpr int WORKER_CHILD_FD_CLOSE_LIMIT = 256;
constexpr int IPC_SAFE_DEFAULT_TILE_SIZE = 24;
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
    int first_slot = 0;
    int slot_count = 1;
    int total_slots = 1;
    int batch_start = 0;
    int batch_count = 0;
    bool command_stream = false;
};

struct IpcWorkerSpec {
    int worker_id = 0;
    int worker_count = 1;
    int worker_threads = 1;
    int first_slot = 0;
    int slot_count = 1;
    int total_slots = 1;
    int batch_start = 0;
    int batch_count = 0;
    std::string hostname;
    bool local = false;
    bool command_stream = false;
};

enum class WorkerPhase : uint8_t {
    STARTED = 1,
    SCENE_LOADED = 2,
    SCENE_LOAD_FAILED = 3,
    BATCH_BEGIN = 4,
    BATCH_RENDERED = 5,
    BATCH_WRITING = 6,
    DONE_SENDING = 7,
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

struct WorkerDonePacket {
    std::array<char, 4> magic = DONE_PACKET_MAGIC;
    uint32_t worker_id = 0;
    uint32_t expected_tiles = 0;
    uint32_t rendered_tiles = 0;
    uint32_t sent_tiles = 0;
    uint32_t failed = 0;
    uint32_t elapsed_ms = 0;
    uint32_t render_ms = 0;
    uint32_t send_ms = 0;
    uint32_t render_cpu_ms = 0;
    uint32_t start_cpu = UINT32_MAX;
    uint32_t end_cpu = UINT32_MAX;
};

struct WorkerBatchDonePacket {
    std::array<char, 4> magic = BATCH_DONE_PACKET_MAGIC;
    uint32_t worker_id = 0;
    uint32_t batch_start = 0;
    uint32_t batch_count = 0;
    uint32_t rendered_tiles = 0;
    uint32_t sent_tiles = 0;
    uint32_t failed = 0;
    uint32_t elapsed_ms = 0;
    uint32_t render_ms = 0;
    uint32_t send_ms = 0;
    uint32_t render_cpu_ms = 0;
};

struct WorkerPhasePacket {
    std::array<char, 4> magic = PHASE_PACKET_MAGIC;
    uint32_t worker_id = 0;
    uint32_t phase = 0;
    uint32_t batch_start = 0;
    uint32_t batch_count = 0;
    uint32_t detail0 = 0;
    uint32_t detail1 = 0;
};

struct WorkerBatchCommand {
    std::array<char, 4> magic = BATCH_COMMAND_MAGIC;
    uint32_t batch_start = 0;
    uint32_t batch_count = 0;
    uint32_t stop = 0;
    uint32_t reserved = 0;
};

struct LiveTileOutputQueue {
    std::vector<std::vector<unsigned char> > packets;
    std::vector<size_t> packet_sizes;
    size_t head = 0;
    size_t tail = 0;
    size_t count = 0;
    bool closed = false;
    bool failed = false;
    uint64_t sent = 0;
    double send_seconds = 0.0;
    int output_fd = WORKER_STDOUT_FD;
    bool lock_ready = false;
    bool not_empty_ready = false;
    bool not_full_ready = false;
    mtx_t lock = {};
    cnd_t not_empty = {};
    cnd_t not_full = {};
};

static_assert(offsetof(WorkerDonePacket, magic) == 0);
static_assert(sizeof(WorkerDonePacket) >= sizeof(TilePacketHeader));
static_assert(offsetof(WorkerBatchDonePacket, magic) == 0);
static_assert(sizeof(WorkerBatchDonePacket) >= sizeof(TilePacketHeader));
static_assert(offsetof(WorkerPhasePacket, magic) == 0);
static_assert(sizeof(WorkerPhasePacket) == sizeof(TilePacketHeader));
static_assert(offsetof(WorkerBatchCommand, magic) == 0);
static_assert(sizeof(TilePacketHeader) % alignof(float) == 0);

struct ChildWorker {
    pid_t pid = -1;
    int read_fd = -1;
    int write_fd = -1;
    int worker_id = 0;
    int worker_count = 1;
    int worker_threads = 1;
    int batch_start = 0;
    int batch_count = 0;
    uint64_t expected_tiles = 0;
    uint64_t received_packets = 0;
    uint64_t unique_tiles = 0;
    uint64_t duplicate_tiles = 0;
    uint64_t foreign_tiles = 0;
    uint64_t last_tile_index = UINT64_MAX;
    bool done_seen = false;
    uint64_t done_expected_tiles = 0;
    uint64_t done_rendered_tiles = 0;
    uint64_t done_sent_tiles = 0;
    bool done_failed = false;
    uint64_t done_elapsed_ms = 0;
    uint64_t done_render_ms = 0;
    uint64_t done_send_ms = 0;
    uint64_t done_render_cpu_ms = 0;
    uint64_t done_start_cpu = UINT32_MAX;
    uint64_t done_end_cpu = UINT32_MAX;
    uint64_t read_bytes = 0;
    uint64_t read_calls = 0;
    uint64_t assigned_batches = 0;
    uint64_t fine_grained_tail_batches = 0;
    uint64_t fine_grained_tail_tiles = 0;
    uint64_t batch_done_packets = 0;
    uint64_t phase_packets = 0;
    uint32_t last_phase = 0;
    uint32_t last_phase_batch_start = 0;
    uint32_t last_phase_batch_count = 0;
    uint32_t last_phase_detail0 = 0;
    uint32_t last_phase_detail1 = 0;
    uint64_t last_batch_rendered_tiles = 0;
    uint64_t last_batch_sent_tiles = 0;
    bool last_batch_failed = false;
    uint64_t last_batch_elapsed_ms = 0;
    double launched_at = 0.0;
    double first_packet_at = 0.0;
    double last_read_at = 0.0;
    double last_assignment_at = 0.0;
    double last_batch_done_at = 0.0;
    double last_phase_at = 0.0;
    double closed_at = 0.0;
    std::string hostname;
    bool local = false;
    std::vector<unsigned char> buffer;
    bool pipe_open = true;
    bool command_stream = false;
    bool ready_for_batch = false;
    bool stop_sent = false;
    bool drain_budget_hit = false;
};

struct HostIpcProfile {
    std::string hostname;
    size_t configured_slots = 0;
    size_t configured_threads = 0;
    size_t completed_runs = 0;
    uint64_t worker_elapsed_total_ms = 0;
    uint64_t worker_render_total_ms = 0;
    uint64_t worker_render_cpu_total_ms = 0;
    uint64_t worker_send_total_ms = 0;
    uint64_t worker_thread_capacity_total_ms = 0;
    uint32_t min_worker_ms = UINT32_MAX;
    uint32_t max_worker_ms = 0;
    uint32_t max_render_ms = 0;
    uint32_t max_render_cpu_ms = 0;
    uint32_t max_send_ms = 0;
    uint64_t worker_cpu_mask = 0;
    uint64_t worker_cpu_migrations = 0;
    uint64_t effective_threads = 0;
};

struct ProcessIpcProfile {
    size_t worker_slots = 0;
    size_t completed_runs = 0;
    uint64_t total_read_bytes = 0;
    uint64_t total_read_calls = 0;
    uint64_t worker_elapsed_total_ms = 0;
    uint32_t min_worker_ms = UINT32_MAX;
    uint32_t max_worker_ms = 0;
    double first_packet_seconds = 0.0;
    double last_close_seconds = 0.0;
    int slowest_worker_id = -1;
    std::string slowest_hostname;
    uint64_t slowest_expected_tiles = 0;
    uint64_t slowest_unique_tiles = 0;
    uint64_t slowest_received_packets = 0;
    uint64_t slowest_read_bytes = 0;
    uint64_t slowest_read_calls = 0;
    uint64_t slowest_render_ms = 0;
    uint64_t slowest_send_ms = 0;
    uint64_t slowest_render_cpu_ms = 0;
    uint64_t slowest_start_cpu = UINT32_MAX;
    uint64_t slowest_end_cpu = UINT32_MAX;
    int slowest_batch_start = 0;
    int slowest_batch_count = 0;
    double slowest_first_packet_seconds = 0.0;
    double slowest_closed_seconds = 0.0;
    int last_worker_id = -1;
    std::string last_hostname;
    uint64_t last_expected_tiles = 0;
    uint64_t last_elapsed_ms = 0;
    uint64_t last_render_ms = 0;
    uint64_t last_render_cpu_ms = 0;
    uint64_t last_send_ms = 0;
    uint64_t last_start_cpu = UINT32_MAX;
    uint64_t last_end_cpu = UINT32_MAX;
    int last_batch_start = 0;
    int last_batch_count = 0;
    double last_closed_seconds = 0.0;
    int persistent_batch_size = 0;
    bool fine_grained_process_tail_enabled = false;
    uint64_t fine_grained_tail_batches = 0;
    uint64_t fine_grained_tail_tiles = 0;
    int effective_reserve_cpus = 0;
    std::vector<HostIpcProfile> hosts;
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

struct WorkerBatchRenderThreadState {
    const tracebench::Scene* scene = nullptr;
    const tracebench::Options* options = nullptr;
    const std::vector<tracebench::Tile>* tiles = nullptr;
    std::atomic<int>* next_tile = nullptr;
    std::atomic<bool>* failed = nullptr;
    std::atomic<uint64_t>* tiles_rendered = nullptr;
    std::vector<std::vector<unsigned char> >* packets = nullptr;
    std::span<unsigned char> packet_buffer;
    bool stream_packets = false;
    LiveTileOutputQueue* output_queue = nullptr;
    double render_seconds = 0.0;
    double render_cpu_seconds = 0.0;
};

#endif

#if !TRACEBENCH_ENABLE_MPI
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

void apply_distributed_ipc_defaults(tracebench::Options& options) {
    if (options.tile_size <= IPC_SAFE_DEFAULT_TILE_SIZE) {
        return;
    }

    if (options.tile_size_explicit) {
        std::println(stderr, "renderbench: distributed ipc tile-size {} exceeds chunk-safe max {}; using {}", options.tile_size,
                     IPC_SAFE_DEFAULT_TILE_SIZE, IPC_SAFE_DEFAULT_TILE_SIZE);
    }
    options.tile_size = IPC_SAFE_DEFAULT_TILE_SIZE;
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
            if (local == 0 && (connected == 0 || cpus <= 0)) {
                continue;
            }
            peers_out.push_back({
                .hostname = hostname,
                .node_id = node,
                .cpus = local != 0 ? std::max(1, cpus) : cpus,
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

    std::ranges::stable_sort(peers_out, [](const WkiPeerInfo& left, const WkiPeerInfo& right) {
        if (left.local != right.local) {
            return !left.local;
        }
        return left.node_id < right.node_id;
    });
    return peers_out;
}

auto local_cpu_count() -> int {
    auto peers = read_wki_peers();
    auto local = std::ranges::find_if(peers, [](const WkiPeerInfo& peer) { return peer.local; });
    if (local != peers.end()) {
        return local->cpus;
    }
    long const ONLINE = ::sysconf(_SC_NPROCESSORS_ONLN);
    return ONLINE > 0 ? static_cast<int>(ONLINE) : 1;
}

#endif

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

#if TRACEBENCH_ENABLE_MPI
auto mpi_worker_count(const tracebench::Options& options) -> int {
    if (options.threads > 0) {
        return options.threads;
    }
    unsigned int const THREADS = std::thread::hardware_concurrency();
    return THREADS == 0 ? 1 : static_cast<int>(THREADS);
}

void print_mpi_metrics_json(const tracebench::Options& options, const tracebench::Progress& progress, double rays_per_second,
                            int world_size, int threads_per_rank) {
    std::printf("{");
    std::printf("\"benchmark\":\"mpi_renderbench\",");
    std::printf("\"run_id\":\"%s\",", options.run_id.c_str());
    std::printf("\"backend\":\"%s\",", tracebench::backend_name(options.backend));
    std::printf("\"placement\":\"%s\",", tracebench::placement_name(options.placement));
    std::printf("\"debug_edge_colors\":%s,", options.debug_edge_colors ? "true" : "false");
    std::printf("\"debug_render_mode\":\"%s\",", tracebench::debug_render_mode_name(options));
    std::printf("\"debug_constant_tile_us\":%d,", options.debug_constant_tile_us);
    std::printf("\"debug_node_thread_batch_size\":%d,", options.debug_node_thread_batch_size);
    std::printf("\"coordinator_reserve_cpus\":%d,", options.coordinator_reserve_cpus);
    std::printf("\"node_worker_reserve_cpus\":%d,", options.node_worker_reserve_cpus);
    std::printf("\"coordinator_skip_local_worker\":%s,", options.coordinator_skip_local_worker ? "true" : "false");
    std::printf("\"live_preview\":%s,", options.live_preview ? "true" : "false");
    std::printf("\"preview_update_interval_seconds\":%.3f,", options.preview_update_interval_seconds);
    std::printf("\"world_size\":%d,", world_size);
    std::printf("\"threads_per_rank\":%d,", threads_per_rank);
    std::printf("\"width\":%d,", options.width);
    std::printf("\"height\":%d,", options.height);
    std::printf("\"spp\":%d,", options.spp);
    std::printf("\"max_depth\":%d,", options.max_depth);
    std::printf("\"tile_size\":%d,", options.tile_size);
    std::printf("\"total_tiles\":%llu,", static_cast<unsigned long long>(progress.total_tiles));
    std::printf("\"primary_samples\":%llu,", static_cast<unsigned long long>(progress.total_samples));
    std::printf("\"elapsed_seconds\":%.9f,", progress.elapsed_seconds);
    std::printf("\"rays_per_second_estimate\":%.3f", rays_per_second);
    std::printf("}\n");
    std::fflush(stdout);
}

auto run_mpi_renderbench(int argc, char** argv) -> int {
    MPI_Init(&argc, &argv);

    int rank = 0;
    int world_size = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    tracebench::Options options;
    auto const PARSE_STATUS = tracebench::parse_options(argc, argv, tracebench::Backend::Mpi, options);
    if (PARSE_STATUS == tracebench::ParseStatus::Help) {
        MPI_Finalize();
        return 0;
    }
    if (PARSE_STATUS == tracebench::ParseStatus::Error) {
        if (rank == 0) {
            tracebench::print_usage(argv[0]);
        }
        MPI_Finalize();
        return 2;
    }
    if (options.backend != tracebench::Backend::Mpi) {
        if (rank == 0) {
            std::fprintf(stderr, "renderbench: Linux MPI target supports --backend mpi\n");
        }
        MPI_Finalize();
        return 2;
    }

    auto scene = tracebench::load_scene(options.scene_path);
    int const LOCAL_SCENE_OK = scene ? 1 : 0;
    int all_scenes_ok = 0;
    MPI_Allreduce(&LOCAL_SCENE_OK, &all_scenes_ok, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    if (all_scenes_ok == 0) {
        MPI_Finalize();
        return 2;
    }

    if (rank == 0 && !tracebench::ensure_output_tree(options)) {
        std::fprintf(stderr, "renderbench: unable to create output tree under %s\n", options.output_root.c_str());
        MPI_Abort(MPI_COMM_WORLD, 2);
    }
    MPI_Barrier(MPI_COMM_WORLD);

    auto tiles = tracebench::make_tiles(options.width, options.height, options.tile_size);
    auto local_storage = tracebench::make_film_storage(options.width, options.height);
    tracebench::FilmView local_film{
        .width = options.width,
        .height = options.height,
        .rgb = std::span<float>(local_storage.data(), local_storage.size()),
    };

    double const STARTED = tracebench::monotonic_seconds();
    if (rank == 0) {
        (void)tracebench::write_status(options, make_progress(options, 0, tiles.size(), STARTED, false));
    }

    int const THREADS = options.placement == tracebench::Placement::NodeThreads ? mpi_worker_count(options) : 1;
    std::atomic<int> next_tile{rank};
    std::atomic<uint64_t> local_tiles_done{0};
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(THREADS));
    for (int thread_id = 0; thread_id < THREADS; ++thread_id) {
        workers.emplace_back([&, thread_id]() {
            for (;;) {
                int const INDEX = next_tile.fetch_add(world_size);
                if (INDEX >= static_cast<int>(tiles.size())) {
                    break;
                }
                tracebench::render_tile(*scene, local_film, options, tiles[static_cast<size_t>(INDEX)],
                                        0x1234ABCDEFULL + static_cast<uint64_t>((rank * 4099) + thread_id) + static_cast<uint64_t>(INDEX));
                local_tiles_done.fetch_add(1);
            }
        });
    }

    for (auto& worker : workers) {
        worker.join();
    }

    std::vector<float> final_storage;
    if (rank == 0) {
        final_storage = tracebench::make_film_storage(options.width, options.height);
    }

    MPI_Reduce(local_storage.data(), rank == 0 ? final_storage.data() : nullptr, static_cast<int>(local_storage.size()), MPI_FLOAT, MPI_SUM,
               0, MPI_COMM_WORLD);

    if (rank == 0) {
        tracebench::FilmView final_film{
            .width = options.width,
            .height = options.height,
            .rgb = std::span<float>(final_storage.data(), final_storage.size()),
        };
        auto progress = make_progress(options, tiles.size(), tiles.size(), STARTED, true);
        double const RAYS_PER_SECOND =
            progress.elapsed_seconds > 0.0 ? static_cast<double>(progress.total_samples) / progress.elapsed_seconds : 0.0;
        (void)tracebench::write_status(options, progress);
        (void)tracebench::write_metrics(options, progress, RAYS_PER_SECOND);
        (void)tracebench::write_final_png(options, final_film);
        (void)tracebench::write_preview_png(options, final_film);
        print_mpi_metrics_json(options, progress, RAYS_PER_SECOND, world_size, THREADS);
    }

    MPI_Finalize();
    return 0;
}

#else
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

auto make_tile_packet_header(const tracebench::Tile& tile, size_t payload_floats) -> TilePacketHeader {
    return {
        .magic = TILE_PACKET_MAGIC,
        .tile_index = static_cast<uint32_t>(tile.index),
        .x0 = static_cast<uint32_t>(tile.x0),
        .y0 = static_cast<uint32_t>(tile.y0),
        .x1 = static_cast<uint32_t>(tile.x1),
        .y1 = static_cast<uint32_t>(tile.y1),
        .float_count = static_cast<uint32_t>(payload_floats),
    };
}

auto write_all(int fd, std::span<const unsigned char> bytes) -> bool {
    while (!bytes.empty()) {
        if (cancel_requested()) {
            return false;
        }
        ssize_t const WRITTEN = ::write(fd, bytes.data(), bytes.size());
        if (WRITTEN < 0) {
            if (errno == EINTR) {
                if (cancel_requested()) {
                    return false;
                }
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

auto write_buffered_tile_packets(int fd, const std::vector<std::vector<unsigned char> >& packets, std::vector<unsigned char>& output_batch,
                                 uint64_t& sent_tiles, double& send_seconds) -> bool {
    output_batch.clear();
    size_t reserve_bytes = 0;
    for (const auto& packet : packets) {
        if (packet.size() >= WORKER_TILE_WRITE_BATCH_BYTES - reserve_bytes) {
            reserve_bytes = WORKER_TILE_WRITE_BATCH_BYTES;
            break;
        }
        reserve_bytes += packet.size();
    }
    try {
        output_batch.reserve(reserve_bytes);
    } catch (...) {
        return false;
    }

    size_t buffered_packets = 0;
    auto flush_output_batch = [&]() -> bool {
        if (output_batch.empty()) {
            return true;
        }
        double const SEND_STARTED = tracebench::monotonic_seconds();
        bool const SENT = write_all(fd, std::span<const unsigned char>(output_batch.data(), output_batch.size()));
        send_seconds += tracebench::monotonic_seconds() - SEND_STARTED;
        if (!SENT) {
            // A partial failed write may contain whole packets, but the caller
            // fails the worker. Keep the unconfirmed batch count conservative.
            return false;
        }
        sent_tiles += buffered_packets;
        buffered_packets = 0;
        output_batch.clear();
        return true;
    };

    for (const auto& packet : packets) {
        if (cancel_requested()) {
            return false;
        }
        if (packet.empty()) {
            (void)flush_output_batch();
            return false;
        }
        if (packet.size() > WORKER_TILE_WRITE_BATCH_BYTES) {
            if (!flush_output_batch()) {
                return false;
            }
            double const SEND_STARTED = tracebench::monotonic_seconds();
            bool const SENT = write_all(fd, std::span<const unsigned char>(packet.data(), packet.size()));
            send_seconds += tracebench::monotonic_seconds() - SEND_STARTED;
            if (!SENT) {
                return false;
            }
            ++sent_tiles;
            continue;
        }
        if (!output_batch.empty() && packet.size() > WORKER_TILE_WRITE_BATCH_BYTES - output_batch.size() && !flush_output_batch()) {
            return false;
        }
        try {
            output_batch.insert(output_batch.end(), packet.begin(), packet.end());
        } catch (...) {
            return false;
        }
        ++buffered_packets;
    }
    return flush_output_batch();
}

auto read_exact(int fd, std::span<unsigned char> bytes) -> bool {
    while (!bytes.empty()) {
        if (cancel_requested()) {
            return false;
        }
        ssize_t const READ = ::read(fd, bytes.data(), bytes.size());
        if (READ < 0) {
            if (errno == EINTR) {
                if (cancel_requested()) {
                    return false;
                }
                continue;
            }
            return false;
        }
        if (READ == 0) {
            return false;
        }
        bytes = bytes.subspan(static_cast<size_t>(READ));
    }
    return true;
}

auto live_output_queue_capacity(size_t expected_tiles) -> size_t { return std::max(LIVE_OUTPUT_QUEUE_MIN_PACKETS, expected_tiles); }

void destroy_live_output_queue(LiveTileOutputQueue& queue) {
    if (queue.not_full_ready) {
        cnd_destroy(&queue.not_full);
        queue.not_full_ready = false;
    }
    if (queue.not_empty_ready) {
        cnd_destroy(&queue.not_empty);
        queue.not_empty_ready = false;
    }
    if (queue.lock_ready) {
        mtx_destroy(&queue.lock);
        queue.lock_ready = false;
    }
    queue.packet_sizes.clear();
    queue.packets.clear();
}

auto init_live_output_queue(LiveTileOutputQueue& queue, size_t expected_tiles, int output_fd) -> bool {
    destroy_live_output_queue(queue);
    size_t const CAPACITY = live_output_queue_capacity(expected_tiles);
    try {
        queue.packets.assign(CAPACITY, std::vector<unsigned char>());
        queue.packet_sizes.assign(CAPACITY, 0);
    } catch (...) {
        return false;
    }
    queue.head = 0;
    queue.tail = 0;
    queue.count = 0;
    queue.closed = false;
    queue.failed = false;
    queue.sent = 0;
    queue.send_seconds = 0.0;
    queue.output_fd = output_fd;
    if (mtx_init(&queue.lock, MTX_PLAIN) != THRD_SUCCESS) {
        destroy_live_output_queue(queue);
        return false;
    }
    queue.lock_ready = true;
    if (cnd_init(&queue.not_empty) != THRD_SUCCESS) {
        destroy_live_output_queue(queue);
        return false;
    }
    queue.not_empty_ready = true;
    if (cnd_init(&queue.not_full) != THRD_SUCCESS) {
        destroy_live_output_queue(queue);
        return false;
    }
    queue.not_full_ready = true;
    return true;
}

void close_live_output_queue(LiveTileOutputQueue& queue) {
    if (!queue.lock_ready || mtx_lock(&queue.lock) != THRD_SUCCESS) {
        return;
    }
    queue.closed = true;
    if (queue.not_empty_ready) {
        (void)cnd_broadcast(&queue.not_empty);
    }
    if (queue.not_full_ready) {
        (void)cnd_broadcast(&queue.not_full);
    }
    (void)mtx_unlock(&queue.lock);
}

void fail_live_output_queue_locked(LiveTileOutputQueue& queue) {
    queue.failed = true;
    queue.closed = true;
    (void)cnd_broadcast(&queue.not_empty);
    (void)cnd_broadcast(&queue.not_full);
}

auto push_live_output_packet(LiveTileOutputQueue& queue, std::span<const unsigned char> bytes) -> bool {
    if (!queue.lock_ready || queue.packets.empty() || bytes.empty()) {
        return false;
    }
    if (mtx_lock(&queue.lock) != THRD_SUCCESS) {
        return false;
    }
    while (queue.count == queue.packets.size() && !queue.closed && !queue.failed && !cancel_requested()) {
        if (cnd_wait(&queue.not_full, &queue.lock) != THRD_SUCCESS) {
            fail_live_output_queue_locked(queue);
            (void)mtx_unlock(&queue.lock);
            return false;
        }
    }
    if (queue.closed || queue.failed || cancel_requested()) {
        if (cancel_requested()) {
            fail_live_output_queue_locked(queue);
        }
        (void)mtx_unlock(&queue.lock);
        return false;
    }

    size_t const INDEX = queue.tail;
    try {
        auto& packet = queue.packets[INDEX];
        packet.assign(bytes.begin(), bytes.end());
        queue.packet_sizes[INDEX] = bytes.size();
    } catch (...) {
        fail_live_output_queue_locked(queue);
        (void)mtx_unlock(&queue.lock);
        return false;
    }
    queue.tail = (queue.tail + 1U) % queue.packets.size();
    ++queue.count;
    (void)cnd_signal(&queue.not_empty);
    (void)mtx_unlock(&queue.lock);
    return true;
}

auto live_output_writer_thread(void* raw) -> int {
    auto* queue = static_cast<LiveTileOutputQueue*>(raw);
    std::vector<unsigned char> packet;
    for (;;) {
        if (mtx_lock(&queue->lock) != THRD_SUCCESS) {
            return 1;
        }
        while (queue->count == 0 && !queue->closed && !queue->failed && !cancel_requested()) {
            if (cnd_wait(&queue->not_empty, &queue->lock) != THRD_SUCCESS) {
                fail_live_output_queue_locked(*queue);
                (void)mtx_unlock(&queue->lock);
                return 1;
            }
        }
        if (queue->count == 0 && (queue->closed || queue->failed || cancel_requested())) {
            if (cancel_requested()) {
                fail_live_output_queue_locked(*queue);
            }
            bool const FAILED = queue->failed;
            (void)mtx_unlock(&queue->lock);
            return FAILED ? 1 : 0;
        }

        size_t const INDEX = queue->head;
        size_t const PACKET_SIZE = queue->packet_sizes[INDEX];
        try {
            auto& slot = queue->packets[INDEX];
            packet.assign(slot.begin(), slot.begin() + static_cast<ptrdiff_t>(PACKET_SIZE));
            slot.clear();
            queue->packet_sizes[INDEX] = 0;
        } catch (...) {
            fail_live_output_queue_locked(*queue);
            (void)mtx_unlock(&queue->lock);
            return 1;
        }
        queue->head = (queue->head + 1U) % queue->packets.size();
        --queue->count;
        (void)cnd_signal(&queue->not_full);
        (void)mtx_unlock(&queue->lock);

        double const SEND_STARTED = tracebench::monotonic_seconds();
        bool const SENT = write_all(queue->output_fd, std::span<const unsigned char>(packet.data(), packet.size()));
        double const SEND_SECONDS = tracebench::monotonic_seconds() - SEND_STARTED;
        if (mtx_lock(&queue->lock) != THRD_SUCCESS) {
            return 1;
        }
        queue->send_seconds += SEND_SECONDS;
        if (!SENT) {
            fail_live_output_queue_locked(*queue);
            (void)mtx_unlock(&queue->lock);
            return 1;
        }
        ++queue->sent;
        (void)mtx_unlock(&queue->lock);
    }
}

auto start_live_output_queue(LiveTileOutputQueue& queue, thrd_t& thread, size_t expected_tiles, int output_fd) -> bool {
    if (!init_live_output_queue(queue, expected_tiles, output_fd)) {
        return false;
    }
    if (thrd_create(&thread, live_output_writer_thread, &queue) != THRD_SUCCESS) {
        close_live_output_queue(queue);
        destroy_live_output_queue(queue);
        return false;
    }
    return true;
}

auto finish_live_output_queue(LiveTileOutputQueue& queue, thrd_t thread) -> bool {
    close_live_output_queue(queue);
    int result = 1;
    thrd_join(thread, &result);
    return result == 0 && !queue.failed;
}

auto seconds_to_milliseconds(double seconds) -> uint32_t {
    if (seconds <= 0.0) {
        return 0;
    }
    double const MS = seconds * 1000.0;
    return static_cast<uint32_t>(std::min<double>(MS, static_cast<double>(UINT32_MAX)));
}

auto elapsed_milliseconds_since(double started) -> uint32_t { return seconds_to_milliseconds(tracebench::monotonic_seconds() - started); }

auto current_thread_cpu_seconds() -> double {
    timespec ts{};
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) != 0) {
        return 0.0;
    }
    return static_cast<double>(ts.tv_sec) + (static_cast<double>(ts.tv_nsec) / 1'000'000'000.0);
}

auto current_worker_cpu() -> uint32_t {
    uint64_t const CPU = ker::multiproc::getcurrent_cpu();
    return CPU <= UINT32_MAX ? static_cast<uint32_t>(CPU) : UINT32_MAX;
}

auto send_worker_phase_packet(int fd, int worker_id, WorkerPhase phase, uint64_t batch_start, uint64_t batch_count, uint64_t detail0 = 0,
                              uint64_t detail1 = 0) -> bool {
    WorkerPhasePacket const PACKET = {
        .magic = PHASE_PACKET_MAGIC,
        .worker_id = static_cast<uint32_t>(std::max(0, worker_id)),
        .phase = static_cast<uint32_t>(phase),
        .batch_start = static_cast<uint32_t>(std::min<uint64_t>(batch_start, UINT32_MAX)),
        .batch_count = static_cast<uint32_t>(std::min<uint64_t>(batch_count, UINT32_MAX)),
        .detail0 = static_cast<uint32_t>(std::min<uint64_t>(detail0, UINT32_MAX)),
        .detail1 = static_cast<uint32_t>(std::min<uint64_t>(detail1, UINT32_MAX)),
    };
    auto const* bytes = reinterpret_cast<const unsigned char*>(&PACKET);
    return write_all(fd, std::span<const unsigned char>(bytes, sizeof(PACKET)));
}

auto send_worker_done_packet(int fd, int worker_id, uint64_t expected_tiles, uint64_t rendered_tiles, uint64_t sent_tiles, bool failed,
                             uint32_t elapsed_ms, uint32_t render_ms, uint32_t send_ms, uint32_t render_cpu_ms, uint32_t start_cpu,
                             uint32_t end_cpu) -> bool {
    WorkerDonePacket const PACKET = {
        .magic = DONE_PACKET_MAGIC,
        .worker_id = static_cast<uint32_t>(std::max(0, worker_id)),
        .expected_tiles = static_cast<uint32_t>(std::min<uint64_t>(expected_tiles, UINT32_MAX)),
        .rendered_tiles = static_cast<uint32_t>(std::min<uint64_t>(rendered_tiles, UINT32_MAX)),
        .sent_tiles = static_cast<uint32_t>(std::min<uint64_t>(sent_tiles, UINT32_MAX)),
        .failed = failed ? 1U : 0U,
        .elapsed_ms = elapsed_ms,
        .render_ms = render_ms,
        .send_ms = send_ms,
        .render_cpu_ms = render_cpu_ms,
        .start_cpu = start_cpu,
        .end_cpu = end_cpu,
    };
    auto const* bytes = reinterpret_cast<const unsigned char*>(&PACKET);
    return write_all(fd, std::span<const unsigned char>(bytes, sizeof(PACKET)));
}

auto send_worker_batch_done_packet(int fd, int worker_id, uint64_t batch_start, uint64_t batch_count, uint64_t rendered_tiles,
                                   uint64_t sent_tiles, bool failed, uint32_t elapsed_ms, uint32_t render_ms, uint32_t send_ms,
                                   uint32_t render_cpu_ms) -> bool {
    WorkerBatchDonePacket const PACKET = {
        .magic = BATCH_DONE_PACKET_MAGIC,
        .worker_id = static_cast<uint32_t>(std::max(0, worker_id)),
        .batch_start = static_cast<uint32_t>(std::min<uint64_t>(batch_start, UINT32_MAX)),
        .batch_count = static_cast<uint32_t>(std::min<uint64_t>(batch_count, UINT32_MAX)),
        .rendered_tiles = static_cast<uint32_t>(std::min<uint64_t>(rendered_tiles, UINT32_MAX)),
        .sent_tiles = static_cast<uint32_t>(std::min<uint64_t>(sent_tiles, UINT32_MAX)),
        .failed = failed ? 1U : 0U,
        .elapsed_ms = elapsed_ms,
        .render_ms = render_ms,
        .send_ms = send_ms,
        .render_cpu_ms = render_cpu_ms,
    };
    auto const* bytes = reinterpret_cast<const unsigned char*>(&PACKET);
    return write_all(fd, std::span<const unsigned char>(bytes, sizeof(PACKET)));
}

auto send_worker_batch_command(int fd, uint64_t batch_start, uint64_t batch_count, bool stop) -> bool {
    WorkerBatchCommand const COMMAND = {
        .magic = BATCH_COMMAND_MAGIC,
        .batch_start = static_cast<uint32_t>(std::min<uint64_t>(batch_start, UINT32_MAX)),
        .batch_count = static_cast<uint32_t>(std::min<uint64_t>(batch_count, UINT32_MAX)),
        .stop = stop ? 1U : 0U,
        .reserved = 0,
    };
    auto const* bytes = reinterpret_cast<const unsigned char*>(&COMMAND);
    return write_all(fd, std::span<const unsigned char>(bytes, sizeof(COMMAND)));
}

auto read_worker_batch_command(int fd, WorkerBatchCommand& command) -> bool {
    auto* bytes = reinterpret_cast<unsigned char*>(&command);
    if (!read_exact(fd, std::span<unsigned char>(bytes, sizeof(command)))) {
        return false;
    }
    return command.magic == BATCH_COMMAND_MAGIC;
}

auto render_worker_tile_packet(WorkerBatchRenderThreadState& state, int tile_offset, const tracebench::Tile& tile) -> bool {
    if (cancel_requested()) {
        state.failed->store(true, std::memory_order_relaxed);
        return false;
    }
    size_t const PAYLOAD_FLOATS = tile_float_count(tile);
    size_t const PAYLOAD_BYTES = PAYLOAD_FLOATS * sizeof(float);
    size_t const PACKET_BYTES = sizeof(TilePacketHeader) + PAYLOAD_BYTES;
    if (state.packet_buffer.size() < PACKET_BYTES || tile_offset < 0 ||
        (!state.stream_packets && std::cmp_greater_equal(tile_offset, state.packets->size()))) {
        state.failed->store(true, std::memory_order_relaxed);
        return false;
    }
    auto* payload_bytes = state.packet_buffer.data() + sizeof(TilePacketHeader);
    auto* payload = reinterpret_cast<float*>(payload_bytes);
    std::span<float> tile_payload(payload, PAYLOAD_FLOATS);
    double const RENDER_STARTED = tracebench::monotonic_seconds();
    double const RENDER_CPU_STARTED = current_thread_cpu_seconds();
    bool const RENDERED =
        tracebench::render_tile_payload(*state.scene, tile_payload, *state.options, tile, 0xBADC0DEULL + static_cast<uint64_t>(tile.index));
    double const RENDER_CPU_FINISHED = current_thread_cpu_seconds();
    state.render_seconds += tracebench::monotonic_seconds() - RENDER_STARTED;
    if (RENDER_CPU_FINISHED >= RENDER_CPU_STARTED) {
        state.render_cpu_seconds += RENDER_CPU_FINISHED - RENDER_CPU_STARTED;
    }
    if (!RENDERED || cancel_requested()) {
        state.failed->store(true, std::memory_order_relaxed);
        return false;
    }

    TilePacketHeader const HEADER = make_tile_packet_header(tile, tile_payload.size());
    std::memcpy(state.packet_buffer.data(), &HEADER, sizeof(HEADER));
    state.tiles_rendered->fetch_add(1, std::memory_order_relaxed);
    if (state.stream_packets) {
        if (state.output_queue == nullptr) {
            state.failed->store(true, std::memory_order_relaxed);
            return false;
        }
        if (!push_live_output_packet(*state.output_queue, std::span<const unsigned char>(state.packet_buffer.data(), PACKET_BYTES))) {
            state.failed->store(true, std::memory_order_relaxed);
            return false;
        }
        return true;
    }

    auto& packet = state.packets->at(static_cast<size_t>(tile_offset));
    packet.assign(state.packet_buffer.begin(), state.packet_buffer.begin() + static_cast<ptrdiff_t>(PACKET_BYTES));
    return true;
}

auto batch_render_thread(void* raw) -> int {
    auto* state = static_cast<WorkerBatchRenderThreadState*>(raw);
    for (;;) {
        if (cancel_requested()) {
            state->failed->store(true, std::memory_order_relaxed);
            break;
        }
        if (state->failed->load(std::memory_order_relaxed)) {
            break;
        }
        int const INDEX = state->next_tile->fetch_add(1);
        if (INDEX >= static_cast<int>(state->tiles->size())) {
            break;
        }
        const auto& tile = state->tiles->at(static_cast<size_t>(INDEX));
        if (!render_worker_tile_packet(*state, INDEX, tile)) {
            break;
        }
    }
    return 0;
}

auto apply_tile_payload(tracebench::FilmView film, const TilePacketHeader& header, std::span<const unsigned char> payload) -> bool {
    if (header.magic != TILE_PACKET_MAGIC) {
        return false;
    }
    if (header.x1 > static_cast<uint32_t>(film.width) || header.y1 > static_cast<uint32_t>(film.height) || header.x0 >= header.x1 ||
        header.y0 >= header.y1) {
        return false;
    }
    if (!tracebench::film_storage_is_complete(film)) {
        return false;
    }
    tracebench::Tile const TILE{
        .x0 = static_cast<int>(header.x0),
        .y0 = static_cast<int>(header.y0),
        .x1 = static_cast<int>(header.x1),
        .y1 = static_cast<int>(header.y1),
        .index = static_cast<int>(header.tile_index),
    };
    if (payload.size() != tile_float_count(TILE) * sizeof(float)) {
        return false;
    }

    size_t in = 0;
    size_t const ROW_FLOATS = static_cast<size_t>(TILE.x1 - TILE.x0) * 3U;
    size_t const ROW_BYTES = ROW_FLOATS * sizeof(float);
    for (int y = TILE.y0; y < TILE.y1; ++y) {
        size_t const TARGET = ((static_cast<size_t>(y) * static_cast<size_t>(film.width)) + static_cast<size_t>(TILE.x0)) * 3U;
        std::memcpy(film.rgb.data() + TARGET, payload.data() + in, ROW_BYTES);
        in += ROW_BYTES;
    }
    return true;
}

enum class WorkerPacketKind : uint8_t {
    NONE,
    TILE,
    DONE,
    BATCH_DONE,
    PHASE,
};

auto packet_magic_at(const std::vector<unsigned char>& bytes, size_t offset, std::span<const char, 4> magic) -> bool {
    if (offset + magic.size() > bytes.size()) {
        return false;
    }
    for (size_t i = 0; i < magic.size(); ++i) {
        if (bytes.at(offset + i) != static_cast<unsigned char>(magic[i])) {
            return false;
        }
    }
    return true;
}

auto packet_kind_at(const std::vector<unsigned char>& bytes, size_t offset) -> WorkerPacketKind {
    if (packet_magic_at(bytes, offset, TILE_PACKET_MAGIC)) {
        return WorkerPacketKind::TILE;
    }
    if (packet_magic_at(bytes, offset, DONE_PACKET_MAGIC)) {
        return WorkerPacketKind::DONE;
    }
    if (packet_magic_at(bytes, offset, BATCH_DONE_PACKET_MAGIC)) {
        return WorkerPacketKind::BATCH_DONE;
    }
    if (packet_magic_at(bytes, offset, PHASE_PACKET_MAGIC)) {
        return WorkerPacketKind::PHASE;
    }
    return WorkerPacketKind::NONE;
}

auto find_packet_magic(const std::vector<unsigned char>& bytes, size_t offset) -> size_t {
    for (size_t i = offset; i + TILE_PACKET_MAGIC.size() <= bytes.size(); ++i) {
        if (packet_kind_at(bytes, i) != WorkerPacketKind::NONE) {
            return i;
        }
    }
    return std::string::npos;
}

auto byte_preview(std::span<const unsigned char> bytes, size_t limit) -> std::string {
    constexpr std::array<char, 16> HEX = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    size_t const COUNT = std::min(bytes.size(), limit);
    std::string out;
    out.reserve((COUNT * 4U) + 8U);
    for (size_t i = 0; i < COUNT; ++i) {
        if (i != 0) {
            out.push_back(' ');
        }
        unsigned char const BYTE = bytes[i];
        out.push_back(HEX[BYTE >> 4U]);
        out.push_back(HEX[BYTE & 0x0FU]);
    }
    if (bytes.size() > COUNT) {
        out += " ...";
    }
    out += " |";
    for (size_t i = 0; i < COUNT; ++i) {
        unsigned char const BYTE = bytes[i];
        out.push_back(BYTE >= 32U && BYTE < 127U ? static_cast<char>(BYTE) : '.');
    }
    if (bytes.size() > COUNT) {
        out += "...";
    }
    out.push_back('|');
    return out;
}

auto validate_tile_header(tracebench::FilmView film, const TilePacketHeader& header, size_t& payload_floats) -> bool {
    if (header.magic != TILE_PACKET_MAGIC) {
        return false;
    }
    if (header.x1 > static_cast<uint32_t>(film.width) || header.y1 > static_cast<uint32_t>(film.height) || header.x0 >= header.x1 ||
        header.y0 >= header.y1) {
        return false;
    }
    if (!tracebench::film_storage_is_complete(film)) {
        return false;
    }

    tracebench::Tile const TILE{
        .x0 = static_cast<int>(header.x0),
        .y0 = static_cast<int>(header.y0),
        .x1 = static_cast<int>(header.x1),
        .y1 = static_cast<int>(header.y1),
        .index = static_cast<int>(header.tile_index),
    };
    payload_floats = tile_float_count(TILE);
    return header.float_count == payload_floats;
}

auto worker_phase_name(uint32_t phase) -> std::string_view {
    switch (phase) {
        case static_cast<uint32_t>(WorkerPhase::STARTED):
            return "started";
        case static_cast<uint32_t>(WorkerPhase::SCENE_LOADED):
            return "scene-loaded";
        case static_cast<uint32_t>(WorkerPhase::SCENE_LOAD_FAILED):
            return "scene-load-failed";
        case static_cast<uint32_t>(WorkerPhase::BATCH_BEGIN):
            return "batch-begin";
        case static_cast<uint32_t>(WorkerPhase::BATCH_RENDERED):
            return "batch-rendered";
        case static_cast<uint32_t>(WorkerPhase::BATCH_WRITING):
            return "batch-writing";
        case static_cast<uint32_t>(WorkerPhase::DONE_SENDING):
            return "done-sending";
        default:
            return "unknown";
    }
}

void note_worker_phase_packet(ChildWorker& worker, const WorkerPhasePacket& packet) {
    ++worker.phase_packets;
    worker.last_phase = packet.phase;
    worker.last_phase_batch_start = packet.batch_start;
    worker.last_phase_batch_count = packet.batch_count;
    worker.last_phase_detail0 = packet.detail0;
    worker.last_phase_detail1 = packet.detail1;
    worker.last_phase_at = tracebench::monotonic_seconds();
    if (packet.worker_id != static_cast<uint32_t>(worker.worker_id)) {
        std::println(stderr, "renderbench: phase packet from {} reported worker {} expected {}", worker.hostname, packet.worker_id,
                     worker.worker_id);
    }
}

void note_worker_done_packet(ChildWorker& worker, const WorkerDonePacket& packet) {
    if (worker.done_seen) {
        std::println(stderr, "renderbench: duplicate done packet from {}", worker.hostname);
    }
    worker.done_seen = true;
    worker.done_expected_tiles = packet.expected_tiles;
    worker.done_rendered_tiles = packet.rendered_tiles;
    worker.done_sent_tiles = packet.sent_tiles;
    worker.done_failed = packet.failed != 0;
    worker.done_elapsed_ms = packet.elapsed_ms;
    worker.done_render_ms = packet.render_ms;
    worker.done_send_ms = packet.send_ms;
    worker.done_render_cpu_ms = packet.render_cpu_ms;
    worker.done_start_cpu = packet.start_cpu;
    worker.done_end_cpu = packet.end_cpu;
    if (packet.worker_id != static_cast<uint32_t>(worker.worker_id)) {
        std::println(stderr, "renderbench: done packet from {} reported worker {} expected {}", worker.hostname, packet.worker_id,
                     worker.worker_id);
    }
    if (worker.done_failed || worker.done_expected_tiles != worker.expected_tiles ||
        worker.done_rendered_tiles != worker.done_expected_tiles || worker.done_sent_tiles != worker.done_expected_tiles) {
        std::println(stderr, "renderbench: worker {} done mismatch host={} expected={} reported_expected={} rendered={} sent={} failed={}",
                     worker.worker_id, worker.hostname, worker.expected_tiles, worker.done_expected_tiles, worker.done_rendered_tiles,
                     worker.done_sent_tiles, worker.done_failed ? 1 : 0);
    }
    if (worker.done_sent_tiles != worker.unique_tiles) {
        std::println(stderr, "renderbench: worker {} done/accepted mismatch host={} accepted_unique={} packets={} done_sent={} buffered={}",
                     worker.worker_id, worker.hostname, worker.unique_tiles, worker.received_packets, worker.done_sent_tiles,
                     worker.buffer.size());
    }
}

auto note_worker_batch_done_packet(ChildWorker& worker, const WorkerBatchDonePacket& packet) -> bool {
    ++worker.batch_done_packets;
    worker.last_batch_done_at = tracebench::monotonic_seconds();
    worker.last_batch_rendered_tiles = packet.rendered_tiles;
    worker.last_batch_sent_tiles = packet.sent_tiles;
    worker.last_batch_failed = packet.failed != 0;
    worker.last_batch_elapsed_ms = packet.elapsed_ms;
    bool batch_ok = worker.command_stream && !worker.ready_for_batch;
    if (packet.worker_id != static_cast<uint32_t>(worker.worker_id)) {
        std::println(stderr, "renderbench: batch-done packet from {} reported worker {} expected {}", worker.hostname, packet.worker_id,
                     worker.worker_id);
        batch_ok = false;
    }
    if (worker.batch_start < 0 || worker.batch_count <= 0 || packet.batch_start != static_cast<uint32_t>(worker.batch_start) ||
        packet.batch_count != static_cast<uint32_t>(worker.batch_count)) {
        std::println(stderr, "renderbench: worker {} batch-done range mismatch host={} expected={}+{} reported={}+{}", worker.worker_id,
                     worker.hostname, worker.batch_start, worker.batch_count, packet.batch_start, packet.batch_count);
        batch_ok = false;
    }
    if (packet.failed != 0 || packet.rendered_tiles != packet.batch_count || packet.sent_tiles != packet.batch_count) {
        std::println(stderr, "renderbench: worker {} batch mismatch host={} batch={}+{} rendered={} sent={} failed={}", worker.worker_id,
                     worker.hostname, packet.batch_start, packet.batch_count, packet.rendered_tiles, packet.sent_tiles, packet.failed);
        batch_ok = false;
    }
    worker.done_failed = worker.done_failed || !batch_ok;
    worker.ready_for_batch = batch_ok && !worker.done_failed;
    return worker.ready_for_batch;
}

auto note_worker_tile_packet(ChildWorker& worker, const TilePacketHeader& header, std::vector<unsigned char>& tile_seen,
                             std::span<const int> tile_owner, uint64_t& tiles_done) -> tracebench::WorkerTileDecision {
    ++worker.received_packets;
    worker.last_tile_index = header.tile_index;

    tracebench::WorkerTileDecision const DECISION = tracebench::decide_worker_tile(
        header.tile_index, worker.worker_id, std::span<unsigned char>(tile_seen.data(), tile_seen.size()), tile_owner);
    if (DECISION == tracebench::WorkerTileDecision::OutOfRange) {
        std::println(stderr, "renderbench: tile index {} from {} outside total {}", header.tile_index, worker.hostname, tile_seen.size());
        return DECISION;
    }

    int const OWNER = tile_owner[static_cast<size_t>(header.tile_index)];
    if (DECISION == tracebench::WorkerTileDecision::Foreign) {
        ++worker.foreign_tiles;
        std::println(stderr, "renderbench: tile {} arrived from {} worker {} but belongs to worker {}", header.tile_index, worker.hostname,
                     worker.worker_id, OWNER);
        return DECISION;
    }

    if (DECISION == tracebench::WorkerTileDecision::Duplicate) {
        ++worker.duplicate_tiles;
        return DECISION;
    }

    ++worker.unique_tiles;
    ++tiles_done;
    return DECISION;
}

auto parse_worker_packets(ChildWorker& worker, tracebench::FilmView film, std::vector<unsigned char>& tile_seen,
                          std::span<const int> tile_owner, uint64_t& tiles_done) -> bool {
    size_t consumed = 0;
    while (worker.buffer.size() - consumed >= sizeof(TilePacketHeader)) {
        WorkerPacketKind const KIND = packet_kind_at(worker.buffer, consumed);
        if (KIND == WorkerPacketKind::NONE) {
            size_t const NEXT = find_packet_magic(worker.buffer, consumed + 1U);
            if (NEXT == std::string::npos) {
                std::span<const unsigned char> const STRAY(worker.buffer.data() + consumed, worker.buffer.size() - consumed);
                std::println(stderr, "renderbench: corrupt worker stream from {}: no packet magic in {} buffered byte(s): {}",
                             worker.hostname, worker.buffer.size() - consumed, byte_preview(STRAY, 64));
                worker.done_failed = true;
                return false;
            }
            std::span<const unsigned char> const SKIPPED(worker.buffer.data() + consumed, NEXT - consumed);
            std::println(stderr, "renderbench: corrupt worker stream from {}: skipped {} stray byte(s) before next packet magic: {}",
                         worker.hostname, NEXT - consumed, byte_preview(SKIPPED, 64));
            worker.done_failed = true;
            return false;
        }

        if (KIND == WorkerPacketKind::DONE) {
            if (worker.buffer.size() - consumed < sizeof(WorkerDonePacket)) {
                break;
            }
            WorkerDonePacket packet = {};
            std::memcpy(&packet, worker.buffer.data() + consumed, sizeof(packet));
            note_worker_done_packet(worker, packet);
            consumed += sizeof(packet);
            continue;
        }

        if (KIND == WorkerPacketKind::BATCH_DONE) {
            if (worker.buffer.size() - consumed < sizeof(WorkerBatchDonePacket)) {
                break;
            }
            WorkerBatchDonePacket packet = {};
            std::memcpy(&packet, worker.buffer.data() + consumed, sizeof(packet));
            if (!note_worker_batch_done_packet(worker, packet)) {
                return false;
            }
            consumed += sizeof(packet);
            continue;
        }

        if (KIND == WorkerPacketKind::PHASE) {
            if (worker.buffer.size() - consumed < sizeof(WorkerPhasePacket)) {
                break;
            }
            WorkerPhasePacket packet = {};
            std::memcpy(&packet, worker.buffer.data() + consumed, sizeof(packet));
            note_worker_phase_packet(worker, packet);
            consumed += sizeof(packet);
            continue;
        }

        TilePacketHeader header = {};
        std::memcpy(&header, worker.buffer.data() + consumed, sizeof(header));

        size_t payload_floats = 0;
        if (!validate_tile_header(film, header, payload_floats)) {
            size_t const NEXT = find_packet_magic(worker.buffer, consumed + 1U);
            if (NEXT == std::string::npos) {
                std::println(stderr, "renderbench: invalid tile header from {} tile={} rect={}x{}..{}x{} floats={} buffered={}",
                             worker.hostname, header.tile_index, header.x0, header.y0, header.x1, header.y1, header.float_count,
                             worker.buffer.size() - consumed);
                return false;
            }
            std::span<const unsigned char> const SKIPPED(worker.buffer.data() + consumed, NEXT - consumed);
            std::println(stderr, "renderbench: corrupt worker stream from {}: invalid tile header, skipped {} byte(s): {}", worker.hostname,
                         NEXT - consumed, byte_preview(SKIPPED, 64));
            worker.done_failed = true;
            return false;
        }

        size_t const PAYLOAD_BYTES = payload_floats * sizeof(float);
        size_t const PACKET_BYTES = sizeof(TilePacketHeader) + PAYLOAD_BYTES;
        if (worker.buffer.size() - consumed < PACKET_BYTES) {
            break;
        }

        if (header.tile_index >= tile_seen.size()) {
            std::println(stderr, "renderbench: invalid tile index from {} tile={} total={}", worker.hostname, header.tile_index,
                         tile_seen.size());
            return false;
        }

        tracebench::WorkerTileDecision const DECISION = note_worker_tile_packet(worker, header, tile_seen, tile_owner, tiles_done);
        if (DECISION == tracebench::WorkerTileDecision::OutOfRange) {
            return false;
        }
        if (DECISION == tracebench::WorkerTileDecision::Accepted) {
            auto const* payload = worker.buffer.data() + consumed + sizeof(TilePacketHeader);
            if (!apply_tile_payload(film, header, std::span<const unsigned char>(payload, PAYLOAD_BYTES))) {
                std::println(stderr, "renderbench: invalid tile packet from {}", worker.hostname);
                return false;
            }
        }
        consumed += PACKET_BYTES;
    }

    if (consumed != 0) {
        worker.buffer.erase(worker.buffer.begin(), worker.buffer.begin() + static_cast<ptrdiff_t>(consumed));
    }
    return true;
}

auto drain_ready_worker_pipe(ChildWorker& worker, tracebench::FilmView film, std::vector<unsigned char>& tile_seen,
                             std::span<const int> tile_owner, uint64_t& tiles_done) -> bool {
    // read() initializes the exact positive prefix consumed below.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
    std::array<unsigned char, WORKER_PIPE_READ_CHUNK> chunk __attribute__((uninitialized));
    size_t drained_bytes = 0;
    worker.drain_budget_hit = false;
    while (worker.read_fd >= 0 && drained_bytes < WORKER_PIPE_DRAIN_BYTE_BUDGET) {
        ssize_t const READ = ::read(worker.read_fd, chunk.data(), chunk.size());
        if (READ > 0) {
            double const NOW = tracebench::monotonic_seconds();
            if (worker.first_packet_at == 0.0) {
                worker.first_packet_at = NOW;
            }
            worker.last_read_at = NOW;
            worker.read_calls++;
            worker.read_bytes += static_cast<uint64_t>(READ);
            drained_bytes += static_cast<size_t>(READ);
            worker.buffer.insert(worker.buffer.end(), chunk.begin(), chunk.begin() + static_cast<ptrdiff_t>(READ));
            continue;
        }
        if (READ == 0) {
            worker.closed_at = tracebench::monotonic_seconds();
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
    worker.drain_budget_hit = worker.read_fd >= 0 && drained_bytes >= WORKER_PIPE_DRAIN_BYTE_BUDGET;
    return parse_worker_packets(worker, film, tile_seen, tile_owner, tiles_done);
}

void print_partial_worker_packet(const ChildWorker& worker) {
    std::println(stderr, "renderbench: partial worker packet left by {} worker={} received={}/{} last_tile={} ({} byte(s) buffered)",
                 worker.hostname, worker.worker_id, worker.unique_tiles, worker.expected_tiles,
                 worker.last_tile_index == UINT64_MAX ? 0 : worker.last_tile_index, worker.buffer.size());
    if (worker.buffer.size() < sizeof(TilePacketHeader)) {
        return;
    }

    TilePacketHeader header = {};
    std::memcpy(&header, worker.buffer.data(), sizeof(header));
    std::println(stderr, "renderbench: partial header tile={} rect={}x{}..{}x{} floats={} magic={},{},{},{}", header.tile_index, header.x0,
                 header.y0, header.x1, header.y1, header.float_count, static_cast<unsigned>(worker.buffer.at(0)),
                 static_cast<unsigned>(worker.buffer.at(1)), static_cast<unsigned>(worker.buffer.at(2)),
                 static_cast<unsigned>(worker.buffer.at(3)));
}

void set_nonblocking(int fd) {
    int const FLAGS = ::fcntl(fd, F_GETFL, 0);
    if (FLAGS >= 0) {
        (void)::fcntl(fd, F_SETFL, FLAGS | O_NONBLOCK);
    }
}

auto renderbench_program_path(const char* argv0) -> std::string {
    if (argv0 != nullptr && std::strchr(argv0, '/') != nullptr) {
        std::string_view const PATH = argv0;
        auto has_suffix = [&](std::string_view suffix) -> bool {
            return PATH.size() >= suffix.size() && PATH.substr(PATH.size() - suffix.size()) == suffix;
        };
        if (PATH.starts_with("/wki/") && has_suffix("/usr/bin/renderbench")) {
            return "/usr/bin/renderbench";
        }
        if (PATH.starts_with("/wki/") && has_suffix("/bin/renderbench")) {
            return "/bin/renderbench";
        }
        return std::string(PATH);
    }
    return "/usr/bin/renderbench";
}

auto path_matches_prefix(std::string_view path, std::string_view prefix) -> bool {
    return path == prefix || (path.size() > prefix.size() && path.starts_with(prefix) && path.substr(prefix.size(), 1) == "/");
}

void install_worker_scene_vfs_policy(const tracebench::Options& options, const IpcWorkerSpec& spec) {
    auto add_local_rule = [&](const char* path, const char* purpose) {
        int const RC = ker::abi::vfs::wki_rule_add_vfs(path, ker::abi::vfs::WKI_VFS_ROUTE_LOCAL);
        if (RC < 0) {
            std::println(stderr, "renderbench: worker {} on {} failed to keep {} local at {} (rc={}); continuing with inherited VFS policy",
                         spec.worker_id, spec.hostname, purpose, path, RC);
        }
    };

    add_local_rule("/usr", "worker runtime");
    add_local_rule("/bin", "worker runtime");
    add_local_rule("/lib", "worker runtime");
    add_local_rule("/lib64", "worker runtime");
    add_local_rule("/usr/bin/renderbench", "worker executable");
    add_local_rule("/bin/renderbench", "worker executable fallback");

    if (!path_matches_prefix(options.scene_path, "/srv")) {
        return;
    }

    add_local_rule("/srv", "scene reads");
}

void close_worker_child_extra_fds() {
    for (int fd = 3; fd < WORKER_CHILD_FD_CLOSE_LIMIT; ++fd) {
        (void)::close(fd);
    }
}

auto make_worker_args(const std::string& program_path, const tracebench::Options& options, const IpcWorkerSpec& spec)
    -> std::vector<std::string> {
    std::vector<std::string> args{
        program_path,
        "--tracebench-worker",
        "--worker-id",
        std::to_string(spec.worker_id),
        "--worker-count",
        std::to_string(spec.worker_count),
        "--worker-threads",
        std::to_string(spec.worker_threads),
        "--worker-first-slot",
        std::to_string(spec.first_slot),
        "--worker-slots",
        std::to_string(spec.slot_count),
        "--worker-total-slots",
        std::to_string(spec.total_slots),
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
    if (options.live_preview) {
        args.emplace_back("--live");
    }
    if (spec.batch_count > 0) {
        args.emplace_back("--worker-batch-start");
        args.emplace_back(std::to_string(spec.batch_start));
        args.emplace_back("--worker-batch-count");
        args.emplace_back(std::to_string(spec.batch_count));
    }
    if (spec.command_stream) {
        args.emplace_back("--worker-command-stream");
    }
    if (options.debug_edge_colors) {
        args.emplace_back("--debug-edge-colors");
    }
    if (options.debug_constant_tile_us > 0) {
        args.emplace_back("--debug-constant-tile-us");
        args.emplace_back(std::to_string(options.debug_constant_tile_us));
    }
    if (options.disable_single_thread_worker_queue) {
        args.emplace_back("--disable-single-thread-worker-queue");
    } else {
        args.emplace_back("--enable-single-thread-worker-queue");
    }
    if (options.disable_worker_output_queue) {
        args.emplace_back("--disable-worker-output-queue");
    }
    if (options.process_persistent_workers) {
        args.emplace_back("--enable-process-persistent-workers");
    } else {
        args.emplace_back("--disable-process-persistent-workers");
    }
    return args;
}

[[noreturn]] void exec_worker_child(const std::string& program_path, const tracebench::Options& options, const IpcWorkerSpec& spec,
                                    int stdout_fd, int command_fd) {
    if (command_fd >= 0 && command_fd != WORKER_COMMAND_FD) {
        if (::dup2(command_fd, WORKER_COMMAND_FD) < 0) {
            std::perror("renderbench: dup2 worker command pipe");
            ::_exit(126);
        }
        ::close(command_fd);
    }
    if (::dup2(stdout_fd, WORKER_STDOUT_FD) < 0) {
        std::perror("renderbench: dup2 worker pipe");
        ::_exit(126);
    }
    if (stdout_fd != WORKER_STDOUT_FD) {
        ::close(stdout_fd);
    }
    close_worker_child_extra_fds();

    install_worker_scene_vfs_policy(options, spec);

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
    std::array<int, 2> output_fds = {-1, -1};
    std::array<int, 2> command_fds = {-1, -1};
    if (::pipe(output_fds.data()) != 0) {
        std::perror("renderbench: pipe");
        return false;
    }
    if (spec.command_stream && ::pipe(command_fds.data()) != 0) {
        std::perror("renderbench: command pipe");
        ::close(output_fds[0]);
        ::close(output_fds[1]);
        return false;
    }

    pid_t const PID = ::fork();
    if (PID < 0) {
        std::perror("renderbench: fork worker");
        ::close(output_fds[0]);
        ::close(output_fds[1]);
        if (command_fds[0] >= 0) {
            ::close(command_fds[0]);
        }
        if (command_fds[1] >= 0) {
            ::close(command_fds[1]);
        }
        return false;
    }

    if (PID == 0) {
        ::close(output_fds[0]);
        if (command_fds[1] >= 0) {
            ::close(command_fds[1]);
        }
        exec_worker_child(program_path, options, spec, output_fds[1], command_fds[0]);
    }

    ::close(output_fds[1]);
    if (command_fds[0] >= 0) {
        ::close(command_fds[0]);
    }
    set_nonblocking(output_fds[0]);

    auto reusable_buffer = std::move(out.buffer);
    reusable_buffer.clear();
    out = {
        .pid = PID,
        .read_fd = output_fds[0],
        .write_fd = command_fds[1],
        .worker_id = spec.worker_id,
        .worker_count = spec.worker_count,
        .worker_threads = spec.worker_threads,
        .batch_start = spec.batch_start,
        .batch_count = spec.batch_count,
        .expected_tiles = static_cast<uint64_t>(std::max(0, spec.batch_count)),
        .launched_at = tracebench::monotonic_seconds(),
        .hostname = spec.hostname,
        .local = spec.local,
        .buffer = {},
        .pipe_open = true,
        .command_stream = spec.command_stream,
        .ready_for_batch = spec.command_stream,
    };
    out.buffer = std::move(reusable_buffer);
    out.buffer.reserve(WORKER_PIPE_BUFFER_RESERVE);
    return true;
}

auto owns_tile_slot(size_t tile_position, int first_slot, int slot_count, int total_slots) -> bool {
    if (total_slots <= 0 || slot_count <= 0 || first_slot < 0 || first_slot >= total_slots) {
        return false;
    }
    int const SLOT = static_cast<int>(tile_position % static_cast<size_t>(total_slots));
    return SLOT >= first_slot && SLOT < first_slot + slot_count;
}

auto expected_tile_count_for_slots(size_t total_tiles, int first_slot, int slot_count, int total_slots) -> uint64_t {
    uint64_t count = 0;
    for (size_t position = 0; position < total_tiles; ++position) {
        if (owns_tile_slot(position, first_slot, slot_count, total_slots)) {
            ++count;
        }
    }
    return count;
}

auto tile_distribution_key(int tile_index) -> uint64_t {
    uint64_t value = static_cast<uint64_t>(std::max(0, tile_index)) + 0x9E3779B97F4A7C15ULL;
    value = (value ^ (value >> 30U)) * 0xBF58476D1CE4E5B9ULL;
    value = (value ^ (value >> 27U)) * 0x94D049BB133111EBULL;
    return value ^ (value >> 31U);
}

void scramble_process_tiles(std::vector<tracebench::Tile>& tiles) {
    std::ranges::stable_sort(tiles, [](const tracebench::Tile& left, const tracebench::Tile& right) {
        uint64_t const LEFT_KEY = tile_distribution_key(left.index);
        uint64_t const RIGHT_KEY = tile_distribution_key(right.index);
        if (LEFT_KEY != RIGHT_KEY) {
            return LEFT_KEY < RIGHT_KEY;
        }
        return left.index < right.index;
    });
}

auto make_tile_owner_map(std::span<const tracebench::Tile> tiles, std::span<const IpcWorkerSpec> specs) -> std::vector<int> {
    std::vector<int> owner(tiles.size(), -1);
    for (const auto& spec : specs) {
        for (size_t position = 0; position < tiles.size(); ++position) {
            if (!owns_tile_slot(position, spec.first_slot, spec.slot_count, spec.total_slots)) {
                continue;
            }
            int const TILE_INDEX = tiles[position].index;
            if (TILE_INDEX >= 0 && static_cast<size_t>(TILE_INDEX) < owner.size()) {
                owner[static_cast<size_t>(TILE_INDEX)] = spec.worker_id;
            }
        }
    }
    return owner;
}

void print_missing_tile_summary(std::span<const unsigned char> tile_seen) {
    constexpr size_t MAX_RANGES = 16;
    size_t printed = 0;
    size_t missing_total = 0;
    size_t index = 0;
    while (index < tile_seen.size()) {
        if (tile_seen[index] != 0) {
            ++index;
            continue;
        }

        size_t const START = index;
        while (index < tile_seen.size() && tile_seen[index] == 0) {
            ++index;
        }
        size_t const END = index - 1U;
        missing_total += END - START + 1U;
        if (printed < MAX_RANGES) {
            if (START == END) {
                std::println(stderr, "renderbench: missing tile {}", START);
            } else {
                std::println(stderr, "renderbench: missing tiles {}..{}", START, END);
            }
            ++printed;
        }
    }
    if (printed == MAX_RANGES && missing_total > printed) {
        std::println(stderr, "renderbench: {} missing tile(s) total", missing_total);
    }
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
        if (worker.write_fd >= 0) {
            ::close(worker.write_fd);
            worker.write_fd = -1;
        }
        worker.pipe_open = false;
        worker.stop_sent = true;
    }
}

void sleep_worker_wait_poll() {
    timespec delay{
        .tv_sec = 0,
        .tv_nsec = WORKER_WAIT_POLL_NS,
    };
    while (::nanosleep(&delay, &delay) != 0 && errno == EINTR) {
    }
}

auto note_worker_exit_status(ChildWorker& worker, int status, bool cancellation_expected) -> bool {
    if (!cancellation_expected && (!WIFEXITED(status) || WEXITSTATUS(status) != 0)) {
        std::println(stderr, "renderbench: worker on {} exited with status {}", worker.hostname, status);
        return false;
    }
    return true;
}

auto wait_for_child(ChildWorker& worker, bool cancellation_expected) -> bool {
    if (worker.pid <= 0) {
        return true;
    }

    bool kill_sent = false;
    double const STARTED = tracebench::monotonic_seconds();
    int status = 0;
    for (;;) {
        pid_t const WAITED = ::waitpid(worker.pid, &status, WNOHANG);
        if (WAITED == worker.pid) {
            bool const OK = note_worker_exit_status(worker, status, cancellation_expected);
            worker.pid = -1;
            return OK;
        }
        if (WAITED < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::perror("renderbench: waitpid");
            worker.pid = -1;
            return false;
        }

        double const ELAPSED = tracebench::monotonic_seconds() - STARTED;
        if (!kill_sent && ELAPSED >= WORKER_EXIT_KILL_AFTER_SECONDS) {
            kill_sent = true;
            std::println(stderr, "renderbench: worker {} on {} pid={} did not exit after {:.1f}s; escalating to SIGKILL", worker.worker_id,
                         worker.hostname, worker.pid, ELAPSED);
            (void)::kill(worker.pid, SIGKILL);
        }
        if (ELAPSED >= WORKER_EXIT_GIVE_UP_AFTER_SECONDS) {
            std::println(stderr, "renderbench: worker {} on {} pid={} did not reap after {:.1f}s", worker.worker_id, worker.hostname,
                         worker.pid, ELAPSED);
            worker.pid = -1;
            return false;
        }

        sleep_worker_wait_poll();
    }
}

auto wait_for_children_after_cancel(std::span<ChildWorker> workers, bool cancellation_expected) -> bool {
    bool ok = true;
    bool kill_sent = false;
    double const STARTED = tracebench::monotonic_seconds();

    for (;;) {
        size_t remaining = 0;
        for (auto& worker : workers) {
            if (worker.pid <= 0) {
                continue;
            }

            int status = 0;
            pid_t const WAITED = ::waitpid(worker.pid, &status, WNOHANG);
            if (WAITED == worker.pid) {
                ok = note_worker_exit_status(worker, status, cancellation_expected) && ok;
                worker.pid = -1;
                continue;
            }
            if (WAITED < 0) {
                if (errno == EINTR) {
                    ++remaining;
                    continue;
                }
                std::perror("renderbench: waitpid");
                worker.pid = -1;
                ok = false;
                continue;
            }

            ++remaining;
        }

        if (remaining == 0) {
            return ok;
        }

        double const NOW = tracebench::monotonic_seconds();
        double const ELAPSED = NOW - STARTED;
        if (!kill_sent && ELAPSED >= WORKER_CANCEL_KILL_AFTER_SECONDS) {
            kill_sent = true;
            std::println(stderr, "renderbench: {} worker(s) still exiting after {:.1f}s; escalating to SIGKILL", remaining, ELAPSED);
            signal_workers(workers, SIGKILL);
        }
        if (ELAPSED >= WORKER_CANCEL_GIVE_UP_AFTER_SECONDS) {
            for (const auto& worker : workers) {
                if (worker.pid > 0) {
                    std::println(stderr, "renderbench: worker {} on {} pid={} did not exit after cancellation", worker.worker_id,
                                 worker.hostname, worker.pid);
                }
            }
            return false;
        }

        sleep_worker_wait_poll();
    }
}

auto dynamic_batch_size(const tracebench::Options& options, size_t total_tiles, size_t worker_count, int worker_threads, bool local_worker)
    -> int {
    (void)local_worker;

    size_t const WORKERS = std::max<size_t>(1, worker_count);
    size_t const THREADS = static_cast<size_t>(std::max(1, worker_threads));

    if (options.placement == tracebench::Placement::ProcessPerCore) {
        size_t const TARGET_WAVES = total_tiles >= WORKERS * 64U ? 8U : 16U;
        size_t target = (total_tiles + ((WORKERS * TARGET_WAVES) - 1U)) / (WORKERS * TARGET_WAVES);
        target = std::clamp<size_t>(target, THREADS * 8U, THREADS * 64U);
        return static_cast<int>(std::max<size_t>(1, target));
    }

    if (options.debug_node_thread_batch_size > 0) {
        return options.debug_node_thread_batch_size;
    }

    size_t const MIN_BATCH = THREADS * 4U;
    size_t const TARGET_WAVES = total_tiles >= WORKERS * MIN_BATCH * 4U ? 64U : 8U;
    size_t target = (total_tiles + ((WORKERS * TARGET_WAVES) - 1U)) / (WORKERS * TARGET_WAVES);
    size_t const MAX_BATCH = std::max(MIN_BATCH, THREADS * 128U);
    if (total_tiles >= WORKERS * MIN_BATCH) {
        target = std::max(target, MIN_BATCH);
    }
    target = std::min(target, MAX_BATCH);
    return static_cast<int>(std::max<size_t>(1, target));
}

auto persistent_batch_worker_threads(const tracebench::Options& options, std::span<const IpcWorkerSpec> specs) -> int {
    int threads = 1;
    for (const auto& spec : specs) {
        threads = std::max(threads, spec.worker_threads);
    }
    if (options.placement == tracebench::Placement::NodeThreads && options.threads <= 0) {
        threads += std::max(0, options.node_worker_reserve_cpus);
    }
    return threads;
}

auto local_coordinator_reserve_cpus(const tracebench::Options& options, size_t peer_count) -> int {
    if (peer_count <= 1U) {
        return 0;
    }
    if (options.coordinator_reserve_cpus >= 0) {
        return options.coordinator_reserve_cpus;
    }
    if (options.placement == tracebench::Placement::ProcessPerCore) {
        return 0;
    }
    return options.threads <= 0 ? 1 : 0;
}

auto assign_worker_batch(ChildWorker& worker, std::span<const tracebench::Tile> tiles, std::vector<int>& tile_owner,
                         size_t& next_tile_position, int batch_size, size_t fine_grained_tail_threshold) -> int {
    if (!worker.command_stream || worker.write_fd < 0 || worker.stop_sent || !worker.ready_for_batch) {
        return 0;
    }

    if (next_tile_position >= tiles.size()) {
        if (!send_worker_batch_command(worker.write_fd, 0, 0, true)) {
            return -1;
        }
        worker.last_assignment_at = tracebench::monotonic_seconds();
        ::close(worker.write_fd);
        worker.write_fd = -1;
        worker.stop_sent = true;
        worker.ready_for_batch = false;
        return 0;
    }

    size_t const START = next_tile_position;
    size_t const REMAINING = tiles.size() - START;
    bool const FINE_GRAINED_TAIL = fine_grained_tail_threshold > 0U && REMAINING <= fine_grained_tail_threshold;
    size_t const COUNT =
        tracebench::persistent_batch_tile_count(REMAINING, static_cast<size_t>(std::max(1, batch_size)), fine_grained_tail_threshold);
    next_tile_position += COUNT;

    for (size_t position = START; position < START + COUNT; ++position) {
        int const TILE_INDEX = tiles[position].index;
        if (TILE_INDEX >= 0 && static_cast<size_t>(TILE_INDEX) < tile_owner.size()) {
            tile_owner[static_cast<size_t>(TILE_INDEX)] = worker.worker_id;
        }
    }

    if (!send_worker_batch_command(worker.write_fd, START, COUNT, false)) {
        return -1;
    }

    worker.batch_start = static_cast<int>(START);
    worker.batch_count = static_cast<int>(COUNT);
    worker.expected_tiles += COUNT;
    ++worker.assigned_batches;
    if (FINE_GRAINED_TAIL) {
        ++worker.fine_grained_tail_batches;
        worker.fine_grained_tail_tiles += COUNT;
    }
    worker.last_assignment_at = tracebench::monotonic_seconds();
    worker.ready_for_batch = false;
    return 1;
}

auto launch_next_worker_batch(const std::string& program_path, const tracebench::Options& options,
                              std::span<const IpcWorkerSpec> base_specs, std::span<const tracebench::Tile> tiles,
                              std::vector<int>& tile_owner, size_t& next_tile_position, size_t worker_index, ChildWorker& worker) -> int {
    if (next_tile_position >= tiles.size()) {
        return 0;
    }

    IpcWorkerSpec spec = base_specs[worker_index];
    int const BATCH_SIZE = dynamic_batch_size(options, tiles.size(), base_specs.size(), spec.worker_threads, spec.local);
    size_t const START = next_tile_position;
    size_t const COUNT = std::min(static_cast<size_t>(BATCH_SIZE), tiles.size() - START);
    spec.batch_start = static_cast<int>(START);
    spec.batch_count = static_cast<int>(COUNT);
    next_tile_position += COUNT;

    for (size_t position = START; position < START + COUNT; ++position) {
        int const TILE_INDEX = tiles[position].index;
        if (TILE_INDEX >= 0 && static_cast<size_t>(TILE_INDEX) < tile_owner.size()) {
            tile_owner[static_cast<size_t>(TILE_INDEX)] = spec.worker_id;
        }
    }

    if (!launch_worker(program_path, options, spec, worker)) {
        return -1;
    }
    worker.expected_tiles = COUNT;
    return 1;
}

auto should_poll_workers_immediately(std::span<const ChildWorker> workers) -> bool {
    return std::ranges::any_of(workers, [](const ChildWorker& worker) {
        return worker.pipe_open && worker.read_fd >= 0 && (worker.drain_budget_hit || !worker.buffer.empty());
    });
}

auto worker_event_age_seconds(double now, double event_at) -> double {
    if (event_at <= 0.0 || now < event_at) {
        return -1.0;
    }
    return now - event_at;
}

auto worker_last_tile_label(const ChildWorker& worker) -> std::string {
    if (worker.last_tile_index == UINT64_MAX) {
        return "none";
    }
    return std::to_string(worker.last_tile_index);
}

auto worker_phase_label(const ChildWorker& worker) -> std::string_view {
    if (worker.phase_packets == 0) {
        return "none";
    }
    return worker_phase_name(worker.last_phase);
}

void print_worker_stall_report(std::span<const ChildWorker> workers, uint64_t tiles_done, size_t total_tiles, size_t next_tile_position,
                               size_t open_pipes, int batch_size, double now) {
    std::println(stderr,
                 "renderbench: ipc stall: no completed tile progress for {:.1f}s tiles={}/{} next_tile={} open_pipes={} batch_size={}",
                 COORDINATOR_STALL_REPORT_SECONDS, tiles_done, total_tiles, next_tile_position, open_pipes, batch_size);
    for (const auto& worker : workers) {
        if (!worker.pipe_open) {
            continue;
        }
        std::println(stderr,
                     "renderbench: ipc stall worker={} host={} local={} pid={} fd={} ready={} stop={} command_stream={} batch={}+{} "
                     "expected={} unique={} packets={} assigned_batches={} batch_done={} last_batch_rendered={} last_batch_sent={} "
                     "last_batch_failed={} last_batch_elapsed_ms={} read_bytes={} read_calls={} buffered={} last_tile={} done={} "
                     "failed={} drain_budget={} phase_packets={} phase={} phase_batch={}+{} phase_detail={}/{} age_read_s={:.3f} "
                     "age_assign_s={:.3f} age_batch_done_s={:.3f} age_phase_s={:.3f}",
                     worker.worker_id, worker.hostname, worker.local ? 1 : 0, worker.pid, worker.read_fd, worker.ready_for_batch ? 1 : 0,
                     worker.stop_sent ? 1 : 0, worker.command_stream ? 1 : 0, worker.batch_start, worker.batch_count, worker.expected_tiles,
                     worker.unique_tiles, worker.received_packets, worker.assigned_batches, worker.batch_done_packets,
                     worker.last_batch_rendered_tiles, worker.last_batch_sent_tiles, worker.last_batch_failed ? 1 : 0,
                     worker.last_batch_elapsed_ms, worker.read_bytes, worker.read_calls, worker.buffer.size(),
                     worker_last_tile_label(worker), worker.done_seen ? 1 : 0, worker.done_failed ? 1 : 0, worker.drain_budget_hit ? 1 : 0,
                     worker.phase_packets, worker_phase_label(worker), worker.last_phase_batch_start, worker.last_phase_batch_count,
                     worker.last_phase_detail0, worker.last_phase_detail1, worker_event_age_seconds(now, worker.last_read_at),
                     worker_event_age_seconds(now, worker.last_assignment_at), worker_event_age_seconds(now, worker.last_batch_done_at),
                     worker_event_age_seconds(now, worker.last_phase_at));
    }
}

void wait_for_worker_pipe_activity(std::span<const ChildWorker> workers, int timeout_ms) {
    std::vector<pollfd> fds;
    fds.reserve(workers.size());
    for (const auto& worker : workers) {
        if (worker.pipe_open && worker.read_fd >= 0) {
            fds.push_back({
                .fd = worker.read_fd,
                .events = POLLIN | POLLHUP | POLLERR,
                .revents = 0,
            });
        }
    }
    if (fds.empty()) {
        return;
    }

    int const READY = ::poll(fds.data(), fds.size(), timeout_ms);
    if (READY < 0 && errno != EINTR) {
        ::usleep(1000);
    }
}

void note_process_ipc_profile(ProcessIpcProfile& profile, const ChildWorker& worker, double started) {
    ++profile.completed_runs;
    profile.total_read_bytes += worker.read_bytes;
    profile.total_read_calls += worker.read_calls;
    profile.fine_grained_tail_batches += worker.fine_grained_tail_batches;
    profile.fine_grained_tail_tiles += worker.fine_grained_tail_tiles;
    auto const WORKER_MS = static_cast<uint32_t>(std::min<uint64_t>(worker.done_elapsed_ms, UINT32_MAX));
    auto const RENDER_MS = static_cast<uint32_t>(std::min<uint64_t>(worker.done_render_ms, UINT32_MAX));
    auto const SEND_MS = static_cast<uint32_t>(std::min<uint64_t>(worker.done_send_ms, UINT32_MAX));
    auto const RENDER_CPU_MS = static_cast<uint32_t>(std::min<uint64_t>(worker.done_render_cpu_ms, UINT32_MAX));
    profile.min_worker_ms = std::min(profile.min_worker_ms, WORKER_MS);
    profile.worker_elapsed_total_ms += WORKER_MS;
    auto host = std::ranges::find_if(profile.hosts, [&](const HostIpcProfile& item) { return item.hostname == worker.hostname; });
    if (host == profile.hosts.end()) {
        profile.hosts.push_back({
            .hostname = worker.hostname,
        });
        host = profile.hosts.end() - 1;
    }
    ++host->completed_runs;
    uint64_t const EFFECTIVE_THREADS = static_cast<uint64_t>(std::max(1, worker.worker_threads));
    host->effective_threads += EFFECTIVE_THREADS;
    host->worker_thread_capacity_total_ms += static_cast<uint64_t>(WORKER_MS) * EFFECTIVE_THREADS;
    host->worker_elapsed_total_ms += WORKER_MS;
    host->worker_render_total_ms += RENDER_MS;
    host->worker_render_cpu_total_ms += RENDER_CPU_MS;
    host->worker_send_total_ms += SEND_MS;
    host->min_worker_ms = std::min(host->min_worker_ms, WORKER_MS);
    host->max_worker_ms = std::max(host->max_worker_ms, WORKER_MS);
    host->max_render_ms = std::max(host->max_render_ms, RENDER_MS);
    host->max_render_cpu_ms = std::max(host->max_render_cpu_ms, RENDER_CPU_MS);
    host->max_send_ms = std::max(host->max_send_ms, SEND_MS);
    if (worker.done_start_cpu < 64) {
        host->worker_cpu_mask |= 1ULL << worker.done_start_cpu;
    }
    if (worker.done_end_cpu < 64) {
        host->worker_cpu_mask |= 1ULL << worker.done_end_cpu;
    }
    if (worker.done_start_cpu != worker.done_end_cpu && worker.done_start_cpu != UINT32_MAX && worker.done_end_cpu != UINT32_MAX) {
        ++host->worker_cpu_migrations;
    }
    if (WORKER_MS > profile.max_worker_ms) {
        profile.max_worker_ms = WORKER_MS;
        profile.slowest_worker_id = worker.worker_id;
        profile.slowest_hostname = worker.hostname;
        profile.slowest_expected_tiles = worker.expected_tiles;
        profile.slowest_unique_tiles = worker.unique_tiles;
        profile.slowest_received_packets = worker.received_packets;
        profile.slowest_read_bytes = worker.read_bytes;
        profile.slowest_read_calls = worker.read_calls;
        profile.slowest_render_ms = worker.done_render_ms;
        profile.slowest_send_ms = worker.done_send_ms;
        profile.slowest_render_cpu_ms = RENDER_CPU_MS;
        profile.slowest_start_cpu = worker.done_start_cpu;
        profile.slowest_end_cpu = worker.done_end_cpu;
        profile.slowest_batch_start = worker.batch_start;
        profile.slowest_batch_count = worker.batch_count;
        profile.slowest_first_packet_seconds = worker.first_packet_at > 0.0 ? worker.first_packet_at - started : 0.0;
        profile.slowest_closed_seconds = worker.closed_at > 0.0 ? worker.closed_at - started : 0.0;
    }
    if (worker.first_packet_at > 0.0) {
        double const FIRST = worker.first_packet_at - started;
        profile.first_packet_seconds = profile.first_packet_seconds == 0.0 ? FIRST : std::min(profile.first_packet_seconds, FIRST);
    }
    if (worker.closed_at > 0.0) {
        double const CLOSED = worker.closed_at - started;
        if (CLOSED >= profile.last_close_seconds) {
            profile.last_close_seconds = CLOSED;
            profile.last_worker_id = worker.worker_id;
            profile.last_hostname = worker.hostname;
            profile.last_expected_tiles = worker.expected_tiles;
            profile.last_elapsed_ms = WORKER_MS;
            profile.last_render_ms = worker.done_render_ms;
            profile.last_render_cpu_ms = RENDER_CPU_MS;
            profile.last_send_ms = worker.done_send_ms;
            profile.last_start_cpu = worker.done_start_cpu;
            profile.last_end_cpu = worker.done_end_cpu;
            profile.last_batch_start = worker.batch_start;
            profile.last_batch_count = worker.batch_count;
            profile.last_closed_seconds = CLOSED;
        }
    }
}

void print_process_ipc_profile(const ProcessIpcProfile& profile, double started, double launch_finished, uint64_t tiles_done,
                               uint64_t total_tiles) {
    if (profile.completed_runs == 0) {
        return;
    }

    uint32_t const MIN_WORKER_MS = profile.min_worker_ms == UINT32_MAX ? 0 : profile.min_worker_ms;
    double const AVG_WORKER_MS = static_cast<double>(profile.worker_elapsed_total_ms) / static_cast<double>(profile.completed_runs);
    std::println(stderr,
                 "renderbench: ipc profile slots={} runs={} tiles={}/{} launch={:.3f}s first_packet={:.3f}s last_close={:.3f}s "
                 "worker_ms min/avg/max={}/{:.1f}/{} read={}B/{} calls tail_batches/tiles={}/{}",
                 profile.worker_slots, profile.completed_runs, tiles_done, total_tiles, launch_finished - started,
                 profile.first_packet_seconds, profile.last_close_seconds, MIN_WORKER_MS, AVG_WORKER_MS, profile.max_worker_ms,
                 profile.total_read_bytes, profile.total_read_calls, profile.fine_grained_tail_batches, profile.fine_grained_tail_tiles);
    if (profile.slowest_worker_id >= 0) {
        std::println(stderr,
                     "renderbench: ipc slowest worker={} host={} batch={}+{} expected={} unique={} packets={} bytes={} calls={} "
                     "elapsed_ms={} render_ms={} render_cpu_ms={} send_ms={} worker_cpu={}->{} first_packet={:.3f}s closed={:.3f}s",
                     profile.slowest_worker_id, profile.slowest_hostname, profile.slowest_batch_start, profile.slowest_batch_count,
                     profile.slowest_expected_tiles, profile.slowest_unique_tiles, profile.slowest_received_packets,
                     profile.slowest_read_bytes, profile.slowest_read_calls, profile.max_worker_ms, profile.slowest_render_ms,
                     profile.slowest_render_cpu_ms, profile.slowest_send_ms, profile.slowest_start_cpu, profile.slowest_end_cpu,
                     profile.slowest_first_packet_seconds, profile.slowest_closed_seconds);
    }
    if (profile.last_worker_id >= 0) {
        std::println(stderr,
                     "renderbench: ipc last worker={} host={} batch={}+{} expected={} elapsed_ms={} render_ms={} render_cpu_ms={} "
                     "send_ms={} worker_cpu={}->{} closed={:.3f}s",
                     profile.last_worker_id, profile.last_hostname, profile.last_batch_start, profile.last_batch_count,
                     profile.last_expected_tiles, profile.last_elapsed_ms, profile.last_render_ms, profile.last_render_cpu_ms,
                     profile.last_send_ms, profile.last_start_cpu, profile.last_end_cpu, profile.last_closed_seconds);
    }
    for (const auto& host : profile.hosts) {
        if (host.completed_runs == 0) {
            continue;
        }
        uint32_t const MIN_WORKER_MS = host.min_worker_ms == UINT32_MAX ? 0 : host.min_worker_ms;
        double const AVG_WORKER_MS = static_cast<double>(host.worker_elapsed_total_ms) / static_cast<double>(host.completed_runs);
        double const AVG_RENDER_MS = static_cast<double>(host.worker_render_total_ms) / static_cast<double>(host.completed_runs);
        double const AVG_RENDER_CPU_MS = static_cast<double>(host.worker_render_cpu_total_ms) / static_cast<double>(host.completed_runs);
        double const AVG_SEND_MS = static_cast<double>(host.worker_send_total_ms) / static_cast<double>(host.completed_runs);
        std::println(stderr,
                     "renderbench: ipc host={} runs={} worker_ms min/avg/max={}/{:.1f}/{} render_ms avg/max={:.1f}/{} "
                     "render_cpu_ms avg/max={:.1f}/{} send_ms avg/max={:.1f}/{} worker_cpu_mask=0x{:x} migrations={}",
                     host.hostname, host.completed_runs, MIN_WORKER_MS, AVG_WORKER_MS, host.max_worker_ms, AVG_RENDER_MS,
                     host.max_render_ms, AVG_RENDER_CPU_MS, host.max_render_cpu_ms, AVG_SEND_MS, host.max_send_ms, host.worker_cpu_mask,
                     host.worker_cpu_migrations);
    }
}

auto json_escape(std::string_view text) -> std::string {
    std::string out;
    out.reserve(text.size() + 8U);
    for (char ch : text) {
        switch (ch) {
            case '\\':
                out += "\\\\";
                break;
            case '"':
                out += "\\\"";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out.push_back(ch);
                break;
        }
    }
    return out;
}

auto ratio_or_zero(double numerator, double denominator) -> double { return denominator > 0.0 ? numerator / denominator : 0.0; }

auto host_thread_capacity_ms(const HostIpcProfile& host) -> double {
    if (host.completed_runs == 0) {
        return 0.0;
    }
    return static_cast<double>(host.worker_thread_capacity_total_ms) / static_cast<double>(host.completed_runs);
}

void initialize_process_ipc_profile(ProcessIpcProfile& profile, std::span<const IpcWorkerSpec> specs) {
    profile.worker_slots = specs.size();
    profile.hosts.reserve(specs.size());
    for (const auto& spec : specs) {
        auto host = std::ranges::find_if(profile.hosts, [&](const HostIpcProfile& item) { return item.hostname == spec.hostname; });
        if (host == profile.hosts.end()) {
            profile.hosts.push_back({
                .hostname = spec.hostname,
            });
            host = profile.hosts.end() - 1;
        }
        ++host->configured_slots;
        host->configured_threads += static_cast<size_t>(std::max(1, spec.worker_threads));
    }
}

auto write_process_ipc_profile_json(const tracebench::Options& options, const ProcessIpcProfile& profile, double launch_seconds,
                                    uint64_t tiles_done, uint64_t total_tiles) -> bool {
    std::ostringstream out;
    uint32_t const MIN_WORKER_MS = profile.min_worker_ms == UINT32_MAX ? 0 : profile.min_worker_ms;
    double const AVG_WORKER_MS = profile.completed_runs == 0
                                     ? 0.0
                                     : static_cast<double>(profile.worker_elapsed_total_ms) / static_cast<double>(profile.completed_runs);

    out << "{\n"
        << "  \"worker_slots\": " << profile.worker_slots << ",\n"
        << "  \"completed_runs\": " << profile.completed_runs << ",\n"
        << "  \"persistent_batch_size\": " << profile.persistent_batch_size << ",\n"
        << "  \"fine_grained_process_tail_enabled\": " << (profile.fine_grained_process_tail_enabled ? "true" : "false") << ",\n"
        << "  \"fine_grained_tail_batches\": " << profile.fine_grained_tail_batches << ",\n"
        << "  \"fine_grained_tail_tiles\": " << profile.fine_grained_tail_tiles << ",\n"
        << "  \"effective_reserve_cpus\": " << profile.effective_reserve_cpus << ",\n"
        << "  \"node_worker_reserve_cpus\": " << options.node_worker_reserve_cpus << ",\n"
        << "  \"worker_output_queue_disabled\": " << (options.disable_worker_output_queue ? "true" : "false") << ",\n"
        << "  \"single_thread_worker_queue_disabled\": " << (options.disable_single_thread_worker_queue ? "true" : "false") << ",\n"
        << "  \"process_persistent_workers\": " << (options.process_persistent_workers ? "true" : "false") << ",\n"
        << "  \"tiles_done\": " << tiles_done << ",\n"
        << "  \"total_tiles\": " << total_tiles << ",\n"
        << "  \"launch_seconds\": " << launch_seconds << ",\n"
        << "  \"first_packet_seconds\": " << profile.first_packet_seconds << ",\n"
        << "  \"last_close_seconds\": " << profile.last_close_seconds << ",\n"
        << "  \"read_bytes\": " << profile.total_read_bytes << ",\n"
        << "  \"read_calls\": " << profile.total_read_calls << ",\n"
        << "  \"worker_ms_min\": " << MIN_WORKER_MS << ",\n"
        << "  \"worker_ms_avg\": " << AVG_WORKER_MS << ",\n"
        << "  \"worker_ms_max\": " << profile.max_worker_ms << ",\n"
        << "  \"hosts\": [\n";

    bool first = true;
    for (const auto& host : profile.hosts) {
        if (!first) {
            out << ",\n";
        }
        first = false;
        uint32_t const HOST_MIN_WORKER_MS = host.min_worker_ms == UINT32_MAX ? 0 : host.min_worker_ms;
        auto const RUNS = static_cast<double>(host.completed_runs);
        double const AVG_HOST_WORKER_MS = ratio_or_zero(static_cast<double>(host.worker_elapsed_total_ms), RUNS);
        double const AVG_RENDER_MS = ratio_or_zero(static_cast<double>(host.worker_render_total_ms), RUNS);
        double const AVG_RENDER_CPU_MS = ratio_or_zero(static_cast<double>(host.worker_render_cpu_total_ms), RUNS);
        double const AVG_SEND_MS = ratio_or_zero(static_cast<double>(host.worker_send_total_ms), RUNS);
        double const CAPACITY_MS = host_thread_capacity_ms(host);
        out << "    {\n"
            << "      \"host\": \"" << json_escape(host.hostname) << "\",\n"
            << "      \"configured_slots\": " << host.configured_slots << ",\n"
            << "      \"configured_threads\": " << host.configured_threads << ",\n"
            << "      \"runs\": " << host.completed_runs << ",\n"
            << "      \"effective_threads\": "
            << ratio_or_zero(static_cast<double>(host.effective_threads), static_cast<double>(host.completed_runs)) << ",\n"
            << "      \"worker_ms_min\": " << HOST_MIN_WORKER_MS << ",\n"
            << "      \"worker_ms_avg\": " << AVG_HOST_WORKER_MS << ",\n"
            << "      \"worker_ms_max\": " << host.max_worker_ms << ",\n"
            << "      \"render_ms_avg\": " << AVG_RENDER_MS << ",\n"
            << "      \"render_ms_max\": " << host.max_render_ms << ",\n"
            << "      \"render_cpu_ms_avg\": " << AVG_RENDER_CPU_MS << ",\n"
            << "      \"render_cpu_ms_max\": " << host.max_render_cpu_ms << ",\n"
            << "      \"send_ms_avg\": " << AVG_SEND_MS << ",\n"
            << "      \"send_ms_max\": " << host.max_send_ms << ",\n"
            << "      \"thread_capacity_ms\": " << CAPACITY_MS << ",\n"
            << "      \"render_cpu_occupancy\": " << ratio_or_zero(AVG_RENDER_CPU_MS, CAPACITY_MS) << ",\n"
            << "      \"pipeline_occupancy\": " << ratio_or_zero(AVG_RENDER_MS, CAPACITY_MS) << ",\n"
            << "      \"render_cpu_to_render_ratio\": "
            << ratio_or_zero(static_cast<double>(host.worker_render_cpu_total_ms), static_cast<double>(host.worker_render_total_ms))
            << ",\n"
            << "      \"worker_cpu_mask\": " << host.worker_cpu_mask << ",\n"
            << "      \"migrations\": " << host.worker_cpu_migrations << "\n"
            << "    }";
    }
    out << "\n  ]\n"
        << "}\n";

    std::ofstream file(tracebench::run_dir(options) + "/ipc_profile.json", std::ios::trunc);
    if (!file) {
        return false;
    }
    file << out.str();
    return static_cast<bool>(file);
}

auto make_node_thread_specs(const tracebench::Options& options, const std::vector<WkiPeerInfo>& peers) -> std::vector<IpcWorkerSpec> {
    struct PendingSpec {
        const WkiPeerInfo* peer = nullptr;
        int worker_threads = 1;
        int slot_count = 1;
    };

    std::vector<PendingSpec> pending;
    pending.reserve(peers.size());
    int total_slots = 0;
    int const LOCAL_COORDINATOR_RESERVE_CPUS = local_coordinator_reserve_cpus(options, peers.size());
    bool const SKIP_LOCAL_WORKER = options.coordinator_skip_local_worker && peers.size() > 1U;
    for (const auto& peer : peers) {
        if (SKIP_LOCAL_WORKER && peer.local) {
            continue;
        }
        int worker_threads = std::max(1, options.threads > 0 ? options.threads : peer.cpus);
        int reserve_cpus = std::max(0, options.node_worker_reserve_cpus);
        if (peer.local) {
            reserve_cpus = std::max(reserve_cpus, LOCAL_COORDINATOR_RESERVE_CPUS);
        }
        if (reserve_cpus > 0) {
            worker_threads = std::max(0, worker_threads - reserve_cpus);
        }
        if (worker_threads <= 0) {
            continue;
        }
        int const SLOT_COUNT = worker_threads;
        pending.push_back({
            .peer = &peer,
            .worker_threads = worker_threads,
            .slot_count = SLOT_COUNT,
        });
        total_slots += SLOT_COUNT;
    }

    std::vector<IpcWorkerSpec> specs;
    specs.reserve(pending.size());
    int const WORKER_COUNT = static_cast<int>(pending.size());
    int next_slot = 0;
    for (int id = 0; id < WORKER_COUNT; ++id) {
        const auto& item = pending.at(static_cast<size_t>(id));
        const auto& peer = *item.peer;
        specs.push_back({
            .worker_id = id,
            .worker_count = WORKER_COUNT,
            .worker_threads = item.worker_threads,
            .first_slot = next_slot,
            .slot_count = item.slot_count,
            .total_slots = total_slots,
            .hostname = peer.hostname,
            .local = peer.local,
        });
        next_slot += item.slot_count;
    }
    return specs;
}

auto make_process_specs(const tracebench::Options& options, const std::vector<WkiPeerInfo>& peers) -> std::vector<IpcWorkerSpec> {
    struct WorkerHost {
        std::string hostname;
        bool local = false;
    };

    int max_cpus = 1;
    for (const auto& peer : peers) {
        max_cpus = std::max(max_cpus, peer.cpus);
    }

    std::vector<WorkerHost> worker_hosts;
    int const LOCAL_COORDINATOR_RESERVE_CPUS = local_coordinator_reserve_cpus(options, peers.size());
    for (int cpu = 0; cpu < max_cpus; ++cpu) {
        for (const auto& peer : peers) {
            int usable_cpus = peer.cpus;
            if (peer.local && LOCAL_COORDINATOR_RESERVE_CPUS > 0) {
                usable_cpus = std::max(0, usable_cpus - LOCAL_COORDINATOR_RESERVE_CPUS);
            }
            if (cpu < usable_cpus) {
                worker_hosts.push_back({
                    .hostname = peer.hostname,
                    .local = peer.local,
                });
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
            .first_slot = id,
            .slot_count = 1,
            .total_slots = WORKER_COUNT,
            .hostname = worker_hosts.at(static_cast<size_t>(id)).hostname,
            .local = worker_hosts.at(static_cast<size_t>(id)).local,
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
    double next_preview = options.live_preview ? STARTED + options.preview_update_interval_seconds : STARTED;
    uint64_t last_preview_tiles_done = 0;

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
        uint64_t const CURRENT_TILES_DONE = tiles_done.load();
        running = CURRENT_TILES_DONE < tiles.size();
        double const NOW = tracebench::monotonic_seconds();
        if (NOW >= next_update || !running) {
            auto progress = make_progress(options, CURRENT_TILES_DONE, tiles.size(), STARTED, !running);
            (void)tracebench::write_status(options, progress);
            next_update = NOW + STATUS_UPDATE_INTERVAL_SECONDS;
        }
        bool const LIVE_PROGRESS_PREVIEW =
            options.live_preview && CURRENT_TILES_DONE != last_preview_tiles_done && (last_preview_tiles_done == 0 || NOW >= next_preview);
        if (options.live_preview && (NOW >= next_preview || !running || LIVE_PROGRESS_PREVIEW)) {
            (void)tracebench::write_preview_png(options, film);
            last_preview_tiles_done = CURRENT_TILES_DONE;
            next_preview = NOW + options.preview_update_interval_seconds;
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
    double const WORKER_STARTED = tracebench::monotonic_seconds();
    uint32_t const WORKER_START_CPU = current_worker_cpu();
    (void)send_worker_phase_packet(WORKER_STDOUT_FD, worker.worker_id, WorkerPhase::STARTED, worker.batch_start, worker.batch_count,
                                   worker.command_stream ? 1U : 0U, static_cast<uint32_t>(worker.worker_threads));
    auto scene = tracebench::load_scene(options.scene_path);
    if (!scene) {
        (void)send_worker_phase_packet(WORKER_STDOUT_FD, worker.worker_id, WorkerPhase::SCENE_LOAD_FAILED, worker.batch_start,
                                       worker.batch_count);
        return 2;
    }
    auto all_tiles = tracebench::make_tiles(options.width, options.height, options.tile_size);
    std::vector<unsigned char> tile_write_batch;
    (void)send_worker_phase_packet(WORKER_STDOUT_FD, worker.worker_id, WorkerPhase::SCENE_LOADED, worker.batch_start, worker.batch_count,
                                   all_tiles.size(), static_cast<uint32_t>(options.tile_size));
    if (options.placement == tracebench::Placement::ProcessPerCore) {
        scramble_process_tiles(all_tiles);
    }
    if (worker.command_stream) {
        int const THREADS = std::max(1, worker.worker_threads);
        size_t max_payload_floats = 0;
        for (const auto& tile : all_tiles) {
            max_payload_floats = std::max(max_payload_floats, tile_float_count(tile));
        }
        size_t const MAX_PACKET_BYTES = sizeof(TilePacketHeader) + (max_payload_floats * sizeof(float));
        std::vector<std::vector<unsigned char> > thread_packets(static_cast<size_t>(THREADS));
        for (auto& packet : thread_packets) {
            packet.resize(MAX_PACKET_BYTES);
        }
        uint64_t total_expected = 0;
        uint64_t total_rendered = 0;
        uint64_t total_sent = 0;
        double total_render_seconds = 0.0;
        double total_send_seconds = 0.0;
        double total_render_cpu_seconds = 0.0;
        bool failed = false;

        auto run_batch = [&](const WorkerBatchCommand& command) -> bool {
            if (cancel_requested()) {
                return false;
            }
            size_t const START = std::min(static_cast<size_t>(command.batch_start), all_tiles.size());
            auto const COUNT = static_cast<size_t>(command.batch_count);
            size_t const END = std::min(START + COUNT, all_tiles.size());
            size_t const EXPECTED = END - START;
            double const BATCH_STARTED = tracebench::monotonic_seconds();
            (void)send_worker_phase_packet(WORKER_STDOUT_FD, worker.worker_id, WorkerPhase::BATCH_BEGIN, command.batch_start, EXPECTED,
                                           static_cast<uint32_t>(THREADS));
            bool const STREAM_PACKETS = options.live_preview;
            if (STREAM_PACKETS) {
                (void)send_worker_phase_packet(WORKER_STDOUT_FD, worker.worker_id, WorkerPhase::BATCH_WRITING, command.batch_start,
                                               EXPECTED, 0U);
            }

            std::vector<tracebench::Tile> batch_tiles;
            batch_tiles.insert(batch_tiles.end(), all_tiles.begin() + static_cast<ptrdiff_t>(START),
                               all_tiles.begin() + static_cast<ptrdiff_t>(END));
            std::vector<std::vector<unsigned char> > batch_packets(STREAM_PACKETS ? 0U : EXPECTED);
            std::vector<thrd_t> batch_threads(static_cast<size_t>(THREADS));
            std::vector<WorkerBatchRenderThreadState> states(static_cast<size_t>(THREADS));
            std::atomic<int> next_tile{0};
            std::atomic<bool> batch_failed{false};
            std::atomic<uint64_t> tiles_rendered{0};
            LiveTileOutputQueue output_queue;
            thrd_t output_thread = {};
            bool const OUTPUT_STARTED = !STREAM_PACKETS || start_live_output_queue(output_queue, output_thread, EXPECTED, WORKER_STDOUT_FD);
            if (!OUTPUT_STARTED) {
                std::println(stderr, "renderbench: failed to start live output writer");
                return false;
            }
            for (int i = 0; i < THREADS; ++i) {
                states.at(static_cast<size_t>(i)) = {
                    .scene = scene.get(),
                    .options = &options,
                    .tiles = &batch_tiles,
                    .next_tile = &next_tile,
                    .failed = &batch_failed,
                    .tiles_rendered = &tiles_rendered,
                    .packets = &batch_packets,
                    .packet_buffer = std::span<unsigned char>(thread_packets.at(static_cast<size_t>(i)).data(),
                                                              thread_packets.at(static_cast<size_t>(i)).size()),
                    .stream_packets = STREAM_PACKETS,
                    .output_queue = STREAM_PACKETS ? &output_queue : nullptr,
                };
            }

            int created_threads = 0;
            if (THREADS == 1) {
                (void)batch_render_thread(&states.at(0));
            } else {
                for (int i = 0; i < THREADS; ++i) {
                    if (thrd_create(&batch_threads.at(static_cast<size_t>(i)), batch_render_thread, &states.at(static_cast<size_t>(i))) !=
                        THRD_SUCCESS) {
                        std::println(stderr, "renderbench: failed to start command-stream batch thread {}", i);
                        batch_failed.store(true, std::memory_order_relaxed);
                        break;
                    }
                    ++created_threads;
                }
            }
            for (int i = 0; i < created_threads; ++i) {
                thrd_join(batch_threads.at(static_cast<size_t>(i)), nullptr);
            }

            uint64_t const RENDERED = tiles_rendered.load(std::memory_order_relaxed);
            double render_seconds = 0.0;
            double render_cpu_seconds = 0.0;
            double send_seconds = 0.0;
            for (int i = 0; i < THREADS; ++i) {
                render_seconds += states.at(static_cast<size_t>(i)).render_seconds;
                render_cpu_seconds += states.at(static_cast<size_t>(i)).render_cpu_seconds;
            }
            bool output_ok = true;
            uint64_t sent = 0;
            if (STREAM_PACKETS) {
                output_ok = finish_live_output_queue(output_queue, output_thread);
                sent = output_queue.sent;
                send_seconds = output_queue.send_seconds;
                destroy_live_output_queue(output_queue);
            }
            (void)send_worker_phase_packet(WORKER_STDOUT_FD, worker.worker_id, WorkerPhase::BATCH_RENDERED, command.batch_start, EXPECTED,
                                           RENDERED, batch_failed.load(std::memory_order_relaxed) ? 1U : 0U);
            if (!STREAM_PACKETS) {
                (void)send_worker_phase_packet(WORKER_STDOUT_FD, worker.worker_id, WorkerPhase::BATCH_WRITING, command.batch_start,
                                               EXPECTED, RENDERED);
                output_ok = write_buffered_tile_packets(WORKER_STDOUT_FD, batch_packets, tile_write_batch, sent, send_seconds);
            }
            uint64_t const SENT = sent;
            bool const BATCH_FAILED = cancel_requested() || batch_failed.load(std::memory_order_relaxed) || !output_ok ||
                                      RENDERED != EXPECTED || SENT != EXPECTED;
            total_expected += EXPECTED;
            total_rendered += RENDERED;
            total_sent += SENT;
            total_render_seconds += render_seconds;
            total_send_seconds += send_seconds;
            total_render_cpu_seconds += render_cpu_seconds;
            bool const BATCH_DONE_SENT = send_worker_batch_done_packet(
                WORKER_STDOUT_FD, worker.worker_id, command.batch_start, EXPECTED, RENDERED, SENT, BATCH_FAILED,
                elapsed_milliseconds_since(BATCH_STARTED), seconds_to_milliseconds(render_seconds), seconds_to_milliseconds(send_seconds),
                seconds_to_milliseconds(render_cpu_seconds));
            return !BATCH_FAILED && BATCH_DONE_SENT;
        };

        while (!failed && !cancel_requested()) {
            WorkerBatchCommand command = {};
            if (!read_worker_batch_command(WORKER_COMMAND_FD, command)) {
                failed = true;
                break;
            }
            if (command.stop != 0 || command.batch_count == 0) {
                break;
            }
            if (!run_batch(command)) {
                failed = true;
                break;
            }
        }

        failed = failed || cancel_requested();
        uint32_t const WORKER_END_CPU = current_worker_cpu();
        (void)send_worker_phase_packet(WORKER_STDOUT_FD, worker.worker_id, WorkerPhase::DONE_SENDING, 0, total_expected, total_rendered,
                                       total_sent);
        bool const DONE_SENT =
            send_worker_done_packet(WORKER_STDOUT_FD, worker.worker_id, total_expected, total_rendered, total_sent, failed,
                                    elapsed_milliseconds_since(WORKER_STARTED), seconds_to_milliseconds(total_render_seconds),
                                    seconds_to_milliseconds(total_send_seconds), seconds_to_milliseconds(total_render_cpu_seconds),
                                    WORKER_START_CPU, WORKER_END_CPU);
        return failed || !DONE_SENT ? 2 : 0;
    }
    std::vector<tracebench::Tile> assigned_tiles;
    if (worker.batch_count > 0) {
        size_t const START = std::min(static_cast<size_t>(worker.batch_start), all_tiles.size());
        size_t const COUNT = static_cast<size_t>(worker.batch_count);
        size_t const END = std::min(START + COUNT, all_tiles.size());
        assigned_tiles.insert(assigned_tiles.end(), all_tiles.begin() + static_cast<ptrdiff_t>(START),
                              all_tiles.begin() + static_cast<ptrdiff_t>(END));
    } else {
        for (size_t index = 0; index < all_tiles.size(); ++index) {
            if (owns_tile_slot(index, worker.first_slot, worker.slot_count, worker.total_slots)) {
                assigned_tiles.push_back(all_tiles[index]);
            }
        }
    }
    int const THREADS = std::max(1, worker.worker_threads);
    (void)send_worker_phase_packet(WORKER_STDOUT_FD, worker.worker_id, WorkerPhase::BATCH_BEGIN, worker.batch_start, assigned_tiles.size(),
                                   static_cast<uint32_t>(THREADS), static_cast<uint32_t>(std::max(1, worker.worker_threads)));
    bool const STREAM_PACKETS = options.live_preview;
    if (STREAM_PACKETS) {
        (void)send_worker_phase_packet(WORKER_STDOUT_FD, worker.worker_id, WorkerPhase::BATCH_WRITING, worker.batch_start,
                                       assigned_tiles.size(), 0U);
    }
    size_t max_payload_floats = 0;
    for (const auto& tile : assigned_tiles) {
        max_payload_floats = std::max(max_payload_floats, tile_float_count(tile));
    }
    size_t const MAX_PACKET_BYTES = sizeof(TilePacketHeader) + (max_payload_floats * sizeof(float));
    std::vector<std::vector<unsigned char> > thread_packets(static_cast<size_t>(THREADS));
    for (auto& packet : thread_packets) {
        packet.resize(MAX_PACKET_BYTES);
    }
    std::vector<std::vector<unsigned char> > batch_packets(STREAM_PACKETS ? 0U : assigned_tiles.size());
    std::vector<thrd_t> threads(static_cast<size_t>(THREADS));
    std::vector<WorkerBatchRenderThreadState> states(static_cast<size_t>(THREADS));
    std::atomic<int> next_tile{0};
    std::atomic<bool> failed{false};
    std::atomic<uint64_t> tiles_rendered{0};
    LiveTileOutputQueue output_queue;
    thrd_t output_thread = {};
    bool const OUTPUT_STARTED =
        !STREAM_PACKETS || start_live_output_queue(output_queue, output_thread, assigned_tiles.size(), WORKER_STDOUT_FD);
    if (!OUTPUT_STARTED) {
        std::println(stderr, "renderbench: failed to start live output writer");
        return 2;
    }

    auto init_thread_state = [&](int index) {
        states.at(static_cast<size_t>(index)) = {
            .scene = scene.get(),
            .options = &options,
            .tiles = &assigned_tiles,
            .next_tile = &next_tile,
            .failed = &failed,
            .tiles_rendered = &tiles_rendered,
            .packets = &batch_packets,
            .packet_buffer = std::span<unsigned char>(thread_packets.at(static_cast<size_t>(index)).data(),
                                                      thread_packets.at(static_cast<size_t>(index)).size()),
            .stream_packets = STREAM_PACKETS,
            .output_queue = STREAM_PACKETS ? &output_queue : nullptr,
        };
    };

    int created_threads = 0;
    if (THREADS == 1) {
        init_thread_state(0);
        (void)batch_render_thread(&states.at(0));
    } else {
        for (int i = 0; i < THREADS; ++i) {
            init_thread_state(i);
            if (thrd_create(&threads[static_cast<size_t>(i)], batch_render_thread, &states[static_cast<size_t>(i)]) != THRD_SUCCESS) {
                std::println(stderr, "renderbench: failed to start IPC worker thread {}", i);
                failed.store(true, std::memory_order_relaxed);
                break;
            }
            ++created_threads;
        }
    }

    for (int i = 0; i < created_threads; ++i) {
        thrd_join(threads[static_cast<size_t>(i)], nullptr);
    }

    uint64_t const EXPECTED = assigned_tiles.size();
    uint64_t const RENDERED = tiles_rendered.load(std::memory_order_relaxed);
    int const ACTIVE_STATES = THREADS == 1 ? 1 : created_threads;
    double render_seconds = 0.0;
    double render_cpu_seconds = 0.0;
    double send_seconds = 0.0;
    for (int i = 0; i < ACTIVE_STATES; ++i) {
        render_seconds += states.at(static_cast<size_t>(i)).render_seconds;
        render_cpu_seconds += states.at(static_cast<size_t>(i)).render_cpu_seconds;
    }
    bool output_ok = true;
    uint64_t sent = 0;
    if (STREAM_PACKETS) {
        output_ok = finish_live_output_queue(output_queue, output_thread);
        sent = output_queue.sent;
        send_seconds = output_queue.send_seconds;
        destroy_live_output_queue(output_queue);
    }
    (void)send_worker_phase_packet(WORKER_STDOUT_FD, worker.worker_id, WorkerPhase::BATCH_RENDERED, worker.batch_start, EXPECTED, RENDERED,
                                   failed.load(std::memory_order_relaxed) ? 1U : 0U);
    if (!STREAM_PACKETS) {
        (void)send_worker_phase_packet(WORKER_STDOUT_FD, worker.worker_id, WorkerPhase::BATCH_WRITING, worker.batch_start, EXPECTED,
                                       RENDERED);
        output_ok = write_buffered_tile_packets(WORKER_STDOUT_FD, batch_packets, tile_write_batch, sent, send_seconds);
    }
    uint64_t const SENT = sent;
    bool const WORKER_FAILED =
        cancel_requested() || failed.load(std::memory_order_relaxed) || !output_ok || RENDERED != EXPECTED || SENT != EXPECTED;
    uint32_t const WORKER_END_CPU = current_worker_cpu();
    (void)send_worker_phase_packet(WORKER_STDOUT_FD, worker.worker_id, WorkerPhase::DONE_SENDING, worker.batch_start, EXPECTED, RENDERED,
                                   SENT);
    bool const DONE_SENT = send_worker_done_packet(WORKER_STDOUT_FD, worker.worker_id, EXPECTED, RENDERED, SENT, WORKER_FAILED,
                                                   elapsed_milliseconds_since(WORKER_STARTED), seconds_to_milliseconds(render_seconds),
                                                   seconds_to_milliseconds(send_seconds), seconds_to_milliseconds(render_cpu_seconds),
                                                   WORKER_START_CPU, WORKER_END_CPU);
    if (WORKER_FAILED || !DONE_SENT) {
        std::println(stderr, "renderbench: worker {} completed host-side with expected={} rendered={} sent={} failed={} done_sent={}",
                     worker.worker_id, EXPECTED, RENDERED, SENT, failed.load(std::memory_order_relaxed) ? 1 : 0, DONE_SENT ? 1 : 0);
    }
    return WORKER_FAILED || !DONE_SENT ? 2 : 0;
}

auto wait_for_children(std::span<ChildWorker> workers, bool cancellation_expected) -> bool {
    bool ok = true;
    for (auto& worker : workers) {
        ok = wait_for_child(worker, cancellation_expected) && ok;
    }
    return ok;
}

auto run_distributed_ipc(const tracebench::Options& options, const std::vector<WkiPeerInfo>& peers, const char* argv0) -> int {
    auto specs = options.placement == tracebench::Placement::NodeThreads ? make_node_thread_specs(options, peers)
                                                                         : make_process_specs(options, peers);
    if (specs.empty()) {
        return run_node_threads(options);
    }

    // The command stream retains only the process and loaded scene. Each
    // batch still creates and joins its render threads in run_ipc_worker().
    bool const USE_PERSISTENT_WORKER_PROCESSES = options.process_persistent_workers;
    for (auto& spec : specs) {
        spec.command_stream = USE_PERSISTENT_WORKER_PROCESSES;
    }

    auto tiles = tracebench::make_tiles(options.width, options.height, options.tile_size);
    auto storage = tracebench::make_film_storage(options.width, options.height);
    tracebench::FilmView film{.width = options.width, .height = options.height, .rgb = std::span<float>(storage.data(), storage.size())};
    std::vector<ChildWorker> workers(specs.size());
    std::vector<unsigned char> tile_seen(tiles.size(), 0);
    bool const USE_DYNAMIC_BATCHES = !USE_PERSISTENT_WORKER_PROCESSES && (options.placement == tracebench::Placement::NodeThreads ||
                                                                          options.placement == tracebench::Placement::ProcessPerCore);
    bool const USE_DYNAMIC_ASSIGNMENT = USE_DYNAMIC_BATCHES || USE_PERSISTENT_WORKER_PROCESSES;
    if (options.placement == tracebench::Placement::ProcessPerCore) {
        scramble_process_tiles(tiles);
    }
    std::vector<int> tile_owner = USE_DYNAMIC_ASSIGNMENT
                                      ? std::vector<int>(tiles.size(), -1)
                                      : make_tile_owner_map(std::span<const tracebench::Tile>(tiles.data(), tiles.size()),
                                                            std::span<const IpcWorkerSpec>(specs.data(), specs.size()));
    std::string const PROGRAM_PATH = renderbench_program_path(argv0);
    int const PERSISTENT_BATCH_THREADS =
        persistent_batch_worker_threads(options, std::span<const IpcWorkerSpec>(specs.data(), specs.size()));
    int const PERSISTENT_BATCH_SIZE =
        USE_PERSISTENT_WORKER_PROCESSES ? dynamic_batch_size(options, tiles.size(), specs.size(), PERSISTENT_BATCH_THREADS, false) : 0;
    bool const HAS_MULTIPLE_WORKER_HOSTS =
        std::ranges::any_of(specs, [&](const IpcWorkerSpec& spec) { return spec.hostname != specs.front().hostname; });
    size_t const FINE_GRAINED_PROCESS_TAIL_THRESHOLD =
        USE_PERSISTENT_WORKER_PROCESSES && options.placement == tracebench::Placement::ProcessPerCore && HAS_MULTIPLE_WORKER_HOSTS
            ? specs.size()
            : 0U;
    if (options.placement == tracebench::Placement::NodeThreads || options.placement == tracebench::Placement::ProcessPerCore) {
        int const EFFECTIVE_LOCAL_RESERVE_CPUS = local_coordinator_reserve_cpus(options, peers.size());
        std::println(stderr,
                     "renderbench: ipc config placement={} workers={} persistent_workers={} batch_size={} fine_grained_tail_enabled={} "
                     "coordinator_reserve_cpus={} effective_reserve_cpus={} node_worker_reserve_cpus={} coordinator_skip_local_worker={} "
                     "worker_output_queue_disabled={} single_thread_worker_queue_disabled={} tile_size={}",
                     tracebench::placement_name(options.placement), specs.size(), USE_PERSISTENT_WORKER_PROCESSES ? 1 : 0,
                     PERSISTENT_BATCH_SIZE, FINE_GRAINED_PROCESS_TAIL_THRESHOLD > 0U ? 1 : 0, options.coordinator_reserve_cpus,
                     EFFECTIVE_LOCAL_RESERVE_CPUS, options.node_worker_reserve_cpus, options.coordinator_skip_local_worker ? 1 : 0,
                     options.disable_worker_output_queue ? 1 : 0, options.disable_single_thread_worker_queue ? 1 : 0, options.tile_size);
    }

    double const STARTED = tracebench::monotonic_seconds();
    double next_update = STARTED;
    double next_preview = options.live_preview ? STARTED + options.preview_update_interval_seconds : STARTED;
    uint64_t last_preview_tiles_done = 0;
    uint64_t tiles_done = 0;
    size_t next_tile_position = USE_DYNAMIC_ASSIGNMENT ? 0 : tiles.size();
    (void)tracebench::write_status(options, make_progress(options, 0, tiles.size(), STARTED, false));

    bool ok = true;
    ProcessIpcProfile ipc_profile{};
    initialize_process_ipc_profile(ipc_profile, std::span<const IpcWorkerSpec>(specs.data(), specs.size()));
    ipc_profile.persistent_batch_size = PERSISTENT_BATCH_SIZE;
    ipc_profile.fine_grained_process_tail_enabled = FINE_GRAINED_PROCESS_TAIL_THRESHOLD > 0U;
    ipc_profile.effective_reserve_cpus = local_coordinator_reserve_cpus(options, peers.size());
    size_t open_pipes = 0;
    for (size_t i = 0; i < specs.size(); ++i) {
        if (USE_PERSISTENT_WORKER_PROCESSES) {
            if (!launch_worker(PROGRAM_PATH, options, specs[i], workers[i])) {
                ok = false;
                break;
            }
            ++open_pipes;
            int const ASSIGNED = assign_worker_batch(workers[i], std::span<const tracebench::Tile>(tiles.data(), tiles.size()), tile_owner,
                                                     next_tile_position, PERSISTENT_BATCH_SIZE, FINE_GRAINED_PROCESS_TAIL_THRESHOLD);
            if (ASSIGNED < 0) {
                ok = false;
                break;
            }
        } else if (USE_DYNAMIC_BATCHES) {
            int const LAUNCHED = launch_next_worker_batch(PROGRAM_PATH, options, std::span<const IpcWorkerSpec>(specs.data(), specs.size()),
                                                          std::span<const tracebench::Tile>(tiles.data(), tiles.size()), tile_owner,
                                                          next_tile_position, i, workers[i]);
            if (LAUNCHED < 0) {
                ok = false;
                break;
            }
            if (LAUNCHED == 0) {
                continue;
            }
        } else {
            if (!launch_worker(PROGRAM_PATH, options, specs[i], workers[i])) {
                ok = false;
                break;
            }
            workers[i].expected_tiles =
                expected_tile_count_for_slots(tiles.size(), specs[i].first_slot, specs[i].slot_count, specs[i].total_slots);
        }
        if (!USE_PERSISTENT_WORKER_PROCESSES) {
            ++open_pipes;
        }
        if (cancel_requested()) {
            signal_workers(std::span<ChildWorker>(workers.data(), workers.size()), static_cast<int>(g_cancel_signal));
            (void)wait_for_children_after_cancel(std::span<ChildWorker>(workers.data(), workers.size()), true);
            return cancel_exit_code();
        }
    }
    double const LAUNCH_FINISHED = tracebench::monotonic_seconds();
    uint64_t last_stall_report_tiles_done = tiles_done;
    double next_stall_report = LAUNCH_FINISHED + COORDINATOR_STALL_REPORT_SECONDS;

    bool cancellation_sent = false;
    bool worker_termination_expected = false;
    bool kill_escalated = false;
    double cancel_started = 0.0;
    while (ok && open_pipes != 0) {
        if (cancel_requested() && !cancellation_sent) {
            cancellation_sent = true;
            worker_termination_expected = true;
            cancel_started = tracebench::monotonic_seconds();
            ok = false;
            signal_workers(std::span<ChildWorker>(workers.data(), workers.size()), static_cast<int>(g_cancel_signal));
        }

        for (auto& worker : workers) {
            if (!worker.pipe_open) {
                continue;
            }
            if (!drain_ready_worker_pipe(worker, film, tile_seen, tile_owner, tiles_done)) {
                ok = false;
                worker.pipe_open = false;
            }
            if (!worker.pipe_open) {
                --open_pipes;
                if (!worker.buffer.empty()) {
                    print_partial_worker_packet(worker);
                    if (worker.unique_tiles != worker.expected_tiles || worker.buffer.size() >= sizeof(TilePacketHeader)) {
                        ok = false;
                    }
                }
                if (worker.done_failed) {
                    ok = false;
                }
                if (!worker.done_seen) {
                    std::println(stderr, "renderbench: worker {} on {} closed without done packet", worker.worker_id, worker.hostname);
                    ok = false;
                }
                if (worker.unique_tiles != worker.expected_tiles) {
                    std::println(stderr,
                                 "renderbench: worker {} on {} delivered {}/{} unique tiles packets={} duplicates={} foreign={} done={}"
                                 " done_sent={}/{}",
                                 worker.worker_id, worker.hostname, worker.unique_tiles, worker.expected_tiles, worker.received_packets,
                                 worker.duplicate_tiles, worker.foreign_tiles, worker.done_seen ? 1 : 0, worker.done_sent_tiles,
                                 worker.done_expected_tiles);
                    ok = false;
                }
                bool const CHILD_OK = wait_for_child(worker, worker_termination_expected);
                ok = CHILD_OK && ok;
                if (CHILD_OK && !worker_termination_expected) {
                    note_process_ipc_profile(ipc_profile, worker, STARTED);
                }
                if (ok && USE_DYNAMIC_BATCHES && !worker_termination_expected) {
                    size_t const WORKER_INDEX = static_cast<size_t>(&worker - workers.data());
                    int const LAUNCHED =
                        launch_next_worker_batch(PROGRAM_PATH, options, std::span<const IpcWorkerSpec>(specs.data(), specs.size()),
                                                 std::span<const tracebench::Tile>(tiles.data(), tiles.size()), tile_owner,
                                                 next_tile_position, WORKER_INDEX, worker);
                    if (LAUNCHED < 0) {
                        ok = false;
                    } else if (LAUNCHED > 0) {
                        ++open_pipes;
                    }
                }
            } else if (ok && USE_PERSISTENT_WORKER_PROCESSES && !worker_termination_expected && worker.ready_for_batch &&
                       !worker.stop_sent) {
                int const ASSIGNED = assign_worker_batch(worker, std::span<const tracebench::Tile>(tiles.data(), tiles.size()), tile_owner,
                                                         next_tile_position, PERSISTENT_BATCH_SIZE, FINE_GRAINED_PROCESS_TAIL_THRESHOLD);
                if (ASSIGNED < 0) {
                    ok = false;
                }
            }
        }

        double const NOW = tracebench::monotonic_seconds();
        if (tiles_done != last_stall_report_tiles_done) {
            last_stall_report_tiles_done = tiles_done;
            next_stall_report = NOW + COORDINATOR_STALL_REPORT_SECONDS;
        } else if (!worker_termination_expected && open_pipes != 0 && NOW >= next_stall_report) {
            print_worker_stall_report(std::span<const ChildWorker>(workers.data(), workers.size()), tiles_done, tiles.size(),
                                      next_tile_position, open_pipes, PERSISTENT_BATCH_SIZE, NOW);
            next_stall_report = NOW + COORDINATOR_STALL_REPORT_SECONDS;
        }
        if (cancellation_sent && !kill_escalated && NOW - cancel_started >= 2.0) {
            kill_escalated = true;
            signal_workers(std::span<ChildWorker>(workers.data(), workers.size()), SIGKILL);
        }
        if (NOW >= next_update || open_pipes == 0) {
            bool const DONE = open_pipes == 0 && tiles_done >= tiles.size();
            auto progress = make_progress(options, tiles_done, tiles.size(), STARTED, DONE);
            (void)tracebench::write_status(options, progress);
            next_update = NOW + STATUS_UPDATE_INTERVAL_SECONDS;
        }
        bool const LIVE_PROGRESS_PREVIEW =
            options.live_preview && tiles_done != last_preview_tiles_done && (last_preview_tiles_done == 0 || NOW >= next_preview);
        if (options.live_preview && (NOW >= next_preview || open_pipes == 0 || LIVE_PROGRESS_PREVIEW)) {
            (void)tracebench::write_preview_png(options, film);
            last_preview_tiles_done = tiles_done;
            next_preview = NOW + options.preview_update_interval_seconds;
        }
        if (open_pipes != 0) {
            int const POLL_TIMEOUT_MS = should_poll_workers_immediately(std::span<const ChildWorker>(workers.data(), workers.size()))
                                            ? 0
                                            : WORKER_PIPE_IDLE_POLL_TIMEOUT_MS;
            wait_for_worker_pipe_activity(std::span<const ChildWorker>(workers.data(), workers.size()), POLL_TIMEOUT_MS);
        }
    }

    if (!ok && !cancellation_sent) {
        std::println(stderr, "renderbench: worker failure detected; canceling remaining workers");
        worker_termination_expected = true;
        signal_workers(std::span<ChildWorker>(workers.data(), workers.size()), SIGTERM);
        close_worker_pipes(std::span<ChildWorker>(workers.data(), workers.size()));
    }

    if (worker_termination_expected) {
        ok = wait_for_children_after_cancel(std::span<ChildWorker>(workers.data(), workers.size()), true) && ok;
    } else {
        ok = wait_for_children(std::span<ChildWorker>(workers.data(), workers.size()), false) && ok;
    }
    if (tiles_done != tiles.size()) {
        std::println(stderr, "renderbench: completed {} of {} tiles", tiles_done, tiles.size());
        print_missing_tile_summary(tile_seen);
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
    (void)write_process_ipc_profile_json(options, ipc_profile, LAUNCH_FINISHED - STARTED, tiles_done, tiles.size());
    (void)tracebench::write_final_png(options, film);
    (void)tracebench::write_preview_png(options, film);
    print_process_ipc_profile(ipc_profile, STARTED, LAUNCH_FINISHED, tiles_done, tiles.size());
    return 0;
}

auto parse_worker_invocation(int argc, char** argv) -> WorkerInvocation {
    WorkerInvocation worker;
    bool explicit_slots = false;
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
        } else if (ARG == "--worker-first-slot") {
            explicit_slots = true;
            nonnegative_value(worker.first_slot);
        } else if (ARG == "--worker-slots") {
            explicit_slots = true;
            positive_value(worker.slot_count);
        } else if (ARG == "--worker-total-slots") {
            explicit_slots = true;
            positive_value(worker.total_slots);
        } else if (ARG == "--worker-batch-start") {
            nonnegative_value(worker.batch_start);
        } else if (ARG == "--worker-batch-count") {
            positive_value(worker.batch_count);
        } else if (ARG == "--worker-command-stream") {
            worker.command_stream = true;
        }
    }
    if (worker.worker_id >= worker.worker_count) {
        worker.worker_id = worker.worker_count - 1;
    }
    if (!explicit_slots) {
        worker.first_slot = worker.worker_id;
        worker.slot_count = 1;
        worker.total_slots = worker.worker_count;
    } else {
        worker.total_slots = std::max(worker.total_slots, worker.worker_count);
        worker.first_slot = std::min(worker.first_slot, worker.total_slots - 1);
        worker.slot_count = std::clamp(worker.slot_count, 1, worker.total_slots - worker.first_slot);
    }
    return worker;
}

}  // namespace

auto main(int argc, char** argv) -> int {
    install_cancel_signal_handlers();
    auto worker = parse_worker_invocation(argc, argv);
    tracebench::Options options;
    auto const PARSE_STATUS = tracebench::parse_options(argc, argv, tracebench::Backend::Ipc, options);
    if (PARSE_STATUS == tracebench::ParseStatus::Help) {
        return 0;
    }
    if (PARSE_STATUS == tracebench::ParseStatus::Error) {
        tracebench::print_usage(argv[0]);
        return 2;
    }
    if (options.backend != tracebench::Backend::Ipc) {
        std::println(stderr, "renderbench: WOS module supports --backend ipc");
        return 2;
    }
    if (worker.enabled) {
        return run_ipc_worker(options, worker);
    }
    if (!tracebench::ensure_output_tree(options)) {
        std::println(stderr, "renderbench: unable to create output tree under {}", options.output_root);
        return 2;
    }

    auto peers = read_wki_peers();
    apply_distributed_ipc_defaults(options);
    return run_distributed_ipc(options, peers, argv[0]);
}
#endif

#if TRACEBENCH_ENABLE_MPI
}  // namespace

auto main(int argc, char** argv) -> int { return run_mpi_renderbench(argc, argv); }
#endif
