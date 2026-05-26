#ifndef TRACEBENCH_ENABLE_MPI
#define TRACEBENCH_ENABLE_MPI 0
#endif

#if TRACEBENCH_ENABLE_MPI
#include <mpi.h>
#else
#include <fcntl.h>
#include <signal.h>
#include <sys/process.h>
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

namespace {

#if !TRACEBENCH_ENABLE_MPI
constexpr std::array<char, 4> TILE_PACKET_MAGIC = {'R', 'B', 'T', 'L'};
constexpr std::array<char, 4> DONE_PACKET_MAGIC = {'R', 'B', 'T', 'D'};
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

struct WorkerDonePacket {
    std::array<char, 4> magic = DONE_PACKET_MAGIC;
    uint32_t worker_id = 0;
    uint32_t expected_tiles = 0;
    uint32_t rendered_tiles = 0;
    uint32_t sent_tiles = 0;
    uint32_t failed = 0;
    uint32_t reserved = 0;
};

static_assert(sizeof(WorkerDonePacket) == sizeof(TilePacketHeader));

struct ChildWorker {
    pid_t pid = -1;
    int read_fd = -1;
    int worker_id = 0;
    int worker_count = 1;
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
    std::atomic<uint64_t>* tiles_rendered = nullptr;
    std::atomic<uint64_t>* tiles_sent = nullptr;
    mtx_t* output_lock = nullptr;
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

auto send_worker_done_packet(int fd, int worker_id, uint64_t expected_tiles, uint64_t rendered_tiles, uint64_t sent_tiles, bool failed)
    -> bool {
    WorkerDonePacket const PACKET = {
        .magic = DONE_PACKET_MAGIC,
        .worker_id = static_cast<uint32_t>(std::max(0, worker_id)),
        .expected_tiles = static_cast<uint32_t>(std::min<uint64_t>(expected_tiles, UINT32_MAX)),
        .rendered_tiles = static_cast<uint32_t>(std::min<uint64_t>(rendered_tiles, UINT32_MAX)),
        .sent_tiles = static_cast<uint32_t>(std::min<uint64_t>(sent_tiles, UINT32_MAX)),
        .failed = failed ? 1U : 0U,
        .reserved = 0,
    };
    auto const* bytes = reinterpret_cast<const unsigned char*>(&PACKET);
    return write_all(fd, std::span<const unsigned char>(bytes, sizeof(PACKET)));
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
        state->tiles_rendered->fetch_add(1, std::memory_order_relaxed);
        mtx_lock(state->output_lock);
        bool const SENT = send_tile_packet(WORKER_STDOUT_FD, state->film, tile);
        mtx_unlock(state->output_lock);
        if (!SENT) {
            state->failed->store(true, std::memory_order_relaxed);
            break;
        }
        state->tiles_sent->fetch_add(1, std::memory_order_relaxed);
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
    tracebench::Tile const TILE{
        .x0 = static_cast<int>(header.x0),
        .y0 = static_cast<int>(header.y0),
        .x1 = static_cast<int>(header.x1),
        .y1 = static_cast<int>(header.y1),
        .index = static_cast<int>(header.tile_index),
    };
    if (payload.size() != tile_float_count(TILE)) {
        return false;
    }

    size_t in = 0;
    for (int y = TILE.y0; y < TILE.y1; ++y) {
        for (int x = TILE.x0; x < TILE.x1; ++x) {
            size_t const TARGET = ((static_cast<size_t>(y) * static_cast<size_t>(film.width)) + static_cast<size_t>(x)) * 3U;
            film.rgb[TARGET] = payload[in++];
            film.rgb[TARGET + 1U] = payload[in++];
            film.rgb[TARGET + 2U] = payload[in++];
        }
    }
    return true;
}

enum class WorkerPacketKind : uint8_t {
    NONE,
    TILE,
    DONE,
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

auto validate_tile_header(tracebench::FilmView film, const TilePacketHeader& header, size_t& payload_floats) -> bool {
    if (header.magic != TILE_PACKET_MAGIC) {
        return false;
    }
    if (header.x1 > static_cast<uint32_t>(film.width) || header.y1 > static_cast<uint32_t>(film.height) || header.x0 >= header.x1 ||
        header.y0 >= header.y1) {
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

void note_worker_done_packet(ChildWorker& worker, const WorkerDonePacket& packet) {
    if (worker.done_seen) {
        std::println(stderr, "renderbench: duplicate done packet from {}", worker.hostname);
    }
    worker.done_seen = true;
    worker.done_expected_tiles = packet.expected_tiles;
    worker.done_rendered_tiles = packet.rendered_tiles;
    worker.done_sent_tiles = packet.sent_tiles;
    worker.done_failed = packet.failed != 0;
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
}

auto note_worker_tile_packet(ChildWorker& worker, const TilePacketHeader& header, std::vector<unsigned char>& tile_seen,
                             uint64_t& tiles_done) -> bool {
    if (header.tile_index >= tile_seen.size()) {
        std::println(stderr, "renderbench: tile index {} from {} outside total {}", header.tile_index, worker.hostname, tile_seen.size());
        return false;
    }

    ++worker.received_packets;
    worker.last_tile_index = header.tile_index;
    if (worker.worker_count > 0 && static_cast<int>(header.tile_index % static_cast<uint32_t>(worker.worker_count)) != worker.worker_id) {
        ++worker.foreign_tiles;
        std::println(stderr, "renderbench: tile {} arrived from {} worker {} but belongs to worker {}", header.tile_index, worker.hostname,
                     worker.worker_id, header.tile_index % static_cast<uint32_t>(worker.worker_count));
    }

    auto& seen = tile_seen.at(static_cast<size_t>(header.tile_index));
    if (seen != 0) {
        ++worker.duplicate_tiles;
        return true;
    }

    seen = 1;
    ++worker.unique_tiles;
    ++tiles_done;
    return true;
}

auto parse_worker_packets(ChildWorker& worker, tracebench::FilmView film, std::vector<unsigned char>& tile_seen, uint64_t& tiles_done)
    -> bool {
    size_t consumed = 0;
    while (worker.buffer.size() - consumed >= sizeof(TilePacketHeader)) {
        WorkerPacketKind const KIND = packet_kind_at(worker.buffer, consumed);
        if (KIND == WorkerPacketKind::NONE) {
            size_t const NEXT = find_packet_magic(worker.buffer, consumed + 1U);
            if (NEXT == std::string::npos) {
                consumed = worker.buffer.size() >= TILE_PACKET_MAGIC.size() - 1U ? worker.buffer.size() - (TILE_PACKET_MAGIC.size() - 1U)
                                                                                 : consumed;
                break;
            }
            std::println(stderr, "renderbench: resynchronized {} after skipping {} stray byte(s)", worker.hostname, NEXT - consumed);
            consumed = NEXT;
            continue;
        }

        if (KIND == WorkerPacketKind::DONE) {
            WorkerDonePacket packet = {};
            std::memcpy(&packet, worker.buffer.data() + consumed, sizeof(packet));
            note_worker_done_packet(worker, packet);
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
            std::println(stderr, "renderbench: resynchronized {} after invalid tile header, skipped {} byte(s)", worker.hostname,
                         NEXT - consumed);
            consumed = NEXT;
            continue;
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

        std::vector<float> payload(payload_floats);
        std::memcpy(payload.data(), worker.buffer.data() + consumed + sizeof(TilePacketHeader), PAYLOAD_BYTES);
        if (!apply_tile_payload(film, header, std::span<const float>(payload.data(), payload.size()))) {
            std::println(stderr, "renderbench: invalid tile packet from {}", worker.hostname);
            return false;
        }
        if (!note_worker_tile_packet(worker, header, tile_seen, tiles_done)) {
            return false;
        }
        consumed += PACKET_BYTES;
    }

    if (consumed != 0) {
        worker.buffer.erase(worker.buffer.begin(), worker.buffer.begin() + static_cast<ptrdiff_t>(consumed));
    }
    return true;
}

auto drain_worker_pipe(ChildWorker& worker, tracebench::FilmView film, std::vector<unsigned char>& tile_seen, uint64_t& tiles_done)
    -> bool {
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
    return parse_worker_packets(worker, film, tile_seen, tiles_done);
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
        return argv0;
    }
    return "/usr/bin/renderbench";
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
    if (options.debug_edge_colors) {
        args.emplace_back("--debug-edge-colors");
    }
    return args;
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
        .worker_id = spec.worker_id,
        .worker_count = spec.worker_count,
        .hostname = spec.hostname,
        .buffer = {},
        .pipe_open = true,
    };
    return true;
}

auto expected_tile_count_for_worker(size_t total_tiles, int worker_id, int worker_count) -> uint64_t {
    if (worker_count <= 0 || worker_id < 0 || worker_id >= worker_count) {
        return 0;
    }
    size_t const FIRST = static_cast<size_t>(worker_id);
    if (FIRST >= total_tiles) {
        return 0;
    }
    return static_cast<uint64_t>(((total_tiles - 1U - FIRST) / static_cast<size_t>(worker_count)) + 1U);
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
    std::atomic<uint64_t> tiles_rendered{0};
    std::atomic<uint64_t> tiles_sent{0};
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
            .tiles_rendered = &tiles_rendered,
            .tiles_sent = &tiles_sent,
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

    uint64_t const EXPECTED = assigned_tiles.size();
    uint64_t const RENDERED = tiles_rendered.load(std::memory_order_relaxed);
    uint64_t const SENT = tiles_sent.load(std::memory_order_relaxed);
    bool const WORKER_FAILED = failed.load(std::memory_order_relaxed) || RENDERED != EXPECTED || SENT != EXPECTED;
    bool const DONE_SENT = send_worker_done_packet(WORKER_STDOUT_FD, worker.worker_id, EXPECTED, RENDERED, SENT, WORKER_FAILED);
    if (WORKER_FAILED || !DONE_SENT) {
        std::println(stderr, "renderbench: worker {} completed host-side with expected={} rendered={} sent={} failed={} done_sent={}",
                     worker.worker_id, EXPECTED, RENDERED, SENT, failed.load(std::memory_order_relaxed) ? 1 : 0, DONE_SENT ? 1 : 0);
    }
    mtx_destroy(&output_lock);
    return WORKER_FAILED || !DONE_SENT ? 2 : 0;
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

auto run_distributed_ipc(const tracebench::Options& options, const std::vector<WkiPeerInfo>& peers, const char* argv0) -> int {
    auto specs = options.placement == tracebench::Placement::NodeThreads ? make_node_thread_specs(options, peers)
                                                                         : make_process_specs(options, peers);
    if (specs.empty()) {
        return run_node_threads(options);
    }

    auto tiles = tracebench::make_tiles(options.width, options.height, options.tile_size);
    auto storage = tracebench::make_film_storage(options.width, options.height);
    tracebench::FilmView film{.width = options.width, .height = options.height, .rgb = std::span<float>(storage.data(), storage.size())};
    std::vector<ChildWorker> workers(specs.size());
    std::vector<unsigned char> tile_seen(tiles.size(), 0);
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
        workers[i].expected_tiles = expected_tile_count_for_worker(tiles.size(), specs[i].worker_id, specs[i].worker_count);
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
            if (!drain_worker_pipe(worker, film, tile_seen, tiles_done)) {
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
                if (worker.unique_tiles != worker.expected_tiles) {
                    std::println(stderr,
                                 "renderbench: worker {} on {} delivered {}/{} unique tiles packets={} duplicates={} foreign={} done={}"
                                 " done_sent={}/{}",
                                 worker.worker_id, worker.hostname, worker.unique_tiles, worker.expected_tiles, worker.received_packets,
                                 worker.duplicate_tiles, worker.foreign_tiles, worker.done_seen ? 1 : 0, worker.done_sent_tiles,
                                 worker.done_expected_tiles);
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
    install_cancel_signal_handlers();
    if (!tracebench::ensure_output_tree(options)) {
        std::println(stderr, "renderbench: unable to create output tree under {}", options.output_root);
        return 2;
    }

    auto peers = read_wki_peers();
    if (peers.size() <= 1 && options.placement == tracebench::Placement::NodeThreads) {
        return run_node_threads(options);
    }
    return run_distributed_ipc(options, peers, argv[0]);
}
#endif

#if TRACEBENCH_ENABLE_MPI
}  // namespace

auto main(int argc, char** argv) -> int { return run_mpi_renderbench(argc, argv); }
#endif
