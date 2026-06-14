#include "render_core.hpp"

#include <mpi.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <span>
#include <thread>
#include <vector>

namespace {

auto worker_count(const tracebench::Options& options) -> int {
    if (options.threads > 0) {
        return options.threads;
    }
    unsigned int const HC = std::thread::hardware_concurrency();
    return HC == 0 ? 1 : static_cast<int>(HC);
}

auto progress_for(const tracebench::Options& options, uint64_t tiles_done, uint64_t total_tiles, double started, bool done) -> tracebench::Progress {
    uint64_t const TOTAL_SAMPLES = static_cast<uint64_t>(options.width) * static_cast<uint64_t>(options.height) * static_cast<uint64_t>(options.spp);
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

}  // namespace

auto main(int argc, char** argv) -> int {
    MPI_Init(&argc, &argv);

    int rank = 0;
    int world = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world);

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
    options.backend = tracebench::Backend::Mpi;
    auto scene = tracebench::load_scene(options.scene_path);
    auto tiles = tracebench::make_tiles(options.width, options.height, options.tile_size);
    auto local_storage = tracebench::make_film_storage(options.width, options.height);
    tracebench::FilmView local_film{.width = options.width, .height = options.height,
                                    .rgb = std::span<float>(local_storage.data(), local_storage.size())};

    if (rank == 0 && !tracebench::ensure_output_tree(options)) {
        std::fprintf(stderr, "wos_mpi_renderbench: unable to create output tree under %s\n", options.output_root.c_str());
        MPI_Abort(MPI_COMM_WORLD, 2);
    }
    MPI_Barrier(MPI_COMM_WORLD);

    double const STARTED = tracebench::monotonic_seconds();
    if (rank == 0) {
        (void)tracebench::write_status(options, progress_for(options, 0, tiles.size(), STARTED, false));
    }

    int const THREADS = options.placement == tracebench::Placement::NodeThreads ? worker_count(options) : 1;
    std::atomic<int> next_tile{rank};
    std::atomic<uint64_t> local_tiles_done{0};
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(THREADS));
    for (int tid = 0; tid < THREADS; ++tid) {
        workers.emplace_back([&, tid]() {
            for (;;) {
                int const INDEX = next_tile.fetch_add(world);
                if (INDEX >= static_cast<int>(tiles.size())) {
                    break;
                }
                tracebench::render_tile(*scene, local_film, options, tiles[static_cast<size_t>(INDEX)],
                                        0x1234ABCDEFULL + static_cast<uint64_t>(rank * 4099 + tid) + static_cast<uint64_t>(INDEX));
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

    MPI_Reduce(local_storage.data(), rank == 0 ? final_storage.data() : nullptr, static_cast<int>(local_storage.size()), MPI_FLOAT, MPI_SUM, 0,
               MPI_COMM_WORLD);

    if (rank == 0) {
        tracebench::FilmView final_film{.width = options.width, .height = options.height,
                                        .rgb = std::span<float>(final_storage.data(), final_storage.size())};
        auto progress = progress_for(options, tiles.size(), tiles.size(), STARTED, true);
        double const RAYS_PER_SECOND = progress.elapsed_seconds > 0.0 ? static_cast<double>(progress.total_samples) / progress.elapsed_seconds : 0.0;
        (void)tracebench::write_status(options, progress);
        (void)tracebench::write_metrics(options, progress, RAYS_PER_SECOND);
        (void)tracebench::write_final_png(options, final_film);
        (void)tracebench::write_preview_png(options, final_film);
    }

    MPI_Finalize();
    return 0;
}
