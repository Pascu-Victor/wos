#pragma once

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace tracebench {

enum class Backend {
    Ipc,
    Mpi,
};

enum class Placement {
    NodeThreads,
    ProcessPerCore,
};

enum class ParseStatus {
    Ok,
    Help,
    Error,
};

struct Options {
    std::string scene_path;
    Backend backend = Backend::Ipc;
    Placement placement = Placement::NodeThreads;
    int width = 640;
    int height = 360;
    int spp = 16;
    int max_depth = 6;
    int tile_size = 32;
    std::string output_root = "/srv/tracebench/runs";
    int threads = 0;
    std::string run_id;
    bool debug_edge_colors = false;
};

struct Tile {
    int x0 = 0;
    int y0 = 0;
    int x1 = 0;
    int y1 = 0;
    int index = 0;
};

struct Progress {
    uint64_t tiles_done = 0;
    uint64_t total_tiles = 0;
    uint64_t samples_done = 0;
    uint64_t total_samples = 0;
    double elapsed_seconds = 0.0;
    bool done = false;
};

struct FilmView {
    int width = 0;
    int height = 0;
    std::span<float> rgb;

    void set_pixel(int x, int y, float r, float g, float b) const;
};

struct Scene;

auto parse_options(int argc, char* const* argv, Backend default_backend, Options& options) -> ParseStatus;
auto backend_name(Backend backend) -> const char*;
auto placement_name(Placement placement) -> const char*;
auto make_tiles(int width, int height, int tile_size) -> std::vector<Tile>;
auto make_film_storage(int width, int height) -> std::vector<float>;
auto load_scene(const std::string& path) -> std::shared_ptr<Scene>;
void render_tile(const Scene& scene, FilmView film, const Options& options, const Tile& tile, uint64_t seed);

auto monotonic_seconds() -> double;
auto make_run_id() -> std::string;
auto run_dir(const Options& options) -> std::string;
auto ensure_output_tree(const Options& options) -> bool;
auto write_status(const Options& options, const Progress& progress) -> bool;
auto write_metrics(const Options& options, const Progress& progress, double rays_per_second) -> bool;
auto write_preview_png(const Options& options, FilmView film) -> bool;
auto write_final_png(const Options& options, FilmView film) -> bool;
void print_usage(const char* argv0);

}  // namespace tracebench
