#include <mpi.h>
#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <format>
#include <iostream>
#include <limits>
#include <numeric>
#include <span>
#include <string>
#include <vector>

#include "mandelbench/config.hpp"
#include "mandelbench/util.hpp"

namespace {

constexpr const char* DEVICE_NAME = "process";

struct Options {
    int width = 2000;
    int height = 2000;
    int max_iteration = 5000;
    int repeat = 5;
    const char* output_root = ".";
};

struct RowRange {
    int start_row = 0;
    int row_count = 0;
};

struct Summary {
    double total = 0.0;
    double min = 0.0;
    double max = 0.0;
    double median = 0.0;
    double avg = 0.0;
    double stdev = 0.0;
};

auto parse_int(const char* value, int& out) -> bool {
    const char* end = value + std::strlen(value);
    auto [ptr, ec] = std::from_chars(value, end, out);
    return ec == std::errc{} && ptr == end;
}

void usage() { std::cerr << "Usage: wos_mpi_mandelbench --width N --height N --max-iter N --repeat N [--output-root DIR]\n"; }

auto parse_args(int argc, char** argv, Options& options) -> bool {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--width") == 0 && i + 1 < argc) {
            if (!parse_int(argv[++i], options.width) || options.width <= 0) {
                return false;
            }
        } else if (std::strcmp(argv[i], "--height") == 0 && i + 1 < argc) {
            if (!parse_int(argv[++i], options.height) || options.height <= 0) {
                return false;
            }
        } else if (std::strcmp(argv[i], "--max-iter") == 0 && i + 1 < argc) {
            if (!parse_int(argv[++i], options.max_iteration) || options.max_iteration <= 0) {
                return false;
            }
        } else if (std::strcmp(argv[i], "--repeat") == 0 && i + 1 < argc) {
            if (!parse_int(argv[++i], options.repeat) || options.repeat <= 0) {
                return false;
            }
        } else if (std::strcmp(argv[i], "--output-root") == 0 && i + 1 < argc) {
            options.output_root = argv[++i];
        } else {
            return false;
        }
    }

    return true;
}

auto rows_for_rank(int height, int world_size, int rank) -> RowRange {
    int const BASE_ROWS = height / world_size;
    int const EXTRA_ROWS = height % world_size;
    int const ROW_COUNT = BASE_ROWS + (rank < EXTRA_ROWS ? 1 : 0);
    int const START_ROW = (rank * BASE_ROWS) + std::min(rank, EXTRA_ROWS);
    return RowRange{.start_row = START_ROW, .row_count = ROW_COUNT};
}

auto checked_byte_count(std::size_t rows, std::size_t width) -> int {
    std::size_t const BYTES = rows * width * 4U;
    if (BYTES > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        return -1;
    }
    return static_cast<int>(BYTES);
}

void generate_rows(std::span<unsigned char> image, const std::vector<unsigned char>& colormap, const Options& options, RowRange range) {
    for (int local_row = 0; local_row < range.row_count; ++local_row) {
        int const ROW = range.start_row + local_row;
        for (int col = 0; col < options.width; ++col) {
            double const C_RE = (col - (options.width / 2.0)) * 4.0 / options.width;
            double const C_IM = (ROW - (options.height / 2.0)) * 4.0 / options.width;
            double x = 0.0;
            double y = 0.0;
            int iteration = 0;
            while ((x * x) + (y * y) <= 4.0 && iteration < options.max_iteration) {
                double const X_NEW = (x * x) - (y * y) + C_RE;
                y = (2.0 * x * y) + C_IM;
                x = X_NEW;
                ++iteration;
            }
            iteration = std::min(iteration, options.max_iteration);
            set_pixel(image.data(), options.width, col, local_row, &colormap[static_cast<std::size_t>(iteration * 3)]);
        }
    }
}

auto summarize(std::span<const double> samples) -> Summary {
    Summary summary{};
    summary.total = std::accumulate(samples.begin(), samples.end(), 0.0);
    auto [min_it, max_it] = std::ranges::minmax_element(samples);
    summary.min = *min_it;
    summary.max = *max_it;
    summary.avg = summary.total / static_cast<double>(samples.size());

    for (double const SAMPLE : samples) {
        summary.stdev += (SAMPLE - summary.avg) * (SAMPLE - summary.avg);
    }
    summary.stdev = std::sqrt(summary.stdev / static_cast<double>(samples.size()));

    std::vector<double> sorted(samples.begin(), samples.end());
    std::ranges::sort(sorted);
    std::size_t const MID = sorted.size() / 2U;
    summary.median = (sorted.size() & 1U) != 0U ? sorted[MID] : (sorted[MID - 1U] + sorted[MID]) / 2.0;
    return summary;
}

void print_json(const Options& options, int world_size, const Summary& summary) {
    std::cout << "{";
    std::cout << R"("benchmark":"mpi_mandelbench",)";
    std::cout << R"("name":")" << DEVICE_NAME << "\",";
    std::cout << "\"width\":" << options.width << ",";
    std::cout << "\"height\":" << options.height << ",";
    std::cout << "\"iterations\":" << options.max_iteration << ",";
    std::cout << "\"workers\":" << world_size << ",";
    std::cout << "\"repeat\":" << options.repeat << ",";
    std::cout << "\"total_compute\":" << summary.total << ",";
    std::cout << "\"min\":" << summary.min << ",";
    std::cout << "\"max\":" << summary.max << ",";
    std::cout << "\"median\":" << summary.median << ",";
    std::cout << "\"avg\":" << summary.avg << ",";
    std::cout << "\"stdev\":" << summary.stdev;
    std::cout << "}" << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int rank = 0;
    int world_size = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    Options options;
    if (!parse_args(argc, argv, options)) {
        if (rank == 0) {
            usage();
        }
        MPI_Finalize();
        return 1;
    }

    if (world_size <= 0 || world_size > options.height) {
        if (rank == 0) {
            std::cerr << "wos_mpi_mandelbench requires 1..height ranks\n";
        }
        MPI_Finalize();
        return 1;
    }

    if (rank == 0) {
        std::error_code ec;
        std::filesystem::create_directories(options.output_root, ec);
        if (ec) {
            std::cerr << "failed to create output root '" << options.output_root << "': " << ec.message() << '\n';
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        if (chdir(options.output_root) != 0) {
            std::cerr << "failed to enter output root '" << options.output_root << "'\n";
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    std::vector<unsigned char> colormap(static_cast<std::size_t>(options.max_iteration + 1) * 3U);
    init_colormap(options.max_iteration + 1, colormap.data());

    RowRange const LOCAL_ROWS = rows_for_rank(options.height, world_size, rank);
    int const LOCAL_BYTES = checked_byte_count(static_cast<std::size_t>(LOCAL_ROWS.row_count), static_cast<std::size_t>(options.width));
    if (LOCAL_BYTES < 0) {
        if (rank == 0) {
            std::cerr << "mandel image chunk is too large for MPI_Gatherv int counts\n";
        }
        MPI_Finalize();
        return 1;
    }

    std::vector<int> recv_counts;
    std::vector<int> displacements;
    std::vector<unsigned char> image;
    if (rank == 0) {
        recv_counts.resize(static_cast<std::size_t>(world_size));
        displacements.resize(static_cast<std::size_t>(world_size));
        int offset = 0;
        for (int peer = 0; peer < world_size; ++peer) {
            RowRange const PEER_ROWS = rows_for_rank(options.height, world_size, peer);
            int const PEER_BYTES =
                checked_byte_count(static_cast<std::size_t>(PEER_ROWS.row_count), static_cast<std::size_t>(options.width));
            if (PEER_BYTES < 0) {
                std::cerr << "mandel image chunk is too large for MPI_Gatherv int counts\n";
                MPI_Abort(MPI_COMM_WORLD, 1);
            }
            recv_counts[static_cast<std::size_t>(peer)] = PEER_BYTES;
            displacements[static_cast<std::size_t>(peer)] = offset;
            offset += PEER_BYTES;
        }
        image.resize(static_cast<std::size_t>(options.width) * static_cast<std::size_t>(options.height) * 4U);
    }

    std::vector<unsigned char> local_image(static_cast<std::size_t>(LOCAL_BYTES));
    std::vector<double> times(static_cast<std::size_t>(options.repeat));

    for (int repeat_index = 0; repeat_index < options.repeat; ++repeat_index) {
        std::ranges::fill(local_image, 0);
        if (rank == 0) {
            std::ranges::fill(image, 0);
        }

        MPI_Barrier(MPI_COMM_WORLD);
        double const START_TIME = MPI_Wtime();
        generate_rows(local_image, colormap, options, LOCAL_ROWS);
        MPI_Gatherv(local_image.data(), LOCAL_BYTES, MPI_UNSIGNED_CHAR, image.data(), recv_counts.data(), displacements.data(),
                    MPI_UNSIGNED_CHAR, 0, MPI_COMM_WORLD);
        double const END_TIME = MPI_Wtime();

        if (rank == 0) {
            times[static_cast<std::size_t>(repeat_index)] = END_TIME - START_TIME;
            std::string const IMAGE_PATH = std::format(IMAGE, DEVICE_NAME, repeat_index);
            save_image(IMAGE_PATH.c_str(), image.data(), static_cast<unsigned>(options.width), static_cast<unsigned>(options.height));
            progress(DEVICE_NAME, options.width, options.height, options.max_iteration, world_size, options.repeat, repeat_index,
                     times[static_cast<std::size_t>(repeat_index)]);
        }
    }

    if (rank == 0) {
        report(DEVICE_NAME, options.width, options.height, options.max_iteration, world_size, options.repeat, times);
        print_json(options, world_size, summarize(times));
    }

    MPI_Finalize();
    return 0;
}
