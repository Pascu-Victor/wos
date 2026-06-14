#include <mpi.h>

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string_view>
#include <vector>

namespace {

enum class BenchMode : std::uint8_t {
    K_PINGPONG,
    K_STREAM,
};

struct Options {
    BenchMode mode = BenchMode::K_PINGPONG;
    std::size_t payload_size = 1024;
    std::uint64_t total_bytes = 64ULL * 1024ULL * 1024ULL;
    int iterations = 10000;
};

auto parse_u64(const char* value, std::uint64_t& out) -> bool {
    const char* end = value + std::strlen(value);
    auto [ptr, ec] = std::from_chars(value, end, out);
    return ec == std::errc{} && ptr == end;
}

auto parse_int(const char* value, int& out) -> bool {
    const char* end = value + std::strlen(value);
    auto [ptr, ec] = std::from_chars(value, end, out);
    return ec == std::errc{} && ptr == end;
}

void usage() { std::cerr << "Usage: wos_mpi_netbench [--mode pingpong|stream] [--payload-size N] [--iterations N] [--total-bytes N]\n"; }

auto parse_args(int argc, char** argv, Options& options) -> bool {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
            std::string_view mode = argv[++i];
            if (mode == "pingpong") {
                options.mode = BenchMode::K_PINGPONG;
            } else if (mode == "stream") {
                options.mode = BenchMode::K_STREAM;
            } else {
                return false;
            }
        } else if (std::strcmp(argv[i], "--payload-size") == 0 && i + 1 < argc) {
            std::uint64_t value = 0;
            if (!parse_u64(argv[++i], value) || value == 0) {
                return false;
            }
            options.payload_size = static_cast<std::size_t>(value);
        } else if (std::strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
            if (!parse_int(argv[++i], options.iterations) || options.iterations <= 0) {
                return false;
            }
        } else if (std::strcmp(argv[i], "--total-bytes") == 0 && i + 1 < argc) {
            if (!parse_u64(argv[++i], options.total_bytes) || options.total_bytes == 0) {
                return false;
            }
        } else {
            return false;
        }
    }

    options.total_bytes = std::max<std::uint64_t>(options.total_bytes, options.payload_size);

    return true;
}

struct PairStats {
    double elapsed_s = 0.0;
    double bytes = 0.0;
    double latency_us = 0.0;
    int active = 0;
};

auto percentile(std::vector<double> values, double fraction) -> double {
    if (values.empty()) {
        return 0.0;
    }
    std::ranges::sort(values);
    const double SCALED_FRACTION = fraction * static_cast<double>(values.size() - 1);
    const auto PERCENTILE_INDEX = static_cast<std::size_t>(SCALED_FRACTION);
    return values[PERCENTILE_INDEX];
}

void print_summary(const Options& options, int world_size, const std::vector<PairStats>& gathered) {
    std::vector<double> elapsed;
    std::vector<double> latencies;
    double total_bytes = 0.0;

    for (const auto& stat : gathered) {
        if (!stat.active) {
            continue;
        }
        elapsed.push_back(stat.elapsed_s);
        total_bytes += stat.bytes;
        if (stat.latency_us > 0.0) {
            latencies.push_back(stat.latency_us);
        }
    }

    if (elapsed.empty()) {
        std::cerr << "No active benchmark pairs produced results\n";
        return;
    }

    const double MAX_ELAPSED = *std::ranges::max_element(elapsed);
    const double AGGREGATE_MIB_PER_S = MAX_ELAPSED > 0.0 ? (total_bytes / (1024.0 * 1024.0)) / MAX_ELAPSED : 0.0;

    std::cout << "{";
    std::cout << R"("benchmark":"mpi_netbench",)";
    std::cout << R"("mode":")" << (options.mode == BenchMode::K_PINGPONG ? "pingpong" : "stream") << "\",";
    std::cout << "\"world_size\":" << world_size << ",";
    std::cout << "\"payload_bytes\":" << options.payload_size << ",";
    if (options.mode == BenchMode::K_PINGPONG) {
        std::cout << "\"iterations\":" << options.iterations << ",";
        std::cout << "\"latency_us\":{";
        std::cout << "\"p50\":" << percentile(latencies, 0.50) << ",";
        std::cout << "\"p95\":" << percentile(latencies, 0.95) << ",";
        std::cout << "\"p99\":" << percentile(latencies, 0.99);
        std::cout << "},";
    } else {
        std::cout << "\"total_bytes\":" << options.total_bytes << ",";
    }
    std::cout << R"("aggregate_mib_per_s":)" << AGGREGATE_MIB_PER_S;
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

    if (world_size < 2 || (world_size % 2) != 0) {
        if (rank == 0) {
            std::cerr << "wos_mpi_netbench requires an even number of ranks >= 2\n";
        }
        MPI_Finalize();
        return 1;
    }

    const int PAIR_COUNT = world_size / 2;
    const bool IS_INITIATOR = rank < PAIR_COUNT;
    const int PARTNER_RANK = IS_INITIATOR ? rank + PAIR_COUNT : rank - PAIR_COUNT;
    std::vector<char> payload(options.payload_size, static_cast<char>(rank & 0xFF));
    PairStats local{};

    MPI_Barrier(MPI_COMM_WORLD);
    const double START_TIME = MPI_Wtime();

    if (options.mode == BenchMode::K_PINGPONG) {
        for (int iteration = 0; iteration < options.iterations; ++iteration) {
            if (IS_INITIATOR) {
                MPI_Send(payload.data(), static_cast<int>(payload.size()), MPI_BYTE, PARTNER_RANK, 0, MPI_COMM_WORLD);
                MPI_Recv(payload.data(), static_cast<int>(payload.size()), MPI_BYTE, PARTNER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            } else {
                MPI_Recv(payload.data(), static_cast<int>(payload.size()), MPI_BYTE, PARTNER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Send(payload.data(), static_cast<int>(payload.size()), MPI_BYTE, PARTNER_RANK, 0, MPI_COMM_WORLD);
            }
        }
        const double END_TIME = MPI_Wtime();
        if (IS_INITIATOR) {
            local.active = 1;
            local.elapsed_s = END_TIME - START_TIME;
            local.bytes = static_cast<double>(options.payload_size) * static_cast<double>(options.iterations) * 2.0;
            local.latency_us = (local.elapsed_s * 1e6) / static_cast<double>(options.iterations);
        }
    } else {
        std::uint64_t bytes_remaining = options.total_bytes;
        while (bytes_remaining > 0) {
            const int CHUNK_SIZE = static_cast<int>(std::min<std::uint64_t>(bytes_remaining, options.payload_size));
            if (IS_INITIATOR) {
                MPI_Send(payload.data(), CHUNK_SIZE, MPI_BYTE, PARTNER_RANK, 1, MPI_COMM_WORLD);
            } else {
                MPI_Recv(payload.data(), CHUNK_SIZE, MPI_BYTE, PARTNER_RANK, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
            bytes_remaining -= static_cast<std::uint64_t>(CHUNK_SIZE);
        }
        MPI_Barrier(MPI_COMM_WORLD);
        const double END_TIME = MPI_Wtime();
        if (IS_INITIATOR) {
            local.active = 1;
            local.elapsed_s = END_TIME - START_TIME;
            local.bytes = static_cast<double>(options.total_bytes);
        }
    }

    std::vector<PairStats> gathered;
    if (rank == 0) {
        gathered.resize(static_cast<std::size_t>(world_size));
    }

    MPI_Gather(&local, static_cast<int>(sizeof(PairStats)), MPI_BYTE, gathered.data(), static_cast<int>(sizeof(PairStats)), MPI_BYTE, 0,
               MPI_COMM_WORLD);

    if (rank == 0) {
        print_summary(options, world_size, gathered);
    }

    MPI_Finalize();
    return 0;
}
