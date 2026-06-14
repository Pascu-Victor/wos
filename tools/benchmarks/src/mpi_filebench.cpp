#include <mpi.h>

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <vector>

namespace {

struct Options {
    const char* path = nullptr;
    std::size_t chunk_size = static_cast<std::size_t>(1024) * static_cast<std::size_t>(1024);
};

auto parse_u64(const char* value, std::uint64_t& out) -> bool {
    const char* end = value + std::strlen(value);
    auto [ptr, ec] = std::from_chars(value, end, out);
    return ec == std::errc{} && ptr == end;
}

void usage() { std::cerr << "Usage: wos_mpi_filebench --path <path> [--chunk-size N]\n"; }

auto parse_args(int argc, char** argv, Options& options) -> bool {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--path") == 0 && i + 1 < argc) {
            options.path = argv[++i];
        } else if (std::strcmp(argv[i], "--chunk-size") == 0 && i + 1 < argc) {
            std::uint64_t parsed = 0;
            if (!parse_u64(argv[++i], parsed) || parsed == 0) {
                return false;
            }
            options.chunk_size = static_cast<std::size_t>(parsed);
        } else {
            return false;
        }
    }
    return options.path != nullptr;
}

auto fnv1a_update(uint64_t hash, const uint8_t* data, size_t size) -> uint64_t {
    constexpr uint64_t FNV_OFFSET = 1469598103934665603ULL;
    constexpr uint64_t FNV_PRIME = 1099511628211ULL;

    if (hash == 0) {
        hash = FNV_OFFSET;
    }
    for (size_t i = 0; i < size; ++i) {
        hash ^= static_cast<uint64_t>(data[i]);
        hash *= FNV_PRIME;
    }
    return hash;
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

    if (world_size < 2) {
        if (rank == 0) {
            std::cerr << "wos_mpi_filebench requires at least 2 ranks\n";
        }
        MPI_Finalize();
        return 1;
    }

    FILE* input = nullptr;
    std::uint64_t file_size = 0;
    if (rank == 0) {
        input = std::fopen(options.path, "rb");
        if (input == nullptr) {
            std::cerr << "failed to open '" << options.path << "'\n";
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        std::fseek(input, 0, SEEK_END);
        long end_position = std::ftell(input);
        std::fseek(input, 0, SEEK_SET);
        if (end_position < 0) {
            std::cerr << "failed to determine file size for '" << options.path << "'\n";
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        file_size = static_cast<std::uint64_t>(end_position);
    }

    MPI_Bcast(&file_size, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    std::vector<uint8_t> buffer(options.chunk_size);
    std::uint64_t local_checksum = 0;
    std::uint64_t bytes_processed = 0;

    MPI_Barrier(MPI_COMM_WORLD);
    const double STARTED_AT = MPI_Wtime();

    while (bytes_processed < file_size) {
        std::uint64_t remaining = file_size - bytes_processed;
        std::uint64_t next_chunk = std::min<std::uint64_t>(remaining, options.chunk_size);
        int broadcast_bytes = static_cast<int>(next_chunk);

        if (rank == 0) {
            size_t actually_read = std::fread(buffer.data(), 1, static_cast<size_t>(broadcast_bytes), input);
            if (actually_read != static_cast<size_t>(broadcast_bytes)) {
                std::cerr << "short read while processing '" << options.path << "'\n";
                MPI_Abort(MPI_COMM_WORLD, 1);
            }
        }

        MPI_Bcast(&broadcast_bytes, 1, MPI_INT, 0, MPI_COMM_WORLD);
        MPI_Bcast(buffer.data(), broadcast_bytes, MPI_BYTE, 0, MPI_COMM_WORLD);

        local_checksum = fnv1a_update(local_checksum, buffer.data(), static_cast<size_t>(broadcast_bytes));
        bytes_processed += static_cast<std::uint64_t>(broadcast_bytes);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    const double FINISHED_AT = MPI_Wtime();

    if (rank == 0) {
        std::fclose(input);
    }

    std::vector<std::uint64_t> checksums;
    if (rank == 0) {
        checksums.resize(static_cast<std::size_t>(world_size));
    }

    MPI_Gather(&local_checksum, 1, MPI_UINT64_T, checksums.data(), 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        bool verified = true;
        for (int i = 1; i < world_size; ++i) {
            if (checksums[static_cast<std::size_t>(i)] != checksums[0]) {
                verified = false;
                break;
            }
        }

        const double ELAPSED_S = FINISHED_AT - STARTED_AT;
        const double DELIVERED_BYTES = static_cast<double>(file_size) * static_cast<double>(world_size - 1);
        const double AGGREGATE_MIB_PER_S = (DELIVERED_BYTES / (1024.0 * 1024.0)) / (ELAPSED_S > 0.0 ? ELAPSED_S : 1.0);

        std::cout << "{";
        std::cout << R"("benchmark":"mpi_filebench",)";
        std::cout << R"("path":")" << options.path << "\",";
        std::cout << "\"world_size\":" << world_size << ",";
        std::cout << "\"file_bytes\":" << file_size << ",";
        std::cout << "\"chunk_size\":" << options.chunk_size << ",";
        std::cout << "\"checksum\":" << checksums[0] << ",";
        std::cout << "\"verified\":" << (verified ? "true" : "false") << ",";
        std::cout << "\"aggregate_mib_per_s\":" << AGGREGATE_MIB_PER_S;
        std::cout << "}" << '\n';
    }

    MPI_Finalize();
    return 0;
}
