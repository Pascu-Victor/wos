#pragma once

#include <array>

#include "init_module.hpp"

namespace ker::init {

// Compile-time adjacency matrix representation
template <size_t N>
struct AdjMatrix {
    std::array<std::array<bool, N>, N> edges = {};  // edges[i][j] = true means i must run before j

    constexpr void add_edge(size_t from, size_t to) { edges[from][to] = true; }

    [[nodiscard]] constexpr bool has_edge(size_t from, size_t to) const { return edges[from][to]; }
};

// Find module index by name in a registry
template <size_t N>
constexpr int find_module_index(const std::array<ModuleMeta, N>& registry, const ModuleId& id) {
    for (size_t i = 0; i < N; ++i) {
        if (registry[i].id == id) {
            return static_cast<int>(i);
        }
    }
    return -1;  // Not found
}

// Build adjacency matrix from module registry
template <size_t N>
constexpr auto build_adj_matrix(const std::array<ModuleMeta, N>& registry) {
    AdjMatrix<N> adj{};

    for (size_t i = 0; i < N; ++i) {
        const auto& mod = registry[i];
        for (size_t d = 0; d < mod.dep_count; ++d) {
            const auto& dep = mod.deps[d];
            int dep_idx = find_module_index(registry, dep.target);
            if (dep_idx >= 0) {
                // dep_idx must run before i
                adj.add_edge(static_cast<size_t>(dep_idx), i);
            }
            // If dep_idx < 0 and HARD dep, validation will catch it
        }
    }

    return adj;
}

// Cycle detection using DFS coloring
// Returns true if a cycle is found
template <size_t N>
constexpr bool has_cycle_dfs(const AdjMatrix<N>& adj, size_t node, std::array<int, N>& colors) {
    colors[node] = 1;  // Gray (visiting)

    for (size_t i = 0; i < N; ++i) {
        if (adj.has_edge(node, i)) {
            if (colors[i] == 1) {
                // Back edge found - cycle!
                return true;
            }
            if (colors[i] == 0) {
                if (has_cycle_dfs(adj, i, colors)) {
                    return true;
                }
            }
        }
    }

    colors[node] = 2;  // Black (done)
    return false;
}

template <size_t N>
constexpr bool detect_cycle(const AdjMatrix<N>& adj) {
    std::array<int, N> colors{};  // 0=white, 1=gray, 2=black

    for (size_t i = 0; i < N; ++i) {
        if (colors[i] == 0) {
            if (has_cycle_dfs(adj, i, colors)) {
                return true;
            }
        }
    }
    return false;
}

// Kahn's algorithm for topological sort (constexpr)
// Returns array where result[i] is the index of the i-th module to initialize
template <size_t N>
constexpr auto topological_sort(const AdjMatrix<N>& adj) {
    std::array<size_t, N> result{};
    size_t result_idx = 0;

    // Calculate in-degrees
    std::array<int, N> in_degree{};
    for (size_t i = 0; i < N; ++i) {
        for (size_t j = 0; j < N; ++j) {
            if (adj.has_edge(i, j)) {
                in_degree[j]++;
            }
        }
    }

    // Queue implementation using array
    std::array<size_t, N> queue{};
    size_t q_front = 0;
    size_t q_back = 0;

    // Add all nodes with in-degree 0
    for (size_t i = 0; i < N; ++i) {
        if (in_degree[i] == 0) {
            queue[q_back++] = i;
        }
    }

    // Process queue
    while (q_front < q_back) {
        size_t node = queue[q_front++];
        result[result_idx++] = node;

        for (size_t i = 0; i < N; ++i) {
            if (adj.has_edge(node, i)) {
                in_degree[i]--;
                if (in_degree[i] == 0) {
                    queue[q_back++] = i;
                }
            }
        }
    }

    return result;
}

// Phase-aware topological sort
// Sorts modules respecting both dependencies AND boot phases
// Modules in earlier phases always come before modules in later phases
template <size_t N>
constexpr auto phase_aware_sort(const std::array<ModuleMeta, N>& registry, const std::array<size_t, N>& topo_order) {
    std::array<size_t, N> result{};
    size_t result_idx = 0;

    // Process each phase in order
    for (int phase = 0; phase < BOOT_PHASE_COUNT; ++phase) {
        // Within each phase, preserve topological order
        for (size_t i = 0; i < N; ++i) {
            size_t mod_idx = topo_order[i];
            if (static_cast<int>(registry[mod_idx].phase) == phase) {
                result[result_idx++] = mod_idx;
            }
        }
    }

    return result;
}

// Compute the final init order from a registry
// This is the main entry point - call this with your MODULE_META_REGISTRY
template <size_t N>
constexpr auto compute_init_order(const std::array<ModuleMeta, N>& registry) {
    auto adj = build_adj_matrix(registry);
    auto topo = topological_sort(adj);
    return phase_aware_sort(registry, topo);
}

}  // namespace ker::init
