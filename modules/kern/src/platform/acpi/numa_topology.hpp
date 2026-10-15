#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <platform/acpi/bounded_tables.hpp>
#include <span>
#include <string_view>

namespace ker::mod::acpi::numa {

inline constexpr size_t MAX_CPUS = 256;
inline constexpr size_t MAX_NODES = 64;
inline constexpr size_t MAX_MEMORY_RANGES = 512;
inline constexpr size_t MAX_RAM_RANGES = 4096;
inline constexpr uint16_t INVALID_NODE = UINT16_MAX;
inline constexpr uint64_t PAGE_BYTES = 4096;

struct CpuInput {
    uint32_t logical_index{};
    uint32_t apic_id{};
};
struct RamRange {
    uint64_t base{};
    uint64_t length{};
};
struct CpuMask {
    std::array<uint64_t, 4> words{};
    [[nodiscard]] constexpr auto contains(size_t cpu) const -> bool {
        return cpu < MAX_CPUS && (words[cpu / 64] & (uint64_t{1} << (cpu % 64))) != 0;
    }
    constexpr void set(size_t cpu) { words[cpu / 64] |= uint64_t{1} << (cpu % 64); }
    auto operator==(const CpuMask&) const -> bool = default;
};
struct Node {
    uint32_t proximity_id{};
    CpuMask cpus{};
    uint64_t usable_bytes{};
    auto operator==(const Node&) const -> bool = default;
};
struct MemoryRange {
    uint64_t base{};
    uint64_t length{};
    uint16_t node{INVALID_NODE};
    uint8_t affinity_flags{};
    auto operator==(const MemoryRange&) const -> bool = default;
};
enum class Provenance : uint8_t { UMA_MISSING_SRAT, SRAT_DEFAULT_DISTANCES, SRAT_SLIT };
struct Topology {
    Provenance provenance{Provenance::UMA_MISSING_SRAT};
    uint16_t cpu_count{};
    uint16_t node_count{};
    uint16_t memory_range_count{};
    std::array<Node, MAX_NODES> nodes{};
    std::array<uint16_t, MAX_CPUS> cpu_nodes{};
    std::array<uint32_t, MAX_CPUS> cpu_apic_ids{};
    std::array<MemoryRange, MAX_MEMORY_RANGES> memory{};
    std::array<std::array<uint8_t, MAX_NODES>, MAX_NODES> distances{};
    [[nodiscard]] auto node_for_proximity(uint32_t id) const -> uint16_t {
        for (uint16_t i = 0; i < node_count; ++i)
            if (nodes[i].proximity_id == id) return i;
        return INVALID_NODE;
    }
    auto operator==(const Topology&) const -> bool = default;
};
enum class Error : uint8_t {
    NONE,
    OUTPUT_ALIAS,
    CPU_INPUT,
    RAM_INPUT,
    CAPACITY,
    SIGNATURE,
    LENGTH,
    CHECKSUM,
    RESERVED,
    RECORD_LENGTH,
    DUPLICATE_CPU,
    MISSING_CPU,
    UNSUPPORTED_CPU,
    MEMORY_RANGE,
    MEMORY_OVERLAP,
    MEMORY_COVERAGE,
    SLIT_SHAPE,
    SLIT_DISTANCE,
    SLIT_COVERAGE,
};
enum class Table : uint8_t { INPUT, SRAT, SLIT };
struct Result {
    Error error{Error::NONE};
    Table table{Table::INPUT};
    size_t offset{};
    [[nodiscard]] auto valid() const -> bool { return error == Error::NONE; }
};
struct CpuAffinity {
    uint32_t apic_id{};
    uint32_t proximity_id{};
};
struct MemoryAffinity {
    uint64_t base{};
    uint64_t length{};
    uint32_t proximity_id{};
    uint8_t flags{};
};
// Caller-owned boot BSS, separate from out. Never put this workspace on a boot
// stack. Scratch is disposable after refusal and is not a published topology.
struct Workspace {
    Topology candidate{};
    std::array<CpuAffinity, MAX_CPUS> cpus{};
    std::array<MemoryAffinity, MAX_MEMORY_RANGES> memory{};
    size_t cpu_count{};
    size_t memory_count{};
};

namespace detail {
inline auto fail(Error error, Table table, size_t offset = 0) -> Result { return {error, table, offset}; }
inline auto read(std::span<const uint8_t> bytes, size_t at, size_t size) -> uint64_t {
    uint64_t value = 0;
    for (size_t i = 0; i < size; ++i) value |= uint64_t{bytes[at + i]} << (8 * i);
    return value;
}
inline auto zero(std::span<const uint8_t> bytes, size_t at, size_t size) -> bool {
    for (size_t i = 0; i < size; ++i)
        if (bytes[at + i] != 0) return false;
    return true;
}
inline auto table(std::span<const uint8_t> bytes, std::string_view signature, size_t minimum, Table kind) -> Result {
    if (bytes.empty()) return {};
    if (bytes.size() < minimum || bytes.size() > bounded::MAX_TABLE_BYTES || read(bytes, 4, 4) != bytes.size())
        return fail(Error::LENGTH, kind, 4);
    for (size_t i = 0; i < signature.size(); ++i)
        if (bytes[i] != static_cast<uint8_t>(signature[i])) return fail(Error::SIGNATURE, kind, i);
    uint8_t sum = 0;
    for (auto byte : bytes) sum = static_cast<uint8_t>(sum + byte);
    return sum == 0 ? Result{} : fail(Error::CHECKSUM, kind, 9);
}
inline void reset(Workspace& work) {
    work.cpu_count = work.memory_count = 0;
    auto& out = work.candidate;
    out.provenance = Provenance::UMA_MISSING_SRAT;
    out.cpu_count = out.node_count = out.memory_range_count = 0;
    out.nodes.fill(Node{});
    out.cpu_nodes.fill(INVALID_NODE);
    out.cpu_apic_ids.fill(0);
    out.memory.fill(MemoryRange{});
    for (auto& row : out.distances) row.fill(0);
}
inline auto add_node(Topology& out, uint32_t proximity) -> bool {
    if (out.node_for_proximity(proximity) != INVALID_NODE) return true;
    if (out.node_count == MAX_NODES) return false;
    out.nodes[out.node_count++].proximity_id = proximity;
    return true;
}
inline void sort_nodes(Topology& out) {
    for (size_t i = 1; i < out.node_count; ++i) {
        auto value = out.nodes[i];
        size_t j = i;
        while (j != 0 && out.nodes[j - 1].proximity_id > value.proximity_id) {
            out.nodes[j] = out.nodes[j - 1];
            --j;
        }
        out.nodes[j] = value;
    }
}
inline auto add_cpu(Workspace& work, uint32_t apic, uint32_t proximity, size_t offset) -> Result {
    for (size_t i = 0; i < work.cpu_count; ++i)
        if (work.cpus[i].apic_id == apic) return fail(Error::DUPLICATE_CPU, Table::SRAT, offset);
    if (work.cpu_count == MAX_CPUS || !add_node(work.candidate, proximity)) return fail(Error::CAPACITY, Table::SRAT, offset);
    work.cpus[work.cpu_count++] = {apic, proximity};
    return {};
}
inline auto srat(std::span<const uint8_t> bytes, Workspace& work) -> Result {
    if (read(bytes, 36, 4) != 1 || !zero(bytes, 40, 8)) return fail(Error::RESERVED, Table::SRAT, 36);
    for (size_t at = 48; at < bytes.size();) {
        if (bytes.size() - at < 2) return fail(Error::RECORD_LENGTH, Table::SRAT, at);
        uint8_t const TYPE = bytes[at];
        size_t const SIZE = bytes[at + 1];
        if (SIZE < 2 || SIZE > bytes.size() - at) return fail(Error::RECORD_LENGTH, Table::SRAT, at);
        if (TYPE == 0 || TYPE == 2) {
            if (SIZE != (TYPE == 0 ? 16U : 24U)) return fail(Error::RECORD_LENGTH, Table::SRAT, at);
            size_t const FLAGS_OFFSET = TYPE == 0 ? 4 : 12;
            uint64_t const FLAGS = read(bytes, at + FLAGS_OFFSET, 4);
            if ((FLAGS & 1) != 0) {
                if ((FLAGS & ~uint64_t{1}) != 0) return fail(Error::RESERVED, Table::SRAT, at + FLAGS_OFFSET);
                if ((TYPE == 0 && bytes[at + 8] != 0) || (TYPE == 2 && (!zero(bytes, at + 2, 2) || !zero(bytes, at + 20, 4))))
                    return fail(Error::UNSUPPORTED_CPU, Table::SRAT, at);
                uint32_t const APIC = static_cast<uint32_t>(TYPE == 0 ? bytes[at + 3] : read(bytes, at + 8, 4));
                uint32_t const PROXIMITY =
                    static_cast<uint32_t>(TYPE == 0 ? bytes[at + 2] | (read(bytes, at + 9, 3) << 8) : read(bytes, at + 4, 4));
                auto result = add_cpu(work, APIC, PROXIMITY, at);
                if (!result.valid()) return result;
            }
        } else if (TYPE == 1) {
            if (SIZE != 40) return fail(Error::RECORD_LENGTH, Table::SRAT, at);
            uint64_t const FLAGS = read(bytes, at + 28, 4);
            if ((FLAGS & 1) != 0) {
                if ((FLAGS & ~uint64_t{7}) != 0 || !zero(bytes, at + 6, 2) || !zero(bytes, at + 24, 4) || !zero(bytes, at + 32, 8))
                    return fail(Error::RESERVED, Table::SRAT, at);
                uint64_t const BASE = read(bytes, at + 8, 8);
                uint64_t const LENGTH = read(bytes, at + 16, 8);
                uint32_t const PROXIMITY = static_cast<uint32_t>(read(bytes, at + 2, 4));
                if (LENGTH == 0 || LENGTH > UINT64_MAX - BASE) return fail(Error::MEMORY_RANGE, Table::SRAT, at);
                for (size_t i = 0; i < work.memory_count; ++i) {
                    auto const& other = work.memory[i];
                    if (BASE < other.base + other.length && other.base < BASE + LENGTH) return fail(Error::MEMORY_OVERLAP, Table::SRAT, at);
                }
                if (work.memory_count == MAX_MEMORY_RANGES || !add_node(work.candidate, PROXIMITY))
                    return fail(Error::CAPACITY, Table::SRAT, at);
                work.memory[work.memory_count++] = {BASE, LENGTH, PROXIMITY, static_cast<uint8_t>(FLAGS)};
            }
        } else if (TYPE == 3 || TYPE == 7) {
            // GICC and RINTC are architectural CPU affinities, not ignorable
            // extensions on x86. Disabled payload is ignored by ACPI.
            if (SIZE != (TYPE == 3 ? 18U : 20U)) return fail(Error::RECORD_LENGTH, Table::SRAT, at);
            if ((read(bytes, at + (TYPE == 3 ? 10 : 12), 4) & 1) != 0) return fail(Error::UNSUPPORTED_CPU, Table::SRAT, at);
        }
        // Unknown well-formed extension rows are bounded and skipped.
        at += SIZE;
    }
    return {};
}
inline auto slit(std::span<const uint8_t> bytes, size_t& count) -> Result {
    count = 0;
    if (bytes.empty()) return {};
    uint64_t const N = read(bytes, 36, 8);
    size_t const PAYLOAD = bytes.size() - 44;
    if (N == 0 || N > PAYLOAD || N > PAYLOAD / N || N * N != PAYLOAD) return fail(Error::SLIT_SHAPE, Table::SLIT, 36);
    count = static_cast<size_t>(N);
    for (size_t row = 0; row < count; ++row)
        for (size_t col = 0; col < count; ++col) {
            uint8_t const VALUE = bytes[44 + row * count + col];
            if ((row == col && VALUE != 10) || (row != col && VALUE <= 10))
                return fail(Error::SLIT_DISTANCE, Table::SLIT, 44 + row * count + col);
        }
    return {};
}
inline auto append_memory(Topology& out, uint64_t base, uint64_t length, uint16_t node, uint8_t flags) -> bool {
    if (out.memory_range_count != 0) {
        auto& last = out.memory[out.memory_range_count - 1];
        if (last.base + last.length == base && last.node == node && last.affinity_flags == flags) {
            last.length += length;
            out.nodes[node].usable_bytes += length;
            return true;
        }
    }
    if (out.memory_range_count == MAX_MEMORY_RANGES) return false;
    out.memory[out.memory_range_count++] = {base, length, node, flags};
    out.nodes[node].usable_bytes += length;
    return true;
}
}  // namespace detail

// Empty table span means absent, not a failed bounded firmware read. The boot
// wrapper must propagate discovery errors rather than turn malformed data into
// absence. RAM is trusted accessible input but its arithmetic/order is checked.
[[nodiscard]] inline auto parse(std::span<const uint8_t> srat, std::span<const uint8_t> slit, std::span<const CpuInput> cpus,
                                std::span<const RamRange> ram, Topology& out, Workspace& work) noexcept -> Result {
    using detail::fail;
    if (&out == &work.candidate) return fail(Error::OUTPUT_ALIAS, Table::INPUT);
    if (cpus.empty() || cpus.size() > MAX_CPUS || ram.size() > MAX_RAM_RANGES) return fail(Error::CAPACITY, Table::INPUT);
    CpuMask seen{};
    for (size_t i = 0; i < cpus.size(); ++i) {
        if (cpus[i].logical_index >= cpus.size() || seen.contains(cpus[i].logical_index)) return fail(Error::CPU_INPUT, Table::INPUT, i);
        for (size_t j = 0; j < i; ++j)
            if (cpus[i].apic_id == cpus[j].apic_id) return fail(Error::CPU_INPUT, Table::INPUT, i);
        seen.set(cpus[i].logical_index);
    }
    uint64_t total_ram = 0;
    for (size_t i = 0; i < ram.size(); ++i) {
        auto const& range = ram[i];
        if (range.length == 0 || range.length > UINT64_MAX - range.base || range.base % PAGE_BYTES != 0 || range.length % PAGE_BYTES != 0 ||
            (i != 0 && ram[i - 1].base + ram[i - 1].length > range.base) || range.length > UINT64_MAX - total_ram)
            return fail(Error::RAM_INPUT, Table::INPUT, i);
        total_ram += range.length;
    }
    auto result = detail::table(srat, "SRAT", 48, Table::SRAT);
    if (!result.valid()) return result;
    result = detail::table(slit, "SLIT", 44, Table::SLIT);
    if (!result.valid()) return result;
    size_t slit_count = 0;
    result = detail::slit(slit, slit_count);
    if (!result.valid()) return result;
    detail::reset(work);
    auto& candidate = work.candidate;
    candidate.cpu_count = static_cast<uint16_t>(cpus.size());
    if (srat.empty()) {
        candidate.node_count = 1;
        for (auto const& range : ram)
            if (!detail::append_memory(candidate, range.base, range.length, 0, 0)) return fail(Error::CAPACITY, Table::INPUT);
        for (auto const& cpu : cpus) {
            candidate.cpu_nodes[cpu.logical_index] = 0;
            candidate.cpu_apic_ids[cpu.logical_index] = cpu.apic_id;
            candidate.nodes[0].cpus.set(cpu.logical_index);
        }
        candidate.distances[0][0] = 10;
        out = candidate;
        return {};
    }
    result = detail::srat(srat, work);
    if (!result.valid()) return result;
    detail::sort_nodes(candidate);
    for (auto const& cpu : cpus) {
        uint16_t node = INVALID_NODE;
        for (size_t i = 0; i < work.cpu_count; ++i)
            if (work.cpus[i].apic_id == cpu.apic_id) node = candidate.node_for_proximity(work.cpus[i].proximity_id);
        if (node == INVALID_NODE) return fail(Error::MISSING_CPU, Table::SRAT, cpu.logical_index);
        candidate.cpu_nodes[cpu.logical_index] = node;
        candidate.cpu_apic_ids[cpu.logical_index] = cpu.apic_id;
        candidate.nodes[node].cpus.set(cpu.logical_index);
    }
    // Produce sorted RAM intersections, excluding all firmware holes/reserves.
    // Coverage cannot silently assign un-affined usable pages to node zero.
    for (auto const& range : ram) {
        uint64_t cursor = range.base;
        uint64_t const END = range.base + range.length;
        while (cursor != END) {
            MemoryAffinity const* affinity = nullptr;
            for (size_t i = 0; i < work.memory_count; ++i) {
                auto const& item = work.memory[i];
                if (item.base <= cursor && cursor < item.base + item.length) affinity = &item;
            }
            if (affinity == nullptr) return fail(Error::MEMORY_COVERAGE, Table::SRAT);
            uint64_t const LIMIT = affinity->base + affinity->length < END ? affinity->base + affinity->length : END;
            if (LIMIT % PAGE_BYTES != 0) return fail(Error::MEMORY_RANGE, Table::SRAT);
            uint16_t const NODE = candidate.node_for_proximity(affinity->proximity_id);
            if (!detail::append_memory(candidate, cursor, LIMIT - cursor, NODE, affinity->flags)) return fail(Error::CAPACITY, Table::SRAT);
            cursor = LIMIT;
        }
    }
    candidate.provenance = slit.empty() ? Provenance::SRAT_DEFAULT_DISTANCES : Provenance::SRAT_SLIT;
    for (size_t i = 0; i < candidate.node_count; ++i)
        for (size_t j = 0; j < candidate.node_count; ++j) {
            if (slit.empty()) {
                candidate.distances[i][j] = i == j ? 10 : 20;
            } else {
                size_t const ROW = candidate.nodes[i].proximity_id;
                size_t const COL = candidate.nodes[j].proximity_id;
                if (ROW >= slit_count || COL >= slit_count) return fail(Error::SLIT_COVERAGE, Table::SLIT);
                candidate.distances[i][j] = slit[44 + ROW * slit_count + COL];
            }
        }
    out = candidate;
    return {};
}

}  // namespace ker::mod::acpi::numa
