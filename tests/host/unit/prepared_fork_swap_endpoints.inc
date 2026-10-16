// Physical/PTE operations are Host endpoints, not a second clone algorithm.
namespace fixture_pte {
struct Cell {
    PageTable* root{};
    uint64_t va{}, raw{};
};
std::array<Cell, 128> cells{};
}  // namespace fixture_pte
struct PageTableEntry {
    uint64_t present : 1;
    uint64_t other : 63;
};
auto pte_raw(const PageTableEntry& e) -> uint64_t {
    uint64_t raw;
    std::memcpy(&raw, &e, sizeof(raw));
    return raw;
}
auto pte_from_raw(uint64_t raw) -> PageTableEntry {
    PageTableEntry out;
    std::memcpy(&out, &raw, sizeof(raw));
    return out;
}
auto is_reserved_leaf(const PageTableEntry& e) -> bool { return (pte_raw(e) & paging::PAGE_RESERVED) != 0; }
auto leaf_entry(PageTable* root, uint64_t va) -> PageTableEntry* {
    for (auto& c : fixture_pte::cells)
        if (c.root == root && c.va == va) return reinterpret_cast<PageTableEntry*>(&c.raw);
    return nullptr;
}
namespace acpi_topology_endpoint {
struct Topology {
    size_t node_count = 2;
} topology;
}  // namespace acpi_topology_endpoint
auto valid_anonymous_placement(const numa::AllocationContext& context) -> bool {
    return context.local_node < acpi_topology_endpoint::topology.node_count &&
           numa::valid_policy(context.policy, acpi_topology_endpoint::topology.node_count);
}
