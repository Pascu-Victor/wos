#include <array>
#include <cstdint>
#include <platform/acpi/numa_boot.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/mm/phys.hpp>
#include <platform/smt/smt.hpp>
#include <string_view>
#include <test/ktest.hpp>

namespace numa = ker::mod::acpi::numa;
namespace phys = ker::mod::mm::phys;

KTEST(NumaBoot, ExplicitTwoDomainFixtureWasAccepted) {
    auto const* topology = numa::boot_topology();
    KREQUIRE_TRUE(topology != nullptr);
    auto const* raw = ker::init::get_kernel_cmdline();
    std::string_view remaining{raw != nullptr ? raw : ""};
    bool expected = false;
    while (!remaining.empty()) {
        auto const BEGIN = remaining.find_first_not_of(" \t\r\n");
        if (BEGIN == std::string_view::npos) break;
        remaining.remove_prefix(BEGIN);
        auto const END = remaining.find_first_of(" \t\r\n");
        expected |= remaining.substr(0, END) == "--numa-test-two-nodes";
        if (END == std::string_view::npos) break;
        remaining.remove_prefix(END);
    }
    if (!expected) return;
    auto const* status = numa::boot_status();
    KREQUIRE_TRUE(status != nullptr);
    KEXPECT_FALSE(status->rejected_firmware);
    KEXPECT_EQ(topology->provenance, numa::Provenance::SRAT_SLIT);
    KREQUIRE_EQ(topology->node_count, 2U);
    KREQUIRE_EQ(topology->cpu_count, 8U);
    KEXPECT_EQ(topology->nodes[0].proximity_id, 0U);
    KEXPECT_EQ(topology->nodes[1].proximity_id, 1U);
    KEXPECT_TRUE(topology->nodes[0].usable_bytes > 0 && topology->nodes[1].usable_bytes > 0);
    KEXPECT_EQ(topology->distances[0][1], 20U);
    KEXPECT_EQ(topology->distances[1][0], 20U);
    for (size_t cpu = 0; cpu < topology->cpu_count; ++cpu) KEXPECT_EQ(topology->cpu_nodes[cpu], cpu < 4 ? 0U : 1U);
}

KTEST(NumaBoot, PublishedCpuMembershipIsExact) {
    auto const* topology = numa::boot_topology();
    auto const* status = numa::boot_status();
    KREQUIRE_TRUE(topology != nullptr);
    KREQUIRE_TRUE(status != nullptr);
    KEXPECT_EQ(topology->cpu_count, ker::mod::smt::get_core_count());
    KREQUIRE_TRUE(topology->node_count > 0 && topology->node_count <= numa::MAX_NODES);
    for (size_t cpu = 0; cpu < topology->cpu_count; ++cpu) {
        size_t memberships = 0;
        for (size_t node = 0; node < topology->node_count; ++node) {
            if (topology->nodes[node].cpus.contains(cpu)) {
                ++memberships;
                KEXPECT_EQ(topology->cpu_nodes[cpu], node);
            }
        }
        KEXPECT_EQ(memberships, 1U);
    }
    if (status->rejected_firmware) KEXPECT_EQ(topology->provenance, numa::Provenance::UMA_MISSING_SRAT);
}

KTEST(NumaBoot, ManagedRamAndNodeTotalsReconcile) {
    auto const* topology = numa::boot_topology();
    KREQUIRE_TRUE(topology != nullptr);
    std::array<uint64_t, numa::MAX_NODES> bytes{};
    uint64_t covered = 0;
    uint64_t previous_end = 0;
    for (size_t i = 0; i < topology->memory_range_count; ++i) {
        auto const& range = topology->memory[i];
        KREQUIRE_TRUE(range.node < topology->node_count);
        KREQUIRE_TRUE(range.length != 0 && range.length <= UINT64_MAX - range.base);
        KEXPECT_TRUE(range.base >= previous_end);
        KEXPECT_EQ(range.base % numa::PAGE_BYTES, 0U);
        KEXPECT_EQ(range.length % numa::PAGE_BYTES, 0U);
        previous_end = range.base + range.length;
        bytes[range.node] += range.length;
        covered += range.length;
    }
    for (size_t node = 0; node < topology->node_count; ++node) KEXPECT_EQ(bytes[node], topology->nodes[node].usable_bytes);
    uint64_t managed = 0;
    for (auto const* zone = phys::get_zones(); zone != nullptr; zone = zone->next) managed += zone->len + numa::PAGE_BYTES;
    if (auto const* huge = phys::get_huge_page_zone(); huge != nullptr) managed += huge->len;
    KEXPECT_EQ(covered, managed);
    phys::PhysicalBalanceSnapshot balance{};
    phys::get_physical_balance_snapshot(balance);
    KEXPECT_EQ(covered / numa::PAGE_BYTES, balance.managed_pages);
}
