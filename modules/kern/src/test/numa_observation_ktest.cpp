#ifdef WOS_SELFTEST
#include <abi/numa.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <memory>
#include <new>
#include <platform/acpi/numa_boot.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/task.hpp>
#include <syscalls_impl/vmem/numa.hpp>
#include <test/ktest.hpp>

namespace {
namespace abi = ker::abi::vmem::numa;
namespace mm = ker::mod::mm;
namespace placement = mm::numa;
namespace task = ker::mod::sched::task;
namespace vmem = ker::syscall::vmem;
constexpr uint64_t REQUEST = 0x4d000000ULL;
constexpr uint64_t OUTPUT = REQUEST + mm::paging::PAGE_SIZE;
constexpr uint64_t PAYLOAD = OUTPUT + mm::paging::PAGE_SIZE;

// Typed actual handler qualification on an exclusively owned never-active
// Task/root. This does not impersonate currentTask or establish CPL3 first touch.
struct Fixture {
    std::unique_ptr<task::Task> actor{new (std::nothrow) task::Task{}};
    mm::paging::PageTable* root{};
    uint8_t* request{};
    uint8_t* output{};
    uint8_t* payload{};
    ~Fixture() {
        if (actor != nullptr) {
            task::release_lazy_vmem_ranges(*actor);
            if (root != nullptr) {
                KEXPECT_EQ(actor->usercopy_pagemap_accesses.load(std::memory_order_acquire), 0U);
                KEXPECT_EQ(actor->detach_pagemap_after_usercopy_quiescence(), root);
            }
        }
        if (root != nullptr) {
            mm::virt::destroy_user_space(root, 0, "numa-observation", "private-fixture");
            mm::virt::release_pagemap(root);
        }
    }
    auto map(uint64_t address) -> uint8_t* {
        auto* page =
            static_cast<uint8_t*>(mm::phys::page_alloc(mm::PhysicalPageOwner::SELFTEST, mm::paging::PAGE_SIZE, "numa-observation"));
        if (page == nullptr) return nullptr;
        std::memset(page, 0, mm::paging::PAGE_SIZE);
        auto const PHYSICAL = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
        mm::virt::map_page(root, address, PHYSICAL, mm::paging::page_types::USER);
        return page;
    }
    auto init() -> bool {
        if (actor == nullptr) return false;
        root = mm::virt::create_pagemap();
        if (root == nullptr) return false;
        actor->type = task::TaskType::PROCESS;
        actor->pagemap = root;
        request = map(REQUEST);
        output = map(OUTPUT);
        payload = map(PAYLOAD);
        return request != nullptr && output != nullptr && payload != nullptr;
    }
    auto query(abi::QueryRequest value) -> int64_t {
        std::memcpy(request, &value, sizeof(value));
        return static_cast<int64_t>(vmem::query_numa_for_task(*actor, REQUEST, sizeof(value), 0, 0));
    }
    auto set(abi::PolicyRequest value) -> int64_t {
        std::memcpy(request, &value, sizeof(value));
        return static_cast<int64_t>(vmem::set_memory_policy_for_task(*actor, REQUEST, sizeof(value), 0, 0));
    }
    template <typename T>
    auto read() -> T {
        T value{};
        std::memcpy(&value, output, sizeof(value));
        return value;
    }
};
auto query(abi::QueryKind kind, uint32_t bytes) -> abi::QueryRequest {
    abi::QueryRequest value{};
    value.version = abi::VERSION;
    value.bytes = sizeof(value);
    value.kind = static_cast<uint32_t>(kind);
    value.output_address = OUTPUT;
    value.output_bytes = bytes;
    return value;
}
auto policy(abi::PolicyMode mode, uint64_t nodes) -> abi::PolicyRequest {
    return {abi::VERSION, sizeof(abi::PolicyRequest), static_cast<uint32_t>(mode), 0, nodes, 0};
}
}  // namespace

KTEST(NumaObservation, ImmutableSummaryAndFullNodeRecord) {
    Fixture fixture;
    KREQUIRE_TRUE(fixture.init());
    auto const* topology = ker::mod::acpi::numa::boot_topology();
    auto const* status = ker::mod::acpi::numa::boot_status();
    KREQUIRE_NE(topology, nullptr);
    KREQUIRE_NE(status, nullptr);
    KEXPECT_EQ(fixture.query(query(abi::QueryKind::SUMMARY, sizeof(abi::Summary))), 0);
    auto const SUMMARY = fixture.read<abi::Summary>();
    KEXPECT_EQ(SUMMARY.version, abi::VERSION);
    KEXPECT_EQ(SUMMARY.features, abi::FEATURES_V1);
    KEXPECT_EQ(SUMMARY.node_count, topology->node_count);
    KEXPECT_EQ(SUMMARY.cpu_count, topology->cpu_count);
    KEXPECT_EQ(SUMMARY.provenance, status->rejected_firmware ? 3U : static_cast<uint32_t>(topology->provenance));
    for (auto reserved : SUMMARY.reserved64) KEXPECT_EQ(reserved, 0U);
    auto request = query(abi::QueryKind::NODE, sizeof(abi::Node));
    request.index = topology->node_count - 1;
    KEXPECT_EQ(fixture.query(request), 0);
    auto const NODE = fixture.read<abi::Node>();
    auto const& SOURCE = topology->nodes[request.index];
    KEXPECT_EQ(NODE.node, request.index);
    KEXPECT_EQ(NODE.proximity_id, SOURCE.proximity_id);
    KEXPECT_EQ(NODE.managed_bytes, SOURCE.usable_bytes);
    for (size_t i = 0; i < abi::CPU_MASK_WORDS; ++i) KEXPECT_EQ(NODE.cpu_words[i], SOURCE.cpus.words[i]);
    KEXPECT_EQ(NODE.reserved16, 0U);
    KEXPECT_EQ(NODE.reserved64, 0U);
}

KTEST(NumaObservation, ActualActorPolicyCommitAndInvalidMaskRefusal) {
    Fixture fixture;
    KREQUIRE_TRUE(fixture.init());
    auto const BEFORE = fixture.actor->memory_policy_snapshot();
    auto const* topology = ker::mod::acpi::numa::boot_topology();
    KREQUIRE_NE(topology, nullptr);
    KREQUIRE_TRUE(topology->node_count != 0 && topology->node_count <= placement::MAX_NODES);
    uint64_t const LOW = 1;
    uint64_t const HIGH = uint64_t{1} << (topology->node_count - 1);
    auto publish = [&](abi::PolicyMode mode, uint64_t nodes) {
        KEXPECT_EQ(fixture.set(policy(mode, nodes)), 0);
        placement::Policy const EXPECTED{nodes, static_cast<placement::Mode>(mode)};
        KEXPECT_TRUE(fixture.actor->memory_policy_snapshot() == EXPECTED);
        KEXPECT_EQ(fixture.query(query(abi::QueryKind::SELF_POLICY, sizeof(abi::SelfPolicy))), 0);
        auto const READBACK = fixture.read<abi::SelfPolicy>();
        KEXPECT_EQ(READBACK.version, abi::VERSION);
        KEXPECT_EQ(READBACK.bytes, sizeof(abi::SelfPolicy));
        KEXPECT_EQ(READBACK.mode, static_cast<uint32_t>(mode));
        KEXPECT_EQ(READBACK.nodes, nodes);
        KEXPECT_EQ(READBACK.reserved32, 0U);
        KEXPECT_EQ(READBACK.reserved64, 0U);
    };
    for (auto mode : {abi::PolicyMode::PREFERRED, abi::PolicyMode::BIND, abi::PolicyMode::INTERLEAVE}) {
        publish(mode, LOW);
        publish(mode, HIGH);
        if (mode != abi::PolicyMode::PREFERRED && LOW != HIGH) publish(mode, LOW | HIGH);
    }
    // Return to the original baseline before its unchanged LOCAL negative/reset.
    KEXPECT_EQ(fixture.set(policy(abi::PolicyMode::LOCAL, 0)), 0);
    KEXPECT_TRUE(fixture.actor->memory_policy_snapshot() == BEFORE);
    KEXPECT_EQ(fixture.set(policy(abi::PolicyMode::LOCAL, 1)), static_cast<int64_t>(-EINVAL));
    KEXPECT_TRUE(fixture.actor->memory_policy_snapshot() == BEFORE);
    // Internal diagnostic publication remains observable through the same ABI.
    KREQUIRE_TRUE(fixture.actor->selftest_set_memory_policy({1, placement::Mode::BIND}));
    KEXPECT_EQ(fixture.query(query(abi::QueryKind::SELF_POLICY, sizeof(abi::SelfPolicy))), 0);
    auto const SNAPSHOT = fixture.read<abi::SelfPolicy>();
    KEXPECT_EQ(SNAPSHOT.mode, static_cast<uint32_t>(abi::PolicyMode::BIND));
    KEXPECT_EQ(SNAPSHOT.nodes, 1U);
    KEXPECT_EQ(fixture.set(policy(abi::PolicyMode::LOCAL, 0)), 0);
    KEXPECT_TRUE(fixture.actor->memory_policy_snapshot() == placement::Policy{});
}

KTEST(NumaObservation, ResidentAndAbsentRowsReleaseRealTaskRootFramePins) {
    Fixture fixture;
    KREQUIRE_TRUE(fixture.init());
    auto request = query(abi::QueryKind::PAGE_NODES, 2 * sizeof(abi::PageNode));
    request.address = PAYLOAD;
    request.count = 2;
    uint32_t const REFS = mm::phys::page_ref_get(fixture.payload);
    uint32_t const ROOT_READERS = mm::virt::selftest_user_pagemap_reader_count(fixture.root);
    KEXPECT_EQ(fixture.query(request), 0);
    auto const ROWS = fixture.read<std::array<abi::PageNode, 2>>();
    auto const PHYSICAL = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(fixture.payload)));
    KEXPECT_EQ(ROWS[0].node, placement::physical_node(PHYSICAL));
    KEXPECT_NE(ROWS[0].node, placement::INVALID_NODE);
    KEXPECT_EQ(ROWS[0].status, static_cast<uint16_t>(abi::PageStatus::RESIDENT));
    KEXPECT_EQ(ROWS[1].node, abi::INVALID_NODE);
    KEXPECT_EQ(ROWS[1].status, static_cast<uint16_t>(abi::PageStatus::UNAVAILABLE));
    KEXPECT_EQ(ROWS[0].reserved32, 0U);
    KEXPECT_EQ(ROWS[1].reserved32, 0U);
    KEXPECT_EQ(mm::phys::page_ref_get(fixture.payload), REFS);
    KEXPECT_EQ(fixture.actor->usercopy_pagemap_accesses.load(std::memory_order_acquire), 0U);
    KEXPECT_EQ(mm::virt::selftest_user_pagemap_reader_count(fixture.root), ROOT_READERS);
    auto const BEFORE = ROWS;
    request.count = abi::MAX_PAGE_NODES + 1;
    KEXPECT_EQ(fixture.query(request), static_cast<int64_t>(-EINVAL));
    auto const AFTER = fixture.read<std::array<abi::PageNode, 2>>();
    KEXPECT_EQ(std::memcmp(BEFORE.data(), AFTER.data(), sizeof(BEFORE)), 0);
}
#endif
