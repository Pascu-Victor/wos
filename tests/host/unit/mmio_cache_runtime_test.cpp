#include <gtest/gtest.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <map>
#include <platform/mm/cache_policy.hpp>
#include <platform/mm/mmio_cache.hpp>
#include <platform/mm/mmio_cache_platform.hpp>
#include <set>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace runtime_ports {
namespace cache = ker::mod::mm::mmio_cache;
namespace detail = cache::detail;
namespace arch = ker::mod::mm::cache_arch;
using Key = std::pair<uint32_t, uint32_t>;
constexpr uint64_t IF = uint64_t{1} << 9;
constexpr uint64_t PAT = 0x0000010500070406ULL;
constexpr uint64_t BAR = 0xd0000000;
constexpr std::array<uint32_t, 11> FIXED{0x250, 0x258, 0x259, 0x268, 0x269, 0x26a, 0x26b, 0x26c, 0x26d, 0x26e, 0x26f};

struct Cpu {
    std::map<Key, detail::Cpuid> cpuid;
    std::map<uint32_t, uint64_t> msr;
    std::set<uint32_t> faults;
    std::map<uint32_t, size_t> reads;
    uint32_t apic{};
    uint64_t cr0{0x80010011};
    uint64_t cr4{0x20};
    bool idt{true};
    bool inherited_present{true};
    bool inherited_supported{true};
    uint64_t inherited_pat{PAT};
    cache::Status boot_status{cache::Status::ready};
    cache::Status current_status{cache::Status::ready};
    size_t boot_checks{};
    bool change_end_apic{};
    std::atomic<bool>* entered{};
    std::atomic<bool>* release{};
};

std::array<Cpu, 64> cpus;
std::atomic<size_t> configured{1};
std::atomic<unsigned> violations{};
std::atomic<unsigned> irq_saves{};
std::atomic<unsigned> irq_restores{};
thread_local size_t selected = 0;
thread_local uint32_t local_apic = 0;
thread_local uint64_t flags = 2 | IF;
thread_local unsigned irq_depth = 0;

void masked() {
    if ((flags & IF) != 0 || irq_depth == 0) violations.fetch_add(1, std::memory_order_relaxed);
}

auto current() -> Cpu& {
    if (selected >= cpus.size()) {
        violations.fetch_add(1, std::memory_order_relaxed);
        return cpus[0];
    }
    return cpus[selected];
}

void select(size_t logical, uint64_t incoming_flags = 2 | IF) {
    selected = logical;
    local_apic = logical < cpus.size() ? cpus[logical].apic : UINT32_MAX;
    flags = incoming_flags;
    irq_depth = 0;
}

auto make_cpu(size_t logical, arch::Vendor vendor) -> Cpu {
    Cpu cpu;
    bool const AMD = vendor == arch::Vendor::amd;
    cpu.apic = static_cast<uint32_t>(0x100 + 3 * logical);
    cpu.cpuid[{0, 0}] = AMD ? detail::Cpuid{7, 0x68747541, 0x444d4163, 0x69746e65} : detail::Cpuid{7, 0x756e6547, 0x6c65746e, 0x49656e69};
    cpu.cpuid[{1, 0}] = {AMD ? 0x00a00f10U : 0x000906e9U, (cpu.apic & 255U) << 24, 0, (1U << 5) | (1U << 12) | (1U << 16)};
    cpu.cpuid[{7, 0}] = {};
    cpu.cpuid[{0x80000000U, 0}] = {0x80000008U, 0, 0, 0};
    cpu.cpuid[{0x80000001U, 0}] = {0, 0, 0, 1U << 29};
    cpu.cpuid[{0x80000008U, 0}] = {36, 0, 0, 0};
    cpu.msr[0x277] = PAT;
    cpu.msr[0xfe] = 0x508;
    cpu.msr[0x2ff] = 0xc00;
    for (auto index : FIXED) cpu.msr[index] = 0x0606060606060606ULL;
    for (size_t i = 0; i < 8; ++i) cpu.msr[0x201 + static_cast<uint32_t>(2 * i)] = 0;
    if (AMD) {
        cpu.msr[0xc0010010U] = 0;
        cpu.msr[0xc0010113U] = 0;
    }
    return cpu;
}

void setup(size_t count, arch::Vendor vendor = arch::Vendor::intel) {
    configured.store(count, std::memory_order_relaxed);
    violations.store(0, std::memory_order_relaxed);
    irq_saves.store(0, std::memory_order_relaxed);
    irq_restores.store(0, std::memory_order_relaxed);
    for (size_t i = 0; i < cpus.size(); ++i) cpus[i] = make_cpu(i, vendor);
    select(0);
}
}  // namespace runtime_ports

namespace ker::mod::mm::mmio_cache::detail::platform {
auto CpuIo::cpuid(uint32_t leaf, uint32_t subleaf) -> Cpuid {
    runtime_ports::masked();
    auto& cpu = runtime_ports::current();
    auto const found = cpu.cpuid.find({leaf, subleaf});
    if (found == cpu.cpuid.end()) {
        runtime_ports::violations.fetch_add(1, std::memory_order_relaxed);
        return {};
    }
    return found->second;
}

auto CpuIo::read_msr(uint32_t index, uint64_t& output) -> bool {
    runtime_ports::masked();
    auto& cpu = runtime_ports::current();
    size_t const count = ++cpu.reads[index];
    if (index == 0x277 && count == 1 && cpu.entered != nullptr && cpu.release != nullptr) {
        cpu.entered->store(true, std::memory_order_release);
        auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (!cpu.release->load(std::memory_order_acquire)) {
            if (std::chrono::steady_clock::now() >= deadline) {
                runtime_ports::violations.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            std::this_thread::yield();
        }
    }
    auto const found = cpu.msr.find(index);
    if (found == cpu.msr.end()) {
        runtime_ports::violations.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    if (cpu.faults.contains(index)) return false;
    output = found->second;
    return true;
}

auto CpuIo::read_cr0() -> uint64_t {
    runtime_ports::masked();
    return runtime_ports::current().cr0;
}
auto CpuIo::read_cr4() -> uint64_t {
    runtime_ports::masked();
    return runtime_ports::current().cr4;
}
auto CpuIo::local_idt_ready() -> bool {
    runtime_ports::masked();
    return runtime_ports::current().idt;
}
auto save_disable_interrupts() -> uint64_t {
    uint64_t const saved = runtime_ports::flags;
    runtime_ports::flags &= ~runtime_ports::IF;
    ++runtime_ports::irq_depth;
    runtime_ports::irq_saves.fetch_add(1, std::memory_order_relaxed);
    return saved;
}
void restore_interrupts(uint64_t saved_flags) {
    if (runtime_ports::irq_depth == 0)
        runtime_ports::violations.fetch_add(1, std::memory_order_relaxed);
    else
        --runtime_ports::irq_depth;
    runtime_ports::flags = saved_flags;
    runtime_ports::irq_restores.fetch_add(1, std::memory_order_relaxed);
}
auto configured_cpu_count() -> size_t { return runtime_ports::configured.load(std::memory_order_relaxed); }
auto verify_boot_cpu(size_t expected_logical, uint32_t& actual_apic) -> Status {
    runtime_ports::masked();
    if (expected_logical != runtime_ports::selected || expected_logical >= configured_cpu_count()) return Status::wrong_cpu;
    auto& cpu = runtime_ports::current();
    ++cpu.boot_checks;
    if (cpu.boot_status != Status::ready) return cpu.boot_status;
    actual_apic = runtime_ports::local_apic + (cpu.change_end_apic && cpu.boot_checks > 1 ? 1U : 0U);
    return Status::ready;
}
auto current_cpu(size_t& logical, uint32_t& actual_apic) -> Status {
    runtime_ports::masked();
    logical = runtime_ports::selected;
    actual_apic = runtime_ports::local_apic;
    return logical < runtime_ports::cpus.size() ? runtime_ports::current().current_status : Status::ready;
}
}  // namespace ker::mod::mm::mmio_cache::detail::platform

namespace ker::mod::mm::cache {
auto cpu_pat(size_t logical, CpuPat& output) -> bool {
    if (logical >= runtime_ports::cpus.size() || !runtime_ports::cpus[logical].inherited_present) return false;
    auto const& cpu = runtime_ports::cpus[logical];
    output = {cpu.inherited_supported, cpu.inherited_pat};
    return true;
}
}  // namespace ker::mod::mm::cache

namespace {
namespace cache = ker::mod::mm::mmio_cache;
namespace arch = ker::mod::mm::cache_arch;
namespace ports = runtime_ports;
using cache::QualifiedUcRange;
using cache::Status;

static_assert(!std::is_copy_constructible_v<QualifiedUcRange> && !std::is_copy_assignable_v<QualifiedUcRange>);
static_assert(std::is_nothrow_move_constructible_v<QualifiedUcRange> && std::is_nothrow_move_assignable_v<QualifiedUcRange>);
static_assert(!std::is_constructible_v<QualifiedUcRange, uint64_t, uint64_t, uint64_t>);

// No Runtime API is called in the parent. Every child inherits the untouched
// actual constinit registry; no private state, predicate or reset is copied here.
template <typename Body>
void isolated(Body body) {
    pid_t const child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        rlimit const no_core{0, 0};
        if (setrlimit(RLIMIT_CORE, &no_core) != 0) _exit(2);
        alarm(15);
        body();
        EXPECT_EQ(ports::violations.load(std::memory_order_relaxed), 0U);
        EXPECT_EQ(ports::irq_saves.load(std::memory_order_relaxed), ports::irq_restores.load(std::memory_order_relaxed));
        bool const failed = ::testing::Test::HasFailure();
        std::fflush(nullptr);
        _exit(failed ? 1 : 0);
    }
    int status = 0;
    pid_t waited = 0;
    do {
        waited = waitpid(child, &status, 0);
    } while (waited == -1 && errno == EINTR);
    ASSERT_EQ(waited, child);
    ASSERT_TRUE(WIFEXITED(status)) << "child status=" << status;
    EXPECT_EQ(WEXITSTATUS(status), 0) << "actual registry child failed";
}

auto expected_mask(size_t count) -> uint64_t { return count == 64 ? UINT64_MAX : (uint64_t{1} << count) - 1; }

void capture_all(size_t count) {
    ASSERT_EQ(cache::configure_boot_cohort(count), Status::ready);
    for (size_t i = 0; i < count; ++i) {
        ports::select(i);
        ASSERT_EQ(cache::capture_boot_cpu(i), Status::ready) << i;
    }
    ports::select(0);
}

void expect_empty(const QualifiedUcRange& range) {
    EXPECT_TRUE(range.empty());
    EXPECT_EQ(range.physical(), 0U);
    EXPECT_EQ(range.bytes(), 0U);
    EXPECT_EQ(range.incarnation(), 0U);
}

TEST(MmioCacheRuntime, UnconfiguredGeometryAndEmptyDiagnosticsDoNotMintOrMutateOutput) {
    isolated([] {
        ports::setup(1);
        EXPECT_EQ(cache::cohort_observation().status, Status::unconfigured);
        cache::CpuObservation observation{};
        observation.logical = 999;
        observation.apic = 888;
        observation.status = Status::wrong_cpu;
        EXPECT_FALSE(cache::cpu_observation(0, observation));
        EXPECT_FALSE(cache::cpu_observation(64, observation));
        EXPECT_EQ(observation.logical, 999U);
        EXPECT_EQ(observation.apic, 888U);
        EXPECT_EQ(observation.status, Status::wrong_cpu);
        for (auto extent : std::array<std::array<uint64_t, 2>, 4>{{{0, 0}, {1, 4096}, {0, 4097}, {UINT64_MAX - 4095, 4096}}}) {
            QualifiedUcRange output;
            EXPECT_EQ(cache::qualify_uc_range(extent[0], extent[1], output), Status::invalid_geometry);
            expect_empty(output);
        }
        QualifiedUcRange output;
        EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, output), Status::unconfigured);
        expect_empty(output);
        EXPECT_EQ(cache::revalidate_current_cpu(output), Status::invalid_configuration);
        EXPECT_EQ(cache::cohort_observation().status, Status::unconfigured);
        EXPECT_EQ(ports::irq_saves.load(), 0U);
    });
}

TEST(MmioCacheRuntime, OneAndSixtyFourCpuBoundariesConfigureCaptureAndSealTheActualRegistry) {
    for (size_t count : {size_t{1}, size_t{64}}) {
        isolated([count] {
            ports::setup(count);
            ASSERT_EQ(cache::configure_boot_cohort(count), Status::ready);
            EXPECT_EQ(cache::configure_boot_cohort(count), Status::ready);
            auto const before = cache::cohort_observation();
            EXPECT_EQ(before.status, Status::pending);
            EXPECT_EQ(before.expected_mask, expected_mask(count));
            EXPECT_EQ(before.terminal_mask | before.ready_mask, 0U);
            capture_all(count);
            QualifiedUcRange range;
            ASSERT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::ready);
            auto const after = cache::cohort_observation();
            EXPECT_EQ(after.status, Status::ready);
            EXPECT_TRUE(after.sealed);
            EXPECT_EQ(after.terminal_mask, expected_mask(count));
            EXPECT_EQ(after.ready_mask, expected_mask(count));
            EXPECT_EQ(after.invalidated_mask, 0U);
            EXPECT_EQ(range.incarnation(), after.incarnation);
            cache::CpuObservation last{};
            ASSERT_TRUE(cache::cpu_observation(count - 1, last));
            EXPECT_EQ(last.logical, count - 1);
            EXPECT_EQ(last.apic, ports::cpus[count - 1].apic);
            ports::select(count - 1);
            EXPECT_EQ(cache::revalidate_current_cpu(range), Status::ready);
        });
    }
}

TEST(MmioCacheRuntime, InvalidZeroAndSixtyFiveConfigurationsPoisonLaterAttemptsPermanently) {
    for (size_t count : {size_t{0}, size_t{65}}) {
        isolated([count] {
            ports::setup(count);
            EXPECT_EQ(cache::configure_boot_cohort(count), Status::invalid_configuration);
            ports::configured.store(1);
            EXPECT_EQ(cache::configure_boot_cohort(1), Status::invalid_configuration);
            EXPECT_EQ(cache::capture_boot_cpu(0), Status::invalid_configuration);
            QualifiedUcRange range;
            EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::invalid_configuration);
            expect_empty(range);
        });
    }
}

TEST(MmioCacheRuntime, ActualCountMismatchAndChangedConfigurationNeverReplaceTheFirstCohort) {
    isolated([] {
        ports::setup(2);
        EXPECT_EQ(cache::configure_boot_cohort(1), Status::invalid_configuration);
        EXPECT_EQ(cache::cohort_observation().expected_mask, 0U);
        EXPECT_EQ(cache::configure_boot_cohort(2), Status::invalid_configuration);
    });
    isolated([] {
        ports::setup(1);
        ASSERT_EQ(cache::configure_boot_cohort(1), Status::ready);
        ports::configured.store(2);
        EXPECT_EQ(cache::configure_boot_cohort(2), Status::invalid_configuration);
        EXPECT_EQ(cache::cohort_observation().expected_mask, 1U);
        ports::configured.store(1);
        EXPECT_EQ(cache::configure_boot_cohort(1), Status::invalid_configuration);
    });
}

TEST(MmioCacheRuntime, PartialCpuPublicationCannotMintTheCohortRange) {
    isolated([] {
        ports::setup(2);
        ASSERT_EQ(cache::configure_boot_cohort(2), Status::ready);
        ASSERT_EQ(cache::capture_boot_cpu(0), Status::ready);
        auto const observed = cache::cohort_observation();
        EXPECT_EQ(observed.status, Status::pending);
        EXPECT_EQ(observed.terminal_mask, 1U);
        EXPECT_EQ(observed.ready_mask, 1U);
        QualifiedUcRange range;
        EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::pending);
        expect_empty(range);
        ports::select(1);
        ASSERT_EQ(cache::capture_boot_cpu(1), Status::ready);
        EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::ready);
    });
}

TEST(MmioCacheRuntime, QueriesCannotReadAReportOrMintWhileItsActualCollectorIsInProgress) {
    isolated([] {
        ports::setup(1);
        ASSERT_EQ(cache::configure_boot_cohort(1), Status::ready);
        std::atomic<bool> entered{false}, release{false};
        ports::cpus[0].entered = &entered;
        ports::cpus[0].release = &release;
        Status result = Status::pending;
        std::thread producer([&] {
            ports::select(0);
            result = cache::capture_boot_cpu(0);
        });
        auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (!entered.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
        EXPECT_TRUE(entered.load(std::memory_order_acquire));
        cache::CpuObservation observation{};
        observation.logical = 555;
        EXPECT_FALSE(cache::cpu_observation(0, observation));
        EXPECT_EQ(observation.logical, 555U);
        EXPECT_EQ(cache::cohort_observation().terminal_mask, 0U);
        QualifiedUcRange range;
        EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::pending);
        expect_empty(range);
        release.store(true, std::memory_order_release);
        producer.join();
        ASSERT_EQ(result, Status::ready);
        EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::ready);
    });
}

TEST(MmioCacheRuntime, ActualIntelAndAmdCollectorsMintOnlyTheObservedOrdinaryUcInterval) {
    for (auto vendor : {arch::Vendor::intel, arch::Vendor::amd}) {
        isolated([vendor] {
            ports::setup(1, vendor);
            capture_all(1);
            cache::CpuObservation report{};
            ASSERT_TRUE(cache::cpu_observation(0, report));
            EXPECT_EQ(report.status, Status::ready);
            EXPECT_EQ(report.raw.mtrr.vendor, vendor);
            EXPECT_EQ(report.raw.mtrr.capability, 0x508U);
            EXPECT_EQ(report.raw.mtrr.default_type, 0xc00U);
            EXPECT_EQ(report.raw.mtrr.observed_masks, 0xffU);
            EXPECT_EQ(report.raw.mtrr.observed_bases, 0U);
            EXPECT_TRUE(report.raw.mtrr.observed_fixed);
            QualifiedUcRange range;
            ASSERT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::ready);
            EXPECT_EQ(range.physical(), ports::BAR);
            EXPECT_EQ(range.bytes(), 4096U);
            EXPECT_EQ(cache::revalidate_current_cpu(range), Status::ready);
            QualifiedUcRange beyond;
            EXPECT_EQ(cache::qualify_uc_range(uint64_t{1} << 36, 4096, beyond), Status::invalid_geometry);
            expect_empty(beyond);
            if (vendor == arch::Vendor::amd) {
                QualifiedUcRange low;
                EXPECT_EQ(cache::qualify_uc_range(0, 4096, low), Status::fixed_routing_unqualified);
                expect_empty(low);
            }
        });
    }
}

TEST(MmioCacheRuntime, PrivateRangeMovesAndNonemptyRefusalPreserveExactBoundsAndIncarnation) {
    isolated([] {
        ports::setup(1);
        capture_all(1);
        QualifiedUcRange source;
        ASSERT_EQ(cache::qualify_uc_range(ports::BAR, 8192, source), Status::ready);
        uint64_t const incarnation = source.incarnation();
        QualifiedUcRange moved(std::move(source));
        expect_empty(source);
        EXPECT_EQ(moved.physical(), ports::BAR);
        EXPECT_EQ(moved.bytes(), 8192U);
        EXPECT_EQ(moved.incarnation(), incarnation);
        EXPECT_EQ(cache::qualify_uc_range(ports::BAR + 4096, 4096, moved), Status::output_not_empty);
        EXPECT_EQ(cache::qualify_uc_range(1, 0, moved), Status::output_not_empty);
        EXPECT_EQ(moved.physical(), ports::BAR);
        EXPECT_EQ(moved.bytes(), 8192U);
        EXPECT_EQ(moved.incarnation(), incarnation);
        QualifiedUcRange assigned;
        assigned = std::move(moved);
        expect_empty(moved);
        EXPECT_EQ(cache::revalidate_current_cpu(assigned), Status::ready);
    });
}

TEST(MmioCacheRuntime, BootIdentityRefusalPublishesATerminalNonreadyCellWithoutReadingCpuRegisters) {
    isolated([] {
        ports::setup(1);
        ports::cpus[0].boot_status = Status::wrong_cpu;
        ASSERT_EQ(cache::configure_boot_cohort(1), Status::ready);
        EXPECT_EQ(cache::capture_boot_cpu(0), Status::wrong_cpu);
        auto const cohort = cache::cohort_observation();
        EXPECT_EQ(cohort.terminal_mask, 1U);
        EXPECT_EQ(cohort.ready_mask, 0U);
        EXPECT_EQ(cohort.status, Status::wrong_cpu);
        EXPECT_TRUE(ports::cpus[0].reads.empty());
        QualifiedUcRange range;
        EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::wrong_cpu);
        expect_empty(range);
    });
}

TEST(MmioCacheRuntime, ChangedEndIdentityCannotPublishTheOtherwiseSuccessfulRawCapture) {
    isolated([] {
        ports::setup(1);
        ports::cpus[0].change_end_apic = true;
        ASSERT_EQ(cache::configure_boot_cohort(1), Status::ready);
        EXPECT_EQ(cache::capture_boot_cpu(0), Status::wrong_cpu);
        EXPECT_EQ(ports::cpus[0].boot_checks, 2U);
        EXPECT_EQ(cache::cohort_observation().ready_mask, 0U);
        QualifiedUcRange range;
        EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::wrong_cpu);
    });
}

TEST(MmioCacheRuntime, MissingUnsupportedOrDifferentInheritedPatPreventsAdmission) {
    for (unsigned mode : {0U, 1U, 2U}) {
        isolated([mode] {
            ports::setup(1);
            if (mode == 0) ports::cpus[0].inherited_present = false;
            if (mode == 1) ports::cpus[0].inherited_supported = false;
            if (mode == 2) ports::cpus[0].inherited_pat ^= 1;
            ASSERT_EQ(cache::configure_boot_cohort(1), Status::ready);
            EXPECT_EQ(cache::capture_boot_cpu(0), Status::inconsistent_policy);
            QualifiedUcRange range;
            EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::inconsistent_policy);
            expect_empty(range);
        });
    }
}

TEST(MmioCacheRuntime, ReadFaultAndUnselectedPrmrrRemainExplicitTerminalRefusals) {
    for (bool prmrr : {false, true}) {
        isolated([prmrr] {
            ports::setup(1);
            if (prmrr)
                ports::cpus[0].msr[0xfe] |= 0x1000;
            else
                ports::cpus[0].faults.insert(0x277);
            Status const expected = prmrr ? Status::protected_ranges_unobservable : Status::read_fault;
            ASSERT_EQ(cache::configure_boot_cohort(1), Status::ready);
            EXPECT_EQ(cache::capture_boot_cpu(0), expected);
            cache::CpuObservation report{};
            ASSERT_TRUE(cache::cpu_observation(0, report));
            EXPECT_EQ(report.status, expected);
            if (!prmrr) EXPECT_EQ(report.raw.failed_msr, 0x277U);
            EXPECT_EQ(ports::cpus[0].reads[0x1f4], 0U);
            EXPECT_EQ(ports::cpus[0].reads[0x2f4], 0U);
            QualifiedUcRange range;
            EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), expected);
        });
    }
}

TEST(MmioCacheRuntime, FullCohortSemanticPatMtrrWidthAndProtectedDifferencesRefuseSealing) {
    for (unsigned mode : {0U, 1U, 2U, 3U}) {
        isolated([mode] {
            ports::setup(2);
            auto& other = ports::cpus[1];
            if (mode == 0) other.inherited_pat = other.msr[0x277] |= uint64_t{6} << 56;
            if (mode == 1) other.msr[0x2ff] |= 6;
            if (mode == 2) other.cpuid[{0x80000008U, 0}].eax = 32;
            if (mode == 3) {
                other.msr[0xfe] |= 0x800;
                other.msr[0x1f3] = 0xfff00800;
                other.msr[0x1f2] = 0x30000006;
            }
            capture_all(2);
            QualifiedUcRange range;
            EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::inconsistent_policy);
            expect_empty(range);
            EXPECT_FALSE(cache::cohort_observation().sealed);
            EXPECT_EQ(cache::cohort_observation().status, Status::inconsistent_policy);
            EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::inconsistent_policy);
        });
    }
}

TEST(MmioCacheRuntime, DuplicateApicIdentitiesCannotSealEvenWhenBothCapturePortsReturnReady) {
    isolated([] {
        ports::setup(2);
        ports::cpus[1].apic = ports::cpus[0].apic;
        capture_all(2);
        QualifiedUcRange range;
        EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::wrong_cpu);
        expect_empty(range);
    });
}

TEST(MmioCacheRuntime, DuplicateCaptureBeforeAndAfterSealPermanentlyInvalidatesExistingAndNewRanges) {
    for (bool after_seal : {false, true}) {
        isolated([after_seal] {
            ports::setup(1);
            capture_all(1);
            QualifiedUcRange original;
            if (after_seal) ASSERT_EQ(cache::qualify_uc_range(ports::BAR, 4096, original), Status::ready);
            EXPECT_EQ(cache::capture_boot_cpu(0), Status::duplicate_capture);
            auto const observation = cache::cohort_observation();
            EXPECT_EQ(observation.status, Status::duplicate_capture);
            EXPECT_EQ(observation.invalidated_mask, 1U);
            EXPECT_EQ(cache::configure_boot_cohort(1), Status::duplicate_capture);
            if (after_seal) EXPECT_EQ(cache::revalidate_current_cpu(original), Status::duplicate_capture);
            QualifiedUcRange next;
            EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, next), Status::duplicate_capture);
            expect_empty(next);
        });
    }
}

TEST(MmioCacheRuntime, WrongUnknownOrOutOfRangeCurrentIdentityPoisonsAdmissionWithoutCpuFallback) {
    for (unsigned mode : {0U, 1U, 2U}) {
        isolated([mode] {
            ports::setup(1);
            capture_all(1);
            QualifiedUcRange range;
            ASSERT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::ready);
            if (mode == 0) ports::local_apic += 1;
            if (mode == 1) ports::cpus[0].current_status = Status::wrong_cpu;
            if (mode == 2) ports::select(64);
            EXPECT_EQ(cache::revalidate_current_cpu(range), Status::wrong_cpu);
            ports::select(0);
            ports::cpus[0].current_status = Status::ready;
            EXPECT_EQ(cache::revalidate_current_cpu(range), Status::wrong_cpu);
            QualifiedUcRange next;
            EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, next), Status::wrong_cpu);
            expect_empty(next);
        });
    }
}

TEST(MmioCacheRuntime, ReadValueRelevantControlAndSmmDriftPermanentlyPoisonsTheActualCohort) {
    for (unsigned mode : {0U, 1U, 2U, 3U, 4U, 5U, 6U, 7U}) {
        isolated([mode] {
            ports::setup(1, mode == 6 ? arch::Vendor::amd : arch::Vendor::intel);
            capture_all(1);
            QualifiedUcRange range;
            ASSERT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::ready);
            auto const original = ports::cpus[0];
            auto& cpu = ports::cpus[0];
            if (mode == 0) cpu.faults.insert(0x277);
            if (mode == 1) cpu.msr[0x277] ^= 1;
            if (mode == 2) cpu.msr[0xfe] ^= 1;
            if (mode == 3) cpu.msr[0x2ff] ^= 1;
            if (mode == 4) cpu.cr0 ^= uint64_t{1} << 30;
            if (mode == 5) cpu.cr4 ^= uint64_t{1} << 7;
            if (mode == 6) cpu.msr[0xc0010113U] = 2;
            if (mode == 7) cpu.cpuid[{7, 0}].ecx ^= uint32_t{1} << 13;
            Status const expected = mode == 0 ? Status::read_fault : Status::state_drift;
            EXPECT_EQ(cache::revalidate_current_cpu(range), expected);
            EXPECT_EQ(cache::cohort_observation().invalidated_mask, 1U);
            if (mode == 6) EXPECT_EQ(cpu.reads[0xc0010112U], 0U);
            if (mode == 7) EXPECT_EQ(cpu.reads[0x982], 0U);
            ports::cpus[0] = original;
            EXPECT_EQ(cache::revalidate_current_cpu(range), expected);
            QualifiedUcRange next;
            EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, next), expected);
            expect_empty(next);
        });
    }
}

TEST(MmioCacheRuntime, TaskSwitchBitsAndTheirDynamicCpuidMirrorsPreserveTheQualifiedPolicy) {
    isolated([] {
        ports::setup(1);
        capture_all(1);
        QualifiedUcRange range;
        ASSERT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::ready);
        auto& cpu = ports::cpus[0];
        cpu.cr0 ^= uint64_t{1} << 3;
        cpu.cr4 |= (uint64_t{1} << 4) | (uint64_t{1} << 18) | (uint64_t{1} << 22);
        cpu.cpuid[{1, 0}].ecx |= uint32_t{1} << 27;
        cpu.cpuid[{7, 0}].ecx |= uint32_t{1} << 4;
        EXPECT_EQ(cache::revalidate_current_cpu(range), Status::ready);
        EXPECT_EQ(cache::cohort_observation().invalidated_mask, 0U);
        QualifiedUcRange next;
        EXPECT_EQ(cache::qualify_uc_range(ports::BAR + 4096, 4096, next), Status::ready);
        cache::CpuObservation immutable{};
        ASSERT_TRUE(cache::cpu_observation(0, immutable));
        EXPECT_EQ(immutable.raw.cr4, 0x20U);
        EXPECT_EQ(immutable.raw.features.ecx & (uint32_t{1} << 27), 0U);
    });
}

TEST(MmioCacheRuntime, CaptureAndBothSuccessfulAndFailedRevalidationRestoreEachIncomingIfState) {
    for (uint64_t incoming : {uint64_t{2}, uint64_t{2} | ports::IF}) {
        isolated([incoming] {
            ports::setup(1);
            ASSERT_EQ(cache::configure_boot_cohort(1), Status::ready);
            ports::select(0, incoming);
            ASSERT_EQ(cache::capture_boot_cpu(0), Status::ready);
            EXPECT_EQ(ports::flags, incoming);
            EXPECT_EQ(ports::irq_depth, 0U);
            QualifiedUcRange range;
            ASSERT_EQ(cache::qualify_uc_range(ports::BAR, 4096, range), Status::ready);
            EXPECT_EQ(cache::revalidate_current_cpu(range), Status::ready);
            EXPECT_EQ(ports::flags, incoming);
            ports::cpus[0].faults.insert(0x277);
            EXPECT_EQ(cache::revalidate_current_cpu(range), Status::read_fault);
            EXPECT_EQ(ports::flags, incoming);
            EXPECT_EQ(ports::irq_depth, 0U);
            EXPECT_EQ(ports::irq_saves.load(), 3U);
            EXPECT_EQ(ports::irq_restores.load(), 3U);
        });
    }
}

TEST(MmioCacheRuntime, ConcurrentDistinctCpuCaptureAndTokenQueriesUseActualPublicationAndSealStates) {
    isolated([] {
        constexpr size_t COUNT = 4;
        ports::setup(COUNT);
        ASSERT_EQ(cache::configure_boot_cohort(COUNT), Status::ready);
        std::atomic<bool> start{false};
        std::atomic<size_t> done{0};
        std::array<Status, COUNT> results{};
        std::array<std::thread, COUNT> producers;
        for (size_t i = 0; i < COUNT; ++i) {
            producers[i] = std::thread([&, i] {
                ports::select(i);
                while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
                results[i] = cache::capture_boot_cpu(i);
                done.fetch_add(1, std::memory_order_acq_rel);
            });
        }
        start.store(true, std::memory_order_release);
        auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        size_t queries = 0;
        do {
            QualifiedUcRange range;
            auto const result = cache::qualify_uc_range(ports::BAR, 4096, range);
            EXPECT_TRUE(result == Status::pending || result == Status::busy || result == Status::ready);
            if (result == Status::ready) {
                EXPECT_FALSE(range.empty());
                EXPECT_EQ(range.physical(), ports::BAR);
            } else
                expect_empty(range);
            ++queries;
            std::this_thread::yield();
        } while (done.load(std::memory_order_acquire) != COUNT && std::chrono::steady_clock::now() < deadline);
        for (auto& producer : producers) producer.join();
        EXPECT_GE(queries, 1U);
        for (auto result : results) EXPECT_EQ(result, Status::ready);
        QualifiedUcRange final;
        EXPECT_EQ(cache::qualify_uc_range(ports::BAR, 4096, final), Status::ready);
        EXPECT_EQ(cache::cohort_observation().terminal_mask, 15U);
        EXPECT_EQ(cache::cohort_observation().ready_mask, 15U);
    });
}
}  // namespace
