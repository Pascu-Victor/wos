#include <array>
#include <atomic>
#include <platform/dbg/dbg.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/cache_policy.hpp>
#include <platform/mm/mmio_cache.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <string_view>
#include <test/cache_policy_runtime_ktest.hpp>
#include <test/ktest.hpp>
#include <utility>

namespace ker::test {
namespace {
namespace cache = ker::mod::mm::cache;
namespace mmio = ker::mod::mm::mmio_cache;
namespace sched = ker::mod::sched;
namespace task = sched::task;
namespace virt = ker::mod::mm::virt;
using log = ker::mod::dbg::logger<"cache-test">;
std::atomic<bool> started{false};

void require(bool condition, const char* reason) {
    if (condition) return;
    log::error("CACHE_POLICY_RUNTIME FAIL %s", reason);
    ker::mod::dbg::panic_handler("scheduled cache policy selftest failed");
}

auto wait_for_reports() -> cache::PatQualification {
    uint64_t const CPUS = ker::mod::smt::get_core_count();
    require(CPUS != 0 && CPUS <= 64, "bounded actual CPU topology");
    uint64_t const EXPECTED = CPUS == 64 ? UINT64_MAX : (uint64_t{1} << CPUS) - 1;
    auto const DEADLINE = sched::saturating_deadline_us(ker::mod::time::get_us(), 10'000'000);
    for (;;) {
        auto const value = cache::qualification();
        require(value.configured && value.expected_mask == EXPECTED, "configured reports match actual CPU topology");
        require((value.reported_mask & ~EXPECTED) == 0, "no unexpected CPU report");
        if (value.reported_mask == EXPECTED) {
            require(value.complete, "all reports collected without duplicate or invalid-input poison");
            return value;
        }
        require(ker::mod::time::get_us() < DEADLINE, "bounded wait for actual AP reports");
        sched::kern_sleep_us(1000);
    }
}

void wait_for_init_mounts() {
    auto const DEADLINE = sched::saturating_deadline_us(ker::mod::time::get_us(), 30'000'000);
    for (;;) {
        auto* init = sched::find_task_by_pid_safe(1);
        if (init != nullptr) {
            task::Task::FilesystemContext context;
            bool const READY = init->capture_filesystem_context(context) && context.root_len == 7 &&
                               std::string_view(context.root.data(), context.root_len) == "/rootfs";
            init->release();
            if (READY) {
                log::info("CACHE_POLICY_RUNTIME INIT_READY root=/rootfs");
                return;
            }
        }
        require(ker::mod::time::get_us() < DEADLINE, "PID 1 finishes actual root mount/pivot before shutdown");
        sched::kern_sleep_us(10000);
    }
}

void observations() {
    auto const PAT = wait_for_reports();
    std::array<cache::CpuPat, 64> captured{};
    bool wb = true, uc = true, wc = true;
    uint64_t mismatch = 0;
    size_t cpus = 0;
    for (size_t i = 0; i < captured.size(); ++i) {
        if ((PAT.expected_mask & (uint64_t{1} << i)) == 0) continue;
        auto& value = captured.at(i);
        require(cache::cpu_pat(i, value), "each published CPU has a readable immutable observation");
        bool const WB = !value.supported || (value.raw & 255U) == 6;
        bool const UC = !value.supported || ((value.raw >> 24U) & 255U) == 0;
        bool const WC = value.supported && ((value.raw >> 40U) & 255U) == 1;
        if (value.supported && (!WB || !UC || !WC)) mismatch |= uint64_t{1} << i;
        wb &= WB;
        uc &= UC;
        wc &= WC;
        ++cpus;
        log::info("CACHE_POLICY_RUNTIME CPU cpu=%lu supported=%u pat=%016lx wb0=%u uc3=%u wc5=%u", i, value.supported ? 1U : 0U, value.raw,
                  WB ? 1U : 0U, UC ? 1U : 0U, WC ? 1U : 0U);
    }
    wc &= wb && uc;
    require(PAT.mismatch_mask == mismatch && PAT.wb_encoding == wb && PAT.uc_encoding == uc && PAT.wc_encoding == wc,
            "aggregate encodings match independent per-CPU observations");
    log::info("CACHE_POLICY_RUNTIME PAT expected=%016lx reported=%016lx mismatch=%016lx complete=%u wb=%u uc=%u wc=%u", PAT.expected_mask,
              PAT.reported_mask, PAT.mismatch_mask, PAT.complete ? 1U : 0U, wb ? 1U : 0U, uc ? 1U : 0U, wc ? 1U : 0U);
    log::info("CACHE_POLICY_RUNTIME CASE_PASS actual_bsp_ap_pat");

    auto const FB = virt::boot_framebuffer_cache_audit();
    require(FB.complete && FB.inherited_hash == FB.kernel_hash, "boot source/target framebuffer cache identities match");
    require((FB.pages == 0) == (FB.selector_mask == 0), "framebuffer page coverage and selector presence agree");
    log::info("CACHE_POLICY_RUNTIME FRAMEBUFFER pages=%lu selectors=0x%x source=%016lx target=%016lx complete=%u coverage=%s", FB.pages,
              static_cast<unsigned>(FB.selector_mask), FB.inherited_hash, FB.kernel_hash, FB.complete ? 1U : 0U,
              FB.pages != 0 ? "present" : "absent");
    log::info("CACHE_POLICY_RUNTIME CASE_PASS inherited_framebuffer_selectors");

    sched::kern_sleep_us(1000);
    auto const AFTER_PAT = cache::qualification();
    auto const AFTER_FB = virt::boot_framebuffer_cache_audit();
    require(AFTER_PAT.expected_mask == PAT.expected_mask && AFTER_PAT.reported_mask == PAT.reported_mask &&
                AFTER_PAT.mismatch_mask == PAT.mismatch_mask && AFTER_PAT.configured == PAT.configured &&
                AFTER_PAT.complete == PAT.complete && AFTER_PAT.wb_encoding == PAT.wb_encoding &&
                AFTER_PAT.uc_encoding == PAT.uc_encoding && AFTER_PAT.wc_encoding == PAT.wc_encoding,
            "CPU qualification remains immutable across ordinary scheduling");
    require(AFTER_FB.pages == FB.pages && AFTER_FB.selector_mask == FB.selector_mask && AFTER_FB.inherited_hash == FB.inherited_hash &&
                AFTER_FB.kernel_hash == FB.kernel_hash && AFTER_FB.complete == FB.complete,
            "framebuffer audit remains immutable across ordinary scheduling");
    for (size_t i = 0; i < captured.size(); ++i) {
        if ((PAT.expected_mask & (uint64_t{1} << i)) == 0) continue;
        cache::CpuPat value{};
        require(cache::cpu_pat(i, value) && value.supported == captured.at(i).supported && value.raw == captured.at(i).raw,
                "per-CPU raw observations remain unchanged");
    }
    log::info("CACHE_POLICY_RUNTIME CASE_PASS immutable_boot_observations");
    log::info("CACHE_POLICY_RUNTIME PASS cases=3 cpus=%lu framebuffer_pages=%lu wb=%u uc=%u wc=%u encoding_observation_only=1", cpus,
              FB.pages, wb ? 1U : 0U, uc ? 1U : 0U, wc ? 1U : 0U);
}

auto has_cmdline_token(std::string_view token) -> bool {
    const char* const CMDLINE = ker::init::get_kernel_cmdline();
    if (CMDLINE == nullptr || token.empty()) return false;
    std::string_view remaining{CMDLINE};
    while (!remaining.empty()) {
        size_t const BEGIN = remaining.find_first_not_of(" \t\r\n");
        if (BEGIN == std::string_view::npos) return false;
        remaining.remove_prefix(BEGIN);
        size_t const END = remaining.find_first_of(" \t\r\n");
        if (remaining.substr(0, END) == token) return true;
        if (END == std::string_view::npos) return false;
        remaining.remove_prefix(END);
    }
    return false;
}

class ScalarHash {
    uint64_t value_{1469598103934665603ULL};

   public:
    template <typename T>
    void add(T value) {
        uint64_t bits = static_cast<uint64_t>(value);
        for (unsigned byte = 0; byte < 8; ++byte) {
            value_ = (value_ ^ (bits & 255U)) * 1099511628211ULL;
            bits >>= 8;
        }
    }
    void add(const mmio::detail::Register& value) {
        add(value.observation);
        add(value.value);
    }
    void add(const mmio::detail::Cpuid& value) {
        add(value.eax);
        add(value.ebx);
        add(value.ecx);
        add(value.edx);
    }
    [[nodiscard]] auto value() const -> uint64_t { return value_; }
};

auto raw_scalar_hash(const mmio::detail::RawCpu& value) -> uint64_t {
    ScalarHash hash;
    hash.add(value.mtrr.vendor);
    hash.add(value.mtrr.physical_bits);
    hash.add(value.mtrr.capability);
    hash.add(value.mtrr.default_type);
    hash.add(value.mtrr.observed_masks);
    hash.add(value.mtrr.observed_bases);
    hash.add(value.mtrr.observed_fixed);
    for (const auto& pair : value.mtrr.variable) {
        hash.add(pair.base);
        hash.add(pair.mask);
    }
    for (auto fixed : value.mtrr.fixed) hash.add(fixed);
    for (const auto& leaf :
         {value.basic, value.features, value.structured, value.extended, value.long_mode, value.address_width, value.encryption})
        hash.add(leaf);
    hash.add(value.cr0);
    hash.add(value.cr4);
    hash.add(value.pat);
    hash.add(value.tme_activate);
    hash.add(value.amd.syscfg);
    hash.add(value.amd.top_mem);
    hash.add(value.amd.top_mem2);
    for (auto reg : value.amd.iorr_base) hash.add(reg);
    for (auto reg : value.amd.iorr_mask) hash.add(reg);
    hash.add(value.amd.encryption_eax);
    hash.add(value.amd.encryption_ebx);
    hash.add(value.amd.sev_status);
    hash.add(value.intel_protected.capability);
    hash.add(value.intel_protected.smrr_base);
    hash.add(value.intel_protected.smrr_mask);
    hash.add(value.intel_protected.seamrr_base);
    hash.add(value.intel_protected.seamrr_mask);
    hash.add(value.amd_protected.smm_base);
    hash.add(value.amd_protected.smm_mask);
    hash.add(value.amd_protected.signature);
    hash.add(value.failed_msr);
    return hash.value();
}

auto observation_scalar_hash(const mmio::CpuObservation& observation, uint64_t raw_hash) -> uint64_t {
    ScalarHash hash;
    hash.add(raw_hash);
    hash.add(observation.logical);
    hash.add(observation.apic);
    hash.add(observation.status);
    auto const& value = observation.policy;
    hash.add(value.mtrr.vendor);
    hash.add(value.mtrr.physical_bits);
    hash.add(value.mtrr.address_ceiling);
    hash.add(value.mtrr.enabled);
    hash.add(value.mtrr.fixed_enabled);
    hash.add(value.mtrr.wc_supported);
    hash.add(value.mtrr.default_type);
    hash.add(value.mtrr.variable_count);
    for (const auto& range : value.mtrr.variable) {
        hash.add(range.begin);
        hash.add(range.end);
        hash.add(range.type);
    }
    for (auto fixed : value.mtrr.fixed) hash.add(fixed);
    hash.add(value.pat);
    hash.add(value.cache_cr0);
    hash.add(value.cache_cr4);
    hash.add(value.amd.fixed_routing);
    hash.add(value.amd.fixed_routing_visible);
    hash.add(value.amd.top_mem_enabled);
    hash.add(value.amd.top_mem2_enabled);
    hash.add(value.amd.force_wb_default);
    hash.add(value.amd.top_mem);
    hash.add(value.amd.top_mem2);
    hash.add(value.amd.iorr_count);
    for (const auto& range : value.amd.iorr) {
        hash.add(range.begin);
        hash.add(range.end);
        hash.add(range.read_memory);
        hash.add(range.write_memory);
    }
    hash.add(value.protected_ranges.count);
    for (const auto& range : value.protected_ranges.ranges) {
        hash.add(range.begin);
        hash.add(range.end);
        hash.add(range.forced_type);
        hash.add(range.kind);
    }
    return hash.value();
}

auto observation_name(mmio::detail::Observation value) -> const char* {
    switch (value) {
        case mmio::detail::Observation::dormant:
            return "dormant";
        case mmio::detail::Observation::value:
            return "value";
        case mmio::detail::Observation::fault:
            return "fault";
    }
    return "invalid";
}

auto wait_for_cache_cohort(uint64_t expected) -> mmio::CohortObservation {
    auto const DEADLINE = sched::saturating_deadline_us(ker::mod::time::get_us(), 10'000'000);
    for (size_t attempt = 0; attempt < 10'000; ++attempt) {
        auto const value = mmio::cohort_observation();
        require(value.expected_mask == expected && value.incarnation != 0, "actual cache cohort matches configured CPUs");
        require((value.terminal_mask & ~expected) == 0 && (value.ready_mask & ~expected) == 0,
                "cache observation masks contain only configured CPUs");
        if (value.terminal_mask == expected) return value;
        require(ker::mod::time::get_us() < DEADLINE, "bounded wait for terminal cache reports");
        sched::kern_sleep_us(1000);
    }
    require(false, "finite iteration bound for terminal cache reports");
    return {};
}

void log_cache_cpu(const mmio::CpuObservation& value, uint64_t raw_hash, uint64_t report_hash) {
    const auto& raw = value.raw;
    log::info(
        "MMIO_CACHE_RUNTIME CPU cpu=%u apic=%u status=%s vendor=%u width=%u cap=%016lx def=%016lx pat=%016lx "
        "cr0=%016lx cr4=%016lx failed_msr=%08x raw_hash=%016lx report_hash=%016lx",
        value.logical, value.apic, mmio::status_name(value.status), static_cast<unsigned>(raw.mtrr.vendor),
        static_cast<unsigned>(raw.mtrr.physical_bits), raw.mtrr.capability, raw.mtrr.default_type, raw.pat, raw.cr0, raw.cr4,
        raw.failed_msr, raw_hash, report_hash);
    log::info(
        "MMIO_CACHE_RUNTIME AMD cpu=%u syscfg=%016lx/%s top=%016lx/%s top2=%016lx/%s sev=%016lx/%s "
        "smm_base=%016lx/%s smm_mask=%016lx/%s signature=%08x",
        value.logical, raw.amd.syscfg.value, observation_name(raw.amd.syscfg.observation), raw.amd.top_mem.value,
        observation_name(raw.amd.top_mem.observation), raw.amd.top_mem2.value, observation_name(raw.amd.top_mem2.observation),
        raw.amd.sev_status.value, observation_name(raw.amd.sev_status.observation), raw.amd_protected.smm_base.value,
        observation_name(raw.amd_protected.smm_base.observation), raw.amd_protected.smm_mask.value,
        observation_name(raw.amd_protected.smm_mask.observation), raw.amd_protected.signature);
    log::info(
        "MMIO_CACHE_RUNTIME INTEL cpu=%u tme=%016lx/%s smrr_base=%016lx/%s smrr_mask=%016lx/%s "
        "seamrr_base=%016lx/%s seamrr_mask=%016lx/%s",
        value.logical, raw.tme_activate.value, observation_name(raw.tme_activate.observation), raw.intel_protected.smrr_base.value,
        observation_name(raw.intel_protected.smrr_base.observation), raw.intel_protected.smrr_mask.value,
        observation_name(raw.intel_protected.smrr_mask.observation), raw.intel_protected.seamrr_base.value,
        observation_name(raw.intel_protected.seamrr_base.observation), raw.intel_protected.seamrr_mask.value,
        observation_name(raw.intel_protected.seamrr_mask.observation));
    for (size_t slot = 0; slot < raw.amd.iorr_base.size(); ++slot)
        log::info("MMIO_CACHE_RUNTIME IORR cpu=%u slot=%lu base=%016lx/%s mask=%016lx/%s", value.logical, slot,
                  raw.amd.iorr_base[slot].value, observation_name(raw.amd.iorr_base[slot].observation), raw.amd.iorr_mask[slot].value,
                  observation_name(raw.amd.iorr_mask[slot].observation));
    require(value.policy.protected_ranges.count <= value.policy.protected_ranges.ranges.size(), "bounded protected observation count");
    for (size_t slot = 0; slot < value.policy.protected_ranges.count; ++slot) {
        const auto& range = value.policy.protected_ranges.ranges[slot];
        log::info("MMIO_CACHE_RUNTIME PROTECTED cpu=%u slot=%lu begin=%016lx end=%016lx kind=%u type=%u diagnostic_only=1", value.logical,
                  slot, range.begin, range.end, static_cast<unsigned>(range.kind), static_cast<unsigned>(range.forced_type));
    }
}

void require_empty(const mmio::QualifiedUcRange& range, const char* reason) {
    require(range.empty() && range.physical() == 0 && range.bytes() == 0 && range.incarnation() == 0, reason);
}

void require_same_cohort(const mmio::CohortObservation& before, const mmio::CohortObservation& after) {
    require(before.status == after.status && before.expected_mask == after.expected_mask && before.terminal_mask == after.terminal_mask &&
                before.ready_mask == after.ready_mask && before.invalidated_mask == after.invalidated_mask &&
                before.incarnation == after.incarnation && before.sealed == after.sealed,
            "sealed/refused cohort observation remains stable across scheduling");
}

void cache_state_observations() {
    bool const REFUSAL = has_cmdline_token("--selftest-cache-state-refusal");
    require(!REFUSAL || has_cmdline_token("--selftest-cache-policy"), "cache-state refusal modifier requires its scheduled base mode");
    uint64_t const CPUS = ker::mod::smt::get_core_count();
    require(CPUS != 0 && CPUS <= 64, "bounded actual cache CPU cohort");
    uint64_t const EXPECTED = CPUS == 64 ? UINT64_MAX : (uint64_t{1} << CPUS) - 1;
    log::info("MMIO_CACHE_RUNTIME BEGIN cpus=%lu refusal_profile=%u cpu_policy_only=1", CPUS, REFUSAL ? 1U : 0U);
    auto const COHORT = wait_for_cache_cohort(EXPECTED);
    std::array<uint64_t, 64> raw_hashes{}, report_hashes{};
    std::array<uint32_t, 64> apics{};
    size_t ready = 0;
    uint64_t ceiling = 0;
    for (size_t cpu = 0; cpu < CPUS; ++cpu) {
        mmio::CpuObservation value{}, copy{};
        require(mmio::cpu_observation(cpu, value), "terminal CPU observation is available by copy");
        require(value.logical == cpu && value.apic == ker::mod::smt::get_cpu(cpu).lapic_id, "actual logical/APIC capture matches SMT");
        for (size_t previous = 0; previous < cpu; ++previous) require(apics[previous] != value.apic, "unique actual APIC capture identity");
        apics[cpu] = value.apic;
        raw_hashes[cpu] = raw_scalar_hash(value.raw);
        report_hashes[cpu] = observation_scalar_hash(value, raw_hashes[cpu]);
        require(mmio::cpu_observation(cpu, copy) && raw_scalar_hash(copy.raw) == raw_hashes[cpu] &&
                    observation_scalar_hash(copy, raw_hashes[cpu]) == report_hashes[cpu],
                "separate copied getters preserve explicit scalar identities");
        log_cache_cpu(value, raw_hashes[cpu], report_hashes[cpu]);
        if (value.status == mmio::Status::ready) ++ready;
        if (REFUSAL) require(value.status == mmio::Status::missing_features, "negative CPU profile has an actual missing-feature report");
        if (cpu == 0) ceiling = value.policy.mtrr.address_ceiling;
    }
    require(COHORT.invalidated_mask == 0, "no duplicate, identity or state-drift invalidation");
    if (REFUSAL)
        require(ready == 0 && COHORT.ready_mask == 0 && COHORT.status == mmio::Status::missing_features && !COHORT.sealed,
                "unsupported CPU profile is terminal without ready credit");
    else
        require(ready == CPUS && COHORT.ready_mask == EXPECTED && COHORT.status == mmio::Status::ready,
                "positive CPU profile requires every real report ready");

    constexpr uint64_t PAGE = ker::mod::mm::cache_arch::PAGE_BYTES;
    for (auto geometry : {std::array<uint64_t, 2>{0, 0}, {1, PAGE}, {0, PAGE - 1}, {UINT64_MAX - PAGE + 1, 2 * PAGE}}) {
        mmio::QualifiedUcRange empty;
        require(mmio::qualify_uc_range(geometry[0], geometry[1], empty) == mmio::Status::invalid_geometry,
                "actual qualifier rejects zero, unaligned and overflowing geometry");
        require_empty(empty, "geometry refusal cannot mint a token");
    }

    std::array<uint64_t, 5> candidates{0xfee00000, 0xfed00000, uint64_t{1} << 32, 0x80000000, ceiling >= PAGE ? ceiling - PAGE : 0};
    size_t const CANDIDATES = ceiling >= PAGE ? candidates.size() : candidates.size() - 1;
    if (ceiling < PAGE) log::info("MMIO_CACHE_RUNTIME CANDIDATE_SKIP kind=ceiling reason=no_decoded_geometry");
    size_t qualified = 0;
    mmio::QualifiedUcRange sample;
    for (size_t candidate = 0; candidate < CANDIDATES; ++candidate) {
        uint64_t const PHYSICAL = candidates[candidate];
        mmio::QualifiedUcRange range;
        auto const STATUS = mmio::qualify_uc_range(PHYSICAL, PAGE, range);
        log::info("MMIO_CACHE_RUNTIME CANDIDATE physical=%016lx bytes=%lu status=%s token=%u incarnation=%lu cpu_policy_only=1", PHYSICAL,
                  PAGE, mmio::status_name(STATUS), range.empty() ? 0U : 1U, range.incarnation());
        if (STATUS != mmio::Status::ready) {
            require_empty(range, "candidate refusal preserves an empty token");
            if (REFUSAL) require(STATUS == mmio::Status::missing_features, "unsupported CPU profile cannot qualify a candidate");
            continue;
        }
        require(!REFUSAL, "refusal profile cannot receive strict UC credit");
        require(!range.empty() && range.physical() == PHYSICAL && range.bytes() == PAGE && range.incarnation() == COHORT.incarnation,
                "private qualifier binds exact geometry and actual cohort incarnation");
        mmio::QualifiedUcRange moved{std::move(range)};
        require_empty(range, "move construction clears the source token");
        mmio::QualifiedUcRange assigned;
        assigned = std::move(moved);
        require_empty(moved, "move assignment clears the source token");
        require(!assigned.empty() && assigned.physical() == PHYSICAL && assigned.bytes() == PAGE &&
                    assigned.incarnation() == COHORT.incarnation,
                "moves preserve the exact privately minted token");
        require(mmio::qualify_uc_range(PHYSICAL, PAGE, assigned) == mmio::Status::output_not_empty && assigned.physical() == PHYSICAL &&
                    assigned.bytes() == PAGE && assigned.incarnation() == COHORT.incarnation,
                "nonempty output refuses replacement without changing its authority");
        bool const BEFORE_IF = sched::interrupts_enabled();
        auto const CURRENT = mmio::revalidate_current_cpu(assigned);
        bool const AFTER_IF = sched::interrupts_enabled();
        require(CURRENT == mmio::Status::ready && BEFORE_IF && AFTER_IF == BEFORE_IF,
                "actual scheduled current-CPU revalidation restores incoming IRQ admission");
        ++qualified;
        if (sample.empty()) sample = std::move(assigned);
    }
    if (REFUSAL) {
        require(qualified == 0, "missing features never count as UC qualification");
        require_empty(sample, "refusal profile retains no sample capability");
    } else {
        require(qualified != 0 && !sample.empty(), "positive fixture requires a real privately minted UC range");
        mmio::QualifiedUcRange empty;
        require(mmio::qualify_uc_range(ceiling, PAGE, empty) == mmio::Status::invalid_geometry,
                "actual observed CPU address ceiling is exclusive");
        require_empty(empty, "address-ceiling refusal cannot mint a token");
    }

    auto const SEALED = mmio::cohort_observation();
    require(SEALED.expected_mask == EXPECTED && SEALED.terminal_mask == EXPECTED && SEALED.invalidated_mask == 0 &&
                SEALED.incarnation == COHORT.incarnation &&
                (REFUSAL ? !SEALED.sealed && SEALED.ready_mask == 0 && SEALED.status == mmio::Status::missing_features
                         : SEALED.sealed && SEALED.ready_mask == EXPECTED && SEALED.status == mmio::Status::ready),
            "only a completely qualified cohort can seal");
    sched::kern_sleep_us(1000);
    require_same_cohort(SEALED, mmio::cohort_observation());
    for (size_t cpu = 0; cpu < CPUS; ++cpu) {
        mmio::CpuObservation value{};
        require(mmio::cpu_observation(cpu, value) && value.logical == cpu && value.apic == apics[cpu] &&
                    value.apic == ker::mod::smt::get_cpu(cpu).lapic_id && raw_scalar_hash(value.raw) == raw_hashes[cpu] &&
                    observation_scalar_hash(value, raw_hashes[cpu]) == report_hashes[cpu],
                "every CPU's explicit raw/decoded scalar observation remains immutable across scheduling");
    }
    if (!sample.empty()) {
        bool const BEFORE_IF = sched::interrupts_enabled();
        auto const CURRENT = mmio::revalidate_current_cpu(sample);
        bool const AFTER_IF = sched::interrupts_enabled();
        require(CURRENT == mmio::Status::ready && BEFORE_IF && AFTER_IF == BEFORE_IF,
                "genuine token revalidates after scheduling with IRQ restoration");
        require_same_cohort(SEALED, mmio::cohort_observation());
    }
    if (REFUSAL)
        log::info("MMIO_CACHE_RUNTIME REFUSAL_PASS cpus=%lu ready=0 qualified=0 reason=missing_features cpu_policy_only=1", CPUS);
    else
        log::info("MMIO_CACHE_RUNTIME PASS cpus=%lu qualified=%lu incarnation=%lu cpu_policy_only=1", CPUS, qualified, SEALED.incarnation);
}

void runtime_entry() {
    {
        auto const HOLD = sched::hold_kernel_shutdown();
        require(ker::mod::mm::phys::can_wait_for_reclaim(), "ordinary scheduled test context");
        log::info("CACHE_POLICY_RUNTIME BEGIN");
        observations();
        cache_state_observations();
        wait_for_init_mounts();
    }
    {
        auto const HOLD = sched::hold_kernel_shutdown();
        auto* init = sched::find_task_by_pid_safe(1);
        require(init != nullptr, "retain PID 1 for normal userspace shutdown request");
        constexpr int WOS_SIGUSR2 = 12;
        init->signal_generate_pending_mask(uint64_t{1} << (WOS_SIGUSR2 - 1));
        sched::wake_task_for_signal(init);
        init->release();
        log::info("CACHE_POLICY_RUNTIME POWEROFF_REQUESTED via=init-sigusr2");
    }
    sched::exit_current_kernel_thread();
}
}  // namespace

KTEST(CachePolicy, ReadOnlyBootReportsPublishedBeforeEarlyTests) {
    auto const PAT = cache::qualification();
    KEXPECT_TRUE(PAT.configured && PAT.complete);
    KEXPECT_NE(PAT.expected_mask, 0U);
    KEXPECT_EQ(PAT.reported_mask, PAT.expected_mask);
    KEXPECT_TRUE(!PAT.wc_encoding || (PAT.wb_encoding && PAT.uc_encoding));
    for (size_t i = 0; i < 64; ++i) {
        if ((PAT.expected_mask & (uint64_t{1} << i)) == 0) continue;
        cache::CpuPat value{};
        KEXPECT_TRUE(cache::cpu_pat(i, value));
        if (!value.supported) KEXPECT_FALSE(PAT.wc_encoding);
    }
    auto const FB = virt::boot_framebuffer_cache_audit();
    KEXPECT_TRUE(FB.complete);
    KEXPECT_EQ(FB.inherited_hash, FB.kernel_hash);
    KEXPECT_EQ(FB.pages == 0, FB.selector_mask == 0);
}

auto start_cache_policy_runtime_selftest() -> bool {
    bool expected = false;
    if (!started.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return false;
    auto* worker = task::Task::create_kernel_thread("cache_policy_runtime_selftest", runtime_entry);
    if (worker == nullptr || !sched::post_task_balanced(worker)) {
        if (worker != nullptr) task::destroy_unpublished_user_thread(worker);
        started.store(false, std::memory_order_release);
        return false;
    }
    return true;
}
}  // namespace ker::test
