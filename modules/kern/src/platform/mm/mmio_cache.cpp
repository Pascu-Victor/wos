#include "mmio_cache.hpp"

#include <array>
#include <atomic>
#include <platform/mm/cache_policy.hpp>

#include "mmio_cache_platform.hpp"

namespace ker::mod::mm::mmio_cache {
namespace detail {
namespace {
constexpr size_t CPU_LIMIT = 64;
// There is exactly one boot cohort in this kernel. It is never reset/recycled
// and cannot escape through a userspace ABI or survive a kernel restart.
constexpr uint64_t COHORT_INCARNATION = 1;
enum class Phase : uint8_t { empty, capturing, terminal };
enum class Seal : uint8_t { empty, preparing, ready, refused };
struct Cell {
    std::atomic<Phase> phase{Phase::empty};
    CpuObservation value{};
};
static_assert(std::atomic<Phase>::is_always_lock_free && std::atomic<Seal>::is_always_lock_free &&
              std::atomic<Status>::is_always_lock_free && std::atomic<uint64_t>::is_always_lock_free);

class InterruptGuard {
    uint64_t saved_{platform::save_disable_interrupts()};

   public:
    InterruptGuard() = default;
    InterruptGuard(const InterruptGuard&) = delete;
    auto operator=(const InterruptGuard&) -> InterruptGuard& = delete;
    ~InterruptGuard() { platform::restore_interrupts(saved_); }
};

auto decode_cpu(const RawCpu& raw, CpuPolicy& output) -> Status {
    // Encryption changes the address fields' interpretation. Report that
    // prerequisite before treating nominal-width masks as malformed geometry.
    if (raw.mtrr.vendor == cache_arch::Vendor::amd) {
        if (raw.amd.syscfg.observed() && (raw.amd.syscfg.value & (uint64_t{15} << 23)) != 0) return Status::needs_address_geometry;
        if ((raw.amd.encryption_eax & (uint32_t{1} << 1)) != 0 && raw.amd.sev_status.observed() && raw.amd.sev_status.value != 0)
            return Status::needs_address_geometry;
    }
    CpuPolicy candidate{};
    auto result = from_mtrr_status(cache_arch::decode_mtrrs(raw.mtrr, candidate.mtrr));
    if (result != Status::ready) return result;
    candidate.pat = raw.pat;
    candidate.cache_cr0 = raw.cr0;
    candidate.cache_cr4 = raw.cr4;
    result = validate_pat(candidate.pat);
    if (result != Status::ready) return result;
    result = validate_controls(candidate.cache_cr0, candidate.cache_cr4);
    if (result != Status::ready) return result;
    if (raw.mtrr.vendor == cache_arch::Vendor::amd) {
        result = decode_amd(raw.amd, candidate.mtrr, candidate.amd);
        if (result != Status::ready) return result;
        result = decode_amd_protected(raw.amd_protected, raw.mtrr.physical_bits, candidate.protected_ranges);
    } else if (raw.mtrr.vendor == cache_arch::Vendor::intel) {
        result = decode_intel_protected(raw.intel_protected, raw.mtrr.physical_bits, candidate.protected_ranges);
    } else {
        return Status::unsupported_vendor;
    }
    if (result != Status::ready) return result;
    output = candidate;
    return Status::ready;
}
}  // namespace

class Runtime {
    std::array<Cell, CPU_LIMIT> cells_{};
    std::atomic<uint64_t> expected_{};
    std::atomic<uint64_t> terminal_{};
    std::atomic<uint64_t> ready_{};
    std::atomic<uint64_t> invalidated_{};
    std::atomic<Status> error_{Status::ready};
    std::atomic<Seal> seal_{Seal::empty};
    Status seal_status_{Status::pending};

    void invalidate(Status status, uint64_t bit) {
        invalidated_.fetch_or(bit, std::memory_order_acq_rel);
        Status empty = Status::ready;
        (void)error_.compare_exchange_strong(empty, status, std::memory_order_acq_rel, std::memory_order_acquire);
    }

    auto first_refusal(uint64_t expected) const -> Status {
        for (size_t i = 0; i < CPU_LIMIT; ++i) {
            if ((expected & (uint64_t{1} << i)) == 0) continue;
            auto const& cell = cells_[i];
            if (cell.phase.load(std::memory_order_acquire) != Phase::terminal) return Status::pending;
            if (cell.value.status != Status::ready) return cell.value.status;
        }
        return Status::ready;
    }

    auto ensure_sealed() -> Status {
        auto const observed = observation();
        if (observed.status != Status::ready) return observed.status;
        auto state = seal_.load(std::memory_order_acquire);
        if (state == Seal::ready) return error_.load(std::memory_order_acquire);
        if (state == Seal::refused) return seal_status_;
        if (state == Seal::preparing ||
            !seal_.compare_exchange_strong(state, Seal::preparing, std::memory_order_acq_rel, std::memory_order_acquire))
            return Status::busy;
        auto result = Status::ready;
        for (size_t i = 0; i < CPU_LIMIT && result == Status::ready; ++i) {
            if ((observed.expected_mask & (uint64_t{1} << i)) == 0) continue;
            for (size_t j = 0; j < i; ++j) {
                if (cells_[j].value.apic == cells_[i].value.apic) {
                    result = Status::wrong_cpu;
                    break;
                }
            }
            if (result == Status::ready) result = equivalent_cpu_policy(cells_[0].value.policy, cells_[i].value.policy);
        }
        auto const error = error_.load(std::memory_order_acquire);
        if (error != Status::ready) result = error;
        seal_status_ = result;
        seal_.store(result == Status::ready ? Seal::ready : Seal::refused, std::memory_order_release);
        return result == Status::ready ? error_.load(std::memory_order_acquire) : result;
    }

   public:
    auto configure(size_t count) -> Status {
        if (count == 0 || count > CPU_LIMIT || count != platform::configured_cpu_count()) {
            invalidate(Status::invalid_configuration, 0);
            return Status::invalid_configuration;
        }
        uint64_t const mask = count == CPU_LIMIT ? UINT64_MAX : (uint64_t{1} << count) - 1;
        uint64_t empty = 0;
        if (!expected_.compare_exchange_strong(empty, mask, std::memory_order_acq_rel, std::memory_order_acquire) && empty != mask) {
            invalidate(Status::invalid_configuration, 0);
            return Status::invalid_configuration;
        }
        return error_.load(std::memory_order_acquire);
    }

    auto capture(size_t logical) -> Status {
        InterruptGuard guard;
        uint64_t const expected = expected_.load(std::memory_order_acquire);
        if (logical >= CPU_LIMIT || (expected & (uint64_t{1} << logical)) == 0) {
            invalidate(Status::invalid_configuration, 0);
            return expected == 0 ? Status::unconfigured : Status::invalid_configuration;
        }
        uint64_t const bit = uint64_t{1} << logical;
        auto& cell = cells_[logical];
        Phase empty = Phase::empty;
        if (!cell.phase.compare_exchange_strong(empty, Phase::capturing, std::memory_order_acq_rel, std::memory_order_acquire)) {
            invalidate(Status::duplicate_capture, bit);
            return Status::duplicate_capture;
        }
        cell.value.logical = static_cast<uint32_t>(logical);
        auto result = platform::verify_boot_cpu(logical, cell.value.apic);
        if (result == Status::ready) {
            platform::CpuIo io;
            result = collect_registers(io, cell.value.raw);
            if (result == Status::ready) {
                cache::CpuPat inherited{};
                if (!cache::cpu_pat(logical, inherited) || !inherited.supported || inherited.raw != cell.value.raw.pat)
                    result = Status::inconsistent_policy;
            }
            if (result == Status::ready) result = decode_cpu(cell.value.raw, cell.value.policy);
            uint32_t end_apic = 0;
            if (result == Status::ready) {
                result = platform::verify_boot_cpu(logical, end_apic);
                if (result == Status::ready && end_apic != cell.value.apic) result = Status::wrong_cpu;
            }
        }
        auto const error = error_.load(std::memory_order_acquire);
        if (error != Status::ready) result = error;
        cell.value.status = result;
        cell.phase.store(Phase::terminal, std::memory_order_release);
        if (result == Status::ready) ready_.fetch_or(bit, std::memory_order_acq_rel);
        terminal_.fetch_or(bit, std::memory_order_acq_rel);
        return result;
    }

    auto observation() const -> CohortObservation {
        CohortObservation value{};
        value.expected_mask = expected_.load(std::memory_order_acquire);
        value.terminal_mask = terminal_.load(std::memory_order_acquire);
        value.ready_mask = ready_.load(std::memory_order_acquire);
        value.invalidated_mask = invalidated_.load(std::memory_order_acquire);
        value.incarnation = value.expected_mask != 0 ? COHORT_INCARNATION : 0;
        auto const state = seal_.load(std::memory_order_acquire);
        value.sealed = state == Seal::ready;
        value.status = error_.load(std::memory_order_acquire);
        if (value.status != Status::ready) return value;
        if (value.expected_mask == 0)
            value.status = Status::unconfigured;
        else if (value.terminal_mask != value.expected_mask)
            value.status = Status::pending;
        else if (value.ready_mask != value.expected_mask)
            value.status = first_refusal(value.expected_mask);
        else if (state == Seal::refused)
            value.status = seal_status_;
        return value;
    }

    auto cpu(size_t logical, CpuObservation& output) const -> bool {
        if (logical >= CPU_LIMIT || cells_[logical].phase.load(std::memory_order_acquire) != Phase::terminal) return false;
        output = cells_[logical].value;
        return true;
    }

    auto qualify(uint64_t physical, uint64_t bytes, QualifiedUcRange& output) -> Status {
        if (!output.empty()) return Status::output_not_empty;
        if (bytes == 0 || ((physical | bytes) & (cache_arch::PAGE_BYTES - 1)) != 0 || bytes > UINT64_MAX - physical)
            return Status::invalid_geometry;
        auto result = ensure_sealed();
        if (result != Status::ready) return result;
        uint64_t const expected = expected_.load(std::memory_order_acquire);
        for (size_t i = 0; i < CPU_LIMIT; ++i) {
            if ((expected & (uint64_t{1} << i)) == 0) continue;
            result = qualify_cpu_uc(cells_[i].value.policy, physical, bytes);
            if (result != Status::ready) return result;
        }
        result = error_.load(std::memory_order_acquire);
        if (result != Status::ready) return result;
        output = QualifiedUcRange(physical, bytes, COHORT_INCARNATION);
        return Status::ready;
    }

    auto qualify(uint64_t physical, uint64_t bytes, QualifiedWcRange& output) -> Status {
        if (!output.empty()) return Status::output_not_empty;
        if (bytes == 0 || ((physical | bytes) & (cache_arch::PAGE_BYTES - 1)) != 0 || bytes > UINT64_MAX - physical)
            return Status::invalid_geometry;
        auto result = ensure_sealed();
        if (result != Status::ready) return result;
        uint64_t const expected = expected_.load(std::memory_order_acquire);
        for (size_t i = 0; i < CPU_LIMIT; ++i) {
            if ((expected & (uint64_t{1} << i)) == 0) continue;
            result = qualify_cpu_wc(cells_[i].value.policy, physical, bytes);
            if (result != Status::ready) return result;
        }
        result = error_.load(std::memory_order_acquire);
        if (result != Status::ready) return result;
        output = QualifiedWcRange(physical, bytes, COHORT_INCARNATION);
        return Status::ready;
    }

   private:
    auto revalidate_range(uint64_t physical, uint64_t bytes, uint64_t incarnation) -> Status {
        if (incarnation != COHORT_INCARNATION) return Status::invalid_configuration;
        auto result = ensure_sealed();
        if (result != Status::ready) return result;
        InterruptGuard guard;
        size_t logical = CPU_LIMIT;
        uint32_t apic = 0;
        result = platform::current_cpu(logical, apic);
        uint64_t const expected = expected_.load(std::memory_order_acquire);
        if (result != Status::ready || logical >= CPU_LIMIT || (expected & (uint64_t{1} << logical)) == 0) {
            if (result == Status::ready) result = Status::wrong_cpu;
            invalidate(result, logical < CPU_LIMIT ? uint64_t{1} << logical : 0);
            return result;
        }
        uint64_t const bit = uint64_t{1} << logical;
        auto const& cell = cells_[logical].value;
        if (apic != cell.apic) {
            invalidate(Status::wrong_cpu, bit);
            return Status::wrong_cpu;
        }
        // The private token already binds this exact interval to the immutable
        // sealed cohort. Do not rebuild boundary scratch in an eventual IPI
        // validator: only scalar token bounds and streamed hardware state belong
        // here. Publication still needs the separate completed CPU barrier.
        uint64_t const ceiling = cell.policy.mtrr.address_ceiling;
        if (bytes == 0 || ((physical | bytes) & (cache_arch::PAGE_BYTES - 1)) != 0 || physical >= ceiling || bytes > ceiling - physical)
            return Status::invalid_geometry;
        platform::CpuIo io;
        uint32_t failed_msr = 0;
        result = revalidate_registers(io, cell.raw, failed_msr);
        if (result != Status::ready) invalidate(result, bit);
        return result == Status::ready ? error_.load(std::memory_order_acquire) : result;
    }

   public:
    auto revalidate(const QualifiedUcRange& range) -> Status { return revalidate_range(range.physical_, range.bytes_, range.incarnation_); }
    auto revalidate(const QualifiedWcRange& range) -> Status { return revalidate_range(range.physical_, range.bytes_, range.incarnation_); }
};

namespace {
constinit Runtime runtime{};
}  // namespace
}  // namespace detail

QualifiedUcRange::QualifiedUcRange(uint64_t physical, uint64_t bytes, uint64_t incarnation)
    : physical_(physical), bytes_(bytes), incarnation_(incarnation) {}
QualifiedUcRange::QualifiedUcRange(QualifiedUcRange&& other) noexcept { *this = static_cast<QualifiedUcRange&&>(other); }
auto QualifiedUcRange::operator=(QualifiedUcRange&& other) noexcept -> QualifiedUcRange& {
    if (this == &other) return *this;
    physical_ = other.physical_;
    bytes_ = other.bytes_;
    incarnation_ = other.incarnation_;
    other.physical_ = other.bytes_ = other.incarnation_ = 0;
    return *this;
}

QualifiedWcRange::QualifiedWcRange(uint64_t physical, uint64_t bytes, uint64_t incarnation)
    : physical_(physical), bytes_(bytes), incarnation_(incarnation) {}
QualifiedWcRange::QualifiedWcRange(QualifiedWcRange&& other) noexcept { *this = static_cast<QualifiedWcRange&&>(other); }
auto QualifiedWcRange::operator=(QualifiedWcRange&& other) noexcept -> QualifiedWcRange& {
    if (this == &other) return *this;
    physical_ = other.physical_;
    bytes_ = other.bytes_;
    incarnation_ = other.incarnation_;
    other.physical_ = other.bytes_ = other.incarnation_ = 0;
    return *this;
}

auto configure_boot_cohort(size_t cpus) -> Status { return detail::runtime.configure(cpus); }
auto capture_boot_cpu(size_t expected_logical) -> Status { return detail::runtime.capture(expected_logical); }
auto cohort_observation() -> CohortObservation { return detail::runtime.observation(); }
auto cpu_observation(size_t logical, CpuObservation& output) -> bool { return detail::runtime.cpu(logical, output); }
auto qualify_uc_range(uint64_t physical, uint64_t page_rounded_bytes, QualifiedUcRange& empty_output) -> Status {
    return detail::runtime.qualify(physical, page_rounded_bytes, empty_output);
}
auto revalidate_current_cpu(const QualifiedUcRange& range) -> Status { return detail::runtime.revalidate(range); }
auto qualify_wc_range(uint64_t physical, uint64_t page_rounded_bytes, QualifiedWcRange& empty_output) -> Status {
    return detail::runtime.qualify(physical, page_rounded_bytes, empty_output);
}
auto revalidate_current_cpu(const QualifiedWcRange& range) -> Status { return detail::runtime.revalidate(range); }

auto status_name(Status status) -> const char* {
    static constexpr std::array NAMES{"ready",
                                      "pending",
                                      "busy",
                                      "unconfigured",
                                      "invalid_configuration",
                                      "duplicate_capture",
                                      "wrong_cpu",
                                      "local_idt_unavailable",
                                      "missing_features",
                                      "unsupported_vendor",
                                      "unsupported_layout",
                                      "unsupported_capability",
                                      "unsupported_mode",
                                      "read_fault",
                                      "invalid_reserved",
                                      "invalid_geometry",
                                      "invalid_type",
                                      "missing_observation",
                                      "undefined_overlap",
                                      "inconsistent_policy",
                                      "state_drift",
                                      "capacity",
                                      "unknown_extension",
                                      "needs_address_geometry",
                                      "fixed_routing_unqualified",
                                      "ambiguous_routing",
                                      "routing_override_unqualified",
                                      "routed_to_memory",
                                      "ambiguous_tom2_default",
                                      "protected_ranges_unobservable",
                                      "protected_range",
                                      "not_strict_uc",
                                      "output_not_empty",
                                      "not_strict_wc"};
    static_assert(NAMES.size() == static_cast<size_t>(Status::not_strict_wc) + 1);
    auto const index = static_cast<size_t>(status);
    return index < NAMES.size() ? NAMES[index] : "invalid_status";
}

}  // namespace ker::mod::mm::mmio_cache
