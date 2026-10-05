#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <platform/ktime/arithmetic.hpp>
#include <platform/ktime/clocksource_policy.hpp>

namespace ker::mod::time::kvmclock {

inline constexpr uint32_t SYSTEM_TIME_MSR = 0x4b564d01;
inline constexpr uint32_t REQUIRED_FEATURES = (1U << 3) | (1U << 24);
inline constexpr uint64_t WATCHDOG_INTERVAL_NS = 10000000;
inline constexpr uint64_t RATE_WINDOW_NS = 1000000000;
inline constexpr uint64_t MAX_RATE_PPM = 1000;
inline constexpr size_t MAX_CPUS = 256;
inline constexpr unsigned SNAPSHOT_ATTEMPTS = 16;

// KVM SYSTEM_TIME_NEW ABI. Each record stays in registered guest RAM until
// shutdown. Natural alignment gives the documented 32-byte layout.
struct alignas(8) TimeInfo {
    uint32_t version{};
    uint32_t pad0{};
    uint64_t tsc_timestamp{};
    uint64_t system_time{};
    uint32_t multiplier{};
    int8_t shift{};
    uint8_t flags{};
    std::array<uint8_t, 2> pad{};
};
static_assert(sizeof(TimeInfo) == 32);
static_assert(offsetof(TimeInfo, tsc_timestamp) == 8);
static_assert(offsetof(TimeInfo, system_time) == 16);
static_assert(offsetof(TimeInfo, multiplier) == 24);
static_assert(offsetof(TimeInfo, shift) == 28);
static_assert(offsetof(TimeInfo, flags) == 29);

constexpr auto supported(uint32_t maximum, uint32_t ebx, uint32_t ecx, uint32_t edx, uint32_t features) -> bool {
    return (maximum == 0 || maximum >= 0x40000001U) && ebx == 0x4b4d564bU && ecx == 0x564b4d56U && edx == 0x4dU &&
           (features & REQUIRED_FEATURES) == REQUIRED_FEATURES;
}

constexpr auto convert(uint64_t counter, uint64_t origin, uint64_t system_time, uint32_t multiplier, int8_t shift, uint8_t flags,
                       uint64_t& result) -> bool {
    // Require KVM's explicit cross-vCPU monotonicity promise on every sample.
    // Bound shifts before evaluating them; the 128-bit product cannot overflow.
    if ((flags & 1U) == 0 || multiplier == 0 || counter < origin || shift < -31 || shift > 31) return false;
    unsigned __int128 delta = counter - origin;
    delta = shift < 0 ? delta >> -shift : delta << shift;
    auto const NS = (delta * multiplier) >> 32;
    if (NS > UINT64_MAX - system_time) return false;
    result = system_time + static_cast<uint64_t>(NS);
    return true;
}

template <typename ReadTsc>
auto sample(const TimeInfo& page, ReadTsc&& read_tsc, uint64_t& result) -> bool {
    for (unsigned attempt = 0; attempt < SNAPSHOT_ATTEMPTS; ++attempt) {
        uint32_t const VERSION = __atomic_load_n(&page.version, __ATOMIC_ACQUIRE);
        if (VERSION == 0 || (VERSION & 1U) != 0) continue;
        uint64_t const ORIGIN = __atomic_load_n(&page.tsc_timestamp, __ATOMIC_RELAXED);
        uint64_t const SYSTEM = __atomic_load_n(&page.system_time, __ATOMIC_RELAXED);
        uint32_t const MULTIPLIER = __atomic_load_n(&page.multiplier, __ATOMIC_RELAXED);
        int8_t const SHIFT = __atomic_load_n(&page.shift, __ATOMIC_RELAXED);
        uint8_t const FLAGS = __atomic_load_n(&page.flags, __ATOMIC_RELAXED);
        uint64_t const COUNTER = read_tsc();
        std::atomic_thread_fence(std::memory_order_acquire);
        if (VERSION != __atomic_load_n(&page.version, __ATOMIC_ACQUIRE)) continue;
        return convert(COUNTER, ORIGIN, SYSTEM, MULTIPLIER, SHIFT, FLAGS, result);
    }
    return false;
}

enum class Mode : uint8_t { off, qualifying, active, failed };

inline auto rate_tolerance(uint64_t elapsed) -> uint64_t {
    return arithmetic::saturating_add(clocksource::MAX_DEVIATION_NS, arithmetic::mul_div(elapsed, MAX_RATE_PPM, 1000000, true));
}

inline auto transition_offset(uint64_t candidate, uint64_t before, uint64_t after, int64_t& offset) -> bool {
    if (after < before) return false;
    uint64_t const REFERENCE = before + ((after - before) / 2);
    uint64_t const MAGNITUDE = candidate >= REFERENCE ? candidate - REFERENCE : REFERENCE - candidate;
    if (MAGNITUDE > INT64_MAX) return false;
    offset = candidate >= REFERENCE ? static_cast<int64_t>(MAGNITUDE) : -static_cast<int64_t>(MAGNITUDE);
    return true;
}

// Providers supply the hardware boundaries only. Qualification and the actual
// read/watchdog path are shared with host tests. The caller prevents migration
// throughout register_cpu/read; no guest writer holds a lock across an IRQ.
class Runtime {
    struct Checkpoint {
        std::atomic<uint64_t> revision{0};
        std::atomic<uint64_t> pv{0};
        std::atomic<uint64_t> before{0};
        std::atomic<uint64_t> after{0};
    };
    struct CheckpointSnapshot {
        uint64_t revision{};
        uint64_t pv{};
        uint64_t before{};
        uint64_t after{};
    };
    struct Slot {
        std::atomic<TimeInfo*> page{nullptr};
        std::atomic<bool> registered{false};
        std::atomic<uint64_t> last_checked{0};
        std::atomic<uint64_t> last_seen{0};
        Checkpoint rate_anchor{};
    };
    std::array<Slot, MAX_CPUS> slots_{};
    std::atomic<Mode> mode_{Mode::off};
    std::atomic<bool> epoch_ready_{false};
    std::atomic<uint64_t> registered_{0};
    std::atomic<uint64_t> deviation_{0};
    std::atomic<uint64_t> checks_{0};
    std::atomic<int64_t> qualified_offset_{0};
    // INT64_MIN is excluded by transition_offset(). Freeze exactly once before
    // publishing failed mode; late in-flight checks cannot move fallback time.
    std::atomic<int64_t> failed_offset_{INT64_MIN};
    uint64_t expected_{};
    uint64_t system_origin_{};
    uint64_t hpet_origin_{};

    auto fail() -> bool {
        int64_t previous = INT64_MIN;
        (void)failed_offset_.compare_exchange_strong(previous, qualified_offset_.load(std::memory_order_acquire),
                                                     std::memory_order_acq_rel);
        mode_.store(Mode::failed, std::memory_order_release);
        return false;
    }

    static auto snapshot(const Checkpoint& anchor, CheckpointSnapshot& result) -> bool {
        for (unsigned attempt = 0; attempt < SNAPSHOT_ATTEMPTS; ++attempt) {
            uint64_t const REVISION = anchor.revision.load(std::memory_order_acquire);
            if ((REVISION & 1U) != 0) continue;
            result = {.revision = REVISION,
                      .pv = anchor.pv.load(std::memory_order_relaxed),
                      .before = anchor.before.load(std::memory_order_relaxed),
                      .after = anchor.after.load(std::memory_order_relaxed)};
            std::atomic_thread_fence(std::memory_order_acquire);
            if (REVISION == anchor.revision.load(std::memory_order_acquire)) return true;
        }
        return false;
    }

    static auto update(Checkpoint& anchor, uint64_t revision, uint64_t pv, uint64_t before, uint64_t after) -> bool {
        if (revision > UINT64_MAX - 2 || !anchor.revision.compare_exchange_strong(revision, revision + 1, std::memory_order_acq_rel))
            return false;
        anchor.pv.store(pv, std::memory_order_relaxed);
        anchor.before.store(before, std::memory_order_relaxed);
        anchor.after.store(after, std::memory_order_relaxed);
        anchor.revision.store(revision + 2, std::memory_order_release);
        return true;
    }

    auto boot_epoch(uint64_t system, uint64_t& result) const -> bool {
        if (system < system_origin_ || system - system_origin_ > UINT64_MAX - hpet_origin_) return false;
        result = hpet_origin_ + (system - system_origin_);
        return true;
    }

   public:
    void prepare(bool available, uint64_t cpus) {
        if (!available || cpus == 0 || cpus > MAX_CPUS) return;
        expected_ = cpus;
        mode_.store(Mode::qualifying, std::memory_order_release);
    }
    auto mode() const -> Mode { return mode_.load(std::memory_order_acquire); }
    auto registered() const -> uint64_t { return registered_.load(std::memory_order_acquire); }
    auto expected() const -> uint64_t { return expected_; }
    auto deviation() const -> uint64_t { return deviation_.load(std::memory_order_relaxed); }
    auto checks() const -> uint64_t { return checks_.load(std::memory_order_relaxed); }
    auto fallback(uint64_t reference) const -> uint64_t {
        int64_t const OFFSET = failed_offset_.load(std::memory_order_acquire);
        return arithmetic::add_signed(reference, OFFSET == INT64_MIN ? 0 : OFFSET);
    }

    template <typename Provider>
    void register_cpu(uint64_t cpu, bool bootstrap, Provider& provider) {
        if (mode() != Mode::qualifying) return;
        if (cpu >= expected_) {
            (void)fail();
            return;
        }
        auto& slot = slots_.at(cpu);
        if (slot.registered.load(std::memory_order_acquire)) return;
        if (!provider.available() || (!bootstrap && !epoch_ready_.load(std::memory_order_acquire))) {
            (void)fail();
            return;
        }
        auto* page = provider.allocate();
        if (page == nullptr) {
            (void)fail();
            return;
        }
        // Do not free a record after registration, including on failure. KVM
        // may still update it until the per-vCPU MSR is explicitly disabled.
        provider.register_page(*page);
        slot.page.store(page, std::memory_order_release);
        uint64_t candidate = 0;
        bool qualified = false;
        for (unsigned attempt = 0; attempt < 8; ++attempt) {
            uint64_t const BEFORE = provider.reference();
            uint64_t system = 0;
            bool const VALID = sample(*page, [&] { return provider.counter(); }, system);
            uint64_t const AFTER = provider.reference();
            if (!VALID || AFTER < BEFORE) break;
            if (AFTER - BEFORE > clocksource::MAX_DEVIATION_NS) continue;
            if (bootstrap) {
                system_origin_ = system;
                hpet_origin_ = BEFORE + ((AFTER - BEFORE) / 2);
                epoch_ready_.store(true, std::memory_order_release);
            }
            if (!boot_epoch(system, candidate)) break;
            uint64_t const ERROR = clocksource::deviation(candidate, BEFORE, AFTER);
            (void)clocksource::publish_max(deviation_, ERROR);
            if (AFTER < hpet_origin_ || ERROR > rate_tolerance(AFTER - hpet_origin_)) break;
            int64_t offset = 0;
            if (!transition_offset(candidate, BEFORE, AFTER, offset) || !update(slot.rate_anchor, 0, candidate, BEFORE, AFTER)) break;
            qualified_offset_.store(offset, std::memory_order_release);
            qualified = true;
            break;
        }
        if (!qualified) {
            (void)fail();
            return;
        }
        slot.last_checked.store(candidate, std::memory_order_relaxed);
        slot.last_seen.store(candidate, std::memory_order_relaxed);
        slot.registered.store(true, std::memory_order_release);
        if (registered_.fetch_add(1, std::memory_order_acq_rel) + 1 == expected_) {
            Mode previous = Mode::qualifying;
            (void)mode_.compare_exchange_strong(previous, Mode::active, std::memory_order_acq_rel);
        }
    }

    template <typename Provider>
    auto read(uint64_t cpu, bool force_watchdog, Provider& provider, uint64_t& result) -> bool {
        if (mode() != Mode::active) return false;
        if (cpu >= expected_) return fail();
        auto& slot = slots_.at(cpu);
        auto* const PAGE = slot.page.load(std::memory_order_acquire);
        uint64_t system = 0;
        uint64_t candidate = 0;
        if (PAGE == nullptr || !sample(*PAGE, [&] { return provider.counter(); }, system) || !boot_epoch(system, candidate)) return fail();
        uint64_t const PREVIOUS = slot.last_seen.load(std::memory_order_relaxed);
        if (candidate < PREVIOUS) return fail();
        uint64_t const CHECKED = slot.last_checked.load(std::memory_order_relaxed);
        if (candidate < CHECKED) return fail();
        if (force_watchdog || candidate - CHECKED >= WATCHDOG_INTERVAL_NS) {
            CheckpointSnapshot anchor;
            if (!snapshot(slot.rate_anchor, anchor)) return fail();
            // A fresh PV snapshot belongs INSIDE the reference bracket. Taking
            // the initial sample before BEFORE would misdiagnose sampling delay.
            uint64_t const BEFORE = provider.reference();
            bool const VALID = sample(*PAGE, [&] { return provider.counter(); }, system) && boot_epoch(system, candidate);
            uint64_t const AFTER = provider.reference();
            checks_.fetch_add(1, std::memory_order_relaxed);
            if (!VALID || AFTER < BEFORE || candidate < PREVIOUS || candidate < anchor.pv || AFTER < anchor.before) return fail();
            uint64_t const ELAPSED = candidate - anchor.pv;
            uint64_t const MINIMUM = BEFORE >= anchor.after ? BEFORE - anchor.after : 0;
            uint64_t const MAXIMUM = AFTER - anchor.before;
            uint64_t const ERROR = clocksource::deviation(ELAPSED, MINIMUM, MAXIMUM);
            (void)clocksource::publish_max(deviation_, ERROR);
            if (ERROR > rate_tolerance(MAXIMUM)) return fail();
            // Accumulate rate evidence over up to one second. Resetting the
            // baseline on every IRQ would repeatedly forgive the sampling slack
            // and allow a persistently bad rate to evade qualification.
            if (ELAPSED >= RATE_WINDOW_NS && !update(slot.rate_anchor, anchor.revision, candidate, BEFORE, AFTER)) return fail();
            if (AFTER - BEFORE <= clocksource::MAX_DEVIATION_NS) {
                int64_t offset = 0;
                if (!transition_offset(candidate, BEFORE, AFTER, offset)) return fail();
                qualified_offset_.store(offset, std::memory_order_release);
            }
            (void)clocksource::publish_max(slot.last_checked, candidate);
        }
        (void)clocksource::publish_max(slot.last_seen, candidate);
        if (mode() != Mode::active) return false;
        result = candidate;
        return true;
    }
};

}  // namespace ker::mod::time::kvmclock
