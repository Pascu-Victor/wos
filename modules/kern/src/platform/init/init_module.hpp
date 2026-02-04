#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::init {

// Boot phases - modules can only depend on modules in same or earlier phase
enum class BootPhase : uint8_t {
    PHASE_0_EARLY_BOOT,       // No heap, no interrupts (fb, serial, mm pages)
    PHASE_1_POST_MM,          // Physical memory manager ready (kmalloc)
    PHASE_2_POST_INTERRUPT,   // Interrupts/APIC ready (syscall, ioapic, gates, irqs)
    PHASE_3_SUBSYSTEMS,       // Subsystems init (vfs, net, devfs)
    PHASE_4_SCHEDULER_SETUP,  // SMT and EpochManager ready (before drivers)
    PHASE_5_DRIVERS,          // Device drivers (pci, virtio, e1000e)
    PHASE_6_POST_SCHEDULER,   // Scheduler + complete drivers ready (wki transports, ipv6)
    PHASE_7_KERNEL_START,     // Final phase - starts scheduler (never returns)
};

inline constexpr int BOOT_PHASE_COUNT = static_cast<int>(BootPhase::PHASE_7_KERNEL_START) + 1;

// Maximum supported modules and dependencies
inline constexpr size_t MAX_MODULES = 64;
inline constexpr size_t MAX_DEPS_PER_MODULE = 16;

// Compile-time string hash (djb2)
constexpr size_t constexpr_hash(const char* str) {
    size_t h = 5381;
    while (*str != '\0') {
        h = ((h << 5) + h) + static_cast<size_t>(*str++);
    }
    return h;
}

// Compile-time string comparison
constexpr bool constexpr_streq(const char* a, const char* b) {
    while (*a != '\0' && *b != '\0') {
        if (*a != *b) {
            return false;
        }
        ++a;
        ++b;
    }
    return *a == *b;
}

// String ID for compile-time module identification
struct ModuleId {
    const char* name;
    size_t hash;

    constexpr ModuleId() : name(nullptr), hash(0) {}
    constexpr ModuleId(const char* n) : name(n), hash(constexpr_hash(n)) {}  // NOLINT

    constexpr bool operator==(const ModuleId& other) const {
        // Use hash for fast rejection, then string comparison for confirmation
        // Note: Cannot compare pointers in constexpr context (unspecified behavior)
        return hash == other.hash && constexpr_streq(name, other.name);
    }

    constexpr bool operator!=(const ModuleId& other) const { return !(*this == other); }

    [[nodiscard]] constexpr bool is_valid() const { return name != nullptr && hash != 0; }
};

// Dependency type
enum class DepType : uint8_t {
    HARD,      // Must run before this module (compile error if missing)
    OPTIONAL,  // Run before if present, skip if not registered
};

// Single dependency entry
struct Dependency {
    ModuleId target;
    DepType type;

    constexpr Dependency() : type(DepType::HARD) {}
    constexpr Dependency(const char* name, DepType t = DepType::HARD) : target(name), type(t) {}  // NOLINT
};

// Function pointer for init (used at runtime only)
using InitFn = void (*)();

// Compile-time module metadata (no function pointers - for constexpr validation)
struct ModuleMeta {
    ModuleId id;
    BootPhase phase;
    size_t dep_count;
    std::array<Dependency, MAX_DEPS_PER_MODULE> deps;

    constexpr ModuleMeta() : phase(BootPhase::PHASE_0_EARLY_BOOT), dep_count(0), deps{} {}

    constexpr ModuleMeta(const char* name, BootPhase p) : id(name), phase(p), dep_count(0), deps{} {}

    template <size_t N>
    constexpr ModuleMeta(const char* name, BootPhase p, const std::array<Dependency, N>& dependencies)
        : id(name), phase(p), dep_count(N), deps{} {
        for (size_t i = 0; i < N && i < MAX_DEPS_PER_MODULE; ++i) {
            deps[i] = dependencies[i];
        }
    }
};

// Helper for creating module metadata with no dependencies
constexpr auto make_meta(const char* name, BootPhase phase) { return ModuleMeta{name, phase}; }

// Helper for creating module metadata with dependencies (variadic)
template <typename... Deps>
constexpr auto make_meta(const char* name, BootPhase phase, Deps... deps) {
    std::array<Dependency, sizeof...(Deps)> dep_array{deps...};
    return ModuleMeta{name, phase, dep_array};
}

// Runtime module descriptor (includes function pointer)
struct ModuleDesc {
    const char* name;
    BootPhase phase;
    InitFn init_fn;
};

}  // namespace ker::init
