#pragma once
#ifdef WOS_SELFTEST
#include <cstddef>
#include <cstdint>
#include <platform/mm/lifecycle_census.hpp>
#include <test/numa_lifecycle_census_wire.hpp>
#include <test/numa_lifecycle_mm_debt.hpp>
namespace ker::mod::sched::task {
struct Task;
}
namespace ker::test::numa_lifecycle {
class CensusHandler;
// Permission belongs to the actual executing syscall; no owner reference.
class CurrentCensusScope final {
   public:
    CurrentCensusScope(const CurrentCensusScope&) = delete;
    CurrentCensusScope(CurrentCensusScope&&) = delete;
    auto operator=(const CurrentCensusScope&) -> CurrentCensusScope& = delete;
    [[nodiscard]] auto actor() const noexcept -> mod::sched::task::Task& { return *actor_; }
    [[nodiscard]] auto cookie() const noexcept -> uint64_t { return cookie_; }
    [[nodiscard]] auto root_incarnation() const noexcept -> uint64_t { return root_incarnation_; }
    // Fast executor identity only. The handler separately validates registered
    // root serial before/after the owner bracket without mutating its epoch.
    [[nodiscard]] auto executing_here() const noexcept -> bool;

   private:
    friend class CensusHandler;
    CurrentCensusScope(mod::sched::task::Task& actor, uint64_t cookie, uint64_t serial) noexcept
        : actor_(&actor), cookie_(cookie), root_incarnation_(serial) {}
    mod::sched::task::Task* actor_;
    uint64_t cookie_, root_incarnation_;
};
static_assert(sizeof(CurrentCensusScope) == 24);
struct NativeDebtSnapshot {
    mod::mm::lifecycle_census::Stamp stamp{};
    uint64_t constructions{}, queue_outstanding{}, return_tails{}, flags{};
    auto operator==(const NativeDebtSnapshot&) const -> bool = default;
};
enum NativeFlags : uint64_t {
    NATIVE_ACTIVE_PRESENT = 1ULL << 0,
    NATIVE_QUEUE_PRESENT = 1ULL << 1,
    NATIVE_ADMISSION_CLOSED = 1ULL << 2,
    NATIVE_COMPLETE = 1ULL << 3,
    NATIVE_ARITHMETIC_VALID = 1ULL << 4
};
static_assert(sizeof(NativeDebtSnapshot) == 56);
static_assert(offsetof(NativeDebtSnapshot, constructions) == 24 && offsetof(NativeDebtSnapshot, flags) == 48);
inline constexpr size_t REGISTRY_ROW_LIMIT = 4096;
struct RegistryRow {
    uint64_t root_identity{}, file_or_family_identity{}, binding_identity{};
    uint64_t start{}, end{}, file_offset{}, kind_flags{}, reserved{};
    auto operator==(const RegistryRow&) const -> bool = default;
};
static_assert(sizeof(RegistryRow) == 64);
enum RegistryRowFlags : uint64_t { LEGACY_ROW = 1ULL << 0, NATIVE_ROW = 1ULL << 1, WRITEBACK_ROW = 1ULL << 2, REGISTERED_ROW = 1ULL << 3 };
enum RegistryFlags : uint64_t {
    REGISTRY_COMPLETE = 1ULL << 0,
    REGISTRY_BUSY = 1ULL << 1,
    REGISTRY_OVERFLOW = 1ULL << 2,
    REGISTRY_INVALID = 1ULL << 3
};
struct RegistrySnapshot {
    mod::mm::lifecycle_census::Stamp stamp{};
    uint64_t legacy_rows{}, native_rows{}, row_count{}, flags{};
    RegistryRow rows[REGISTRY_ROW_LIMIT]{};
};
static_assert(offsetof(RegistrySnapshot, rows) == 56 && sizeof(RegistrySnapshot) == 262200);
// Boot-only publication before tasks/APs execute; no runtime disable/reset.
void enable_current_census_boot() noexcept;
// Private operation; no installed ABI or public nonLOCAL feature.
auto dispatch_current_census(uint64_t address, uint64_t bytes, uint64_t unused3, uint64_t unused4) -> uint64_t;
}  // namespace ker::test::numa_lifecycle
#endif
