#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::dev::e1000e::detail {
class MmioRetirementAccess;
}
namespace ker::mod::mm::cpu_maintenance::detail {
class Controller;
}

namespace ker::mod::mm::mmio {
namespace detail {
class Runtime;
}

enum class Status : uint8_t {
    ready,
    disabled,
    complete,
    busy,
    not_ready,
    invalid_context,
    invalid_claim,
    output_not_empty,
    capacity,
    generation_exhausted,
    incomplete_inventory,
    unsupported_profile,
    stale_geometry,
    foreign_overlap,
    legacy_exposed,
    initial_bme_active,
    irq_active,
    dma_present,
    bad_probe,
    decode_readback,
    boot_memory,
    alias_present,
    history_exposed,
    history_incomplete,
    arena_unavailable,
    allocation_failed,
    bad_hierarchy,
    pending_retained,
    failed_retained,
    invalid_mapping,
    borrow_busy,
    not_quiesced,
    clock_unavailable,
};

enum class Phase : uint8_t {
    empty,
    reserved,
    cache_pending,
    leaves_ready,
    active,
    closed,
    withdrawal_pending,
    complete,
    retained,
    physical_pending = 9
};

inline constexpr size_t VIEW_CAPACITY = 8;

// A requested bounded window inside an actual retained PCI BAR. The actual
// issuer resolves physical geometry; these scalars grant no mapping authority.
struct ViewRequest {
    uint8_t bar{};
    uint64_t offset{}, bytes{};
};

struct ViewReport {
    Status status{Status::invalid_mapping};
    Phase phase{Phase::empty};
    uint8_t bar{};
    uint64_t bar_offset{}, physical{}, virtual_base{}, bytes{};
    uint64_t initial_ticket{}, withdrawal_ticket{}, cohort_incarnation{};
    size_t table_pages{};
    bool timed_out_ever{};
};

// Copied observations only; none of these scalars can adopt a mapping or grant
// physical ownership, register access, hardware retirement or CPU completion.
struct Report {
    Status status{Status::invalid_mapping};
    Phase phase{Phase::empty};
    uint64_t id{};
    uint64_t physical{};
    uint64_t virtual_base{};
    uint64_t bytes{};
    uint64_t normal_readers{};
    uint64_t initial_ticket{};
    uint64_t withdrawal_ticket{};
    uint64_t cohort_incarnation{};
    size_t table_pages{};
    size_t physical_spans{};
    bool admission_closed{};
    bool terminal_active{};
    bool timed_out_ever{};
    size_t view_count{};
    std::array<ViewReport, VIEW_CAPACITY> views{};
    uint64_t resource_steps{}, resource_visits{}, resource_restarts{}, resource_epoch{}, resource_root_frame{};
};

class Borrow {
    enum class Kind : uint8_t { empty, normal, terminal };
    uint64_t id_{};
    uint16_t slot_{UINT16_MAX};
    Kind kind_{Kind::empty};
    friend class detail::Runtime;
    Borrow(uint16_t slot, uint64_t id, Kind kind) noexcept;

   public:
    Borrow() = default;
    ~Borrow();
    Borrow(const Borrow&) = delete;
    auto operator=(const Borrow&) -> Borrow& = delete;
    Borrow(Borrow&& other) noexcept;
    auto operator=(Borrow&& other) noexcept -> Borrow&;
    [[nodiscard]] auto valid() const noexcept -> bool;
    void reset() noexcept;
    // Caller holds this actual affine borrow for the whole hardware operation.
    // Existing borrows survive gate closure. Invalid offsets/owners trap before
    // a volatile access; register access never allocates, waits or logs.
    [[nodiscard]] auto read32(size_t byte_offset) const noexcept -> uint32_t;
    void write32(size_t byte_offset, uint32_t value) const noexcept;
    [[nodiscard]] auto read32(size_t view_index, size_t byte_offset) const noexcept -> uint32_t;
    void write32(size_t view_index, size_t byte_offset, uint32_t value) const noexcept;
};

class QuiescedMapping;

class Mapping {
    uint64_t id_{};
    uint16_t slot_{UINT16_MAX};
    friend class detail::Runtime;
    friend class QuiescedMapping;
    Mapping(uint16_t slot, uint64_t id) noexcept;

   public:
    Mapping() = default;
    // Closing/abandonment retains the real record, leaves, tickets and physical
    // tombstones. Destruction is never an unmap or hardware-stop operation.
    ~Mapping();
    Mapping(const Mapping&) = delete;
    auto operator=(const Mapping&) -> Mapping& = delete;
    Mapping(Mapping&& other) noexcept;
    auto operator=(Mapping&& other) noexcept -> Mapping&;
    [[nodiscard]] auto empty() const noexcept -> bool { return id_ == 0; }
    [[nodiscard]] auto borrow() const noexcept -> Borrow;
    void close() noexcept;
    [[nodiscard]] auto borrows_drained() const noexcept -> bool;
    // Unique terminal access requires closed admission and zero normal readers.
    [[nodiscard]] auto terminal_borrow() const noexcept -> Borrow;
    [[nodiscard]] auto report() const noexcept -> Report;
    [[nodiscard]] auto withdraw(const QuiescedMapping&) -> Status;
    [[nodiscard]] auto withdraw(const cpu_maintenance::detail::Controller&, const QuiescedMapping&) -> Status;
};

// Only the actual driver's final hardware-retirement continuation can issue
// this synchronous witness. It binds the original mapping, carries no Task or
// callback, and must be issued after the terminal register borrow is dropped.
class QuiescedMapping final {
    friend class ker::dev::e1000e::detail::MmioRetirementAccess;
    friend class detail::Runtime;
    uint64_t id_{};
    uint16_t slot_{UINT16_MAX};
    explicit QuiescedMapping(const Mapping&) noexcept;

   public:
    QuiescedMapping(const QuiescedMapping&) = delete;
    auto operator=(const QuiescedMapping&) -> QuiescedMapping& = delete;
    QuiescedMapping(QuiescedMapping&&) = delete;
    auto operator=(QuiescedMapping&&) -> QuiescedMapping& = delete;
};

// Immutable boot selection only, never an owner or fallback permission.
[[nodiscard]] auto enabled() noexcept -> bool;
[[nodiscard]] auto status_name(Status) noexcept -> const char*;

}  // namespace ker::mod::mm::mmio
