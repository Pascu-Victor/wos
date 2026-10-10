#pragma once

#include <cstddef>
#include <cstdint>

namespace ker::mod::sched::pid_ns {

inline constexpr size_t MAX_NAMESPACES = 64;
inline constexpr uint32_t MAX_LOCAL_ID = INT32_MAX;
struct Namespace;
class OperationLease;

// Kernel ownership only. Empty is the host; no integer-to-reference join API.
// Retention/drop is atomic-only and never tears down Tasks or other namespaces.
class NamespaceRef {
   public:
    constexpr NamespaceRef() = default;
    NamespaceRef(const NamespaceRef&) = delete;
    auto operator=(const NamespaceRef&) -> NamespaceRef& = delete;
    NamespaceRef(NamespaceRef&& other) noexcept;
    auto operator=(NamespaceRef&& other) noexcept -> NamespaceRef&;
    ~NamespaceRef();

    [[nodiscard]] auto is_host() const -> bool { return namespace_ == nullptr; }
    [[nodiscard]] auto generation() const -> uint64_t;
    [[nodiscard]] auto closed() const -> bool;
    // Outstanding admitted operations retain this exact slot until cleanup.
    [[nodiscard]] auto operations_drained() const -> bool;
    [[nodiscard]] auto retain(NamespaceRef& out) const -> int;
    // Reserve an immutable local PID/TID; failures preserve out. Unused IDs
    // are not recycled, including failed unpublished construction.
    [[nodiscard]] auto allocate_id(uint32_t& out) const -> int;
    // Serializes with native index publication; no registry lock or teardown.
    void close() const;
    void reset();
    [[nodiscard]] auto operator==(const NamespaceRef& other) const -> bool { return namespace_ == other.namespace_; }

   private:
    friend auto create_namespace(NamespaceRef& out) -> int;
    friend class PublicationGuard;
    friend class OperationLease;
    Namespace* namespace_{};
};

// A retained lifetime admission, not a held spinlock. Owning syscalls must
// register cleanup before blocking; closure still gates individual effects.
class OperationLease {
   public:
    OperationLease() = default;
    OperationLease(const OperationLease&) = delete;
    auto operator=(const OperationLease&) -> OperationLease& = delete;
    ~OperationLease();
    [[nodiscard]] auto begin(const NamespaceRef& scope) -> int;
    void reset();
    [[nodiscard]] auto active() const -> bool { return active_; }
    [[nodiscard]] auto scope() const -> const NamespaceRef* { return active_ ? &scope_ : nullptr; }

   private:
    NamespaceRef scope_{};
    bool active_{};
};

// Caller retains scope. Lock order: runqueue -> registry -> this IRQ-safe
// guard. Never close/reset that scope, allocate, block or finalize under it.
class PublicationGuard {
   public:
    explicit PublicationGuard(const NamespaceRef& scope);
    PublicationGuard(const PublicationGuard&) = delete;
    auto operator=(const PublicationGuard&) -> PublicationGuard& = delete;
    ~PublicationGuard();
    [[nodiscard]] auto open() const -> bool;

   private:
    Namespace* namespace_{};
    uint64_t flags_{};
};

[[nodiscard]] auto create_namespace(NamespaceRef& out) -> int;
[[nodiscard]] auto live_namespace_count() -> size_t;

}  // namespace ker::mod::sched::pid_ns
