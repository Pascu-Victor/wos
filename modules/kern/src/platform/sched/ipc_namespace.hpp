#pragma once

#include <cstddef>
#include <cstdint>

namespace ker::mod::sched::ipc {

inline constexpr size_t MAX_NAMESPACES = 64;
struct Namespace;

// Private kernel ownership, not a numeric join handle. Empty is the host
// namespace. Retention/drop never allocates, blocks or invokes finalizers.
class NamespaceRef {
   public:
    constexpr NamespaceRef() = default;
    NamespaceRef(const NamespaceRef&) = delete;
    auto operator=(const NamespaceRef&) -> NamespaceRef& = delete;
    NamespaceRef(NamespaceRef&& other) noexcept;
    auto operator=(NamespaceRef&& other) noexcept -> NamespaceRef&;
    ~NamespaceRef();

    [[nodiscard]] auto is_host() const -> bool { return m_namespace == nullptr; }
    [[nodiscard]] auto generation() const -> uint64_t;
    [[nodiscard]] auto closed() const -> bool;
    [[nodiscard]] auto retain(NamespaceRef& out) const -> int;
    void close() const;
    void reset();
    [[nodiscard]] auto operator==(const NamespaceRef& other) const -> bool { return m_namespace == other.m_namespace; }

   private:
    friend auto create_namespace(NamespaceRef& out) -> int;
    Namespace* m_namespace{};
};

// Bounded admission. Failure preserves out; no API converts an ID into a ref.
[[nodiscard]] auto create_namespace(NamespaceRef& out) -> int;
// Diagnostic count; exact baseline comparisons require quiescent callers.
[[nodiscard]] auto live_namespace_count() -> size_t;

}  // namespace ker::mod::sched::ipc
