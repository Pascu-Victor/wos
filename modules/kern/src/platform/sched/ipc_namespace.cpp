#include "ipc_namespace.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <utility>

namespace ker::mod::sched::ipc {

struct Namespace {
    std::atomic<uint64_t> references{};
    uint64_t generation{};
    std::atomic<bool> closed{};
};

namespace {
constexpr uint64_t RESERVED = UINT64_MAX;
constinit std::array<Namespace, MAX_NAMESPACES> namespaces{};
constinit std::atomic<uint64_t> next_generation{1};

auto allocate_generation() -> uint64_t {
    auto value = next_generation.load(std::memory_order_relaxed);
    while (value != 0) {
        auto const NEXT = value == UINT64_MAX ? 0 : value + 1;
        if (next_generation.compare_exchange_weak(value, NEXT, std::memory_order_relaxed)) return value;
    }
    return 0;
}
}  // namespace

NamespaceRef::NamespaceRef(NamespaceRef&& other) noexcept : m_namespace(std::exchange(other.m_namespace, nullptr)) {}

auto NamespaceRef::operator=(NamespaceRef&& other) noexcept -> NamespaceRef& {
    if (this != &other) {
        reset();
        m_namespace = std::exchange(other.m_namespace, nullptr);
    }
    return *this;
}

NamespaceRef::~NamespaceRef() { reset(); }

void NamespaceRef::reset() {
    if (auto* old = std::exchange(m_namespace, nullptr); old != nullptr) {
        // A slot cannot be prepared again until all exact owners drop it.
        old->references.fetch_sub(1, std::memory_order_acq_rel);
    }
}

auto NamespaceRef::retain(NamespaceRef& out) const -> int {
    NamespaceRef candidate;
    if (m_namespace != nullptr) {
        auto count = m_namespace->references.load(std::memory_order_relaxed);
        for (;;) {
            if (count == 0 || count >= RESERVED - 1) return -EOVERFLOW;
            if (m_namespace->references.compare_exchange_weak(count, count + 1, std::memory_order_relaxed)) break;
        }
        candidate.m_namespace = m_namespace;
    }
    out = std::move(candidate);
    return 0;
}

auto NamespaceRef::generation() const -> uint64_t { return m_namespace != nullptr ? m_namespace->generation : 0; }

auto NamespaceRef::closed() const -> bool { return m_namespace != nullptr && m_namespace->closed.load(std::memory_order_acquire); }

void NamespaceRef::close() const {
    if (m_namespace != nullptr) m_namespace->closed.store(true, std::memory_order_release);
}

auto create_namespace(NamespaceRef& out) -> int {
    for (auto& slot : namespaces) {
        uint64_t expected = 0;
        if (!slot.references.compare_exchange_strong(expected, RESERVED, std::memory_order_acquire)) continue;
        auto const GENERATION = allocate_generation();
        if (GENERATION == 0) {
            slot.references.store(0, std::memory_order_release);
            return -EOVERFLOW;
        }
        slot.generation = GENERATION;
        slot.closed.store(false, std::memory_order_relaxed);
        slot.references.store(1, std::memory_order_release);
        NamespaceRef candidate;
        candidate.m_namespace = &slot;
        out = std::move(candidate);
        return 0;
    }
    return -ENOSPC;
}

auto live_namespace_count() -> size_t {
    size_t count = 0;
    for (auto const& slot : namespaces) {
        if (slot.references.load(std::memory_order_acquire) != 0) ++count;
    }
    return count;
}

}  // namespace ker::mod::sched::ipc
