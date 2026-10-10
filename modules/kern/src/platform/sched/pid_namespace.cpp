#include "pid_namespace.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <platform/sys/spinlock.hpp>
#include <utility>

namespace ker::mod::sched::pid_ns {

struct Namespace {
    std::atomic<uint64_t> references{};
    uint64_t generation{};
    std::atomic<bool> closed{};
    std::atomic<uint32_t> next_id{1};
    sys::Spinlock publication;
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

NamespaceRef::NamespaceRef(NamespaceRef&& other) noexcept : namespace_(std::exchange(other.namespace_, nullptr)) {}

auto NamespaceRef::operator=(NamespaceRef&& other) noexcept -> NamespaceRef& {
    if (this != &other) {
        reset();
        namespace_ = std::exchange(other.namespace_, nullptr);
    }
    return *this;
}

NamespaceRef::~NamespaceRef() { reset(); }

void NamespaceRef::reset() {
    if (auto* old = std::exchange(namespace_, nullptr); old != nullptr) old->references.fetch_sub(1, std::memory_order_acq_rel);
}

auto NamespaceRef::retain(NamespaceRef& out) const -> int {
    NamespaceRef candidate;
    if (namespace_ != nullptr) {
        auto count = namespace_->references.load(std::memory_order_relaxed);
        for (;;) {
            if (count == 0 || count >= RESERVED - 1) return -EOVERFLOW;
            if (namespace_->references.compare_exchange_weak(count, count + 1, std::memory_order_relaxed)) break;
        }
        candidate.namespace_ = namespace_;
    }
    out = std::move(candidate);
    return 0;
}

auto NamespaceRef::generation() const -> uint64_t { return namespace_ != nullptr ? namespace_->generation : 0; }

auto NamespaceRef::closed() const -> bool { return namespace_ != nullptr && namespace_->closed.load(std::memory_order_acquire); }

auto NamespaceRef::allocate_id(uint32_t& out) const -> int {
    if (namespace_ == nullptr) return -EINVAL;
    if (closed()) return -EIDRM;
    auto id = namespace_->next_id.load(std::memory_order_relaxed);
    for (;;) {
        if (id == 0 || id > MAX_LOCAL_ID) return -EOVERFLOW;
        if (namespace_->next_id.compare_exchange_weak(id, id + 1, std::memory_order_relaxed)) {
            out = id;
            return 0;
        }
    }
}

void NamespaceRef::close() const {
    if (namespace_ == nullptr) return;
    auto const FLAGS = namespace_->publication.lock_irqsave();
    namespace_->closed.store(true, std::memory_order_release);
    namespace_->publication.unlock_irqrestore(FLAGS);
}

PublicationGuard::PublicationGuard(const NamespaceRef& scope) : namespace_(scope.namespace_) {
    if (namespace_ != nullptr) flags_ = namespace_->publication.lock_irqsave();
}

PublicationGuard::~PublicationGuard() {
    if (namespace_ != nullptr) namespace_->publication.unlock_irqrestore(flags_);
}

auto PublicationGuard::open() const -> bool { return namespace_ == nullptr || !namespace_->closed.load(std::memory_order_acquire); }

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
        slot.next_id.store(1, std::memory_order_relaxed);
        slot.references.store(1, std::memory_order_release);
        NamespaceRef candidate;
        candidate.namespace_ = &slot;
        out = std::move(candidate);
        return 0;
    }
    return -ENOSPC;
}

auto live_namespace_count() -> size_t {
    size_t count = 0;
    for (auto const& slot : namespaces)
        if (slot.references.load(std::memory_order_acquire) != 0) ++count;
    return count;
}

}  // namespace ker::mod::sched::pid_ns
