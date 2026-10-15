#ifdef WOS_SELFTEST
#include <cstddef>
#include <new>
#include <platform/mm/lifecycle_storage.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <test/compartment_lifecycle_runtime.hpp>
#include <vfs/file.hpp>

// These actual kernel hooks exist in kmalloc.opt.cpp even when libc++ hides
// their declarations because the compiler disables sized-delete selection.
void operator delete(void* storage, std::size_t size) noexcept;
void operator delete[](void* storage, std::size_t size) noexcept;

namespace ker::mod::mm::lifecycle_storage {
namespace {
constinit sys::Spinlock counter_lock{};
constinit Snapshot current{};

class CounterGuard {
   public:
    CounterGuard() : flags_(counter_lock.lock_irqsave()) {}
    ~CounterGuard() { counter_lock.unlock_irqrestore(flags_); }
    CounterGuard(const CounterGuard&) = delete;
    auto operator=(const CounterGuard&) -> CounterGuard& = delete;

   private:
    uint64_t flags_;
};

auto counts_for(Kind kind) noexcept -> Counts* {
    switch (kind) {
        case Kind::TASK:
            return &current.task;
        case Kind::FILE:
            return &current.file;
        case Kind::FAMILY:
            return &current.family;
        case Kind::BINDING:
            return &current.binding;
    }
    current.valid = false;
    return nullptr;
}

void advance_sequence() noexcept {
    if (current.sequence == UINT64_MAX)
        current.valid = false;
    else
        ++current.sequence;
}
}  // namespace

void begin_allocation(Kind kind) noexcept {
    CounterGuard const GUARD;
    advance_sequence();
    auto* const COUNTS = counts_for(kind);
    if (COUNTS == nullptr) return;
    if (COUNTS->allocating == UINT64_MAX) {
        current.valid = false;
        return;
    }
    ++COUNTS->allocating;
}

void finish_allocation(Kind kind, bool allocated) noexcept {
    CounterGuard const GUARD;
    advance_sequence();
    auto* const COUNTS = counts_for(kind);
    if (COUNTS == nullptr) return;
    if (COUNTS->allocating == 0) {
        current.valid = false;
        return;
    }
    --COUNTS->allocating;
    if (!allocated) return;
    if (COUNTS->live_blocks == UINT64_MAX) {
        current.valid = false;
        return;
    }
    ++COUNTS->live_blocks;
}

void finish_deallocation(Kind kind) noexcept {
    CounterGuard const GUARD;
    advance_sequence();
    auto* const COUNTS = counts_for(kind);
    if (COUNTS == nullptr) return;
    if (COUNTS->live_blocks == 0) {
        current.valid = false;
        return;
    }
    --COUNTS->live_blocks;
}

auto snapshot() noexcept -> Snapshot {
    CounterGuard const GUARD;
    return current;
}

namespace {
template <Kind KIND, bool ARRAY>
auto allocate(std::size_t size) -> void* {
    begin_allocation(KIND);
    void* storage = nullptr;
    if constexpr (ARRAY)
        storage = ::operator new[](size);
    else
        storage = ::operator new(size);
    finish_allocation(KIND, storage != nullptr);
    return storage;
}

template <Kind KIND, bool ARRAY>
auto allocate(std::size_t size, const std::nothrow_t& tag) noexcept -> void* {
    begin_allocation(KIND);
    void* storage = nullptr;
    if constexpr (ARRAY)
        storage = ::operator new[](size, tag);
    else
        storage = ::operator new(size, tag);
    finish_allocation(KIND, storage != nullptr);
    return storage;
}
}  // namespace
}  // namespace ker::mod::mm::lifecycle_storage

// Actual types use ordinary default-aligned allocation. Raising either
// alignment requires a deliberate aligned-allocation instrumentation review.
static_assert(alignof(ker::mod::sched::task::Task) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);
static_assert(alignof(ker::vfs::File) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);

namespace ker::mod::sched::task {
namespace storage = ker::mod::mm::lifecycle_storage;

auto Task::operator new(std::size_t size) -> void* { return storage::allocate<storage::Kind::TASK, false>(size); }

auto Task::operator new(std::size_t size, const std::nothrow_t& tag) noexcept -> void* {
    return storage::allocate<storage::Kind::TASK, false>(size, tag);
}

auto Task::operator new(std::size_t size, void* place) noexcept -> void* { return ::operator new(size, place); }

void Task::operator delete(void* memory) noexcept {
    bool const ALLOCATED = memory != nullptr;
    auto const CLAIM = ker::test::lifecycle60::before_task_deallocation(memory);
    ::operator delete(memory);
    if (ALLOCATED) storage::finish_deallocation(storage::Kind::TASK);
    ker::test::lifecycle60::after_task_deallocation(CLAIM);
}

void Task::operator delete(void* memory, std::size_t size) noexcept {
    bool const ALLOCATED = memory != nullptr;
    auto const CLAIM = ker::test::lifecycle60::before_task_deallocation(memory);
    ::operator delete(memory, size);
    if (ALLOCATED) storage::finish_deallocation(storage::Kind::TASK);
    ker::test::lifecycle60::after_task_deallocation(CLAIM);
}

void Task::operator delete(void* memory, const std::nothrow_t& /*tag*/) noexcept {
    bool const ALLOCATED = memory != nullptr;
    auto const CLAIM = ker::test::lifecycle60::before_task_deallocation(memory);
    // The kernel has no global nothrow-delete definition; its free is unsized.
    ::operator delete(memory);
    if (ALLOCATED) storage::finish_deallocation(storage::Kind::TASK);
    ker::test::lifecycle60::after_task_deallocation(CLAIM);
}

void Task::operator delete(void* memory, void* place) noexcept { ::operator delete(memory, place); }

auto Task::operator new[](std::size_t size) -> void* { return storage::allocate<storage::Kind::TASK, true>(size); }

auto Task::operator new[](std::size_t size, const std::nothrow_t& tag) noexcept -> void* {
    return storage::allocate<storage::Kind::TASK, true>(size, tag);
}

auto Task::operator new[](std::size_t size, void* place) noexcept -> void* { return ::operator new[](size, place); }

void Task::operator delete[](void* memory) noexcept {
    bool const ALLOCATED = memory != nullptr;
    ::operator delete[](memory);
    if (ALLOCATED) storage::finish_deallocation(storage::Kind::TASK);
}

void Task::operator delete[](void* memory, std::size_t size) noexcept {
    bool const ALLOCATED = memory != nullptr;
    ::operator delete[](memory, size);
    if (ALLOCATED) storage::finish_deallocation(storage::Kind::TASK);
}

void Task::operator delete[](void* memory, const std::nothrow_t& /*tag*/) noexcept {
    bool const ALLOCATED = memory != nullptr;
    // The kernel has no global nothrow-delete definition; its free is unsized.
    ::operator delete[](memory);
    if (ALLOCATED) storage::finish_deallocation(storage::Kind::TASK);
}

void Task::operator delete[](void* memory, void* place) noexcept { ::operator delete[](memory, place); }
}  // namespace ker::mod::sched::task

namespace ker::vfs {
namespace storage = ker::mod::mm::lifecycle_storage;

auto File::operator new(std::size_t size) -> void* { return storage::allocate<storage::Kind::FILE, false>(size); }

auto File::operator new(std::size_t size, const std::nothrow_t& tag) noexcept -> void* {
    return storage::allocate<storage::Kind::FILE, false>(size, tag);
}

auto File::operator new(std::size_t size, void* place) noexcept -> void* { return ::operator new(size, place); }

void File::operator delete(void* memory) noexcept {
    bool const ALLOCATED = memory != nullptr;
    ::operator delete(memory);
    if (ALLOCATED) storage::finish_deallocation(storage::Kind::FILE);
}

void File::operator delete(void* memory, std::size_t size) noexcept {
    bool const ALLOCATED = memory != nullptr;
    ::operator delete(memory, size);
    if (ALLOCATED) storage::finish_deallocation(storage::Kind::FILE);
}

void File::operator delete(void* memory, const std::nothrow_t& /*tag*/) noexcept {
    bool const ALLOCATED = memory != nullptr;
    // The kernel has no global nothrow-delete definition; its free is unsized.
    ::operator delete(memory);
    if (ALLOCATED) storage::finish_deallocation(storage::Kind::FILE);
}

void File::operator delete(void* memory, void* place) noexcept { ::operator delete(memory, place); }

auto File::operator new[](std::size_t size) -> void* { return storage::allocate<storage::Kind::FILE, true>(size); }

auto File::operator new[](std::size_t size, const std::nothrow_t& tag) noexcept -> void* {
    return storage::allocate<storage::Kind::FILE, true>(size, tag);
}

auto File::operator new[](std::size_t size, void* place) noexcept -> void* { return ::operator new[](size, place); }

void File::operator delete[](void* memory) noexcept {
    bool const ALLOCATED = memory != nullptr;
    ::operator delete[](memory);
    if (ALLOCATED) storage::finish_deallocation(storage::Kind::FILE);
}

void File::operator delete[](void* memory, std::size_t size) noexcept {
    bool const ALLOCATED = memory != nullptr;
    ::operator delete[](memory, size);
    if (ALLOCATED) storage::finish_deallocation(storage::Kind::FILE);
}

void File::operator delete[](void* memory, const std::nothrow_t& /*tag*/) noexcept {
    bool const ALLOCATED = memory != nullptr;
    // The kernel has no global nothrow-delete definition; its free is unsized.
    ::operator delete[](memory);
    if (ALLOCATED) storage::finish_deallocation(storage::Kind::FILE);
}

void File::operator delete[](void* memory, void* place) noexcept { ::operator delete[](memory, place); }
}  // namespace ker::vfs

#endif
