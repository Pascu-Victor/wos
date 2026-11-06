#include <array>
#include <mutex>
#include <object_pages.hpp>
#include <platform/mm/lifetime_retirement_executor.hpp>

namespace object_fixture {
namespace {
namespace fm = ker::mod::mm::file_mapping;
enum class State { free, reserved, ready, running };
struct Slot {
    fm::LifetimeRetirementTicket ticket;
    fm::LifetimeRetirementRun run{};
    void* argument{};
    State state{};
};
std::array<Slot, 128> slots;
std::mutex queue_lock;
size_t completed{};
auto reserve(void*, fm::LifetimeRetirementRun run, void* argument) noexcept -> fm::LifetimeRetirementTicket* {
    check(irq_depth == 0 && lock_depth == 0 && run != nullptr && argument != nullptr);
    std::lock_guard guard(queue_lock);
    if (fail_reserve_after == 0) return nullptr;
    for (auto& slot : slots) {
        if (slot.state != State::free) continue;
        if (fail_reserve_after > 0) --fail_reserve_after;
        slot.run = run;
        slot.argument = argument;
        slot.state = State::reserved;
        return &slot.ticket;
    }
    return nullptr;
}
void submit(void*, fm::LifetimeRetirementTicket& ticket) noexcept {
    check(lock_depth == 0 && mutex_depth == 0);
    std::lock_guard guard(queue_lock);
    for (auto& slot : slots) {
        if (&slot.ticket != &ticket) continue;
        check(slot.state == State::reserved);
        slot.state = State::ready;
        return;
    }
    std::abort();
}
auto cancel(void*, fm::LifetimeRetirementTicket& ticket, fm::LifetimeRetirementRun run, void* argument) noexcept -> bool {
    check(irq_depth == 0 && lock_depth == 0);
    std::lock_guard guard(queue_lock);
    for (auto& slot : slots) {
        if (&slot.ticket != &ticket || slot.state != State::reserved || slot.run != run || slot.argument != argument) continue;
        slot.state = State::free;
        slot.run = nullptr;
        slot.argument = nullptr;
        return true;
    }
    return false;
}
auto observe(void*, fm::LifetimeRetirementTicket&, fm::LifetimeRetirementRun, void*, fm::RetirementCompletion&) noexcept -> bool {
    return false;  // Exact executor generation-return proof belongs to native.
}
const fm::LifetimeRetirementPort port{nullptr, reserve, submit, observe, cancel};
}  // namespace
auto queued() -> size_t {
    std::lock_guard guard(queue_lock);
    size_t count = 0;
    for (auto const& slot : slots) count += slot.state == State::ready;
    return count;
}
auto reserved() -> size_t {
    std::lock_guard guard(queue_lock);
    size_t count = 0;
    for (auto const& slot : slots) count += slot.state != State::free;
    return count;
}
auto callbacks() -> size_t { return completed; }
auto run_one() -> bool {
    check(ordinary && irq_depth == 0 && lock_depth == 0 && mutex_depth == 0);
    Slot* selected = nullptr;
    {
        std::lock_guard guard(queue_lock);
        for (auto& slot : slots) {
            if (slot.state != State::ready) continue;
            selected = &slot;
            selected->state = State::running;
            break;
        }
    }
    if (selected == nullptr) return false;
    check(selected->run(selected->argument) == 0);
    {
        std::lock_guard guard(queue_lock);
        selected->state = State::free;
        selected->run = nullptr;
        selected->argument = nullptr;
        ++completed;
    }
    return true;
}
void drain() {
    for (size_t index = 0; index < slots.size(); ++index)
        if (!run_one()) return;
    std::abort();
}
}  // namespace object_fixture

namespace ker::mod::mm::file_mapping {
auto LifetimeRetirementExecutor::port() noexcept -> const LifetimeRetirementPort* {
    return object_fixture::available ? &object_fixture::port : nullptr;
}
}  // namespace ker::mod::mm::file_mapping
