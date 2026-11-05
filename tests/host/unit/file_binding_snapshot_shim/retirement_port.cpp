#include <array>
#include <platform/mm/file_mapping_lifetime.hpp>
#include <platform/mm/lifetime_retirement_executor.hpp>
#include <snapshot_fixture.hpp>

namespace fm = ker::mod::mm::file_mapping;
namespace fixture = snapshot_fixture;

namespace snapshot_fixture {
namespace {
enum class QueueState { FREE, RESERVED, READY, RUNNING };
struct Slot {
    fm::LifetimeRetirementTicket ticket{};
    fm::LifetimeRetirementRun run{};
    void* argument{};
    QueueState state{};
};
std::array<Slot, 128> slots{};
Spinlock queue_lock{};

auto reserve(void*, fm::LifetimeRetirementRun run, void* argument) noexcept -> fm::LifetimeRetirementTicket* {
    check(lock_depth == 0 && index_depth == 0 && run != nullptr && argument != nullptr);
    auto const flags = queue_lock.lock_irqsave();
    if (fail_reserve_after == 0) {
        queue_lock.unlock_irqrestore(flags);
        return nullptr;
    }
    for (auto& slot : slots) {
        if (slot.state != QueueState::FREE) continue;
        if (fail_reserve_after > 0) --fail_reserve_after;
        slot.run = run;
        slot.argument = argument;
        slot.state = QueueState::RESERVED;
        queue_lock.unlock_irqrestore(flags);
        return &slot.ticket;
    }
    queue_lock.unlock_irqrestore(flags);
    return nullptr;
}
void submit(void*, fm::LifetimeRetirementTicket& ticket) noexcept {
    auto const flags = queue_lock.lock_irqsave();
    for (auto& slot : slots) {
        if (&slot.ticket != &ticket) continue;
        check(slot.state == QueueState::RESERVED);
        slot.state = QueueState::READY;
        queue_lock.unlock_irqrestore(flags);
        return;
    }
    std::abort();
}
auto observe(void*, fm::LifetimeRetirementTicket&, fm::LifetimeRetirementRun, void*, fm::RetirementCompletion&) noexcept -> bool {
    return false;  // Real generation-return observations belong to native tests.
}
auto cancel(void*, fm::LifetimeRetirementTicket&, fm::LifetimeRetirementRun, void*) noexcept -> bool { return false; }
const fm::LifetimeRetirementPort port{nullptr, reserve, submit, observe, cancel};
}  // namespace

auto run_one() -> bool {
    check(lock_depth == 0 && index_depth == 0);
    Slot* selected = nullptr;
    auto const flags = queue_lock.lock_irqsave();
    for (auto& slot : slots) {
        if (slot.state != QueueState::READY) continue;
        selected = &slot;
        selected->state = QueueState::RUNNING;
        break;
    }
    queue_lock.unlock_irqrestore(flags);
    if (selected == nullptr) return false;
    last_callback_result = selected->run(selected->argument);
    ++callbacks;
    auto const finished_flags = queue_lock.lock_irqsave();
    selected->state = QueueState::FREE;
    selected->run = nullptr;
    selected->argument = nullptr;
    queue_lock.unlock_irqrestore(finished_flags);
    return true;
}
void drain() {
    for (size_t i = 0; i <= slots.size(); ++i)
        if (!run_one()) return;
    std::abort();
}
auto queue_empty() -> bool {
    for (auto const& slot : slots)
        if (slot.state != QueueState::FREE) return false;
    return true;
}
}  // namespace snapshot_fixture

namespace ker::mod::mm::file_mapping {
auto LifetimeRetirementExecutor::port() noexcept -> const LifetimeRetirementPort* { return &fixture::port; }
// Unused completion interfaces cannot manufacture host callback-return credit.
RetirementCompletion::RetirementCompletion(RetirementCompletion&&) noexcept = default;
auto RetirementCompletion::operator=(RetirementCompletion&&) noexcept -> RetirementCompletion& = default;
RetirementCompletion::~RetirementCompletion() = default;
void RetirementCompletion::reset() noexcept { fixture::check(empty()); }
auto RetirementCompletion::query() const noexcept -> RetirementReturn {
    fixture::check(empty());
    return {};
}
}  // namespace ker::mod::mm::file_mapping
