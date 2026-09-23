#include "selftest.hpp"

#ifdef WOS_SELFTEST
#include <cerrno>
#include <dev/dma/dma.hpp>
#include <dev/ivshmem/callback.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/power/power.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>

namespace ker::dev::ivshmem {
namespace {
using log = mod::dbg::logger<"ivshmem-test">;
PciResources* resource = nullptr;
const char* driver = nullptr;
int (*retire_driver)() = nullptr;
bool (*late_calls)() = nullptr;
void (*extra_lease)(bool) = nullptr;
std::atomic<bool> entered{false}, release_owner{false}, released{false}, returned{false}, done{false};
std::atomic<unsigned> protected_yields{0};
std::atomic<int> result{-EBUSY};

template <typename Predicate>
auto wait(Predicate predicate, uint64_t timeout_ms = 10000) -> bool {
    auto const START = mod::time::get_ms();
    while (!predicate()) {
        if (mod::time::get_ms() - START >= timeout_ms) return false;
        mod::sched::kern_sleep_us(1000);
    }
    return true;
}

void hold_until_released() {
    entered.store(true, std::memory_order_release);
    auto const START = mod::time::get_ms();
    while (!release_owner.load(std::memory_order_acquire) && mod::time::get_ms() - START < 20000) {
        mod::sched::kern_sleep_us(1000);
        auto* current = mod::sched::get_current_task();
        if (mod::sched::kernel_threads_shutdown_requested() && current->kernel_shutdown_requested.load(std::memory_order_acquire))
            protected_yields.fetch_add(1, std::memory_order_release);
    }
}
void holder() {
    if (selftest_flag("--ivshmem-test-hold-lease")) {
        auto const HOLD = mod::sched::hold_kernel_shutdown();
        extra_lease(true);
        hold_until_released();
        extra_lease(false);
    } else {
        CallbackGuard const GUARD(*resource);
        if (!GUARD) return;
        hold_until_released();
    }
    released.store(true, std::memory_order_release);
    mod::sched::kern_yield();
    returned.store(true, std::memory_order_release);
}
void retirer() {
    auto const HOLD = mod::sched::hold_kernel_shutdown();
    result.store(retire_driver(), std::memory_order_release);
    done.store(true, std::memory_order_release);
}
auto post(const char* name, void (*entry)(), uint64_t cpu) -> mod::sched::task::Task* {
    auto* task = mod::sched::task::Task::create_kernel_thread(name, entry);
    if (task == nullptr || !task->try_acquire()) return nullptr;
    mod::sched::post_task_pinned_cpu(cpu, task);
    return task;
}
void runner() {
    mod::sched::kern_sleep_us(3'000'000);
    bool const MSIX = resource->mode == model::Mode::msix;
    bool ok = dma::stats(resource->device).regions == 0;
    if (MSIX) {
        {
            CallbackGuard const GUARD(*resource);
            if (GUARD) resource->ring(static_cast<uint16_t>(resource->regs[2]));
        }
        ok = wait([] { return resource->test_irqs.load(std::memory_order_acquire) != 0; }) && ok;
    }
    log::info("%s live ready=%u mode=%s IRQ=%lu", driver, ok, MSIX ? "msix" : "plain", resource->test_irqs.load());
    if (!ok) mod::dbg::panic_handler("ivshmem terminal fixture failed before retirement");
    if (selftest_flag("--ivshmem-test-terminal-power")) {
        log::info("%s ordinary power finalizer fixture", driver);
        mod::power::selftest_shutdown();
    }
    auto* owner = post("ivshmem_owner", holder, 2);
    ok = owner != nullptr && wait([] { return entered.load(std::memory_order_acquire); });
    auto* retiring = ok ? post("ivshmem_retire", retirer, 3) : nullptr;
    ok = retiring != nullptr && wait([] { return resource->callbacks.closed(); }) && ok;
    bool const LEASE = selftest_flag("--ivshmem-test-hold-lease");
    if (LEASE) ok = wait([] { return resource->callbacks.drained(); }) && ok;
    bool const WAITED = ok && (LEASE || !resource->callbacks.drained()) && !done.load(std::memory_order_acquire);
    auto const REQUEST = mod::sched::request_kernel_threads_shutdown(1000);
    bool const DEFERRED = ok && wait([] { return protected_yields.load(std::memory_order_acquire) >= 2; });
    bool const DRAIN_FAIL = selftest_flag("--ivshmem-test-withhold-drain");
    if (DRAIN_FAIL) ok = wait([] { return done.load(std::memory_order_acquire); }) && ok;
    release_owner.store(true, std::memory_order_release);
    bool const RETIRED = wait([] { return done.load(std::memory_order_acquire); });
    bool const EXITED = owner != nullptr && wait([owner] { return owner->exit_in_progress.load(std::memory_order_acquire); });
    bool const WITHHELD = DRAIN_FAIL || selftest_flag("--ivshmem-test-withhold-stop");
    int const EXPECTED = WITHHELD ? -EIO : 0;
    bool const BME_OFF =
        (pci::pci_config_read16(resource->device->bus, resource->device->slot, resource->device->function, pci::PCI_COMMAND) &
         pci::PCI_COMMAND_BUS_MASTER) == 0;
    bool const LATE = RETIRED && EXITED && late_calls();
    int const AGAIN = retire_driver();
    ok = ok && WAITED && DEFERRED && RETIRED && EXITED && released.load() && !returned.load() && resource->callbacks.drained() && LATE &&
         BME_OFF && result.load() == EXPECTED && AGAIN == EXPECTED && resource->irq_owned == (MSIX && WITHHELD) &&
         dma::stats(resource->device).regions == 0;
    log::info(
        "%s terminal %s waited=%u deferred=%u protected=%u requested=%zu exit=%u late=%u result=%d repeat=%d BME-off=%u IRQ-owned=%u "
        "DMA=%zu BAR-retained=1",
        driver, ok ? "PASS" : "FAIL", WAITED, DEFERRED, protected_yields.load(), REQUEST.requested, EXITED, LATE, result.load(), AGAIN,
        BME_OFF, resource->irq_owned, dma::stats(resource->device).regions);
    if (owner != nullptr) owner->release();
    if (retiring != nullptr) retiring->release();
    if (!ok) mod::dbg::panic_handler("ivshmem terminal ownership test failed");
    mod::power::selftest_shutdown();
}
}  // namespace

void selftest_start(PciResources* resources, const char* kind, int (*retire)(), bool (*late)(), void (*lease)(bool)) {
    if (!selftest_flag("--ivshmem-test-terminal") && !selftest_flag("--ivshmem-test-terminal-power")) return;
    if (resource != nullptr) return;
    if (mod::smt::get_core_count() < 4) mod::dbg::panic_handler("ivshmem terminal test needs four CPUs");
    resource = resources;
    driver = kind;
    retire_driver = retire;
    late_calls = late;
    extra_lease = lease;
    if (selftest_flag("--ivshmem-test-hold-lease") && lease == nullptr) mod::dbg::panic_handler("ivshmem fixture has no extra lease");
    auto* task = post("ivshmem_test", runner, 0);
    if (task == nullptr) mod::dbg::panic_handler("ivshmem terminal test task allocation failed");
    task->release();
}
}  // namespace ker::dev::ivshmem
#endif
