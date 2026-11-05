#include <array>
#include <atomic>
#include <dev/firmware/boot_source.hpp>
#include <dev/firmware/firmware.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <string_view>
#include <test/firmware_runtime_ktest.hpp>

namespace ker::test {
namespace {
namespace fw = ker::dev::firmware;
namespace sched = ker::mod::sched;
namespace task = sched::task;
using log = ker::mod::dbg::logger<"firmware-test">;
constexpr uint64_t WAIT_US = 10'000'000;
std::atomic<bool> started{false};
// Independent trusted catalog, not data read from the supplied archive.
constexpr fw::Policy POLICY{"wos/goal41.bin",
                            31,
                            31,
                            {0x6c, 0xf7, 0xc3, 0xc7, 0x61, 0x3f, 0xb9, 0x0a, 0x0f, 0x03, 0x39, 0xac, 0xfb, 0xa9, 0x03, 0x25,
                             0xb3, 0x0b, 0xc8, 0xd7, 0xb1, 0x18, 0x2e, 0x75, 0xb0, 0xe6, 0x2c, 0x42, 0x87, 0x5e, 0x88, 0x6e},
                            1};

void require(bool condition, const char* reason) {
    if (condition) return;
    log::error("FIRMWARE_RUNTIME FAIL %s", reason);
    ker::mod::dbg::panic_handler("scheduled firmware selftest failed");
}

auto read_stats() -> fw::Stats {
    fw::Stats result{};
    require(fw::snapshot(result) == fw::Error::none, "ordinary metadata snapshot");
    return result;
}

struct Worker {
    task::Task* retained = nullptr;
    fw::View view{};
    bool explicit_release = false;
    std::atomic<bool>* gate = nullptr;
    std::atomic<uint32_t> ready{0};
    std::atomic<bool> finish{false};
};

void worker_entry() {
    auto* owner = sched::get_current_task();
    auto* box = static_cast<Worker*>(owner->kernel_thread_context);
    require(box != nullptr, "worker owns stable mailbox");
    // No outer shutdown hold: the actual public Scope must protect this worker
    // across sleeps while the coordinator observes a live shared reference.
    {
        fw::Scope scope;
        box->ready.store(1, std::memory_order_release);
        auto const START_DEADLINE = sched::saturating_deadline_us(ker::mod::time::get_us(), WAIT_US);
        while (!box->gate->load(std::memory_order_acquire)) {
            require(ker::mod::time::get_us() < START_DEADLINE, "worker bounded start barrier");
            sched::kern_sleep_us(1000);
        }
        require(scope.request(POLICY, 1, box->view) == fw::Error::none, "worker trusted request");
        require(owner->kernel_shutdown_ownership.held(), "Scope retains shutdown ownership");
        box->ready.store(2, std::memory_order_release);
        auto const DEADLINE = sched::saturating_deadline_us(ker::mod::time::get_us(), WAIT_US);
        while (!box->finish.load(std::memory_order_acquire)) {
            require(ker::mod::time::get_us() < DEADLINE, "worker finite retained wait");
            sched::kern_sleep_us(1000);
        }
        require(owner->kernel_shutdown_ownership.held(), "Scope survives voluntary scheduling");
        if (box->explicit_release) {
            require(scope.release(box->view) == fw::Error::none, "explicit lease release");
            require(scope.release(box->view) == fw::Error::stale, "duplicate lease release refused");
        }
    }
    require(!owner->kernel_shutdown_ownership.held(), "Scope releases shutdown ownership last");
    owner->kernel_thread_context = nullptr;
    box->ready.store(3, std::memory_order_release);  // Final mailbox access.
    sched::exit_current_kernel_thread();
}

void wait_ready(Worker& worker, uint32_t state) {
    auto const DEADLINE = sched::saturating_deadline_us(ker::mod::time::get_us(), WAIT_US);
    while (worker.ready.load(std::memory_order_acquire) != state) {
        require(ker::mod::time::get_us() < DEADLINE, "coordinator bounded mailbox wait");
        sched::kern_sleep_us(1000);
    }
}

void finish_worker(Worker& worker) {
    worker.finish.store(true, std::memory_order_release);
    wait_ready(worker, 3);
    auto const DEADLINE = sched::saturating_deadline_us(ker::mod::time::get_us(), WAIT_US);
    while (worker.retained->state.load(std::memory_order_acquire) != task::TaskState::DEAD) {
        require(ker::mod::time::get_us() < DEADLINE, "actual DEAD join before mailbox destruction");
        sched::kern_sleep_us(1000);
    }
    require(worker.retained->kernel_thread_context == nullptr, "worker detached mailbox before DEAD");
    worker.retained->release();
    worker.retained = nullptr;
}

void sharing() {
    auto const BASE = read_stats();
    require(BASE.objects == 0 && BASE.references == 0, "fresh cache baseline");
    std::array<Worker, 2> workers{};
    std::atomic<bool> gate{false};
    for (size_t i = 0; i < workers.size(); ++i) {
        auto& worker = workers.at(i);
        worker.explicit_release = i == 0;
        worker.gate = &gate;
        worker.retained = task::Task::create_kernel_thread(i == 0 ? "firmware-a" : "firmware-b", worker_entry);
        if (worker.retained == nullptr) {
            gate.store(true, std::memory_order_release);
            for (size_t prior = 0; prior < i; ++prior) {
                wait_ready(workers.at(prior), 2);
                finish_worker(workers.at(prior));
            }
        }
        require(worker.retained != nullptr, "create actual scheduled worker");
        uint32_t refs = 1;
        require(worker.retained->ref_count.compare_exchange_strong(refs, 2, std::memory_order_acq_rel), "exact fresh Task join lease");
        worker.retained->kernel_thread_context = &worker;
        if (!sched::post_task_balanced(worker.retained)) {
            worker.retained->kernel_thread_context = nullptr;
            worker.retained->release();
            task::destroy_unpublished_user_thread(worker.retained);
            worker.retained = nullptr;
            gate.store(true, std::memory_order_release);
            for (size_t prior = 0; prior < i; ++prior) {
                wait_ready(workers.at(prior), 2);
                finish_worker(workers.at(prior));
            }
            require(false, "worker post failed with owned rollback");
        }
    }
    for (auto& worker : workers) wait_ready(worker, 1);
    gate.store(true, std::memory_order_release);
    for (auto& worker : workers) wait_ready(worker, 2);
    auto const BOTH = read_stats();
    require(BOTH.objects == 1 && BOTH.references == 2 && BOTH.verifications == BASE.verifications + 1 && BOTH.hits == BASE.hits + 1,
            "two tasks share one verification and exact retained object");
    require(workers.at(0).view.object == workers.at(1).view.object && workers.at(0).view.bytes.data() == workers.at(1).view.bytes.data(),
            "actual object generation and immutable bytes shared");
    {
        fw::Scope foreign;
        fw::View local{};
        require(foreign.request(POLICY, 1, local) == fw::Error::none, "foreign scope holds same object");
        require(foreign.release(workers.at(0).view) == fw::Error::stale, "foreign view rejected despite identical object");
        require(foreign.release(local) == fw::Error::none, "foreign scope retains own lease");
    }
    finish_worker(workers.at(0));
    require(read_stats().references == 1, "first return preserves second task reference");
    finish_worker(workers.at(1));
    auto const END = read_stats();
    require(END.objects == BASE.objects && END.references == BASE.references, "automatic destructor drains final owner");
    log::info("FIRMWARE_RUNTIME CASE_PASS actual_tasks_share_and_release");
}

void negative_and_cycles() {
    fw::Scope scope;
    fw::View untouched{{}, {9, 99}, 123, 456};
    auto expect = [&](fw::Policy policy, uint32_t revision, fw::Error error) {
        require(scope.request(policy, revision, untouched) == error, "trusted policy refusal");
        require(untouched.object == fw::Handle{9, 99} && untouched.scope == 123 && untouched.lease == 456 && untouched.bytes.empty(),
                "failed request preserves caller output");
        auto const STATE = read_stats();
        require(STATE.objects == 0 && STATE.references == 0, "failed request publishes no object");
    };
    auto bad = POLICY;
    bad.name = "wos/missing.bin";
    expect(bad, 1, fw::Error::not_found);
    bad = POLICY;
    bad.length = 30;
    expect(bad, 1, fw::Error::length);
    bad = POLICY;
    bad.maximum = fw::MAX_BLOB_BYTES + 1;
    expect(bad, 1, fw::Error::length);
    bad = POLICY;
    bad.sha256.at(0) ^= 1;
    expect(bad, 1, fw::Error::digest);
    expect(POLICY, 2, fw::Error::revision);
    auto const BEFORE = read_stats();
    fw::Handle previous{};
    for (size_t cycle = 0; cycle < 1000; ++cycle) {
        fw::View view{};
        require(scope.request(POLICY, 1, view) == fw::Error::none, "cycle trusted request");
        require(view.object != previous && view.bytes.size() == POLICY.length && view.bytes.front() == 'W' && view.bytes.back() == '\n',
                "fresh generation and expected immutable payload");
        previous = view.object;
        require(scope.release(view) == fw::Error::none && scope.release(view) == fw::Error::stale, "cycle balanced release");
        auto const STATE = read_stats();
        require(STATE.objects == 0 && STATE.references == 0, "every cycle returns to baseline");
    }
    require(read_stats().verifications == BEFORE.verifications + 1000, "each last-release eviction requires new verification");
    log::info("FIRMWARE_RUNTIME CASE_PASS negative_policy_and_1000_cycles");
}

void bounded_scope() {
    {
        sched::PreemptGuard const GUARD;
        fw::Scope denied;
        fw::View view{};
        require(denied.request(POLICY, 1, view) == fw::Error::context, "actual preempt-disabled context refused");
        fw::Stats unchanged{123, 0, 0, 0};
        require(fw::snapshot(unchanged) == fw::Error::context && unchanged.objects == 123, "unsafe snapshot leaves output unchanged");
    }
    {
        fw::Scope scope;
        fw::View view{};
        for (size_t i = 0; i < fw::SCOPE_LEASES; ++i)
            require(scope.request(POLICY, 1, view) == fw::Error::none, "scope admits bounded leases");
        require(scope.request(POLICY, 1, view) == fw::Error::scope_full, "scope refuses excess without acquisition");
        require(read_stats().references == fw::SCOPE_LEASES, "scope cap has exact reference count");
    }
    require(read_stats().references == 0 && read_stats().objects == 0, "Scope destructor drains bounded array");
    log::info("FIRMWARE_RUNTIME CASE_PASS scope_capacity_drains");
}

void wait_for_init_mounts() {
    // The tiny firmware workload finishes before PID 1's first XFS mount.
    // Observe its actual admitted root after pivot instead of racing shutdown
    // against mount construction or guessing a fixed startup delay.
    auto const DEADLINE = sched::saturating_deadline_us(ker::mod::time::get_us(), 30'000'000);
    for (;;) {
        auto* init = sched::find_task_by_pid_safe(1);
        if (init != nullptr) {
            task::Task::FilesystemContext context;
            bool const READY = init->capture_filesystem_context(context) && context.root_len == 7 &&
                               std::string_view(context.root.data(), context.root_len) == "/rootfs";
            init->release();
            if (READY) {
                log::info("FIRMWARE_RUNTIME INIT_READY root=/rootfs");
                return;
            }
        }
        require(ker::mod::time::get_us() < DEADLINE, "PID 1 completes actual root mount/pivot before test shutdown");
        sched::kern_sleep_us(10000);
    }
}

void runtime_entry() {
    {
        auto const HOLD = sched::hold_kernel_shutdown();
        require(ker::mod::mm::phys::can_wait_for_reclaim(), "ordinary scheduled test context");
        require(fw::source_status() == fw::Error::none, "qualified immutable tagged boot source");
        wait_for_init_mounts();
        log::info("FIRMWARE_RUNTIME BEGIN");
        sharing();
        negative_and_cycles();
        bounded_scope();
        auto const END = read_stats();
        require(END.objects == 0 && END.references == 0, "all firmware owners return before shutdown");
        log::info("FIRMWARE_RUNTIME PASS cases=3 cycles=1000 objects=%lu references=%lu verifications=%lu hits=%lu", END.objects,
                  END.references, END.verifications, END.hits);
    }
    {
        auto const HOLD = sched::hold_kernel_shutdown();
        auto* init = sched::find_task_by_pid_safe(1);
        require(init != nullptr, "retain PID 1 for ordinary userspace shutdown request");
        // WOS SIGUSR2 (12) is PID 1's normal poweroff request. Let init finish
        // its startup, stop services and journald, sync and enter the ordinary
        // power finalizer itself; do not stop providers beneath live init work.
        constexpr int WOS_SIGUSR2 = 12;
        init->signal_generate_pending_mask(uint64_t{1} << (WOS_SIGUSR2 - 1));
        sched::wake_task_for_signal(init);
        init->release();
        log::info("FIRMWARE_RUNTIME POWEROFF_REQUESTED via=init-sigusr2");
    }
    sched::exit_current_kernel_thread();
}
}  // namespace

auto start_firmware_runtime_selftest() -> bool {
    bool expected = false;
    if (!started.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return false;
    auto* worker = task::Task::create_kernel_thread("firmware_runtime_selftest", runtime_entry);
    if (worker == nullptr || !sched::post_task_balanced(worker)) {
        if (worker != nullptr) task::destroy_unpublished_user_thread(worker);
        started.store(false, std::memory_order_release);
        return false;
    }
    return true;
}
}  // namespace ker::test
