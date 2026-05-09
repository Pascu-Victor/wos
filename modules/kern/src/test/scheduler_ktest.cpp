#include <cstdint>
#include <platform/sched/run_heap.hpp>
#include <platform/sched/task.hpp>
#include <test/ktest.hpp>

// ---------------------------------------------------------------------------
// Pure EEVDF math tests — no real tasks, no scheduler calls.
// ---------------------------------------------------------------------------

// kNiceToWeight values (nice=0 → 1024, nice=5 → 335, nice=-5 → 3121).
// Hardcoded to match the table in scheduler.cpp.
static constexpr uint32_t WEIGHT_NICE_0 = 1024;
static constexpr uint32_t WEIGHT_NICE_P5 = 335;   // nice=+5 (lower prio)
static constexpr uint32_t WEIGHT_NICE_N5 = 3121;  // nice=-5 (higher prio)

KTEST(Sched, VruntimeOrdering) {
    // vruntime delta = elapsed_ns * 1024 / weight
    // Lower weight → larger delta → vruntime accumulates faster.
    constexpr uint64_t ELAPSED_NS = 1'000'000ULL;  // 1 ms

    uint64_t dv_nice0 = (ELAPSED_NS * 1024ULL) / WEIGHT_NICE_0;
    uint64_t dv_nice_p5 = (ELAPSED_NS * 1024ULL) / WEIGHT_NICE_P5;
    uint64_t dv_nice_n5 = (ELAPSED_NS * 1024ULL) / WEIGHT_NICE_N5;

    // Higher nice → lower weight → faster vruntime accumulation
    KEXPECT_TRUE(dv_nice_p5 > dv_nice0);
    KEXPECT_TRUE(dv_nice0 > dv_nice_n5);
    KEXPECT_TRUE(dv_nice_n5 > 0ULL);
}

KTEST(Sched, DeadlineComputation) {
    // vdeadline = vruntime + (sliceNs * 1024) / weight
    // A task with lower weight gets a larger deadline increment,
    // so for equal vruntimes it is considered less urgent.
    constexpr uint64_t SLICE_NS = 4'000'000ULL;  // 4 ms
    constexpr int64_t VRUNTIME = 1'000'000LL;

    int64_t dl_w0 = VRUNTIME + static_cast<int64_t>((SLICE_NS * 1024ULL) / WEIGHT_NICE_0);
    int64_t dl_wp5 = VRUNTIME + static_cast<int64_t>((SLICE_NS * 1024ULL) / WEIGHT_NICE_P5);
    int64_t dl_wn5 = VRUNTIME + static_cast<int64_t>((SLICE_NS * 1024ULL) / WEIGHT_NICE_N5);

    // Lower weight (higher nice) → larger deadline → less urgent
    KEXPECT_TRUE(dl_wp5 > dl_w0);
    KEXPECT_TRUE(dl_w0 > dl_wn5);
}

// ---------------------------------------------------------------------------
// RunHeap ordering test — push tasks with known deadlines, verify min-order.
// ---------------------------------------------------------------------------

// File-scope to avoid __cxa_guard_acquire (unavailable in freestanding kernel).
// Task embeds a 512-byte FxState; keeping these off the stack is also safer.
static constexpr int HEAP_TEST_N = 5;
static ker::mod::sched::task::Task g_heap_tasks[HEAP_TEST_N];  // NOLINT

KTEST(Sched, RunHeapOrder) {
    using namespace ker::mod::sched;

    constexpr int N = HEAP_TEST_N;
    auto* tasks = static_cast<task::Task*>(g_heap_tasks);

    // Reset relevant fields; static zero-init leaves heapIndex=0 which
    // RunHeap treats as "already in heap", so force -1 explicitly.
    constexpr int64_t DEADLINES[N] = {500, 100, 300, 200, 400};
    for (int i = 0; i < N; ++i) {
        tasks[i].heap_index = -1;
        tasks[i].vdeadline = DEADLINES[i];
        tasks[i].vruntime = 0;
    }

    RunHeap heap{};
    heap.init();

    for (int i = 0; i < N; ++i) {
        KEXPECT_TRUE(heap.insert(&tasks[i]));
    }
    KEXPECT_EQ(static_cast<int>(heap.size), N);

    // Pop in deadline order; each successive peekMin must be >= the previous.
    int64_t prev_dl = INT64_MIN;
    for (int i = 0; i < N; ++i) {
        task::Task* t = heap.peek_min();
        KREQUIRE_NE(t, nullptr);
        KEXPECT_TRUE(t->vdeadline >= prev_dl);
        prev_dl = t->vdeadline;
        KEXPECT_TRUE(heap.remove(t));
    }
    KEXPECT_EQ(static_cast<int>(heap.size), 0);
}
