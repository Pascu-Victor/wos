#include <array>
#include <platform/ktime/timer_queue.hpp>
#include <test/ktest.hpp>

namespace {
void timer_test_callback(void* context) { ++*static_cast<unsigned*>(context); }
}  // namespace

KTEST(TimerQueue, BoundedStorm) {
    using namespace ker::mod::time;
    TimerQueue queue;
    std::array<TimerNode, 128> nodes;
    std::array<TimerToken, 8> tokens{};
    unsigned callbacks = 0;
    for (auto& node : nodes) KEXPECT_TRUE(queue.arm_at(node, 10, timer_test_callback, &callbacks));
    KEXPECT_TRUE(queue.tree_height() <= 10);
    KEXPECT_EQ(queue.extract_due(9, tokens), size_t{0});
    for (size_t batch = 0; batch < nodes.size() / tokens.size(); ++batch) {
        size_t const COUNT = queue.extract_due(10, tokens);
        KEXPECT_EQ(COUNT, tokens.size());
        KEXPECT_EQ(callbacks, static_cast<unsigned>(batch * tokens.size()));
        for (size_t i = 0; i < COUNT; ++i) {
            TimerInvocation invocation;
            bool const CLAIMED = queue.claim(tokens[i], invocation);
            KEXPECT_TRUE(CLAIMED);
            if (CLAIMED) invocation.callback(invocation.context);
            KEXPECT_TRUE(queue.finish(tokens[i], 10));
        }
    }
    KEXPECT_EQ(callbacks, 128U);
    KEXPECT_EQ(queue.size(), size_t{0});
}

KTEST(TimerQueue, CancellationGenerationAndPeriodicPhase) {
    using namespace ker::mod::time;
    TimerQueue queue;
    TimerNode node;
    std::array<TimerToken, 1> tokens{};
    unsigned callbacks = 0;
    KEXPECT_TRUE(queue.arm_at(node, 10, timer_test_callback, &callbacks, 7));
    KEXPECT_EQ(queue.extract_due(10, tokens), size_t{1});
    TimerToken const OLD = tokens[0];
    KEXPECT_TRUE(queue.cancel(node) == TimerCancel::CANCELLED);
    KEXPECT_TRUE(queue.arm_at(node, 10, timer_test_callback, &callbacks, 7));
    TimerInvocation invocation;
    KEXPECT_FALSE(queue.claim(OLD, invocation));
    KEXPECT_EQ(queue.extract_due(100, tokens), size_t{1});
    KEXPECT_TRUE(queue.claim(tokens[0], invocation));
    KEXPECT_TRUE(queue.finish(tokens[0], 100));
    KEXPECT_EQ(node.deadline(), uint64_t{101});
    KEXPECT_EQ(queue.extract_due(101, tokens), size_t{1});
    KEXPECT_TRUE(queue.claim(tokens[0], invocation));
    KEXPECT_TRUE(queue.cancel(node) == TimerCancel::RUNNING);
    KEXPECT_TRUE(queue.finish(tokens[0], 101));
    KEXPECT_EQ(queue.size(), size_t{0});
}
