#include "platform/sched/job_control_model.hpp"

#include <gtest/gtest.h>

#include <array>
#include <iostream>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace jc = ker::mod::sched::job_control;

TEST(JobControlBarrier, StopWaitsForEveryRunningMember) {
    jc::StopBarrier group;
    jc::Member leader, worker;
    ASSERT_TRUE(group.attach(leader));
    ASSERT_TRUE(group.attach(worker));
    ASSERT_TRUE(group.release(leader));
    ASSERT_TRUE(group.release(worker));
    auto ticket = group.request_stop(19);
    ASSERT_TRUE(ticket);
    EXPECT_FALSE(group.complete());
    ASSERT_TRUE(group.acknowledge(leader, *ticket));
    EXPECT_FALSE(group.complete());
    EXPECT_FALSE(group.release(leader));
    EXPECT_FALSE(group.release(worker));
    ASSERT_TRUE(group.acknowledge(worker, *ticket));
    EXPECT_TRUE(group.complete());
    EXPECT_TRUE(group.request_continue());
    EXPECT_TRUE(group.release(leader));
    EXPECT_TRUE(group.release(worker));
    EXPECT_EQ(group.quiescent_members(), 0U);
}

TEST(JobControlBarrier, BirthDuringStopCannotEscapeOrReopenCompletedBarrier) {
    jc::StopBarrier group;
    jc::Member creator, newborn;
    ASSERT_TRUE(group.attach(creator));
    ASSERT_TRUE(group.release(creator));
    auto ticket = group.request_stop(19);
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(group.attach(newborn));
    EXPECT_TRUE(newborn.quiescent());
    EXPECT_FALSE(group.release(newborn));
    EXPECT_FALSE(group.complete());
    ASSERT_TRUE(group.acknowledge(creator, *ticket));
    EXPECT_TRUE(group.complete());
    jc::Member later;
    ASSERT_TRUE(group.attach(later));
    EXPECT_TRUE(group.complete());
    EXPECT_FALSE(group.release(later));
}

TEST(JobControlBarrier, ExitOfUnacknowledgedWorkerCompletesRemainingGroup) {
    jc::StopBarrier group;
    jc::Member leader, worker;
    ASSERT_TRUE(group.attach(leader));
    ASSERT_TRUE(group.attach(worker));
    ASSERT_TRUE(group.release(worker));
    ASSERT_TRUE(group.request_stop(19));
    EXPECT_FALSE(group.complete());
    ASSERT_TRUE(group.detach(worker));
    EXPECT_TRUE(group.complete());
    ASSERT_TRUE(group.detach(leader));
    EXPECT_FALSE(group.complete());
    EXPECT_EQ(group.members(), 0U);
    EXPECT_EQ(group.quiescent_members(), 0U);
}

TEST(JobControlBarrier, ContinueCancelsIncompleteStopAndRejectsOldAcknowledgments) {
    jc::StopBarrier group;
    jc::Member worker;
    ASSERT_TRUE(group.attach(worker));
    ASSERT_TRUE(group.release(worker));
    auto first = group.request_stop(19);
    ASSERT_TRUE(first);
    ASSERT_TRUE(group.request_continue());
    EXPECT_FALSE(group.acknowledge(worker, *first));
    auto second = group.request_stop(20);
    ASSERT_TRUE(second);
    EXPECT_NE(first->generation, second->generation);
    EXPECT_FALSE(group.acknowledge(worker, *first));
    EXPECT_FALSE(group.complete());
    EXPECT_TRUE(group.acknowledge(worker, *second));
    EXPECT_TRUE(group.complete());
    EXPECT_EQ(group.stop_signal(), 20U);
}

TEST(JobControlBarrier, StopAheadOfResumeKeepsPhysicallyQuiescentThreadsStopped) {
    jc::StopBarrier group;
    jc::Member worker;
    ASSERT_TRUE(group.attach(worker));
    auto first = group.request_stop(19);
    ASSERT_TRUE(first);
    ASSERT_TRUE(group.complete());
    ASSERT_TRUE(group.request_continue());
    auto second = group.request_stop(21);
    ASSERT_TRUE(second);
    EXPECT_TRUE(group.complete());
    EXPECT_FALSE(group.release(worker));
    EXPECT_FALSE(group.acknowledge(worker, *first));
    EXPECT_TRUE(group.acknowledge(worker, *second));
}

TEST(JobControlBarrier, DuplicateAndForeignOperationsPreserveMembership) {
    jc::StopBarrier group, foreign;
    jc::Member worker;
    ASSERT_TRUE(group.attach(worker));
    EXPECT_FALSE(group.attach(worker));
    EXPECT_FALSE(foreign.attach(worker));
    EXPECT_FALSE(foreign.release(worker));
    EXPECT_FALSE(foreign.detach(worker));
    auto local_stop = group.request_stop(19);
    auto foreign_stop = foreign.request_stop(19);
    ASSERT_TRUE(local_stop);
    ASSERT_TRUE(foreign_stop);
    EXPECT_FALSE(group.acknowledge(worker, *foreign_stop));
    EXPECT_FALSE(foreign.acknowledge(worker, *local_stop));
    EXPECT_TRUE(group.acknowledge(worker, *local_stop));
    EXPECT_TRUE(group.acknowledge(worker, *local_stop));
    EXPECT_EQ(group.members(), 1U);
    EXPECT_EQ(group.quiescent_members(), 1U);
    EXPECT_EQ(foreign.members(), 0U);
    EXPECT_TRUE(group.detach(worker));
    EXPECT_FALSE(group.detach(worker));
    EXPECT_FALSE(group.acknowledge(worker, *local_stop));
}

TEST(JobControlBarrier, RepeatedStopsCoalesceAndInvalidSignalsDoNotMutate) {
    jc::StopBarrier group;
    jc::Member member;
    ASSERT_TRUE(group.attach(member));
    ASSERT_TRUE(group.release(member));
    for (uint32_t signal : {0U, 9U, 18U, 23U, UINT32_MAX}) EXPECT_FALSE(group.request_stop(signal));
    EXPECT_FALSE(group.stopping());
    EXPECT_EQ(group.generation(), 0U);
    auto first = group.request_stop(22);
    ASSERT_TRUE(first);
    auto second = group.request_stop(19);
    ASSERT_TRUE(second);
    EXPECT_EQ(first->generation, second->generation);
    EXPECT_EQ(group.stop_signal(), 22U);
    EXPECT_FALSE(group.request_stop(18));
    EXPECT_TRUE(group.stopping());
    EXPECT_EQ(group.quiescent_members(), 0U);
}

TEST(JobControlBarrier, ExhaustionNeverWrapsOrPartiallyAppliesNewStop) {
    jc::StopBarrier group(UINT64_MAX - 1);
    jc::Member member;
    ASSERT_TRUE(group.attach(member));
    auto last = group.request_stop(19);
    ASSERT_TRUE(last);
    EXPECT_EQ(last->generation, UINT64_MAX);
    EXPECT_TRUE(group.request_stop(19));
    ASSERT_TRUE(group.request_continue());
    ASSERT_TRUE(group.release(member));
    EXPECT_FALSE(group.request_stop(20));
    EXPECT_FALSE(group.stopping());
    EXPECT_EQ(group.generation(), UINT64_MAX);
    EXPECT_EQ(group.stop_signal(), 19U);
    EXPECT_EQ(group.members(), 1U);
    EXPECT_EQ(group.quiescent_members(), 0U);
    EXPECT_FALSE(group.acknowledge(member, *last));
}

namespace {
// Independent oracle stores actual member sets, not the implementation's
// aggregate counts. Exhaustive replay covers each reachable abstract state
// within eight operations on three reusable thread slots.
struct Oracle {
    std::set<unsigned> attached;
    std::set<unsigned> quiet;
    unsigned epoch = 0;
    unsigned signal = 0;
    bool stopping = false;

    auto step(unsigned action) -> bool {
        unsigned const SLOT = action % 3;
        if (action < 3) {
            if (!attached.insert(SLOT).second) return false;
            quiet.insert(SLOT);
        } else if (action < 6) {
            if (!attached.contains(SLOT) || stopping) return false;
            quiet.erase(SLOT);
        } else if (action < 9 || action >= 15) {
            unsigned const TICKET = action < 9 ? epoch : 1;
            if (!attached.contains(SLOT) || !stopping || epoch != TICKET) return false;
            quiet.insert(SLOT);
        } else if (action < 12) {
            if (!attached.erase(SLOT)) return false;
            quiet.erase(SLOT);
        } else if (action < 14) {
            if (!stopping) {
                ++epoch;
                signal = action == 12 ? 19 : 20;
                stopping = true;
            }
        } else {
            bool const PREVIOUS = stopping;
            stopping = false;
            return PREVIOUS;
        }
        return true;
    }

    auto key() const -> std::string {
        std::string result = std::to_string(epoch) + ":" + std::to_string(signal) + (stopping ? ":S:" : ":R:");
        for (unsigned i = 0; i < 3; ++i) result += !attached.contains(i) ? '-' : quiet.contains(i) ? 'Q' : 'A';
        return result;
    }
};

void replay(const std::vector<unsigned>& actions, Oracle& oracle) {
    jc::StopBarrier group;
    std::array<jc::Member, 3> members;
    std::optional<jc::StopTicket> latest;
    for (unsigned action : actions) {
        SCOPED_TRACE(action);
        auto& member = members.at(action % 3);
        bool result;
        if (action < 3)
            result = group.attach(member);
        else if (action < 6)
            result = group.release(member);
        else if (action < 9)
            result = group.acknowledge(member, latest.value_or(jc::StopTicket{&group, 0}));
        else if (action < 12)
            result = group.detach(member);
        else if (action < 14) {
            latest = group.request_stop(action == 12 ? 19 : 20);
            result = latest.has_value();
        } else if (action == 14)
            result = group.request_continue();
        else
            result = group.acknowledge(member, jc::StopTicket{&group, 1});
        ASSERT_EQ(result, oracle.step(action));
        ASSERT_EQ(group.stopping(), oracle.stopping);
        ASSERT_EQ(group.complete(), oracle.stopping && !oracle.attached.empty() && oracle.attached == oracle.quiet);
        ASSERT_EQ(group.members(), oracle.attached.size());
        ASSERT_EQ(group.quiescent_members(), oracle.quiet.size());
        ASSERT_EQ(group.generation(), oracle.epoch);
        ASSERT_EQ(group.stop_signal(), oracle.signal);
        for (unsigned i = 0; i < members.size(); ++i) {
            ASSERT_EQ(members.at(i).attached(), oracle.attached.contains(i));
            ASSERT_EQ(members.at(i).quiescent(), oracle.quiet.contains(i));
        }
    }
}
}  // namespace

TEST(JobControlBarrier, ExhaustiveMembershipAndSignalInterleavings) {
    std::vector<std::vector<unsigned>> pending(1);
    std::set<std::string> visited{Oracle{}.key()};
    size_t transitions = 0;
    for (size_t index = 0; index < pending.size(); ++index) {
        auto const PREFIX = pending.at(index);
        if (PREFIX.size() == 8) continue;
        for (unsigned action = 0; action < 18; ++action) {
            auto trace = PREFIX;
            trace.push_back(action);
            Oracle oracle;
            replay(trace, oracle);
            ASSERT_FALSE(HasFatalFailure());
            ++transitions;
            if (visited.insert(oracle.key()).second) pending.push_back(std::move(trace));
        }
    }
    EXPECT_GT(visited.size(), 100U);
    EXPECT_GT(transitions, 1000U);
    std::cout << "stop barrier explored states=" << visited.size() << " transitions=" << transitions << '\n';
}
