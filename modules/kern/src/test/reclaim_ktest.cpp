#include <platform/mm/reclaim.hpp>
#include <test/ktest.hpp>

namespace reclaim = ker::mod::mm::reclaim;

KTEST(MMReclaim, PriorityAndProgress) { KEXPECT_TRUE(reclaim::selftest_priority_and_progress()); }

KTEST(MMReclaim, NoProgressBackoff) { KEXPECT_TRUE(reclaim::selftest_no_progress_backoff()); }

KTEST(MMReclaim, RecursionGuard) { KEXPECT_TRUE(reclaim::selftest_recursion_guard()); }

KTEST(MMReclaim, ContextFiltering) { KEXPECT_TRUE(reclaim::selftest_context_filtering()); }

KTEST(MMReclaim, BudgetBounds) { KEXPECT_TRUE(reclaim::selftest_budget_bounds()); }
