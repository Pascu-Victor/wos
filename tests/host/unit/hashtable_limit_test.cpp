#include <gtest/gtest.h>

#include <cstdint>
#include <util/hashtable.hpp>

namespace {

struct TestEntry {
    uint64_t key = 0;
    int value = 0;
    TestEntry* hash_next = nullptr;
};

struct TestKeyExtract {
    auto operator()(const TestEntry& entry) const -> uint64_t { return entry.key; }
};

}  // namespace

TEST(IntrHashTableLimit, ZeroLimitDoesNotRemove) {
    ker::util::IntrHashTable<TestEntry, TestKeyExtract, ker::util::IntHash, ker::util::IntEqual> table(4);

    TestEntry entry{.key = 42, .value = 1};
    ASSERT_TRUE(table.insert(&entry));

    bool called = false;
    size_t const accepted = table.remove_by_key_limit(42ULL, 0, [&](TestEntry*) {
        called = true;
        return true;
    });

    EXPECT_EQ(accepted, 0u);
    EXPECT_FALSE(called);
    EXPECT_EQ(table.size(), 1u);
    EXPECT_EQ(table.find(42ULL), &entry);
}

TEST(IntrHashTableLimit, LimitCountsOnlyAcceptedRemovals) {
    ker::util::IntrHashTable<TestEntry, TestKeyExtract, ker::util::IntHash, ker::util::IntEqual> table(4);

    TestEntry other{.key = 7, .value = 70};
    TestEntry remaining{.key = 42, .value = 3};
    TestEntry accepted_second{.key = 42, .value = 2};
    TestEntry accepted_first{.key = 42, .value = 1};
    TestEntry stale{.key = 42, .value = 0};

    ASSERT_TRUE(table.insert(&other));
    ASSERT_TRUE(table.insert(&remaining));
    ASSERT_TRUE(table.insert(&accepted_second));
    ASSERT_TRUE(table.insert(&accepted_first));
    ASSERT_TRUE(table.insert(&stale));

    int callback_count = 0;
    size_t const accepted = table.remove_by_key_limit(42ULL, 2, [&](TestEntry* entry) {
        callback_count++;
        return entry->value != 0;
    });

    EXPECT_EQ(accepted, 2u);
    EXPECT_EQ(callback_count, 3);
    EXPECT_EQ(table.size(), 2u);
    EXPECT_EQ(table.find(7ULL), &other);
    EXPECT_EQ(table.find(42ULL), &remaining);
    EXPECT_EQ(stale.hash_next, nullptr);
    EXPECT_EQ(accepted_first.hash_next, nullptr);
    EXPECT_EQ(accepted_second.hash_next, nullptr);
}
