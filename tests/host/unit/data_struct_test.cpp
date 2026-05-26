// Unit tests for kernel data structures: RadixTree, SmallVec, IntrHashTable.
// These compile against the real kernel headers via the host shim layer.

#include <gtest/gtest.h>
#include <util/radix_tree.hpp>
#include <util/smallvec.hpp>
#include <util/hashtable.hpp>
#include <cstdint>

using namespace ker::util;

// =========================================================================
// RadixTree tests
// =========================================================================

TEST(RadixTree, InsertAndLookup) {
    RadixTree<int*> tree;

    int a = 10, b = 20, c = 30;
    ASSERT_TRUE(tree.insert(0, &a));
    ASSERT_TRUE(tree.insert(1, &b));
    ASSERT_TRUE(tree.insert(100, &c));

    EXPECT_EQ(tree.lookup(0), &a);
    EXPECT_EQ(tree.lookup(1), &b);
    EXPECT_EQ(tree.lookup(100), &c);
    EXPECT_EQ(tree.lookup(2), nullptr);  // not inserted
    EXPECT_EQ(tree.size(), 3u);
}

TEST(RadixTree, Remove) {
    RadixTree<int*> tree;

    int a = 1, b = 2;
    ASSERT_TRUE(tree.insert(5, &a));
    ASSERT_TRUE(tree.insert(10, &b));

    int* removed = tree.remove(5);
    EXPECT_EQ(removed, &a);
    EXPECT_EQ(tree.lookup(5), nullptr);
    EXPECT_EQ(tree.lookup(10), &b);
    EXPECT_EQ(tree.size(), 1u);
}

TEST(RadixTree, RemoveNonExistent) {
    RadixTree<int*> tree;
    int* removed = tree.remove(999);
    EXPECT_EQ(removed, nullptr);
}

TEST(RadixTree, FindFirstUnset) {
    RadixTree<int*> tree;

    int vals[5];
    // Insert at 0, 1, 2 — gap at 3
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(tree.insert(static_cast<uint64_t>(i), &vals[i]));
    }

    EXPECT_EQ(tree.find_first_unset(0), 3u);
    EXPECT_EQ(tree.find_first_unset(1), 3u);
    EXPECT_EQ(tree.find_first_unset(3), 3u);
}

TEST(RadixTree, ForEach) {
    RadixTree<int*> tree;

    int vals[4] = {10, 20, 30, 40};
    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(tree.insert(static_cast<uint64_t>(i * 10), &vals[i]));
    }

    int count = 0;
    tree.for_each([&](uint64_t key, int* val) {
        (void)key;
        EXPECT_NE(val, nullptr);
        count++;
    });
    EXPECT_EQ(count, 4);
}

TEST(RadixTree, LargeKey) {
    RadixTree<int*> tree;
    int val = 42;
    // Key well beyond initial capacity (64)
    ASSERT_TRUE(tree.insert(10000, &val));
    EXPECT_EQ(tree.lookup(10000), &val);
    EXPECT_EQ(tree.lookup(9999), nullptr);
}

TEST(RadixTree, CloneFrom) {
    RadixTree<int*> orig;
    int a = 1, b = 2, c = 3;
    ASSERT_TRUE(orig.insert(0, &a));
    ASSERT_TRUE(orig.insert(50, &b));
    ASSERT_TRUE(orig.insert(200, &c));

    RadixTree<int*> clone;
    ASSERT_TRUE(clone.clone_from(orig));
    EXPECT_EQ(clone.size(), 3u);
    EXPECT_EQ(clone.lookup(0), &a);
    EXPECT_EQ(clone.lookup(50), &b);
    EXPECT_EQ(clone.lookup(200), &c);
}

// =========================================================================
// SmallVec tests
// =========================================================================

TEST(SmallVec, InlineStorage) {
    SmallVec<int, 4> vec;
    ASSERT_TRUE(vec.push_back(1));
    ASSERT_TRUE(vec.push_back(2));
    ASSERT_TRUE(vec.push_back(3));
    ASSERT_TRUE(vec.push_back(4));

    EXPECT_FALSE(vec.is_spilled());
    EXPECT_EQ(vec.size(), 4u);
    EXPECT_EQ(vec[0], 1);
    EXPECT_EQ(vec[3], 4);
}

TEST(SmallVec, SpillToHeap) {
    SmallVec<int, 2> vec;
    ASSERT_TRUE(vec.push_back(10));
    ASSERT_TRUE(vec.push_back(20));
    ASSERT_TRUE(vec.push_back(30)); // spills

    EXPECT_TRUE(vec.is_spilled());
    EXPECT_EQ(vec.size(), 3u);
    EXPECT_EQ(vec[0], 10);
    EXPECT_EQ(vec[1], 20);
    EXPECT_EQ(vec[2], 30);
}

TEST(SmallVec, RemoveAt) {
    SmallVec<int, 4> vec;
    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(vec.push_back(i * 10));
    }

    ASSERT_TRUE(vec.remove_at(1)); // remove 10
    EXPECT_EQ(vec.size(), 3u);
    EXPECT_EQ(vec[0], 0);
    EXPECT_EQ(vec[1], 20);
    EXPECT_EQ(vec[2], 30);
}

TEST(SmallVec, Remove) {
    SmallVec<int, 4> vec;
    ASSERT_TRUE(vec.push_back(5));
    ASSERT_TRUE(vec.push_back(15));
    ASSERT_TRUE(vec.push_back(25));

    EXPECT_TRUE(vec.remove(15));
    EXPECT_EQ(vec.size(), 2u);
    EXPECT_FALSE(vec.contains(15));
    EXPECT_TRUE(vec.contains(5));
    EXPECT_TRUE(vec.contains(25));
}

TEST(SmallVec, FindAndContains) {
    SmallVec<int, 4> vec;
    ASSERT_TRUE(vec.push_back(100));
    ASSERT_TRUE(vec.push_back(200));

    EXPECT_EQ(vec.find(100), 0u);
    EXPECT_EQ(vec.find(200), 1u);
    EXPECT_EQ(vec.find(300), vec.size()); // not found
    EXPECT_TRUE(vec.contains(100));
    EXPECT_FALSE(vec.contains(300));
}

TEST(SmallVec, PopBack) {
    SmallVec<int, 4> vec;
    ASSERT_TRUE(vec.push_back(1));
    ASSERT_TRUE(vec.push_back(2));
    vec.pop_back();
    EXPECT_EQ(vec.size(), 1u);
    EXPECT_EQ(vec[0], 1);
}

TEST(SmallVec, Clear) {
    SmallVec<int, 2> vec;
    ASSERT_TRUE(vec.push_back(1));
    ASSERT_TRUE(vec.push_back(2));
    ASSERT_TRUE(vec.push_back(3)); // spill
    vec.clear();
    EXPECT_EQ(vec.size(), 0u);
    EXPECT_TRUE(vec.empty());
    // Heap allocation is kept for reuse
    EXPECT_TRUE(vec.is_spilled());
}

TEST(SmallVec, MoveConstruct) {
    SmallVec<int, 2> a;
    ASSERT_TRUE(a.push_back(10));
    ASSERT_TRUE(a.push_back(20));
    ASSERT_TRUE(a.push_back(30)); // spill

    SmallVec<int, 2> b(std::move(a));
    EXPECT_EQ(b.size(), 3u);
    EXPECT_EQ(b[0], 10);
    EXPECT_EQ(b[2], 30);
    EXPECT_EQ(a.size(), 0u);
}

TEST(SmallVec, CloneFrom) {
    SmallVec<int, 4> orig;
    ASSERT_TRUE(orig.push_back(1));
    ASSERT_TRUE(orig.push_back(2));
    ASSERT_TRUE(orig.push_back(3));

    SmallVec<int, 4> clone;
    ASSERT_TRUE(clone.clone_from(orig));
    EXPECT_EQ(clone.size(), 3u);
    EXPECT_EQ(clone[0], 1);
    EXPECT_EQ(clone[1], 2);
    EXPECT_EQ(clone[2], 3);
}

// =========================================================================
// IntrHashTable tests
// =========================================================================

struct TestEntry {
    uint64_t key;
    int value;
    TestEntry* hash_next = nullptr;
};

struct TestKeyExtract {
    uint64_t operator()(const TestEntry& e) const { return e.key; }
};

TEST(IntrHashTable, InsertAndFind) {
    IntrHashTable<TestEntry, TestKeyExtract, IntHash, IntEqual> table(16);
    ASSERT_TRUE(table.valid());

    TestEntry e1{.key = 1, .value = 100};
    TestEntry e2{.key = 2, .value = 200};
    TestEntry e3{.key = 3, .value = 300};

    ASSERT_TRUE(table.insert(&e1));
    ASSERT_TRUE(table.insert(&e2));
    ASSERT_TRUE(table.insert(&e3));

    EXPECT_EQ(table.size(), 3u);

    auto* found = table.find(2ULL);
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->value, 200);

    EXPECT_EQ(table.find(999ULL), nullptr);
}

TEST(IntrHashTable, RemoveByPointer) {
    IntrHashTable<TestEntry, TestKeyExtract, IntHash, IntEqual> table(16);

    TestEntry e1{.key = 10, .value = 1};
    TestEntry e2{.key = 20, .value = 2};
    ASSERT_TRUE(table.insert(&e1));
    ASSERT_TRUE(table.insert(&e2));

    EXPECT_TRUE(table.remove(&e1));
    EXPECT_EQ(table.size(), 1u);
    EXPECT_EQ(table.find(10ULL), nullptr);
    EXPECT_NE(table.find(20ULL), nullptr);
}

TEST(IntrHashTable, RemoveByKey) {
    IntrHashTable<TestEntry, TestKeyExtract, IntHash, IntEqual> table(16);

    TestEntry e1{.key = 42, .value = 999};
    ASSERT_TRUE(table.insert(&e1));

    auto* removed = table.remove_by_key(42ULL);
    EXPECT_EQ(removed, &e1);
    EXPECT_EQ(table.size(), 0u);
}

TEST(IntrHashTable, ForEach) {
    IntrHashTable<TestEntry, TestKeyExtract, IntHash, IntEqual> table(4);

    TestEntry entries[5];
    for (int i = 0; i < 5; i++) {
        entries[i].key = static_cast<uint64_t>(i);
        entries[i].value = i * 10;
        entries[i].hash_next = nullptr;
        ASSERT_TRUE(table.insert(&entries[i]));
    }

    int sum = 0;
    table.for_each([&](TestEntry* e) { sum += e->value; });
    EXPECT_EQ(sum, 0 + 10 + 20 + 30 + 40);
}

TEST(IntrHashTable, DeferredInit) {
    IntrHashTable<TestEntry, TestKeyExtract, IntHash, IntEqual> table;
    EXPECT_FALSE(table.valid());

    ASSERT_TRUE(table.init(32));
    EXPECT_TRUE(table.valid());

    TestEntry e{.key = 1, .value = 42};
    ASSERT_TRUE(table.insert(&e));
    EXPECT_EQ(table.find(1ULL)->value, 42);
}

TEST(IntrHashTable, CollisionHandling) {
    // Small bucket count to force collisions
    IntrHashTable<TestEntry, TestKeyExtract, IntHash, IntEqual> table(2);

    TestEntry entries[8];
    for (int i = 0; i < 8; i++) {
        entries[i].key = static_cast<uint64_t>(i);
        entries[i].value = i;
        entries[i].hash_next = nullptr;
        ASSERT_TRUE(table.insert(&entries[i]));
    }

    EXPECT_EQ(table.size(), 8u);

    // All entries should be findable despite collisions
    for (int i = 0; i < 8; i++) {
        auto* found = table.find(static_cast<uint64_t>(i));
        ASSERT_NE(found, nullptr);
        EXPECT_EQ(found->value, i);
    }
}
