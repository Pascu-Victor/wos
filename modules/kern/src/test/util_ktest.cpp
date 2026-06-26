#include <cstddef>
#include <cstdint>
#include <minimalist_malloc/bitmap.hpp>
#include <test/ktest.hpp>
#include <util/hashtable.hpp>
#include <util/object_pool.hpp>
#include <util/radix_tree.hpp>
#include <util/smallvec.hpp>

// ---------------------------------------------------------------------------
// Bitmap tests
// ---------------------------------------------------------------------------

KTEST(Bitmap, InitAllClear) {
    Bitmap<64> bm{};
    bm.init();
    for (unsigned i = 0; i < 64; ++i) {
        KEXPECT_FALSE(bm.check_used(i));
    }
}

KTEST(Bitmap, SetAndCheck) {
    Bitmap<64> bm{};
    bm.init();
    bm.set_used(0);
    bm.set_used(31);
    bm.set_used(63);
    KEXPECT_TRUE(bm.check_used(0));
    KEXPECT_TRUE(bm.check_used(31));
    KEXPECT_TRUE(bm.check_used(63));
    KEXPECT_FALSE(bm.check_used(1));
    KEXPECT_FALSE(bm.check_used(32));
}

KTEST(Bitmap, SetUnset) {
    Bitmap<64> bm{};
    bm.init();
    bm.set_used(10);
    KEXPECT_TRUE(bm.check_used(10));
    bm.set_unused(10);
    KEXPECT_FALSE(bm.check_used(10));
}

KTEST(Bitmap, FindUnused) {
    Bitmap<64> bm{};
    bm.init();
    // All clear: first unused is 0
    KEXPECT_EQ(bm.find_unused(0), 0U);
    bm.set_used(0);
    // Now first unused is 1
    KEXPECT_EQ(bm.find_unused(0), 1U);
    bm.set_used(1);
    bm.set_used(2);
    KEXPECT_EQ(bm.find_unused(0), 3U);
    // Search from 5: first unused >= 5
    bm.set_used(5);
    KEXPECT_EQ(bm.find_unused(4), 4U);
}

KTEST(Bitmap, FindUsed) {
    Bitmap<64> bm{};
    bm.init();
    // All clear: no used bit
    KEXPECT_EQ(bm.find_used(0), static_cast<unsigned>(BITMAP_NO_BITS_LEFT));
    bm.set_used(7);
    KEXPECT_EQ(bm.find_used(0), 7U);
    KEXPECT_EQ(bm.find_used(7), 7U);
    KEXPECT_EQ(bm.find_used(8), static_cast<unsigned>(BITMAP_NO_BITS_LEFT));
}

KTEST(Bitmap, AllFull) {
    Bitmap<32> bm{};
    bm.init();
    for (unsigned i = 0; i < 32; ++i) {
        bm.set_used(i);
    }
    KEXPECT_EQ(bm.find_unused(0), static_cast<unsigned>(BITMAP_NO_BITS_LEFT));
}

// ---------------------------------------------------------------------------
// SmallVec tests
// ---------------------------------------------------------------------------

KTEST(SmallVec, PushBackInline) {
    ker::util::SmallVec<uint32_t, 4> v;
    KREQUIRE_TRUE(v.push_back(10));
    KREQUIRE_TRUE(v.push_back(20));
    KREQUIRE_TRUE(v.push_back(30));
    KEXPECT_EQ(v.size(), 3UL);
    KEXPECT_EQ(v.at(0), 10U);
    KEXPECT_EQ(v.at(1), 20U);
    KEXPECT_EQ(v.at(2), 30U);
}

KTEST(SmallVec, PushBackSpillToHeap) {
    ker::util::SmallVec<uint32_t, 2> v;
    KREQUIRE_TRUE(v.push_back(1));
    KREQUIRE_TRUE(v.push_back(2));
    KREQUIRE_TRUE(v.push_back(3));  // spills to heap
    KREQUIRE_TRUE(v.push_back(4));
    KEXPECT_EQ(v.size(), 4UL);
    KEXPECT_EQ(v.at(0), 1U);
    KEXPECT_EQ(v.at(1), 2U);
    KEXPECT_EQ(v.at(2), 3U);
    KEXPECT_EQ(v.at(3), 4U);
}

KTEST(SmallVec, RangeForInline) {
    ker::util::SmallVec<uint32_t, 4> v;
    KREQUIRE_TRUE(v.push_back(10));
    KREQUIRE_TRUE(v.push_back(20));
    KREQUIRE_TRUE(v.push_back(30));

    uint32_t sum = 0;
    for (uint32_t const VAL : v) {
        sum += VAL;
    }
    KEXPECT_EQ(sum, 60U);

    for (auto& val : v) {
        val++;
    }
    KEXPECT_EQ(v.at(0), 11U);
    KEXPECT_EQ(v.at(1), 21U);
    KEXPECT_EQ(v.at(2), 31U);
}

KTEST(SmallVec, RangeForSpilledConst) {
    ker::util::SmallVec<uint32_t, 2> v;
    KREQUIRE_TRUE(v.push_back(1));
    KREQUIRE_TRUE(v.push_back(2));
    KREQUIRE_TRUE(v.push_back(3));
    KREQUIRE_TRUE(v.push_back(4));

    auto const& cv = v;
    uint32_t expected = 1;
    size_t count = 0;
    for (uint32_t const VAL : cv) {
        KEXPECT_EQ(VAL, expected);
        expected++;
        count++;
    }
    KEXPECT_EQ(count, 4UL);
}

KTEST(SmallVec, Clear) {
    ker::util::SmallVec<uint32_t, 4> v;
    KREQUIRE_TRUE(v.push_back(1));
    KREQUIRE_TRUE(v.push_back(2));
    v.clear();
    KEXPECT_EQ(v.size(), 0UL);
    KREQUIRE_TRUE(v.push_back(99));
    KEXPECT_EQ(v.at(0), 99U);
}

KTEST(SmallVec, Empty) {
    ker::util::SmallVec<uint32_t, 4> v;
    KEXPECT_TRUE(v.empty());
    KREQUIRE_TRUE(v.push_back(1));
    KEXPECT_FALSE(v.empty());
}

KTEST(SmallVec, LastElement) {
    ker::util::SmallVec<uint32_t, 4> v;
    KREQUIRE_TRUE(v.push_back(42));
    KREQUIRE_TRUE(v.push_back(99));
    KEXPECT_EQ(v.at(v.size() - 1), 99U);
}

// ---------------------------------------------------------------------------
// RadixTree tests
// ---------------------------------------------------------------------------

KTEST(RadixTree, InsertLookup) {
    ker::util::RadixTree<uint64_t> tree;
    KREQUIRE_TRUE(tree.insert(0, 100ULL));
    KREQUIRE_TRUE(tree.insert(1, 200ULL));
    KREQUIRE_TRUE(tree.insert(63, 300ULL));
    KEXPECT_EQ(tree.lookup(0), 100ULL);
    KEXPECT_EQ(tree.lookup(1), 200ULL);
    KEXPECT_EQ(tree.lookup(63), 300ULL);
    KEXPECT_EQ(tree.lookup(2), 0ULL);  // not inserted
}

KTEST(RadixTree, Remove) {
    ker::util::RadixTree<uint64_t> tree;
    KREQUIRE_TRUE(tree.insert(5, 555ULL));
    KEXPECT_EQ(tree.lookup(5), 555ULL);
    uint64_t const REMOVED = tree.remove(5);
    KEXPECT_EQ(REMOVED, 555ULL);
    KEXPECT_EQ(tree.lookup(5), 0ULL);
}

KTEST(RadixTree, LargeKey) {
    ker::util::RadixTree<uint64_t> tree;
    uint64_t const BIG_KEY = 0x00FF'FFFF'FFFF'FFFFULL;
    KREQUIRE_TRUE(tree.insert(BIG_KEY, 42ULL));
    KEXPECT_EQ(tree.lookup(BIG_KEY), 42ULL);
    KEXPECT_EQ(tree.lookup(BIG_KEY - 1), 0ULL);
}

KTEST(RadixTree, FindFirstUnset) {
    ker::util::RadixTree<uint64_t> tree;
    // Empty tree: first unset is 0
    KEXPECT_EQ(tree.find_first_unset(0), 0ULL);
    KREQUIRE_TRUE(tree.insert(0, 1ULL));
    KREQUIRE_TRUE(tree.insert(1, 2ULL));
    KREQUIRE_TRUE(tree.insert(2, 3ULL));
    KEXPECT_EQ(tree.find_first_unset(0), 3ULL);
}

KTEST(RadixTree, FindFirstUnsetBelowLimit) {
    ker::util::RadixTree<uint64_t> tree;
    for (uint64_t key = 0; key < 130; ++key) {
        KREQUIRE_TRUE(tree.insert(key, key + 1));
    }

    KEXPECT_EQ(tree.find_first_unset_below(0, 130), UINT64_MAX);
    KEXPECT_EQ(tree.find_first_unset_below(0, 131), 130ULL);
    KEXPECT_EQ(tree.remove(17), 18ULL);
    KEXPECT_EQ(tree.find_first_unset_below(0, 130), 17ULL);
    KEXPECT_EQ(tree.find_first_unset_below(18, 130), UINT64_MAX);
    KEXPECT_EQ(tree.find_first_unset_below(130, 130), UINT64_MAX);
}

KTEST(RadixTree, SizeTracking) {
    ker::util::RadixTree<uint64_t> tree;
    KEXPECT_EQ(tree.size(), 0UL);
    KREQUIRE_TRUE(tree.insert(10, 1ULL));
    KEXPECT_EQ(tree.size(), 1UL);
    KREQUIRE_TRUE(tree.insert(20, 2ULL));
    KEXPECT_EQ(tree.size(), 2UL);
    tree.remove(10);
    KEXPECT_EQ(tree.size(), 1UL);
}

// ---------------------------------------------------------------------------
// IntrHashTable tests
// ---------------------------------------------------------------------------

namespace {

struct HNode {
    uint64_t key = 0;
    uint64_t val = 0;
    HNode* hash_next = nullptr;
};

struct HKeyExtract {
    auto operator()(const HNode& n) const -> uint64_t { return n.key; }
};

using TestHashTable = ker::util::IntrHashTable<HNode, HKeyExtract, ker::util::IntHash, ker::util::IntEqual>;

}  // namespace

KTEST(HashTable, InsertLookup) {
    TestHashTable ht(16);
    KREQUIRE_TRUE(ht.valid());
    HNode nodes[10];
    for (int i = 0; i < 10; ++i) {
        nodes[i].key = (static_cast<uint64_t>(i) * 7U) + 1U;
        nodes[i].val = static_cast<uint64_t>(i) * 100U;
        nodes[i].hash_next = nullptr;
        KEXPECT_TRUE(ht.insert(&nodes[i]));
    }
    KEXPECT_EQ(ht.size(), 10UL);
    for (int i = 0; i < 10; ++i) {
        HNode* found = ht.find(nodes[i].key);
        KEXPECT_NE(found, nullptr);
        if (found != nullptr) {
            KEXPECT_EQ(found->val, nodes[i].val);
        }
    }
}

KTEST(HashTable, Remove) {
    TestHashTable ht(16);
    KREQUIRE_TRUE(ht.valid());
    HNode n{.key = 42, .val = 999};
    KEXPECT_TRUE(ht.insert(&n));
    KEXPECT_NE(ht.find(uint64_t{42}), nullptr);
    KEXPECT_TRUE(ht.remove(&n));
    KEXPECT_EQ(ht.find(uint64_t{42}), nullptr);
    KEXPECT_EQ(ht.size(), 0UL);
}

KTEST(HashTable, Collision) {
    // 2 buckets: IntHash(0)&1 == 0, IntHash(2)&1 == 0 → both land in bucket 0
    TestHashTable ht(2);
    KREQUIRE_TRUE(ht.valid());
    HNode a{.key = 0, .val = 111};
    HNode b{.key = 2, .val = 222};
    KEXPECT_TRUE(ht.insert(&a));
    KEXPECT_TRUE(ht.insert(&b));
    KEXPECT_EQ(ht.size(), 2UL);
    HNode* fa = ht.find(uint64_t{0});
    HNode* fb = ht.find(uint64_t{2});
    KEXPECT_NE(fa, nullptr);
    KEXPECT_NE(fb, nullptr);
    if (fa != nullptr) {
        KEXPECT_EQ(fa->val, 111ULL);
    }
    if (fb != nullptr) {
        KEXPECT_EQ(fb->val, 222ULL);
    }
}

// ---------------------------------------------------------------------------
// ObjectPool tests
// ---------------------------------------------------------------------------

namespace {

struct PoolObj {
    uint32_t x;
    uint32_t y;
};

}  // namespace

KTEST(ObjectPool, AllocFree) {
    ker::util::ObjectPool<PoolObj> pool;
    uint64_t id1 = 0;
    PoolObj* obj = pool.alloc(&id1);
    KREQUIRE_NE(obj, nullptr);
    obj->x = 0xDEAD;
    obj->y = 0xBEEF;
    KEXPECT_EQ(obj->x, 0xDEADU);
    KEXPECT_EQ(obj->y, 0xBEEFU);
    pool.free(obj);
    KEXPECT_EQ(pool.live_count(), 0UL);
    KEXPECT_EQ(pool.free_count(), 1UL);
    // Reuse from free-list; alloc zeroes the object
    uint64_t id2 = 0;
    PoolObj* obj2 = pool.alloc(&id2);
    KREQUIRE_NE(obj2, nullptr);
    KEXPECT_EQ(obj2->x, 0U);
    KEXPECT_EQ(obj2->y, 0U);
    KEXPECT_NE(id2, id1);  // IDs are monotonically increasing
    pool.free(obj2);
}

KTEST(ObjectPool, MultipleObjects) {
    ker::util::ObjectPool<PoolObj> pool;
    constexpr int N = 8;
    // NOLINTNEXTLINE(misc-const-correctness)
    PoolObj* ptrs[N] = {};
    for (int i = 0; i < N; ++i) {
        ptrs[i] = pool.alloc();
        KEXPECT_NE(ptrs[i], nullptr);
    }
    KEXPECT_EQ(pool.live_count(), static_cast<size_t>(N));
    for (int i = 0; i < N; ++i) {
        for (int j = i + 1; j < N; ++j) {
            KEXPECT_NE(ptrs[i], ptrs[j]);
        }
    }
    for (int i = 0; i < N; ++i) {
        if (ptrs[i] != nullptr) {
            pool.free(ptrs[i]);
        }
    }
    KEXPECT_EQ(pool.live_count(), 0UL);
    KEXPECT_EQ(pool.free_count(), static_cast<size_t>(N));
}
