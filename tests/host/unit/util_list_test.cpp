#include <gtest/gtest.h>

#include <util/list.hpp>

TEST(UtilList, PushBackAndSize) {
    ker::util::List<int> l;
    EXPECT_EQ(l.size(), 0u);
    l.push_back(1);
    l.push_back(2);
    l.push_back(3);
    EXPECT_EQ(l.size(), 3u);
}

TEST(UtilList, PushFrontAndSize) {
    ker::util::List<int> l;
    l.push_front(10);
    l.push_front(20);
    l.push_front(30);
    EXPECT_EQ(l.size(), 3u);
}

TEST(UtilList, FrontBack) {
    ker::util::List<int> l;
    l.push_back(1);
    l.push_back(2);
    l.push_back(3);
    EXPECT_EQ(l.front(), 1);
    EXPECT_EQ(l.back(), 3);
}

TEST(UtilList, PushFrontOrder) {
    ker::util::List<int> l;
    l.push_front(1);
    l.push_front(2);
    l.push_front(3);
    // Head is last push_front, tail is first push_front
    EXPECT_EQ(l.front(), 3);
    EXPECT_EQ(l.back(), 1);
}

TEST(UtilList, PopBack) {
    ker::util::List<int> l;
    l.push_back(10);
    l.push_back(20);
    l.push_back(30);
    EXPECT_EQ(l.pop_back(), 30);
    EXPECT_EQ(l.size(), 2u);
    EXPECT_EQ(l.pop_back(), 20);
    EXPECT_EQ(l.size(), 1u);
    EXPECT_EQ(l.pop_back(), 10);
    EXPECT_EQ(l.size(), 0u);
}

TEST(UtilList, PopFront) {
    ker::util::List<int> l;
    l.push_back(10);
    l.push_back(20);
    l.push_back(30);
    EXPECT_EQ(l.pop_front(), 10);
    EXPECT_EQ(l.size(), 2u);
    EXPECT_EQ(l.front(), 20);
}

TEST(UtilList, PopBackEmpty) {
    ker::util::List<int> l;
    int val = l.pop_back();
    EXPECT_EQ(val, int{});
    EXPECT_EQ(l.size(), 0u);
}

TEST(UtilList, PopFrontEmpty) {
    ker::util::List<int> l;
    int val = l.pop_front();
    EXPECT_EQ(val, int{});
    EXPECT_EQ(l.size(), 0u);
}

TEST(UtilList, SingleElement) {
    ker::util::List<int> l;
    l.push_back(42);
    EXPECT_EQ(l.size(), 1u);
    EXPECT_EQ(l.front(), 42);
    EXPECT_EQ(l.back(), 42);
    EXPECT_EQ(l.get_head(), l.get_tail());
    EXPECT_EQ(l.pop_back(), 42);
    EXPECT_EQ(l.size(), 0u);
    EXPECT_EQ(l.get_head(), nullptr);
    EXPECT_EQ(l.get_tail(), nullptr);
}

TEST(UtilList, MixedPushFrontBack) {
    ker::util::List<int> l;
    l.push_back(2);
    l.push_front(1);
    l.push_back(3);
    // Order: 1 -> 2 -> 3
    EXPECT_EQ(l.front(), 1);
    EXPECT_EQ(l.back(), 3);
    EXPECT_EQ(l.size(), 3u);
}

TEST(UtilList, NodeLinkIntegrity) {
    ker::util::List<int> l;
    l.push_back(10);
    l.push_back(20);
    l.push_back(30);
    auto* node = l.get_head();
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->data, 10);
    node = node->next;
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->data, 20);
    node = node->next;
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->data, 30);
    EXPECT_EQ(node->next, nullptr);
    EXPECT_EQ(node->prev->data, 20);
    EXPECT_EQ(node->prev->prev->data, 10);
    EXPECT_EQ(node->prev->prev->prev, nullptr);
}

TEST(UtilList, Remove) {
    ker::util::List<int> l;
    l.push_back(1);
    l.push_back(2);
    l.push_back(3);
    l.push_back(2);  // duplicate
    l.remove(2);     // removes all occurrences
    EXPECT_EQ(l.size(), 2u);
    EXPECT_EQ(l.front(), 1);
    EXPECT_EQ(l.back(), 3);
}

TEST(UtilList, RemoveHead) {
    ker::util::List<int> l;
    l.push_back(1);
    l.push_back(2);
    l.push_back(3);
    l.remove(1);
    EXPECT_EQ(l.size(), 2u);
    EXPECT_EQ(l.front(), 2);
}

TEST(UtilList, RemoveTail) {
    ker::util::List<int> l;
    l.push_back(1);
    l.push_back(2);
    l.push_back(3);
    l.remove(3);
    EXPECT_EQ(l.size(), 2u);
    EXPECT_EQ(l.back(), 2);
}
