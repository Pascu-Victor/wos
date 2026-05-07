#include <gtest/gtest.h>

#include <util/list.hpp>

// Tests for the kernel's intrusive doubly-linked std::list<T> in util/list.hpp.

TEST(UtilList, PushBackAndSize) {
    std::list<int> l;
    EXPECT_EQ(l.size(), 0u);
    l.push_back(1);
    l.push_back(2);
    l.push_back(3);
    EXPECT_EQ(l.size(), 3u);
}

TEST(UtilList, PushFrontAndSize) {
    std::list<int> l;
    l.push_front(10);
    l.push_front(20);
    l.push_front(30);
    EXPECT_EQ(l.size(), 3u);
}

TEST(UtilList, FrontBack) {
    std::list<int> l;
    l.push_back(1);
    l.push_back(2);
    l.push_back(3);
    EXPECT_EQ(l.front(), 1);
    EXPECT_EQ(l.back(), 3);
}

TEST(UtilList, PushFrontOrder) {
    std::list<int> l;
    l.push_front(1);
    l.push_front(2);
    l.push_front(3);
    // Head is last push_front, tail is first push_front
    EXPECT_EQ(l.front(), 3);
    EXPECT_EQ(l.back(), 1);
}

TEST(UtilList, PopBack) {
    std::list<int> l;
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
    std::list<int> l;
    l.push_back(10);
    l.push_back(20);
    l.push_back(30);
    EXPECT_EQ(l.pop_front(), 10);
    EXPECT_EQ(l.size(), 2u);
    EXPECT_EQ(l.front(), 20);
}

TEST(UtilList, PopBackEmpty) {
    std::list<int> l;
    int val = l.pop_back();
    EXPECT_EQ(val, int{});
    EXPECT_EQ(l.size(), 0u);
}

TEST(UtilList, PopFrontEmpty) {
    std::list<int> l;
    int val = l.pop_front();
    EXPECT_EQ(val, int{});
    EXPECT_EQ(l.size(), 0u);
}

TEST(UtilList, SingleElement) {
    std::list<int> l;
    l.push_back(42);
    EXPECT_EQ(l.size(), 1u);
    EXPECT_EQ(l.front(), 42);
    EXPECT_EQ(l.back(), 42);
    EXPECT_EQ(l.getHead(), l.getTail());
    EXPECT_EQ(l.pop_back(), 42);
    EXPECT_EQ(l.size(), 0u);
    EXPECT_EQ(l.getHead(), nullptr);
    EXPECT_EQ(l.getTail(), nullptr);
}

TEST(UtilList, MixedPushFrontBack) {
    std::list<int> l;
    l.push_back(2);
    l.push_front(1);
    l.push_back(3);
    // Order: 1 -> 2 -> 3
    EXPECT_EQ(l.front(), 1);
    EXPECT_EQ(l.back(), 3);
    EXPECT_EQ(l.size(), 3u);
}

TEST(UtilList, NodeLinkIntegrity) {
    std::list<int> l;
    l.push_back(10);
    l.push_back(20);
    l.push_back(30);
    auto* node = l.getHead();
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
    std::list<int> l;
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
    std::list<int> l;
    l.push_back(1);
    l.push_back(2);
    l.push_back(3);
    l.remove(1);
    EXPECT_EQ(l.size(), 2u);
    EXPECT_EQ(l.front(), 2);
}

TEST(UtilList, RemoveTail) {
    std::list<int> l;
    l.push_back(1);
    l.push_back(2);
    l.push_back(3);
    l.remove(3);
    EXPECT_EQ(l.size(), 2u);
    EXPECT_EQ(l.back(), 2);
}
