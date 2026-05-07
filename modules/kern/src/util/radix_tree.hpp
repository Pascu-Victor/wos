#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <platform/mm/dyn/kmalloc.hpp>

namespace ker::util {

// RadixTree - sparse integer-keyed map for kernel use.
//
// 64-bit key space, 6-bit fan-out per level (64-way branching).
// Max depth = 11 levels (covers full 64-bit key space: 6*11=66 > 64).
// Nodes allocated via kmalloc on demand (lazy allocation).
//
// Designed for FD tables: keys are small non-negative integers (0..N),
// with fast find_first_unset() for FD allocation.
//
// Thread safety: NONE. Caller must hold an external lock.

template <typename T>
class RadixTree {
   public:
    static constexpr unsigned BITS_PER_LEVEL = 6;
    static constexpr unsigned FANOUT = 1u << BITS_PER_LEVEL;  // 64
    static constexpr unsigned MAX_DEPTH = 11;
    static constexpr uint64_t LEVEL_MASK = FANOUT - 1;

    RadixTree() = default;

    ~RadixTree() { destroy_node(m_root, 0); }

    // No copy
    RadixTree(const RadixTree&) = delete;
    RadixTree& operator=(const RadixTree&) = delete;

    // Move
    RadixTree(RadixTree&& other) noexcept : m_root(other.m_root), m_size(other.m_size), m_depth(other.m_depth) {
        other.m_root = nullptr;
        other.m_size = 0;
    }

    RadixTree& operator=(RadixTree&& other) noexcept {
        if (this != &other) {
            destroy_node(m_root, 0);
            m_root = other.m_root;
            m_size = other.m_size;
            m_depth = other.m_depth;
            other.m_root = nullptr;
            other.m_size = 0;
        }
        return *this;
    }

    // Insert or replace value at key. Returns false on OOM.
    [[nodiscard]] bool insert(uint64_t key, T value) {
        // Ensure tree is deep enough for this key
        while (key >= capacity_at_depth(m_depth)) {
            if (!grow_depth()) return false;
        }

        // Lazy-allocate root node (default constructor leaves m_root null)
        if (!m_root) {
            m_root = alloc_node();
            if (!m_root) return false;
        }

        Node* node = m_root;
        for (unsigned level = m_depth; level > 1; --level) {
            unsigned idx = slot_index(key, level);
            if (!node->children[idx]) {
                node->children[idx] = alloc_node();
                if (!node->children[idx]) return false;
                node->populated++;
            }
            node = node->children[idx];
        }

        // Leaf level: store value
        unsigned idx = slot_index(key, 1);
        bool was_empty = (node->values[idx] == T{});
        node->values[idx] = value;
        if (was_empty && value != T{}) {
            node->populated++;
            m_size++;
        } else if (!was_empty && value == T{}) {
            node->populated--;
            m_size--;
        }
        return true;
    }

    // Remove value at key. Returns the removed value, or T{} if not present.
    T remove(uint64_t key) {
        if (!m_root || key >= capacity_at_depth(m_depth)) return T{};

        // Walk down, recording path for cleanup
        struct PathEntry {
            Node* node;
            unsigned idx;
        };
        PathEntry path[MAX_DEPTH];
        int path_len = 0;

        Node* node = m_root;
        for (unsigned level = m_depth; level > 1; --level) {
            unsigned idx = slot_index(key, level);
            if (!node->children[idx]) return T{};
            path[path_len++] = {node, idx};
            node = node->children[idx];
        }

        unsigned idx = slot_index(key, 1);
        T old = node->values[idx];
        if (old == T{}) return T{};

        node->values[idx] = T{};
        node->populated--;
        m_size--;

        // Prune empty nodes bottom-up
        if (node->populated == 0 && path_len > 0) {
            free_node(node);
            for (int i = path_len - 1; i >= 0; --i) {
                path[i].node->children[path[i].idx] = nullptr;
                path[i].node->populated--;
                if (path[i].node->populated == 0 && i > 0) {
                    free_node(path[i].node);
                } else {
                    break;
                }
            }
        }

        return old;
    }

    // Lookup value at key. Returns T{} (nullptr for pointers) if not present.
    [[nodiscard]] T lookup(uint64_t key) const {
        if (!m_root || key >= capacity_at_depth(m_depth)) return T{};

        const Node* node = m_root;
        for (unsigned level = m_depth; level > 1; --level) {
            unsigned idx = slot_index(key, level);
            if (!node->children[idx]) return T{};
            node = node->children[idx];
        }

        return node->values[slot_index(key, 1)];
    }

    // Find first key >= start_key where value == T{} (unset).
    // Returns the key, or UINT64_MAX if none found within current capacity.
    [[nodiscard]] uint64_t find_first_unset(uint64_t start_key = 0) const {
        uint64_t cap = capacity_at_depth(m_depth);
        for (uint64_t k = start_key; k < cap; ++k) {
            if (lookup(k) == T{}) return k;
        }
        return UINT64_MAX;
    }

    // Iterate all set entries. Callback: void(uint64_t key, T value)
    template <typename Fn>
    void for_each(Fn&& fn) const {
        if (m_root) {
            iterate_node(m_root, m_depth, 0, fn);
        }
    }

    // Deep copy for fork(). Returns false on OOM (partial copy is cleaned up).
    [[nodiscard]] bool clone_from(const RadixTree& other) {
        destroy_node(m_root, 0);
        m_root = nullptr;
        m_size = 0;
        m_depth = other.m_depth;

        if (!other.m_root) return true;

        bool ok = true;
        other.for_each([&](uint64_t key, T value) {
            if (ok && value != T{}) {
                ok = insert(key, value);
            }
        });

        if (!ok) {
            // Cleanup partial copy
            destroy_node(m_root, 0);
            m_root = nullptr;
            m_size = 0;
        }
        return ok;
    }

    [[nodiscard]] size_t size() const { return m_size; }
    [[nodiscard]] bool empty() const { return m_size == 0; }
    [[nodiscard]] uint64_t current_capacity() const { return capacity_at_depth(m_depth); }

   private:
    // Each node is either an interior node (children) or a leaf (values).
    // We use a union to save memory. Depth 1 = leaf, depth > 1 = interior.
    struct Node {
        union {
            Node* children[FANOUT];
            T values[FANOUT];
        };
        uint16_t populated{0};  // count of non-null children or non-null values

        Node() {
            memset(children, 0, sizeof(children));
            populated = 0;
        }
    };

    Node* m_root{nullptr};
    size_t m_size{0};
    unsigned m_depth{1};  // minimum depth = 1 (single leaf, 64 entries)

    static unsigned slot_index(uint64_t key, unsigned level) { return (key >> ((level - 1) * BITS_PER_LEVEL)) & LEVEL_MASK; }

    static uint64_t capacity_at_depth(unsigned depth) {
        if (depth == 0) return 0;
        if (depth >= 11) return UINT64_MAX;  // overflow guard
        uint64_t cap = 1;
        for (unsigned i = 0; i < depth; ++i) {
            cap *= FANOUT;
        }
        return cap;
    }

    Node* alloc_node() { return new (std::nothrow) Node; }

    void free_node(Node* n) { delete n; }

    // Grow tree depth by 1: allocate new root, make old root its child[0]
    [[nodiscard]] bool grow_depth() {
        Node* new_root = alloc_node();
        if (!new_root) return false;

        if (m_root) {
            new_root->children[0] = m_root;
            new_root->populated = 1;
        }
        m_root = new_root;
        m_depth++;
        return true;
    }

    void destroy_node(Node* node, unsigned depth_from_root) {
        if (!node) return;
        unsigned level = m_depth - depth_from_root;
        if (level > 1) {
            // Interior node: recurse into children
            for (unsigned i = 0; i < FANOUT; ++i) {
                if (node->children[i]) {
                    destroy_node(node->children[i], depth_from_root + 1);
                }
            }
        }
        free_node(node);
    }

    template <typename Fn>
    void iterate_node(const Node* node, unsigned level, uint64_t prefix, Fn&& fn) const {
        if (level == 1) {
            // Leaf: iterate values
            for (unsigned i = 0; i < FANOUT; ++i) {
                if (node->values[i] != T{}) {
                    fn(prefix | i, node->values[i]);
                }
            }
        } else {
            // Interior: recurse
            for (unsigned i = 0; i < FANOUT; ++i) {
                if (node->children[i]) {
                    uint64_t child_prefix = prefix | (static_cast<uint64_t>(i) << ((level - 1) * BITS_PER_LEVEL));
                    iterate_node(node->children[i], level - 1, child_prefix, fn);
                }
            }
        }
    }
};

}  // namespace ker::util
