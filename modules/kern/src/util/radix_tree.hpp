#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iterator>
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
    static constexpr unsigned FANOUT = 1U << BITS_PER_LEVEL;  // 64
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
            if (!grow_depth()) {
                return false;
            }
        }

        // Lazy-allocate root node (default constructor leaves m_root null)
        if (m_root == nullptr) {
            m_root = (m_depth == 1) ? alloc_leaf_node() : alloc_interior_node();
            if (m_root == nullptr) {
                return false;
            }
        }

        Node* node = m_root;
        for (unsigned level = m_depth; level > 1; --level) {
            unsigned const IDX = slot_index(key, level);
            Node*& child_slot = child_at(node, IDX);
            if (child_slot == nullptr) {
                Node* child = (level == 2) ? alloc_leaf_node() : alloc_interior_node();
                if (child == nullptr) {
                    return false;
                }
                child_slot = child;
                node->populated++;
            }
            node = child_slot;
        }

        // Leaf level: store value
        unsigned const IDX = slot_index(key, 1);
        T& slot = value_at(node, IDX);
        bool const WAS_EMPTY = (slot == T{});
        slot = value;
        if (WAS_EMPTY && value != T{}) {
            node->populated++;
            m_size++;
        } else if (!WAS_EMPTY && value == T{}) {
            node->populated--;
            m_size--;
        }
        return true;
    }

    // Remove value at key. Returns the removed value, or T{} if not present.
    T remove(uint64_t key) {
        if ((m_root == nullptr) || key >= capacity_at_depth(m_depth)) {
            return T{};
        }

        // Walk down, recording path for cleanup
        struct PathEntry {
            Node* node;
            unsigned idx;
        };
        std::array<PathEntry, MAX_DEPTH> path{};
        int path_len = 0;

        Node* node = m_root;
        for (unsigned level = m_depth; level > 1; --level) {
            unsigned const IDX = slot_index(key, level);
            Node* child = child_at(node, IDX);
            if (child == nullptr) {
                return T{};
            }
            *std::next(path.begin(), static_cast<ptrdiff_t>(path_len)) = {node, IDX};
            path_len++;
            node = child;
        }

        unsigned const IDX = slot_index(key, 1);
        T& leaf_slot = value_at(node, IDX);
        T old = leaf_slot;
        if (old == T{}) {
            return T{};
        }

        leaf_slot = T{};
        node->populated--;
        m_size--;

        // Prune empty nodes bottom-up
        if (node->populated == 0 && path_len > 0) {
            free_node(node);
            for (int i = path_len - 1; i >= 0; --i) {
                auto& entry = *std::next(path.begin(), static_cast<ptrdiff_t>(i));
                child_at(entry.node, entry.idx) = nullptr;
                entry.node->populated--;
                if (entry.node->populated == 0 && i > 0) {
                    free_node(entry.node);
                } else {
                    break;
                }
            }
        }

        return old;
    }

    // Lookup value at key. Returns T{} (nullptr for pointers) if not present.
    [[nodiscard]] T lookup(uint64_t key) const {
        if ((m_root == nullptr) || key >= capacity_at_depth(m_depth)) {
            return T{};
        }

        const Node* node = m_root;
        for (unsigned level = m_depth; level > 1; --level) {
            unsigned const IDX = slot_index(key, level);
            const Node* child = child_at(node, IDX);
            if (child == nullptr) {
                return T{};
            }
            node = child;
        }

        return value_at(node, slot_index(key, 1));
    }

    // Find first key >= start_key where value == T{} (unset).
    // Returns the key, or UINT64_MAX if none found within current capacity.
    [[nodiscard]] uint64_t find_first_unset(uint64_t start_key = 0) const {
        return find_first_unset_below(start_key, capacity_at_depth(m_depth));
    }

    // Find first key in [start_key, limit) where value == T{}.
    // Ranges beyond the allocated depth are implicitly unset.
    [[nodiscard]] uint64_t find_first_unset_below(uint64_t start_key, uint64_t limit) const {
        if (start_key >= limit) {
            return UINT64_MAX;
        }

        uint64_t const CAP = capacity_at_depth(m_depth);
        if (m_root == nullptr || start_key >= CAP) {
            return start_key;
        }

        uint64_t const SEARCH_LIMIT = std::min(limit, CAP);
        uint64_t const FOUND = find_first_unset_in_node(m_root, m_depth, 0, start_key, SEARCH_LIMIT);
        if (FOUND != UINT64_MAX) {
            return FOUND;
        }
        return CAP < limit ? CAP : UINT64_MAX;
    }

    // Iterate all set entries. Callback: void(uint64_t key, T value)
    template <typename Fn>
    void for_each(const Fn& fn) const {
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

        if (other.m_root == nullptr) {
            return true;
        }

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
    struct InteriorNodeTag {};
    struct LeafNodeTag {};

    // Each node is either an interior node (children) or a leaf (values).
    // We use a union to save memory. Depth 1 = leaf, depth > 1 = interior.
    struct Node {
        union {
            std::array<Node*, FANOUT> children;
            std::array<T, FANOUT> values;
        };
        uint16_t populated{0};  // count of non-null children or non-null values

        explicit Node(InteriorNodeTag /*unused*/) : children{} {}
        explicit Node(LeafNodeTag /*unused*/) : values{} {}

        static_assert(sizeof(children) == sizeof(Node*) * FANOUT);
        static_assert(alignof(decltype(children)) == alignof(Node*));
        static_assert(sizeof(values) == sizeof(T) * FANOUT);
        static_assert(alignof(decltype(values)) == alignof(T));
    };

    Node* m_root{nullptr};
    size_t m_size{0};
    unsigned m_depth{1};  // minimum depth = 1 (single leaf, 64 entries)

    static unsigned slot_index(uint64_t key, unsigned level) { return (key >> ((level - 1) * BITS_PER_LEVEL)) & LEVEL_MASK; }

    static Node*& child_at(Node* node, unsigned idx) { return *std::next(node->children.begin(), static_cast<ptrdiff_t>(idx)); }

    static Node* child_at(const Node* node, unsigned idx) { return *std::next(node->children.begin(), static_cast<ptrdiff_t>(idx)); }

    static T& value_at(Node* node, unsigned idx) { return *std::next(node->values.begin(), static_cast<ptrdiff_t>(idx)); }

    static const T& value_at(const Node* node, unsigned idx) { return *std::next(node->values.begin(), static_cast<ptrdiff_t>(idx)); }

    static uint64_t capacity_at_depth(unsigned depth) {
        if (depth == 0) {
            return 0;
        }
        if (depth >= 11) {
            return UINT64_MAX;  // overflow guard
        }
        uint64_t cap = 1;
        for (unsigned i = 0; i < depth; ++i) {
            cap *= FANOUT;
        }
        return cap;
    }

    static uint64_t saturating_add(uint64_t lhs, uint64_t rhs) {
        if (lhs > UINT64_MAX - rhs) {
            return UINT64_MAX;
        }
        return lhs + rhs;
    }

    Node* alloc_interior_node() {
        void* storage = ker::mod::mm::dyn::kmalloc::malloc(sizeof(Node));
        if (storage == nullptr) {
            return nullptr;
        }
        return new (storage) Node(InteriorNodeTag{});
    }

    Node* alloc_leaf_node() {
        void* storage = ker::mod::mm::dyn::kmalloc::malloc(sizeof(Node));
        if (storage == nullptr) {
            return nullptr;
        }
        return new (storage) Node(LeafNodeTag{});
    }

    void free_node(Node* n) {
        if (n == nullptr) {
            return;
        }
        n->~Node();
        ker::mod::mm::dyn::kmalloc::free(n);
    }

    // Grow tree depth by 1: allocate new root, make old root its child[0]
    [[nodiscard]] bool grow_depth() {
        Node* new_root = alloc_interior_node();
        if (new_root == nullptr) {
            return false;
        }

        if (m_root) {
            child_at(new_root, 0) = m_root;
            new_root->populated = 1;
        }
        m_root = new_root;
        m_depth++;
        return true;
    }

    void destroy_node(Node* node, unsigned depth_from_root) {
        if (node == nullptr) {
            return;
        }
        unsigned const LEVEL = m_depth - depth_from_root;
        if (LEVEL > 1) {
            // Interior node: recurse into children
            for (unsigned i = 0; i < FANOUT; ++i) {
                Node* child = child_at(node, i);
                if (child != nullptr) {
                    destroy_node(child, depth_from_root + 1);
                }
            }
        }
        free_node(node);
    }

    template <typename Fn>
    void iterate_node(const Node* node, unsigned level, uint64_t prefix, const Fn& fn) const {
        if (level == 1) {
            // Leaf: iterate values
            for (unsigned i = 0; i < FANOUT; ++i) {
                const T& value = value_at(node, i);
                if (value != T{}) {
                    fn(prefix | i, value);
                }
            }
        } else {
            // Interior: recurse
            for (unsigned i = 0; i < FANOUT; ++i) {
                const Node* child = child_at(node, i);
                if (child != nullptr) {
                    uint64_t const CHILD_PREFIX = prefix | (static_cast<uint64_t>(i) << ((level - 1) * BITS_PER_LEVEL));
                    iterate_node(child, level - 1, CHILD_PREFIX, fn);
                }
            }
        }
    }

    uint64_t find_first_unset_in_node(const Node* node, unsigned level, uint64_t prefix, uint64_t start_key, uint64_t limit) const {
        if (start_key >= limit) {
            return UINT64_MAX;
        }
        if (node == nullptr) {
            uint64_t const CANDIDATE = std::max(prefix, start_key);
            return CANDIDATE < limit ? CANDIDATE : UINT64_MAX;
        }

        if (level == 1) {
            uint64_t const START_IDX = (start_key > prefix) ? std::min<uint64_t>(start_key - prefix, FANOUT) : 0;
            uint64_t const END_IDX = (limit > prefix) ? std::min<uint64_t>(limit - prefix, FANOUT) : 0;
            for (uint64_t idx = START_IDX; idx < END_IDX; ++idx) {
                if (value_at(node, static_cast<unsigned>(idx)) == T{}) {
                    return prefix + idx;
                }
            }
            return UINT64_MAX;
        }

        uint64_t const CHILD_CAP = capacity_at_depth(level - 1);
        if (CHILD_CAP == 0) {
            return UINT64_MAX;
        }

        uint64_t const REL_START = (start_key > prefix) ? start_key - prefix : 0;
        uint64_t const START_IDX = std::min<uint64_t>(REL_START / CHILD_CAP, FANOUT);
        for (uint64_t idx = START_IDX; idx < FANOUT; ++idx) {
            if (idx > (UINT64_MAX - prefix) / CHILD_CAP) {
                break;
            }
            uint64_t const CHILD_PREFIX = prefix + (idx * CHILD_CAP);
            if (CHILD_PREFIX >= limit) {
                break;
            }

            uint64_t const CHILD_END = saturating_add(CHILD_PREFIX, CHILD_CAP);
            if (CHILD_END <= start_key) {
                continue;
            }

            const Node* child = child_at(node, static_cast<unsigned>(idx));
            uint64_t const FOUND = find_first_unset_in_node(child, level - 1, CHILD_PREFIX, start_key, limit);
            if (FOUND != UINT64_MAX) {
                return FOUND;
            }
        }
        return UINT64_MAX;
    }
};

}  // namespace ker::util
