#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <platform/mm/dyn/kmalloc.hpp>

namespace ker::util {

// SmallVec<T, InlineN> - inline-buffered dynamic array for kernel use.
//
// Stores up to InlineN elements inline (zero heap allocation). On overflow,
// spills to a kmalloc'd heap buffer with 2x growth.  All mutating operations
// return bool (false = OOM) so callers can propagate ENOMEM.
//
// Thread safety: NONE. Caller must hold a spinlock or ensure single-threaded access.
//
// Perf integration: if a PerfSubsystem is wired in by the subsystem using this
// container, the subsystem is responsible for calling record_container_stat()
// at the appropriate points - SmallVec itself is a dumb container.

template <typename T, size_t InlineN>
class SmallVec {
   public:
    SmallVec() = default;

    ~SmallVec() {
        if (m_heap) {
            ker::mod::mm::dyn::kmalloc::free(m_heap);
        }
    }

    // No copy - explicit clone() instead to make allocation visible
    SmallVec(const SmallVec&) = delete;
    SmallVec& operator=(const SmallVec&) = delete;

    // Move
    SmallVec(SmallVec&& other) noexcept { move_from(other); }

    SmallVec& operator=(SmallVec&& other) noexcept {
        if (this != &other) {
            if (m_heap) {
                ker::mod::mm::dyn::kmalloc::free(m_heap);
            }
            move_from(other);
        }
        return *this;
    }

    // Deep copy - returns false on OOM
    [[nodiscard]] bool clone_from(const SmallVec& other) {
        if (this == &other) return true;
        clear();
        for (size_t i = 0; i < other.m_size; ++i) {
            if (!push_back(other[i])) return false;
        }
        return true;
    }

    // Append element. Returns false on OOM (element NOT added).
    [[nodiscard]] bool push_back(const T& val) {
        if (m_size < InlineN) {
            m_inline[m_size++] = val;
            return true;
        }
        // Need heap
        if (m_size >= m_capacity) {
            if (!grow()) return false;
        }
        m_heap[m_size - InlineN] = val;
        m_size++;
        return true;
    }

    void pop_back() {
        if (m_size > 0) {
            m_size--;
        }
    }

    // Remove element at index, shift remaining left. Returns false if index out of bounds.
    bool remove_at(size_t index) {
        if (index >= m_size) return false;
        // Shift everything left by 1
        for (size_t i = index; i < m_size - 1; ++i) {
            at_mut(i) = at_ref(i + 1);
        }
        m_size--;
        return true;
    }

    // Remove first occurrence of val. Returns true if found and removed.
    bool remove(const T& val) {
        for (size_t i = 0; i < m_size; ++i) {
            if (at_ref(i) == val) {
                return remove_at(i);
            }
        }
        return false;
    }

    // Find index of val, returns m_size if not found
    [[nodiscard]] size_t find(const T& val) const {
        for (size_t i = 0; i < m_size; ++i) {
            if (at_ref(i) == val) return i;
        }
        return m_size;
    }

    [[nodiscard]] bool contains(const T& val) const { return find(val) < m_size; }

    // Access
    const T& operator[](size_t i) const { return at_ref(i); }
    T& operator[](size_t i) { return at_mut(i); }

    [[nodiscard]] size_t size() const { return m_size; }
    [[nodiscard]] bool empty() const { return m_size == 0; }
    [[nodiscard]] size_t capacity() const { return m_capacity > 0 ? InlineN + m_capacity : InlineN; }
    [[nodiscard]] bool is_spilled() const { return m_heap != nullptr; }

    void clear() {
        m_size = 0;
        // Keep heap allocation around for reuse
    }

    // Iterate
    template <typename Fn>
    void for_each(Fn&& fn) const {
        for (size_t i = 0; i < m_size; ++i) {
            fn(at_ref(i));
        }
    }

    template <typename Fn>
    void for_each(Fn&& fn) {
        for (size_t i = 0; i < m_size; ++i) {
            fn(at_mut(i));
        }
    }

    // Pointer access for C-style APIs
    T* data() {
        if (m_size == 0) return nullptr;
        if (m_size <= InlineN) return m_inline;
        // Can't return contiguous pointer across inline+heap
        return nullptr;
    }

   private:
    T m_inline[InlineN]{};
    T* m_heap{nullptr};
    size_t m_size{0};
    size_t m_capacity{0};  // heap capacity (elements beyond InlineN)

    const T& at_ref(size_t i) const {
        if (i < InlineN) return m_inline[i];
        return m_heap[i - InlineN];
    }

    T& at_mut(size_t i) {
        if (i < InlineN) return m_inline[i];
        return m_heap[i - InlineN];
    }

    [[nodiscard]] bool grow() {
        size_t new_cap = m_capacity == 0 ? InlineN : m_capacity * 2;
        auto* new_buf = static_cast<T*>(ker::mod::mm::dyn::kmalloc::malloc(new_cap * sizeof(T)));
        if (!new_buf) return false;  // OOM

        // Copy existing heap elements
        if (m_heap && m_capacity > 0) {
            size_t heap_used = m_size - InlineN;
            memcpy(new_buf, m_heap, heap_used * sizeof(T));
            ker::mod::mm::dyn::kmalloc::free(m_heap);
        }
        m_heap = new_buf;
        m_capacity = new_cap;
        return true;
    }

    void move_from(SmallVec& other) {
        memcpy(m_inline, other.m_inline, InlineN * sizeof(T));
        m_heap = other.m_heap;
        m_size = other.m_size;
        m_capacity = other.m_capacity;
        other.m_heap = nullptr;
        other.m_size = 0;
        other.m_capacity = 0;
    }
};

}  // namespace ker::util
