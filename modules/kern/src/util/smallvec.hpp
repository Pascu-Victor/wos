#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <new>
#include <platform/mm/dyn/kmalloc.hpp>
#include <util/hcf.hpp>

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
    class Iterator;
    class ConstIterator;

    using iterator = Iterator;
    using const_iterator = ConstIterator;

    class Iterator {
       public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = T;
        using difference_type = ptrdiff_t;
        using pointer = T*;
        using reference = T&;

        Iterator() = default;

        auto operator*() const -> T& { return m_vec->at_mut(m_index); }
        auto operator->() const -> T* { return &m_vec->at_mut(m_index); }

        auto operator++() -> Iterator& {
            m_index++;
            return *this;
        }

        auto operator++(int) -> Iterator {
            auto old = *this;
            ++(*this);
            return old;
        }

        friend auto operator==(const Iterator& lhs, const Iterator& rhs) -> bool {
            return lhs.m_vec == rhs.m_vec && lhs.m_index == rhs.m_index;
        }

        friend auto operator!=(const Iterator& lhs, const Iterator& rhs) -> bool { return !(lhs == rhs); }

       private:
        friend class SmallVec;
        friend class ConstIterator;

        Iterator(SmallVec* vec, size_t index) : m_vec(vec), m_index(index) {}

        SmallVec* m_vec{nullptr};
        size_t m_index{0};
    };

    class ConstIterator {
       public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = T;
        using difference_type = ptrdiff_t;
        using pointer = const T*;
        using reference = const T&;

        ConstIterator() = default;

        ConstIterator(const iterator& other) : m_vec(other.m_vec), m_index(other.m_index) {}

        auto operator*() const -> const T& { return m_vec->at_ref(m_index); }
        auto operator->() const -> const T* { return &m_vec->at_ref(m_index); }

        auto operator++() -> ConstIterator& {
            m_index++;
            return *this;
        }

        auto operator++(int) -> ConstIterator {
            auto old = *this;
            ++(*this);
            return old;
        }

        friend auto operator==(const ConstIterator& lhs, const ConstIterator& rhs) -> bool {
            return lhs.m_vec == rhs.m_vec && lhs.m_index == rhs.m_index;
        }

        friend auto operator!=(const ConstIterator& lhs, const ConstIterator& rhs) -> bool { return !(lhs == rhs); }

       private:
        friend class SmallVec;

        ConstIterator(const SmallVec* vec, size_t index) : m_vec(vec), m_index(index) {}

        const SmallVec* m_vec{nullptr};
        size_t m_index{0};
    };

    SmallVec() = default;

    ~SmallVec() { delete[] m_heap; }

    // No copy - explicit clone() instead to make allocation visible
    SmallVec(const SmallVec&) = delete;
    auto operator=(const SmallVec&) -> SmallVec& = delete;

    // Move
    SmallVec(SmallVec&& other) noexcept { move_from(other); }

    auto operator=(SmallVec&& other) noexcept -> SmallVec& {
        if (this != &other) {
            delete[] m_heap;
            move_from(other);
        }
        return *this;
    }

    // Deep copy - returns false on OOM
    [[nodiscard]] auto clone_from(const SmallVec& other) -> bool {
        if (this == &other) {
            return true;
        }
        clear();
        for (size_t i = 0; i < other.m_size; ++i) {
            if (!push_back(other.at_ref(i))) {
                return false;
            }
        }
        return true;
    }

    // Append element. Returns false on OOM (element NOT added).
    [[nodiscard]] auto push_back(const T& val) -> bool {
        if (m_size < InlineN) {
            *std::next(m_inline.begin(), static_cast<ptrdiff_t>(m_size)) = val;
            m_size++;
            return true;
        }
        // Need heap
        if (m_size >= m_capacity) {
            if (!grow()) {
                return false;
            }
        }
        *std::next(m_heap, static_cast<ptrdiff_t>(m_size - InlineN)) = val;
        m_size++;
        return true;
    }

    void pop_back() {
        if (m_size > 0) {
            m_size--;
        }
    }

    // Remove element at index, shift remaining left. Returns false if index out of bounds.
    auto remove_at(size_t index) -> bool {
        if (index >= m_size) {
            return false;
        }
        // Shift everything left by 1
        for (size_t i = index; i < m_size - 1; ++i) {
            at_mut(i) = at_ref(i + 1);
        }
        m_size--;
        return true;
    }

    // Remove first occurrence of val. Returns true if found and removed.
    auto remove(const T& val) -> bool {
        for (size_t i = 0; i < m_size; ++i) {
            if (at_ref(i) == val) {
                return remove_at(i);
            }
        }
        return false;
    }

    // Find index of val, returns m_size if not found
    [[nodiscard]] auto find(const T& val) const -> size_t {
        for (size_t i = 0; i < m_size; ++i) {
            if (at_ref(i) == val) {
                return i;
            }
        }
        return m_size;
    }

    [[nodiscard]] auto contains(const T& val) const -> bool { return find(val) < m_size; }

    // Access
    auto operator[](size_t i) const -> const T& { return at_ref(i); }
    auto operator[](size_t i) -> T& { return at_mut(i); }
    [[nodiscard]] auto at(size_t i) const -> const T& {
        if (i >= m_size) {
            hcf();
        }
        return at_ref(i);
    }
    auto at(size_t i) -> T& {
        if (i >= m_size) {
            hcf();
        }
        return at_mut(i);
    }

    [[nodiscard]] auto size() const -> size_t { return m_size; }
    [[nodiscard]] auto empty() const -> bool { return m_size == 0; }
    [[nodiscard]] auto capacity() const -> size_t { return m_capacity > 0 ? InlineN + m_capacity : InlineN; }
    [[nodiscard]] auto is_spilled() const -> bool { return m_heap != nullptr; }

    [[nodiscard]] auto begin() -> iterator { return iterator(this, 0); }
    [[nodiscard]] auto end() -> iterator { return iterator(this, m_size); }
    [[nodiscard]] auto begin() const -> const_iterator { return const_iterator(this, 0); }
    [[nodiscard]] auto end() const -> const_iterator { return const_iterator(this, m_size); }
    [[nodiscard]] auto cbegin() const -> const_iterator { return begin(); }
    [[nodiscard]] auto cend() const -> const_iterator { return end(); }

    void clear() {
        m_size = 0;
        // Keep heap allocation around for reuse
    }

    // Iterate
    template <typename Fn>
    void for_each(Fn& fn) const {
        for (size_t i = 0; i < m_size; ++i) {
            fn(at_ref(i));
        }
    }

    template <typename Fn>
    void for_each(Fn& fn) {
        for (size_t i = 0; i < m_size; ++i) {
            fn(at_mut(i));
        }
    }

    // Pointer access for C-style APIs
    auto data() -> T* {
        if (m_size == 0) {
            return nullptr;
        }
        if (m_size <= InlineN) {
            return m_inline.data();
        }
        // Can't return contiguous pointer across inline+heap
        return nullptr;
    }

   private:
    std::array<T, InlineN> m_inline{};
    T* m_heap{nullptr};
    size_t m_size{0};
    size_t m_capacity{0};  // heap capacity (elements beyond InlineN)

    [[nodiscard]] auto at_ref(size_t i) const -> const T& {
        if (i < InlineN) {
            return *std::next(m_inline.begin(), static_cast<ptrdiff_t>(i));
        }
        return *std::next(m_heap, static_cast<ptrdiff_t>(i - InlineN));
    }

    auto at_mut(size_t i) -> T& {
        if (i < InlineN) {
            return *std::next(m_inline.begin(), static_cast<ptrdiff_t>(i));
        }
        return *std::next(m_heap, static_cast<ptrdiff_t>(i - InlineN));
    }

    [[nodiscard]] auto grow() -> bool {
        size_t const NEW_CAP = m_capacity == 0 ? InlineN : m_capacity * 2;
        auto* new_buf = new (std::nothrow) T[NEW_CAP];
        if (new_buf == nullptr) {
            return false;  // OOM
        }

        // Copy existing heap elements
        if (m_heap && m_capacity > 0) {
            size_t const HEAP_USED = m_size - InlineN;
            std::copy_n(m_heap, HEAP_USED, new_buf);
            delete[] m_heap;
        }
        m_heap = new_buf;
        m_capacity = NEW_CAP;
        return true;
    }

    void move_from(SmallVec& other) {
        std::copy_n(other.m_inline.data(), InlineN, m_inline.data());
        m_heap = other.m_heap;
        m_size = other.m_size;
        m_capacity = other.m_capacity;
        other.m_heap = nullptr;
        other.m_size = 0;
        other.m_capacity = 0;
    }
};

}  // namespace ker::util
