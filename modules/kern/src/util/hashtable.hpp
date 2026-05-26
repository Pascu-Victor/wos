#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sys/spinlock.hpp>
#include <utility>

namespace ker::util {

// IntrHashTable - intrusive, spinlock-per-bucket hash table for kernel use.
//
// Requirements on T:
//   - T must have a `T* hash_next` member (intrusive chain link)
//
// Template parameters:
//   - T:          Element type (must have hash_next pointer)
//   - KeyExtract: Functor: Key operator()(const T&) - extracts the key from an element
//   - Hash:       Functor: uint64_t operator()(const Key&) - hashes the key
//   - KeyEqual:   Functor: bool operator()(const Key&, const Key&) - key equality
//
// Bucket count is set at construction and can be resized dynamically.
// All public methods acquire per-bucket locks internally (IRQ-safe).
//
// Thread safety: fully concurrent - per-bucket ticket spinlocks.

template <typename T, typename KeyExtract, typename Hash, typename KeyEqual>
class IntrHashTable {
   public:
    struct Bucket {
        ker::mod::sys::Spinlock lock;
        T* head{nullptr};
        uint32_t count{0};
    };

    // Default constructor: creates an empty, un-initialized table.
    // Must call init() before use. Safe for global/static objects that
    // construct before the kernel memory allocator is available.
    IntrHashTable() = default;

    explicit IntrHashTable(size_t initial_buckets) : m_bucket_count(next_pow2(initial_buckets)), m_mask(m_bucket_count - 1) {
        m_buckets = new (std::nothrow) Bucket[m_bucket_count];
    }

    // Deferred initialization — call after memory allocator is available.
    // Returns false on OOM.
    [[nodiscard]] bool init(size_t initial_buckets = 64) {
        if (m_buckets) {
            return true;  // already initialized
        }
        m_bucket_count = next_pow2(initial_buckets);
        m_mask = m_bucket_count - 1;
        m_buckets = new (std::nothrow) Bucket[m_bucket_count];
        return m_buckets != nullptr;
    }

    ~IntrHashTable() { delete[] m_buckets; }

    // No copy
    IntrHashTable(const IntrHashTable&) = delete;
    IntrHashTable& operator=(const IntrHashTable&) = delete;

    // Move
    IntrHashTable(IntrHashTable&& other) noexcept : m_buckets(other.m_buckets), m_bucket_count(other.m_bucket_count), m_mask(other.m_mask) {
        m_size.store(other.m_size.load(std::memory_order_relaxed), std::memory_order_relaxed);
        other.m_buckets = nullptr;
        other.m_bucket_count = 0;
        other.m_mask = 0;
        other.m_size.store(0, std::memory_order_relaxed);
    }

    [[nodiscard]] bool valid() const { return m_buckets != nullptr; }

    // Insert element into table. Element must not already be present.
    // Returns false if table is invalid (OOM at construction).
    bool insert(T* elem) {
        if ((m_buckets == nullptr) || (elem == nullptr)) {
            return false;
        }

        auto key = m_key_extract(*elem);
        size_t const IDX = m_hash(key) & m_mask;
        auto& bucket = m_buckets[IDX];

        auto saved = bucket.lock.lock_irqsave();
        elem->hash_next = bucket.head;
        bucket.head = elem;
        bucket.count++;
        bucket.lock.unlock_irqrestore(saved);

        m_size.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    // Insert element only if predicate returns true while holding the target
    // bucket lock. on_inserted runs after the element is linked but before
    // the bucket is unlocked, which lets callers publish side state atomically
    // with respect to remove/remove_all_by_key on the same key.
    template <typename Predicate, typename OnInserted>
    bool insert_if(T* elem, Predicate&& predicate, OnInserted&& on_inserted) {
        if ((m_buckets == nullptr) || (elem == nullptr)) {
            return false;
        }

        auto key = m_key_extract(*elem);
        size_t const IDX = m_hash(key) & m_mask;
        auto& bucket = m_buckets[IDX];

        auto saved = bucket.lock.lock_irqsave();
        if (!std::forward<Predicate>(predicate)()) {
            bucket.lock.unlock_irqrestore(saved);
            return false;
        }

        elem->hash_next = bucket.head;
        bucket.head = elem;
        bucket.count++;
        m_size.fetch_add(1, std::memory_order_relaxed);
        std::forward<OnInserted>(on_inserted)();
        bucket.lock.unlock_irqrestore(saved);
        return true;
    }

    // Remove element from table. Returns true if found and removed.
    bool remove(T* elem) {
        if ((m_buckets == nullptr) || (elem == nullptr)) {
            return false;
        }

        auto key = m_key_extract(*elem);
        size_t const IDX = m_hash(key) & m_mask;
        auto& bucket = m_buckets[IDX];

        auto saved = bucket.lock.lock_irqsave();
        T** pp = &bucket.head;
        while (*pp) {
            if (*pp == elem) {
                *pp = elem->hash_next;
                elem->hash_next = nullptr;
                bucket.count--;
                bucket.lock.unlock_irqrestore(saved);
                m_size.fetch_sub(1, std::memory_order_relaxed);
                return true;
            }
            pp = &((*pp)->hash_next);
        }
        bucket.lock.unlock_irqrestore(saved);
        return false;
    }

    // Find element by key. Returns nullptr if not found.
    // chain_len is set to the number of links traversed (for perf monitoring).
    template <typename Key>
    T* find(const Key& key, uint32_t* chain_len = nullptr) {
        if (!m_buckets) {
            return nullptr;
        }

        size_t const IDX = m_hash(key) & m_mask;
        auto& bucket = m_buckets[IDX];
        uint32_t traversed = 0;

        auto saved = bucket.lock.lock_irqsave();
        T* cur = bucket.head;
        while (cur) {
            traversed++;
            if (m_key_equal(m_key_extract(*cur), key)) {
                bucket.lock.unlock_irqrestore(saved);
                if (chain_len) {
                    *chain_len = traversed;
                }
                return cur;
            }
            cur = cur->hash_next;
        }
        bucket.lock.unlock_irqrestore(saved);
        if (chain_len) {
            *chain_len = traversed;
        }
        return nullptr;
    }

    // Remove by key. Returns pointer to removed element, or nullptr.
    template <typename Key>
    T* remove_by_key(const Key& key) {
        if (!m_buckets) {
            return nullptr;
        }

        size_t idx = m_hash(key) & m_mask;
        auto& bucket = m_buckets[idx];

        auto saved = bucket.lock.lock_irqsave();
        T** pp = &bucket.head;
        while (*pp) {
            if (m_key_equal(m_key_extract(**pp), key)) {
                T* found = *pp;
                *pp = found->hash_next;
                found->hash_next = nullptr;
                bucket.count--;
                bucket.lock.unlock_irqrestore(saved);
                m_size.fetch_sub(1, std::memory_order_relaxed);
                return found;
            }
            pp = &((*pp)->hash_next);
        }
        bucket.lock.unlock_irqrestore(saved);
        return nullptr;
    }

    // Remove all elements matching key. Callback receives each removed element.
    // Returns count of removed elements.
    template <typename Key, typename Fn>
    size_t remove_all_by_key(const Key& key, Fn fn) {
        if (!m_buckets) {
            return 0;
        }

        size_t const IDX = m_hash(key) & m_mask;
        auto& bucket = m_buckets[IDX];
        size_t removed = 0;

        auto saved = bucket.lock.lock_irqsave();
        T** pp = &bucket.head;
        while (*pp) {
            if (m_key_equal(m_key_extract(**pp), key)) {
                T* found = *pp;
                *pp = found->hash_next;
                found->hash_next = nullptr;
                bucket.count--;
                removed++;
                fn(found);
            } else {
                pp = &((*pp)->hash_next);
            }
        }
        bucket.lock.unlock_irqrestore(saved);
        if (removed > 0) {
            m_size.fetch_sub(removed, std::memory_order_relaxed);
        }
        return removed;
    }

    // Iterate all elements. Callback: void(T*). Acquires each bucket lock in turn.
    // NOT safe to insert/remove during iteration.
    template <typename Fn>
    void for_each(Fn&& fn) {
        if (!m_buckets) {
            return;
        }
        for (size_t i = 0; i < m_bucket_count; ++i) {
            auto& bucket = m_buckets[i];
            auto saved = bucket.lock.lock_irqsave();
            T* cur = bucket.head;
            while (cur) {
                T* next = cur->hash_next;  // save before callback might modify
                std::forward<Fn>(fn)(cur);
                cur = next;
            }
            bucket.lock.unlock_irqrestore(saved);
        }
    }

    [[nodiscard]] size_t size() const { return m_size.load(std::memory_order_relaxed); }
    [[nodiscard]] size_t bucket_count() const { return m_bucket_count; }

    // Compute max chain length across all buckets (diagnostic)
    [[nodiscard]] uint32_t max_chain_length() const {
        uint32_t max_len = 0;
        if (m_buckets == nullptr) {
            return 0;
        }
        for (size_t i = 0; i < m_bucket_count; ++i) {
            if (m_buckets[i].count > max_len) {
                max_len = m_buckets[i].count;
            }
        }
        return max_len;
    }

   private:
    Bucket* m_buckets{nullptr};
    size_t m_bucket_count{0};
    size_t m_mask{0};
    std::atomic<size_t> m_size{0};

    KeyExtract m_key_extract{};
    Hash m_hash{};
    KeyEqual m_key_equal{};

    static size_t next_pow2(size_t v) {
        if (v == 0) {
            return 1;
        }
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v |= v >> 32;
        return v + 1;
    }
};

// Multiplicative hash for integer keys (Knuth's golden ratio)
struct IntHash {
    uint64_t operator()(uint64_t key) const { return key * 0x9E3779B97F4A7C15ULL; }
};

struct IntEqual {
    bool operator()(uint64_t a, uint64_t b) const { return a == b; }
};

// FNV-1a string hash
struct StrHash {
    uint64_t operator()(const char* key) const {
        uint64_t h = 0xcbf29ce484222325ULL;
        while ((*key) != 0) {
            h ^= static_cast<uint8_t>(*key++);
            h *= 0x100000001b3ULL;
        }
        return h;
    }
};

struct StrEqual {
    bool operator()(const char* a, const char* b) const { return strcmp(a, b) == 0; }
};

}  // namespace ker::util
