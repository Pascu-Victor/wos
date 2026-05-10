#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::util {

// ObjectPool<T> - typed object pool with ID assignment for kernel use.
//
// Allocates objects via kmalloc and maintains a free-list for fast reuse.
// Each object gets a monotonically increasing ID (useful for PTY numbers,
// device IDs, etc.). Freed objects are recycled for reuse.
//
// Thread safety: all public methods acquire the internal spinlock (IRQ-safe).

template <typename T>
class ObjectPool {
   public:
    struct PoolEntry {
        T object;
        uint64_t id;
        PoolEntry* free_next;  // Intrusive free-list link (only valid when freed)
        bool in_use;
    };

    ObjectPool() = default;

    ~ObjectPool() {
        // Note: does NOT free live objects - caller is responsible for draining
        // before destruction. We only free the free-list entries.
        auto saved = m_lock.lock_irqsave();
        PoolEntry const* cur = m_free_list;
        while (cur) {
            PoolEntry const* next = cur->free_next;
            delete cur;
            cur = next;
        }
        m_free_list = nullptr;
        m_lock.unlock_irqrestore(saved);
    }

    // No copy/move
    ObjectPool(const ObjectPool&) = delete;
    ObjectPool& operator=(const ObjectPool&) = delete;

    // Allocate a new object. Returns pointer to T within PoolEntry, or nullptr on OOM.
    // *out_id receives the assigned ID.
    T* alloc(uint64_t* out_id = nullptr) {
        auto saved = m_lock.lock_irqsave();

        PoolEntry* entry = nullptr;

        // Try free-list first
        if (m_free_list) {
            entry = m_free_list;
            m_free_list = entry->free_next;
            m_free_count--;
        } else {
            // Allocate new
            entry = new (std::nothrow) PoolEntry;
            if (entry == nullptr) {
                m_lock.unlock_irqrestore(saved);
                return nullptr;
            }
        }

        entry->id = m_next_id++;
        entry->free_next = nullptr;
        entry->in_use = true;
        memset(&entry->object, 0, sizeof(T));
        m_live_count++;

        m_peak_count = std::max(m_live_count, m_peak_count);

        if (out_id != nullptr) {
            *out_id = entry->id;
        }

        m_lock.unlock_irqrestore(saved);
        return &entry->object;
    }

    // Free an object previously returned by alloc().
    // The pointer must point to the T within a PoolEntry (i.e., from alloc()).
    void free(T* obj) {
        if (obj == nullptr) {
            return;
        }
        // Recover PoolEntry from T* (T is first member)
        auto* entry = reinterpret_cast<PoolEntry*>(obj);

        auto saved = m_lock.lock_irqsave();
        if (!entry->in_use) {
            // Double-free detection
            m_lock.unlock_irqrestore(saved);
            return;
        }
        entry->in_use = false;
        entry->free_next = m_free_list;
        m_free_list = entry;
        m_free_count++;
        m_live_count--;
        m_lock.unlock_irqrestore(saved);
    }

    // Get the ID of an object returned by alloc()
    static uint64_t id_of(const T* obj) {
        const auto* entry = reinterpret_cast<const PoolEntry*>(obj);
        return entry->id;
    }

    [[nodiscard]] size_t live_count() const { return m_live_count; }
    [[nodiscard]] size_t free_count() const { return m_free_count; }
    [[nodiscard]] size_t peak_count() const { return m_peak_count; }

   private:
    ker::mod::sys::Spinlock m_lock;
    PoolEntry* m_free_list{nullptr};
    uint64_t m_next_id{0};
    size_t m_live_count{0};
    size_t m_free_count{0};
    size_t m_peak_count{0};
};

}  // namespace ker::util
