#pragma once

#include <platform/security/credentials.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::mod::security {

// A reader owns its immutable value copy after the short IRQ-safe lock. No
// heap, refcount increment-after-free window, seqlock data race, or reclamation
// wait is needed. Process integration must share this store among siblings.
class CredentialStore {
   public:
    explicit CredentialStore(const Snapshot& initial = {}) : m_snapshot(initial) {}
    CredentialStore(const CredentialStore&) = delete;
    auto operator=(const CredentialStore&) -> CredentialStore& = delete;

    struct Observation {
        Snapshot credentials;
        uint64_t generation;
    };

    class Transaction {
       public:
        explicit Transaction(CredentialStore& store) : m_store(store), m_flags(store.m_lock.lock_irqsave()) {}
        ~Transaction() { m_store.m_lock.unlock_irqrestore(m_flags); }
        Transaction(const Transaction&) = delete;
        auto operator=(const Transaction&) -> Transaction& = delete;
        [[nodiscard]] auto current() const -> const Snapshot& { return m_store.m_snapshot; }
        [[nodiscard]] auto can_publish(const Snapshot& next) const -> bool {
            return next == current() || m_store.m_generation != UINT64_MAX;
        }
        [[nodiscard]] auto publish(const Snapshot& next) -> int {
            if (!can_publish(next)) return -EOVERFLOW;
            if (next != current()) {
                m_store.m_snapshot = next;
                ++m_store.m_generation;
            }
            return 0;
        }

       private:
        CredentialStore& m_store;
        uint64_t m_flags;
    };

    [[nodiscard]] auto read() const -> Observation {
        uint64_t const FLAGS = m_lock.lock_irqsave();
        Observation const RESULT{m_snapshot, m_generation};
        m_lock.unlock_irqrestore(FLAGS);
        return RESULT;
    }

    // The input span must refer to stable kernel memory, never user memory.
    // Dependent UID population transactions will use the process integration
    // seam, not this standalone model publication API.
    [[nodiscard]] auto update(const Request& request) -> int {
        Transaction transaction(*this);
        Snapshot next;
        int const RESULT = transaction.current().prepare(request, next);
        return RESULT == 0 ? transaction.publish(next) : RESULT;
    }

   private:
    mutable sys::Spinlock m_lock;
    Snapshot m_snapshot;
    uint64_t m_generation{1};
};

}  // namespace ker::mod::security
