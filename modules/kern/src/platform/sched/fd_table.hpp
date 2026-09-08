#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::mod::sched::task {
template <size_t Capacity>
class FixedFdTable {
   public:
    FixedFdTable() = default;

    FixedFdTable(const FixedFdTable&) = delete;
    auto operator=(const FixedFdTable&) -> FixedFdTable& = delete;

    FixedFdTable(FixedFdTable&&) = delete;
    auto operator=(FixedFdTable&&) -> FixedFdTable& = delete;

    [[nodiscard]] auto insert(uint64_t key, void* value) -> bool {
        if (key >= Capacity || m_reservations[key] != nullptr) {
            return false;
        }

        void*& slot = slot_at(static_cast<size_t>(key));
        if (slot == nullptr && value != nullptr) {
            ++m_size;
        } else if (slot != nullptr && value == nullptr) {
            --m_size;
        }
        slot = value;
        return true;
    }

    auto remove(uint64_t key) -> void* {
        if (key >= Capacity) {
            return nullptr;
        }

        void*& slot = slot_at(static_cast<size_t>(key));
        void* const old = slot;
        if (old != nullptr) {
            slot = nullptr;
            --m_size;
        }
        return old;
    }

    [[nodiscard]] auto lookup(uint64_t key) const -> void* {
        if (key >= Capacity) {
            return nullptr;
        }
        return slot_at(static_cast<size_t>(key));
    }

    [[nodiscard]] auto find_first_unset_below(uint64_t start_key, uint64_t limit) const -> uint64_t {
        if (start_key >= limit || start_key >= Capacity) {
            return UINT64_MAX;
        }

        uint64_t const SEARCH_LIMIT = limit < Capacity ? limit : Capacity;
        for (uint64_t key = start_key; key < SEARCH_LIMIT; ++key) {
            if (slot_at(static_cast<size_t>(key)) == nullptr && m_reservations[key] == nullptr) {
                return key;
            }
        }
        return UINT64_MAX;
    }

    template <typename Fn>
    void for_each(const Fn& fn) const {
        for (size_t i = 0; i < Capacity; ++i) {
            void* const value = slot_at(i);
            if (value != nullptr) {
                fn(static_cast<uint64_t>(i), value);
            }
        }
    }

    [[nodiscard]] auto size() const -> size_t { return m_size; }
    [[nodiscard]] auto empty() const -> bool { return m_size == 0; }

    // Reserved slots are invisible to lookup/iteration and are not inherited.
    [[nodiscard]] auto reserve(uint64_t start, uint64_t limit, const void* owner) -> uint64_t {
        if (owner == nullptr) {
            return UINT64_MAX;
        }
        uint64_t const SLOT = find_first_unset_below(start, limit);
        if (SLOT != UINT64_MAX) {
            m_reservations[SLOT] = owner;
            ++m_reserved;
        }
        return SLOT;
    }
    [[nodiscard]] auto cancel(uint64_t slot, const void* owner) -> bool {
        if (slot >= Capacity || owner == nullptr || m_reservations[slot] != owner) {
            return false;
        }
        m_reservations[slot] = nullptr;
        --m_reserved;
        return true;
    }
    [[nodiscard]] auto commit(uint64_t slot, const void* owner, void* file) -> bool {
        if (file == nullptr || !cancel(slot, owner)) {
            return false;
        }
        return insert(slot, file);
    }
    [[nodiscard]] auto reserved(uint64_t slot) const -> bool { return slot < Capacity && m_reservations[slot] != nullptr; }
    [[nodiscard]] auto owned_by(uint64_t slot, const void* owner) const -> bool {
        return slot < Capacity && owner != nullptr && m_reservations[slot] == owner;
    }
    [[nodiscard]] auto reserved_count() const -> size_t { return m_reserved; }

   private:
    [[nodiscard]] auto slot_at(size_t index) -> void*& {
        // Capacity-bound callers validate keys before indexing the table.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        return m_slots[index];
    }

    [[nodiscard]] auto slot_at(size_t index) const -> void* {
        // Capacity-bound callers validate keys before indexing the table.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        return m_slots[index];
    }

    std::array<void*, Capacity> m_slots{};
    std::array<const void*, Capacity> m_reservations{};
    size_t m_size{};
    size_t m_reserved{};
};

}  // namespace ker::mod::sched::task
