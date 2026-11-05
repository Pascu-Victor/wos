#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <dev/firmware/types.hpp>
#include <span>

namespace ker::dev::firmware {
namespace detail {
[[nodiscard]] auto cache_policy_error(const Policy& policy, uint32_t required_revision) -> Error;
[[nodiscard]] auto cache_index_error(std::span<const ArchiveEntry> entries) -> Error;
[[nodiscard]] auto cache_digest_matches(std::span<const uint8_t> bytes, const Digest& expected) -> bool;
}  // namespace detail

// The caller serializes every operation, including stats and binding. Index
// storage, names and payload bytes must remain immutable and accessible for the
// whole cache lifetime. Handles belong to this exact instance; the public Scope
// keeps that ownership private. Neither this engine nor a Handle owns a Task.
template <size_t SlotCount = CACHE_SLOTS, uint32_t ReferenceLimit = MAX_REFERENCES, uint64_t GenerationLimit = UINT64_MAX>
class CacheEngine {
    static_assert(SlotCount > 0 && SlotCount <= CACHE_SLOTS);
    static_assert(ReferenceLimit > 0 && ReferenceLimit <= MAX_REFERENCES);
    static_assert(GenerationLimit > 0);

   public:
    CacheEngine() = default;
    CacheEngine(const CacheEngine&) = delete;
    auto operator=(const CacheEngine&) -> CacheEngine& = delete;
    CacheEngine(CacheEngine&&) = delete;
    auto operator=(CacheEngine&&) -> CacheEngine& = delete;

    [[nodiscard]] auto bind(std::span<const ArchiveEntry> entries) -> Error {
        if (bound_) return Error::policy;
        Error const ERROR = detail::cache_index_error(entries);
        if (ERROR != Error::none) return ERROR;
        entries_ = entries;
        bound_ = true;  // An empty successful binding is still permanent.
        return Error::none;
    }

    [[nodiscard]] auto acquire(const Policy& policy, uint32_t required_revision) -> CacheResult {
        Error const POLICY_ERROR = detail::cache_policy_error(policy, required_revision);
        if (POLICY_ERROR != Error::none) return {.error = POLICY_ERROR};
        if (!bound_) return {.error = Error::unavailable};

        size_t free_slot = SlotCount;
        for (size_t i = 0; i < slots_.size(); ++i) {
            auto& slot = slots_[i];
            if (slot.references == 0) {
                if (free_slot == SlotCount) free_slot = i;
                continue;
            }
            if (slot.name_view() != policy.name) continue;
            if (!slot.matches(policy)) return {.error = Error::policy_conflict};
            if (slot.references == ReferenceLimit) return {.error = Error::reference_limit};
            ++slot.references;
            ++stats_.references;
            increment(stats_.hits);
            return {.handle = {static_cast<uint16_t>(i), slot.generation}, .bytes = slot.bytes};
        }

        const ArchiveEntry* source = nullptr;
        for (auto const& entry : entries_) {
            if (entry.name == policy.name) {
                source = &entry;
                break;
            }
        }
        if (source == nullptr) return {.error = Error::not_found};
        if (source->bytes.size() != policy.length || source->bytes.size() > policy.maximum) return {.error = Error::length};
        if (free_slot == SlotCount) return {.error = Error::cache_full};
        if (generation_ == GenerationLimit) return {.error = Error::generation_exhausted};

        increment(stats_.verifications);
        if (!detail::cache_digest_matches(source->bytes, policy.sha256)) return {.error = Error::digest};

        auto& slot = slots_[free_slot];
        slot.name_length = policy.name.size();
        for (size_t i = 0; i < slot.name_length; ++i) slot.name[i] = policy.name[i];
        slot.name[slot.name_length] = '\0';
        slot.length = policy.length;
        slot.maximum = policy.maximum;
        slot.sha256 = policy.sha256;
        slot.revision = policy.revision;
        slot.bytes = source->bytes;
        slot.generation = ++generation_;  // Capacity was checked before hashing.
        slot.references = 1;              // Publish only the fully verified key/view.
        ++stats_.objects;
        ++stats_.references;
        return {.handle = {static_cast<uint16_t>(free_slot), slot.generation}, .bytes = slot.bytes};
    }

    [[nodiscard]] auto release(Handle handle) -> Error {
        if (handle.slot >= slots_.size() || handle.generation == 0) return Error::stale;
        auto& slot = slots_[handle.slot];
        if (slot.references == 0 || slot.generation != handle.generation) return Error::stale;
        --slot.references;
        --stats_.references;
        if (slot.references == 0) {
            slot = {};  // No payload allocation, callback or source mutation.
            --stats_.objects;
        }
        return Error::none;
    }

    [[nodiscard]] auto stats() const -> Stats { return stats_; }

   private:
    struct Slot {
        std::array<char, MAX_NAME_BYTES> name{};
        size_t name_length = 0;
        uint64_t length = 0;
        uint64_t maximum = 0;
        Digest sha256{};
        uint32_t revision = 0;
        std::span<const uint8_t> bytes{};
        uint64_t generation = 0;
        uint32_t references = 0;

        [[nodiscard]] auto name_view() const -> std::string_view { return {name.data(), name_length}; }
        [[nodiscard]] auto matches(const Policy& policy) const -> bool {
            return length == policy.length && maximum == policy.maximum && sha256 == policy.sha256 && revision == policy.revision;
        }
    };
    static void increment(uint64_t& counter) {
        if (counter != UINT64_MAX) ++counter;  // Diagnostics never wrap ownership.
    }
    std::array<Slot, SlotCount> slots_{};
    std::span<const ArchiveEntry> entries_{};
    uint64_t generation_ = 0;  // Shared across slots, including evicted incarnations.
    Stats stats_{};
    bool bound_ = false;
};

using Cache = CacheEngine<>;
}  // namespace ker::dev::firmware
