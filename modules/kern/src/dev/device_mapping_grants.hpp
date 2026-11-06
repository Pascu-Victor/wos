#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <dev/device_mapping_object.hpp>
#include <memory>
#include <platform/sys/mutex.hpp>

namespace ker::dev::mapping {

struct ClientAccess;
class Client;
struct Grant {
    uint64_t generation{}, base{}, end{};
    [[nodiscard]] auto empty() const noexcept -> bool { return generation == 0 && base == 0 && end == 0; }
};

namespace detail {
// Actual production allocator; a separate local instance can test exhaustion.
// No reset/accessor mutates the permanent global allocator.
class OffsetIntervals {
   public:
    static constexpr uint64_t LIMIT = static_cast<uint64_t>(INT64_MAX) & ~(OBJECT_PAGE_SIZE - 1);
    explicit constexpr OffsetIntervals(uint64_t start = OBJECT_PAGE_SIZE, uint64_t limit = LIMIT) : next_(start), limit_(limit) {}
    [[nodiscard]] auto reserve(uint64_t length, Grant& empty) noexcept -> bool;

   private:
    static_assert(std::atomic<uint64_t>::is_always_lock_free);
    std::atomic<uint64_t> next_;
    const uint64_t limit_;
};
}  // namespace detail

class PreparedGrant {
   public:
    PreparedGrant() = default;
    PreparedGrant(const PreparedGrant&) = delete;
    auto operator=(const PreparedGrant&) -> PreparedGrant& = delete;
    PreparedGrant(PreparedGrant&&) noexcept = default;
    auto operator=(PreparedGrant&&) noexcept -> PreparedGrant& = default;
    [[nodiscard]] auto object() const noexcept -> const ObjectRef& { return object_; }
    [[nodiscard]] auto object_offset() const noexcept -> uint64_t { return offset_; }
    [[nodiscard]] auto length() const noexcept -> uint64_t { return length_; }
    [[nodiscard]] auto max_protections() const noexcept -> uint64_t { return max_protections_; }
    [[nodiscard]] auto generation() const noexcept -> uint64_t { return generation_; }
    [[nodiscard]] auto take_object() noexcept -> ObjectRef;
    void reset() noexcept;
    explicit operator bool() const noexcept { return static_cast<bool>(object_); }

   private:
    friend class GrantTable;
    ObjectRef object_;
    uint64_t offset_{}, length_{}, max_protections_{}, generation_{};
};

class GrantTable {
   public:
    static constexpr size_t MAX_CAPACITY = 256;
    [[nodiscard]] static auto create(size_t capacity, std::unique_ptr<GrantTable>& empty) -> int;
    ~GrantTable();
    GrantTable(const GrantTable&) = delete;
    auto operator=(const GrantTable&) -> GrantTable& = delete;

   private:
    friend class Client;
    struct Entry;
    GrantTable() = default;
    [[nodiscard]] auto grant(ClientAccess, const ObjectRef&, uint64_t object_offset, uint64_t length, uint64_t max_protections,
                             Grant& empty) -> int;
    [[nodiscard]] auto lookup(ClientAccess, uint64_t fake_offset, uint64_t length, uint64_t protections, uint64_t flags,
                              PreparedGrant& empty) -> int;
    [[nodiscard]] auto remove(Grant) -> int;
    void close() noexcept;
    ker::mod::sys::Mutex lock_;
    std::unique_ptr<Entry[]> entries_;
    size_t capacity_{};
    bool closed_{};
};

}  // namespace ker::dev::mapping
