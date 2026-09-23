#pragma once
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
namespace ker::dev::ivshmem::shared_ring {
struct Extent {
    uint32_t offset = 0, bytes = 0;
};
constexpr auto contains(uint64_t capacity, uint64_t offset, uint64_t bytes) -> bool {
    return offset <= capacity && bytes <= capacity - offset;
}
constexpr auto disjoint(Extent a, Extent b) -> bool {
    return uint64_t{a.offset} + a.bytes <= b.offset || uint64_t{b.offset} + b.bytes <= a.offset;
}
// Header values must be copied exactly once; retain this validated copy, not
// pointers to peer-writable geometry. Ring extents include 8 index bytes.
constexpr auto layout(uint64_t capacity, uint32_t header, std::array<Extent, 2> rings, Extent extra = {}) -> bool {
    if (header < 8 || header % 4 || !contains(capacity, 0, header)) return false;
    for (auto r : rings)
        if (r.offset < header || r.offset % 4 || r.bytes < 16 || r.bytes % 4 || !contains(capacity, r.offset, r.bytes)) return false;
    if (!disjoint(rings[0], rings[1])) return false;
    if (extra.bytes && (extra.offset < header || !contains(capacity, extra.offset, extra.bytes) || !disjoint(extra, rings[0]) ||
                        !disjoint(extra, rings[1])))
        return false;
    return true;
}
enum class Error : uint8_t { none, empty, full, geometry, index, length, incomplete, destination };
struct Result {
    Error error = Error::none;
    uint16_t bytes = 0;
};
constexpr auto valid(uint32_t capacity, uint32_t head, uint32_t tail) -> Error {
    if (capacity < 8 || capacity % 4) return Error::geometry;
    return head >= capacity || tail >= capacity || head % 4 || tail % 4 ? Error::index : Error::none;
}
// Backend loads indices with acquire ordering; publishes only its owned index
// with release ordering. Data access is bounded by the immutable capacity.
// Producer serialization and single-consumer ownership belong to the caller.
template <class Backend>
auto write(Backend& b, uint32_t capacity, std::span<const uint8_t> data) -> Result {
    auto head = b.head(), tail = b.tail();
    auto error = valid(capacity, head, tail);
    if (error != Error::none) return {error};
    if (data.empty() || data.size() > UINT16_MAX) return {Error::length};
    auto length = static_cast<uint16_t>(data.size());
    uint32_t padded = (uint32_t{length} + 5U) & ~3U;
    auto space = head >= tail ? capacity - head + tail : tail - head;
    if (padded >= space) return {Error::full};
    auto at = [&](uint64_t offset) { return static_cast<uint32_t>((uint64_t{head} + offset) % capacity); };
    b.store(at(0), static_cast<uint8_t>(length));
    b.store(at(1), static_cast<uint8_t>(length >> 8U));
    for (size_t i = 0; i < data.size(); ++i) b.store(at(i + 2), data[i]);
    for (uint32_t i = uint32_t{length} + 2; i < padded; ++i) b.store(at(i), 0);
    b.publish_head(at(padded));
    return {Error::none, length};
}
template <class Backend>
auto read(Backend& b, uint32_t capacity, std::span<uint8_t> output) -> Result {
    auto head = b.head(), tail = b.tail();
    auto error = valid(capacity, head, tail);
    if (error != Error::none) return {error};
    if (head == tail) return {Error::empty};
    auto available = head >= tail ? head - tail : capacity - tail + head;
    auto at = [&](uint64_t offset) { return static_cast<uint32_t>((uint64_t{tail} + offset) % capacity); };
    auto length = static_cast<uint16_t>(b.load(at(0)) | uint16_t{b.load(at(1))} << 8U);
    if (length == 0) return {Error::length};
    uint32_t padded = (uint32_t{length} + 5U) & ~3U;
    if (padded > available) return {Error::incomplete};
    if (length > output.size()) return {Error::destination, length};
    for (size_t i = 0; i < length; ++i) output[i] = b.load(at(i + 2));
    b.publish_tail(at(padded));
    return {Error::none, length};
}

// BAR2 is mapped strongly uncacheable (PWT+PCD), not write-combining. Aligned
// lock-free index loads/stores order volatile payload accesses; no MMIO RMW is
// used on a device register. These words are in CPU-accessible shared RAM.
inline auto load_word(uint32_t* word) -> uint32_t { return std::atomic_ref<uint32_t>(*word).load(std::memory_order_acquire); }
inline void store_word(uint32_t* word, uint32_t value) { std::atomic_ref<uint32_t>(*word).store(value, std::memory_order_release); }
static_assert(std::atomic_ref<uint32_t>::is_always_lock_free);
struct View {
    uint32_t* head_ptr = nullptr;
    uint32_t* tail_ptr = nullptr;
    volatile uint8_t* data = nullptr;
    uint32_t size = 0;
    auto head() -> uint32_t { return load_word(head_ptr); }
    auto tail() -> uint32_t { return load_word(tail_ptr); }
    auto load(uint32_t index) -> uint8_t { return data[index]; }
    void store(uint32_t index, uint8_t value) { data[index] = value; }
    void publish_head(uint32_t value) { store_word(head_ptr, value); }
    void publish_tail(uint32_t value) { store_word(tail_ptr, value); }
};
inline auto bind(std::span<uint8_t> shared, Extent area) -> std::optional<View> {
    if (shared.data() == nullptr || (reinterpret_cast<uintptr_t>(shared.data()) & 3U) != 0 || area.offset % 4 || area.bytes < 16 ||
        area.bytes % 4 || !contains(shared.size(), area.offset, area.bytes))
        return {};
    auto* base = shared.data() + area.offset;
    return View{reinterpret_cast<uint32_t*>(base), reinterpret_cast<uint32_t*>(base + 4), base + 8, area.bytes - 8};
}
// Existing raw-rkey API: bounds validation only, not peer authorization.
constexpr auto remote_extent(uint64_t capacity, uint32_t key, uint64_t offset, uint32_t bytes) -> std::optional<uint64_t> {
    if (key == 0 || key >= capacity || offset > capacity - key) return {};
    uint64_t const start = key + offset;
    return contains(capacity, start, bytes) ? std::optional{start} : std::nullopt;
}
constexpr auto rounded_pages(uint32_t bytes, uint64_t capacity) -> std::optional<uint32_t> {
    uint64_t const pages = (uint64_t{bytes} + 4095) / 4096;
    return pages != 0 && pages <= capacity / 4096 ? std::optional{static_cast<uint32_t>(pages)} : std::nullopt;
}

enum class ClaimState : uint8_t { initialize, attach, wait, refused };
struct Claim {
    ClaimState state = ClaimState::refused;
    uint32_t role = 0;
};
constexpr uint32_t INITIALIZING = UINT32_MAX;
// initializer_role is published before magic; only read it after acquiring magic.
inline auto claim(uint32_t* magic, uint32_t* initializer_role, uint32_t expected, int32_t configured_role) -> Claim {
    if (configured_role < -1 || configured_role > 1 || expected == 0 || expected == INITIALIZING) return {};
    auto value = load_word(magic);
    if (value == expected) {
        if (configured_role >= 0) return {ClaimState::attach, static_cast<uint32_t>(configured_role)};
        auto const INITIALIZER = load_word(initializer_role);
        return INITIALIZER <= 1 ? Claim{ClaimState::attach, 1U - INITIALIZER} : Claim{};
    }
    if (value == 0) {
        if (std::atomic_ref<uint32_t>(*magic).compare_exchange_strong(value, INITIALIZING, std::memory_order_acq_rel))
            return {ClaimState::initialize, configured_role < 0 ? 0U : static_cast<uint32_t>(configured_role)};
    }
    return {value == 0 || value == INITIALIZING || value == expected ? ClaimState::wait : ClaimState::refused, 0};
}
struct Layout {
    std::array<Extent, 2> rings{};
    Extent extra{};
};
inline auto snapshot(std::span<uint8_t> shared, uint32_t expected, bool extra) -> std::optional<Layout> {
    if (shared.data() == nullptr || (reinterpret_cast<uintptr_t>(shared.data()) & 3U) != 0 || shared.size() < 64) return {};
    auto* words = reinterpret_cast<uint32_t*>(shared.data());
    if (load_word(words) != expected || load_word(words + 1) != 1) return {};
    Layout result{{{{load_word(words + 2), load_word(words + 3)}, {load_word(words + 4), load_word(words + 5)}}}, {}};
    if (extra) result.extra = {load_word(words + 6), load_word(words + 7)};
    if (load_word(words) != expected || !layout(shared.size(), 64, result.rings, result.extra)) return {};
    return result;
}
}  // namespace ker::dev::ivshmem::shared_ring
