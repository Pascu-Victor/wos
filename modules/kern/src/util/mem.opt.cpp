#include <cstddef>
#include <cstdint>
#include <cstring>

namespace {

constexpr size_t SINGLE_BYTE = 1;
constexpr size_t WORD16_BYTES = sizeof(uint16_t);
constexpr size_t WORD32_BYTES = sizeof(uint32_t);
constexpr size_t WORD64_BYTES = sizeof(uint64_t);
constexpr size_t TINY16_MAX = 3;
constexpr size_t TINY32_MAX = 7;
constexpr size_t TINY_THRESHOLD = 16;
constexpr unsigned BITS_PER_BYTE = 8U;
constexpr unsigned BYTE_INDEX_SHIFT = 3U;
constexpr uint64_t BYTE_MASK = 0xffU;
constexpr uint16_t FILL16_MULTIPLIER = 0x0101U;
constexpr uint32_t FILL32_MULTIPLIER = 0x01010101U;
constexpr uint64_t FILL64_MULTIPLIER = 0x0101010101010101ULL;

template <typename T>
struct [[gnu::packed, gnu::may_alias]] Unaligned {
    T value;
};

template <typename T>
[[nodiscard]] inline auto load_unaligned(const void* ptr) -> T {
    return reinterpret_cast<const Unaligned<T>*>(ptr)->value;
}

template <typename T>
inline void store_unaligned(void* ptr, T value) {
    reinterpret_cast<Unaligned<T>*>(ptr)->value = value;
}

inline void memcpy_tiny(unsigned char* dst, const unsigned char* src, size_t size) {
    if (size == 0) {
        return;
    }
    if (size == SINGLE_BYTE) {
        dst[0] = src[0];
        return;
    }
    if (size <= TINY16_MAX) {
        store_unaligned<uint16_t>(dst, load_unaligned<uint16_t>(src));
        store_unaligned<uint16_t>(dst + size - WORD16_BYTES, load_unaligned<uint16_t>(src + size - WORD16_BYTES));
        return;
    }
    if (size <= TINY32_MAX) {
        store_unaligned<uint32_t>(dst, load_unaligned<uint32_t>(src));
        store_unaligned<uint32_t>(dst + size - WORD32_BYTES, load_unaligned<uint32_t>(src + size - WORD32_BYTES));
        return;
    }

    store_unaligned<uint64_t>(dst, load_unaligned<uint64_t>(src));
    store_unaligned<uint64_t>(dst + size - WORD64_BYTES, load_unaligned<uint64_t>(src + size - WORD64_BYTES));
}

inline void memset_tiny(unsigned char* dst, unsigned char byte, size_t size) {
    const uint16_t FILL16 = static_cast<uint16_t>(byte) * FILL16_MULTIPLIER;
    const uint32_t FILL32 = static_cast<uint32_t>(byte) * FILL32_MULTIPLIER;
    const uint64_t FILL64 = static_cast<uint64_t>(byte) * FILL64_MULTIPLIER;

    if (size == 0) {
        return;
    }
    if (size == SINGLE_BYTE) {
        dst[0] = byte;
        return;
    }
    if (size <= TINY16_MAX) {
        store_unaligned<uint16_t>(dst, FILL16);
        store_unaligned<uint16_t>(dst + size - WORD16_BYTES, FILL16);
        return;
    }
    if (size <= TINY32_MAX) {
        store_unaligned<uint32_t>(dst, FILL32);
        store_unaligned<uint32_t>(dst + size - WORD32_BYTES, FILL32);
        return;
    }

    store_unaligned<uint64_t>(dst, FILL64);
    store_unaligned<uint64_t>(dst + size - WORD64_BYTES, FILL64);
}

}  // namespace

extern "C" {
// NOLINTBEGIN(readability-identifier-naming)
auto memcpy(void* __restrict __dest, const void* __restrict __src, size_t __size) -> void* {
    auto* dst = static_cast<unsigned char*>(__dest);
    const auto* src = static_cast<const unsigned char*>(__src);

    if (__size <= TINY_THRESHOLD) {
        memcpy_tiny(dst, src, __size);
        return __dest;
    }

    asm volatile(
        "cld\n\t"
        "rep movsb\n\t"
        : "+D"(dst), "+S"(src), "+c"(__size)
        :
        : "memory", "cc");

    return __dest;
}

auto memset(void* __dest, int __c, size_t __size) -> void* {
    auto* dst = static_cast<unsigned char*>(__dest);
    auto byte = static_cast<unsigned char>(__c);

    if (__size <= TINY_THRESHOLD) {
        memset_tiny(dst, byte, __size);
        return __dest;
    }

    asm volatile(
        "cld\n\t"
        "rep stosb\n\t"
        : "+D"(dst), "+c"(__size)
        : "a"(byte)
        : "memory", "cc");

    return __dest;
}

auto memmove(void* __dest, const void* __src, size_t __size) -> void* {
    auto* dst = static_cast<unsigned char*>(__dest);
    const auto* src = static_cast<const unsigned char*>(__src);

    if (__size == 0 || dst == src) {
        return __dest;
    }

    if (dst < src || dst >= src + __size) {
        return memcpy(__dest, __src, __size);
    }

    auto* dst_end = dst + __size;
    const auto* src_end = src + __size;
    while (__size >= WORD64_BYTES) {
        dst_end -= WORD64_BYTES;
        src_end -= WORD64_BYTES;
        __size -= WORD64_BYTES;
        store_unaligned<uint64_t>(dst_end, load_unaligned<uint64_t>(src_end));
    }
    while (__size != 0) {
        --dst_end;
        --src_end;
        --__size;
        *dst_end = *src_end;
    }

    return __dest;
}

auto memcmp(const void* __a, const void* __b, size_t __size) -> int {
    const auto* lhs = static_cast<const unsigned char*>(__a);
    const auto* rhs = static_cast<const unsigned char*>(__b);

    while (__size >= WORD64_BYTES) {
        const auto left = load_unaligned<uint64_t>(lhs);
        const auto right = load_unaligned<uint64_t>(rhs);
        if (left != right) {
            const uint64_t diff = left ^ right;
            const auto byte_index = static_cast<unsigned>(__builtin_ctzll(diff) >> BYTE_INDEX_SHIFT);
            const auto bit_shift = byte_index * BITS_PER_BYTE;
            const auto left_byte = static_cast<unsigned>((left >> bit_shift) & BYTE_MASK);
            const auto right_byte = static_cast<unsigned>((right >> bit_shift) & BYTE_MASK);
            return static_cast<int>(left_byte) - static_cast<int>(right_byte);
        }
        lhs += WORD64_BYTES;
        rhs += WORD64_BYTES;
        __size -= WORD64_BYTES;
    }

    while (__size != 0) {
        const int diff = static_cast<int>(*lhs) - static_cast<int>(*rhs);
        if (diff != 0) {
            return diff;
        }
        ++lhs;
        ++rhs;
        --__size;
    }

    return 0;
}
// NOLINTEND(readability-identifier-naming)
}  // extern "C"
