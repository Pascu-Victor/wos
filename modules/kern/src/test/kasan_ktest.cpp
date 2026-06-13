#include <array>
#include <cstddef>
#include <cstdint>
#include <test/ktest.hpp>

#ifdef WOS_KASAN
#include <sanitizer/kasan.hpp>

extern "C" auto __asan_address_is_poisoned(void const volatile* addr) -> int;

namespace {

auto is_poisoned(const std::array<uint8_t, 32>& storage, size_t offset) -> bool {
    return __asan_address_is_poisoned(&storage[offset]) != 0;
}

}  // namespace

KTEST(Kasan, PartialUnpoisonKeepsTrailingRedzone) {
    alignas(8) std::array<uint8_t, 32> storage{};

    ker::mod::kasan::poison_range(storage.data(), storage.size(), ker::mod::kasan::SHADOW_HEAP_FREED);
    ker::mod::kasan::unpoison_range(storage.data(), 17);

    KEXPECT_FALSE(is_poisoned(storage, 0));
    KEXPECT_FALSE(is_poisoned(storage, 16));
    KEXPECT_TRUE(is_poisoned(storage, 17));
    KEXPECT_TRUE(is_poisoned(storage, 23));
    KEXPECT_TRUE(is_poisoned(storage, 24));
}

KTEST(Kasan, PartialPoisonCoversFinalAllocatedByte) {
    alignas(8) std::array<uint8_t, 32> storage{};

    ker::mod::kasan::unpoison_range(storage.data(), 17);
    ker::mod::kasan::poison_range(storage.data(), 17, ker::mod::kasan::SHADOW_HEAP_FREED);

    KEXPECT_TRUE(is_poisoned(storage, 0));
    KEXPECT_TRUE(is_poisoned(storage, 16));
}

KTEST(Kasan, RedzonePoisonPreservesPartialPrefix) {
    alignas(8) std::array<uint8_t, 32> storage{};

    ker::mod::kasan::unpoison_range(storage.data(), 24);
    ker::mod::kasan::poison_range(storage.data() + 17, 7, ker::mod::kasan::SHADOW_HEAP_RREDZONE);

    KEXPECT_FALSE(is_poisoned(storage, 16));
    KEXPECT_TRUE(is_poisoned(storage, 17));
    KEXPECT_TRUE(is_poisoned(storage, 23));
}
#endif
