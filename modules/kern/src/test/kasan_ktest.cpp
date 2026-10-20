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
KTEST(Kasan, AlignedUnpoisonPreservesAdjacentShadowAcrossPages) {
    // Four shadow pages exercise the bulk path; the first/last application
    // granules remain poisoned around the exact cleared interior.
    alignas(8) std::array<uint8_t, 131088> storage{};
    auto* const BEGIN = storage.data();
    ker::mod::kasan::poison_range(BEGIN, storage.size(), ker::mod::kasan::SHADOW_HEAP_FREED);
    ker::mod::kasan::unpoison_range(BEGIN + 8, storage.size() - 16);
    KEXPECT_TRUE(__asan_address_is_poisoned(BEGIN) != 0);
    KEXPECT_TRUE(__asan_address_is_poisoned(BEGIN + 7) != 0);
    for (size_t offset : {size_t{8}, size_t{32767}, size_t{32768}, size_t{65536}, storage.size() - 9})
        KEXPECT_TRUE(__asan_address_is_poisoned(BEGIN + offset) == 0);
    KEXPECT_TRUE(__asan_address_is_poisoned(BEGIN + storage.size() - 8) != 0);
    KEXPECT_TRUE(__asan_address_is_poisoned(BEGIN + storage.size() - 1) != 0);
    ker::mod::kasan::unpoison_range(BEGIN, storage.size());
}
KTEST(Kasan, EnsureBackingPreservesExistingPartialPoison) {
    alignas(8) std::array<uint8_t, 32> storage{};
    std::array<bool, 32> before{};
    ker::mod::kasan::poison_range(storage.data(), storage.size(), ker::mod::kasan::SHADOW_HEAP_FREED);
    ker::mod::kasan::unpoison_range(storage.data(), 17);
    for (size_t offset = 0; offset < storage.size(); ++offset) before[offset] = is_poisoned(storage, offset);
    KEXPECT_FALSE(before[0]);
    KEXPECT_FALSE(before[16]);
    KEXPECT_TRUE(before[17]);
    KEXPECT_TRUE(before[31]);

    for (size_t repeat = 0; repeat < 2; ++repeat) {
        KEXPECT_TRUE(ker::mod::kasan::ensure_shadow_backing(storage.data(), storage.size()));
        for (size_t offset = 0; offset < storage.size(); ++offset) KEXPECT_EQ(is_poisoned(storage, offset), before[offset]);
    }
    ker::mod::kasan::unpoison_range(storage.data(), storage.size());
}

#endif
