#include <array>
#include <cstdint>
#include <platform/dbg/dbg.hpp>
#include <test/ktest.hpp>

#ifdef WOS_KCOV
#include <sanitizer/kcov.hpp>

extern "C" char __kernel_text_start[];  // NOLINT(readability-identifier-naming)
extern "C" char __kernel_text_end[];    // NOLINT(readability-identifier-naming)

__attribute__((no_sanitize("address", "undefined", "coverage"))) static auto kcov_text_fnv64() -> uint64_t {
    constexpr uint64_t FNV_OFFSET_BASIS = 1469598103934665603ULL;
    constexpr uint64_t FNV_PRIME = 1099511628211ULL;
    auto const* cur = reinterpret_cast<const volatile uint8_t*>(__kernel_text_start);
    auto const* const END = reinterpret_cast<const volatile uint8_t*>(__kernel_text_end);
    uint64_t hash = FNV_OFFSET_BASIS;
    while (cur < END) {
        hash ^= static_cast<uint64_t>(*cur);
        hash *= FNV_PRIME;
        ++cur;
    }
    return hash;
}

#endif

namespace ker::test {

int g_pass = 0;
int g_fail = 0;

void check_eq_log(const char* file, int line, uint64_t a, uint64_t b, const char* as, const char* bs) {
    ker::mod::dbg::log("[KTEST] FAIL %s:%d  expected %s == %s  (0x%llx != 0x%llx)", file, line, as, bs, static_cast<unsigned long long>(a),
                       static_cast<unsigned long long>(b));
}

void check_ne_log(const char* file, int line, uint64_t a, uint64_t /*b*/, const char* as, const char* bs) {
    ker::mod::dbg::log("[KTEST] FAIL %s:%d  expected %s != %s  (both 0x%llx)", file, line, as, bs, static_cast<unsigned long long>(a));
}

void check_true_log(const char* file, int line, const char* cexpr) {
    ker::mod::dbg::log("[KTEST] FAIL %s:%d  expected true: %s", file, line, cexpr);
}

void check_null_log(const char* file, int line, const void* ptr, const char* pexpr) {
    ker::mod::dbg::log("[KTEST] FAIL %s:%d  expected null: %s (got 0x%llx)", file, line, pexpr,
                       static_cast<unsigned long long>(reinterpret_cast<uintptr_t>(ptr)));
}

void run_all() {
    ker::mod::dbg::log("[KTEST] === WOS Kernel Self-Test Suite ===");
    g_pass = 0;
    g_fail = 0;

#ifdef WOS_KCOV
    auto const TEXT_START = reinterpret_cast<uintptr_t>(__kernel_text_start);
    auto const TEXT_END = reinterpret_cast<uintptr_t>(__kernel_text_end);
    if (ker::sanitizer::kcov::alloc_text_bitmap(TEXT_START, TEXT_END) != 0) {
        ker::mod::dbg::panic_handler("selftest: unable to allocate KCOV text bitmap");
    }
    ker::sanitizer::kcov::enable();
#endif

    const KTest* ktest_begin = __start_ktests;  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    const KTest* ktest_end = __stop_ktests;     // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    for (const KTest* t = ktest_begin; t < ktest_end; ++t) {
        if (t->fn == nullptr || t->suite == nullptr || t->name == nullptr) {
            continue;
        }  // skip sentinel entries
        if (!t->enabled) {
            ker::mod::dbg::log("[KTEST] SKIP  %s/%s", t->suite, t->name);
            continue;
        }
        int const PASS_BEFORE = g_pass;
        int const FAIL_BEFORE = g_fail;
        ker::mod::dbg::log("[KTEST] RUN   %s/%s", t->suite, t->name);
        t->fn();
        if (g_pass == PASS_BEFORE && g_fail == FAIL_BEFORE) {
            ker::mod::dbg::log("[KTEST] FAIL  %s/%s: no assertions executed", t->suite, t->name);
            g_fail++;
            continue;
        }
        if (g_fail == FAIL_BEFORE) {
            ker::mod::dbg::log("[KTEST] PASS  %s/%s", t->suite, t->name);
        }
    }

#ifdef WOS_KCOV
    ker::sanitizer::kcov::disable();
    uint64_t const TEXT_FNV64 = kcov_text_fnv64();
    ker::mod::dbg::log("[KCOV_ELF] text_start=0x%llx text_end=0x%llx text_fnv64=0x%llx", static_cast<unsigned long long>(TEXT_START),
                       static_cast<unsigned long long>(TEXT_END), static_cast<unsigned long long>(TEXT_FNV64));
    auto* buf = ker::sanitizer::kcov::current_buffer();
    if (buf != nullptr) {
        ker::mod::dbg::log("[KCOV_BEGIN] %llu", static_cast<unsigned long long>(buf->count));
        if (buf->truncated) {
            ker::mod::dbg::log("[KCOV_TRUNCATED] count=%llu capacity=%llu", static_cast<unsigned long long>(buf->count),
                               static_cast<unsigned long long>(buf->capacity));
        }
        std::array<uint64_t, 8> pcs{};
        uint64_t pending = 0;
        uint64_t const WORDS = (buf->text_bytes / 64) + (buf->text_bytes % 64 != 0);
        for (uint64_t word = 0; word < WORDS; ++word) {
            for (uint64_t bits = buf->bitmap[word]; bits != 0; bits &= bits - 1) {
                auto const BIT = static_cast<uint64_t>(__builtin_ctzll(bits));
                pcs[pending++] = TEXT_START + (word * 64) + BIT;
                if (pending == 8) {
                    ker::mod::dbg::log("[KCOV] 0x%llx 0x%llx 0x%llx 0x%llx 0x%llx 0x%llx 0x%llx 0x%llx",
                                       static_cast<unsigned long long>(pcs[0]), static_cast<unsigned long long>(pcs[1]),
                                       static_cast<unsigned long long>(pcs[2]), static_cast<unsigned long long>(pcs[3]),
                                       static_cast<unsigned long long>(pcs[4]), static_cast<unsigned long long>(pcs[5]),
                                       static_cast<unsigned long long>(pcs[6]), static_cast<unsigned long long>(pcs[7]));
                    pending = 0;
                }
            }
        }
        for (uint64_t i = 0; i < pending; ++i) {
            ker::mod::dbg::log("[KCOV] 0x%llx", static_cast<unsigned long long>(pcs[i]));
        }
        ker::mod::dbg::log("[KCOV_END]");
    }
#endif

    ker::mod::dbg::log("[KTEST] === %d passed, %d failed ===", g_pass, g_fail);
    if (g_fail > 0) {
        ker::mod::dbg::panic_handler("selftest: failures detected");
    }
}

}  // namespace ker::test
