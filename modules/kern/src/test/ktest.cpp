#include <cstdint>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
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

__attribute__((no_sanitize("address", "undefined", "coverage"))) static void kcov_heapify(uint64_t* a, uint64_t n, uint64_t root) {
    while (true) {
        uint64_t largest = root;
        uint64_t const L = (2 * root) + 1;
        uint64_t const R = (2 * root) + 2;
        if (L < n && a[L] > a[largest]) {
            largest = L;
        }
        if (R < n && a[R] > a[largest]) {
            largest = R;
        }
        if (largest == root) {
            break;
        }
        uint64_t const TMP = a[root];
        a[root] = a[largest];
        a[largest] = TMP;
        root = largest;
    }
}

__attribute__((no_sanitize("address", "undefined", "coverage"))) static auto kcov_sort_dedup(uint64_t* pcs, uint64_t count) -> uint64_t {
    if (count == 0) {
        return 0;
    }
    for (uint64_t i = count / 2; i-- > 0;) {
        kcov_heapify(pcs, count, i);
    }
    for (uint64_t i = count - 1; i > 0; --i) {
        uint64_t const TMP = pcs[0];
        pcs[0] = pcs[i];
        pcs[i] = TMP;
        kcov_heapify(pcs, i, 0);
    }
    uint64_t out = 1;
    for (uint64_t i = 1; i < count; ++i) {
        if (pcs[i] != pcs[out - 1]) {
            pcs[out++] = pcs[i];
        }
    }
    return out;
}

__attribute__((no_sanitize("address", "undefined", "coverage"))) static auto kcov_round_up_to_page(uint64_t bytes) -> uint64_t {
    constexpr uint64_t PAGE_SIZE = ker::mod::mm::paging::PAGE_SIZE;
    if (bytes == 0 || bytes > UINT64_MAX - (PAGE_SIZE - 1)) {
        return 0;
    }
    return (bytes + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);
}

__attribute__((no_sanitize("address", "undefined", "coverage"))) static auto kcov_bitmap_bytes(uint64_t bit_count) -> uint64_t {
    constexpr uint64_t BITS_PER_WORD = 64;
    if (bit_count > UINT64_MAX - (BITS_PER_WORD - 1)) {
        return 0;
    }
    uint64_t const WORDS = (bit_count + BITS_PER_WORD - 1) / BITS_PER_WORD;
    if (WORDS > UINT64_MAX / sizeof(uint64_t)) {
        return 0;
    }
    return kcov_round_up_to_page(WORDS * sizeof(uint64_t));
}

__attribute__((no_sanitize("address", "undefined", "coverage"))) static auto kcov_mark_seen(uint64_t* seen, uint64_t bit) -> bool {
    constexpr uint64_t BITS_PER_WORD = 64;
    uint64_t const WORD = bit / BITS_PER_WORD;
    uint64_t const MASK = 1ULL << (bit % BITS_PER_WORD);
    if ((seen[WORD] & MASK) != 0) {
        return false;
    }
    seen[WORD] |= MASK;
    return true;
}

__attribute__((no_sanitize("address", "undefined", "coverage"))) static auto kcov_output_contains(uint64_t const* pcs, uint64_t count,
                                                                                                  uint64_t pc) -> bool {
    for (uint64_t i = 0; i < count; ++i) {
        if (pcs[i] == pc) {
            return true;
        }
    }
    return false;
}

__attribute__((no_sanitize("address", "undefined", "coverage"))) static auto kcov_bitmap_dedup(uint64_t* pcs, uint64_t count,
                                                                                               uintptr_t text_start, uintptr_t text_end)
    -> uint64_t {
    if (count == 0) {
        return 0;
    }
    if (text_end <= text_start) {
        return kcov_sort_dedup(pcs, count);
    }

    uint64_t const TEXT_BYTES = static_cast<uint64_t>(text_end - text_start);
    uint64_t const BITMAP_BYTES = kcov_bitmap_bytes(TEXT_BYTES);
    if (BITMAP_BYTES == 0) {
        return kcov_sort_dedup(pcs, count);
    }

    auto* seen = static_cast<uint64_t*>(ker::mod::mm::phys::page_alloc_may_fail(BITMAP_BYTES, "kcov_seen"));
    if (seen == nullptr) {
        return kcov_sort_dedup(pcs, count);
    }

    uint64_t out = 0;
    for (uint64_t i = 0; i < count; ++i) {
        uint64_t const PC = pcs[i];
        if (PC >= text_start && PC < text_end) {
            if (!kcov_mark_seen(seen, PC - text_start)) {
                continue;
            }
        } else if (kcov_output_contains(pcs, out, PC)) {
            continue;
        }
        pcs[out++] = PC;
    }

    ker::mod::mm::phys::page_free(seen);
    return out;
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
    ker::sanitizer::kcov::alloc_buffer(0);
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
    auto const TEXT_START = reinterpret_cast<uintptr_t>(__kernel_text_start);
    auto const TEXT_END = reinterpret_cast<uintptr_t>(__kernel_text_end);
    uint64_t const TEXT_FNV64 = kcov_text_fnv64();
    ker::mod::dbg::log("[KCOV_ELF] text_start=0x%llx text_end=0x%llx text_fnv64=0x%llx", static_cast<unsigned long long>(TEXT_START),
                       static_cast<unsigned long long>(TEXT_END), static_cast<unsigned long long>(TEXT_FNV64));
    auto* buf = ker::sanitizer::kcov::current_buffer();
    if (buf != nullptr && buf->count > 0) {
        uint64_t const RECORDED = buf->count < buf->capacity ? buf->count : buf->capacity;
        bool const TRUNCATED = buf->truncated;
        uint64_t const UNIQUE = kcov_bitmap_dedup(buf->pcs, RECORDED, TEXT_START, TEXT_END);
        ker::mod::dbg::log("[KCOV_BEGIN] %llu", static_cast<unsigned long long>(UNIQUE));
        if (TRUNCATED) {
            ker::mod::dbg::log("[KCOV_TRUNCATED] count=%llu capacity=%llu", static_cast<unsigned long long>(buf->count),
                               static_cast<unsigned long long>(buf->capacity));
        }
        uint64_t i = 0;
        for (; i + 8 <= UNIQUE; i += 8) {
            ker::mod::dbg::log("[KCOV] 0x%llx 0x%llx 0x%llx 0x%llx 0x%llx 0x%llx 0x%llx 0x%llx",
                               static_cast<unsigned long long>(buf->pcs[i]), static_cast<unsigned long long>(buf->pcs[i + 1]),
                               static_cast<unsigned long long>(buf->pcs[i + 2]), static_cast<unsigned long long>(buf->pcs[i + 3]),
                               static_cast<unsigned long long>(buf->pcs[i + 4]), static_cast<unsigned long long>(buf->pcs[i + 5]),
                               static_cast<unsigned long long>(buf->pcs[i + 6]), static_cast<unsigned long long>(buf->pcs[i + 7]));
        }
        for (; i < UNIQUE; ++i) {
            ker::mod::dbg::log("[KCOV] 0x%llx", static_cast<unsigned long long>(buf->pcs[i]));
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
