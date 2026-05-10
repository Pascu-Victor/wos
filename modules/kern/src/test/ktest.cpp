#include <cstdint>
#include <platform/dbg/dbg.hpp>
#include <test/ktest.hpp>

#ifdef WOS_KCOV
#include <sanitizer/kcov.hpp>

static void kcov_heapify(uint64_t* a, uint64_t n, uint64_t root) {
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

static auto kcov_sort_dedup(uint64_t* pcs, uint64_t count) -> uint64_t {
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
                       static_cast<unsigned long long>((uintptr_t)ptr));
}

void run_all() {
    ker::mod::dbg::log("[KTEST] === WOS Kernel Self-Test Suite ===");
    g_pass = 0;
    g_fail = 0;

#ifdef WOS_KCOV
    ker::sanitizer::kcov::alloc_buffer(ker::sanitizer::kcov::KCOV_MAX_ENTRIES);
    ker::sanitizer::kcov::enable();
#endif

    const KTest* ktest_begin = __start_ktests;  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    const KTest* ktest_end = __stop_ktests;     // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    for (const KTest* t = ktest_begin; t < ktest_end; ++t) {
        if (t->fn == nullptr) {
            continue;
        }  // skip sentinel entries
        if (!t->enabled) {
            ker::mod::dbg::log("[KTEST] SKIP  %s/%s", t->suite, t->name);
            continue;
        }
        int const FAIL_BEFORE = g_fail;
        ker::mod::dbg::log("[KTEST] RUN   %s/%s", t->suite, t->name);
        t->fn();
        if (g_fail == FAIL_BEFORE) {
            ker::mod::dbg::log("[KTEST] PASS  %s/%s", t->suite, t->name);
        }
    }

#ifdef WOS_KCOV
    ker::sanitizer::kcov::disable();
    auto* buf = ker::sanitizer::kcov::current_buffer();
    if (buf != nullptr && buf->count > 0) {
        uint64_t const UNIQUE = kcov_sort_dedup(buf->pcs, buf->count);
        ker::mod::dbg::log("[KCOV_BEGIN] %llu", static_cast<unsigned long long>(UNIQUE));
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
