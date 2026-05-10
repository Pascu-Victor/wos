#pragma once

#include <cstdint>
#include <type_traits>

namespace ker::test {

struct KTest {
    const char* suite;
    const char* name;
    void (*fn)();
    bool enabled;
};

// Implemented in ktest.cpp
void run_all();

extern int g_pass;
extern int g_fail;

// Non-template log helpers (defined in ktest.cpp)
void check_eq_log(const char* file, int line, uint64_t a, uint64_t b, const char* as, const char* bs);
void check_ne_log(const char* file, int line, uint64_t a, uint64_t b, const char* as, const char* bs);
void check_true_log(const char* file, int line, const char* cexpr);
void check_null_log(const char* file, int line, const void* ptr, const char* pexpr);

template <typename T>
static inline uint64_t kval(T v) {
    if constexpr (std::is_pointer_v<T>) {
        return reinterpret_cast<uint64_t>(v);
    } else {
        uint64_t r = 0;
        static_assert(sizeof(T) <= sizeof(uint64_t), "type too large for ktest comparison");
        __builtin_memcpy(&r, &v, sizeof(T));
        return r;
    }
}

template <typename A, typename B>
inline bool check_eq(const char* file, int line, A a, B b, const char* as, const char* bs) {
    uint64_t const UA = kval(a);
    uint64_t const UB = kval(b);
    if (UA == UB) {
        g_pass++;
        return true;
    }
    check_eq_log(file, line, UA, UB, as, bs);
    g_fail++;
    return false;
}

template <typename A, typename B>
inline bool check_ne(const char* file, int line, A a, B b, const char* as, const char* bs) {
    uint64_t const UA = kval(a);
    uint64_t const UB = kval(b);
    if (UA != UB) {
        g_pass++;
        return true;
    }
    check_ne_log(file, line, UA, UB, as, bs);
    g_fail++;
    return false;
}

inline bool check_true(const char* file, int line, bool cond, const char* cexpr) {
    if (cond) {
        g_pass++;
        return true;
    }
    check_true_log(file, line, cexpr);
    g_fail++;
    return false;
}

inline auto check_null(const char* file, int line, const void* ptr, const char* pexpr) -> bool {
    if (ptr == nullptr) {
        g_pass++;
        return true;
    }
    check_null_log(file, line, ptr, pexpr);
    g_fail++;
    return false;
}

}  // namespace ker::test

// Linker-section boundaries (provided by linker.ld)
// NOLINTBEGIN(readability-identifier-naming)
extern const ker::test::KTest __start_ktests[];
extern const ker::test::KTest __stop_ktests[];
// NOLINTEND(readability-identifier-naming)

//  Declare + register a test.  The function body follows the macro.
//  S = suite identifier, N = test identifier (no spaces, used as C names).
//  E = enabled flag (true/false).
#define KTEST_IMPL(S, N, E)                                                             \
    static void _kt_##S##_##N();                                                        \
    [[gnu::section(".ktests"), gnu::used]]                                              \
    static constexpr ker::test::KTest _ktrec_##S##_##N = {#S, #N, &_kt_##S##_##N, (E)}; \
    static void _kt_##S##_##N()

#define KTEST(S, N) KTEST_IMPL(S, N, true)
#define KTEST_OFF(S, N) KTEST_IMPL(S, N, false)

// Assertion helpers.
// KEXPECT_*: log failure and continue.
// KREQUIRE_*: log failure and return from the test function.
#define KEXPECT_EQ(a, b) ker::test::check_eq(__FILE__, __LINE__, (a), (b), #a, #b)
#define KEXPECT_NE(a, b) ker::test::check_ne(__FILE__, __LINE__, (a), (b), #a, #b)
#define KEXPECT_TRUE(c) ker::test::check_true(__FILE__, __LINE__, static_cast<bool>(c), #c)
#define KEXPECT_FALSE(c) ker::test::check_true(__FILE__, __LINE__, !static_cast<bool>(c), "!" #c)
#define KEXPECT_NULL(p) ker::test::check_null(__FILE__, __LINE__, static_cast<const void*>(p), #p)

#define KREQUIRE_TRUE(c)                                                                  \
    do {                                                                                  \
        if (!ker::test::check_true(__FILE__, __LINE__, static_cast<bool>(c), #c)) return; \
    } while (0)
#define KREQUIRE_EQ(a, b)                                                       \
    do {                                                                        \
        if (!ker::test::check_eq(__FILE__, __LINE__, (a), (b), #a, #b)) return; \
    } while (0)
#define KREQUIRE_NE(a, b)                                                       \
    do {                                                                        \
        if (!ker::test::check_ne(__FILE__, __LINE__, (a), (b), #a, #b)) return; \
    } while (0)
