#include <__config>
#include <array>
#include <cstdarg>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <util/hcf.hpp>
// NOLINTBEGIN(readability-identifier-naming,misc-use-internal-linkage,misc-use-anonymous-namespace)
extern "C" {
// Kernel-friendly assert handler used by C library headers that call
// __assert_fail. Print to kernel log then halt the CPU.
__attribute__((noreturn)) void __assert_fail(const char* expr, const char* file, unsigned int line, const char* func) {
    using namespace ker::mod::dbg;
    log("Assertion failed: %s, at %s:%d (%s)", (expr != nullptr) ? expr : "(null)", (file != nullptr) ? file : "(unknown)", line,
        (func != nullptr) ? func : "(unknown)");
    hcf();
}

// Minimal __cxa_atexit support: store callbacks and run them when requested.
struct AtExitEntry {
    void (*func)(void*);
    void* arg;
    void* dso;
};

static std::array<AtExitEntry, 64> atExitTable{};
static size_t atExitCount = 0;

auto __cxa_atexit(void (*func)(void*), void* arg, void* dso_handle) -> int {
    if (atExitCount >= atExitTable.size()) {
        return -1;
    }
    // Bounded by the size check above; this storage backs the C ABI hook.
    size_t const ENTRY_INDEX = atExitCount++;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    atExitTable[ENTRY_INDEX] = {.func = func, .arg = arg, .dso = dso_handle};
    return 0;
}

void run_atexit_handlers() {
    // Run in reverse order like atexit
    for (size_t i = atExitCount; i > 0; --i) {
        auto& e = atExitTable[i - 1];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        if (e.func != nullptr) {
            e.func(e.arg);
        }
    }
    atExitCount = 0;
}

}  // extern "C"

// Provide a small verbose abort used by libc++ internals. Define it in both
// the public `std` namespace and libc++'s inline namespace `std::__1` so the
// linker finds the symbol regardless of the toolchain's inline-namespace use.
// NOLINTBEGIN(bugprone-std-namespace-modification,modernize-avoid-c-arrays,modernize-avoid-variadic-functions)
namespace std {
_LIBCPP_WEAK void __libcpp_verbose_abort(char const* format, ...) noexcept {
    ker::mod::dbg::log_string((format != nullptr) ? format : "__libcpp_verbose_abort");
    hcf();
}
}  // namespace std

namespace std::inline __1 {
_LIBCPP_WEAK void __libcpp_verbose_abort(char const* format, ...) noexcept {
    ker::mod::dbg::log_string((format != nullptr) ? format : "__libcpp_verbose_abort");
    hcf();
}

[[noreturn]] _LIBCPP_WEAK void __throw_bad_array_new_length() {
    ker::mod::dbg::log("%s", "bad_array_new_length");
    hcf();
}
}  // namespace std::inline __1
// NOLINTEND(bugprone-std-namespace-modification,modernize-avoid-c-arrays,modernize-avoid-variadic-functions)

// Provide sized/aligned new/delete hooks referenced by libc++.
auto operator new(std::size_t sz, std::align_val_t alignment) -> void* {
    (void)alignment;
    return ker::mod::mm::dyn::kmalloc::malloc(sz);
}

// NOLINTNEXTLINE(readability-inconsistent-declaration-parameter-name)
void operator delete(void* ptr, std::size_t size, std::align_val_t alignment) noexcept {
    (void)size;
    (void)alignment;
    ker::mod::mm::dyn::kmalloc::free(ptr);
}
// NOLINTEND(readability-identifier-naming,misc-use-internal-linkage,misc-use-anonymous-namespace)
