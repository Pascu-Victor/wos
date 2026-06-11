#include "asan_crasher.hpp"

#include <pthread.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <print>

namespace {

void print_usage() {
    std::println("usage: testprog asan-crasher <case>");
    std::println("cases:");
    std::println("  heap-buffer-overflow     write just past a heap allocation");
    std::println("  stack-buffer-overflow    write just past a stack allocation");
    std::println("  use-after-free           write through a freed heap pointer");
    std::println("  global-buffer-overflow   write just past a global allocation");
    std::println("  memcpy-write-overflow    memcpy writes past a stack destination");
    std::println("  memcpy-read-overflow     memcpy reads past a stack source");
    std::println("  thread-stack-buffer-overflow");
    std::println("                            stack overflow from a pthread-created thread");
    std::println("  thread-memcpy-write-overflow");
    std::println("                            memcpy write overflow from a pthread-created thread");
    std::println("  thread-memcpy-read-overflow");
    std::println("                            memcpy read overflow from a pthread-created thread");
    std::println("  pthread-key-cleanup       non-crashing check for pthread key destructors");
    std::println("  pthread-stack-query       non-crashing check for pthread stack bounds");
}

auto normalize_case_name(const char* text) -> const char* {
    if (text != nullptr && text[0] == '-' && text[1] == '-') {
        return text + 2;
    }
    return text;
}

#ifdef WOS_UASAN

volatile int g_runtime_zero = 0;
volatile unsigned char g_sink = 0;
std::array<unsigned char, 16> g_global_buffer{};
std::atomic<int> g_key_destructor_calls{0};
std::atomic<int> g_stack_query_result{0};
pthread_key_t g_cleanup_key{};  // NOLINT(misc-include-cleaner): provided by WOS pthread.h via bits/posix/pthread_key_t.h.

[[gnu::noinline]] auto runtime_index(std::size_t value) -> std::size_t { return value + static_cast<std::size_t>(g_runtime_zero); }

[[gnu::noinline]] auto runtime_size(std::size_t value) -> std::size_t { return value + static_cast<std::size_t>(g_runtime_zero); }

[[gnu::noinline]] auto hide_pointer(unsigned char* ptr) -> unsigned char* { return ptr + g_runtime_zero; }

[[gnu::noinline]] void keep_alive(unsigned char value) {
    unsigned char const PREVIOUS = g_sink;
    g_sink = static_cast<unsigned char>(PREVIOUS ^ value);
}

[[gnu::noinline]] void crash_heap_buffer_overflow() {
    auto* buffer = new unsigned char[16];
    buffer[runtime_index(16)] = 0xA1;
    keep_alive(buffer[0]);
    delete[] buffer;
}

[[gnu::noinline]] void crash_stack_buffer_overflow() {
    std::array<unsigned char, 16> buffer{};
    buffer[runtime_index(buffer.size())] = 0xB1;
    keep_alive(buffer[0]);
}

[[gnu::noinline]] void crash_use_after_free() {
    auto* buffer = new unsigned char[16];
    buffer[0] = 0xC1;
    auto* stale = hide_pointer(buffer);
    delete[] buffer;
    // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDelete): intentional ASAN use-after-free trigger.
    stale[runtime_index(0)] = 0xC2;
}

[[gnu::noinline]] void crash_global_buffer_overflow() {
    g_global_buffer[runtime_index(g_global_buffer.size())] = 0xD1;
    keep_alive(g_global_buffer[0]);
}

[[gnu::noinline]] void crash_memcpy_write_overflow() {
    std::array<unsigned char, 8> destination{};
    std::array<unsigned char, 16> source{};
    std::memset(source.data(), 0xE1, source.size());
    std::memcpy(destination.data(), source.data(), runtime_size(12));
    keep_alive(destination[0]);
}

[[gnu::noinline]] void crash_memcpy_read_overflow() {
    std::array<unsigned char, 16> destination{};
    std::array<unsigned char, 8> source{};
    std::memset(source.data(), 0xF1, source.size());
    std::memcpy(destination.data(), source.data(), runtime_size(12));
    keep_alive(destination[0]);
}

void* run_thread_stack_buffer_overflow([[maybe_unused]] void* unused) {
    crash_stack_buffer_overflow();
    return nullptr;
}

void* run_thread_memcpy_write_overflow([[maybe_unused]] void* unused) {
    crash_memcpy_write_overflow();
    return nullptr;
}

void* run_thread_memcpy_read_overflow([[maybe_unused]] void* unused) {
    crash_memcpy_read_overflow();
    return nullptr;
}

void cleanup_key_destructor(void* value) {
    if (value != nullptr) {
        g_key_destructor_calls.fetch_add(1, std::memory_order_relaxed);
    }
}

void* run_pthread_key_cleanup_thread([[maybe_unused]] void* unused) {
    pthread_setspecific(g_cleanup_key, &g_cleanup_key);
    return nullptr;
}

void* run_pthread_stack_query_thread([[maybe_unused]] void* unused) {
    pthread_attr_t attr{};  // NOLINT(misc-include-cleaner): provided by WOS pthread.h via bits/posix/pthread_types.h.
    int result = pthread_getattr_np(pthread_self(), &attr);
    if (result != 0) {
        g_stack_query_result.store(result, std::memory_order_relaxed);
        return nullptr;
    }

    void* stack_addr = nullptr;
    std::size_t stack_size = 0;
    result = pthread_attr_getstack(&attr, &stack_addr, &stack_size);
    if (result != 0) {
        pthread_attr_destroy(&attr);
        g_stack_query_result.store(result, std::memory_order_relaxed);
        return nullptr;
    }

    auto marker = std::array<unsigned char, 1>{};
    auto const BOTTOM = reinterpret_cast<std::uintptr_t>(stack_addr);
    auto const TOP = BOTTOM + stack_size;
    auto const MARKER = reinterpret_cast<std::uintptr_t>(marker.data());
    pthread_attr_destroy(&attr);

    if (stack_addr == nullptr || stack_size == 0 || TOP <= BOTTOM || MARKER < BOTTOM || MARKER >= TOP) {
        g_stack_query_result.store(ERANGE, std::memory_order_relaxed);
        return nullptr;
    }

    g_stack_query_result.store(0, std::memory_order_relaxed);
    return nullptr;
}

auto run_pthread_key_cleanup_check() -> int {
    g_key_destructor_calls.store(0, std::memory_order_relaxed);
    int result = pthread_key_create(&g_cleanup_key, cleanup_key_destructor);
    if (result != 0) {
        std::println(stderr, "asan-crasher: pthread_key_create failed: {}", result);
        return 1;
    }

    pthread_t thread{};  // NOLINT(misc-include-cleaner): provided by WOS pthread.h via bits/posix/pthread_t.h.
    result = pthread_create(&thread, nullptr, run_pthread_key_cleanup_thread, nullptr);
    if (result != 0) {
        std::println(stderr, "asan-crasher: pthread_create failed: {}", result);
        pthread_key_delete(g_cleanup_key);
        return 1;
    }

    void* thread_result = nullptr;
    result = pthread_join(thread, &thread_result);
    if (result != 0) {
        std::println(stderr, "asan-crasher: pthread_join failed: {}", result);
        pthread_key_delete(g_cleanup_key);
        return 1;
    }

    pthread_key_delete(g_cleanup_key);
    int const CALLS = g_key_destructor_calls.load(std::memory_order_relaxed);
    if (CALLS != 1) {
        std::println(stderr, "asan-crasher: expected one pthread key destructor call, got {}", CALLS);
        return 1;
    }
    std::println("asan-crasher: pthread key cleanup ok");
    return 0;
}

auto run_pthread_stack_query_check() -> int {
    g_stack_query_result.store(EINPROGRESS, std::memory_order_relaxed);
    pthread_t thread{};  // NOLINT(misc-include-cleaner): provided by WOS pthread.h via bits/posix/pthread_t.h.
    int result = pthread_create(&thread, nullptr, run_pthread_stack_query_thread, nullptr);
    if (result != 0) {
        std::println(stderr, "asan-crasher: pthread_create failed: {}", result);
        return 1;
    }

    void* thread_result = nullptr;
    result = pthread_join(thread, &thread_result);
    if (result != 0) {
        std::println(stderr, "asan-crasher: pthread_join failed: {}", result);
        return 1;
    }

    int const QUERY_RESULT = g_stack_query_result.load(std::memory_order_relaxed);
    if (QUERY_RESULT != 0) {
        std::println(stderr, "asan-crasher: pthread stack query failed: {}", QUERY_RESULT);
        return 1;
    }

    std::println("asan-crasher: pthread stack query ok");
    return 0;
}

auto run_threaded_crash(void* (*entry)(void*)) -> int {
    pthread_t thread{};  // NOLINT(misc-include-cleaner): provided by WOS pthread.h via bits/posix/pthread_t.h.
    int const CREATE_RESULT = pthread_create(&thread, nullptr, entry, nullptr);
    if (CREATE_RESULT != 0) {
        std::println(stderr, "asan-crasher: pthread_create failed: {}", CREATE_RESULT);
        return 1;
    }
    void* result = nullptr;
    int const JOIN_RESULT = pthread_join(thread, &result);
    if (JOIN_RESULT != 0) {
        std::println(stderr, "asan-crasher: pthread_join failed: {}", JOIN_RESULT);
        return 1;
    }
    std::println(stderr, "asan-crasher: child thread returned without triggering ASAN");
    return 1;
}

auto run_enabled_asan_crasher(const char* case_name) -> int {
    if (std::strcmp(case_name, "heap") == 0 || std::strcmp(case_name, "heap-buffer-overflow") == 0) {
        crash_heap_buffer_overflow();
        return 0;
    }
    if (std::strcmp(case_name, "stack") == 0 || std::strcmp(case_name, "stack-buffer-overflow") == 0) {
        crash_stack_buffer_overflow();
        return 0;
    }
    if (std::strcmp(case_name, "uaf") == 0 || std::strcmp(case_name, "use-after-free") == 0) {
        crash_use_after_free();
        return 0;
    }
    if (std::strcmp(case_name, "global") == 0 || std::strcmp(case_name, "global-buffer-overflow") == 0) {
        crash_global_buffer_overflow();
        return 0;
    }
    if (std::strcmp(case_name, "memcpy-write") == 0 || std::strcmp(case_name, "memcpy-write-overflow") == 0) {
        crash_memcpy_write_overflow();
        return 0;
    }
    if (std::strcmp(case_name, "memcpy-read") == 0 || std::strcmp(case_name, "memcpy-read-overflow") == 0) {
        crash_memcpy_read_overflow();
        return 0;
    }
    if (std::strcmp(case_name, "thread-stack") == 0 || std::strcmp(case_name, "thread-stack-buffer-overflow") == 0) {
        return run_threaded_crash(run_thread_stack_buffer_overflow);
    }
    if (std::strcmp(case_name, "thread-memcpy-write") == 0 || std::strcmp(case_name, "thread-memcpy-write-overflow") == 0) {
        return run_threaded_crash(run_thread_memcpy_write_overflow);
    }
    if (std::strcmp(case_name, "thread-memcpy-read") == 0 || std::strcmp(case_name, "thread-memcpy-read-overflow") == 0) {
        return run_threaded_crash(run_thread_memcpy_read_overflow);
    }
    if (std::strcmp(case_name, "cleanup") == 0 || std::strcmp(case_name, "thread-cleanup") == 0 ||
        std::strcmp(case_name, "pthread-key-cleanup") == 0) {
        return run_pthread_key_cleanup_check();
    }
    if (std::strcmp(case_name, "stack-query") == 0 || std::strcmp(case_name, "pthread-stack-query") == 0) {
        return run_pthread_stack_query_check();
    }

    std::println(stderr, "asan-crasher: unknown case '{}'", case_name);
    print_usage();
    return 1;
}

#endif  // WOS_UASAN

}  // namespace

auto run_asan_crasher(int argc, char** argv) -> int {
    if (argc < 1 || argv == nullptr || argv[0] == nullptr) {
        print_usage();
        return 1;
    }

    const char* case_name = normalize_case_name(argv[0]);
    if (std::strcmp(case_name, "help") == 0 || std::strcmp(case_name, "list") == 0) {
        print_usage();
        return 0;
    }

#ifdef WOS_UASAN
    return run_enabled_asan_crasher(case_name);
#else
    std::println(stderr, "asan-crasher: testprog was built without WOS_UASAN; refusing to run intentional memory corruption");
    return 2;
#endif
}
