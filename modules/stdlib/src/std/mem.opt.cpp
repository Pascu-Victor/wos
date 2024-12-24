#include "mem.hpp"

__attribute__((used)) void *__dso_handle;

namespace std {
struct AtExitEntry {
    void (*func)(void *);
    void *arg;
    void *dso_handle;
};

std::vector<AtExitEntry> at_exit_list;
std::mutex at_exit_mutex;

void run_atexit_handlers() {
    std::lock_guard<std::mutex> lock(at_exit_mutex);
    while (!at_exit_list.empty()) {
        AtExitEntry entry = at_exit_list.back();
        at_exit_list.pop_back();
        entry.func(entry.arg);
    }
}

}  // namespace std

extern "C" {
void *memcpy(void *dest, const void *src, size_t n) {
    uint8_t *pdest = (uint8_t *)dest;
    const uint8_t *psrc = (const uint8_t *)src;

    for (size_t i = 0; i < n; i++) {
        pdest[i] = psrc[i];
    }

    return dest;
}

void *memset(void *s, int c, size_t n) {
    uint8_t *p = (uint8_t *)s;

    for (size_t i = 0; i < n; i++) {
        p[i] = (uint8_t)c;
    }

    return s;
}

void *memmove(void *dest, const void *src, size_t n) {
    uint8_t *pdest = (uint8_t *)dest;
    const uint8_t *psrc = (const uint8_t *)src;

    if (src > dest) {
        for (size_t i = 0; i < n; i++) {
            pdest[i] = psrc[i];
        }
    } else if (src < dest) {
        for (size_t i = n; i > 0; i--) {
            pdest[i - 1] = psrc[i - 1];
        }
    }

    return dest;
}

int memcmp(const void *s1, const void *s2, size_t n) {
    const uint8_t *p1 = (const uint8_t *)s1;
    const uint8_t *p2 = (const uint8_t *)s2;

    for (size_t i = 0; i < n; i++) {
        if (p1[i] != p2[i]) {
            return p1[i] < p2[i] ? -1 : 1;
        }
    }

    return 0;
}

int __cxa_atexit(void (*func)(void *), void *arg, void *dso_handle) {
    std::lock_guard<std::mutex> lock(std::at_exit_mutex);
    std::at_exit_list.push_back({func, arg, dso_handle});
    return 0;
}

void __cxa_finalize(void *dso_handle) {
    std::lock_guard<std::mutex> lock(std::at_exit_mutex);
    for (auto it = std::at_exit_list.rbegin(); it != std::at_exit_list.rend(); ++it) {
        if (it->dso_handle == dso_handle || dso_handle == nullptr) {
            it->func(it->arg);
            it = std::vector<std::AtExitEntry>::reverse_iterator(std::at_exit_list.erase((++it).base()));
        }
    }
}
}  // extern "C"
