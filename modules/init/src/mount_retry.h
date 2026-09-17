#pragma once

#include <cerrno>

namespace wos::init {

inline constexpr unsigned MOUNT_MAX_ATTEMPTS = 100;
inline constexpr long MOUNT_RETRY_MS = 50;

// A stale mount lookup has already rolled back its prepared backend. Reissue
// the complete syscall so each attempt obtains fresh authorization and path
// state. Permanent failures and successful publication are never retried.
template <class Operation, class Pause>
auto retry_mount(Operation operation, Pause pause) -> int {
    for (unsigned attempt = 1;; ++attempt) {
        int const RESULT = operation();
        if (RESULT != -EAGAIN || attempt == MOUNT_MAX_ATTEMPTS) return RESULT;
        pause(attempt);
    }
}

}  // namespace wos::init
