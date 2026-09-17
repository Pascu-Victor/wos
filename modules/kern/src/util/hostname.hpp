#pragma once

#include <array>
#include <cstddef>

namespace ker::util::hostname {

constexpr size_t HOSTNAME_MAX = 64;  // Matches Linux HOST_NAME_MAX (including NUL)

// Read /etc/hostname and cache it. Call once during boot after VFS is ready.
void init();

using Name = std::array<char, HOSTNAME_MAX>;

// Copy one complete, terminated name. No pointer into mutable storage escapes.
auto snapshot() -> Name;

// Programmatically set the hostname (e.g. sethostname syscall).
// Returns 0 on success, -EINVAL on bad input.
int set(const char* name, size_t len);

}  // namespace ker::util::hostname
