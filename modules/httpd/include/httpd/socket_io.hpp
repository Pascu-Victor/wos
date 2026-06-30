#pragma once

#include <bits/ssize_t.h>

#include <cstddef>
#include <cstdint>

namespace httpd {

auto wait_fd_ready_until(int fd, short events, int64_t deadline_ms, int fallback_timeout_ms) -> int;

auto set_nonblocking_for_timeout(int fd, int& old_flags) -> bool;
void restore_fd_flags(int fd, int old_flags);
void set_fd_cloexec_best_effort(int fd);

auto retryable_socket_result(ssize_t result) -> bool;

auto send_all_timeout(int fd, const void* data, size_t len, int timeout_ms) -> ssize_t;
auto send_all(int fd, const void* data, size_t len) -> ssize_t;
void drain_client_input_timeout(int fd, int timeout_ms);

}  // namespace httpd
