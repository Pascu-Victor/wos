#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace httpd {

inline constexpr uint16_t HTTP_PORT = 80;
inline constexpr const char* SERVE_ROOT = "/";

inline constexpr size_t REQUEST_BUFFER_SIZE = 4096;
inline constexpr size_t MAX_REQUEST_BYTES = static_cast<size_t>(64) * 1024;
inline constexpr size_t MAX_CONTENT_LENGTH = static_cast<size_t>(32) * 1024;
inline constexpr size_t FILE_STREAM_BUFFER_SIZE = static_cast<size_t>(4096) * 1024;
inline constexpr std::string_view HEADER_TERMINATOR = "\r\n\r\n";

inline constexpr int MAX_PENDING_CONNECTIONS = 128;
inline constexpr int CLIENT_IO_TIMEOUT_MS = 30000;
inline constexpr int CLIENT_DRAIN_TIMEOUT_MS = 1000;
inline constexpr int CHILD_WAIT_POLL_US = 1000;
inline constexpr int MOUNT_CHILD_TIMEOUT_MS = 30000;
inline constexpr int MOUNT_CHILD_REAP_RETRIES = 1000;
inline constexpr int MSEC_PER_SEC = 1000;
inline constexpr int NSEC_PER_MSEC = 1000000;
inline constexpr int64_t NSEC_PER_SEC = 1000LL * NSEC_PER_MSEC;
inline constexpr int USEC_PER_MSEC = 1000;

}  // namespace httpd
