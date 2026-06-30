#include "httpd/request.hpp"

#include <bits/ssize_t.h>
#include <poll.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>

#include "httpd/config.hpp"
#include "httpd/socket_io.hpp"
#include "httpd/time.hpp"

namespace httpd {

auto url_decode(std::string_view encoded, bool plus_as_space) -> std::string {
    std::string result;
    result.reserve(encoded.size());

    for (size_t i = 0; i < encoded.size(); ++i) {
        char const CURRENT = encoded.at(i);
        if (CURRENT == '%' && i + 2 < encoded.size()) {
            std::array<char, 3> hex = {encoded.at(i + 1), encoded.at(i + 2), '\0'};
            char* end = nullptr;
            long const VAL = strtol(hex.data(), &end, 16);
            if (end == hex.data() + 2) {
                result += static_cast<char>(VAL);
                i += 2;
                continue;
            }
        } else if (CURRENT == '+' && plus_as_space) {
            result += ' ';
            continue;
        }
        result += CURRENT;
    }
    return result;
}

auto is_safe_path(std::string_view path) -> bool { return !path.contains(".."); }

auto parse_request_method(std::string_view request) -> std::string_view {
    auto first_space = request.find(' ');
    if (first_space == std::string_view::npos) {
        return "GET";
    }
    return request.substr(0, first_space);
}

auto parse_request_path(std::string_view request) -> std::string_view {
    auto line_end = request.find("\r\n");
    if (line_end == std::string_view::npos) {
        return "/";
    }
    auto first_line = request.substr(0, line_end);

    auto first_space = first_line.find(' ');
    if (first_space == std::string_view::npos) {
        return "/";
    }

    auto second_space = first_line.find(' ', first_space + 1);
    if (second_space == std::string_view::npos) {
        return "/";
    }

    auto target = first_line.substr(first_space + 1, second_space - first_space - 1);
    auto query_start = target.find('?');
    if (query_start != std::string_view::npos) {
        return target.substr(0, query_start);
    }
    return target;
}

auto parse_request_body(std::string_view request) -> std::string_view {
    auto body_start = request.find(HEADER_TERMINATOR);
    if (body_start == std::string_view::npos) {
        return {};
    }
    return request.substr(body_start + HEADER_TERMINATOR.size());
}

auto get_form_field(std::string_view body, std::string_view key) -> std::string {
    std::string search_key{key};
    search_key += '=';

    size_t pos = 0;
    while (pos < body.size()) {
        if (body.substr(pos).starts_with(search_key)) {
            pos += search_key.size();
            auto end = body.find('&', pos);
            if (end == std::string_view::npos) {
                end = body.size();
            }
            return url_decode(body.substr(pos, end - pos), true);
        }
        auto amp = body.find('&', pos);
        if (amp == std::string_view::npos) {
            break;
        }
        pos = amp + 1;
    }
    return {};
}

namespace {

auto ascii_lower(char ch) -> char {
    if (ch >= 'A' && ch <= 'Z') {
        return static_cast<char>(ch - 'A' + 'a');
    }
    return ch;
}

auto ascii_iequals(std::string_view lhs, std::string_view rhs) -> bool {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.size(); ++i) {
        if (ascii_lower(lhs.at(i)) != ascii_lower(rhs.at(i))) {
            return false;
        }
    }
    return true;
}

auto trim_optional_whitespace(std::string_view value) -> std::string_view {
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
        value.remove_prefix(1);
    }
    while (!value.empty() && (value.back() == ' ' || value.back() == '\t')) {
        value.remove_suffix(1);
    }
    return value;
}

auto parse_content_length_value(std::string_view value, size_t& content_length) -> bool {
    value = trim_optional_whitespace(value);
    if (value.empty()) {
        return false;
    }

    size_t parsed = 0;
    for (char const CH : value) {
        if (CH < '0' || CH > '9') {
            return false;
        }
        auto const DIGIT = static_cast<size_t>(CH - '0');
        if (parsed > (MAX_CONTENT_LENGTH - DIGIT) / 10) {
            return false;
        }
        parsed = (parsed * 10) + DIGIT;
    }

    content_length = parsed;
    return true;
}

auto parse_content_length(std::string_view headers, size_t& content_length) -> bool {
    content_length = 0;
    bool found = false;

    size_t line_start = headers.find("\r\n");
    if (line_start == std::string_view::npos) {
        return true;
    }
    line_start += 2;

    while (line_start < headers.size()) {
        size_t line_end = headers.find("\r\n", line_start);
        if (line_end == std::string_view::npos) {
            line_end = headers.size();
        }

        std::string_view const LINE = headers.substr(line_start, line_end - line_start);
        if (LINE.empty()) {
            break;
        }

        size_t const COLON = LINE.find(':');
        if (COLON != std::string_view::npos && ascii_iequals(trim_optional_whitespace(LINE.substr(0, COLON)), "Content-Length")) {
            size_t parsed = 0;
            if (!parse_content_length_value(LINE.substr(COLON + 1), parsed)) {
                errno = EINVAL;
                return false;
            }
            if (found && parsed != content_length) {
                errno = EINVAL;
                return false;
            }
            content_length = parsed;
            found = true;
        }

        line_start = line_end + 2;
    }

    return true;
}

}  // namespace

auto read_request_timeout(int fd, std::string& request, int timeout_ms) -> bool {
    int old_flags = -1;
    if (!set_nonblocking_for_timeout(fd, old_flags)) {
        return false;
    }

    request.clear();
    request.reserve(REQUEST_BUFFER_SIZE);

    std::array<char, REQUEST_BUFFER_SIZE> buffer{};
    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    for (;;) {
        size_t const HEADER_END = request.find(HEADER_TERMINATOR);
        if (HEADER_END != std::string::npos) {
            size_t content_length = 0;
            if (!parse_content_length(request.substr(0, HEADER_END), content_length)) {
                restore_fd_flags(fd, old_flags);
                return false;
            }

            size_t const HEADER_SIZE = HEADER_END + HEADER_TERMINATOR.size();
            if (HEADER_SIZE > MAX_REQUEST_BYTES || content_length > MAX_REQUEST_BYTES - HEADER_SIZE) {
                errno = EMSGSIZE;
                restore_fd_flags(fd, old_flags);
                return false;
            }

            size_t const EXPECTED_SIZE = HEADER_SIZE + content_length;
            if (request.size() >= EXPECTED_SIZE) {
                request.resize(EXPECTED_SIZE);
                restore_fd_flags(fd, old_flags);
                return true;
            }
        }

        if (request.size() >= MAX_REQUEST_BYTES) {
            errno = EMSGSIZE;
            restore_fd_flags(fd, old_flags);
            return false;
        }

        size_t const ROOM = MAX_REQUEST_BYTES - request.size();
        size_t const CHUNK_LEN = std::min(buffer.size(), ROOM);
        if (wait_fd_ready_until(fd, POLLIN, DEADLINE_MS, timeout_ms) <= 0) {
            restore_fd_flags(fd, old_flags);
            return false;
        }

        errno = 0;
        ssize_t const RECEIVED = recv(fd, buffer.data(), CHUNK_LEN, 0);
        if (RECEIVED > 0) {
            request.append(buffer.data(), static_cast<size_t>(RECEIVED));
            continue;
        }
        if (RECEIVED == 0) {
            errno = ECONNRESET;
            restore_fd_flags(fd, old_flags);
            return false;
        }
        if (retryable_socket_result(RECEIVED)) {
            continue;
        }

        restore_fd_flags(fd, old_flags);
        return false;
    }
}

}  // namespace httpd
