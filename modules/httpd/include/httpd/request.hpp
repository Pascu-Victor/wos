#pragma once

#include <string>
#include <string_view>

namespace httpd {

auto url_decode(std::string_view encoded, bool plus_as_space = false) -> std::string;
auto is_safe_path(std::string_view path) -> bool;

auto parse_request_method(std::string_view request) -> std::string_view;
auto parse_request_path(std::string_view request) -> std::string_view;
auto parse_request_body(std::string_view request) -> std::string_view;
auto get_form_field(std::string_view body, std::string_view key) -> std::string;

auto read_request_timeout(int fd, std::string& request, int timeout_ms) -> bool;

}  // namespace httpd
