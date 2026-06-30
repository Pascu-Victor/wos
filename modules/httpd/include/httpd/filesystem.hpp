#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace httpd {

auto get_mime_type(std::string_view path) -> const char*;
auto format_size(size_t size) -> std::string;
auto generate_directory_listing(const std::string& fs_path, std::string_view url_path) -> std::string;
auto serve_file(int client_fd, const std::string& fs_path, std::string_view url_path, uint64_t tid, uint64_t pid) -> bool;

}  // namespace httpd
