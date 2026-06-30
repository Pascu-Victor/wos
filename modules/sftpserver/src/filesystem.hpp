#pragma once

#include <stdint.h>
#include <sys/stat.h>

#include <string>
#include <string_view>

#include "protocol.hpp"

namespace sftpserver {

auto attrs_from_stat(const struct stat& st) -> Attributes;
auto make_longname(std::string_view name, const struct stat& st) -> std::string;
auto has_nul(std::string_view path) -> bool;
auto round_up(uint64_t value, uint64_t alignment) -> uint64_t;
auto normalize_path(std::string_view input) -> std::string;
auto join_path(std::string_view base, std::string_view name) -> std::string;
auto path_for_client(std::string path) -> std::string;
auto stat_entry(std::string_view path, std::string_view name) -> DirEntry;
auto retry_rename_over_symlink(const std::string& old_path, const std::string& new_path, int original_error) -> int;
auto apply_path_attrs(const std::string& path, const Attributes& attrs) -> int;
auto apply_fd_attrs(int fd, const Attributes& attrs) -> int;

}  // namespace sftpserver
