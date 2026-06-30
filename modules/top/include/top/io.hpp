#pragma once

#include <dirent.h>

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

namespace top {

class ScopedFd {
   public:
    explicit ScopedFd(int fd = -1);
    ScopedFd(const ScopedFd&) = delete;
    auto operator=(const ScopedFd&) -> ScopedFd& = delete;
    ScopedFd(ScopedFd&& other) noexcept;
    auto operator=(ScopedFd&& other) noexcept -> ScopedFd&;
    ~ScopedFd();

    [[nodiscard]] auto get() const -> int;
    [[nodiscard]] auto valid() const -> bool;
    void reset(int new_fd = -1);

   private:
    int fd;
};

class ScopedDir {
   public:
    explicit ScopedDir(DIR* dir = nullptr);
    ScopedDir(const ScopedDir&) = delete;
    auto operator=(const ScopedDir&) -> ScopedDir& = delete;
    ~ScopedDir();

    [[nodiscard]] auto get() const -> DIR*;

   private:
    DIR* dir;
};

auto write_all(int fd, std::string_view text) -> bool;
void write_stdout_best_effort(std::string_view text);
auto read_file(std::string_view path, size_t limit = 65536) -> std::optional<std::string>;

}  // namespace top
