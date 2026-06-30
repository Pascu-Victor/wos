#include "top/io.hpp"

#include <abi-bits/fcntl.h>
#include <bits/ssize_t.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <utility>

namespace top {

ScopedFd::ScopedFd(int fd) : fd(fd) {}

ScopedFd::ScopedFd(ScopedFd&& other) noexcept : fd(std::exchange(other.fd, -1)) {}

auto ScopedFd::operator=(ScopedFd&& other) noexcept -> ScopedFd& {
    if (this != &other) {
        reset();
        fd = std::exchange(other.fd, -1);
    }
    return *this;
}

ScopedFd::~ScopedFd() { reset(); }

auto ScopedFd::get() const -> int { return fd; }

auto ScopedFd::valid() const -> bool { return fd >= 0; }

void ScopedFd::reset(int new_fd) {
    if (fd >= 0) {
        close(fd);
    }
    fd = new_fd;
}

ScopedDir::ScopedDir(DIR* dir) : dir(dir) {}

ScopedDir::~ScopedDir() {
    if (dir != nullptr) {
        closedir(dir);
    }
}

auto ScopedDir::get() const -> DIR* { return dir; }

auto write_all(int fd, std::string_view text) -> bool {
    size_t done = 0;
    while (done < text.size()) {
        ssize_t const N = write(fd, text.data() + done, text.size() - done);
        if (N < 0 && errno == EINTR) {
            continue;
        }
        if (N <= 0) {
            return false;
        }
        done += static_cast<size_t>(N);
    }
    return true;
}

void write_stdout_best_effort(std::string_view text) {
    while (!text.empty()) {
        ssize_t const N = write(STDOUT_FILENO, text.data(), text.size());
        if (N < 0 && errno == EINTR) {
            continue;
        }
        if (N <= 0) {
            return;
        }
        text.remove_prefix(static_cast<size_t>(N));
    }
}

auto read_file(std::string_view path, size_t limit) -> std::optional<std::string> {
    std::string path_copy(path);
    ScopedFd fd(open(path_copy.c_str(), O_RDONLY));
    if (!fd.valid()) {
        return std::nullopt;
    }

    std::string out;
    out.reserve(std::min<size_t>(limit, 4096));
    std::array<char, 1024> buf{};
    while (out.size() < limit) {
        ssize_t const N = read(fd.get(), buf.data(), std::min(buf.size(), limit - out.size()));
        if (N < 0 && errno == EINTR) {
            continue;
        }
        if (N <= 0) {
            break;
        }
        out.append(buf.data(), static_cast<size_t>(N));
    }
    return out;
}

}  // namespace top
