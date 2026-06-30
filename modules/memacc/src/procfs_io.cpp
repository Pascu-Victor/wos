#include "procfs_io.hpp"

#include <abi-bits/fcntl.h>
#include <bits/ssize_t.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <utility>

namespace memacc {
namespace {

class ScopedFd {
   public:
    explicit ScopedFd(int fd = -1) : fd(fd) {}
    ScopedFd(const ScopedFd&) = delete;
    auto operator=(const ScopedFd&) -> ScopedFd& = delete;
    ScopedFd(ScopedFd&& other) noexcept : fd(std::exchange(other.fd, -1)) {}
    auto operator=(ScopedFd&& other) noexcept -> ScopedFd& {
        if (this != &other) {
            reset();
            fd = std::exchange(other.fd, -1);
        }
        return *this;
    }
    ~ScopedFd() { reset(); }
    [[nodiscard]] auto get() const -> int { return fd; }
    [[nodiscard]] auto valid() const -> bool { return fd >= 0; }
    void reset(int new_fd = -1) {
        if (fd >= 0) {
            close(fd);
        }
        fd = new_fd;
    }

   private:
    int fd;
};

}  // namespace

auto memacc_path(std::string_view file) -> std::string {
    if (!file.empty() && file.front() == '/') {
        return std::string(file);
    }
    std::string path(MEMACC_ROOT);
    path.push_back('/');
    path.append(file);
    return path;
}

auto read_file(std::string_view path, size_t max_bytes) -> std::optional<std::string> {
    ScopedFd fd(open(std::string(path).c_str(), O_RDONLY));
    if (!fd.valid()) {
        return std::nullopt;
    }

    std::string out;
    out.reserve(std::min(max_bytes, READ_CHUNK_CAPACITY));
    std::array<char, READ_CHUNK_CAPACITY> buf{};
    while (true) {
        size_t const REMAINING = max_bytes - out.size();
        if (REMAINING == 0) {
            char extra = '\0';
            ssize_t const COUNT = read(fd.get(), &extra, 1);
            if (COUNT < 0 && errno == EINTR) {
                continue;
            }
            if (COUNT < 0 || COUNT > 0) {
                return std::nullopt;
            }
            return out;
        }

        ssize_t const COUNT = read(fd.get(), buf.data(), std::min(buf.size(), REMAINING));
        if (COUNT < 0 && errno == EINTR) {
            continue;
        }
        if (COUNT < 0) {
            return std::nullopt;
        }
        if (COUNT == 0) {
            break;
        }
        out.append(buf.data(), static_cast<size_t>(COUNT));
    }
    return out;
}

auto write_file(std::string_view path, std::string_view text) -> bool {
    ScopedFd fd(open(std::string(path).c_str(), O_WRONLY));
    if (!fd.valid()) {
        return false;
    }
    size_t done = 0;
    while (done < text.size()) {
        ssize_t const n = write(fd.get(), text.data() + done, text.size() - done);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            return false;
        }
        done += static_cast<size_t>(n);
    }
    return true;
}

}  // namespace memacc
