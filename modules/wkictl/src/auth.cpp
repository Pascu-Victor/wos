#include "wkictl/auth.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <print>

namespace wkictl {

auto handle_auth(int argc, char** argv) -> int {
    if (argc != 3 || std::strcmp(argv[2], "status") != 0) {
        std::println(stderr, "usage: wkictl auth status");
        return 1;
    }
    int const FD = open("/proc/wki/auth", O_RDONLY);
    if (FD < 0) {
        std::println(stderr, "wkictl auth: {}", std::strerror(errno));
        return 1;
    }
    std::array<char, 1024> buffer{};
    for (;;) {
        ssize_t const COUNT = read(FD, buffer.data(), buffer.size());
        if (COUNT == 0) {
            break;
        }
        if (COUNT < 0 || write(STDOUT_FILENO, buffer.data(), static_cast<size_t>(COUNT)) != COUNT) {
            int const SAVED_ERRNO = errno;
            close(FD);
            std::println(stderr, "wkictl auth: {}", std::strerror(SAVED_ERRNO));
            return 1;
        }
    }
    close(FD);
    return 0;
}

}  // namespace wkictl
