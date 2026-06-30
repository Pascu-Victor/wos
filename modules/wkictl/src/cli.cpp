#include "wkictl/cli.hpp"

#include <abi-bits/fcntl.h>
#include <bits/ssize_t.h>
#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <print>

namespace wkictl {

auto usage() -> int {
    std::println(stderr,
                 "usage:\n  locally <command> [args...]\n  remotely <command> [args...]\n  homeward <command> [args...]\n  on <hostname> "
                 "<command> [args...]\n  forward "
                 "[+include_path] [-exclude_path] [--] <command> [args...]\n  wosid\n  wkictl "
                 "target <show|clear|set>\n  wkictl vfs <list|defaults|clear|add|probe>\n  wkictl perf <show>\n  wkictl wosid");
    return 1;
}

auto exec_command(char** argv) -> int {
    execvp(argv[0], argv);
    std::println(stderr, "{}: exec failed: {}", argv[0], std::strerror(errno));
    return 127;
}

auto read_trimmed_file(const char* path, char* out, std::size_t out_size) -> bool {
    if (path == nullptr || out == nullptr || out_size == 0) {
        return false;
    }

    int const FD = open(path, O_RDONLY);
    if (FD < 0) {
        out[0] = '\0';
        return false;
    }

    ssize_t n = read(FD, out, out_size - 1);
    int const SAVED_ERRNO = errno;
    close(FD);
    errno = SAVED_ERRNO;
    if (n <= 0) {
        out[0] = '\0';
        return false;
    }

    while (n > 0 && (out[n - 1] == '\n' || out[n - 1] == '\r')) {
        --n;
    }
    out[n] = '\0';
    return true;
}

}  // namespace wkictl
