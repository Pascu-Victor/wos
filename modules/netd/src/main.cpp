#include "netd/daemon.hpp"

#include <unistd.h>

#include <cstddef>

namespace {

void netd_boot_trace(const char* message) {
    if (message == nullptr) {
        return;
    }
    size_t len = 0;
    while (message[len] != '\0') {
        len++;
    }
    (void)::write(STDERR_FILENO, message, len);
}

}  // namespace

auto main(int argc, char** argv) -> int {
    (void)argc;
    (void)argv;
    netd_boot_trace("netd-boot: main entered\n");
    return netd::run_dhcp_client();
}
