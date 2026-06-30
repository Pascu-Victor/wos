#include "netd/daemon.hpp"

auto main(int argc, char** argv) -> int {
    (void)argc;
    (void)argv;
    return netd::run_dhcp_client();
}
