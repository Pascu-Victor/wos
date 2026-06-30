#include <signal.h>  // NOLINT(modernize-deprecated-headers,misc-include-cleaner): WOS signal constants live here.

#include "server.hpp"

auto main() -> int {
    signal(SIGPIPE, SIG_IGN);  // NOLINT(misc-include-cleaner)
    sftpserver::Server server;
    server.run();
    return 0;
}
