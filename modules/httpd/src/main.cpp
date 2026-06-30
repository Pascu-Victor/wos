#include "httpd/server.hpp"

// std::format can theoretically throw, but WOS service entry points use the normal process boundary for fatal failures.
// NOLINTNEXTLINE(bugprone-exception-escape)
auto main(int argc, char** argv) -> int {
    (void)argc;
    (void)argv;

    return httpd::run_server();
}
