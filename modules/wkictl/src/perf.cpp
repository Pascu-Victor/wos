#include "wkictl/perf.hpp"

#include <cstring>
#include <print>

#include "wkictl/cli.hpp"

namespace wkictl {

auto handle_perf(int argc, char** argv) -> int {
    if (argc >= 3 && std::strcmp(argv[2], "show") != 0) {
        return usage();
    }
    std::println("perf: WKI placement is observable through journal logs, /proc/*/wki_runner, and the perf WKI event scopes.");
    return 0;
}

}  // namespace wkictl
