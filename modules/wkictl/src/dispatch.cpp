#include "wkictl/dispatch.hpp"

#include <cstring>

#include "wkictl/auth.hpp"
#include "wkictl/chaos.hpp"
#include "wkictl/cli.hpp"
#include "wkictl/perf.hpp"
#include "wkictl/target.hpp"
#include "wkictl/vfs.hpp"
#include "wkictl/wosid.hpp"

namespace {

auto run_wkictl(int argc, char** argv) -> int {
    if (argc < 2) {
        return wkictl::usage();
    }
    if (std::strcmp(argv[1], "wosid") == 0) {
        return wkictl::print_wosid();
    }
    if (std::strcmp(argv[1], "auth") == 0) {
        return wkictl::handle_auth(argc, argv);
    }
    if (std::strcmp(argv[1], "target") == 0) {
        return wkictl::handle_target(argc, argv);
    }
    if (std::strcmp(argv[1], "vfs") == 0) {
        return wkictl::handle_vfs(argc, argv);
    }
    if (std::strcmp(argv[1], "perf") == 0) {
        return wkictl::handle_perf(argc, argv);
    }
    if (std::strcmp(argv[1], "chaos") == 0) {
        return wkictl::handle_chaos(argc, argv);
    }
    if (std::strcmp(argv[1], "chaos-workload") == 0) {
        return wkictl::handle_chaos_workload(argc, argv);
    }
    return wkictl::usage();
}

}  // namespace

namespace wkictl {

auto run(int argc, char** argv) -> int {
    int status = 0;
    if (run_launcher(argc, argv, status)) {
        return status;
    }
    return run_wkictl(argc, argv);
}

}  // namespace wkictl
