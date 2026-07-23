#include "wkictl/dispatch.hpp"

#include <cstring>

#include "wkictl/cli.hpp"
#include "wkictl/perf.hpp"
#include "wkictl/target.hpp"
#include "wkictl/vfs.hpp"
#include "wkictl/wosid.hpp"

namespace {

auto command_basename(const char* path) -> const char* {
    if (path == nullptr) {
        return "";
    }
    const char* slash = std::strrchr(path, '/');
    return slash == nullptr ? path : slash + 1;
}

auto run_wkictl(int argc, char** argv) -> int {
    if (argc < 2) {
        return wkictl::usage();
    }
    if (std::strcmp(argv[1], "wosid") == 0) {
        return wkictl::print_wosid();
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
    return wkictl::usage();
}

}  // namespace

namespace wkictl {

auto run(int argc, char** argv) -> int {
    const char* name = command_basename(argc > 0 ? argv[0] : "wkictl");
    if (std::strcmp(name, "locally") == 0) {
        return run_locally(argc, argv);
    }
    if (std::strcmp(name, "remotely") == 0) {
        return run_remotely(argc, argv);
    }
    if (std::strcmp(name, "anywhere") == 0) {
        return run_anywhere(argc, argv);
    }
    if (std::strcmp(name, "homeward") == 0) {
        return run_homeward(argc, argv);
    }
    if (std::strcmp(name, "on") == 0) {
        return run_on(argc, argv);
    }
    if (std::strcmp(name, "forward") == 0) {
        return run_forward(argc, argv);
    }
    if (std::strcmp(name, "wosid") == 0) {
        return print_wosid();
    }
    return run_wkictl(argc, argv);
}

}  // namespace wkictl
