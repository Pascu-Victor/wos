#include <cstring>

#include "wkictl/dispatch.hpp"
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

}  // namespace

namespace wkictl {

auto run_launcher(int argc, char** argv, int& status) -> bool {
    const char* name = command_basename(argc > 0 ? argv[0] : "wkictl");
    if (std::strcmp(name, "locally") == 0) {
        status = run_locally(argc, argv);
        return true;
    }
    if (std::strcmp(name, "remotely") == 0) {
        status = run_remotely(argc, argv);
        return true;
    }
    if (std::strcmp(name, "anywhere") == 0) {
        status = run_anywhere(argc, argv);
        return true;
    }
    if (std::strcmp(name, "homeward") == 0) {
        status = run_homeward(argc, argv);
        return true;
    }
    if (std::strcmp(name, "on") == 0) {
        status = run_on(argc, argv);
        return true;
    }
    if (std::strcmp(name, "forward") == 0) {
        status = run_forward(argc, argv);
        return true;
    }
    if (std::strcmp(name, "wosid") == 0) {
        status = print_wosid();
        return true;
    }
    return false;
}

}  // namespace wkictl
