#include "wkictl/cli.hpp"
#include "wkictl/dispatch.hpp"

auto main(int argc, char** argv) -> int {
    int status = 0;
    return wkictl::run_launcher(argc, argv, status) ? status : wkictl::usage();
}
