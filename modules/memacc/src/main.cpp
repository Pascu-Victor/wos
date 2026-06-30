#include <string_view>

#include "commands.hpp"

auto main(int argc, char** argv) -> int {
    std::string_view cmd = argc >= 2 ? std::string_view(argv[1]) : std::string_view("dump");

    if (cmd == "help" || cmd == "--help" || cmd == "-h") {
        memacc::usage();
        return 0;
    }
    if (cmd == "dump") {
        return memacc::run_dump(argc, argv);
    }
    if (cmd == "summary") {
        return memacc::run_summary();
    }
    if (cmd == "procs") {
        return memacc::run_procs(argc, argv);
    }
    if (cmd == "proc") {
        return memacc::run_proc(argc, argv);
    }
    if (cmd == "kernel") {
        return memacc::run_kernel();
    }
    if (cmd == "allocs") {
        return memacc::run_allocs(argc, argv);
    }
    if (cmd == "raw") {
        return memacc::run_raw(argc, argv);
    }
    if (cmd == "watch") {
        return memacc::run_watch_command(argc, argv);
    }
    if (cmd == "track") {
        return memacc::run_track(argc, argv);
    }
    if (cmd == "reclaim") {
        return memacc::run_reclaim(argc, argv);
    }

    memacc::usage();
    return 1;
}
