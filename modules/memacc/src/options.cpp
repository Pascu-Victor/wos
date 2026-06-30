#include "options.hpp"

#include <cstdlib>

namespace memacc {

auto parse_u64_arg(std::string_view text) -> uint64_t {
    return static_cast<uint64_t>(std::strtoull(std::string(text).c_str(), nullptr, 10));
}

auto parse_u64_arg_strict(std::string_view text, uint64_t& out) -> bool {
    if (text.empty()) {
        return false;
    }
    uint64_t value = 0;
    for (char ch : text) {
        if (ch < '0' || ch > '9') {
            return false;
        }
        auto const DIGIT = static_cast<uint64_t>(ch - '0');
        if (value > (UINT64_MAX - DIGIT) / 10) {
            return false;
        }
        value = (value * 10) + DIGIT;
    }
    out = value;
    return true;
}

auto parse_options(int argc, char** argv, int start) -> Options {
    Options opt;
    for (int i = start; i < argc; ++i) {
        std::string_view arg(argv[i]);
        if (arg == "--full") {
            opt.full = true;
        } else if (arg == "--pid" && i + 1 < argc) {
            opt.pid = parse_u64_arg(argv[++i]);
        } else if (arg == "--name" && i + 1 < argc) {
            opt.name = argv[++i];
        } else if (arg == "--state" && i + 1 < argc) {
            opt.state = argv[++i];
        } else if (arg == "--min-kib" && i + 1 < argc) {
            opt.min_kib = parse_u64_arg(argv[++i]);
        } else if (arg == "--sort" && i + 1 < argc) {
            opt.sort = argv[++i];
        } else if (arg == "--limit" && i + 1 < argc) {
            opt.limit = static_cast<int>(std::strtol(argv[++i], nullptr, 10));
        } else if (arg == "-n" && i + 1 < argc) {
            opt.interval_seconds = static_cast<int>(std::strtol(argv[++i], nullptr, 10));
            if (opt.interval_seconds <= 0) {
                opt.interval_seconds = 1;
            }
        }
    }
    return opt;
}

}  // namespace memacc
