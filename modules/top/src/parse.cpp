#include "top/parse.hpp"

#include <cstdlib>
#include <string>

namespace top {

auto parse_u64(std::string_view text, uint64_t& out) -> bool {
    if (text.empty()) {
        return false;
    }
    uint64_t value = 0;
    for (char ch : text) {
        if (ch < '0' || ch > '9') {
            return false;
        }
        value = (value * 10ULL) + static_cast<uint64_t>(ch - '0');
    }
    out = value;
    return true;
}

auto parse_i64(std::string_view text, int64_t& out) -> bool {
    if (text.empty()) {
        return false;
    }
    bool neg = false;
    if (text.front() == '-') {
        neg = true;
        text.remove_prefix(1);
    }
    uint64_t value = 0;
    if (!parse_u64(text, value)) {
        return false;
    }
    out = neg ? -static_cast<int64_t>(value) : static_cast<int64_t>(value);
    return true;
}

auto split_ws(std::string_view text) -> std::vector<std::string_view> {
    std::vector<std::string_view> tokens;
    size_t pos = 0;
    while (pos < text.size()) {
        while (pos < text.size() && (text[pos] == ' ' || text[pos] == '\t' || text[pos] == '\n' || text[pos] == '\r')) {
            pos++;
        }
        size_t const START = pos;
        while (pos < text.size() && text[pos] != ' ' && text[pos] != '\t' && text[pos] != '\n' && text[pos] != '\r') {
            pos++;
        }
        if (pos > START) {
            tokens.emplace_back(text.data() + START, pos - START);
        }
    }
    return tokens;
}

auto parse_double(std::string_view text) -> double {
    std::string copy(text);
    char* end = nullptr;
    double const value = std::strtod(copy.c_str(), &end);
    return (end != nullptr && end != copy.c_str()) ? value : 0.0;
}

}  // namespace top
