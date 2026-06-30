#include "rows.hpp"

#include <cstdlib>
#include <utility>

#include "procfs_io.hpp"

namespace memacc {
namespace {

auto hex_value(char c) -> int {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

auto percent_decode(std::string_view value) -> std::string {
    std::string out;
    out.reserve(value.size());
    for (size_t i = 0; i < value.size(); ++i) {
        if (value[i] == '%' && i + 2 < value.size()) {
            int const HI = hex_value(value[i + 1]);
            int const LO = hex_value(value[i + 2]);
            if (HI >= 0 && LO >= 0) {
                out.push_back(static_cast<char>((HI << 4) | LO));
                i += 2;
                continue;
            }
        }
        out.push_back(value[i]);
    }
    return out;
}

}  // namespace

auto parse_rows(std::string_view text) -> std::vector<Row> {
    std::vector<Row> rows;
    size_t pos = 0;
    while (pos < text.size()) {
        size_t end = text.find('\n', pos);
        if (end == std::string_view::npos) {
            end = text.size();
        }
        std::string_view line = text.substr(pos, end - pos);
        pos = end + 1;
        if (line.empty()) {
            continue;
        }

        Row row;
        size_t token_start = 0;
        size_t token_end = line.find(' ');
        row.record = std::string(line.substr(0, token_end));
        token_start = token_end == std::string_view::npos ? line.size() : token_end + 1;
        while (token_start < line.size()) {
            token_end = line.find(' ', token_start);
            if (token_end == std::string_view::npos) {
                token_end = line.size();
            }
            std::string_view token = line.substr(token_start, token_end - token_start);
            size_t const EQ = token.find('=');
            if (EQ != std::string_view::npos && EQ > 0) {
                row.kv.emplace(std::string(token.substr(0, EQ)), percent_decode(token.substr(EQ + 1)));
            }
            token_start = token_end + 1;
        }
        rows.push_back(std::move(row));
    }
    return rows;
}

auto read_rows(std::string_view file) -> std::vector<Row> {
    auto text = read_file(memacc_path(file));
    if (!text.has_value()) {
        return {};
    }
    return parse_rows(*text);
}

auto first_record(const std::vector<Row>& rows, std::string_view name) -> const Row* {
    for (const auto& row : rows) {
        if (row.record == name) {
            return &row;
        }
    }
    return nullptr;
}

auto get_string(const Row& row, std::string_view key) -> std::string {
    auto it = row.kv.find(std::string(key));
    return it == row.kv.end() ? std::string{} : it->second;
}

auto get_u64(const Row& row, std::string_view key) -> uint64_t {
    std::string const VALUE = get_string(row, key);
    if (VALUE.empty() || VALUE == "-") {
        return 0;
    }
    int base = 10;
    const char* s = VALUE.c_str();
    if (VALUE.size() > 2 && VALUE[0] == '0' && (VALUE[1] == 'x' || VALUE[1] == 'X')) {
        base = 16;
    }
    return static_cast<uint64_t>(std::strtoull(s, nullptr, base));
}

}  // namespace memacc
