#include "perf.hpp"
namespace perf {

void cmd_show_map() {
    if (access(PERF_DATA_FILE.begin(), R_OK) != 0) {
        std::println("perf: no perf.data found (run 'perf record' or 'perf run' first)");
        return;
    }

    auto buffer = read_file(PERF_DATA_FILE);
    if (!buffer.has_value() || buffer->empty()) {
        return;
    }

    std::string_view const VIEW(*buffer);
    std::size_t const HEADER_POS = VIEW.find(SECTION_PROC_MAP);
    if (HEADER_POS == std::string_view::npos) {
        std::println("perf: perf.data has no PROC_MAP section");
        return;
    }

    std::size_t pos = VIEW.find('\n', HEADER_POS);
    if (pos == std::string_view::npos) {
        return;
    }
    ++pos;

    std::println("=== perf show-map [{}] ==============================================", PERF_DATA_FILE);
    std::println("{:>6}  {:<20}  {}", "PID", "COMM", "CMDLINE");
    std::println("{:->6}  {:->20}  {:->40}", "", "", "");

    while (pos < VIEW.size()) {
        std::string_view const LINE = next_line(VIEW, pos);
        if (LINE.starts_with(END_PREFIX)) {
            break;
        }
        if (LINE.empty()) {
            continue;
        }

        std::size_t const PID_POS = LINE.find("pid=");
        std::size_t const COMM_POS = LINE.find(COMM_FIELD_PREFIX);
        std::size_t const CMD_POS = LINE.find(CMD_FIELD_PREFIX);
        if (PID_POS == std::string_view::npos || COMM_POS == std::string_view::npos) {
            continue;
        }

        std::string_view const PID_TEXT = LINE.substr(PID_POS + PID_PREFIX_SIZE, COMM_POS - (PID_POS + PID_PREFIX_SIZE));
        std::string_view comm_text = CMD_POS == std::string_view::npos
                                         ? LINE.substr(COMM_POS + COMM_PREFIX_SIZE)
                                         : LINE.substr(COMM_POS + COMM_PREFIX_SIZE, CMD_POS - (COMM_POS + COMM_PREFIX_SIZE));
        std::string_view cmd_text = CMD_POS == std::string_view::npos ? std::string_view{} : LINE.substr(CMD_POS + CMD_PREFIX_SIZE);

        std::println("{:>6}  {:<20}  {}", parse_u64(PID_TEXT), comm_text, cmd_text);
    }
}

}  // namespace perf
