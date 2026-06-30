#include "top/app.hpp"

#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "top/io.hpp"
#include "top/parse.hpp"
#include "top/procfs_reader.hpp"
#include "top/render.hpp"
#include "top/terminal.hpp"
#include "top/types.hpp"

namespace top {
namespace {

auto parse_interval_ms(int argc, char** argv) -> int {
    if (argc < 2) {
        return DEFAULT_INTERVAL_MS;
    }
    uint64_t parsed = 0;
    if (!parse_u64(argv[1], parsed) || parsed == 0 || parsed > 60000) {
        return DEFAULT_INTERVAL_MS;
    }
    return static_cast<int>(parsed);
}

}  // namespace

auto run(int argc, char** argv) -> int {
    int const INTERVAL_MS = parse_interval_ms(argc, argv);
    bool const INTERACTIVE = isatty(STDIN_FILENO) && isatty(STDOUT_FILENO);
    [[maybe_unused]] TerminalMode const terminal_mode(INTERACTIVE);
    auto users = read_passwd();

    std::optional<Snapshot> previous;
    int row_offset = 0;
    int col_offset = 0;
    bool quit = false;

    while (!quit) {
        Snapshot snap = make_snapshot(previous.has_value() ? &*previous : nullptr, users);
        TerminalSize const TERM = terminal_size();
        int const PAGE = std::max(1, TERM.rows - HEADER_LINES);
        row_offset = std::clamp(row_offset, 0, std::max(0, static_cast<int>(snap.rows.size()) - PAGE));

        std::string screen = render(snap, previous.has_value() ? &*previous : nullptr, row_offset, col_offset, INTERACTIVE);
        if (!write_all(STDOUT_FILENO, screen)) {
            return 1;
        }

        previous = std::move(snap);
        if (!INTERACTIVE) {
            break;
        }

        int remaining_ms = INTERVAL_MS;
        while (remaining_ms > 0 && !quit) {
            int const WAIT_MS = std::min(remaining_ms, 100);
            pollfd pfd{
                .fd = STDIN_FILENO,
                .events = POLLIN,
                .revents = 0,
            };
            int const READY = poll(&pfd, 1, WAIT_MS);
            if (READY < 0) {
                if (errno == EINTR) {
                    continue;
                }
                quit = true;
                break;
            }
            if (READY > 0 && (pfd.revents & (POLLHUP | POLLERR | POLLNVAL)) != 0) {
                quit = true;
                break;
            }
            if (READY > 0 && (pfd.revents & POLLIN) != 0) {
                Key const KEY = read_key();
                if (KEY == Key::QUIT || KEY == Key::INPUT_CLOSED) {
                    quit = true;
                    break;
                }
                apply_key(KEY, row_offset, col_offset, static_cast<int>(previous->rows.size()));
                if (KEY != Key::NONE) {
                    break;
                }
            }
            remaining_ms -= WAIT_MS;
        }
    }

    return 0;
}

}  // namespace top
