#include "top/terminal.hpp"

#include <abi-bits/fcntl.h>
#include <bits/ssize_t.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <string_view>

#include "top/io.hpp"
#include "top/types.hpp"

namespace top {
namespace {

void soft_clear_visible_screen() {
    TerminalSize const TERM = terminal_size();
    for (int row = 0; row < TERM.rows; ++row) {
        static constexpr std::string_view NEWLINE = "\r\n";
        write_stdout_best_effort(NEWLINE);
    }
}

}  // namespace

TerminalMode::TerminalMode(bool interactive) {
    if (!interactive) {
        return;
    }
    if (tcgetattr(STDIN_FILENO, &old_term) != 0) {
        return;
    }
    termios raw = old_term;
    cfmakeraw(&raw);
    if (tcsetattr(STDIN_FILENO, TCSANOW, &raw) != 0) {
        return;
    }
    old_flags = fcntl(STDIN_FILENO, F_GETFL, 0);
    if (old_flags >= 0) {
        (void)fcntl(STDIN_FILENO, F_SETFL, old_flags | O_NONBLOCK);
    }
    active = true;
    soft_clear_visible_screen();
}

TerminalMode::~TerminalMode() {
    if (!active) {
        return;
    }
    (void)tcsetattr(STDIN_FILENO, TCSANOW, &old_term);
    if (old_flags >= 0) {
        (void)fcntl(STDIN_FILENO, F_SETFL, old_flags);
    }
    static constexpr std::string_view SHOW_CURSOR = "\x1b[?25h\x1b[0m\r\n";
    write_stdout_best_effort(SHOW_CURSOR);
}

auto terminal_size() -> TerminalSize {
    TerminalSize size{};
    winsize ws{};
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0) {
        if (ws.ws_row > 0) {
            size.rows = ws.ws_row;
        }
        if (ws.ws_col > 0) {
            size.cols = ws.ws_col;
        }
    }
    return size;
}

auto read_key() -> Key {
    std::array<char, 16> buf{};
    ssize_t const N = read(STDIN_FILENO, buf.data(), buf.size());
    if (N == 0) {
        return Key::INPUT_CLOSED;
    }
    if (N < 0) {
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            return Key::NONE;
        }
        return Key::INPUT_CLOSED;
    }
    if (N <= 0) {
        return Key::NONE;
    }
    if (buf[0] == 'q' || buf[0] == 'Q') {
        return Key::QUIT;
    }
    if (buf[0] != '\x1b' || N < 3 || buf[1] != '[') {
        return Key::NONE;
    }
    switch (buf[2]) {
        case 'A':
            return Key::UP;
        case 'B':
            return Key::DOWN;
        case 'C':
            return Key::RIGHT;
        case 'D':
            return Key::LEFT;
        case 'H':
            return Key::HOME;
        case 'F':
            return Key::END;
        case '1':
            return (N >= 4 && buf[3] == '~') ? Key::HOME : Key::NONE;
        case '4':
            return (N >= 4 && buf[3] == '~') ? Key::END : Key::NONE;
        case '5':
            return (N >= 4 && buf[3] == '~') ? Key::PAGE_UP : Key::NONE;
        case '6':
            return (N >= 4 && buf[3] == '~') ? Key::PAGE_DOWN : Key::NONE;
        default:
            return Key::NONE;
    }
}

void apply_key(Key key, int& row_offset, int& col_offset, int row_count) {
    TerminalSize const TERM = terminal_size();
    int const PAGE = std::max(1, TERM.rows - HEADER_LINES);
    int const MAX_ROW = std::max(0, row_count - PAGE);
    switch (key) {
        case Key::UP:
            row_offset = std::max(0, row_offset - 1);
            break;
        case Key::DOWN:
            row_offset = std::min(MAX_ROW, row_offset + 1);
            break;
        case Key::LEFT:
            col_offset = std::max(0, col_offset - 4);
            break;
        case Key::RIGHT:
            col_offset += 4;
            break;
        case Key::PAGE_UP:
            row_offset = std::max(0, row_offset - PAGE);
            break;
        case Key::PAGE_DOWN:
            row_offset = std::min(MAX_ROW, row_offset + PAGE);
            break;
        case Key::HOME:
            row_offset = 0;
            col_offset = 0;
            break;
        case Key::END:
            row_offset = MAX_ROW;
            break;
        case Key::NONE:
        case Key::QUIT:
        case Key::INPUT_CLOSED:
            break;
    }
}

}  // namespace top
