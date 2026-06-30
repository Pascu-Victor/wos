#pragma once

#include <termios.h>

#include <cstdint>

namespace top {

struct TerminalSize {
    int rows = 24;
    int cols = 80;
};

class TerminalMode {
   public:
    explicit TerminalMode(bool interactive);
    TerminalMode(const TerminalMode&) = delete;
    auto operator=(const TerminalMode&) -> TerminalMode& = delete;
    ~TerminalMode();

   private:
    termios old_term{};
    int old_flags = -1;
    bool active = false;
};

enum class Key : uint8_t {
    NONE,
    QUIT,
    INPUT_CLOSED,
    UP,
    DOWN,
    LEFT,
    RIGHT,
    PAGE_UP,
    PAGE_DOWN,
    HOME,
    END,
};

auto terminal_size() -> TerminalSize;
auto read_key() -> Key;
void apply_key(Key key, int& row_offset, int& col_offset, int row_count);

}  // namespace top
