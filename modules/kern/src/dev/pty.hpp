#pragma once

#include <cstddef>
#include <cstdint>

#include "device.hpp"

namespace ker::dev::pty {

// Linux-compatible winsize struct
struct Winsize {
    uint16_t ws_row;
    uint16_t ws_col;
    uint16_t ws_xpixel;
    uint16_t ws_ypixel;
};

// Kernel-side termios struct (matches mlibc ABI layout exactly)
static constexpr size_t KERNEL_NCCS = 32;

struct KTermios {
    uint32_t c_iflag;
    uint32_t c_oflag;
    uint32_t c_cflag;
    uint32_t c_lflag;
    uint8_t c_line;
    uint8_t c_cc[KERNEL_NCCS];
    uint32_t ibaud;
    uint32_t obaud;
};

// c_cc indices
static constexpr int CC_VINTR = 0;
static constexpr int CC_VQUIT = 1;
static constexpr int CC_VERASE = 2;
static constexpr int CC_VKILL = 3;
static constexpr int CC_VEOF = 4;
static constexpr int CC_VTIME = 5;
static constexpr int CC_VMIN = 6;
static constexpr int CC_VSTART = 8;
static constexpr int CC_VSTOP = 9;
static constexpr int CC_VSUSP = 10;
static constexpr int CC_VEOL = 11;

// c_iflag bits
static constexpr uint32_t TIOS_IGNBRK = 0000001;
static constexpr uint32_t TIOS_BRKINT = 0000002;
static constexpr uint32_t TIOS_IGNPAR = 0000004;
static constexpr uint32_t TIOS_PARMRK = 0000010;
static constexpr uint32_t TIOS_INPCK = 0000020;
static constexpr uint32_t TIOS_ISTRIP = 0000040;
static constexpr uint32_t TIOS_INLCR = 0000100;
static constexpr uint32_t TIOS_IGNCR = 0000200;
static constexpr uint32_t TIOS_ICRNL = 0000400;
static constexpr uint32_t TIOS_IXON = 0002000;
static constexpr uint32_t TIOS_IXOFF = 0010000;

// c_oflag bits
static constexpr uint32_t TIOS_OPOST = 0000001;
static constexpr uint32_t TIOS_ONLCR = 0000004;
static constexpr uint32_t TIOS_OCRNL = 0000010;

// c_cflag bits
static constexpr uint32_t TIOS_CS8 = 0000060;
static constexpr uint32_t TIOS_CREAD = 0000200;
static constexpr uint32_t TIOS_CLOCAL = 0004000;

// c_lflag bits
static constexpr uint32_t TIOS_ISIG = 0000001;
static constexpr uint32_t TIOS_ICANON = 0000002;
static constexpr uint32_t TIOS_ECHO = 0000010;
static constexpr uint32_t TIOS_ECHOE = 0000020;
static constexpr uint32_t TIOS_ECHOK = 0000040;
static constexpr uint32_t TIOS_ECHONL = 0000100;
static constexpr uint32_t TIOS_NOFLSH = 0000200;
static constexpr uint32_t TIOS_TOSTOP = 0000400;
static constexpr uint32_t TIOS_IEXTEN = 0100000;

// Returns a default termios (cooked mode, echo on, signals on)
KTermios default_termios();

// Ring buffer for PTY data flow
static constexpr size_t PTY_BUF_SIZE = 4096;

struct PtyRingBuf {
    uint8_t data[PTY_BUF_SIZE]{};
    size_t head = 0;   // write position
    size_t tail = 0;   // read position
    size_t count = 0;  // bytes in buffer

    auto write(const void* src, size_t len) -> size_t;
    auto read(void* dst, size_t len) -> size_t;
    auto available() const -> size_t { return count; }
    auto space() const -> size_t { return PTY_BUF_SIZE - count; }
    void flush() { head = tail = count = 0; }
};

// Canonical line buffer size
static constexpr size_t CANON_BUF_SIZE = 256;

// A single PTY pair (master + slave)
struct PtyPair {
    int index;                // PTY number (for /dev/pts/N)
    bool allocated;           // In use
    bool slave_locked;        // TIOCSPTLCK lock (must be unlocked before slave can be opened)
    int slave_opened;         // Slave open refcount (0 = closed)
    int master_opened;        // Master open refcount (0 = closed)
    Winsize winsize;          // Terminal dimensions
    int foreground_pgrp = 0;  // Foreground process group (TIOCGPGRP/TIOCSPGRP)

    KTermios termios;  // Terminal attributes (line discipline settings)

    PtyRingBuf m2s;  // master → slave (master write → slave read)
    PtyRingBuf s2m;  // slave → master (slave write → master read)

    // Canonical mode line editing buffer
    uint8_t canon_buf[CANON_BUF_SIZE]{};
    size_t canon_len = 0;

    // Device structs for master and slave
    Device master_dev;
    Device slave_dev;
};

// Maximum concurrent PTY pairs
static constexpr size_t PTY_MAX = 64;

// Initialize PTY subsystem: registers /dev/ptmx, creates /dev/pts/ directory
void pty_init();

// Allocate a new PTY pair, register the slave device at /dev/pts/<N>.
// Returns the PtyPair index, or -1 on failure.
auto pty_alloc() -> int;

// Get PtyPair by index (nullptr if invalid/unallocated)
auto pty_get(int index) -> PtyPair*;

// Get the ptmx device (for explicit open)
auto get_ptmx_device() -> Device*;

}  // namespace ker::dev::pty
