#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <platform/sys/spinlock.hpp>
#include <util/smallvec.hpp>

#include "device.hpp"

namespace ker::vfs {
struct File;
}  // namespace ker::vfs

namespace ker::dev::pty {

// Linux-compatible winsize struct
struct Winsize {
    uint16_t ws_row;
    uint16_t ws_col;
    uint16_t ws_xpixel;
    uint16_t ws_ypixel;
};
static_assert(sizeof(Winsize) == 8);  // NOLINT

// Kernel-side termios struct (matches mlibc ABI layout exactly)
static constexpr size_t KERNEL_NCCS = 32;

struct KTermios {
    uint32_t c_iflag;
    uint32_t c_oflag;
    uint32_t c_cflag;
    uint32_t c_lflag;
    uint8_t c_line;
    uint8_t c_cc[KERNEL_NCCS];  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays): mlibc termios ABI.
    uint32_t ibaud;
    uint32_t obaud;
};
static_assert(offsetof(KTermios, ibaud) == 52);  // NOLINT
static_assert(sizeof(KTermios) == 60);           // NOLINT

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

// ioctl command numbers (Linux-compatible)
static constexpr unsigned long TIOCGPTN = 0x80045430;    // Get PTY number
static constexpr unsigned long TIOCSPTLCK = 0x40045431;  // Set/clear PTY lock
static constexpr unsigned long TIOCGWINSZ = 0x5413;      // Get window size
static constexpr unsigned long TIOCSWINSZ = 0x5414;      // Set window size
static constexpr unsigned long TIOCSCTTY = 0x540E;       // Set controlling terminal
static constexpr unsigned long TIOCGPGRP = 0x540F;       // Get foreground process group
static constexpr unsigned long TIOCSPGRP = 0x5410;       // Set foreground process group
static constexpr unsigned long TIOCNOTTY = 0x5422;       // Disconnect from controlling terminal
static constexpr unsigned long TCGETS = 0x5401;          // Get termios
static constexpr unsigned long TCSETS = 0x5402;          // Set termios immediately
static constexpr unsigned long TCSETSW = 0x5403;         // Set termios after output drain
static constexpr unsigned long TCSETSF = 0x5404;         // Set termios after flush
static constexpr unsigned long TCFLSH = 0x540B;          // Flush terminal I/O

// Signal numbers
static constexpr int SIG_INT = 2;
static constexpr int SIG_QUIT = 3;
static constexpr int SIG_TSTP = 20;

// poll event bits (Linux-compatible)
static constexpr int POLLIN = 0x001;
static constexpr int POLLOUT = 0x004;
static constexpr int POLLERR = 0x008;
static constexpr int POLLHUP = 0x010;

// Returns a default termios (cooked mode, echo on, signals on)
auto default_termios() -> KTermios;

// Ring buffer for PTY data flow
static constexpr size_t PTY_BUF_SIZE = 65536;

struct PtyRingBuf {
    std::array<uint8_t, PTY_BUF_SIZE> data{};
    size_t head = 0;   // write position
    size_t tail = 0;   // read position
    size_t count = 0;  // bytes in buffer

    auto write(const void* src, size_t len) -> size_t;
    auto read(void* dst, size_t len) -> size_t;
    [[nodiscard]] auto available() const -> size_t { return count; }
    [[nodiscard]] auto space() const -> size_t { return PTY_BUF_SIZE - count; }
    void flush() { head = tail = count = 0; }
};

// Canonical line buffer size
static constexpr size_t CANON_BUF_SIZE = 256;
static constexpr size_t CPR_FILTER_BUF_SIZE = 32;
static constexpr size_t SLAVE_NAME_BUF_SIZE = 8;

// A single PTY pair (master + slave)
struct PtyPair {
    // Lifetime is shared by the radix tree and each live master/slave File.
    // This prevents the embedded waiter SmallVecs from being freed while a
    // devfs File wrapper can still reach the pair during poll/retry paths.
    std::atomic<uint32_t> refcount{1};
    int index;                     // PTY number (for /dev/pts/N)
    bool allocated;                // In use
    bool slave_locked;             // TIOCSPTLCK lock (must be unlocked before slave can be opened)
    bool freeing = false;          // Set under lock by first closer to prevent double-free
    int slave_opened;              // Slave open refcount (0 = closed)
    int master_opened;             // Master open refcount (0 = closed)
    Winsize winsize;               // Terminal dimensions
    uint64_t foreground_pgrp = 0;  // Foreground process group (TIOCGPGRP/TIOCSPGRP)

    KTermios termios;         // Terminal attributes (line discipline settings)
    mod::sys::Spinlock lock;  // Protects termios, canonical state, and ring buffers

    PtyRingBuf m2s;  // master -> slave (master write -> slave read)
    PtyRingBuf s2m;  // slave -> master (slave write -> master read)

    // Canonical mode line editing buffer
    std::array<uint8_t, CANON_BUF_SIZE> canon_buf{};
    size_t canon_len = 0;

    // Tracks potential terminal cursor-position reports (ESC[row;colR)
    // across fragmented master writes so stale ASK_TERMINAL replies can be
    // consumed in-kernel without leaking into userspace input.
    std::array<uint8_t, CPR_FILTER_BUF_SIZE> cpr_filter_buf{};
    size_t cpr_filter_len = 0;
    bool cpr_filter_active = false;

    ker::util::SmallVec<uint64_t, 2> master_poll_waiters;
    ker::util::SmallVec<uint64_t, 2> slave_poll_waiters;
    ker::util::SmallVec<uint64_t, 2> master_read_waiters;
    ker::util::SmallVec<uint64_t, 2> master_write_waiters;
    ker::util::SmallVec<uint64_t, 2> slave_read_waiters;
    ker::util::SmallVec<uint64_t, 2> slave_write_waiters;

    // Persistent slave name buffer (e.g., "0", "12")
    std::array<char, SLAVE_NAME_BUF_SIZE> slave_name{};

    // Device structs for master and slave
    Device master_dev;
    Device slave_dev;
};

// Maximum concurrent PTY pairs (soft limit for PTY number assignment)
static constexpr size_t PTY_MAX = 256;

// Initialize PTY subsystem: registers /dev/ptmx, creates /dev/pts/ directory
void pty_init();

// Allocate a new PTY pair, register the slave device at /dev/pts/<N>.
// Returns the PtyPair index, or -1 on failure.
auto pty_alloc() -> int;

// Get PtyPair by index (nullptr if invalid/unallocated)
auto pty_get(int index) -> PtyPair*;

// Release a PtyPair reference previously obtained from pty_get().
void pty_put(PtyPair* pair);

// Get the ptmx device (for explicit open)
auto get_ptmx_device() -> Device*;

// Returns true if the file is a devfs-backed PTY master or slave file.
auto pty_is_file(ker::vfs::File* f) -> bool;

// Returns a stable per-endpoint identity for PTY-backed files, suitable for
// coalescing duplicate fd exports that point at the same master or slave
// device. Returns nullptr for non-PTY files.
auto pty_file_identity_key(ker::vfs::File* f) -> const void*;

}  // namespace ker::dev::pty
