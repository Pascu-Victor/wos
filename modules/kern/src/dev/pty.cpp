#include "pty.hpp"

#include <cerrno>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/scheduler.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/devfs.hpp>

namespace ker::dev::pty {

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

// Return a default termios (cooked mode: echo, canonical, signals)
KTermios default_termios() {
    KTermios t{};
    t.c_iflag = TIOS_ICRNL | TIOS_IXON;
    t.c_oflag = TIOS_OPOST | TIOS_ONLCR;
    t.c_cflag = TIOS_CS8 | TIOS_CREAD | TIOS_CLOCAL;
    t.c_lflag = TIOS_ISIG | TIOS_ICANON | TIOS_ECHO | TIOS_ECHOE | TIOS_ECHOK | TIOS_IEXTEN;
    t.c_line = 0;
    // Default control characters
    t.c_cc[CC_VINTR] = 3;     // ^C
    t.c_cc[CC_VQUIT] = 28;    // ^\‍
    t.c_cc[CC_VERASE] = 127;  // DEL
    t.c_cc[CC_VKILL] = 21;    // ^U
    t.c_cc[CC_VEOF] = 4;      // ^D
    t.c_cc[CC_VTIME] = 0;
    t.c_cc[CC_VMIN] = 1;
    t.c_cc[CC_VSTART] = 17;  // ^Q
    t.c_cc[CC_VSTOP] = 19;   // ^S
    t.c_cc[CC_VSUSP] = 26;   // ^Z
    t.c_cc[CC_VEOL] = 0;
    t.ibaud = 38400;
    t.obaud = 38400;
    return t;
}

// --- Ring buffer implementation ---

auto PtyRingBuf::write(const void* src, size_t len) -> size_t {
    auto* bytes = static_cast<const uint8_t*>(src);
    size_t written = 0;
    while (written < len && count < PTY_BUF_SIZE) {
        data[head] = bytes[written];
        head = (head + 1) % PTY_BUF_SIZE;
        count++;
        written++;
    }
    return written;
}

auto PtyRingBuf::read(void* dst, size_t len) -> size_t {
    auto* bytes = static_cast<uint8_t*>(dst);
    size_t rd = 0;
    while (rd < len && count > 0) {
        bytes[rd] = data[tail];
        tail = (tail + 1) % PTY_BUF_SIZE;
        count--;
        rd++;
    }
    return rd;
}

// --- PTY pair pool ---

namespace {

PtyPair pty_pool[PTY_MAX]{};
bool pty_initialized = false;

// --- Master-side device operations ---

int ptmx_open(ker::vfs::File* file) {
    // Allocate a new PTY pair
    int idx = pty_alloc();
    if (idx < 0) {
        return -ENOMEM;
    }

    auto* pair = pty_get(idx);
    if (pair == nullptr) {
        return -ENOMEM;
    }

    pair->master_opened++;

    // Store the PtyPair pointer in the device's private_data so master
    // read/write/ioctl can find it.
    pair->master_dev.private_data = pair;

    // The DevFSFile wrapper created by devfs_open_path still points to the
    // singleton ptmx_dev.  Redirect it to pair->master_dev so that
    // pair_from_file() on subsequent ioctl/read/write calls can locate the
    // correct PtyPair through device->private_data.
    if (file != nullptr && file->private_data != nullptr) {
        struct DevFSFileHack {
            void* node;
            ker::dev::Device* device;
            uint32_t magic;
        };
        auto* dff = static_cast<DevFSFileHack*>(file->private_data);
        dff->device = &pair->master_dev;
    }

    return 0;
}

int master_close(ker::vfs::File* file) {
    // Recover PtyPair from the device private_data
    // devfs wraps file->private_data as DevFSFile which has device ptr
    // We look at the device that is associated through devfs
    // The device's private_data points to our PtyPair
    if (file == nullptr || file->private_data == nullptr) return 0;

    // Walk through devfs wrapper to get device
    struct DevFSFileHack {
        void* node;
        ker::dev::Device* device;
        uint32_t magic;
    };
    auto* dff = static_cast<DevFSFileHack*>(file->private_data);
    if (dff->device == nullptr) return 0;
    auto* pair = static_cast<PtyPair*>(dff->device->private_data);
    if (pair == nullptr) return 0;

    pair->master_opened--;

    // If slave is also closed, free the pair
    if (pair->slave_opened <= 0) {
        pair->allocated = false;
        // Zero out buffers
        pair->m2s = PtyRingBuf{};
        pair->s2m = PtyRingBuf{};
    }

    return 0;
}

// Get PtyPair* from a devfs-wrapped File
PtyPair* pair_from_file(ker::vfs::File* f) {
    if (f == nullptr || f->private_data == nullptr) return nullptr;
    struct DevFSFileHack {
        void* node;
        ker::dev::Device* device;
        uint32_t magic;
    };
    auto* dff = static_cast<DevFSFileHack*>(f->private_data);
    if (dff->device == nullptr) return nullptr;
    return static_cast<PtyPair*>(dff->device->private_data);
}

ssize_t master_read(ker::vfs::File* file, void* buf, size_t count) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) return -EBADF;

    // Master reads from slave→master buffer
    size_t rd = pair->s2m.read(buf, count);
    if (rd == 0) {
        // If slave is closed, return EOF
        if (!pair->slave_opened) return 0;
        // Otherwise would block (no data yet)
        return -EAGAIN;
    }
    return static_cast<ssize_t>(rd);
}

// --- Line discipline helpers ---

// Send signal to the foreground process group
static void pty_signal_fg(PtyPair* pair, int sig) {
    if (pair->foreground_pgrp <= 0) return;
    ker::mod::sched::signal_process_group(static_cast<uint64_t>(pair->foreground_pgrp), sig);
}

// Echo a single byte to s2m, applying output post-processing
static void pty_echo_byte(PtyPair* pair, uint8_t ch) {
    if ((pair->termios.c_oflag & TIOS_OPOST) && (pair->termios.c_oflag & TIOS_ONLCR) && ch == '\n') {
        uint8_t cr = '\r';
        pair->s2m.write(&cr, 1);
    }
    pair->s2m.write(&ch, 1);
}

// Echo a control character as ^X
static void pty_echo_ctrl(PtyPair* pair, uint8_t ch) {
    uint8_t hat = '^';
    uint8_t letter = (ch < 32) ? static_cast<uint8_t>(ch + '@') : static_cast<uint8_t>('?');
    pair->s2m.write(&hat, 1);
    pair->s2m.write(&letter, 1);
}

ssize_t master_write(ker::vfs::File* file, const void* buf, size_t count) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) return -EBADF;

    auto* bytes = static_cast<const uint8_t*>(buf);
    size_t processed = 0;

    for (size_t i = 0; i < count; i++) {
        uint8_t ch = bytes[i];

        // Input processing (c_iflag)
        if ((pair->termios.c_iflag & TIOS_IGNCR) && ch == '\r') {
            processed++;
            continue;  // Discard CR
        }
        if ((pair->termios.c_iflag & TIOS_ICRNL) && ch == '\r') {
            ch = '\n';  // CR → NL
        }
        if ((pair->termios.c_iflag & TIOS_INLCR) && ch == '\n') {
            ch = '\r';  // NL → CR
        }
        if ((pair->termios.c_iflag & TIOS_ISTRIP)) {
            ch &= 0x7F;  // Strip high bit
        }

        // Signal generation (c_lflag ISIG)
        if (pair->termios.c_lflag & TIOS_ISIG) {
            if (ch == pair->termios.c_cc[CC_VINTR] && pair->termios.c_cc[CC_VINTR] != 0) {
                pty_signal_fg(pair, SIG_INT);
                if (pair->termios.c_lflag & TIOS_ECHO) {
                    pty_echo_ctrl(pair, ch);
                    pty_echo_byte(pair, '\n');
                }
                if (!(pair->termios.c_lflag & TIOS_NOFLSH)) {
                    pair->m2s.flush();
                    pair->canon_len = 0;
                }
                processed++;
                continue;
            }
            if (ch == pair->termios.c_cc[CC_VQUIT] && pair->termios.c_cc[CC_VQUIT] != 0) {
                pty_signal_fg(pair, SIG_QUIT);
                if (pair->termios.c_lflag & TIOS_ECHO) {
                    pty_echo_ctrl(pair, ch);
                    pty_echo_byte(pair, '\n');
                }
                if (!(pair->termios.c_lflag & TIOS_NOFLSH)) {
                    pair->m2s.flush();
                    pair->canon_len = 0;
                }
                processed++;
                continue;
            }
            if (ch == pair->termios.c_cc[CC_VSUSP] && pair->termios.c_cc[CC_VSUSP] != 0) {
                pty_signal_fg(pair, SIG_TSTP);
                if (pair->termios.c_lflag & TIOS_ECHO) {
                    pty_echo_ctrl(pair, ch);
                    pty_echo_byte(pair, '\n');
                }
                processed++;
                continue;
            }
        }

        // Canonical mode (ICANON)
        if (pair->termios.c_lflag & TIOS_ICANON) {
            // VERASE - delete last character
            // Accept both the configured VERASE char (default DEL/127) and BS (0x08)
            // since SSH clients may send either for backspace
            bool is_erase = (ch == pair->termios.c_cc[CC_VERASE] && pair->termios.c_cc[CC_VERASE] != 0);
            if (!is_erase && (ch == '\b' || ch == 127)) {
                is_erase = true;
            }
            if (is_erase) {
                if (pair->canon_len > 0) {
                    pair->canon_len--;
                    if (pair->termios.c_lflag & TIOS_ECHOE) {
                        uint8_t erase[] = {'\b', ' ', '\b'};
                        pair->s2m.write(erase, 3);
                    }
                }
                processed++;
                continue;
            }
            // VKILL - erase entire line
            if (ch == pair->termios.c_cc[CC_VKILL] && pair->termios.c_cc[CC_VKILL] != 0) {
                if (pair->termios.c_lflag & (TIOS_ECHOK | TIOS_ECHOE)) {
                    while (pair->canon_len > 0) {
                        uint8_t erase[] = {'\b', ' ', '\b'};
                        pair->s2m.write(erase, 3);
                        pair->canon_len--;
                    }
                }
                pair->canon_len = 0;
                processed++;
                continue;
            }
            // VEOF - flush canonical buffer (EOF)
            if (ch == pair->termios.c_cc[CC_VEOF] && pair->termios.c_cc[CC_VEOF] != 0) {
                if (pair->canon_len > 0) {
                    pair->m2s.write(pair->canon_buf, pair->canon_len);
                    pair->canon_len = 0;
                }
                // Don't put VEOF in the buffer; an empty read signals EOF to slave
                processed++;
                continue;
            }

            // Accumulate in canonical buffer
            if (pair->canon_len < CANON_BUF_SIZE) {
                pair->canon_buf[pair->canon_len++] = ch;
            }

            // Echo
            if ((pair->termios.c_lflag & TIOS_ECHO) || ((pair->termios.c_lflag & TIOS_ECHONL) && ch == '\n')) {
                if (ch < 32 && ch != '\n' && ch != '\t') {
                    // Echo control characters as ^X to avoid cursor artifacts
                    pty_echo_ctrl(pair, ch);
                } else {
                    pty_echo_byte(pair, ch);
                }
            }

            // If newline, flush canonical buffer to m2s
            if (ch == '\n') {
                pair->m2s.write(pair->canon_buf, pair->canon_len);
                pair->canon_len = 0;
            }

            processed++;
        } else {
            // Non-canonical (raw) mode — pass through directly
            if (pair->termios.c_lflag & TIOS_ECHO) {
                pty_echo_byte(pair, ch);
            }

            size_t wr = pair->m2s.write(&ch, 1);
            if (wr == 0) {
                if (processed == 0) return -EAGAIN;
                break;
            }
            processed++;
        }
    }

    if (processed == 0) return -EAGAIN;
    return static_cast<ssize_t>(processed);
}

bool master_isatty(ker::vfs::File* /*file*/) { return false; }

int master_ioctl(ker::vfs::File* file, unsigned long cmd, unsigned long arg) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) return -EBADF;

    switch (cmd) {
        case TIOCGPTN: {
            if (arg == 0) return -EFAULT;
            auto* out = reinterpret_cast<int*>(arg);
            *out = pair->index;
            return 0;
        }
        case TIOCSPTLCK: {
            if (arg == 0) return -EFAULT;
            auto* lock_val = reinterpret_cast<const int*>(arg);
            pair->slave_locked = (*lock_val != 0);
            return 0;
        }
        case TIOCGWINSZ: {
            if (arg == 0) return -EFAULT;
            auto* ws = reinterpret_cast<Winsize*>(arg);
            *ws = pair->winsize;
            return 0;
        }
        case TIOCSWINSZ: {
            if (arg == 0) return -EFAULT;
            auto* ws = reinterpret_cast<const Winsize*>(arg);
            pair->winsize = *ws;
            return 0;
        }
        case TCGETS: {
            if (arg == 0) return -EFAULT;
            auto* out = reinterpret_cast<KTermios*>(arg);
            *out = pair->termios;
            return 0;
        }
        case TCSETS:
        case TCSETSW:
        case TCSETSF: {
            if (arg == 0) return -EFAULT;
            auto* in = reinterpret_cast<const KTermios*>(arg);
            if (cmd == TCSETSF) {
                pair->m2s.flush();
                pair->canon_len = 0;
            }
            pair->termios = *in;
            return 0;
        }
        case TCFLSH: {
            int queue = static_cast<int>(arg);
            if (queue == 0 || queue == 2) {
                pair->m2s.flush();
                pair->canon_len = 0;
            }
            if (queue == 1 || queue == 2) {
                pair->s2m.flush();
            }
            return 0;
        }
        default:
            return -ENOTTY;
    }
}

int master_poll_check(ker::vfs::File* file, int events) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) return 0;

    int ready = 0;
    if ((events & POLLIN) && pair->s2m.available() > 0) {
        ready |= POLLIN;
    }
    if ((events & POLLOUT) && pair->m2s.space() > 0) {
        ready |= POLLOUT;
    }
    return ready;
}

CharDeviceOps master_ops = {
    .open = ptmx_open,
    .close = master_close,
    .read = master_read,
    .write = master_write,
    .isatty = master_isatty,
    .ioctl = master_ioctl,
    .poll_check = master_poll_check,
};

// --- Slave-side device operations ---

int slave_open(ker::vfs::File* file) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) return -ENODEV;

    if (pair->slave_locked) {
        return -EIO;  // Slave is locked, cannot open
    }

    pair->slave_opened++;

    // Set initial foreground process group to the opener's pgid (only on first open)
    if (pair->foreground_pgrp == 0) {
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr) {
            pair->foreground_pgrp = static_cast<int>((task->pgid != 0) ? task->pgid : task->pid);
        }
    }

    return 0;
}

int slave_close(ker::vfs::File* file) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) return 0;

    pair->slave_opened--;

    // If master is also closed, free the pair
    if (pair->master_opened <= 0) {
        pair->allocated = false;
        pair->m2s = PtyRingBuf{};
        pair->s2m = PtyRingBuf{};
    }

    return 0;
}

ssize_t slave_read(ker::vfs::File* file, void* buf, size_t count) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) return -EBADF;

    // Slave reads from master→slave buffer
    size_t rd = pair->m2s.read(buf, count);
    if (rd == 0) {
        // If master is closed, return EOF
        if (!pair->master_opened) return 0;
        return -EAGAIN;
    }
    return static_cast<ssize_t>(rd);
}

ssize_t slave_write(ker::vfs::File* file, const void* buf, size_t count) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) return -EBADF;

    auto* bytes = static_cast<const uint8_t*>(buf);
    size_t written = 0;

    for (size_t i = 0; i < count; i++) {
        uint8_t ch = bytes[i];

        // Output post-processing (OPOST)
        if ((pair->termios.c_oflag & TIOS_OPOST) && (pair->termios.c_oflag & TIOS_ONLCR) && ch == '\n') {
            if (pair->s2m.space() < 2) {
                if (written == 0) return -EAGAIN;
                break;
            }
            uint8_t cr = '\r';
            pair->s2m.write(&cr, 1);
        }

        size_t wr = pair->s2m.write(&ch, 1);
        if (wr == 0) {
            if (written == 0) return -EAGAIN;
            break;
        }
        written++;
    }

    if (written == 0) return -EAGAIN;
    return static_cast<ssize_t>(written);
}

bool slave_isatty(ker::vfs::File* /*file*/) { return true; }

int slave_ioctl(ker::vfs::File* file, unsigned long cmd, unsigned long arg) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) {
        return -EBADF;
    }

    switch (cmd) {
        case TIOCGPTN: {
            if (arg == 0) return -EFAULT;
            auto* out = reinterpret_cast<int*>(arg);
            *out = pair->index;
            return 0;
        }
        case TIOCGWINSZ: {
            if (arg == 0) return -EFAULT;
            auto* ws = reinterpret_cast<Winsize*>(arg);
            *ws = pair->winsize;
            return 0;
        }
        case TIOCSWINSZ: {
            if (arg == 0) return -EFAULT;
            const auto* ws = reinterpret_cast<const Winsize*>(arg);
            pair->winsize = *ws;
            return 0;
        }
        case TIOCSCTTY: {
            auto* task = ker::mod::sched::get_current_task();
            if (task != nullptr) {
                task->controlling_tty = pair->index;
                pair->foreground_pgrp = static_cast<int>((task->pgid != 0) ? task->pgid : task->pid);
            }
            return 0;
        }
        case TIOCNOTTY: {
            auto* task = ker::mod::sched::get_current_task();
            if (task != nullptr) {
                task->controlling_tty = -1;
            }
            return 0;
        }
        case TIOCGPGRP: {
            if (arg == 0) return -EFAULT;
            auto* out = reinterpret_cast<int*>(arg);
            *out = static_cast<int>(pair->foreground_pgrp);
            return 0;
        }
        case TIOCSPGRP: {
            if (arg == 0) return -EFAULT;
            auto* in = reinterpret_cast<const int*>(arg);
            pair->foreground_pgrp = *in;
            return 0;
        }
        case TCGETS: {
            if (arg == 0) return -EFAULT;
            auto* out = reinterpret_cast<KTermios*>(arg);
            *out = pair->termios;
            return 0;
        }
        case TCSETS:
        case TCSETSW:
        case TCSETSF: {
            if (arg == 0) return -EFAULT;
            auto* in = reinterpret_cast<const KTermios*>(arg);
            if (cmd == TCSETSF) {
                pair->m2s.flush();
                pair->canon_len = 0;
            }
            pair->termios = *in;
            return 0;
        }
        case TCFLSH: {
            int queue = static_cast<int>(arg);
            if (queue == 0 || queue == 2) {
                pair->m2s.flush();
                pair->canon_len = 0;
            }
            if (queue == 1 || queue == 2) {
                pair->s2m.flush();
            }
            return 0;
        }
        default:
            return -ENOTTY;
    }
}

int slave_poll_check(ker::vfs::File* file, int events) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) {
        return 0;
    }

    int ready = 0;
    if (((events & POLLIN) != 0) && pair->m2s.available() > 0) {
        ready |= POLLIN;
    }
    if (((events & POLLOUT) != 0) && pair->s2m.space() > 0) {
        ready |= POLLOUT;
    }
    return ready;
}

CharDeviceOps slave_ops = {
    .open = slave_open,
    .close = slave_close,
    .read = slave_read,
    .write = slave_write,
    .isatty = slave_isatty,
    .ioctl = slave_ioctl,
    .poll_check = slave_poll_check,
};

// --- ptmx device (singleton — opening allocates a new PTY pair) ---

Device ptmx_dev = {
    .major = 5,
    .minor = 2,
    .name = "ptmx",
    .type = DeviceType::CHAR,
    .private_data = nullptr,
    .char_ops = &master_ops,
};

}  // anonymous namespace

// --- Public API ---

void pty_init() {
    ker::mod::io::serial::write("pty: initializing PTY subsystem\n");

    // Zero out the pool
    for (size_t i = 0; i < PTY_MAX; i++) {
        pty_pool[i].index = static_cast<int>(i);
        pty_pool[i].allocated = false;
        pty_pool[i].slave_locked = true;  // Locked by default (must unlock before slave open)
        pty_pool[i].slave_opened = 0;
        pty_pool[i].master_opened = 0;
        pty_pool[i].winsize = {.ws_row = 24, .ws_col = 80, .ws_xpixel = 0, .ws_ypixel = 0};
    }

    // Register /dev/ptmx
    dev_register(&ptmx_dev);

    // pty_init runs AFTER devfs_init, so we must explicitly add ptmx to the
    // devfs tree (devfs_init already scanned the device table).
    vfs::devfs::devfs_add_device_node("ptmx", &ptmx_dev);

    // Create /dev/pts/ directory in devfs
    vfs::devfs::devfs_create_directory("pts");

    pty_initialized = true;
    ker::mod::io::serial::write("pty: initialized (max 64 pairs)\n");
}

auto pty_alloc() -> int {
    for (auto& i : pty_pool) {
        if (!i.allocated) {
            auto& pair = i;
            pair.allocated = true;
            pair.slave_locked = true;
            pair.slave_opened = 0;
            pair.master_opened = 0;
            pair.m2s = PtyRingBuf{};
            pair.s2m = PtyRingBuf{};
            pair.winsize = {.ws_row = 24, .ws_col = 80, .ws_xpixel = 0, .ws_ypixel = 0};
            pair.termios = default_termios();
            pair.canon_len = 0;
            pair.foreground_pgrp = 0;

            // Build the slave device name: "pts/<N>"
            // We need a persistent name string — use the Device name field
            // Build "N" as a string (0-63, max 2 digits)
            static char slave_names[PTY_MAX][8]{};
            int n = pair.index;
            if (n < 10) {
                slave_names[n][0] = '0' + static_cast<char>(n);
                slave_names[n][1] = '\0';
            } else {
                slave_names[n][0] = '0' + static_cast<char>(n / 10);
                slave_names[n][1] = '0' + static_cast<char>(n % 10);
                slave_names[n][2] = '\0';
            }

            // Set up the slave Device struct
            pair.slave_dev = Device{
                .major = 136,
                .minor = static_cast<unsigned>(n),
                .name = slave_names[n],
                .type = DeviceType::CHAR,
                .private_data = &pair,
                .char_ops = &slave_ops,
            };

            // Set up the master Device struct (already allocated in ptmx open,
            // but we need per-pair master for devfs wrapping)
            pair.master_dev = Device{
                .major = 5,
                .minor = static_cast<unsigned>(2 + n),  // Each master gets unique minor
                .name = "ptmx",                         // Master is always accessed via /dev/ptmx
                .type = DeviceType::CHAR,
                .private_data = &pair,
                .char_ops = &master_ops,
            };

            // Register slave device into devfs as /dev/pts/<N>
            // Build path "pts/<N>"
            char pts_path[16]{};
            std::memcpy(pts_path, "pts/", 4);
            std::memcpy(pts_path + 4, slave_names[n], std::strlen(slave_names[n]) + 1);

            // Register the slave device, then add to devfs
            dev_register(&pair.slave_dev);
            vfs::devfs::devfs_add_device_node(pts_path, &pair.slave_dev);

            ker::mod::dbg::log("pty: allocated pair %d\n", n);
            return n;
        }
    }
    return -1;  // No free PTY pairs
}

auto pty_get(int index) -> PtyPair* {
    if (index < 0 || index >= static_cast<int>(PTY_MAX)) return nullptr;
    if (!pty_pool[index].allocated) return nullptr;
    return &pty_pool[index];
}

auto get_ptmx_device() -> Device* { return &ptmx_dev; }

}  // namespace ker::dev::pty
