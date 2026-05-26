#include "pty.hpp"

#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <span>
#include <util/radix_tree.hpp>
#include <utility>
#include <vfs/file.hpp>
#include <vfs/fs/devfs.hpp>

#include "dev/device.hpp"
#include "platform/sched/task.hpp"
#include "platform/sys/spinlock.hpp"
#include "util/smallvec.hpp"

namespace ker::dev::pty {

using log = ker::mod::dbg::logger<"pty">;
// Fairness cadence for long PTY write bursts. Yielding periodically prevents
// a single writer from monopolizing CPU and improves terminal interactivity.
static constexpr size_t PTY_WRITE_FAIR_YIELD_INTERVAL = 4096;

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
    const auto* bytes = static_cast<const uint8_t*>(src);
    size_t written = 0;
    while (written < len && count < PTY_BUF_SIZE) {
        data.at(head) = bytes[written];
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
        bytes[rd] = data.at(tail);
        tail = (tail + 1) % PTY_BUF_SIZE;
        count--;
        rd++;
    }
    return rd;
}

// --- PTY pair pool ---

namespace {

constexpr uint32_t DEVFS_FILE_MAGIC = 0xDEADBEEF;

auto is_valid_kernel_pointer(const void* ptr) -> bool {
    auto addr = reinterpret_cast<uintptr_t>(ptr);
    bool const IN_HHDM = (addr >= 0xffff800000000000ULL && addr < 0xffff900000000000ULL);
    bool const IN_KERNEL_STATIC = (addr >= 0xffffffff80000000ULL && addr < 0xffffffffc0000000ULL);
    return IN_HHDM || IN_KERNEL_STATIC;
}

struct DevFSFileHack {
    void* node;
    ker::dev::Device* device;
    uint32_t magic;
};

auto devfs_file_from_file(ker::vfs::File* f, const char* op) -> DevFSFileHack* {
    if (f == nullptr || f->private_data == nullptr) {
        return nullptr;
    }
    auto* dff = static_cast<DevFSFileHack*>(f->private_data);
    if (!is_valid_kernel_pointer(dff)) {
        log::warn("pty_%s: invalid devfs wrapper %p", op, dff);
        return nullptr;
    }
    if (dff->magic != DEVFS_FILE_MAGIC) {
        log::warn("pty_%s: bad devfs wrapper magic 0x%x at %p", op, dff->magic, dff);
        return nullptr;
    }
    if (dff->device != nullptr && !is_valid_kernel_pointer(dff->device)) {
        log::warn("pty_%s: invalid device pointer %p in wrapper %p", op, dff->device, dff);
        return nullptr;
    }
    return dff;
}

auto pair_from_device(ker::dev::Device* device, const char* op) -> PtyPair* {
    if (device == nullptr) {
        return nullptr;
    }
    auto* pair = static_cast<PtyPair*>(device->private_data);
    if (pair == nullptr || !is_valid_kernel_pointer(pair)) {
        log::warn("pty_%s: invalid pair pointer %p from device %p", op, pair, device);
        return nullptr;
    }
    if (device != &pair->master_dev && device != &pair->slave_dev) {
        log::warn("pty_%s: device %p does not belong to pair %p", op, device, pair);
        return nullptr;
    }
    return pair;
}

auto device_from_file(ker::vfs::File* f) -> ker::dev::Device* {
    auto* dff = devfs_file_from_file(f, "device_from_file");
    if (dff == nullptr) {
        return nullptr;
    }
    return dff->device;
}

auto pair_from_file(ker::vfs::File* f) -> PtyPair*;

auto current_task_has_deliverable_signal() -> bool {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return false;
    }
    return (task->sig_pending & ~task->sig_mask) != 0;
}

auto make_devfs_name(const char* name) -> std::array<char, vfs::devfs::DEVFS_NAME_MAX> {
    std::array<char, vfs::devfs::DEVFS_NAME_MAX> devfs_name{};
    size_t const COPY_LEN = std::min(std::strlen(name) + 1, devfs_name.size());
    std::copy_n(name, COPY_LEN, devfs_name.data());
    devfs_name.back() = '\0';
    return devfs_name;
}

auto make_pts_path(const std::array<char, 8>& slave_name) -> std::array<char, vfs::devfs::DEVFS_NAME_MAX> {
    constexpr std::array<char, 4> PTS_PREFIX = {'p', 't', 's', '/'};

    std::array<char, vfs::devfs::DEVFS_NAME_MAX> pts_path{};
    std::ranges::copy(PTS_PREFIX, pts_path.begin());
    std::copy_n(slave_name.data(), std::strlen(slave_name.data()) + 1, pts_path.data() + PTS_PREFIX.size());
    return pts_path;
}

void pty_unregister_slave(PtyPair* pair) {
    if (pair == nullptr) {
        return;
    }

    auto pts_path = make_pts_path(pair->slave_name);
    ker::vfs::devfs::devfs_remove_node(pts_path.data());
    ker::dev::dev_unregister(&pair->slave_dev);
}

void pty_pair_acquire(PtyPair* pair) {
    if (pair != nullptr) {
        pair->refcount.fetch_add(1, std::memory_order_acq_rel);
    }
}

void pty_pair_release(PtyPair* pair) {
    if (pair != nullptr && pair->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        delete pair;
    }
}

void pty_detach_devices(PtyPair* pair) {
    if (pair == nullptr) {
        return;
    }
    pair->master_dev.private_data = nullptr;
    pair->slave_dev.private_data = nullptr;
}

auto register_waiter(ker::util::SmallVec<uint64_t, 2>& waiters, uint64_t pid) -> bool {
    for (uint64_t const WAITER_PID : waiters) {
        if (WAITER_PID == pid) {
            return true;
        }
    }
    return waiters.push_back(pid);
}

auto block_current_task(ker::util::SmallVec<uint64_t, 2>& waiters, const char* wchan = nullptr) -> bool {
    auto* current_task = ker::mod::sched::get_current_task();
    if (current_task == nullptr) {
        return false;
    }
    if (!register_waiter(waiters, current_task->pid)) {
        return false;
    }
    current_task->wait_channel = wchan;
    return true;
}

void wake_waiters(std::span<const uint64_t> waiters) {
    for (uint64_t const WAITER_PID : waiters) {
        auto* waiter = ker::mod::sched::find_task_by_pid_safe(WAITER_PID);
        if (waiter == nullptr) {
            continue;
        }
        ker::mod::sched::wake_task_from_event(waiter, ker::mod::sched::EventWakeDeferredSwitch::CANCEL);
        waiter->release();
    }
}

void drain_and_wake_waiters(ker::mod::sys::Spinlock& lock, ker::util::SmallVec<uint64_t, 2>& waiters) {
    for (;;) {
        std::array<uint64_t, 32> pending{};
        size_t pending_count = 0;
        uint64_t const IRQF = lock.lock_irqsave();
        while (pending_count < pending.size() && !waiters.empty()) {
            pending.at(pending_count++) = waiters.at(0);
            static_cast<void>(waiters.remove_at(0));
        }
        lock.unlock_irqrestore(IRQF);

        if (pending_count == 0) {
            return;
        }

        wake_waiters(std::span(pending.data(), pending_count));
    }
}

void wake_master_pollers(PtyPair* pair) {
    if (pair == nullptr) {
        return;
    }
    drain_and_wake_waiters(pair->lock, pair->master_poll_waiters);
}

void wake_slave_pollers(PtyPair* pair) {
    if (pair == nullptr) {
        return;
    }
    drain_and_wake_waiters(pair->lock, pair->slave_poll_waiters);
}

void wake_master_readers(PtyPair* pair) {
    if (pair == nullptr) {
        return;
    }
    drain_and_wake_waiters(pair->lock, pair->master_read_waiters);
}

void wake_master_writers(PtyPair* pair) {
    if (pair == nullptr) {
        return;
    }
    drain_and_wake_waiters(pair->lock, pair->master_write_waiters);
}

void wake_slave_readers(PtyPair* pair) {
    if (pair == nullptr) {
        return;
    }
    drain_and_wake_waiters(pair->lock, pair->slave_read_waiters);
}

void wake_slave_writers(PtyPair* pair) {
    if (pair == nullptr) {
        return;
    }
    drain_and_wake_waiters(pair->lock, pair->slave_write_waiters);
}

void wake_both_pollers(PtyPair* pair) {
    if (pair == nullptr) {
        return;
    }
    wake_master_pollers(pair);
    wake_slave_pollers(pair);
}

void wake_master_output_available(PtyPair* pair) {
    wake_master_readers(pair);
    wake_master_pollers(pair);
}

auto pty_poll_register_waiter(ker::vfs::File* file, uint64_t pid) -> bool {
    auto* pair = pair_from_file(file);
    auto* device = device_from_file(file);
    if (pair == nullptr || device == nullptr) {
        return false;
    }

    uint64_t const IRQF = pair->lock.lock_irqsave();
    bool ok = false;
    if (device == &pair->master_dev) {
        ok = register_waiter(pair->master_poll_waiters, pid);
    } else if (device == &pair->slave_dev) {
        ok = register_waiter(pair->slave_poll_waiters, pid);
    }
    pair->lock.unlock_irqrestore(IRQF);
    return ok;
}

enum class CprPrefixMatch : uint8_t {
    PREFIX,
    COMPLETE,
    INVALID,
};

enum class CprFeedAction : uint8_t {
    PASS_THROUGH,
    HOLD,
    DROP,
    REPLAY,
};

constexpr auto is_dec_digit(uint8_t ch) -> bool { return ch >= '0' && ch <= '9'; }

auto classify_cpr_prefix(const uint8_t* seq, size_t len) -> CprPrefixMatch {
    if (seq == nullptr || len == 0) {
        return CprPrefixMatch::INVALID;
    }

    if (seq[0] != 0x1B) {
        return CprPrefixMatch::INVALID;
    }
    if (len == 1) {
        return CprPrefixMatch::PREFIX;
    }

    if (seq[1] != '[') {
        return CprPrefixMatch::INVALID;
    }
    if (len == 2) {
        return CprPrefixMatch::PREFIX;
    }

    size_t pos = 2;
    size_t row_digits = 0;
    while (pos < len && is_dec_digit(seq[pos])) {
        row_digits++;
        pos++;
    }
    if (row_digits == 0) {
        return CprPrefixMatch::INVALID;
    }
    if (pos == len) {
        return CprPrefixMatch::PREFIX;
    }

    if (seq[pos] != ';') {
        return CprPrefixMatch::INVALID;
    }
    pos++;
    if (pos == len) {
        return CprPrefixMatch::PREFIX;
    }

    size_t col_digits = 0;
    while (pos < len && is_dec_digit(seq[pos])) {
        col_digits++;
        pos++;
    }
    if (col_digits == 0) {
        return CprPrefixMatch::INVALID;
    }
    if (pos == len) {
        return CprPrefixMatch::PREFIX;
    }

    if (seq[pos] == 'R' && (pos + 1) == len) {
        return CprPrefixMatch::COMPLETE;
    }
    return CprPrefixMatch::INVALID;
}

auto cpr_filter_feed(PtyPair* pair, uint8_t ch, uint8_t* replay_out, size_t* replay_len_out) -> CprFeedAction {
    if (pair == nullptr || replay_out == nullptr || replay_len_out == nullptr) {
        return CprFeedAction::PASS_THROUGH;
    }

    *replay_len_out = 0;

    if (!pair->cpr_filter_active) {
        if (ch != 0x1B) {
            return CprFeedAction::PASS_THROUGH;
        }
        pair->cpr_filter_active = true;
        pair->cpr_filter_len = 0;
    }

    if (pair->cpr_filter_len < CPR_FILTER_BUF_SIZE) {
        pair->cpr_filter_buf.at(pair->cpr_filter_len++) = ch;
    } else {
        for (size_t i = 0; i < pair->cpr_filter_len; i++) {
            replay_out[i] = pair->cpr_filter_buf.at(i);
        }
        *replay_len_out = pair->cpr_filter_len;
        pair->cpr_filter_active = false;
        pair->cpr_filter_len = 0;
        return CprFeedAction::REPLAY;
    }

    switch (classify_cpr_prefix(pair->cpr_filter_buf.data(), pair->cpr_filter_len)) {
        case CprPrefixMatch::PREFIX:
            return CprFeedAction::HOLD;
        case CprPrefixMatch::COMPLETE:
            pair->cpr_filter_active = false;
            pair->cpr_filter_len = 0;
            return CprFeedAction::DROP;
        case CprPrefixMatch::INVALID:
        default:
            for (size_t i = 0; i < pair->cpr_filter_len; i++) {
                replay_out[i] = pair->cpr_filter_buf.at(i);
            }
            *replay_len_out = pair->cpr_filter_len;
            pair->cpr_filter_active = false;
            pair->cpr_filter_len = 0;
            return CprFeedAction::REPLAY;
    }
}

ker::util::RadixTree<PtyPair*> pty_tree;
ker::mod::sys::Spinlock pty_tree_lock;
bool pty_initialized = false;

// --- Master-side device operations ---

int ptmx_open(ker::vfs::File* file) {
    // Allocate a new PTY pair
    int const IDX = pty_alloc();
    if (IDX < 0) {
        return -ENOMEM;
    }

    auto* pair = pty_get(IDX);
    if (pair == nullptr) {
        return -ENOMEM;
    }
    uint64_t const IRQF = pair->lock.lock_irqsave();
    pair->master_opened++;
    pair->lock.unlock_irqrestore(IRQF);

    // Store the PtyPair pointer in the device's private_data so master
    // read/write/ioctl can find it.
    pair->master_dev.private_data = pair;

    // The DevFSFile wrapper created by devfs_open_path still points to the
    // singleton ptmx_dev.  Redirect it to pair->master_dev so that
    // pair_from_file() on subsequent ioctl/read/write calls can locate the
    // correct PtyPair through device->private_data.
    if (file != nullptr && file->private_data != nullptr) {
        auto* dff = static_cast<DevFSFileHack*>(file->private_data);
        dff->device = &pair->master_dev;
        return 0;
    }

    uint64_t const CLOSE_IRQF = pair->lock.lock_irqsave();
    pair->master_opened--;
    pair->lock.unlock_irqrestore(CLOSE_IRQF);
    pty_put(pair);
    return -ENOMEM;
}

int master_close(ker::vfs::File* file) {
    // Recover PtyPair from the device private_data
    // devfs wraps file->private_data as DevFSFile which has device ptr
    // We look at the device that is associated through devfs
    // The device's private_data points to our PtyPair
    if (file == nullptr || file->private_data == nullptr) {
        return 0;
    }

    // Walk through devfs wrapper to get device
    auto* dff = static_cast<DevFSFileHack*>(file->private_data);
    if (dff->device == nullptr) {
        return 0;
    }
    auto* pair = static_cast<PtyPair*>(dff->device->private_data);
    if (pair == nullptr) {
        return 0;
    }

    uint64_t const IRQF = pair->lock.lock_irqsave();
    pair->master_opened--;

    bool should_free = false;
    // If slave is also closed AND no one else is already freeing, free the pair
    if (pair->slave_opened <= 0 && !pair->freeing) {
        pair->allocated = false;
        pair->freeing = true;
        pty_detach_devices(pair);
        should_free = true;
    }
    pair->lock.unlock_irqrestore(IRQF);

    wake_slave_readers(pair);
    wake_slave_writers(pair);
    wake_slave_pollers(pair);

    if (should_free) {
        pty_unregister_slave(pair);
        uint64_t const TREE_IRQF = pty_tree_lock.lock_irqsave();
        pty_tree.remove(static_cast<uint64_t>(pair->index));
        size_t const PTY_COUNT = pty_tree.size();
        pty_tree_lock.unlock_irqrestore(TREE_IRQF);
        ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::PTY_POOL, static_cast<uint32_t>(pair->index),
                                              ker::mod::perf::PERF_FLAG_CT_REMOVE, static_cast<int64_t>(PTY_COUNT), 0, 0);
        pty_pair_release(pair);
    }

    pty_pair_release(pair);

    return 0;
}

// Get PtyPair* from a devfs-wrapped File
auto pair_from_file(ker::vfs::File* f) -> PtyPair* {
    auto* dff = devfs_file_from_file(f, "pair_from_file");
    if (dff == nullptr || dff->device == nullptr) {
        return nullptr;
    }
    return pair_from_device(dff->device, "pair_from_file");
}

ssize_t master_read(ker::vfs::File* file, void* buf, size_t count) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) {
        return -EBADF;
    }
    pty_pair_acquire(pair);
    auto finish = [&](ssize_t rc) -> ssize_t {
        pty_pair_release(pair);
        return rc;
    };
    const int OPEN_FLAGS = file->open_flags;

    for (;;) {
        // Master reads from slave->master buffer
        uint64_t const IRQF = pair->lock.lock_irqsave();
        size_t const RD = pair->s2m.read(buf, count);
        bool const SLAVE_OPENED = pair->slave_opened > 0;
        bool should_block = false;
        if (RD == 0 && SLAVE_OPENED && (OPEN_FLAGS & 04000) == 0) {
            if (current_task_has_deliverable_signal()) {
                pair->lock.unlock_irqrestore(IRQF);
                return finish(-EINTR);
            }
            should_block = block_current_task(pair->master_read_waiters, "pty_master_read");
        }
        pair->lock.unlock_irqrestore(IRQF);
        if (RD == 0) {
            // If slave is closed, return EOF
            if (!SLAVE_OPENED) {
                return finish(0);
            }
            // Non-blocking fd: return EAGAIN immediately (propagates to application)
            if ((OPEN_FLAGS & 04000) != 0) {
                return finish(-EAGAIN);  // O_NONBLOCK = 04000
            }
            if (current_task_has_deliverable_signal()) {
                continue;
            }
            if (should_block) {
                ker::mod::sched::preemptible_syscall_park("pty_master_read");
            } else {
                ker::mod::sched::kern_yield();
            }
            if (current_task_has_deliverable_signal()) {
                continue;
            }
            continue;
        }
        wake_slave_writers(pair);
        wake_slave_pollers(pair);
        return finish(static_cast<ssize_t>(RD));
    }
}

// --- Line discipline helpers ---

// Send signal to the foreground process group
void pty_signal_fg(PtyPair* pair, int sig) {
    if (pair->foreground_pgrp <= 0) {
        return;
    }
    ker::mod::sched::signal_process_group(static_cast<uint64_t>(pair->foreground_pgrp), sig);
}

// Echo a single byte to s2m, applying output post-processing
void pty_echo_byte(PtyPair* pair, uint8_t ch) {
    if (((pair->termios.c_oflag & TIOS_OPOST) != 0U) && ((pair->termios.c_oflag & TIOS_ONLCR) != 0U) && ch == '\n') {
        uint8_t cr = '\r';
        pair->s2m.write(&cr, 1);
    }
    pair->s2m.write(&ch, 1);
}

// Echo a control character as ^X
void pty_echo_ctrl(PtyPair* pair, uint8_t ch) {
    uint8_t hat = '^';
    uint8_t letter = (ch < 32) ? static_cast<uint8_t>(ch + '@') : static_cast<uint8_t>('?');
    pair->s2m.write(&hat, 1);
    pair->s2m.write(&letter, 1);
}

void pty_echo_erase(PtyPair* pair) {
    constexpr std::array<uint8_t, 3> ERASE = {'\b', ' ', '\b'};
    pair->s2m.write(ERASE.data(), ERASE.size());
}

ssize_t master_write(ker::vfs::File* file, const void* buf, size_t count) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) {
        return -EBADF;
    }
    pty_pair_acquire(pair);
    auto finish = [&](ssize_t rc) -> ssize_t {
        pty_pair_release(pair);
        return rc;
    };
    const int OPEN_FLAGS = file->open_flags;
    if (count == 0) {
        return finish(0);
    }

    {
        uint64_t const IRQF = pair->lock.lock_irqsave();
        bool const SLAVE_OPENED = pair->slave_opened > 0;
        pair->lock.unlock_irqrestore(IRQF);
        if (!SLAVE_OPENED) {
            return finish(-EIO);
        }
    }

    const auto* bytes = static_cast<const uint8_t*>(buf);
    size_t processed = 0;

    std::array<uint8_t, CPR_FILTER_BUF_SIZE> replay_buf{};
    size_t replay_pos = 0;
    size_t replay_len = 0;

    for (size_t i = 0; (i < count) || (replay_pos < replay_len);) {
        bool from_replay = false;
        uint8_t ch = 0;

        if (replay_pos < replay_len) {
            ch = replay_buf.at(replay_pos++);
            from_replay = true;
            if (replay_pos == replay_len) {
                replay_pos = 0;
                replay_len = 0;
            }
        } else {
            ch = bytes[i++];
        }

        // Input processing (c_iflag)
        uint64_t const IRQF = pair->lock.lock_irqsave();

        if (!from_replay) {
            std::array<uint8_t, CPR_FILTER_BUF_SIZE> filter_replay{};
            size_t filter_replay_len = 0;
            CprFeedAction const FILTER_ACTION = cpr_filter_feed(pair, ch, filter_replay.data(), &filter_replay_len);

            if (FILTER_ACTION == CprFeedAction::HOLD || FILTER_ACTION == CprFeedAction::DROP) {
                pair->lock.unlock_irqrestore(IRQF);
                processed++;
                continue;
            }
            if (FILTER_ACTION == CprFeedAction::REPLAY) {
                pair->lock.unlock_irqrestore(IRQF);
                processed++;
                if (filter_replay_len > 0) {
                    std::copy_n(filter_replay.data(), filter_replay_len, replay_buf.data());
                    replay_pos = 0;
                    replay_len = filter_replay_len;
                }
                continue;
            }
        }

        if (((pair->termios.c_iflag & TIOS_IGNCR) != 0U) && ch == '\r') {
            pair->lock.unlock_irqrestore(IRQF);
            if (!from_replay) {
                processed++;
            }
            continue;  // Discard CR
        }
        if (((pair->termios.c_iflag & TIOS_ICRNL) != 0U) && ch == '\r') {
            ch = '\n';  // CR -> NL
        }
        if (((pair->termios.c_iflag & TIOS_INLCR) != 0U) && ch == '\n') {
            ch = '\r';  // NL -> CR
        }
        if ((pair->termios.c_iflag & TIOS_ISTRIP) != 0U) {
            ch &= 0x7F;  // Strip high bit
        }

        // Signal generation (c_lflag ISIG)
        if ((pair->termios.c_lflag & TIOS_ISIG) != 0U) {
            if (ch == pair->termios.c_cc[CC_VINTR] && pair->termios.c_cc[CC_VINTR] != 0) {
                pty_signal_fg(pair, SIG_INT);
                if ((pair->termios.c_lflag & TIOS_ECHO) != 0U) {
                    pty_echo_ctrl(pair, ch);
                    pty_echo_byte(pair, '\n');
                }
                if ((pair->termios.c_lflag & TIOS_NOFLSH) == 0U) {
                    pair->m2s.flush();
                    pair->canon_len = 0;
                }
                pair->lock.unlock_irqrestore(IRQF);
                if (!from_replay) {
                    processed++;
                }
                continue;
            }
            if (ch == pair->termios.c_cc[CC_VQUIT] && pair->termios.c_cc[CC_VQUIT] != 0) {
                pty_signal_fg(pair, SIG_QUIT);
                if ((pair->termios.c_lflag & TIOS_ECHO) != 0U) {
                    pty_echo_ctrl(pair, ch);
                    pty_echo_byte(pair, '\n');
                }
                if ((pair->termios.c_lflag & TIOS_NOFLSH) == 0U) {
                    pair->m2s.flush();
                    pair->canon_len = 0;
                }
                pair->lock.unlock_irqrestore(IRQF);
                if (!from_replay) {
                    processed++;
                }
                continue;
            }
            if (ch == pair->termios.c_cc[CC_VSUSP] && pair->termios.c_cc[CC_VSUSP] != 0) {
                pty_signal_fg(pair, SIG_TSTP);
                if ((pair->termios.c_lflag & TIOS_ECHO) != 0U) {
                    pty_echo_ctrl(pair, ch);
                    pty_echo_byte(pair, '\n');
                }
                pair->lock.unlock_irqrestore(IRQF);
                if (!from_replay) {
                    processed++;
                }
                continue;
            }
        }

        // Canonical mode (ICANON)
        if ((pair->termios.c_lflag & TIOS_ICANON) != 0U) {
            bool is_erase = (ch == pair->termios.c_cc[CC_VERASE] && pair->termios.c_cc[CC_VERASE] != 0);
            if (!is_erase && (ch == '\b' || ch == 127)) {
                is_erase = true;
            }
            if (is_erase) {
                if (pair->canon_len > 0) {
                    pair->canon_len--;
                    if ((pair->termios.c_lflag & TIOS_ECHOE) != 0U) {
                        pty_echo_erase(pair);
                    }
                }
                pair->lock.unlock_irqrestore(IRQF);
                if (!from_replay) {
                    processed++;
                }
                continue;
            }

            if (ch == pair->termios.c_cc[CC_VKILL] && pair->termios.c_cc[CC_VKILL] != 0) {
                if ((pair->termios.c_lflag & (TIOS_ECHOK | TIOS_ECHOE)) != 0U) {
                    while (pair->canon_len > 0) {
                        pty_echo_erase(pair);
                        pair->canon_len--;
                    }
                }
                pair->canon_len = 0;
                pair->lock.unlock_irqrestore(IRQF);
                if (!from_replay) {
                    processed++;
                }
                continue;
            }

            if (ch == pair->termios.c_cc[CC_VEOF] && pair->termios.c_cc[CC_VEOF] != 0) {
                if (pair->canon_len > 0) {
                    pair->m2s.write(pair->canon_buf.data(), pair->canon_len);
                    pair->canon_len = 0;
                }
                pair->lock.unlock_irqrestore(IRQF);
                if (!from_replay) {
                    processed++;
                }
                continue;
            }

            if (pair->canon_len < CANON_BUF_SIZE) {
                pair->canon_buf.at(pair->canon_len++) = ch;
            }

            if (((pair->termios.c_lflag & TIOS_ECHO) != 0U) || (((pair->termios.c_lflag & TIOS_ECHONL) != 0U) && ch == '\n')) {
                if (ch < 32 && ch != '\n' && ch != '\t') {
                    pty_echo_ctrl(pair, ch);
                } else {
                    pty_echo_byte(pair, ch);
                }
            }

            if (ch == '\n') {
                pair->m2s.write(pair->canon_buf.data(), pair->canon_len);
                pair->canon_len = 0;
            }

            pair->lock.unlock_irqrestore(IRQF);
            if (!from_replay) {
                processed++;
            }
        } else {
            if ((pair->termios.c_lflag & TIOS_ECHO) != 0U) {
                pty_echo_byte(pair, ch);
            }
            pair->lock.unlock_irqrestore(IRQF);

            if ((OPEN_FLAGS & 04000) != 0) {
                uint64_t const WR_IRQF = pair->lock.lock_irqsave();
                size_t const WR = pair->m2s.write(&ch, 1);
                bool const SLAVE_OPENED = pair->slave_opened > 0;
                pair->lock.unlock_irqrestore(WR_IRQF);
                if (!SLAVE_OPENED) {
                    return finish(processed > 0 ? static_cast<ssize_t>(processed) : -EIO);
                }
                if (WR == 0) {
                    if (processed == 0) {
                        return finish(-EAGAIN);
                    }
                    break;
                }
            } else {
                while (true) {
                    uint64_t const WR_IRQF = pair->lock.lock_irqsave();
                    size_t const WR = pair->m2s.write(&ch, 1);
                    bool const SLAVE_OPENED = pair->slave_opened > 0;
                    bool should_block = false;
                    if (WR == 0 && SLAVE_OPENED && processed == 0) {
                        should_block = block_current_task(pair->master_write_waiters, "pty_master_write");
                    }
                    pair->lock.unlock_irqrestore(WR_IRQF);
                    if (WR != 0) {
                        break;
                    }
                    if (!SLAVE_OPENED) {
                        return finish(processed > 0 ? static_cast<ssize_t>(processed) : -EIO);
                    }
                    if (processed != 0) {
                        break;
                    }
                    if (current_task_has_deliverable_signal()) {
                        return finish(-EINTR);
                    }
                    if (should_block) {
                        ker::mod::sched::preemptible_syscall_park("pty_master_write");
                    } else {
                        ker::mod::sched::kern_yield();
                    }
                    if (current_task_has_deliverable_signal()) {
                        return finish(-EINTR);
                    }
                }
            }
            if (!from_replay) {
                processed++;
            }
        }

        if ((processed != 0) && ((processed % PTY_WRITE_FAIR_YIELD_INTERVAL) == 0)) {
            if (current_task_has_deliverable_signal()) {
                return finish(static_cast<ssize_t>(processed));
            }
            ker::mod::sched::kern_yield();
            if (current_task_has_deliverable_signal()) {
                return finish(static_cast<ssize_t>(processed));
            }
        }
    }

    if (processed == 0) {
        return finish(-EAGAIN);
    }
    wake_master_readers(pair);
    wake_slave_readers(pair);
    wake_both_pollers(pair);
    return finish(static_cast<ssize_t>(processed));
}

bool master_isatty(ker::vfs::File* /*file*/) { return false; }

int master_ioctl(ker::vfs::File* file, unsigned long cmd, unsigned long arg) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) {
        return -EBADF;
    }

    switch (cmd) {
        case TIOCGPTN: {
            if (arg == 0) {
                return -EFAULT;
            }
            auto* out = reinterpret_cast<int*>(arg);
            uint64_t const IRQF = pair->lock.lock_irqsave();
            *out = pair->index;
            pair->lock.unlock_irqrestore(IRQF);
            return 0;
        }
        case TIOCSPTLCK: {
            if (arg == 0) {
                return -EFAULT;
            }
            const auto* lock_val = reinterpret_cast<const int*>(arg);
            uint64_t const IRQF = pair->lock.lock_irqsave();
            pair->slave_locked = (*lock_val != 0);
            pair->lock.unlock_irqrestore(IRQF);
            return 0;
        }
        case TIOCGWINSZ: {
            if (arg == 0) {
                return -EFAULT;
            }
            auto* ws = reinterpret_cast<Winsize*>(arg);
            uint64_t const IRQF = pair->lock.lock_irqsave();
            *ws = pair->winsize;
            pair->lock.unlock_irqrestore(IRQF);
            return 0;
        }
        case TIOCSWINSZ: {
            if (arg == 0) {
                return -EFAULT;
            }
            const auto* ws = reinterpret_cast<const Winsize*>(arg);
            uint64_t const IRQF = pair->lock.lock_irqsave();
            pair->winsize = *ws;
            pair->lock.unlock_irqrestore(IRQF);
            return 0;
        }
        case TCGETS: {
            if (arg == 0) {
                return -EFAULT;
            }
            auto* out = reinterpret_cast<KTermios*>(arg);
            uint64_t const IRQF = pair->lock.lock_irqsave();
            *out = pair->termios;
            pair->lock.unlock_irqrestore(IRQF);
            return 0;
        }
        case TCSETS:
        case TCSETSW:
        case TCSETSF: {
            if (arg == 0) {
                return -EFAULT;
            }
            const auto* in = reinterpret_cast<const KTermios*>(arg);
            uint64_t const IRQF = pair->lock.lock_irqsave();
            if (cmd == TCSETSF) {
                pair->m2s.flush();
                pair->canon_len = 0;
            }
            pair->termios = *in;
            pair->lock.unlock_irqrestore(IRQF);
            if (cmd == TCSETSF) {
                wake_master_writers(pair);
                wake_master_pollers(pair);
            }
            return 0;
        }
        case TCFLSH: {
            int const QUEUE = static_cast<int>(arg);
            bool flushed_m2s = false;
            bool flushed_s2m = false;
            uint64_t const IRQF = pair->lock.lock_irqsave();
            if (QUEUE == 0 || QUEUE == 2) {
                pair->m2s.flush();
                pair->canon_len = 0;
                flushed_m2s = true;
            }
            if (QUEUE == 1 || QUEUE == 2) {
                pair->s2m.flush();
                flushed_s2m = true;
            }
            pair->lock.unlock_irqrestore(IRQF);
            if (flushed_m2s) {
                wake_master_writers(pair);
                wake_master_pollers(pair);
            }
            if (flushed_s2m) {
                wake_slave_writers(pair);
                wake_slave_pollers(pair);
            }
            return 0;
        }
        default:
            return -ENOTTY;
    }
}

int master_poll_check(ker::vfs::File* file, int events) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) {
        return 0;
    }

    int ready = 0;
    uint64_t const IRQF = pair->lock.lock_irqsave();
    if (((events & POLLIN) != 0) && pair->s2m.available() > 0) {
        ready |= POLLIN;
    }
    if (((events & POLLOUT) != 0) && pair->m2s.space() > 0) {
        ready |= POLLOUT;
    }
    // When the slave side is closed, report POLLHUP so readers (e.g. dropbear)
    // detect the hangup and can clean up the session.
    if (pair->slave_opened <= 0) {
        ready |= POLLHUP;
    }
    pair->lock.unlock_irqrestore(IRQF);
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
    .poll_register_waiter = pty_poll_register_waiter,
};

// --- Slave-side device operations ---

int slave_open(ker::vfs::File* file) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) {
        return -ENODEV;
    }

    uint64_t irqf = pair->lock.lock_irqsave();
    bool const SLAVE_LOCKED = pair->slave_locked;
    pair->lock.unlock_irqrestore(irqf);
    if (SLAVE_LOCKED) {
        return -EIO;  // Slave is locked, cannot open
    }

    pty_pair_acquire(pair);
    irqf = pair->lock.lock_irqsave();
    pair->slave_opened++;

    // Set initial foreground process group to the opener's pgid (only on first open)
    if (pair->foreground_pgrp == 0) {
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr) {
            pair->foreground_pgrp = static_cast<int>((task->pgid != 0) ? task->pgid : task->pid);
        }
    }
    pair->lock.unlock_irqrestore(irqf);

    wake_master_pollers(pair);

    return 0;
}

int slave_close(ker::vfs::File* file) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) {
        return 0;
    }

    uint64_t const IRQF = pair->lock.lock_irqsave();
    pair->slave_opened--;

    bool should_free = false;
    // If master is also closed AND no one else is already freeing, free the pair
    if (pair->master_opened <= 0 && !pair->freeing) {
        pair->allocated = false;
        pair->freeing = true;
        pty_detach_devices(pair);
        should_free = true;
    }
    pair->lock.unlock_irqrestore(IRQF);

    wake_master_readers(pair);
    wake_master_writers(pair);
    wake_master_pollers(pair);

    if (should_free) {
        pty_unregister_slave(pair);
        uint64_t const TREE_IRQF = pty_tree_lock.lock_irqsave();
        pty_tree.remove(static_cast<uint64_t>(pair->index));
        size_t const PTY_COUNT = pty_tree.size();
        pty_tree_lock.unlock_irqrestore(TREE_IRQF);
        ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::PTY_POOL, static_cast<uint32_t>(pair->index),
                                              ker::mod::perf::PERF_FLAG_CT_REMOVE, static_cast<int64_t>(PTY_COUNT), 0, 0);
        pty_pair_release(pair);
    }

    pty_pair_release(pair);

    return 0;
}

ssize_t slave_read(ker::vfs::File* file, void* buf, size_t count) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) {
        return -EBADF;
    }
    pty_pair_acquire(pair);
    auto finish = [&](ssize_t rc) -> ssize_t {
        pty_pair_release(pair);
        return rc;
    };
    const int OPEN_FLAGS = file->open_flags;

    for (;;) {
        // Slave reads from master->slave buffer
        uint64_t const IRQF = pair->lock.lock_irqsave();
        size_t const RD = pair->m2s.read(buf, count);
        bool const MASTER_OPENED = pair->master_opened > 0;
        bool should_block = false;
        if (RD == 0 && MASTER_OPENED && (OPEN_FLAGS & 04000) == 0) {
            if (current_task_has_deliverable_signal()) {
                pair->lock.unlock_irqrestore(IRQF);
                return finish(-EINTR);
            }
            should_block = block_current_task(pair->slave_read_waiters, "pty_slave_read");
        }
        pair->lock.unlock_irqrestore(IRQF);
        if (RD == 0) {
            // If master is closed, return EOF
            if (!MASTER_OPENED) {
                return finish(0);
            }
            // Non-blocking fd: return EAGAIN immediately
            if ((OPEN_FLAGS & 04000) != 0) {
                return finish(-EAGAIN);  // O_NONBLOCK = 04000
            }
            if (current_task_has_deliverable_signal()) {
                continue;
            }
            if (should_block) {
                ker::mod::sched::preemptible_syscall_park("pty_slave_read");
            } else {
                ker::mod::sched::kern_yield();
            }
            if (current_task_has_deliverable_signal()) {
                continue;
            }
            continue;
        }
        wake_master_writers(pair);
        wake_master_pollers(pair);
        return finish(static_cast<ssize_t>(RD));
    }
}

ssize_t slave_write(ker::vfs::File* file, const void* buf, size_t count) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) {
        return -EBADF;
    }
    pty_pair_acquire(pair);
    auto finish = [&](ssize_t rc) -> ssize_t {
        pty_pair_release(pair);
        return rc;
    };
    const int OPEN_FLAGS = file->open_flags;
    if (count == 0) {
        return finish(0);
    }

    // If master is closed, writes should fail (Linux returns EIO)
    {
        uint64_t const IRQF = pair->lock.lock_irqsave();
        bool const MASTER_OPENED = pair->master_opened > 0;
        pair->lock.unlock_irqrestore(IRQF);
        if (!MASTER_OPENED) {
            return finish(-EIO);
        }
    }

    const auto* bytes = static_cast<const uint8_t*>(buf);
    size_t written = 0;

    for (size_t i = 0; i < count; i++) {
        uint8_t ch = bytes[i];

        // Output post-processing (OPOST)
        if (((pair->termios.c_oflag & TIOS_OPOST) != 0U) && ((pair->termios.c_oflag & TIOS_ONLCR) != 0U) && ch == '\n') {
            if ((OPEN_FLAGS & 04000) != 0) {
                uint64_t const IRQF = pair->lock.lock_irqsave();
                bool const HAS_SPACE = pair->s2m.space() >= 2;
                pair->lock.unlock_irqrestore(IRQF);
                if (!HAS_SPACE) {
                    if (written == 0) {
                        return finish(-EAGAIN);
                    }
                    break;
                }
            } else {
                while (true) {
                    uint64_t const IRQF = pair->lock.lock_irqsave();
                    bool const HAS_SPACE = pair->s2m.space() >= 2;
                    bool const MASTER_OPENED = pair->master_opened > 0;
                    bool should_block = false;
                    if (HAS_SPACE) {
                        pair->lock.unlock_irqrestore(IRQF);
                        break;
                    }
                    if (MASTER_OPENED && written == 0) {
                        should_block = block_current_task(pair->slave_write_waiters, "pty_slave_write");
                    }
                    pair->lock.unlock_irqrestore(IRQF);
                    if (!MASTER_OPENED) {
                        if (written > 0) {
                            goto wake_and_return;
                        }
                        return finish(-EIO);
                    }
                    if (written != 0) {
                        goto wake_and_return;
                    }
                    if (current_task_has_deliverable_signal()) {
                        return finish(-EINTR);
                    }
                    if (should_block) {
                        wake_master_readers(pair);
                        wake_both_pollers(pair);
                        ker::mod::sched::preemptible_syscall_park("pty_slave_write");
                    } else {
                        ker::mod::sched::kern_yield();
                    }
                    if (current_task_has_deliverable_signal()) {
                        return finish(-EINTR);
                    }
                }
            }
            uint8_t cr = '\r';
            uint64_t const IRQF = pair->lock.lock_irqsave();
            bool const WAS_EMPTY = pair->s2m.available() == 0;
            size_t const WR = pair->s2m.write(&cr, 1);
            pair->lock.unlock_irqrestore(IRQF);
            if (WR != 0 && (WAS_EMPTY || written == 0)) {
                wake_master_output_available(pair);
            }
        }

        if ((OPEN_FLAGS & 04000) != 0) {
            uint64_t const IRQF = pair->lock.lock_irqsave();
            bool const WAS_EMPTY = pair->s2m.available() == 0;
            size_t const WR = pair->s2m.write(&ch, 1);
            pair->lock.unlock_irqrestore(IRQF);
            if (WR != 0 && (WAS_EMPTY || written == 0)) {
                wake_master_output_available(pair);
            }
            if (WR == 0) {
                if (written == 0) {
                    return finish(-EAGAIN);
                }
                break;
            }
        } else {
            while (true) {
                uint64_t const IRQF = pair->lock.lock_irqsave();
                bool const WAS_EMPTY = pair->s2m.available() == 0;
                size_t const WR = pair->s2m.write(&ch, 1);
                bool const MASTER_OPENED = pair->master_opened > 0;
                bool should_block = false;
                if (WR == 0 && MASTER_OPENED && written == 0) {
                    should_block = block_current_task(pair->slave_write_waiters, "pty_slave_write");
                }
                pair->lock.unlock_irqrestore(IRQF);
                if (WR != 0) {
                    if (WAS_EMPTY || written == 0) {
                        wake_master_output_available(pair);
                    }
                    break;
                }
                if (!MASTER_OPENED) {
                    if (written > 0) {
                        goto wake_and_return;
                    }
                    return finish(-EIO);
                }
                if (written != 0) {
                    goto wake_and_return;
                }
                if (current_task_has_deliverable_signal()) {
                    return finish(-EINTR);
                }
                if (should_block) {
                    wake_master_readers(pair);
                    wake_both_pollers(pair);
                    ker::mod::sched::preemptible_syscall_park("pty_slave_write");
                } else {
                    ker::mod::sched::kern_yield();
                }
                if (current_task_has_deliverable_signal()) {
                    return finish(-EINTR);
                }
            }
        }
        written++;

        // Prevent long-running writers from starving the scheduler when output
        // arrives faster than the SSH/network side can consume it.
        if ((written % PTY_WRITE_FAIR_YIELD_INTERVAL) == 0) {
            if (current_task_has_deliverable_signal()) {
                goto wake_and_return;
            }
            ker::mod::sched::kern_yield();
            if (current_task_has_deliverable_signal()) {
                goto wake_and_return;
            }
        }
    }

    if (written == 0) {
        return finish(-EAGAIN);
    }
wake_and_return:
    wake_master_readers(pair);
    wake_both_pollers(pair);
    return finish(static_cast<ssize_t>(written));
}

bool slave_isatty(ker::vfs::File* /*file*/) { return true; }

int slave_ioctl(ker::vfs::File* file, unsigned long cmd, unsigned long arg) {
    auto* pair = pair_from_file(file);
    if (pair == nullptr) {
        return -EBADF;
    }

    switch (cmd) {
        case TIOCGPTN: {
            if (arg == 0) {
                return -EFAULT;
            }
            auto* out = reinterpret_cast<int*>(arg);
            uint64_t const IRQF = pair->lock.lock_irqsave();
            *out = pair->index;
            pair->lock.unlock_irqrestore(IRQF);
            return 0;
        }
        case TIOCGWINSZ: {
            if (arg == 0) {
                return -EFAULT;
            }
            auto* ws = reinterpret_cast<Winsize*>(arg);
            uint64_t const IRQF = pair->lock.lock_irqsave();
            *ws = pair->winsize;
            pair->lock.unlock_irqrestore(IRQF);
            return 0;
        }
        case TIOCSWINSZ: {
            if (arg == 0) {
                return -EFAULT;
            }
            const auto* ws = reinterpret_cast<const Winsize*>(arg);
            uint64_t const IRQF = pair->lock.lock_irqsave();
            pair->winsize = *ws;
            pair->lock.unlock_irqrestore(IRQF);
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
            if (arg == 0) {
                return -EFAULT;
            }
            auto* out = reinterpret_cast<int*>(arg);
            uint64_t const IRQF = pair->lock.lock_irqsave();
            *out = pair->foreground_pgrp;
            pair->lock.unlock_irqrestore(IRQF);
            return 0;
        }
        case TIOCSPGRP: {
            if (arg == 0) {
                return -EFAULT;
            }
            const auto* in = reinterpret_cast<const int*>(arg);
            uint64_t const IRQF = pair->lock.lock_irqsave();
            pair->foreground_pgrp = *in;
            pair->lock.unlock_irqrestore(IRQF);
            return 0;
        }
        case TCGETS: {
            if (arg == 0) {
                return -EFAULT;
            }
            auto* out = reinterpret_cast<KTermios*>(arg);
            uint64_t const IRQF = pair->lock.lock_irqsave();
            *out = pair->termios;
            pair->lock.unlock_irqrestore(IRQF);
            return 0;
        }
        case TCSETS:
        case TCSETSW:
        case TCSETSF: {
            if (arg == 0) {
                return -EFAULT;
            }
            const auto* in = reinterpret_cast<const KTermios*>(arg);
            uint64_t const IRQF = pair->lock.lock_irqsave();
            if (cmd == TCSETSF) {
                pair->m2s.flush();
                pair->canon_len = 0;
            }
            pair->termios = *in;
            pair->lock.unlock_irqrestore(IRQF);
            if (cmd == TCSETSF) {
                wake_master_writers(pair);
                wake_master_pollers(pair);
            }
            return 0;
        }
        case TCFLSH: {
            int const QUEUE = static_cast<int>(arg);
            bool flushed_m2s = false;
            bool flushed_s2m = false;
            uint64_t const IRQF = pair->lock.lock_irqsave();
            if (QUEUE == 0 || QUEUE == 2) {
                pair->m2s.flush();
                pair->canon_len = 0;
                flushed_m2s = true;
            }
            if (QUEUE == 1 || QUEUE == 2) {
                pair->s2m.flush();
                flushed_s2m = true;
            }
            pair->lock.unlock_irqrestore(IRQF);
            if (flushed_m2s) {
                wake_master_writers(pair);
                wake_master_pollers(pair);
            }
            if (flushed_s2m) {
                wake_slave_writers(pair);
                wake_slave_pollers(pair);
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
    uint64_t const IRQF = pair->lock.lock_irqsave();
    if (((events & POLLIN) != 0) && pair->m2s.available() > 0) {
        ready |= POLLIN;
    }
    if (((events & POLLOUT) != 0) && pair->s2m.space() > 0) {
        ready |= POLLOUT;
    }
    // When the master side is closed, report POLLHUP.
    if (pair->master_opened <= 0) {
        ready |= POLLHUP;
    }
    pair->lock.unlock_irqrestore(IRQF);
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
    .poll_register_waiter = pty_poll_register_waiter,
};

// --- ptmx device (singleton - opening allocates a new PTY pair) ---

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
    log::info("initializing PTY subsystem");

    // Register /dev/ptmx
    dev_register(&ptmx_dev);

    // pty_init runs AFTER devfs_init, so we must explicitly add ptmx to the
    // devfs tree (devfs_init already scanned the device table).
    auto ptmx_name = make_devfs_name("ptmx");
    vfs::devfs::devfs_add_device_node(ptmx_name, &ptmx_dev);

    // Create /dev/pts/ directory in devfs
    vfs::devfs::devfs_create_directory("pts");

    pty_initialized = true;
    log::info("initialized (dynamic allocation)");
}

auto pty_alloc() -> int {
    // Find next available PTY index
    uint64_t irqf = pty_tree_lock.lock_irqsave();
    uint64_t const SLOT = pty_tree.find_first_unset(0);
    pty_tree_lock.unlock_irqrestore(irqf);
    if (SLOT == UINT64_MAX || SLOT >= PTY_MAX) {
        return -1;  // No free PTY pairs
    }

    int const N = static_cast<int>(SLOT);

    auto* pair = new PtyPair{};
    if (pair == nullptr) {
        return -1;
    }

    pair->index = N;
    pair->allocated = true;
    pair->slave_locked = true;
    pair->slave_opened = 0;
    pair->master_opened = 0;
    pair->m2s = PtyRingBuf{};
    pair->s2m = PtyRingBuf{};
    pair->winsize = {.ws_row = 24, .ws_col = 80, .ws_xpixel = 0, .ws_ypixel = 0};
    pair->termios = default_termios();
    pair->canon_len = 0;
    pair->cpr_filter_len = 0;
    pair->cpr_filter_active = false;
    pair->foreground_pgrp = 0;

    // Build the slave name: "N" (e.g., "0", "12")
    if (N < 10) {
        pair->slave_name.at(0) = static_cast<char>('0' + N);
        pair->slave_name.at(1) = '\0';
    } else if (N < 100) {
        pair->slave_name.at(0) = static_cast<char>('0' + (N / 10));
        pair->slave_name.at(1) = static_cast<char>('0' + (N % 10));
        pair->slave_name.at(2) = '\0';
    } else {
        pair->slave_name.at(0) = static_cast<char>('0' + (N / 100));
        pair->slave_name.at(1) = static_cast<char>('0' + ((N / 10) % 10));
        pair->slave_name.at(2) = static_cast<char>('0' + (N % 10));
        pair->slave_name.at(3) = '\0';
    }

    // Set up the slave Device struct
    pair->slave_dev = Device{
        .major = 136,
        .minor = static_cast<unsigned>(N),
        .name = pair->slave_name.data(),
        .type = DeviceType::CHAR,
        .private_data = pair,
        .char_ops = &slave_ops,
    };

    // Set up the master Device struct
    pair->master_dev = Device{
        .major = 5,
        .minor = static_cast<unsigned>(2 + N),
        .name = "ptmx",
        .type = DeviceType::CHAR,
        .private_data = pair,
        .char_ops = &master_ops,
    };

    // Insert into radix tree
    irqf = pty_tree_lock.lock_irqsave();
    bool const INSERTED = pty_tree.insert(SLOT, pair);
    size_t const PTY_COUNT = pty_tree.size();
    pty_tree_lock.unlock_irqrestore(irqf);
    if (!INSERTED) {
        delete pair;
        return -1;
    }

    // Register slave device into devfs as /dev/pts/<N>
    auto pts_path = make_pts_path(pair->slave_name);

    dev_register(&pair->slave_dev);
    if (vfs::devfs::devfs_add_device_node(pts_path, &pair->slave_dev) == nullptr) {
        dev_unregister(&pair->slave_dev);
        irqf = pty_tree_lock.lock_irqsave();
        pty_tree.remove(SLOT);
        pty_tree_lock.unlock_irqrestore(irqf);
        pty_pair_release(pair);
        return -1;
    }
    ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::PTY_POOL, static_cast<uint32_t>(N),
                                          ker::mod::perf::PERF_FLAG_CT_INSERT, static_cast<int64_t>(PTY_COUNT), 0, 0);
#ifdef DEBUG_PTY
    log::debug("allocated pair %d", N);
#endif
    return N;
}

auto pty_get(int index) -> PtyPair* {
    if (index < 0 || std::cmp_greater_equal(index, PTY_MAX)) {
        return nullptr;
    }
    uint64_t const IRQF = pty_tree_lock.lock_irqsave();
    auto* ptr = pty_tree.lookup(static_cast<uint64_t>(index));
    if (ptr != nullptr) {
        pty_pair_acquire(ptr);
    }
    pty_tree_lock.unlock_irqrestore(IRQF);
    return ptr;
}

void pty_put(PtyPair* pair) { pty_pair_release(pair); }

auto get_ptmx_device() -> Device* { return &ptmx_dev; }

auto pty_is_file(ker::vfs::File* f) -> bool {
    if (f == nullptr || f->fs_type != ker::vfs::FSType::DEVFS) {
        return false;
    }

    auto* dff = devfs_file_from_file(f, "is_file");
    if (dff == nullptr || dff->device == nullptr) {
        return false;
    }

    auto* device = dff->device;
    if (device->char_ops != &master_ops && device->char_ops != &slave_ops) {
        return false;
    }

    auto* pair = static_cast<PtyPair*>(device->private_data);
    if (pair == nullptr || !is_valid_kernel_pointer(pair)) {
        return false;
    }

    return device == &pair->master_dev || device == &pair->slave_dev;
}

auto pty_file_identity_key(ker::vfs::File* f) -> const void* {
    auto* dff = devfs_file_from_file(f, "identity_key");
    if (dff == nullptr || dff->device == nullptr) {
        return nullptr;
    }

    auto* device = dff->device;
    auto* pair = static_cast<PtyPair*>(device->private_data);
    if (pair == nullptr || !is_valid_kernel_pointer(pair)) {
        return nullptr;
    }

    if (device != &pair->master_dev && device != &pair->slave_dev) {
        return nullptr;
    }

    return device;
}

}  // namespace ker::dev::pty
