#include "journal.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <dev/device.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/smallvec.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/devfs.hpp>

#include "platform/dbg/dbg.hpp"
#include "platform/ktime/ktime.hpp"
#include "platform/sched/task.hpp"

namespace ker::mod::dbg::journal {
namespace {

constexpr size_t JOURNAL_RING_SIZE = 4096;
constexpr size_t JOURNAL_MODULE_COUNT = 128;
constexpr uint64_t NO_SEQUENCE = 0;
constexpr int EPOLLIN_VALUE = 0x001;
constexpr int EPOLLOUT_VALUE = 0x004;

struct ModuleDevice {
    bool used = false;
    bool device_registered = false;
    char module[JOURNAL_MODULE_MAX]{};
    char dev_name[64]{};
    ker::dev::Device device{};
};

struct DevFSFileHack {
    void* node;
    ker::dev::Device* device;
    uint32_t magic;
};

sys::Spinlock s_lock{};
std::array<JournalRecord, JOURNAL_RING_SIZE> s_ring{};
std::array<ModuleDevice, JOURNAL_MODULE_COUNT> s_modules{};
ker::util::SmallVec<uint64_t, 16> s_poll_waiters;
std::atomic<uint64_t> s_next_sequence{1};
std::atomic<uint64_t> s_oldest_sequence{1};
std::atomic<uint64_t> s_latest_sequence{0};
std::atomic<bool> s_time_available{false};
std::atomic<bool> s_devices_registered{false};
std::atomic<bool> s_devfs_ready{false};
std::atomic<uint8_t> s_serial_threshold{static_cast<uint8_t>(LogLevel::TRACE)};
uint64_t s_boot_id = 0x574f530000000001ULL;

auto level_name(LogLevel level) -> const char* {
    switch (level) {
        case LogLevel::TRACE:
            return "trace";
        case LogLevel::DEBUG:
            return "debug";
        case LogLevel::INFO:
            return "info";
        case LogLevel::NOTICE:
            return "notice";
        case LogLevel::WARN:
            return "warn";
        case LogLevel::ERROR:
            return "error";
        case LogLevel::CRITICAL:
            return "critical";
        case LogLevel::PANIC:
            return "panic";
    }
    return "unknown";
}

auto is_upper_ascii(char c) -> bool { return c >= 'A' && c <= 'Z'; }
auto is_digit_ascii(char c) -> bool { return c >= '0' && c <= '9'; }
auto is_module_char(char c) -> bool {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-';
}
auto to_lower_ascii(char c) -> char { return is_upper_ascii(c) ? static_cast<char>(c - 'A' + 'a') : c; }

void copy_lower_module(char* dst, size_t dst_size, const char* module) {
    if (dst_size == 0) {
        return;
    }
    const char* src = (module != nullptr && module[0] != '\0') ? module : "kernel";
    size_t i = 0;
    for (; i + 1 < dst_size && src[i] != '\0'; i++) {
        dst[i] = to_lower_ascii(src[i]);
    }
    dst[i] = '\0';
}

auto extract_prefix_module(const char* message, char* out_module, size_t out_module_size, const char** out_message) -> bool {
    if (message == nullptr || message[0] != '[' || out_module == nullptr || out_message == nullptr) {
        return false;
    }

    size_t i = 1;
    bool has_non_digit = false;
    while (message[i] != '\0' && message[i] != ']' && i <= JOURNAL_MODULE_MAX) {
        if (!is_module_char(message[i])) {
            return false;
        }
        if (!is_digit_ascii(message[i])) {
            has_non_digit = true;
        }
        i++;
    }
    if (message[i] != ']' || i == 1 || !has_non_digit) {
        return false;
    }

    size_t module_len = std::min(i - 1, out_module_size - 1);
    for (size_t j = 0; j < module_len; j++) {
        out_module[j] = to_lower_ascii(message[j + 1]);
    }
    out_module[module_len] = '\0';

    const char* body = message + i + 1;
    if (*body == ' ') {
        body++;
    }
    *out_message = body;
    return true;
}

auto find_module_locked(const char* module) -> ModuleDevice* {
    for (auto& entry : s_modules) {
        if (entry.used && std::strcmp(entry.module, module) == 0) {
            return &entry;
        }
    }
    return nullptr;
}

auto alloc_module_locked(const char* module) -> ModuleDevice* {
    if (auto* existing = find_module_locked(module); existing != nullptr) {
        return existing;
    }

    for (auto& entry : s_modules) {
        if (entry.used) {
            continue;
        }
        entry.used = true;
        copy_lower_module(entry.module, sizeof(entry.module), module);
        const char* prefix = "journal/modules/";
        size_t prefix_len = std::strlen(prefix);
        std::memcpy(entry.dev_name, prefix, prefix_len);
        size_t module_len = std::strlen(entry.module);
        if (prefix_len + module_len >= sizeof(entry.dev_name)) {
            module_len = sizeof(entry.dev_name) - prefix_len - 1;
        }
        std::memcpy(entry.dev_name + prefix_len, entry.module, module_len);
        entry.dev_name[prefix_len + module_len] = '\0';
        entry.device = {
            .major = 10,
            .minor = static_cast<unsigned>(200 + (&entry - s_modules.data())),
            .name = entry.dev_name,
            .type = ker::dev::DeviceType::CHAR,
            .private_data = entry.module,
            .char_ops = nullptr,
        };
        return &entry;
    }
    return nullptr;
}

auto module_from_file(ker::vfs::File* file) -> const char* {
    if (file == nullptr || file->private_data == nullptr) {
        return nullptr;
    }
    auto* dff = static_cast<DevFSFileHack*>(file->private_data);
    if (dff->device == nullptr) {
        return nullptr;
    }
    return static_cast<const char*>(dff->device->private_data);
}

auto record_matches(const JournalRecord& rec, const char* module_filter) -> bool {
    return module_filter == nullptr || module_filter[0] == '\0' || std::strcmp(rec.module, module_filter) == 0;
}

auto oldest_sequence_locked() -> uint64_t {
    uint64_t latest = s_latest_sequence.load(std::memory_order_relaxed);
    if (latest == 0) {
        return 1;
    }
    if (latest >= JOURNAL_RING_SIZE) {
        return latest - JOURNAL_RING_SIZE + 1;
    }
    return 1;
}

void serial_write_record(const JournalRecord& rec) {
    auto ms = static_cast<uint64_t>((rec.monotonic_us / 1000ULL) % 1000ULL);
    if (mod::io::serial::isPanicMode()) {
        mod::io::serial::writeUnlocked('[');
        mod::io::serial::writeUnlocked(static_cast<uint64_t>(rec.monotonic_us / 1000000ULL));
        mod::io::serial::writeUnlocked('.');
        if (ms < 10) {
            mod::io::serial::writeUnlocked('0');
        }
        if (ms < 100) {
            mod::io::serial::writeUnlocked('0');
        }
        mod::io::serial::writeUnlocked(ms);
        mod::io::serial::writeUnlocked("] ");
        mod::io::serial::writeUnlocked(level_name(static_cast<LogLevel>(rec.level)));
        mod::io::serial::writeUnlocked(' ');
        mod::io::serial::writeUnlocked(static_cast<const char*>(rec.module));
        mod::io::serial::writeUnlocked(": ");
        mod::io::serial::writeUnlocked(static_cast<const char*>(rec.message), rec.message_len);
        mod::io::serial::writeUnlocked('\n');
        return;
    }

    mod::io::serial::ScopedLock lock;
    mod::io::serial::writeUnlocked('[');
    mod::io::serial::writeUnlocked(static_cast<uint64_t>(rec.monotonic_us / 1000000ULL));
    mod::io::serial::writeUnlocked('.');
    if (ms < 10) {
        mod::io::serial::writeUnlocked('0');
    }
    if (ms < 100) {
        mod::io::serial::writeUnlocked('0');
    }
    mod::io::serial::writeUnlocked(ms);
    mod::io::serial::writeUnlocked("] ");
    mod::io::serial::writeUnlocked(level_name(static_cast<LogLevel>(rec.level)));
    mod::io::serial::writeUnlocked(' ');
    mod::io::serial::writeUnlocked(static_cast<const char*>(rec.module));
    mod::io::serial::writeUnlocked(": ");
    mod::io::serial::writeUnlocked(static_cast<const char*>(rec.message), rec.message_len);
    mod::io::serial::writeUnlocked('\n');
}

void wake_waiters() {
    uint64_t pending[64]{};
    size_t pending_count = 0;
    uint64_t flags = s_lock.lock_irqsave();
    pending_count = std::min(s_poll_waiters.size(), size_t{64});
    for (size_t i = 0; i < pending_count; i++) {
        pending[i] = s_poll_waiters[i];
    }
    s_poll_waiters.clear();
    s_lock.unlock_irqrestore(flags);

    for (size_t i = 0; i < pending_count; i++) {
        auto* waiter = ker::mod::sched::find_task_by_pid_safe(pending[i]);
        if (waiter == nullptr) {
            continue;
        }
        waiter->deferredTaskSwitch = false;
        uint64_t target_cpu = waiter->cpu;
        if (waiter->schedQueue == ker::mod::sched::task::Task::SchedQueue::WAITING || waiter->voluntaryBlock) {
            target_cpu = ker::mod::sched::get_least_loaded_cpu();
        }
        ker::mod::sched::reschedule_task_for_cpu(target_cpu, waiter);
        waiter->release();
    }
}

int journal_open(ker::vfs::File* file) {
    if (file != nullptr) {
        file->pos = NO_SEQUENCE;
    }
    return 0;
}

int journal_close(ker::vfs::File* /*file*/) { return 0; }

ssize_t journal_read(ker::vfs::File* file, void* buf, size_t count) { return read_records(file, buf, count, module_from_file(file)); }

ssize_t journal_write(ker::vfs::File* /*file*/, const void* buf, size_t count) {
    if (buf == nullptr) {
        return -EINVAL;
    }
    char message[JOURNAL_MESSAGE_MAX]{};
    size_t copy_len = std::min(count, JOURNAL_MESSAGE_MAX - 1);
    std::memcpy(message, buf, copy_len);
    message[copy_len] = '\0';
    emit(LogLevel::INFO, "userspace", message, (count >= JOURNAL_MESSAGE_MAX) ? JOURNAL_FLAG_TRUNCATED : 0);
    return static_cast<ssize_t>(count);
}

auto journal_isatty(ker::vfs::File* /*file*/) -> bool { return false; }

auto journal_poll_check(ker::vfs::File* file, int events) -> int { return poll_check(file, events, module_from_file(file)); }

auto journal_poll_register_waiter(ker::vfs::File* file, uint64_t pid) -> bool {
    (void)file;
    return poll_register_waiter(file, pid);
}

ker::dev::CharDeviceOps s_journal_ops = {
    .open = journal_open,
    .close = journal_close,
    .read = journal_read,
    .write = journal_write,
    .isatty = journal_isatty,
    .ioctl = nullptr,
    .poll_check = journal_poll_check,
    .poll_register_waiter = journal_poll_register_waiter,
};

ker::dev::Device s_journal_device = {
    .major = 10,
    .minor = 199,
    .name = "journal",
    .type = ker::dev::DeviceType::CHAR,
    .private_data = nullptr,
    .char_ops = &s_journal_ops,
};

void maybe_register_module_device(ModuleDevice& entry) {
    if (!s_devices_registered.load(std::memory_order_acquire) || entry.device_registered) {
        return;
    }
    entry.device.char_ops = &s_journal_ops;
    if (ker::dev::dev_register(&entry.device) == 0) {
        entry.device_registered = true;
        if (s_devfs_ready.load(std::memory_order_acquire)) {
            ker::vfs::devfs::devfs_add_device_node(entry.dev_name, &entry.device);
        }
    }
}

}  // namespace

void init() { s_boot_id = 0x574f530000000000ULL ^ reinterpret_cast<uint64_t>(&s_ring) ^ reinterpret_cast<uint64_t>(&s_modules); }

void enable_time() { s_time_available.store(true, std::memory_order_release); }

void register_devices() {
    if (s_devices_registered.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    ker::dev::dev_register(&s_journal_device);

    uint64_t flags = s_lock.lock_irqsave();
    for (auto& entry : s_modules) {
        if (entry.used) {
            maybe_register_module_device(entry);
        }
    }
    s_lock.unlock_irqrestore(flags);
}

void mark_devfs_ready() {
    s_devfs_ready.store(true, std::memory_order_release);
    ker::vfs::devfs::devfs_create_directory("journal/modules");
    uint64_t flags = s_lock.lock_irqsave();
    for (auto& entry : s_modules) {
        if (entry.used && entry.device_registered) {
            ker::vfs::devfs::devfs_add_device_node(entry.dev_name, &entry.device);
        }
    }
    s_lock.unlock_irqrestore(flags);
}

void set_serial_threshold(LogLevel level) { s_serial_threshold.store(static_cast<uint8_t>(level), std::memory_order_release); }

auto get_serial_threshold() -> LogLevel { return static_cast<LogLevel>(s_serial_threshold.load(std::memory_order_acquire)); }

void emit(LogLevel level, const char* module, const char* message, uint32_t flags) {
    char module_buf[JOURNAL_MODULE_MAX]{};
    const char* body = message != nullptr ? message : "";
    uint32_t rec_flags = flags;

    if (!extract_prefix_module(body, module_buf, sizeof(module_buf), &body)) {
        copy_lower_module(module_buf, sizeof(module_buf), module);
    } else {
        rec_flags |= JOURNAL_FLAG_PREFIX_COMPAT;
    }

    if (mod::io::serial::isPanicMode()) {
        JournalRecord rec{};
        rec.magic = JOURNAL_RECORD_MAGIC;
        rec.version = JOURNAL_RECORD_VERSION;
        rec.header_size = sizeof(JournalRecord) - JOURNAL_MESSAGE_MAX;
        rec.boot_id = s_boot_id;
        rec.monotonic_us = s_time_available.load(std::memory_order_acquire) ? ker::mod::time::getUs() : 0;
        rec.cpu = static_cast<uint32_t>(ker::mod::cpu::getCurrentCpuIdSafe());
        rec.level = static_cast<uint8_t>(level);
        rec.flags = rec_flags;
        std::memcpy(rec.module, module_buf, sizeof(rec.module));

        size_t msg_len = std::strlen(body);
        if (msg_len >= JOURNAL_MESSAGE_MAX) {
            msg_len = JOURNAL_MESSAGE_MAX - 1;
            rec.flags |= JOURNAL_FLAG_TRUNCATED;
        }
        std::memcpy(rec.message, body, msg_len);
        rec.message[msg_len] = '\0';
        rec.message_len = static_cast<uint16_t>(msg_len);

        serial_write_record(rec);
        return;
    }

    JournalRecord rec{};
    rec.magic = JOURNAL_RECORD_MAGIC;
    rec.version = JOURNAL_RECORD_VERSION;
    rec.header_size = sizeof(JournalRecord) - JOURNAL_MESSAGE_MAX;
    rec.boot_id = s_boot_id;
    rec.monotonic_us = s_time_available.load(std::memory_order_acquire) ? ker::mod::time::getUs() : 0;
    rec.cpu = static_cast<uint32_t>(ker::mod::cpu::getCurrentCpuIdSafe());
    rec.level = static_cast<uint8_t>(level);
    rec.flags = rec_flags;
    std::memcpy(rec.module, module_buf, sizeof(rec.module));

    if (ker::mod::sched::can_query_current_task()) {
        if (auto* task = ker::mod::sched::get_current_task(); task != nullptr) {
            rec.pid = task->ownerPid != 0 ? task->ownerPid : task->pid;
            rec.tid = task->pid;
        }
    }

    size_t msg_len = std::strlen(body);
    if (msg_len >= JOURNAL_MESSAGE_MAX) {
        msg_len = JOURNAL_MESSAGE_MAX - 1;
        rec.flags |= JOURNAL_FLAG_TRUNCATED;
    }
    std::memcpy(rec.message, body, msg_len);
    rec.message[msg_len] = '\0';
    rec.message_len = static_cast<uint16_t>(msg_len);

    uint64_t irq_flags = s_lock.lock_irqsave();
    auto* module_entry = alloc_module_locked(rec.module);
    if (module_entry != nullptr) {
        maybe_register_module_device(*module_entry);
    }
    rec.sequence = s_next_sequence.fetch_add(1, std::memory_order_relaxed);
    s_ring[(rec.sequence - 1) % JOURNAL_RING_SIZE] = rec;
    s_latest_sequence.store(rec.sequence, std::memory_order_release);
    s_oldest_sequence.store(oldest_sequence_locked(), std::memory_order_release);
    s_lock.unlock_irqrestore(irq_flags);

    if (rec.level >= s_serial_threshold.load(std::memory_order_acquire)) {
        serial_write_record(rec);
    }
    wake_waiters();
}

void emit_v(LogLevel level, const char* module, const char* format, va_list args, uint32_t flags) {
    char buf[JOURNAL_MESSAGE_MAX]{};
    if (format == nullptr) {
        emit(level, module, "(null)", flags);
        return;
    }
    int written = std::vsnprintf(buf, sizeof(buf), format, args);
    uint32_t rec_flags = flags;
    if (written < 0) {
        emit(level, module, "log formatting failed", rec_flags);
        return;
    }
    if (static_cast<size_t>(written) >= sizeof(buf)) {
        rec_flags |= JOURNAL_FLAG_TRUNCATED;
    }
    emit(level, module, buf, rec_flags);
}

auto read_records(ker::vfs::File* file, void* buf, size_t count, const char* module_filter) -> ssize_t {
    if (file == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    size_t record_count = count / sizeof(JournalRecord);
    if (record_count == 0) {
        return 0;
    }

    auto* out = static_cast<JournalRecord*>(buf);
    size_t copied = 0;
    uint64_t flags = s_lock.lock_irqsave();
    uint64_t latest = s_latest_sequence.load(std::memory_order_acquire);
    if (latest == 0) {
        s_lock.unlock_irqrestore(flags);
        return 0;
    }

    uint64_t oldest = oldest_sequence_locked();
    auto cursor = static_cast<uint64_t>(file->pos);
    if (cursor == NO_SEQUENCE || cursor < oldest) {
        cursor = oldest;
    }

    for (uint64_t seq = cursor; seq <= latest && copied < record_count; seq++) {
        const auto& rec = s_ring[(seq - 1) % JOURNAL_RING_SIZE];
        if (rec.sequence != seq || !record_matches(rec, module_filter)) {
            continue;
        }
        out[copied++] = rec;
        file->pos = static_cast<off_t>(seq + 1);
    }
    if (copied == 0) {
        file->pos = static_cast<off_t>(latest + 1);
    }
    s_lock.unlock_irqrestore(flags);
    return static_cast<ssize_t>(copied * sizeof(JournalRecord));
}

auto poll_check(ker::vfs::File* file, int events, const char* module_filter) -> int {
    if ((events & EPOLLIN_VALUE) == 0) {
        return events & EPOLLOUT_VALUE;
    }
    if (file == nullptr) {
        return events & EPOLLIN_VALUE;
    }
    uint64_t flags = s_lock.lock_irqsave();
    uint64_t latest = s_latest_sequence.load(std::memory_order_acquire);
    uint64_t oldest = oldest_sequence_locked();
    auto cursor = static_cast<uint64_t>(file->pos);
    if (cursor == NO_SEQUENCE || cursor < oldest) {
        cursor = oldest;
    }
    for (uint64_t seq = cursor; seq <= latest; seq++) {
        const auto& rec = s_ring[(seq - 1) % JOURNAL_RING_SIZE];
        if (rec.sequence == seq && record_matches(rec, module_filter)) {
            s_lock.unlock_irqrestore(flags);
            return events & EPOLLIN_VALUE;
        }
    }
    s_lock.unlock_irqrestore(flags);
    return 0;
}

auto poll_register_waiter(ker::vfs::File* /*file*/, uint64_t pid) -> bool {
    uint64_t flags = s_lock.lock_irqsave();
    for (size_t i = 0; i < s_poll_waiters.size(); i++) {
        if (s_poll_waiters[i] == pid) {
            s_lock.unlock_irqrestore(flags);
            return true;
        }
    }
    bool ok = s_poll_waiters.push_back(pid);
    s_lock.unlock_irqrestore(flags);
    return ok;
}

}  // namespace ker::mod::dbg::journal
