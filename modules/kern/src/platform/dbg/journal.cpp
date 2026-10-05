#include "journal.hpp"

#include <bits/off_t.h>
#include <bits/ssize_t.h>

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
#include <platform/security/authorization.hpp>
#include <platform/sys/spinlock.hpp>
#include <span>
#include <util/smallvec.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/devfs.hpp>
#ifdef WOS_SELFTEST
#include <vfs/vfs.hpp>
#endif

#include "platform/asm/msr.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/ktime/ktime.hpp"
#include "platform/sched/task.hpp"
#include "release.hpp"

namespace ker::mod::dbg::journal {
namespace {

constexpr size_t JOURNAL_RING_SIZE = 4096;
constexpr size_t JOURNAL_MODULE_COUNT = 128;
constexpr size_t MAX_POLL_WAKEUPS = 64;
constexpr uint64_t NO_SEQUENCE = 0;
constexpr int EPOLLIN_VALUE = 0x001;
constexpr int EPOLLOUT_VALUE = 0x004;
constexpr int EPOLLERR_VALUE = 0x008;

auto caller_credentials() -> security::Snapshot {
    auto* task = sched::can_query_current_task() ? sched::get_current_task() : nullptr;
    return task != nullptr ? task->credentials() : security::Snapshot{};
}

auto authorize_read(const security::Snapshot& caller) -> int { return security::authorize_journal_read(caller); }

struct ModuleOutputRoute {
    const char* module;
    uint32_t required_flags;
    uint32_t rejected_flags;
    uint32_t devices;
};

constexpr std::array<ModuleOutputRoute, 2> DEFAULT_MODULE_OUTPUT_ROUTES{{
    {.module = "init", .required_flags = 0, .rejected_flags = JOURNAL_FLAG_KERNEL, .devices = JOURNAL_OUTPUT_SERIAL},
    {.module = "netd", .required_flags = 0, .rejected_flags = JOURNAL_FLAG_KERNEL, .devices = JOURNAL_OUTPUT_SERIAL},
}};

struct ModuleDevice {
    bool used = false;
    bool device_registered = false;
    bool devfs_registered = false;
    bool output_devices_overridden = false;
    uint32_t output_devices = JOURNAL_OUTPUT_NONE;
    std::array<char, JOURNAL_MODULE_MAX> module{};
    std::array<char, vfs::devfs::DEVFS_NAME_MAX> dev_name{};
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
std::atomic<bool> s_registration_pending{false};
std::atomic_flag s_registration_publishing{};
std::atomic<bool> s_publisher_started{false};
std::atomic<uint8_t> s_serial_threshold{static_cast<uint8_t>(LogLevel::TRACE)};
uint64_t s_boot_id = 0x574f530000000001ULL;

auto mix_boot_id(uint64_t value) -> uint64_t {
    value ^= value >> 33;
    value *= 0xff51afd7ed558ccdULL;
    value ^= value >> 33;
    value *= 0xc4ceb9fe1a85ec53ULL;
    value ^= value >> 33;
    return value;
}

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

auto default_output_devices_for_record(const char* module, uint32_t flags) -> uint32_t {
    if ((flags & JOURNAL_FLAG_KERNEL) != 0) {
        return JOURNAL_OUTPUT_SERIAL;
    }

    for (const auto& route : DEFAULT_MODULE_OUTPUT_ROUTES) {
        if (std::strcmp(route.module, module) == 0 && (flags & route.required_flags) == route.required_flags &&
            (flags & route.rejected_flags) == 0) {
            return route.devices;
        }
    }
    return JOURNAL_OUTPUT_NONE;
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

    size_t const MODULE_LEN = std::min(i - 1, out_module_size - 1);
    for (size_t j = 0; j < MODULE_LEN; j++) {
        out_module[j] = to_lower_ascii(message[j + 1]);
    }
    out_module[MODULE_LEN] = '\0';

    const char* body = message + i + 1;
    if (*body == ' ') {
        body++;
    }
    *out_message = body;
    return true;
}

auto find_module_locked(const char* module) -> ModuleDevice* {
    for (auto& entry : s_modules) {
        if (entry.used && std::strcmp(entry.module.data(), module) == 0) {
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
        copy_lower_module(entry.module.data(), entry.module.size(), module);
        const char* prefix = "journal/modules/";
        size_t const PREFIX_LEN = std::strlen(prefix);
        std::copy_n(prefix, PREFIX_LEN, entry.dev_name.begin());
        size_t module_len = std::strlen(entry.module.data());
        if (PREFIX_LEN + module_len >= entry.dev_name.size()) {
            module_len = entry.dev_name.size() - PREFIX_LEN - 1;
        }
        std::copy_n(entry.module.begin(), module_len, entry.dev_name.data() + PREFIX_LEN);
        entry.dev_name.at(PREFIX_LEN + module_len) = '\0';
        entry.device = {
            .major = 10,
            .minor = static_cast<unsigned>(200 + (&entry - s_modules.data())),
            .name = entry.dev_name.data(),
            .type = ker::dev::DeviceType::CHAR,
            .private_data = entry.module.data(),
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
    // JournalRecord is a packed userspace ABI record, so its C string fields are
    // deliberately raw arrays.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    return module_filter == nullptr || module_filter[0] == '\0' || std::strcmp(rec.module, module_filter) == 0;
}

auto oldest_sequence_locked() -> uint64_t {
    uint64_t const LATEST = s_latest_sequence.load(std::memory_order_relaxed);
    if (LATEST == 0) {
        return 1;
    }
    if (LATEST >= JOURNAL_RING_SIZE) {
        return LATEST - JOURNAL_RING_SIZE + 1;
    }
    return 1;
}

auto should_write_serial(const JournalRecord& rec, uint32_t output_devices) -> bool {
    if (rec.level >= static_cast<uint8_t>(LogLevel::PANIC)) {
        return true;
    }
    return (output_devices & JOURNAL_OUTPUT_SERIAL) != 0 && rec.level >= s_serial_threshold.load(std::memory_order_acquire);
}

void serial_write_record(const JournalRecord& rec) {
    auto ms = static_cast<uint64_t>((rec.monotonic_us / 1000ULL) % 1000ULL);
    if (mod::io::serial::is_panic_mode()) {
        mod::io::serial::write_unlocked('[');
        mod::io::serial::write_unlocked(static_cast<uint64_t>(rec.monotonic_us / 1000000ULL));
        mod::io::serial::write_unlocked('.');
        if (ms < 10) {
            mod::io::serial::write_unlocked('0');
        }
        if (ms < 100) {
            mod::io::serial::write_unlocked('0');
        }
        mod::io::serial::write_unlocked(ms);
        mod::io::serial::write_unlocked("] ");
        mod::io::serial::write_unlocked(level_name(static_cast<LogLevel>(rec.level)));
        mod::io::serial::write_unlocked(' ');
        mod::io::serial::write_unlocked(static_cast<const char*>(rec.module));
        mod::io::serial::write_unlocked(": ");
        mod::io::serial::write_unlocked(static_cast<const char*>(rec.message), rec.message_len);
        mod::io::serial::write_unlocked('\n');
        return;
    }

    mod::io::serial::ScopedLock const LOCK;
    mod::io::serial::write_unlocked('[');
    mod::io::serial::write_unlocked(static_cast<uint64_t>(rec.monotonic_us / 1000000ULL));
    mod::io::serial::write_unlocked('.');
    if (ms < 10) {
        mod::io::serial::write_unlocked('0');
    }
    if (ms < 100) {
        mod::io::serial::write_unlocked('0');
    }
    mod::io::serial::write_unlocked(ms);
    mod::io::serial::write_unlocked("] ");
    mod::io::serial::write_unlocked(level_name(static_cast<LogLevel>(rec.level)));
    mod::io::serial::write_unlocked(' ');
    mod::io::serial::write_unlocked(static_cast<const char*>(rec.module));
    mod::io::serial::write_unlocked(": ");
    mod::io::serial::write_unlocked(static_cast<const char*>(rec.message), rec.message_len);
    mod::io::serial::write_unlocked('\n');
    mod::io::serial::flush_unlocked();
}

void wake_waiters() {
    std::array<uint64_t, MAX_POLL_WAKEUPS> pending{};
    uint64_t const FLAGS = s_lock.lock_irqsave();
    size_t const PENDING_COUNT = std::min(s_poll_waiters.size(), pending.size());
    for (size_t i = 0; i < PENDING_COUNT; i++) {
        pending.at(i) = s_poll_waiters.at(i);
    }
    s_poll_waiters.clear();
    s_lock.unlock_irqrestore(FLAGS);

    for (uint64_t const PID : std::span(pending.data(), PENDING_COUNT)) {
        auto* waiter = ker::mod::sched::find_task_by_pid_safe(PID);
        if (waiter == nullptr) {
            continue;
        }
        ker::mod::sched::wake_task_from_event(waiter, ker::mod::sched::EventWakeDeferredSwitch::CANCEL);
        waiter->release();
    }
}

auto journal_open_with_credentials(ker::vfs::File* file, const security::Snapshot& caller, sched::task::Task* /*opener*/) -> int {
    if (file == nullptr) return -EINVAL;
    if ((file->open_flags & 3) != 1 && authorize_read(caller) != 0) return -EACCES;
    if (file != nullptr) {
        file->pos = NO_SEQUENCE;
    }
    return 0;
}

int journal_close(ker::vfs::File* /*file*/) { return 0; }

void preserve_sequence_cursor_after_vfs_transfer(ker::vfs::File* file, ssize_t bytes) {
    if (file == nullptr || bytes <= 0) {
        return;
    }
    // VFS applies byte-offset accounting after device ops; journal stores a sequence cursor in File::pos.
    file->pos -= static_cast<off_t>(bytes);
}

ssize_t journal_read(ker::vfs::File* file, void* buf, size_t count) {
    ssize_t const RET = read_records(file, buf, count, module_from_file(file));
    preserve_sequence_cursor_after_vfs_transfer(file, RET);
    return RET;
}

ssize_t journal_write(ker::vfs::File* file, const void* buf, size_t count) {
    if (buf == nullptr) {
        return -EINVAL;
    }
    std::array<char, JOURNAL_MESSAGE_MAX> message{};
    size_t const COPY_LEN = std::min(count, JOURNAL_MESSAGE_MAX - 1);
    std::memcpy(message.data(), buf, COPY_LEN);
    message.at(COPY_LEN) = '\0';
    emit(LogLevel::INFO, "userspace", message.data(), (count >= JOURNAL_MESSAGE_MAX) ? JOURNAL_FLAG_TRUNCATED : 0);
    auto const WRITTEN = static_cast<ssize_t>(count);
    preserve_sequence_cursor_after_vfs_transfer(file, WRITTEN);
    return WRITTEN;
}

auto journal_isatty(ker::vfs::File* /*file*/) -> bool { return false; }

auto journal_poll_check(ker::vfs::File* file, int events) -> int { return poll_check(file, events, module_from_file(file)); }

auto journal_poll_register_waiter(ker::vfs::File* file, uint64_t pid) -> bool {
    (void)file;
    return poll_register_waiter(file, pid);
}

ker::dev::CharDeviceOps s_journal_ops = {
    .close = journal_close,
    .read = journal_read,
    .write = journal_write,
    .isatty = journal_isatty,
    .ioctl = nullptr,
    .poll_check = journal_poll_check,
    .poll_register_waiter = journal_poll_register_waiter,
    .open_with_credentials = journal_open_with_credentials,
};

ker::dev::Device s_journal_device = {
    .major = 10,
    .minor = 199,
    .name = "journal",
    .type = ker::dev::DeviceType::CHAR,
    .private_data = nullptr,
    .char_ops = &s_journal_ops,
};

// Called with s_lock held, including IRQ and allocator logging paths. Queue
// only: registry/devfs publication can allocate, log, and take namespace locks.
void note_module_registration_locked(const ModuleDevice& entry) {
    if (entry.used && s_devices_registered.load(std::memory_order_acquire) &&
        (!entry.device_registered || (s_devfs_ready.load(std::memory_order_acquire) && !entry.devfs_registered))) {
        s_registration_pending.store(true, std::memory_order_release);
    }
}

void publish_pending_modules() {
    if (!s_registration_pending.load(std::memory_order_acquire) || s_registration_publishing.test_and_set(std::memory_order_acquire))
        return;
    struct Release {
        ~Release() { s_registration_publishing.clear(std::memory_order_release); }
    } release;
    s_registration_pending.store(false, std::memory_order_release);
    for (auto& entry : s_modules) {
        uint64_t flags = s_lock.lock_irqsave();
        bool const REGISTER_DEVICE = entry.used && !entry.device_registered && s_devices_registered.load(std::memory_order_acquire);
        bool const REGISTER_NODE = entry.used && !entry.devfs_registered && (entry.device_registered || REGISTER_DEVICE) &&
                                   s_devfs_ready.load(std::memory_order_acquire);
        if (REGISTER_DEVICE) entry.device.char_ops = &s_journal_ops;
        s_lock.unlock_irqrestore(flags);
        // Entries have static lifetime; identity is immutable after used=true.
        // The single-flight publisher alone updates their registration flags.
        bool const DEVICE_OK = !REGISTER_DEVICE || ker::dev::dev_register(&entry.device) == 0;
        bool const NODE_OK =
            !REGISTER_NODE || (DEVICE_OK && ker::vfs::devfs::devfs_add_device_node(entry.dev_name, &entry.device) != nullptr);
        flags = s_lock.lock_irqsave();
        if (REGISTER_DEVICE && DEVICE_OK) entry.device_registered = true;
        if (REGISTER_NODE && NODE_OK) entry.devfs_registered = true;
        note_module_registration_locked(entry);
        s_lock.unlock_irqrestore(flags);
    }
}

void module_publisher_main() {
    for (;;) {
        publish_pending_modules();
        ker::mod::sched::kern_sleep_us(100000);
    }
}

}  // namespace

void init() {
    uint64_t seed = 0x574f530000000001ULL;
    seed ^= rdtsc();
    seed ^= reinterpret_cast<uint64_t>(&s_ring);
    seed ^= reinterpret_cast<uint64_t>(&s_modules);
    seed ^= ker::mod::cpu::get_current_cpu_id_safe() << 48;
    s_boot_id = mix_boot_id(seed);
    if (s_boot_id == 0) {
        s_boot_id = 1;
    }
    // separate journal logs from the uefi/bootloader logs
    mod::io::serial::write("\n");
    mod::io::serial::write(ker::release::WOS_LOGO);
    mod::io::serial::write(ker::release::NAME);
    mod::io::serial::write(" version=");
    mod::io::serial::write(ker::release::VERSION);
    mod::io::serial::write(" boot_id=");
    mod::io::serial::write_hex(s_boot_id);
    mod::io::serial::write("\ncompiler=");
    mod::io::serial::write(ker::release::COMPILER);
    mod::io::serial::write("\nbuilder=");
    mod::io::serial::write(ker::release::BUILDER);
    mod::io::serial::write("\n");
}

void enable_time() { s_time_available.store(true, std::memory_order_release); }

void register_devices() {
    if (s_devices_registered.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    ker::dev::dev_register(&s_journal_device);

    uint64_t const FLAGS = s_lock.lock_irqsave();
    for (auto& entry : s_modules) {
        if (entry.used) {
            note_module_registration_locked(entry);
        }
    }
    s_lock.unlock_irqrestore(FLAGS);
    publish_pending_modules();
}

void mark_devfs_ready() {
    s_devfs_ready.store(true, std::memory_order_release);
    ker::vfs::devfs::devfs_create_directory("journal/modules");
    uint64_t const FLAGS = s_lock.lock_irqsave();
    for (auto& entry : s_modules) {
        if (entry.used) note_module_registration_locked(entry);
    }
    s_lock.unlock_irqrestore(FLAGS);
    publish_pending_modules();
}

auto start_module_publisher() -> bool {
    bool expected = false;
    if (!s_publisher_started.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return true;
    auto* worker = sched::task::Task::create_kernel_thread("journal_publish", module_publisher_main);
    if (worker == nullptr) {
        s_publisher_started.store(false, std::memory_order_release);
        return false;
    }
    static_cast<void>(sched::post_task_balanced(worker));
    return true;
}

void set_serial_threshold(LogLevel level) { s_serial_threshold.store(static_cast<uint8_t>(level), std::memory_order_release); }

auto get_serial_threshold() -> LogLevel { return static_cast<LogLevel>(s_serial_threshold.load(std::memory_order_acquire)); }

void set_module_output_devices(const char* module, uint32_t devices) {
    std::array<char, JOURNAL_MODULE_MAX> module_buf{};
    copy_lower_module(module_buf.data(), module_buf.size(), module);

    uint64_t const FLAGS = s_lock.lock_irqsave();
    auto* module_entry = alloc_module_locked(module_buf.data());
    if (module_entry != nullptr) {
        module_entry->output_devices_overridden = true;
        module_entry->output_devices = devices;
        note_module_registration_locked(*module_entry);
    }
    s_lock.unlock_irqrestore(FLAGS);
}

auto get_module_output_devices(const char* module) -> uint32_t {
    std::array<char, JOURNAL_MODULE_MAX> module_buf{};
    copy_lower_module(module_buf.data(), module_buf.size(), module);

    uint64_t const FLAGS = s_lock.lock_irqsave();
    auto* module_entry = find_module_locked(module_buf.data());
    uint32_t const DEVICES = module_entry != nullptr && module_entry->output_devices_overridden
                                 ? module_entry->output_devices
                                 : default_output_devices_for_record(module_buf.data(), 0);
    s_lock.unlock_irqrestore(FLAGS);
    return DEVICES;
}

void emit(LogLevel level, const char* module, const char* message, uint32_t flags) {
    std::array<char, JOURNAL_MODULE_MAX> module_buf{};
    const char* body = message != nullptr ? message : "";
    uint32_t rec_flags = flags;

    if (!extract_prefix_module(body, module_buf.data(), module_buf.size(), &body)) {
        copy_lower_module(module_buf.data(), module_buf.size(), module);
    } else {
        rec_flags |= JOURNAL_FLAG_PREFIX_COMPAT;
    }

    if (mod::io::serial::is_panic_mode()) {
        JournalRecord rec{};
        rec.magic = JOURNAL_RECORD_MAGIC;
        rec.version = JOURNAL_RECORD_VERSION;
        rec.header_size = sizeof(JournalRecord) - JOURNAL_MESSAGE_MAX;
        rec.boot_id = s_boot_id;
        rec.monotonic_us = s_time_available.load(std::memory_order_acquire) ? ker::mod::time::get_us() : 0;
        rec.cpu = static_cast<uint32_t>(ker::mod::cpu::get_current_cpu_id_safe());
        rec.level = static_cast<uint8_t>(level);
        rec.flags = rec_flags;
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        std::memcpy(rec.module, module_buf.data(), sizeof(rec.module));

        size_t msg_len = std::strlen(body);
        if (msg_len >= JOURNAL_MESSAGE_MAX) {
            msg_len = JOURNAL_MESSAGE_MAX - 1;
            rec.flags |= JOURNAL_FLAG_TRUNCATED;
        }
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        std::memcpy(rec.message, body, msg_len);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
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
    rec.monotonic_us = s_time_available.load(std::memory_order_acquire) ? ker::mod::time::get_us() : 0;
    rec.cpu = static_cast<uint32_t>(ker::mod::cpu::get_current_cpu_id_safe());
    rec.level = static_cast<uint8_t>(level);
    rec.flags = rec_flags;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    std::memcpy(rec.module, module_buf.data(), sizeof(rec.module));

    if (ker::mod::sched::can_query_current_task()) {
        if (auto* task = ker::mod::sched::get_current_task(); task != nullptr) {
            rec.pid = task->owner_pid != 0 ? task->owner_pid : task->pid;
            rec.tid = task->pid;
        }
    }

    size_t msg_len = std::strlen(body);
    if (msg_len >= JOURNAL_MESSAGE_MAX) {
        msg_len = JOURNAL_MESSAGE_MAX - 1;
        rec.flags |= JOURNAL_FLAG_TRUNCATED;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    std::memcpy(rec.message, body, msg_len);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    rec.message[msg_len] = '\0';
    rec.message_len = static_cast<uint16_t>(msg_len);

    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    uint32_t output_devices = default_output_devices_for_record(rec.module, rec.flags);
    uint64_t const IRQ_FLAGS = s_lock.lock_irqsave();
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    auto* module_entry = alloc_module_locked(rec.module);
    if (module_entry != nullptr && module_entry->output_devices_overridden) {
        output_devices = module_entry->output_devices;
    }
    if (module_entry != nullptr) {
        note_module_registration_locked(*module_entry);
    }
    rec.sequence = s_next_sequence.fetch_add(1, std::memory_order_relaxed);
    s_ring.at((rec.sequence - 1) % JOURNAL_RING_SIZE) = rec;
    s_latest_sequence.store(rec.sequence, std::memory_order_release);
    s_oldest_sequence.store(oldest_sequence_locked(), std::memory_order_release);
    s_lock.unlock_irqrestore(IRQ_FLAGS);

    if (should_write_serial(rec, output_devices)) {
        serial_write_record(rec);
    }
    wake_waiters();
}

void emit_v(LogLevel level, const char* module, const char* format, va_list args, uint32_t flags) {
    std::array<char, JOURNAL_MESSAGE_MAX> buf{};
    if (format == nullptr) {
        emit(level, module, "(null)", flags);
        return;
    }
    int const WRITTEN = std::vsnprintf(buf.data(), buf.size(), format, args);
    uint32_t rec_flags = flags;
    if (WRITTEN < 0) {
        emit(level, module, "log formatting failed", rec_flags);
        return;
    }
    if (static_cast<size_t>(WRITTEN) >= buf.size()) {
        rec_flags |= JOURNAL_FLAG_TRUNCATED;
    }
    emit(level, module, buf.data(), rec_flags);
}

namespace {
auto read_records_with_credentials(const security::Snapshot& caller, ker::vfs::File* file, void* buf, size_t count,
                                   const char* module_filter) -> ssize_t {
    if (authorize_read(caller) != 0) return -EACCES;
    if (file == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    size_t const RECORD_COUNT = count / sizeof(JournalRecord);
    if (RECORD_COUNT == 0) {
        return 0;
    }

    auto* out = static_cast<JournalRecord*>(buf);
    size_t copied = 0;
    uint64_t const FLAGS = s_lock.lock_irqsave();
    uint64_t const LATEST = s_latest_sequence.load(std::memory_order_acquire);
    if (LATEST == 0) {
        s_lock.unlock_irqrestore(FLAGS);
        return 0;
    }

    uint64_t const OLDEST = oldest_sequence_locked();
    auto cursor = static_cast<uint64_t>(file->pos);
    if (cursor == NO_SEQUENCE || cursor < OLDEST) {
        cursor = OLDEST;
    }

    for (uint64_t seq = cursor; seq <= LATEST && copied < RECORD_COUNT; seq++) {
        const auto& rec = s_ring.at((seq - 1) % JOURNAL_RING_SIZE);
        if (rec.sequence != seq || !record_matches(rec, module_filter)) {
            continue;
        }
        out[copied++] = rec;
        file->pos = static_cast<off_t>(seq + 1);
    }
    if (copied == 0) {
        file->pos = static_cast<off_t>(LATEST + 1);
    }
    s_lock.unlock_irqrestore(FLAGS);
    return static_cast<ssize_t>(copied * sizeof(JournalRecord));
}

auto poll_check_with_credentials(const security::Snapshot& caller, ker::vfs::File* file, int events, const char* module_filter) -> int {
    if ((events & EPOLLIN_VALUE) != 0 && authorize_read(caller) != 0) return EPOLLERR_VALUE | (events & EPOLLOUT_VALUE);
    if ((events & EPOLLIN_VALUE) == 0) {
        return events & EPOLLOUT_VALUE;
    }
    if (file == nullptr) {
        return events & EPOLLIN_VALUE;
    }
    uint64_t const FLAGS = s_lock.lock_irqsave();
    uint64_t const LATEST = s_latest_sequence.load(std::memory_order_acquire);
    uint64_t const OLDEST = oldest_sequence_locked();
    auto cursor = static_cast<uint64_t>(file->pos);
    if (cursor == NO_SEQUENCE || cursor < OLDEST) {
        cursor = OLDEST;
    }
    for (uint64_t seq = cursor; seq <= LATEST; seq++) {
        const auto& rec = s_ring.at((seq - 1) % JOURNAL_RING_SIZE);
        if (rec.sequence == seq && record_matches(rec, module_filter)) {
            s_lock.unlock_irqrestore(FLAGS);
            return events & EPOLLIN_VALUE;
        }
    }
    s_lock.unlock_irqrestore(FLAGS);
    return 0;
}

auto poll_register_waiter_with_credentials(const security::Snapshot& caller, uint64_t pid) -> bool {
    if (authorize_read(caller) != 0) return false;
    uint64_t const FLAGS = s_lock.lock_irqsave();
    for (uint64_t const WAITER_PID : s_poll_waiters) {
        if (WAITER_PID == pid) {
            s_lock.unlock_irqrestore(FLAGS);
            return true;
        }
    }
    bool const OK = s_poll_waiters.push_back(pid);
    s_lock.unlock_irqrestore(FLAGS);
    return OK;
}

}  // namespace

auto read_records(ker::vfs::File* file, void* buf, size_t count, const char* module_filter) -> ssize_t {
    return read_records_with_credentials(caller_credentials(), file, buf, count, module_filter);
}

auto poll_check(ker::vfs::File* file, int events, const char* module_filter) -> int {
    return poll_check_with_credentials(caller_credentials(), file, events, module_filter);
}

auto poll_register_waiter(ker::vfs::File* /*file*/, uint64_t pid) -> bool {
    return poll_register_waiter_with_credentials(caller_credentials(), pid);
}

#ifdef WOS_SELFTEST
auto selftest_module_publication() -> bool {
    constexpr const char* MODULE = "goal19-publisher";
    constexpr const char* PATH = "journal/modules/goal19-publisher";
    publish_pending_modules();
    if (vfs::devfs::devfs_acquire_path(PATH)) return false;
    size_t const BEFORE = ker::dev::dev_get_count();
    emit(LogLevel::INFO, MODULE, "deferred publication test", JOURNAL_FLAG_KERNEL);
    if (ker::dev::dev_get_count() != BEFORE || vfs::devfs::devfs_acquire_path(PATH)) return false;
    // The record is immediately readable even though its alias is still queued.
    vfs::File file{};
    JournalRecord record{};
    auto const ROOT = security::Snapshot::bootstrap_root();
    if (read_records_with_credentials(ROOT, &file, &record, sizeof(record), MODULE) != sizeof(record) ||
        std::strcmp(record.module, MODULE) != 0 || std::strcmp(record.message, "deferred publication test") != 0)
        return false;
    publish_pending_modules();
    auto retained = vfs::devfs::devfs_acquire_path(PATH);
    if (!retained || ker::dev::dev_get_count() != BEFORE + 1 || retained->device == nullptr || retained->device->char_ops != &s_journal_ops)
        return false;
    if (vfs::devfs::devfs_chmod_node(retained.get(), ROOT, 0600, 0) != 0) return false;
    emit(LogLevel::INFO, MODULE, "already published", JOURNAL_FLAG_KERNEL);
    publish_pending_modules();
    vfs::Stat stat{};
    bool const OKAY = ker::dev::dev_get_count() == BEFORE + 1 && vfs::devfs::devfs_stat_node(retained.get(), 0, &stat) == 0 &&
                      (stat.st_mode & 07777) == 0600 && vfs::devfs::devfs_acquire_path(PATH).get() == retained.get();
    static_cast<void>(vfs::devfs::devfs_chmod_node(retained.get(), ROOT, 0666, 0));
    return OKAY;
}

auto selftest_open_admission() -> bool {
    namespace devfs = ker::vfs::devfs;
    auto const ROOT = security::Snapshot::bootstrap_root();
    security::Snapshot user, restricted_root, reader, retired_reader;
    if (ROOT.prepare({.change = security::Change::SET_UID, .real = 1000}, user) != 0 ||
        ROOT.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, restricted_root) != 0 ||
        ROOT.prepare({.change = security::Change::NO_NEW_PRIVILEGES}, reader) != 0 ||
        reader.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 1}, reader) != 0 ||
        reader.prepare({.change = security::Change::SET_UID, .real = 101}, reader) != 0 ||
        reader.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, retired_reader) != 0)
        return false;
    // Use the real shared journal operation table through the devfs dispatcher.
    // No current-task credential publication or journal record mutation occurs.
    constexpr const char* PATH = "pts/ktest_journal_admission";
    auto* node = devfs::devfs_add_device_node(PATH, &s_journal_device);
    if (node == nullptr) return false;
    auto const REFS = node->pts_refs.load();
    bool passed = s_journal_ops.open == nullptr && s_journal_ops.open_with_credentials != nullptr;
    for (auto const& caller : {ROOT, user, restricted_root, reader, retired_reader}) {
        for (int flags : {0, 1, 2}) {
            int error = 0;
            auto* file = devfs::devfs_open_path(caller, PATH, flags, 0, &error);
            bool const ALLOWED = flags == 1 || caller == ROOT || caller == reader;
            passed &= ALLOWED ? file != nullptr && error == 0 : file == nullptr && error == -EACCES;
            if (file != nullptr) {
                passed &= file->pos == NO_SEQUENCE;
                ker::vfs::vfs_put_file(file);
            }
            passed &= node->pts_refs.load() == REFS;
        }
    }
    passed &= devfs::devfs_remove_node(PATH);
    return passed;
}

auto selftest_read_authorization() -> bool {
    if (s_journal_ops.open != nullptr || s_journal_ops.open_with_credentials == nullptr) return false;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto user = ROOT;
    auto limited = ROOT;
    bool ok = ROOT.prepare({.change = security::Change::SET_UID, .real = 1000}, user) == 0 &&
              ROOT.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, limited) == 0;
    ker::vfs::File file{};
    std::array<uint8_t, sizeof(JournalRecord)> output{};
    for (auto const& denied : {security::Snapshot{}, user, limited}) {
        for (int flags : {0, 2}) {
            file.open_flags = flags;
            file.pos = 123;
            ok &= s_journal_ops.open_with_credentials(&file, denied, nullptr) == -EACCES && file.pos == 123;
        }
        for (const char* filter : {static_cast<const char*>(nullptr), "kernel", "userspace"}) {
            for (size_t count : {size_t{0}, size_t{1}, output.size()}) {
                output.fill(0xA5);
                file.pos = 123;
                ok &= read_records_with_credentials(denied, &file, output.data(), count, filter) == -EACCES && file.pos == 123;
                ok &= std::all_of(output.begin(), output.end(), [](uint8_t byte) { return byte == 0xA5; });
            }
            ok &= poll_check_with_credentials(denied, &file, EPOLLIN_VALUE, filter) == EPOLLERR_VALUE;
            ok &= poll_check_with_credentials(denied, &file, EPOLLIN_VALUE | EPOLLOUT_VALUE, filter) == (EPOLLERR_VALUE | EPOLLOUT_VALUE);
            ok &= poll_check_with_credentials(denied, &file, EPOLLOUT_VALUE, filter) == EPOLLOUT_VALUE;
        }
        size_t const WAITERS = s_poll_waiters.size();
        ok &= !poll_register_waiter_with_credentials(denied, UINT64_MAX - 12) && s_poll_waiters.size() == WAITERS;
        file.open_flags = 1;
        ok &= s_journal_ops.open_with_credentials(&file, denied, nullptr) == 0 && file.pos == NO_SEQUENCE;
    }
    emit(LogLevel::INFO, "credential_journal", "credential journal fixture", 0);
    file.open_flags = 0;
    ok &= s_journal_ops.open_with_credentials(&file, ROOT, nullptr) == 0;
    ok &= poll_check_with_credentials(ROOT, &file, EPOLLIN_VALUE, "credential_journal") == EPOLLIN_VALUE;
    JournalRecord record{};
    ok &= read_records_with_credentials(ROOT, &file, &record, sizeof(record), "credential_journal") == sizeof(record) &&
          record.magic == JOURNAL_RECORD_MAGIC && std::strcmp(record.message, "credential journal fixture") == 0;
    return ok;
}
#endif

}  // namespace ker::mod::dbg::journal
