#pragma once

#include <sys/types.h>

#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <platform/dbg/dbg.hpp>

namespace ker::vfs {
struct File;
}

namespace ker::mod::dbg::journal {

constexpr uint32_t JOURNAL_RECORD_MAGIC = 0x4a4f5753;  // "JOWS"
constexpr uint16_t JOURNAL_RECORD_VERSION = 1;
constexpr size_t JOURNAL_MODULE_MAX = 32;
constexpr size_t JOURNAL_MESSAGE_MAX = 512;
constexpr uint32_t JOURNAL_FLAG_TRUNCATED = 1U << 0;
constexpr uint32_t JOURNAL_FLAG_KERNEL = 1U << 1;
constexpr uint32_t JOURNAL_FLAG_PREFIX_COMPAT = 1U << 2;
constexpr uint32_t JOURNAL_OUTPUT_NONE = 0;
constexpr uint32_t JOURNAL_OUTPUT_SERIAL = 1U << 0;

// On-disk/userspace ABI: raw fixed arrays are intentional and covered by the
// size assertion below.
// NOLINTBEGIN(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
struct __attribute__((packed)) JournalRecord {
    uint32_t magic;
    uint16_t version;
    uint16_t header_size;
    uint64_t sequence;
    uint64_t boot_id;
    uint64_t monotonic_us;
    uint64_t pid;
    uint64_t tid;
    uint32_t cpu;
    uint8_t level;
    uint8_t reserved0[3];
    uint32_t flags;
    char module[JOURNAL_MODULE_MAX];
    uint16_t message_len;
    uint16_t reserved1;
    char message[JOURNAL_MESSAGE_MAX];
};
// NOLINTEND(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

static_assert(sizeof(JournalRecord) == 608, "JournalRecord ABI size changed");

void init();
void enable_time();
void register_devices();
void mark_devfs_ready();
void set_serial_threshold(LogLevel level);
auto get_serial_threshold() -> LogLevel;
void set_module_output_devices(const char* module, uint32_t devices);
auto get_module_output_devices(const char* module) -> uint32_t;
void emit(LogLevel level, const char* module, const char* message, uint32_t flags);
void emit_v(LogLevel level, const char* module, const char* format, va_list args, uint32_t flags);
auto read_records(ker::vfs::File* file, void* buf, size_t count, const char* module_filter) -> ssize_t;
auto poll_check(ker::vfs::File* file, int events, const char* module_filter) -> int;
auto poll_register_waiter(ker::vfs::File* file, uint64_t pid) -> bool;

}  // namespace ker::mod::dbg::journal
