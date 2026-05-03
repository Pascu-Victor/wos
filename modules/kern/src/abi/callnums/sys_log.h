#pragma once
#include <cstdint>

namespace ker::abi::sys_log {
enum class sys_log_ops : uint64_t {
    log,
    logLine,
    logEx,
};

enum class sys_log_device : uint64_t {
    serial,
    vga,
};

enum class sys_log_level : uint64_t {
    trace = 0,
    debug = 1,
    info = 2,
    notice = 3,
    warn = 4,
    error = 5,
    critical = 6,
    panic = 7,
};

constexpr uint32_t JOURNAL_RECORD_MAGIC = 0x4a4f5753;
constexpr uint16_t JOURNAL_RECORD_VERSION = 1;
constexpr uint64_t JOURNAL_MODULE_MAX = 32;
constexpr uint64_t JOURNAL_MESSAGE_MAX = 512;

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
}  // namespace ker::abi::sys_log
