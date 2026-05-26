#pragma once
#include <cstddef>
#include <cstdint>

namespace ker::abi::sys_log {
// NOLINTBEGIN(performance-enum-size)
enum class sys_log_ops : uint64_t {
    LOG,
    LOG_LINE,
    LOG_EX,
};

enum class sys_log_device : uint64_t {
    SERIAL,
    VGA,
};

enum class sys_log_level : uint64_t {
    TRACE = 0,
    DEBUG = 1,
    INFO = 2,
    NOTICE = 3,
    WARN = 4,
    ERROR = 5,
    CRITICAL = 6,
    PANIC = 7,
};
// NOLINTEND(performance-enum-size)
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
    // Raw arrays preserve the packed syscall ABI and C-header field shape.
    uint8_t reserved0[3];  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    uint32_t flags;
    char module[JOURNAL_MODULE_MAX];  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    uint16_t message_len;
    uint16_t reserved1;
    char message[JOURNAL_MESSAGE_MAX];  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
};

static_assert(sizeof(JournalRecord) == 608, "JournalRecord ABI size changed");
static_assert(offsetof(JournalRecord, magic) == 0, "JournalRecord::magic offset changed");
static_assert(offsetof(JournalRecord, version) == 4, "JournalRecord::version offset changed");
static_assert(offsetof(JournalRecord, header_size) == 6, "JournalRecord::header_size offset changed");
static_assert(offsetof(JournalRecord, sequence) == 8, "JournalRecord::sequence offset changed");
static_assert(offsetof(JournalRecord, boot_id) == 16, "JournalRecord::boot_id offset changed");
static_assert(offsetof(JournalRecord, monotonic_us) == 24, "JournalRecord::monotonic_us offset changed");
static_assert(offsetof(JournalRecord, pid) == 32, "JournalRecord::pid offset changed");
static_assert(offsetof(JournalRecord, tid) == 40, "JournalRecord::tid offset changed");
static_assert(offsetof(JournalRecord, cpu) == 48, "JournalRecord::cpu offset changed");
static_assert(offsetof(JournalRecord, level) == 52, "JournalRecord::level offset changed");
static_assert(offsetof(JournalRecord, reserved0) == 53, "JournalRecord::reserved0 offset changed");
static_assert(offsetof(JournalRecord, flags) == 56, "JournalRecord::flags offset changed");
static_assert(offsetof(JournalRecord, module) == 60, "JournalRecord::module offset changed");
static_assert(offsetof(JournalRecord, message_len) == 92, "JournalRecord::message_len offset changed");
static_assert(offsetof(JournalRecord, reserved1) == 94, "JournalRecord::reserved1 offset changed");
static_assert(offsetof(JournalRecord, message) == 96, "JournalRecord::message offset changed");
}  // namespace ker::abi::sys_log
