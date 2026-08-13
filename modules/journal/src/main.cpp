#include <abi-bits/fcntl.h>
#include <bits/off_t.h>
#include <bits/ssize_t.h>
#include <fcntl.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): mlibc exposes POSIX nanosleep here.
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <wos/telemetry.hpp>

#include "callnums/sys_log.h"

namespace {

using ker::abi::sys_log::JournalRecord;

constexpr const char* JOURNAL_DEVICE = "/dev/journal";
constexpr const char* JOURNAL_FILE = "/var/log/journal/wos.journal";
constexpr const char* JOURNAL_FILE_OLD = "/var/log/journal/wos.journal.1";
constexpr off_t ROTATE_BYTES = static_cast<off_t>(8) * 1024 * 1024;
constexpr uint32_t FLAG_KERNEL = 1U << 1;
constexpr uint16_t JOURNAL_HEADER_SIZE = sizeof(JournalRecord) - ker::abi::sys_log::JOURNAL_MESSAGE_MAX;

auto base_name(const char* path) -> const char* {
    if (path == nullptr) {
        return "";
    }
    const char* last = path;
    for (const char* p = path; *p != '\0'; p++) {
        if (*p == '/') {
            last = p + 1;
        }
    }
    return last;
}

auto level_name(uint8_t level) -> const char* {
    switch (level) {
        case 0:
            return "trace";
        case 1:
            return "debug";
        case 2:
            return "info";
        case 3:
            return "notice";
        case 4:
            return "warn";
        case 5:
            return "error";
        case 6:
            return "critical";
        case 7:
            return "panic";
        default:
            return "unknown";
    }
}

auto parse_level(const char* text, uint8_t* out) -> bool {
    if (text == nullptr || out == nullptr) {
        return false;
    }
    if (text[0] >= '0' && text[0] <= '7' && text[1] == '\0') {
        *out = static_cast<uint8_t>(text[0] - '0');
        return true;
    }
    struct Pair {
        const char* name;
        uint8_t level;
    };
    constexpr std::array<Pair, 11> PAIRS{{
        {.name = "trace", .level = 0},
        {.name = "debug", .level = 1},
        {.name = "info", .level = 2},
        {.name = "notice", .level = 3},
        {.name = "warn", .level = 4},
        {.name = "warning", .level = 4},
        {.name = "err", .level = 5},
        {.name = "error", .level = 5},
        {.name = "crit", .level = 6},
        {.name = "critical", .level = 6},
        {.name = "panic", .level = 7},
    }};
    return std::ranges::any_of(PAIRS, [&](const auto& pair) {
        if (std::strcmp(text, pair.name) == 0) {
            *out = pair.level;
            return true;
        }
        return false;
    });
}

void sleep_short() {
    timespec remaining{
        .tv_sec = 0,
        .tv_nsec = 200L * 1000L * 1000L,
    };
    while (nanosleep(&remaining, &remaining) < 0 && errno == EINTR) {
    }
}

auto write_all(int fd, const void* data, size_t len) -> bool {
    const auto* p = static_cast<const char*>(data);
    size_t done = 0;
    while (done < len) {
        ssize_t const N = write(fd, p + done, len - done);
        if (N < 0 && errno == EINTR) {
            continue;
        }
        if (N <= 0) {
            return false;
        }
        done += static_cast<size_t>(N);
    }
    return true;
}

auto bounded_string_length(const char* text, size_t limit) -> size_t {
    if (text == nullptr) {
        return 0;
    }
    size_t len = 0;
    while (len < limit && text[len] != '\0') {
        len++;
    }
    return len;
}

void report_errno(const char* context) {
    int const saved_errno = errno;
    std::fprintf(stderr, "%s: %s\n", context, std::strerror(saved_errno));
}

auto valid_record(const JournalRecord& rec) -> bool {
    if (rec.magic != ker::abi::sys_log::JOURNAL_RECORD_MAGIC || rec.version != ker::abi::sys_log::JOURNAL_RECORD_VERSION ||
        rec.header_size != JOURNAL_HEADER_SIZE) {
        return false;
    }
    if (rec.level > 7 || rec.message_len >= ker::abi::sys_log::JOURNAL_MESSAGE_MAX) {
        return false;
    }
    if (bounded_string_length(rec.module, ker::abi::sys_log::JOURNAL_MODULE_MAX) >= ker::abi::sys_log::JOURNAL_MODULE_MAX) {
        return false;
    }
    return bounded_string_length(rec.message, static_cast<size_t>(rec.message_len) + 1) == rec.message_len;
}

auto read_journal_record(int fd, JournalRecord& rec, bool* partial_record = nullptr) -> bool {
    if (partial_record != nullptr) {
        *partial_record = false;
    }
    auto* out = reinterpret_cast<char*>(&rec);
    size_t done = 0;
    while (done < sizeof(rec)) {
        ssize_t const N = read(fd, out + done, sizeof(rec) - done);
        if (N < 0 && errno == EINTR) {
            continue;
        }
        if (N <= 0) {
            if (partial_record != nullptr && done != 0) {
                *partial_record = true;
            }
            return false;
        }
        done += static_cast<size_t>(N);
    }
    return true;
}

auto read_journal_batch(int fd, std::array<JournalRecord, 16>& batch, size_t& count, bool* malformed_batch = nullptr) -> bool {
    count = 0;
    if (malformed_batch != nullptr) {
        *malformed_batch = false;
    }
    for (;;) {
        ssize_t const N = read(fd, batch.data(), batch.size() * sizeof(JournalRecord));
        if (N < 0 && errno == EINTR) {
            continue;
        }
        if (N <= 0) {
            return false;
        }
        const size_t BYTES = static_cast<size_t>(N);
        if (malformed_batch != nullptr && (BYTES % sizeof(JournalRecord)) != 0) {
            *malformed_batch = true;
        }
        count = BYTES / sizeof(JournalRecord);
        return count > 0;
    }
}

struct Options {
    bool daemon = false;
    bool follow = false;
    bool kernel_only = false;
    uint8_t min_level = 0;
    const char* module = nullptr;
    size_t tail = 0;
    uint64_t since_us = 0;
    bool structured = false;
};

auto record_matches(const JournalRecord& rec, const Options& opts) -> bool {
    if (!valid_record(rec)) {
        return false;
    }
    if (rec.level < opts.min_level) {
        return false;
    }
    if (opts.kernel_only && (rec.flags & FLAG_KERNEL) == 0) {
        return false;
    }
    if (opts.module != nullptr && opts.module[0] != '\0' &&
        std::strcmp(rec.module, opts.module) != 0) {  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay): ABI record field.
        return false;
    }
    if (opts.since_us != 0 && rec.monotonic_us < opts.since_us) {
        return false;
    }
    return true;
}

auto print_record(const JournalRecord& rec) -> bool {
    size_t const MODULE_LEN = bounded_string_length(rec.module, ker::abi::sys_log::JOURNAL_MODULE_MAX);
    std::array<char, 96 + ker::abi::sys_log::JOURNAL_MESSAGE_MAX + 1> line{};
    int const prefix_len = std::snprintf(line.data(), line.size(), "[%llu.%03llu] %-8s %-16.*s ",
                                         static_cast<unsigned long long>(rec.monotonic_us / 1000000ULL),
                                         static_cast<unsigned long long>((rec.monotonic_us / 1000ULL) % 1000ULL), level_name(rec.level),
                                         static_cast<int>(MODULE_LEN), rec.module);
    if (prefix_len < 0 || static_cast<size_t>(prefix_len) >= line.size()) {
        return false;
    }
    size_t cursor = static_cast<size_t>(prefix_len);
    if (cursor + static_cast<size_t>(rec.message_len) + 1 > line.size()) {
        return false;
    }
    std::memcpy(line.data() + cursor, rec.message, rec.message_len);
    cursor += static_cast<size_t>(rec.message_len);
    line[cursor++] = '\n';
    return write_all(STDOUT_FILENO, line.data(), cursor);
}

auto node_identity() -> const std::string& {
    static const std::string NAME = [] {
        std::array<char, 256> hostname{};
        if (gethostname(hostname.data(), hostname.size() - 1) != 0 || hostname.front() == '\0') {
            return std::string{};
        }
        return std::string(hostname.data(), bounded_string_length(hostname.data(), hostname.size()));
    }();
    return NAME;
}

auto hex_bytes(const void* data, size_t size) -> std::string {
    constexpr std::string_view DIGITS = "0123456789abcdef";
    const auto* bytes = static_cast<const unsigned char*>(data);
    std::string result(size * 2, '0');
    for (size_t index = 0; index < size; ++index) {
        result[index * 2] = DIGITS[bytes[index] >> 4U];
        result[index * 2 + 1] = DIGITS[bytes[index] & 0x0fU];
    }
    return result;
}

auto telemetry_text(std::string_view text, bool& valid_utf8) -> wos::telemetry::Value {
    const auto probe = wos::telemetry::serialize(wos::telemetry::Value(text));
    valid_utf8 = static_cast<bool>(probe);
    return valid_utf8 ? wos::telemetry::Value(text) : wos::telemetry::Value(nullptr);
}

auto structured_record(const JournalRecord& rec) -> std::optional<std::string> {
    namespace telemetry = wos::telemetry;

    telemetry::Value::Object identity{
        {"boot_id", telemetry::Value::unsigned_integer(rec.boot_id)},
        {"pid", telemetry::Value::unsigned_integer(rec.pid)},
        {"tid", telemetry::Value::unsigned_integer(rec.tid)},
        {"cpu", telemetry::Value::number(telemetry::decimal_u64_string(rec.cpu))},
    };
    if (!node_identity().empty()) {
        identity.emplace("node_id", node_identity());
    }
    telemetry::Value::Object clock{
        {"domain", telemetry::Value("boot_monotonic")},
        {"value", telemetry::Value::unsigned_integer(rec.monotonic_us)},
        {"unit", telemetry::Value("us")},
        {"quality", telemetry::Value("local")},
    };
    telemetry::Value::Object correlation{
        {"journal_sequence", telemetry::Value::unsigned_integer(rec.sequence)},
    };
    telemetry::Value::Array reserved0;
    reserved0.reserve(std::size(rec.reserved0));
    for (const auto value : rec.reserved0) {
        reserved0.push_back(telemetry::Value::number(telemetry::decimal_u64_string(value)));
    }
    const std::string module(rec.module, bounded_string_length(rec.module, ker::abi::sys_log::JOURNAL_MODULE_MAX));
    const std::string message(rec.message, rec.message_len);
    bool module_utf8 = false;
    bool message_utf8 = false;
    auto module_value = telemetry_text(module, module_utf8);
    auto message_value = telemetry_text(message, message_utf8);
    telemetry::Value::Object payload{
        {"magic", telemetry::Value::unsigned_integer(rec.magic)},
        {"record_version", telemetry::Value::number(telemetry::decimal_u64_string(rec.version))},
        {"header_size", telemetry::Value::number(telemetry::decimal_u64_string(rec.header_size))},
        {"sequence", telemetry::Value::unsigned_integer(rec.sequence)},
        {"boot_id", telemetry::Value::unsigned_integer(rec.boot_id)},
        {"monotonic_us", telemetry::Value::unsigned_integer(rec.monotonic_us)},
        {"pid", telemetry::Value::unsigned_integer(rec.pid)},
        {"tid", telemetry::Value::unsigned_integer(rec.tid)},
        {"cpu", telemetry::Value::number(telemetry::decimal_u64_string(rec.cpu))},
        {"level", telemetry::Value::number(telemetry::decimal_u64_string(rec.level))},
        {"level_name", telemetry::Value(level_name(rec.level))},
        {"reserved0", telemetry::Value(std::move(reserved0))},
        {"flags", telemetry::Value::unsigned_integer(rec.flags)},
        {"module", std::move(module_value)},
        {"module_utf8", telemetry::Value(module_utf8)},
        {"module_bytes_hex", telemetry::Value(hex_bytes(module.data(), module.size()))},
        {"message_len", telemetry::Value::number(telemetry::decimal_u64_string(rec.message_len))},
        {"reserved1", telemetry::Value::number(telemetry::decimal_u64_string(rec.reserved1))},
        {"message", std::move(message_value)},
        {"message_utf8", telemetry::Value(message_utf8)},
        {"message_bytes_hex", telemetry::Value(hex_bytes(message.data(), message.size()))},
        {"raw_record_hex", telemetry::Value(hex_bytes(&rec, sizeof(rec)))},
    };
    auto encoded = telemetry::serialize(telemetry::make_envelope("journal", rec.version, "journal.record", std::move(identity),
                                                                 std::move(clock), std::move(correlation), std::move(payload)));
    if (!encoded) {
        return std::nullopt;
    }
    encoded.text->push_back('\n');
    return std::move(*encoded.text);
}

auto emit_record(const JournalRecord& rec, const Options& opts) -> bool {
    if (!opts.structured) {
        return print_record(rec);
    }
    auto line = structured_record(rec);
    return line && write_all(STDOUT_FILENO, line->data(), line->size());
}

void load_records_from_fd(int fd, std::vector<JournalRecord>& records, bool* invalid_seen = nullptr) {
    JournalRecord rec{};
    for (;;) {
        bool partial_record = false;
        if (!read_journal_record(fd, rec, &partial_record)) {
            if (partial_record && invalid_seen != nullptr) {
                *invalid_seen = true;
            }
            break;
        }
        if (valid_record(rec)) {
            records.push_back(rec);
        } else if (invalid_seen != nullptr) {
            *invalid_seen = true;
        }
    }
}

auto open_journal_file_append() -> int {
    int fd = open(JOURNAL_FILE, O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (fd < 0) {
        return fd;
    }
    off_t const END = lseek(fd, 0, SEEK_END);
    if (END >= ROTATE_BYTES) {
        close(fd);
        unlink(JOURNAL_FILE_OLD);
        rename(JOURNAL_FILE, JOURNAL_FILE_OLD);
        fd = open(JOURNAL_FILE, O_CREAT | O_WRONLY | O_APPEND, 0644);
    }
    if (fd >= 0) {
        lseek(fd, 0, SEEK_END);
    }
    return fd;
}

auto persist_record(int& fd, const JournalRecord& rec) -> bool {
    if (write_all(fd, &rec, sizeof(rec))) {
        return true;
    }

    close(fd);
    fd = open_journal_file_append();
    if (fd < 0) {
        return false;
    }
    return write_all(fd, &rec, sizeof(rec));
}

auto run_daemon() -> int {
    int const DEV = open(JOURNAL_DEVICE, O_RDONLY);
    if (DEV < 0) {
        return 1;
    }

    int out = open_journal_file_append();

    for (;;) {
        std::array<JournalRecord, 16> batch{};
        size_t records = 0;
        if (!read_journal_batch(DEV, batch, records)) {
            sleep_short();
            continue;
        }
        for (size_t i = 0; i < records; i++) {
            const auto& rec = *std::next(batch.begin(), static_cast<ptrdiff_t>(i));
            if (!valid_record(rec)) {
                continue;
            }
            if (out < 0) {
                out = open_journal_file_append();
                if (out < 0) {
                    continue;
                }
            }
            off_t const POS = lseek(out, 0, SEEK_END);
            if (POS >= ROTATE_BYTES) {
                close(out);
                unlink(JOURNAL_FILE_OLD);
                rename(JOURNAL_FILE, JOURNAL_FILE_OLD);
                out = open_journal_file_append();
                if (out < 0) {
                    close(DEV);
                    return 1;
                }
            }
            if (!persist_record(out, rec)) {
                close(out);
                out = -1;
            }
        }
    }
}

void usage() {
    constexpr std::string_view USAGE =
        "usage: journalctl [-k] [-p level] [-u module|-m module] [-n count] [-f] [--since usec] [--structured]\n";
    (void)write_all(STDOUT_FILENO, USAGE.data(), USAGE.size());
}

auto parse_args(int argc, char** argv, Options& opts) -> bool {
    for (int i = 1; i < argc; i++) {
        std::string_view const ARG(argv[i]);
        if (ARG == "--daemon") {
            opts.daemon = true;
        } else if (ARG == "-f") {
            opts.follow = true;
        } else if (ARG == "-k") {
            opts.kernel_only = true;
        } else if ((ARG == "-p") && i + 1 < argc) {
            if (!parse_level(argv[++i], &opts.min_level)) {
                return false;
            }
        } else if ((ARG == "-u" || ARG == "-m") && i + 1 < argc) {
            opts.module = argv[++i];
        } else if (ARG == "-n" && i + 1 < argc) {
            opts.tail = static_cast<size_t>(strtoull(argv[++i], nullptr, 10));
        } else if (ARG == "--since" && i + 1 < argc) {
            opts.since_us = static_cast<uint64_t>(strtoull(argv[++i], nullptr, 10));
        } else if (ARG == "--structured") {
            opts.structured = true;
        } else {
            return false;
        }
    }
    return true;
}

auto run_query(const Options& opts) -> int {
    std::vector<JournalRecord> records;
    bool invalid_seen = false;
    uint64_t persisted_boot = 0;
    uint64_t persisted_latest = 0;

    int const FILE = open(JOURNAL_FILE, O_RDONLY);
    if (FILE >= 0) {
        load_records_from_fd(FILE, records, &invalid_seen);
        close(FILE);
        for (const auto& rec : records) {
            persisted_boot = rec.boot_id;
            persisted_latest = std::max(persisted_latest, rec.sequence);
        }
    }

    int const DEV = open(JOURNAL_DEVICE, O_RDONLY);
    if (DEV >= 0) {
        std::vector<JournalRecord> live;
        load_records_from_fd(DEV, live, &invalid_seen);
        for (const auto& rec : live) {
            if (rec.boot_id == persisted_boot && rec.sequence <= persisted_latest) {
                continue;
            }
            records.push_back(rec);
        }
    }

    if (opts.structured && invalid_seen) {
        std::fprintf(stderr, "journalctl: structured export rejected an unsupported or malformed JournalRecord\n");
        if (DEV >= 0) {
            close(DEV);
        }
        return 1;
    }

    std::vector<JournalRecord> filtered;
    for (const auto& rec : records) {
        if (record_matches(rec, opts)) {
            filtered.push_back(rec);
        }
    }

    size_t start = 0;
    if (opts.tail != 0 && filtered.size() > opts.tail) {
        start = filtered.size() - opts.tail;
    }
    for (auto it = std::next(filtered.cbegin(), static_cast<ptrdiff_t>(start)); it != filtered.cend(); ++it) {
        if (!emit_record(*it, opts)) {
            report_errno("journalctl: failed to write output");
            if (DEV >= 0) {
                close(DEV);
            }
            return 1;
        }
    }

    if (opts.follow && DEV >= 0) {
        for (;;) {
            std::array<JournalRecord, 16> batch{};
            size_t records = 0;
            bool malformed_batch = false;
            if (!read_journal_batch(DEV, batch, records, &malformed_batch)) {
                if (opts.structured && malformed_batch) {
                    std::fprintf(stderr, "journalctl: structured follow rejected a partial JournalRecord batch\n");
                    close(DEV);
                    return 1;
                }
                sleep_short();
                continue;
            }
            if (opts.structured && malformed_batch) {
                std::fprintf(stderr, "journalctl: structured follow rejected a partial JournalRecord batch\n");
                close(DEV);
                return 1;
            }
            for (size_t i = 0; i < records; i++) {
                const auto& rec = *std::next(batch.begin(), static_cast<ptrdiff_t>(i));
                if (opts.structured && !valid_record(rec)) {
                    std::fprintf(stderr, "journalctl: structured follow rejected an unsupported or malformed JournalRecord\n");
                    close(DEV);
                    return 1;
                }
                if (record_matches(rec, opts)) {
                    if (!emit_record(rec, opts)) {
                        report_errno("journalctl: failed to write output");
                        close(DEV);
                        return 1;
                    }
                }
            }
        }
    }

    if (DEV >= 0) {
        close(DEV);
    }
    return 0;
}

}  // namespace

int main(int argc, char** argv) {  // NOLINT(bugprone-exception-escape): userspace entry point uses STL formatting/allocation.
    Options opts{};
    if (std::strcmp(base_name(argv[0]), "journald") == 0) {
        opts.daemon = true;
    }
    if (!parse_args(argc, argv, opts)) {
        usage();
        return 1;
    }
    if (opts.daemon && opts.structured) {
        usage();
        return 1;
    }
    if (opts.daemon) {
        return run_daemon();
    }
    return run_query(opts);
}
