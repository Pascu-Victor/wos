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
#include <string_view>
#include <utility>
#include <vector>

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

auto read_journal_record(int fd, JournalRecord& rec) -> bool {
    auto* out = reinterpret_cast<char*>(&rec);
    size_t done = 0;
    while (done < sizeof(rec)) {
        ssize_t const N = read(fd, out + done, sizeof(rec) - done);
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

auto read_journal_batch(int fd, std::array<JournalRecord, 16>& batch, size_t& count) -> bool {
    count = 0;
    for (;;) {
        ssize_t const N = read(fd, batch.data(), batch.size() * sizeof(JournalRecord));
        if (N < 0 && errno == EINTR) {
            continue;
        }
        if (N <= 0) {
            return false;
        }
        count = static_cast<size_t>(N) / sizeof(JournalRecord);
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

void load_records_from_fd(int fd, std::vector<JournalRecord>& records) {
    JournalRecord rec{};
    for (;;) {
        if (!read_journal_record(fd, rec)) {
            break;
        }
        if (valid_record(rec)) {
            records.push_back(rec);
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
    constexpr std::string_view USAGE = "usage: journalctl [-k] [-p level] [-u module|-m module] [-n count] [-f] [--since usec]\n";
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
        } else {
            return false;
        }
    }
    return true;
}

auto run_query(const Options& opts) -> int {
    std::vector<JournalRecord> records;
    uint64_t persisted_boot = 0;
    uint64_t persisted_latest = 0;

    int const FILE = open(JOURNAL_FILE, O_RDONLY);
    if (FILE >= 0) {
        load_records_from_fd(FILE, records);
        close(FILE);
        for (const auto& rec : records) {
            persisted_boot = rec.boot_id;
            persisted_latest = std::max(persisted_latest, rec.sequence);
        }
    }

    int const DEV = open(JOURNAL_DEVICE, O_RDONLY);
    if (DEV >= 0) {
        std::vector<JournalRecord> live;
        load_records_from_fd(DEV, live);
        for (const auto& rec : live) {
            if (rec.boot_id == persisted_boot && rec.sequence <= persisted_latest) {
                continue;
            }
            records.push_back(rec);
        }
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
        if (!print_record(*it)) {
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
            if (!read_journal_batch(DEV, batch, records)) {
                sleep_short();
                continue;
            }
            for (size_t i = 0; i < records; i++) {
                const auto& rec = *std::next(batch.begin(), static_cast<ptrdiff_t>(i));
                if (record_matches(rec, opts)) {
                    if (!print_record(rec)) {
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
    if (opts.daemon) {
        return run_daemon();
    }
    return run_query(opts);
}
