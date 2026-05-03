#include <fcntl.h>
#include <sys/logging.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <print>
#include <string_view>
#include <vector>

#include "callnums/sys_log.h"

namespace {

using ker::abi::sys_log::JournalRecord;

constexpr const char* JOURNAL_DEVICE = "/dev/journal";
constexpr const char* JOURNAL_FILE = "/var/log/journal/wos.journal";
constexpr const char* JOURNAL_FILE_OLD = "/var/log/journal/wos.journal.1";
constexpr off_t ROTATE_BYTES = 8 * 1024 * 1024;
constexpr uint32_t FLAG_KERNEL = 1U << 1;

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
    constexpr Pair pairs[] = {
        {"trace", 0}, {"debug", 1}, {"info", 2}, {"notice", 3},   {"warn", 4},  {"warning", 4},
        {"err", 5},   {"error", 5}, {"crit", 6}, {"critical", 6}, {"panic", 7},
    };
    for (const auto& pair : pairs) {
        if (std::strcmp(text, pair.name) == 0) {
            *out = pair.level;
            return true;
        }
    }
    return false;
}

void sleep_short() {
    timespec ts{
        .tv_sec = 0,
        .tv_nsec = 200 * 1000 * 1000,
    };
    nanosleep(&ts, nullptr);
}

auto write_all(int fd, const void* data, size_t len) -> bool {
    const auto* p = static_cast<const char*>(data);
    size_t done = 0;
    while (done < len) {
        ssize_t n = write(fd, p + done, len - done);
        if (n <= 0) {
            return false;
        }
        done += static_cast<size_t>(n);
    }
    return true;
}

auto valid_record(const JournalRecord& rec) -> bool {
    return rec.magic == ker::abi::sys_log::JOURNAL_RECORD_MAGIC && rec.version == ker::abi::sys_log::JOURNAL_RECORD_VERSION;
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
    if (opts.module != nullptr && opts.module[0] != '\0' && std::strcmp(rec.module, opts.module) != 0) {
        return false;
    }
    if (opts.since_us != 0 && rec.monotonic_us < opts.since_us) {
        return false;
    }
    return true;
}

void print_record(const JournalRecord& rec) {
    std::println("[{}.{:03}] {:8} {:16} {:.{}}", static_cast<unsigned long long>(rec.monotonic_us / 1000000ULL),
                 ((rec.monotonic_us / 1000ULL) % 1000ULL), level_name(rec.level), rec.module, rec.message,
                 static_cast<int>(rec.message_len));
}

void load_records_from_fd(int fd, std::vector<JournalRecord>& records) {
    JournalRecord rec{};
    for (;;) {
        ssize_t n = read(fd, &rec, sizeof(rec));
        if (n == 0) {
            break;
        }
        if (n != static_cast<ssize_t>(sizeof(rec))) {
            break;
        }
        if (valid_record(rec)) {
            records.push_back(rec);
        }
    }
}

auto open_journal_file_append() -> int {
    int fd = open(JOURNAL_FILE, O_CREAT | O_WRONLY, 0644);
    if (fd < 0) {
        return fd;
    }
    off_t end = lseek(fd, 0, SEEK_END);
    if (end >= ROTATE_BYTES) {
        close(fd);
        unlink(JOURNAL_FILE_OLD);
        rename(JOURNAL_FILE, JOURNAL_FILE_OLD);
        fd = open(JOURNAL_FILE, O_CREAT | O_WRONLY, 0644);
    }
    if (fd >= 0) {
        lseek(fd, 0, SEEK_END);
    }
    return fd;
}

auto run_daemon() -> int {
    int dev = open(JOURNAL_DEVICE, O_RDONLY);
    if (dev < 0) {
        return 1;
    }

    int out = open_journal_file_append();
    if (out < 0) {
        close(dev);
        return 1;
    }

    for (;;) {
        JournalRecord batch[16]{};
        ssize_t n = read(dev, batch, sizeof(batch));
        if (n <= 0) {
            sleep_short();
            continue;
        }
        size_t records = static_cast<size_t>(n) / sizeof(JournalRecord);
        for (size_t i = 0; i < records; i++) {
            if (!valid_record(batch[i])) {
                continue;
            }
            off_t pos = lseek(out, 0, SEEK_END);
            if (pos >= ROTATE_BYTES) {
                close(out);
                unlink(JOURNAL_FILE_OLD);
                rename(JOURNAL_FILE, JOURNAL_FILE_OLD);
                out = open_journal_file_append();
                if (out < 0) {
                    close(dev);
                    return 1;
                }
            }
            write_all(out, &batch[i], sizeof(batch[i]));
        }
    }
}

void usage() { std::printf("usage: journalctl [-k] [-p level] [-u module|-m module] [-n count] [-f] [--since usec]\n"); }

auto parse_args(int argc, char** argv, Options& opts) -> bool {
    for (int i = 1; i < argc; i++) {
        std::string_view arg(argv[i]);
        if (arg == "--daemon") {
            opts.daemon = true;
        } else if (arg == "-f") {
            opts.follow = true;
        } else if (arg == "-k") {
            opts.kernel_only = true;
        } else if ((arg == "-p") && i + 1 < argc) {
            if (!parse_level(argv[++i], &opts.min_level)) {
                return false;
            }
        } else if ((arg == "-u" || arg == "-m") && i + 1 < argc) {
            opts.module = argv[++i];
        } else if (arg == "-n" && i + 1 < argc) {
            opts.tail = static_cast<size_t>(strtoull(argv[++i], nullptr, 10));
        } else if (arg == "--since" && i + 1 < argc) {
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

    int file = open(JOURNAL_FILE, O_RDONLY);
    if (file >= 0) {
        load_records_from_fd(file, records);
        close(file);
        for (const auto& rec : records) {
            persisted_boot = rec.boot_id;
            persisted_latest = std::max(persisted_latest, rec.sequence);
        }
    }

    int dev = open(JOURNAL_DEVICE, O_RDONLY);
    if (dev >= 0) {
        std::vector<JournalRecord> live;
        load_records_from_fd(dev, live);
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
    for (size_t i = start; i < filtered.size(); i++) {
        print_record(filtered[i]);
    }

    if (opts.follow && dev >= 0) {
        for (;;) {
            JournalRecord batch[16]{};
            ssize_t n = read(dev, batch, sizeof(batch));
            if (n <= 0) {
                sleep_short();
                continue;
            }
            size_t count = static_cast<size_t>(n) / sizeof(JournalRecord);
            for (size_t i = 0; i < count; i++) {
                if (record_matches(batch[i], opts)) {
                    print_record(batch[i]);
                }
            }
        }
    }

    if (dev >= 0) {
        close(dev);
    }
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
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
