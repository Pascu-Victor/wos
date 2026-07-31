#include "commands.hpp"

#include <time.h>  // NOLINT(modernize-deprecated-headers): POSIX nanosleep is declared here.

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "options.hpp"
#include "processes.hpp"
#include "procfs_io.hpp"
#include "reports.hpp"
#include "rows.hpp"

namespace memacc {
namespace {

struct WatchSnapshot {
    uint64_t free_bytes = 0;
    uint64_t used_bytes = 0;
    uint64_t tracked_physical_bytes = 0;
    std::unordered_map<uint64_t, uint64_t> proc_bytes;
    std::unordered_map<uint64_t, std::string> proc_names;
};

auto collect_watch_snapshot() -> WatchSnapshot {
    WatchSnapshot snap;
    auto summary_rows = read_rows("summary");
    if (const Row* summary = first_record(summary_rows, "summary"); summary != nullptr) {
        snap.free_bytes = get_u64(*summary, "free_bytes");
        snap.used_bytes = get_u64(*summary, "used_bytes");
        snap.tracked_physical_bytes = get_u64(*summary, "owner_bytes") + get_u64(*summary, "allocator_metadata_bytes") +
                                      get_u64(*summary, "zone_descriptor_bytes") + get_u64(*summary, "per_cpu_page_cache_reserve_bytes");
    }
    for (const auto& proc : load_procs()) {
        snap.proc_bytes[proc.pid] = proc.total_bytes;
        snap.proc_names[proc.pid] = proc.name;
    }
    return snap;
}

void sleep_seconds(int seconds) {
    timespec req{.tv_sec = seconds, .tv_nsec = 0};
    while (nanosleep(&req, &req) != 0 && errno == EINTR) {
    }
}

void print_delta_line(const char* label, uint64_t old_value, uint64_t new_value) {
    int64_t const DELTA = static_cast<int64_t>(new_value) - static_cast<int64_t>(old_value);
    std::printf("%-12s %12llu KiB  delta %+lld KiB\n", label, static_cast<unsigned long long>(bytes_to_kib(new_value)),
                static_cast<long long>(DELTA / static_cast<int64_t>(KIB)));
}

void run_watch(const Options& opt) {
    std::optional<WatchSnapshot> prev;
    while (true) {
        WatchSnapshot cur = collect_watch_snapshot();
        std::printf("\x1b[2J\x1b[H");
        std::printf("memacc watch interval=%d\n\n", opt.interval_seconds);
        if (prev.has_value()) {
            print_delta_line("free", prev->free_bytes, cur.free_bytes);
            print_delta_line("used", prev->used_bytes, cur.used_bytes);
            print_delta_line("tracked phys", prev->tracked_physical_bytes, cur.tracked_physical_bytes);

            struct Mover {
                uint64_t pid;
                int64_t delta;
                uint64_t now;
                std::string name;
            };
            std::vector<Mover> movers;
            for (const auto& [pid, now] : cur.proc_bytes) {
                uint64_t const OLD = prev->proc_bytes.contains(pid) ? prev->proc_bytes.at(pid) : 0;
                int64_t const DELTA = static_cast<int64_t>(now) - static_cast<int64_t>(OLD);
                if (DELTA != 0) {
                    movers.push_back(Mover{.pid = pid, .delta = DELTA, .now = now, .name = cur.proc_names[pid]});
                }
            }
            std::sort(movers.begin(), movers.end(), [](const Mover& a, const Mover& b) {
                uint64_t const AA = a.delta < 0 ? static_cast<uint64_t>(-a.delta) : static_cast<uint64_t>(a.delta);
                uint64_t const BB = b.delta < 0 ? static_cast<uint64_t>(-b.delta) : static_cast<uint64_t>(b.delta);
                return AA > BB;
            });
            std::printf("\nTop movers\n");
            int shown = 0;
            for (const auto& mover : movers) {
                if (opt.limit > 0 && shown >= opt.limit) {
                    break;
                }
                std::printf("%6llu %+10lld KiB now=%10llu KiB %s\n", static_cast<unsigned long long>(mover.pid),
                            static_cast<long long>(mover.delta / static_cast<int64_t>(KIB)),
                            static_cast<unsigned long long>(bytes_to_kib(mover.now)), mover.name.c_str());
                shown++;
            }
        } else {
            print_delta_line("free", cur.free_bytes, cur.free_bytes);
            print_delta_line("used", cur.used_bytes, cur.used_bytes);
            print_delta_line("tracked phys", cur.tracked_physical_bytes, cur.tracked_physical_bytes);
        }
        prev = std::move(cur);
        sleep_seconds(opt.interval_seconds);
    }
}

auto normalize_feature(std::string_view feature) -> std::string {
    if (feature == "page_callers" || feature == "pages" || feature == "page") {
        return "page_callers";
    }
    if (feature == "kmalloc_debug" || feature == "kmalloc") {
        return "kmalloc_debug";
    }
    return {};
}

}  // namespace

void usage() {
    std::printf(
        "usage: memacc [dump [--full]|summary|procs|proc <pid>|kernel|allocs|raw [file]|watch [-n sec]|track <feature> <action>|reclaim "
        "<buffer_cache|packet_pool|xfs_inode|file_mmap_cache> [target|grow=N]]\n");
}

auto run_dump(int argc, char** argv) -> int {
    print_dump(parse_options(argc, argv, 2));
    return 0;
}

auto run_summary() -> int {
    print_summary();
    return 0;
}

auto run_procs(int argc, char** argv) -> int {
    print_proc_table(filtered_procs(parse_options(argc, argv, 2)));
    return 0;
}

auto run_proc(int argc, char** argv) -> int {
    if (argc < 3) {
        usage();
        return 1;
    }
    Options opt = parse_options(argc, argv, 3);
    opt.pid = parse_u64_arg(argv[2]);
    opt.limit = 0;
    print_proc_table(filtered_procs(opt));
    return 0;
}

auto run_kernel() -> int {
    print_kernel();
    return 0;
}

auto run_allocs(int argc, char** argv) -> int {
    print_alloc_rows();
    Options opt = parse_options(argc, argv, 2);
    if (opt.full) {
        if (auto text = read_file(memacc_path("slabs")); text.has_value()) {
            std::printf("\n%s", text->c_str());
        }
        if (auto text = read_file(memacc_path("kmalloc_callers")); text.has_value()) {
            std::printf("\n%s", text->c_str());
        }
        if (auto text = read_file(memacc_path("kmalloc_live")); text.has_value()) {
            std::printf("\n%s", text->c_str());
        }
        if (auto text = read_file(memacc_path("page_callers")); text.has_value()) {
            std::printf("\n%s", text->c_str());
        }
    }
    return 0;
}

auto run_raw(int argc, char** argv) -> int {
    std::string file = argc >= 3 ? argv[2] : "summary";
    if (file == "all") {
        constexpr std::array<std::string_view, 14> FILES{"summary",
                                                         "zones",
                                                         "procs",
                                                         "dead",
                                                         "alloc_totals",
                                                         "slabs",
                                                         "kmalloc_callers",
                                                         "kmalloc_live",
                                                         "page_callers",
                                                         "features",
                                                         "reclaim/buffer_cache",
                                                         "reclaim/packet_pool",
                                                         "reclaim/xfs_inode",
                                                         "reclaim/file_mmap_cache"};
        for (auto one : FILES) {
            auto text = read_file(memacc_path(one));
            if (text.has_value()) {
                auto const name = std::string(one);
                std::printf("== %s ==\n%s", name.c_str(), text->c_str());
            }
        }
        return 0;
    }
    auto text = read_file(memacc_path(file));
    if (!text.has_value()) {
        std::printf("memacc: failed to read %s\n", memacc_path(file).c_str());
        return 1;
    }
    std::printf("%s", text->c_str());
    return 0;
}

auto run_watch_command(int argc, char** argv) -> int {
    run_watch(parse_options(argc, argv, 2));
    return 0;
}

auto run_track(int argc, char** argv) -> int {
    if (argc < 4) {
        usage();
        return 1;
    }
    std::string feature = normalize_feature(argv[2]);
    if (feature.empty()) {
        std::printf("memacc: unknown feature '%s'\n", argv[2]);
        return 1;
    }
    std::string action = argv[3];
    std::string path = memacc_path(std::string("track/") + feature);
    if (action != "status" && !write_file(path, action)) {
        std::printf("memacc: failed to write %s\n", path.c_str());
        return 1;
    }
    auto text = read_file(path);
    if (!text.has_value()) {
        std::printf("memacc: failed to read %s\n", path.c_str());
        return 1;
    }
    std::printf("%s", text->c_str());
    return 0;
}

auto run_reclaim(int argc, char** argv) -> int {
    if (argc < 3) {
        usage();
        return 1;
    }

    std::string_view target_arg(argv[2]);
    std::string target;
    if (target_arg == "buffer_cache" || target_arg == "bcache" || target_arg == "cache") {
        target = "buffer_cache";
    } else if (target_arg == "packet_pool" || target_arg == "packets" || target_arg == "pkt") {
        target = "packet_pool";
    } else if (target_arg == "xfs_inode" || target_arg == "xfs" || target_arg == "inodes") {
        target = "xfs_inode";
    } else if (target_arg == "file_mmap_cache" || target_arg == "file_mmap" || target_arg == "mmap_cache") {
        target = "file_mmap_cache";
    } else {
        std::printf("memacc: unknown reclaim target '%s'\n", argv[2]);
        return 1;
    }

    std::string const PROC_FILE = std::string("reclaim/") + target;
    std::string path = memacc_path(PROC_FILE);
    auto before_rows = read_rows(PROC_FILE);
    const Row* before = first_record(before_rows, "reclaim");

    std::string command = "drop";
    if (argc >= 4) {
        std::string_view arg(argv[3]);
        if (arg == "drop" || arg == "all") {
            command = std::string(arg);
        } else if (target == "packet_pool" && arg.starts_with("grow=")) {
            uint64_t grow_count = 0;
            if (!parse_u64_arg_strict(arg.substr(5), grow_count) || grow_count == 0) {
                std::printf("memacc: invalid packet growth count '%s'\n", argv[3]);
                return 1;
            }
            command = std::string(arg);
        } else {
            uint64_t target_kib = 0;
            if (!parse_u64_arg_strict(arg, target_kib)) {
                std::printf("memacc: invalid reclaim target '%s'\n", argv[3]);
                return 1;
            }
            uint64_t const TARGET_KIB = target_kib;
            if (TARGET_KIB > UINT64_MAX / KIB) {
                std::printf("memacc: reclaim target is too large\n");
                return 1;
            }
            command = target == "buffer_cache" ? std::to_string(TARGET_KIB * KIB) : std::to_string(TARGET_KIB);
        }
    }

    if (!write_file(path, command)) {
        std::printf("memacc: failed to write %s\n", path.c_str());
        return 1;
    }

    auto after_rows = read_rows(PROC_FILE);
    const Row* after = first_record(after_rows, "reclaim");
    if (after == nullptr) {
        std::printf("memacc: reclaim wrote %s but status is unavailable\n", path.c_str());
        return 1;
    }

    if (target == "buffer_cache") {
        uint64_t const BEFORE_BYTES = before != nullptr ? get_u64(*before, "total_bytes") : 0;
        uint64_t const AFTER_BYTES = get_u64(*after, "total_bytes");
        uint64_t const FREED_BYTES = BEFORE_BYTES >= AFTER_BYTES ? BEFORE_BYTES - AFTER_BYTES : 0;
        std::printf("buffer_cache before=%llu KiB after=%llu KiB freed=%llu KiB clean=%llu KiB dirty=%llu KiB buffers=%llu\n",
                    static_cast<unsigned long long>(bytes_to_kib(BEFORE_BYTES)), static_cast<unsigned long long>(bytes_to_kib(AFTER_BYTES)),
                    static_cast<unsigned long long>(bytes_to_kib(FREED_BYTES)),
                    static_cast<unsigned long long>(bytes_to_kib(get_u64(*after, "clean_bytes"))),
                    static_cast<unsigned long long>(bytes_to_kib(get_u64(*after, "dirty_bytes"))),
                    static_cast<unsigned long long>(get_u64(*after, "buffers")));
    } else if (target == "packet_pool") {
        uint64_t const BEFORE_BYTES = before != nullptr ? get_u64(*before, "total_bytes") : 0;
        uint64_t const BEFORE_CAPACITY = before != nullptr ? get_u64(*before, "capacity") : 0;
        uint64_t const AFTER_BYTES = get_u64(*after, "total_bytes");
        uint64_t const AFTER_CAPACITY = get_u64(*after, "capacity");
        uint64_t const FREED_BYTES = BEFORE_BYTES >= AFTER_BYTES ? BEFORE_BYTES - AFTER_BYTES : 0;
        uint64_t const FREED_BUFFERS = BEFORE_CAPACITY >= AFTER_CAPACITY ? BEFORE_CAPACITY - AFTER_CAPACITY : 0;
        std::printf(
            "packet_pool before=%llu buffers after=%llu buffers freed=%llu buffers freed_bytes=%llu KiB active=%llu free=%llu used=%llu "
            "baseline=%llu draining=%llu draining_free=%llu\n",
            static_cast<unsigned long long>(BEFORE_CAPACITY), static_cast<unsigned long long>(AFTER_CAPACITY),
            static_cast<unsigned long long>(FREED_BUFFERS), static_cast<unsigned long long>(bytes_to_kib(FREED_BYTES)),
            static_cast<unsigned long long>(get_u64(*after, "active_capacity")), static_cast<unsigned long long>(get_u64(*after, "free")),
            static_cast<unsigned long long>(get_u64(*after, "used")), static_cast<unsigned long long>(get_u64(*after, "baseline_capacity")),
            static_cast<unsigned long long>(get_u64(*after, "draining_buffers")),
            static_cast<unsigned long long>(get_u64(*after, "draining_free")));
    } else if (target == "xfs_inode") {
        uint64_t const BEFORE_IDLE = before != nullptr ? get_u64(*before, "idle_inodes") : 0;
        uint64_t const AFTER_IDLE = get_u64(*after, "idle_inodes");
        uint64_t const FREED = BEFORE_IDLE >= AFTER_IDLE ? BEFORE_IDLE - AFTER_IDLE : 0;
        std::printf("xfs_inode before=%llu idle after=%llu idle freed=%llu idle retain_limit=%llu total_reclaim_victims=%llu\n",
                    static_cast<unsigned long long>(BEFORE_IDLE), static_cast<unsigned long long>(AFTER_IDLE),
                    static_cast<unsigned long long>(FREED), static_cast<unsigned long long>(get_u64(*after, "retain_limit")),
                    static_cast<unsigned long long>(get_u64(*after, "reclaim_victims")));
    } else {
        uint64_t const BEFORE_PAGES = before != nullptr ? get_u64(*before, "pages") : 0;
        uint64_t const AFTER_PAGES = get_u64(*after, "pages");
        uint64_t const FREED = BEFORE_PAGES >= AFTER_PAGES ? BEFORE_PAGES - AFTER_PAGES : 0;
        std::printf("file_mmap_cache before=%llu pages after=%llu pages released=%llu pages capacity=%llu pages\n",
                    static_cast<unsigned long long>(BEFORE_PAGES), static_cast<unsigned long long>(AFTER_PAGES),
                    static_cast<unsigned long long>(FREED), static_cast<unsigned long long>(get_u64(*after, "capacity_pages")));
    }
    return 0;
}

}  // namespace memacc
