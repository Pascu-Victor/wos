// Private scheduled CPL3 witness; never a public page-in or policy interface.
#define WOS_SELFTEST 1
#include <fcntl.h>
#include <grp.h>
#include <sys/mman.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <test/anonymous_usercopy_wire.hpp>

namespace {
namespace fixture = ker::test::anonymous_usercopy;
using Control = ker::abi::process::CredentialControl;
constexpr size_t PAGE = 4096;
constexpr char DIRECTORY[] = "/var/lib/anonymous-usercopy";
constexpr char REPORT[] = "/var/lib/anonymous-usercopy/reports.bin";
constexpr char WITNESS[] = "/var/lib/anonymous-usercopy/witness.txt";
constexpr uint64_t DEBUG = uint64_t{1} << static_cast<unsigned>(ker::abi::process::CredentialAuthority::KERNEL_DEBUG);
struct alignas(PAGE) Transport {
    alignas(PAGE) fixture::Request request{};
    alignas(PAGE) fixture::Report report{};
    alignas(PAGE) timespec timeout{};
    alignas(PAGE) std::array<uint8_t, PAGE> bytes{};
};
Transport transport{};
uint64_t deadline{};

auto now() -> uint64_t {
    timespec value{};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 || value.tv_sec < 0 || value.tv_nsec < 0 || value.tv_nsec >= 1'000'000'000)
        return UINT64_MAX;
    uint64_t const SECONDS = static_cast<uint64_t>(value.tv_sec);
    if (SECONDS > (UINT64_MAX - static_cast<uint64_t>(value.tv_nsec)) / 1'000'000'000) return UINT64_MAX;
    return SECONDS * 1'000'000'000 + static_cast<uint64_t>(value.tv_nsec);
}
auto time_left() -> bool {
    uint64_t const CURRENT = now();
    return CURRENT != UINT64_MAX && CURRENT < deadline;
}
auto pattern(size_t index) -> uint8_t { return static_cast<uint8_t>((index * 131 + 0x53) & 255); }
auto replacement(size_t index) -> uint8_t { return static_cast<uint8_t>((index * 29 + 0xa7) & 255); }
auto decimal(const char* text, uint64_t& out) -> bool {
    if (text == nullptr || *text == '\0') return false;
    uint64_t value = 0;
    for (; *text != '\0'; ++text) {
        if (*text < '0' || *text > '9' || value > (UINT64_MAX - static_cast<unsigned>(*text - '0')) / 10) return false;
        value = value * 10 + static_cast<unsigned>(*text - '0');
    }
    if (value == 0) return false;
    out = value;
    return true;
}
auto transfer(int fd, void* data, size_t size, bool writing) -> bool {
    auto* bytes = static_cast<uint8_t*>(data);
    while (size != 0 && time_left()) {
        ssize_t const COUNT = writing ? ::write(fd, bytes, size) : ::read(fd, bytes, size);
        if (COUNT < 0 && errno == EINTR) continue;
        if (COUNT <= 0 || static_cast<size_t>(COUNT) > size) return false;
        bytes += COUNT;
        size -= static_cast<size_t>(COUNT);
    }
    return size == 0;
}
void prefault() {
    auto* bytes = reinterpret_cast<volatile uint8_t*>(&transport);
    for (size_t i = 0; i < sizeof(transport); ++i) {
        uint8_t const VALUE = bytes[i];
        bytes[i] = VALUE;
    }
}
auto identity() -> bool {
    uid_t r{}, e{}, s{};
    gid_t gr{}, ge{}, gs{};
    return ker::process::getpid() > 1 && ker::process::getppid() == 1 && ::getresuid(&r, &e, &s) == 0 && ::getresgid(&gr, &ge, &gs) == 0 &&
           r == 0 && e == 0 && s == 0 && gr == 0 && ge == 0 && gs == 0 && ::getgroups(0, nullptr) == 0 &&
           ker::process::prctl(static_cast<int>(Control::GET_AUTHORITY_CEILING), 0, 0, 0, 0) == static_cast<int64_t>(DEBUG) &&
           ker::process::prctl(static_cast<int>(Control::GET_NO_NEW_PRIVILEGES), 0, 0, 0, 0) == 1 &&
           ker::process::prctl(static_cast<int>(Control::GET_SERVICE_PRIVILEGES), 0, 0, 0, 0) ==
               static_cast<int64_t>(ker::abi::process::SERVICE_PRIVILEGES_LOCKED);
}
auto report_valid(const fixture::Report& row, uint64_t nonce, unsigned index) -> bool {
    int64_t const EXPECTED = index == 1 ? 128 : index == 3 ? -ETIMEDOUT : PAGE;
    return row.version == fixture::VERSION && row.bytes == sizeof(row) && row.nonce == nonce && row.case_id == index &&
           row.actor_pid == static_cast<uint64_t>(ker::process::getpid()) && row.root_serial != 0 && row.checks == 2 && row.valid == 1 &&
           row.operation_result == EXPECTED && row.expected_result == EXPECTED && row.source_ref1 == 1 && row.resident_ref1 == 1 &&
           row.selected_pages == 1 && row.used_before == 0 && row.used_swapped == 1 && row.used_after_copy == 0 && row.total_before == 0 &&
           row.total_active == 16 && row.total_after_cleanup == 0 && row.free_active == 16 && row.used_after_cleanup == 0 &&
           row.block_writes == 1 && row.block_reads == 1 && row.history_overflow == 0 && row.pattern_bytes_checked == 2 * PAGE &&
           row.guard_bytes_checked == PAGE && row.mapping_consumed == 1 && row.cleanup_complete == 1 && row.task_access_after == 0 &&
           row.root_readers_after == 0 && row.futex_waiter_after == 0 && row.reserved == 0;
}
struct CaseOwner {
    void* mapping = MAP_FAILED;
    size_t size{};
    int fd = -1;
    std::array<char, 128> path{};
    bool owns_path{};
    auto cleanup_file() -> bool {
        bool okay = true;
        if (fd >= 0) {
            int const OWNED_FD = fd;
            fd = -1;
            okay = ::close(OWNED_FD) == 0;
        }
        if (owns_path) {
            bool const UNLINKED = ::unlink(path.data()) == 0;
            if (UNLINKED) owns_path = false;
            okay = UNLINKED && okay;
        }
        return okay;
    }
    ~CaseOwner() {
        (void)cleanup_file();
        // The handler may already have consumed it. Never inspect the mapping.
        if (mapping != MAP_FAILED) (void)::munmap(mapping, size);
    }
};
auto run_case(uint64_t nonce, unsigned index, int reports) -> bool {
    if (!time_left()) return false;
    CaseOwner owned;
    owned.size = index == 2 ? 2 * PAGE : PAGE;
    owned.mapping = ::mmap(nullptr, owned.size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (owned.mapping == MAP_FAILED) return false;
    auto* data = static_cast<uint8_t*>(owned.mapping);
    for (size_t i = 0; i < owned.size; ++i) data[i] = pattern(i);
    if (index == 2 && ::mprotect(data + PAGE, PAGE, PROT_NONE) != 0) return false;
    if (index != 3) {
        int const SIZE =
            std::snprintf(owned.path.data(), owned.path.size(), "/tmp/anonymous-usercopy-%llu-%llu-%u",
                          static_cast<unsigned long long>(ker::process::getpid()), static_cast<unsigned long long>(nonce), index);
        if (SIZE <= 0 || static_cast<size_t>(SIZE) >= owned.path.size()) return false;
        owned.fd = ::open(owned.path.data(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (owned.fd < 0) return false;
        owned.owns_path = true;
        if (index == 1) {
            for (size_t i = 0; i < 128; ++i) transport.bytes[i] = replacement(i);
            if (!transfer(owned.fd, transport.bytes.data(), 128, true) || ::lseek(owned.fd, 0, SEEK_SET) != 0) return false;
        }
    }
    transport.report = {};
    transport.timeout = {};
    transport.request = {.version = fixture::VERSION,
                         .bytes = sizeof(fixture::Request),
                         .case_id = index,
                         .flags = 0,
                         .nonce = nonce,
                         .address = reinterpret_cast<uint64_t>(owned.mapping),
                         .length = owned.size,
                         .fd = owned.fd,
                         .timeout_address = index == 3 ? reinterpret_cast<uint64_t>(&transport.timeout) : 0,
                         .output_address = reinterpret_cast<uint64_t>(&transport.report),
                         .output_bytes = sizeof(fixture::Report),
                         .reserved = 0};
    prefault();
    int64_t const RESULT = static_cast<int64_t>(::syscall(static_cast<long>(ker::abi::callnums::vmem), fixture::PRIVATE_VMEM_OP,
                                                          reinterpret_cast<uint64_t>(&transport.request), sizeof(fixture::Request), 0, 0));
    if (RESULT != 0 || !time_left() || !report_valid(transport.report, nonce, index)) {
        std::fprintf(stderr, "anonymous-usercopy case=%u result=%lld valid=%llu\n", index, static_cast<long long>(RESULT),
                     static_cast<unsigned long long>(transport.report.valid));
        return false;
    }
    owned.mapping = MAP_FAILED;  // Exact validated report proves kernel consumption.
    if (index != 3) {
        size_t const EXPECTED_SIZE = index == 1 ? 128 : PAGE;
        struct stat stat{};
        if (::fstat(owned.fd, &stat) != 0 || stat.st_size != static_cast<off_t>(EXPECTED_SIZE) || ::lseek(owned.fd, 0, SEEK_SET) != 0 ||
            !transfer(owned.fd, transport.bytes.data(), EXPECTED_SIZE, false))
            return false;
        for (size_t i = 0; i < EXPECTED_SIZE; ++i)
            if (transport.bytes[i] != (index == 1 ? replacement(i) : pattern(i))) return false;
    }
    return owned.cleanup_file() && transfer(reports, &transport.report, sizeof(transport.report), true);
}
}  // namespace

auto main(int argc, char** argv) -> int {
    uint64_t nonce{};
    if (argc != 2 || !decimal(argv[1], nonce) || !identity()) return 64;
    uint64_t const PID = ker::process::getpid();
    uint64_t const START = now();
    if (START == UINT64_MAX || START > UINT64_MAX - 60'000'000'000) return 65;
    deadline = START + 60'000'000'000;
    struct stat directory{};
    if (::lstat(DIRECTORY, &directory) != 0 || !S_ISDIR(directory.st_mode) || (directory.st_mode & 0777) != 0700 || directory.st_uid != 0 ||
        directory.st_gid != 0)
        return 66;
    int const REPORTS = ::open(REPORT, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (REPORTS < 0) return 67;
    bool passed = true;
    uint64_t root_serial = 0;
    for (unsigned i = 0; i < 4 && passed; ++i) {
        passed = run_case(nonce, i, REPORTS);
        if (i == 0)
            root_serial = transport.report.root_serial;
        else
            passed = passed && transport.report.root_serial == root_serial;
    }
    passed = ::fsync(REPORTS) == 0 && passed;
    passed = ::close(REPORTS) == 0 && passed;
    if (!passed || !time_left() || !identity() || static_cast<uint64_t>(ker::process::getpid()) != PID) return 68;
    uint64_t const PPID = ker::process::getppid();
    if (PPID != 1) return 68;
    int const WITNESS_FD = ::open(WITNESS, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (WITNESS_FD < 0) return 69;
    char witness[256]{};
    int const SIZE =
        std::snprintf(witness, sizeof(witness), "anonymous_usercopy_v1 nonce=%llu pid=%llu ppid=%llu cases=4 reports=1024 status=0\n",
                      static_cast<unsigned long long>(nonce), static_cast<unsigned long long>(PID), static_cast<unsigned long long>(PPID));
    bool const WRITTEN =
        SIZE > 0 && static_cast<size_t>(SIZE) < sizeof(witness) && transfer(WITNESS_FD, witness, SIZE, true) && ::fsync(WITNESS_FD) == 0;
    bool const CLOSED = ::close(WITNESS_FD) == 0;
    return WRITTEN && CLOSED && time_left() ? 0 : 70;
}
