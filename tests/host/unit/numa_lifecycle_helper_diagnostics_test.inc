// Actual helper bodies use explicit raw syscall, clock and owned-file ports.
// No WOS process, credential transition, native context or VM is constructed.
#include <link.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>
#include <test/numa_lifecycle_census_wire.hpp>
#include <vector>

/* @CALLNUMS@ */

namespace fixture {
namespace nl = ker::test::numa_lifecycle;
struct Wait {
    int64_t result;
    int32_t status;
};
std::vector<Wait> waits;
size_t wait_index{};
unsigned assertions{}, failures{}, cases{}, forks{}, polls{}, kills{}, pauses{}, clocks{}, census_calls{}, seeks{}, writes{}, syncs{};
int64_t fork_result{41}, census_result{};
uint64_t now{1'000'000'000}, step{};
int pause_error{}, clock_error{}, seek_error{}, write_error{}, sync_error{};
int header_case{};
bool wait_once{};
ssize_t short_write{-1};
std::string record;
std::vector<int> endpoint_fds;
unsigned endpoint_syncs{}, image_walks{};
int endpoint_fail_index{-1}, endpoint_late_index{-1}, iterate_error{};
bool iterate_empty{};
uint64_t endpoint_late_at{};
size_t observed_pages{};
alignas(4096) unsigned char loaded_pages[3 * 4096]{};
auto iterate_phdr(int (*callback)(dl_phdr_info*, size_t, void*), void*) -> int;

// These ports represent raw ABI results, including signed mask bit63. They
// never invoke a host scheduler or a WOS task.
uint64_t current_tid{50}, current_cpu{}, observed_mask{1};
uint64_t mask_xor{};
unsigned tid_reads{}, cpu_reads{}, affinity_reads{}, affinity_sets{}, yields{};
std::vector<int64_t> set_results;
std::vector<uint64_t> requested_masks;
int64_t yield_result{}, mask_error{};
uint64_t cpu_after_yield{UINT64_MAX}, cpu_after_set{UINT64_MAX}, tid_after_set{};
uint64_t late_after_yield{}, late_after_set{}, fork_late_at{};
bool endpoint_cpu_drift{}, poll_cpu_drift{};
unsigned startup_prefaults{}, startup_files{};
auto currentThreadId() -> uint64_t {
    ++tid_reads;
    return current_tid;
}
auto getcurrent_cpu() -> uint64_t {
    ++cpu_reads;
    return current_cpu;
}
auto getThreadAffinityMask(uint64_t tid) -> int64_t {
    ++affinity_reads;
    if (tid != 50) return -ESRCH;
    return mask_error != 0 ? mask_error : static_cast<int64_t>(observed_mask ^ mask_xor);
}
auto setThreadAffinityMask(uint64_t tid, uint64_t mask) -> int64_t {
    auto const index = affinity_sets++;
    requested_masks.push_back(mask);
    auto const result = index < set_results.size() ? set_results[index] : int64_t{0};
    if (tid != 50 || current_cpu >= 64 || mask != (uint64_t{1} << current_cpu)) return -EINVAL;
    if (result == 0) observed_mask = mask;
    if (cpu_after_set != UINT64_MAX) current_cpu = cpu_after_set;
    if (tid_after_set != 0) current_tid = tid_after_set;
    if (late_after_set != 0) now = late_after_set;
    return result;
}
auto yield() -> uint64_t {
    ++yields;
    if (cpu_after_yield != UINT64_MAX) current_cpu = cpu_after_yield;
    if (late_after_yield != 0) now = late_after_yield;
    return static_cast<uint64_t>(yield_result);
}
auto umask(mode_t) -> mode_t;
void main_prefault();
auto prepare_files() -> bool;

void check(bool value, const char* why) {
    ++assertions;
    if (!value && failures++ < 24) std::fprintf(stderr, "FAIL: %s\n", why);
}
auto endpoint_fsync(int fd) -> int {
    unsigned const index = endpoint_syncs++;
    check(fd == 5 + static_cast<int>(index % 5), "actual fixed five-FD endpoint fsync order");
    endpoint_fds.push_back(fd);
    if (endpoint_late_index == static_cast<int>(index)) now = endpoint_late_at;
    if (endpoint_fail_index == static_cast<int>(index)) {
        errno = EIO;
        return -1;
    }
    return 0;
}

auto clock_gettime(clockid_t, timespec* output) -> int {
    ++clocks;
    if (clock_error) {
        errno = clock_error;
        return -1;
    }
    output->tv_sec = static_cast<time_t>(now / 1'000'000'000);
    output->tv_nsec = static_cast<long>(now % 1'000'000'000);
    now += step;
    return 0;
}
auto nanosleep(const timespec* delay, timespec*) -> int {
    ++pauses;
    check(delay->tv_sec == 0 && delay->tv_nsec == 10'000'000, "original pause operand");
    if (pause_error) {
        errno = pause_error;
        return -1;
    }
    return 0;
}
auto lseek(int fd, off_t offset, int whence) -> off_t {
    ++seeks;
    check(fd == 9 && offset == 0 && whence == SEEK_SET, "only owned witness seek");
    if (seek_error) {
        errno = seek_error;
        return -1;
    }
    return 0;
}
auto write(int fd, const void* bytes, size_t size) -> ssize_t {
    ++writes;
    check(fd == 9 && size > 0 && size < 1024, "bounded owned witness write");
    if (write_error) {
        errno = write_error;
        return -1;
    }
    auto count = short_write < 0 ? static_cast<ssize_t>(size) : short_write;
    record.assign(static_cast<const char*>(bytes), static_cast<size_t>(count));
    return count;
}
auto fsync(int fd) -> int {
    ++syncs;
    check(fd == 9, "only owned witness fsync");
    if (sync_error) {
        errno = sync_error;
        return -1;
    }
    return 0;
}
auto census_syscall(long selector, uint64_t operation, uint64_t request_address, uint64_t bytes, int a4, int a5) -> uint64_t {
    ++census_calls;
    check(selector == static_cast<long>(ker::abi::callnums::vmem) && operation == nl::PRIVATE_VMEM_OP && bytes == sizeof(nl::Request) &&
              a4 == 0 && a5 == 0,
          "actual private transport operands");
    auto const& request = *reinterpret_cast<const nl::Request*>(request_address);
    if (census_result != 0) return static_cast<uint64_t>(census_result);
    auto& h = reinterpret_cast<nl::Report*>(request.output_address)->header;
    h = {.version = nl::VERSION,
         .bytes = sizeof(nl::Report),
         .phase = request.phase,
         .outcome = static_cast<uint64_t>(nl::Outcome::ACCEPT),
         .cookie = 7,
         .nonce = request.nonce,
         .coverage = nl::KERNEL_COVERAGE_REQUIRED,
         .required_coverage = nl::KERNEL_COVERAGE_REQUIRED,
         .capture_us = 1,
         .absolute_deadline_us = 100,
         .calls = 1,
         .waits = 0,
         .reason = 0,
         .node_count = 2,
         .root_incarnation = 9,
         .reserved = 0};
    switch (header_case) {
        case 1:
            h.version = 2;
            break;
        case 2:
            h.bytes = 1;
            break;
        case 3:
            h.phase = 9;
            break;
        case 4:
            ++h.nonce;
            break;
        case 5:
            h.cookie = 0;
            break;
        case 6:
            h.required_coverage = 0;
            break;
        case 7:
            h.reserved = 1;
            break;
        case 8:
            h.root_incarnation = 0;
            break;
        case 9:
            h.calls = 0;
            break;
        case 10:
            h.capture_us = h.absolute_deadline_us;
            break;
        case 11:
            h.reason = 5;
            break;
        case 12:
            h.coverage = 0;
            break;
        case 13:
            h.node_count = 0;
            break;
        case 14:
            h.outcome = static_cast<uint64_t>(nl::Outcome::REFUSE);
            h.coverage = 0;
            h.reason = 12;
            break;
        case 15:
            h.outcome = static_cast<uint64_t>(nl::Outcome::WAIT);
            h.coverage = 1;
            break;
        default:
            break;
    }
    if (wait_once && census_calls == 1) {
        h.outcome = static_cast<uint64_t>(nl::Outcome::WAIT);
        h.coverage = 0;
    }
    if (poll_cpu_drift) current_cpu = 1;
    return 0;
}
}  // namespace fixture

namespace ker::process {
auto fork() -> int64_t {
    ++fixture::forks;
    if (fixture::fork_late_at != 0) fixture::now = fixture::fork_late_at;
    return fixture::fork_result;
}
auto waitpid(int64_t pid, int32_t* status, int options, void*) -> int64_t {
    ++fixture::polls;
    fixture::check(pid == fixture::fork_result && options == WNOHANG, "only exact child nonblocking wait");
    if (fixture::wait_index >= fixture::waits.size()) return -ECHILD;
    auto const row = fixture::waits[fixture::wait_index++];
    *status = row.status;
    return row.result;
}
auto kill(int64_t pid, int number) -> int64_t {
    ++fixture::kills;
    fixture::check(pid == fixture::fork_result && number == SIGKILL, "only exact owned child kill");
    errno = ERANGE;  // Prove the earlier diagnostic did not reread ambient errno.
    return 0;
}
}  // namespace ker::process

namespace {
namespace nl = ker::test::numa_lifecycle;
/* @ACTUAL_STORAGE_AND_DIAGNOSTIC@ */
/* @ACTUAL_CLOCK_AND_PREFAULT@ */
/* @ACTUAL_SUPERVISOR_PLACEMENT@ */
/* @ACTUAL_ENDPOINT_PREPARATION@ */
[[noreturn]] void run_child(int, uint64_t, const char*) { std::abort(); }  // Declared unexecuted CPL3/exec endpoint.
/* @ACTUAL_CHILD@ */
/* @ACTUAL_CHILD_CALL@ */
/* @ACTUAL_CENSUS_CALL@ */
auto startup_after_deadline() -> int {
    /* @ACTUAL_STARTUP_BRANCH@ */
    return 0;
}
auto after_prepare() -> int {
    constexpr uint64_t parent = 50;
    char seed[] = "11";
    char* argv[]{nullptr, nullptr, seed};
    /* @ACTUAL_WARM_BEGIN_BRANCH@ */
    return 0;
}
auto after_both_endpoints() -> int {
    constexpr uint64_t parent = 50;
    char seed[] = "11";
    char* argv[]{nullptr, nullptr, seed};
    /* @ACTUAL_BOTH_ENDPOINT_BRANCH@ */
    return 0;
}
}  // namespace

namespace fixture {
auto umask(mode_t mask) -> mode_t {
    check(mask == 0077, "actual startup umask operand");
    return 0;
}
void main_prefault() {
    ++startup_prefaults;
    check(affinity_sets != 0 && supervisor_still_here(), "actual supervisor pin before startup CPL3 prefault");
}
auto prepare_files() -> bool {
    ++startup_files;
    check(startup_prefaults == 1 && affinity_sets != 0 && supervisor_still_here(),
          "actual supervisor pin and prefault precede owned files and PREPARE");
    return true;
}
auto iterate_phdr(int (*callback)(dl_phdr_info*, size_t, void*), void* opaque) -> int {
    ++image_walks;
    if (iterate_error != 0) return iterate_error;
    if (iterate_empty) return 0;
    ElfW(Phdr) headers[2]{};
    headers[0].p_type = PT_LOAD;
    headers[0].p_flags = PF_R;
    headers[0].p_vaddr = 3;
    headers[0].p_memsz = 8190;
    headers[1].p_type = PT_LOAD;
    headers[1].p_flags = PF_W;
    headers[1].p_vaddr = UINTPTR_MAX;
    headers[1].p_memsz = UINTPTR_MAX;
    dl_phdr_info info{};
    info.dlpi_addr = reinterpret_cast<uintptr_t>(loaded_pages);
    info.dlpi_phdr = headers;
    info.dlpi_phnum = 2;
    int const result = callback(&info, sizeof(info), opaque);
    observed_pages = static_cast<EndpointPrefault*>(opaque)->pages;
    if (endpoint_cpu_drift) current_cpu = 1;
    return result;
}
void reset() {
    ++cases;
    waits.clear();
    wait_index = 0;
    forks = polls = kills = pauses = clocks = census_calls = seeks = writes = syncs = 0;
    fork_result = 41;
    census_result = 0;
    now = 1'000'000'000;
    step = 0;
    pause_error = clock_error = seek_error = write_error = sync_error = header_case = 0;
    wait_once = false;
    short_write = -1;
    record.clear();
    endpoint_fds.clear();
    endpoint_syncs = image_walks = 0;
    endpoint_fail_index = endpoint_late_index = -1;
    endpoint_late_at = 0;
    iterate_error = 0;
    iterate_empty = false;
    observed_pages = 0;
    failure = {};
    nonce = 11;
    cookie = 0;
    diagnostic_parent = 50;
    supervisor_cpu = 0;
    current_tid = 50;
    current_cpu = 0;
    observed_mask = 1;
    mask_xor = 0;
    tid_reads = cpu_reads = affinity_reads = affinity_sets = yields = startup_prefaults = startup_files = 0;
    set_results.clear();
    requested_masks.clear();
    yield_result = mask_error = 0;
    cpu_after_yield = cpu_after_set = UINT64_MAX;
    tid_after_set = late_after_yield = late_after_set = fork_late_at = 0;
    endpoint_cpu_drift = poll_cpu_drift = false;
    deadline_ns = 850'000'000'000;
    for (unsigned i = 0; i < 5; ++i) files[i] = 5 + static_cast<int>(i);
    transport = {};
    errno = EACCES;
}
void expect_stage(const char* phase, const char* stage) {
    check(failure.phase != nullptr && std::strcmp(failure.phase, phase) == 0, "exact failure phase");
    check(failure.stage != nullptr && std::strcmp(failure.stage, stage) == 0, "exact failure stage");
}
void supervisor_cases() {
    for (uint64_t cpu : {uint64_t{0}, uint64_t{7}, uint64_t{63}}) {
        reset();
        supervisor_cpu = UINT64_MAX;
        current_cpu = cpu;
        check(pin_supervisor() && supervisor_cpu == cpu && affinity_sets == 1 && affinity_reads == 1 && yields == 0 &&
                  requested_masks == std::vector<uint64_t>{uint64_t{1} << cpu},
              "actual pin validates exact current-CPU mask including signed CPU63 getter");
        check(supervisor_still_here(), "actual pinned supervisor remains on exact TID/mask/CPU");
    }
    reset();
    supervisor_cpu = UINT64_MAX;
    set_results = {-EBUSY, 0};
    cpu_after_yield = 1;
    check(pin_supervisor() && supervisor_cpu == 1 && affinity_sets == 2 && yields == 1 && requested_masks == std::vector<uint64_t>{1, 2},
          "actual EBUSY retry recaptures CPU before the second local-only SET");
    for (int64_t error : {int64_t{-EINVAL}, int64_t{-EPERM}, int64_t{-ENOSYS}, int64_t{1}}) {
        reset();
        set_results = {error};
        check(!pin_supervisor() && affinity_sets == 1 && yields == 0, "non-EBUSY SET result fails without retry");
    }
    reset();
    set_results = {-EBUSY};
    yield_result = -EIO;
    check(!pin_supervisor() && affinity_sets == 1 && yields == 1, "raw yield error refuses EBUSY retry");
    reset();
    set_results = {-EBUSY};
    late_after_yield = deadline_ns;
    check(!pin_supervisor() && affinity_sets == 1 && yields == 1, "recapture never extends absolute deadline");
    reset();
    deadline_ns = now;
    check(!pin_supervisor() && affinity_sets == 0, "expired original deadline forbids first SET");
    for (uint64_t cpu : {uint64_t{64}, UINT64_MAX, static_cast<uint64_t>(-EIO)}) {
        reset();
        current_cpu = cpu;
        check(!pin_supervisor() && affinity_sets == 0 && yields == 0, "CPU64 and raw CPU error refuse before shift or SET");
    }
    for (uint64_t tid : {uint64_t{0}, uint64_t{1}, uint64_t{51}, static_cast<uint64_t>(-EIO)}) {
        reset();
        current_tid = tid;
        check(!pin_supervisor() && affinity_sets == 0 && cpu_reads == 0, "invalid or foreign raw TID refuses before CPU/SET");
    }
    reset();
    mask_error = -EIO;
    check(!pin_supervisor() && affinity_sets == 1 && affinity_reads == 1, "raw GET_AFFINITY failure is not exact mask");
    reset();
    mask_xor = 3;
    check(!pin_supervisor() && affinity_sets == 1 && affinity_reads == 1, "wrong mask readback refuses successful SET");
    reset();
    cpu_after_set = 1;
    check(!pin_supervisor(), "actual CPU drift after successful SET refuses");
    reset();
    tid_after_set = 51;
    check(!pin_supervisor(), "actual TID drift after successful SET refuses");
    reset();
    late_after_set = deadline_ns;
    check(!pin_supervisor(), "successful SET cannot renew expired deadline");
    for (uint64_t cpu : {uint64_t{64}, UINT64_MAX}) {
        reset();
        supervisor_cpu = cpu;
        check(!supervisor_still_here() && affinity_reads == 0, "invalid saved CPU refuses before mask shift/readback");
    }
    for (uint64_t parent : {uint64_t{0}, uint64_t{1}}) {
        reset();
        diagnostic_parent = parent;
        check(!supervisor_still_here() && tid_reads == 0, "invalid saved parent refuses without task query");
    }
    reset();
    observed_mask = 2;
    check(!supervisor_still_here(), "actual later mask drift refuses");
    reset();
    current_cpu = 1;
    check(!prepare_endpoint() && endpoint_syncs == 0 && image_walks == 0, "endpoint guard precedes every owned FD and page read");
    reset();
    endpoint_cpu_drift = true;
    check(!prepare_endpoint() && endpoint_syncs == 5 && image_walks == 1, "endpoint final guard detects drift after image walk");
    for (unsigned index : {0U, 1U}) {
        reset();
        current_cpu = 1;
        int64_t pid = 123;
        check(!child(index, 50, "11", pid) && forks == 0 && polls == 0 && kills == 0 && pid == 123,
              "both actual child guards refuse before fork/adoption");
        expect_stage(index == 0 ? "warm" : "measured", "supervisor-placement");
    }
    reset();
    current_cpu = 1;
    check(!poll_census(nl::Phase::BEGIN) && census_calls == 0 && failure.valid == 0,
          "census placement guard precedes request/syscall and invents no returned header");
    expect_stage("BEGIN", "supervisor-placement");
    reset();
    wait_once = poll_cpu_drift = true;
    check(!poll_census(nl::Phase::END) && census_calls == 1 && pauses == 1 && failure.valid == 0,
          "actual WAIT loop rechecks placement before each next census syscall");
    expect_stage("END", "supervisor-placement");
    reset();
    supervisor_cpu = UINT64_MAX;
    check(startup_after_deadline() == 0 && startup_prefaults == 1 && startup_files == 1 && census_calls == 1 && affinity_sets == 1,
          "actual main orders pin before CPL3 prefault, owned files and PREPARE");
    reset();
    supervisor_cpu = UINT64_MAX;
    set_results = {-EPERM};
    check(startup_after_deadline() == 73 && startup_prefaults == 0 && startup_files == 0 && census_calls == 0 && forks == 0,
          "actual failed main pin prevents all future prefault/prepare/fork work");
    expect_stage("main", "supervisor-placement");
}
void endpoint_cases() {
    reset();
    waits = {{41, 0}, {41, 0}};
    check(after_both_endpoints() == 0 && census_calls == 2 && image_walks == 2, "actual both endpoint caller branches");
    check(endpoint_fds == std::vector<int>{5, 6, 7, 8, 9, 5, 6, 7, 8, 9} && endpoint_syncs == 10 && observed_pages == 3,
          "identical five owned FDs plus actual unaligned multi-page readable PT_LOAD at both endpoints");
    check(syncs == 0 && seeks == 0 && writes == 0, "endpoint maintenance adds no failure diagnostic writes");
    for (int fail = 0; fail < 5; ++fail) {
        reset();
        waits = {{41, 0}};
        endpoint_fail_index = fail;
        check(after_prepare() == 74 && census_calls == 0 && image_walks == 0 && endpoint_syncs == static_cast<unsigned>(fail + 1),
              "actual endpoint fsync refusal prevents BEGIN");
    }
    reset();
    waits = {{41, 0}, {41, 0}};
    endpoint_fail_index = 7;
    check(after_both_endpoints() == 75 && census_calls == 1 && image_walks == 1, "measured endpoint failure prevents END");
    reset();
    waits = {{41, 256}};
    check(after_prepare() == 74 && endpoint_syncs == 0 && image_walks == 0 && census_calls == 0,
          "actual nonzero child raw status prevents endpoint work");
    reset();
    waits = {{41, 0}};
    iterate_empty = true;
    check(after_prepare() == 74 && census_calls == 0 && endpoint_syncs == 5 && image_walks == 1,
          "empty actual object enumeration cannot admit BEGIN");
    reset();
    files[0] = -1;
    check(!prepare_endpoint() && endpoint_syncs == 0 && image_walks == 0, "invalid owned FD refuses before endpoint work");
    reset();
    waits = {{41, 0}};
    iterate_error = 1;
    check(after_prepare() == 74 && census_calls == 0, "enumeration refusal prevents BEGIN");
    reset();
    deadline_ns = now;
    check(!prepare_endpoint() && endpoint_syncs == 0, "original endpoint deadline never renewed");
    reset();
    endpoint_late_index = 0;
    endpoint_late_at = deadline_ns;
    check(!prepare_endpoint() && endpoint_syncs == 1 && image_walks == 0, "late actual fsync completion cannot renew deadline");
    reset();
    clock_error = EIO;
    check(!prepare_endpoint() && endpoint_syncs == 0, "invalid endpoint clock refuses before I/O");
    reset();
    ElfW(Phdr) header{};
    header.p_type = PT_LOAD;
    header.p_flags = PF_R;
    header.p_memsz = 1;
    dl_phdr_info info{};
    info.dlpi_addr = reinterpret_cast<uintptr_t>(loaded_pages);
    info.dlpi_phdr = &header;
    info.dlpi_phnum = 1;
    EndpointPrefault scan{};
    check(prefault_loaded_object(&info, sizeof(info), &scan) == 0 && scan.pages == 1, "actual one-page readable callback");
    static const unsigned char readonly_page[] = {0x41};
    header.p_flags = PF_R | PF_W;
    info.dlpi_addr = reinterpret_cast<uintptr_t>(readonly_page);
    scan = {};
    check(prefault_loaded_object(&info, sizeof(info), &scan) == 0 && readonly_page[0] == 0x41,
          "PF_W metadata does not write a final read-only object");
    header.p_flags = PF_R;
    info.dlpi_addr = reinterpret_cast<uintptr_t>(loaded_pages);
    scan = {};
    scan.objects = ENDPOINT_MAX_OBJECTS;
    check(prefault_loaded_object(&info, sizeof(info), &scan) != 0 && scan.pages == 0, "object cap refuses before read");
    scan = {};
    scan.headers = ENDPOINT_MAX_HEADERS;
    check(prefault_loaded_object(&info, sizeof(info), &scan) != 0 && scan.pages == 0, "header cap refuses before read");
    scan = {};
    scan.pages = ENDPOINT_MAX_PAGES;
    check(prefault_loaded_object(&info, sizeof(info), &scan) != 0 && scan.pages == ENDPOINT_MAX_PAGES, "page cap refuses before read");
    scan = {};
    info.dlpi_addr = UINTPTR_MAX;
    header.p_vaddr = 1;
    check(prefault_loaded_object(&info, sizeof(info), &scan) != 0 && scan.pages == 0, "base plus vaddr overflow refuses before read");
    scan = {};
    info.dlpi_addr = UINTPTR_MAX - 1;
    header.p_vaddr = 0;
    header.p_memsz = 2;
    check(prefault_loaded_object(&info, sizeof(info), &scan) != 0 && scan.pages == 0, "segment end overflow refuses before read");
    scan = {};
    check(prefault_loaded_object(&info, sizeof(info) - 1, &scan) != 0 && scan.objects == 0, "short phdr-info refusal");
    info.dlpi_addr = reinterpret_cast<uintptr_t>(loaded_pages);
    header.p_memsz = 1;
    scan = {};
    step = deadline_ns - now;
    check(prefault_loaded_object(&info, sizeof(info), &scan) != 0 && scan.pages == 0,
          "page read refuses original deadline after object check");
    step = 0;
    now = 1'000'000'000;
    info.dlpi_addr = reinterpret_cast<uintptr_t>(loaded_pages);
    info.dlpi_phdr = nullptr;
    check(prefault_loaded_object(&info, sizeof(info), &scan) != 0 && scan.pages == 0, "missing actual phdr pointer refusal");
}
void writer_cases() {
    reset();
    failure = {.phase = "warm", .stage = "fork", .valid = FORK_RESULT, .fork_result = -ENOMEM};
    check(finish_failure(74, "fallback") == 74, "writer preserves original exit");
    check(seeks == 1 && writes == 1 && syncs == 1 && record.size() < 1024, "one bounded seek/write/fsync");
    check(record.starts_with("NUMA_CENSUS_FAILURE version=1 ") && record.ends_with("\n") &&
              record.find("exit=74 phase=warm stage=fork valid=1 fork_result=-12 ") != std::string::npos,
          "failure witness includes actual fork result");
    reset();
    files[4] = -1;
    check(finish_failure(73, "prepare-files") == 73 && seeks == 0 && writes == 0 && syncs == 0, "missing fd adds no I/O");
    reset();
    seek_error = EIO;
    check(finish_failure(74, "warm-BEGIN") == 74 && seeks == 1 && writes == 0 && syncs == 0, "failed seek cannot write");
    reset();
    short_write = 7;
    check(finish_failure(75, "measured-END") == 75 && writes == 1 && syncs == 0 && record.size() == 7, "short write no retry or fsync");
    reset();
    write_error = EINTR;
    check(finish_failure(74, "warm-BEGIN") == 74 && writes == 1 && syncs == 0, "EINTR write no retry");
    reset();
    sync_error = EIO;
    check(finish_failure(76, "report-save") == 76 && seeks == 1 && writes == 1 && syncs == 1, "fsync failure keeps exit");
    reset();
    failure = {.phase = "END",
               .stage = "census-header",
               .valid = 63,
               .fork_result = INT64_MIN,
               .wait_result = INT64_MIN,
               .census_result = INT64_MIN,
               .wait_status = INT32_MIN,
               .libc_errno = INT32_MIN,
               .header_reject = UINT64_MAX,
               .outcome = UINT64_MAX,
               .reason = UINT64_MAX};
    nonce = cookie = diagnostic_parent = UINT64_MAX;
    check(finish_failure(77, "fallback") == 77 && writes == 1 && record.size() < 1024, "largest scalar record remains bounded");
}
void child_cases() {
    reset();
    fork_result = -ENOMEM;
    check(after_prepare() == 74, "raw fork refusal returns original74");
    expect_stage("warm", "fork");
    check(failure.valid == FORK_RESULT && failure.fork_result == -ENOMEM && failure.libc_errno == 0 && polls == 0 && kills == 0 &&
              census_calls == 0,
          "fork errno is raw and no invented child");
    for (auto pid : {int64_t{1}, int64_t{50}}) {
        reset();
        fork_result = pid;
        check(after_prepare() == 74, "invalid PID keeps original74");
        expect_stage("warm", "fork-pid");
        check(polls == 0 && kills == 0, "invalid PID never adopted");
    }
    for (auto status : {int32_t{71 << 8}, int32_t{72 << 8}, int32_t{9}}) {
        reset();
        waits = {{41, status}};
        check(after_prepare() == 74, "consumed child failure returns original74");
        expect_stage("warm", "child-status");
        check(failure.valid == (FORK_RESULT | WAIT_RESULT | WAIT_STATUS) && failure.wait_result == 41 && failure.wait_status == status &&
                  kills == 0 && census_calls == 0,
              "exact consumed raw status no redundant kill");
    }
    reset();
    waits = {{-ECHILD, 123}};
    check(after_prepare() == 74, "ECHILD refusal returns74");
    expect_stage("warm", "wait-result");
    check(failure.valid == (FORK_RESULT | WAIT_RESULT) && failure.wait_result == -ECHILD && failure.wait_status == 0 && kills == 0,
          "ECHILD supplies no consumed child status");
    reset();
    waits = {{-EIO, 123}, {-ECHILD, 0}};
    check(after_prepare() == 74, "unexpected wait returns74");
    expect_stage("warm", "wait-result");
    check(failure.wait_result == -EIO && failure.wait_status == 0 && polls == 2 && kills == 1,
          "diagnostic precedes original destructor cleanup");
    reset();
    waits = {{0, 123}, {-ECHILD, 0}};
    fork_late_at = deadline_ns;  // Reach the original wait-budget after the new valid pre-fork guard.
    check(after_prepare() == 74, "wait budget returns74");
    expect_stage("warm", "wait-budget");
    check(failure.wait_result == 0 && (failure.valid & WAIT_STATUS) == 0 && kills == 1, "pending wait never validates status");
    reset();
    waits = {{0, 123}, {-ECHILD, 0}};
    pause_error = EIO;
    check(after_prepare() == 74, "wait pause refusal returns74");
    expect_stage("warm", "wait-pause");
    check(failure.libc_errno == EIO && (failure.valid & LIBC_ERRNO) != 0 && errno != EIO && kills == 1,
          "libc pause errno captured before destructor overwrites it");
    reset();
    waits = {{-EINTR, 123}, {41, 0}};
    pause_error = EINTR;
    check(after_prepare() == 0 && kills == 0 && census_calls == 1 && seeks == 0 && writes == 0 && syncs == 0,
          "original EINTR success has no new diagnostic I/O");
    check(failure.stage == nullptr && cookie == 7, "successful warm and BEGIN leave diagnostic untouched");
    reset();
    waits = {{41, 75 << 8}};
    int64_t child_pid = 0;
    check(!child(1, 50, "11", child_pid), "measured child failure");
    expect_stage("measured", "child-status");
}
void census_cases() {
    reset();
    waits = {{41, 0}};
    census_result = -EAGAIN;
    transport.output.header.outcome = 123;
    transport.output.header.reason = 321;
    check(after_prepare() == 74, "BEGIN raw refusal keeps original74");
    expect_stage("BEGIN", "census-transport");
    check(failure.valid == CENSUS_RESULT && failure.census_result == -EAGAIN && failure.libc_errno == 0 && failure.outcome == 0 &&
              failure.reason == 0 && failure.header_reject == 0,
          "raw transport refusal never exports stale header");
    for (int i = 1; i <= 13; ++i) {
        reset();
        header_case = i;
        check(!poll_census(nl::Phase::BEGIN), "bad returned header/acceptance refuses");
        expect_stage("BEGIN", i <= 10 ? "census-header" : "census-accept");
        check(failure.valid == (CENSUS_RESULT | HEADER) && failure.census_result == 0 && failure.header_reject == (uint64_t{1} << (i - 1)),
              "exact returned-field rejection mask");
    }
    reset();
    header_case = 14;
    check(!poll_census(nl::Phase::END), "kernel REFUSE stays failure");
    expect_stage("END", "census-outcome");
    check(failure.outcome == static_cast<uint64_t>(nl::Outcome::REFUSE) && failure.reason == 12, "retained refusal outcome/reason");
    reset();
    header_case = 15;
    check(!poll_census(nl::Phase::BEGIN), "invalid WAIT coverage stays failure");
    expect_stage("BEGIN", "census-outcome");
    reset();
    wait_once = true;
    pause_error = EIO;
    check(!poll_census(nl::Phase::PREPARE), "census pause refuses");
    expect_stage("PREPARE", "census-pause");
    check(failure.libc_errno == EIO && failure.valid == (CENSUS_RESULT | HEADER | LIBC_ERRNO), "census libc errno has explicit validity");
    reset();
    deadline_ns = now;
    check(!poll_census(nl::Phase::BEGIN), "census budget refuses");
    expect_stage("BEGIN", "census-budget");
    check(failure.valid == 0 && census_calls == 0, "no invented call on exhausted budget");
    reset();
    clock_error = EIO;
    check(!poll_census(nl::Phase::PREPARE), "invalid clock refuses at budget frontier");
    expect_stage("PREPARE", "census-budget");
    reset();
    wait_once = true;
    check(poll_census(nl::Phase::BEGIN) && census_calls == 2 && pauses == 1 && failure.stage == nullptr && seeks == 0 && writes == 0 &&
              syncs == 0,
          "original WAIT then ACCEPT success remains diagnostic-free");
}
}  // namespace fixture

auto main() -> int {
    fixture::supervisor_cases();
    fixture::endpoint_cases();
    fixture::writer_cases();
    fixture::child_cases();
    fixture::census_cases();
    std::printf("helper diagnostics cases=%u assertions=%u failures=%u; finite Host endpoints only\n", fixture::cases, fixture::assertions,
                fixture::failures);
    return fixture::failures ? 1 : 0;
}
