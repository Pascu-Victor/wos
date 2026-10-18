// Private consuming-init-exit observer. It starts after accepted census END.
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>

namespace {
namespace init = ker::abi::init_control;
using Control = ker::abi::process::CredentialControl;
constexpr uint64_t SIGNAL = uint64_t{1} << static_cast<unsigned>(ker::abi::process::CredentialAuthority::SIGNAL);
constexpr uint64_t GUARD = 0x39c45be17ab062d1;
constexpr char WITNESS[] = "/var/lib/numa-lifecycle/witness.txt";
constexpr char SNAPSHOT[] = "/var/lib/numa-lifecycle/init-exit-snapshot.bin";
constexpr char STATUS[] = "/var/lib/numa-lifecycle/init-exit-status";
constexpr uint64_t WAIT_NS = 30'000'000'000;
struct GuardedSnapshot {
    uint64_t before = GUARD;
    init::StatusSnapshot value{};
    uint64_t after = ~GUARD;
};
static_assert(sizeof(init::StatusSnapshot) == 4160 && init::ABI_VERSION == 1 && offsetof(GuardedSnapshot, value) == 8 &&
              offsetof(GuardedSnapshot, after) == 4168);
struct WitnessIdentity {
    uint64_t nonce{}, cookie{}, pid{}, warm_pid{}, measured_pid{};
};
auto control(Control op, uint64_t value = 0) -> int64_t { return ker::process::prctl(static_cast<int>(op), value, 0, 0, 0); }
auto identity() -> bool {
    uid_t r{}, e{}, s{};
    gid_t gr{}, ge{}, gs{};
    return ::getresuid(&r, &e, &s) == 0 && ::getresgid(&gr, &ge, &gs) == 0 && r == 0 && e == 0 && s == 0 && gr == 0 && ge == 0 && gs == 0 &&
           ::getgroups(0, nullptr) == 0 && control(Control::GET_AUTHORITY_CEILING) == static_cast<int64_t>(SIGNAL) &&
           control(Control::GET_NO_NEW_PRIVILEGES) == 1 &&
           control(Control::GET_SERVICE_PRIVILEGES) == static_cast<int64_t>(ker::abi::process::SERVICE_PRIVILEGES_LOCKED) &&
           ker::process::getppid() == 1 && ker::process::getpid() > 1;
}
auto now_ns() -> uint64_t {
    timespec value{};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 || value.tv_sec < 0 || value.tv_nsec < 0 || value.tv_nsec >= 1'000'000'000)
        return UINT64_MAX;
    auto const seconds = static_cast<uint64_t>(value.tv_sec);
    return seconds <= (UINT64_MAX - static_cast<uint64_t>(value.tv_nsec)) / 1'000'000'000
               ? seconds * 1'000'000'000 + static_cast<uint64_t>(value.tv_nsec)
               : UINT64_MAX;
}
auto literal(const char*& cursor, const char* text) -> bool {
    auto const size = std::strlen(text);
    if (std::strncmp(cursor, text, size) != 0) return false;
    cursor += size;
    return true;
}
auto decimal(const char*& cursor, uint64_t& value) -> bool {
    value = 0;
    if (*cursor < '1' || *cursor > '9') return false;
    do {
        auto const digit = static_cast<unsigned>(*cursor - '0');
        if (value > (UINT64_MAX - digit) / 10) return false;
        value = value * 10 + digit;
        ++cursor;
    } while (*cursor >= '0' && *cursor <= '9');
    return true;
}
auto parse_witness(const std::array<char, 4096>& bytes, uint64_t expected_nonce, WitnessIdentity& out) -> bool {
    auto const* newline = static_cast<const char*>(std::memchr(bytes.data(), '\n', bytes.size()));
    if (newline == nullptr) return false;
    for (auto const* p = newline + 1; p != bytes.data() + bytes.size(); ++p)
        if (*p != 0) return false;
    const char* cursor = bytes.data();
    return literal(cursor, "NUMA_CENSUS_WITNESS version=1 nonce=") && decimal(cursor, out.nonce) && literal(cursor, " cookie=") &&
           decimal(cursor, out.cookie) && literal(cursor, " pid=") && decimal(cursor, out.pid) && literal(cursor, " warm_pid=") &&
           decimal(cursor, out.warm_pid) && literal(cursor, " warm_cycles=1000 warm_wait0=1 measured_pid=") &&
           decimal(cursor, out.measured_pid) &&
           literal(cursor, " measured_cycles=1000 measured_wait0=1 kernel_coverage=255 external_work_wait=1 combined_coverage=511\n") &&
           cursor == newline + 1 && out.nonce == expected_nonce && out.pid > 1 && out.pid <= INT64_MAX && out.warm_pid > 1 &&
           out.warm_pid <= INT64_MAX && out.measured_pid > 1 && out.measured_pid <= INT64_MAX && out.pid != out.warm_pid &&
           out.pid != out.measured_pid && out.warm_pid != out.measured_pid;
}
auto read_witness(uint64_t nonce, WitnessIdentity& out) -> bool {
    int const fd = ::open(WITNESS, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) return false;
    struct stat metadata{};
    std::array<char, 4096> bytes{};
    bool good = ::fstat(fd, &metadata) == 0 && metadata.st_mode == (S_IFREG | 0600) && metadata.st_uid == 0 && metadata.st_gid == 0 &&
                metadata.st_nlink == 1 && metadata.st_size == 4096;
    size_t offset{};
    while (good && offset != bytes.size()) {
        auto const count = ::read(fd, bytes.data() + offset, bytes.size() - offset);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0 || static_cast<size_t>(count) > bytes.size() - offset) {
            good = false;
            break;
        }
        offset += static_cast<size_t>(count);
    }
    bool const closed = ::close(fd) == 0;
    return good && closed && parse_witness(bytes, nonce, out);
}
// Existing Goal39 wire validation, with this ONESHOT's READY exit semantics.
auto validate_snapshot(const init::StatusSnapshot& snapshot, init::ServiceStatus& selected, bool& observed) -> bool {
    bool valid = snapshot.size == sizeof(snapshot) && snapshot.version == init::ABI_VERSION && snapshot.sequence != 0 &&
                 snapshot.published_mono_ns != 0 &&
                 (snapshot.state == init::SupervisorState::STARTING || snapshot.state == init::SupervisorState::RUNNING) &&
                 snapshot.flags == 0 && snapshot.service_count == 3 && snapshot.reserved0 == 0;
    for (auto item : snapshot.reserved) valid = item == 0 && valid;
    std::array<const char*, 3> names{"journald", "numa-census", "numa-census-exit-witness"};
    std::array<unsigned, 3> matches{};
    for (uint32_t i = 0; valid && i < snapshot.service_count; ++i) {
        auto const& service = snapshot.services[i];
        valid = std::memchr(service.service, '\0', sizeof(service.service)) != nullptr && service.reserved0 == 0 &&
                service.reserved1 == 0 && service.history_count <= init::MAX_TRANSITION_HISTORY && service.state_since_mono_ns != 0 &&
                service.state_since_mono_ns <= snapshot.published_mono_ns && static_cast<unsigned>(service.state) >= 1 &&
                static_cast<unsigned>(service.state) <= 8 && static_cast<unsigned>(service.readiness) >= 1 &&
                static_cast<unsigned>(service.readiness) <= 3 && (service.flags & ~7U) == 0;
        for (auto item : service.reserved) valid = item == 0 && valid;
        if (!valid) break;
        auto const* nul = static_cast<const char*>(std::memchr(service.service, '\0', sizeof(service.service)));
        for (auto const* p = nul; p < service.service + sizeof(service.service); ++p) valid = *p == '\0' && valid;
        for (size_t name = 0; name < names.size(); ++name)
            if (std::strcmp(service.service, names[name]) == 0) {
                ++matches[name];
                if (name == 1) selected = service;
            }
        uint64_t previous{};
        for (uint32_t j = 0; j < init::MAX_TRANSITION_HISTORY; ++j) {
            auto const& transition = service.history[j];
            if (j < service.history_count) {
                valid = transition.timestamp_mono_ns != 0 && transition.timestamp_mono_ns >= previous &&
                        transition.timestamp_mono_ns <= snapshot.published_mono_ns && transition.reserved0 == 0 &&
                        static_cast<unsigned>(transition.from_state) <= 8 && static_cast<unsigned>(transition.to_state) <= 8 &&
                        static_cast<unsigned>(transition.reason) >= 1 && static_cast<unsigned>(transition.reason) <= 9 && valid;
                previous = transition.timestamp_mono_ns;
            } else {
                init::Transition empty{};
                valid = std::memcmp(&transition, &empty, sizeof(empty)) == 0 && valid;
            }
        }
    }
    for (auto count : matches) valid = count == 1 && valid;
    for (uint32_t i = snapshot.service_count; valid && i < init::MAX_STATUS_SERVICES; ++i) {
        init::ServiceStatus empty{};
        valid = std::memcmp(&snapshot.services[i], &empty, sizeof(empty)) == 0;
    }
    bool history = false;
    for (uint32_t i = 0; i < selected.history_count && i < init::MAX_TRANSITION_HISTORY; ++i) {
        auto const& transition = selected.history[i];
        if (transition.reason == init::TransitionReason::EXIT && transition.to_state == init::ServiceState::EXITED &&
            transition.timestamp_mono_ns >= selected.state_since_mono_ns)
            history = true;
    }
    observed = valid && snapshot.state == init::SupervisorState::RUNNING && selected.generation == 1 && selected.restart_count == 0 &&
               selected.state == init::ServiceState::EXITED && selected.readiness == init::ReadinessState::READY && selected.pid == -1 &&
               selected.pgid == -1 && selected.last_wait_status == 0 && selected.last_error == 0 && selected.deadline_mono_ns == 0 &&
               selected.flags == 3 && history;
    return valid;
}
auto save_file(const char* path, const void* data, size_t bytes) -> bool {
    int const fd = ::open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0) return false;
    auto const* current = static_cast<const unsigned char*>(data);
    bool good = true;
    while (bytes != 0) {
        auto const count = ::write(fd, current, bytes);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0 || static_cast<size_t>(count) > bytes) {
            good = false;
            break;
        }
        current += count;
        bytes -= static_cast<size_t>(count);
    }
    good = good && ::fsync(fd) == 0;
    bool const closed = ::close(fd) == 0;
    return good && closed;
}
}  // namespace

auto main(int argc, char** argv) -> int {
    uint64_t nonce{};
    if (argc != 3 || std::strcmp(argv[1], "--nonce") != 0 || !identity()) return 70;
    const char* number = argv[2];
    if (!decimal(number, nonce) || *number != 0) return 70;
    auto const start = now_ns();
    if (start == UINT64_MAX || start > UINT64_MAX - WAIT_NS) return 70;
    auto const deadline = start + WAIT_NS;
    (void)::umask(0077);
    WitnessIdentity witness{};
    if (!read_witness(nonce, witness)) return 71;
    init::StatusSnapshot snapshot{};
    init::ServiceStatus selected{};
    bool observed = false;
    while (!observed) {
        auto const before = now_ns();
        if (before == UINT64_MAX || before >= deadline) return 72;
        GuardedSnapshot value{};
        auto const result = ker::process::init_status_read(&value.value);
        auto const after = now_ns();
        if (after == UINT64_MAX || after >= deadline || value.before != GUARD || value.after != ~GUARD) return 72;
        if (result != 0 && result != -EAGAIN) return 72;
        if (result == 0) {
            snapshot = value.value;
            if (!validate_snapshot(snapshot, selected, observed)) return 72;
        }
        if (!observed) {
            timespec const delay{.tv_sec = 0, .tv_nsec = 10'000'000};
            if (::nanosleep(&delay, nullptr) != 0 && errno != EINTR) return 72;
        }
    }
    if (!identity() || !save_file(SNAPSHOT, &snapshot, sizeof(snapshot))) return 73;
    std::array<char, 512> receipt{};
    auto const length =
        std::snprintf(receipt.data(), receipt.size(),
                      "PASS NUMA_CENSUS_INIT_EXIT version=1 nonce=%llu cookie=%llu witness_pid=%llu generation=1 restarts=0 sequence=%llu "
                      "state=6 readiness=2 wait=0 error=0 history_exit=1 snapshot_bytes=4160 abi=1 failures=0\n",
                      static_cast<unsigned long long>(nonce), static_cast<unsigned long long>(witness.cookie),
                      static_cast<unsigned long long>(witness.pid), static_cast<unsigned long long>(snapshot.sequence));
    if (length <= 0 || static_cast<size_t>(length) >= receipt.size() || !save_file(STATUS, receipt.data(), receipt.size()) || !identity() ||
        ker::process::kill(1, SIGUSR2) != 0)
        return 73;
    return 0;
}
