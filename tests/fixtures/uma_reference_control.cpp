#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>

#include "uma_reference_arm.hpp"
#include "uma_reference_owner.hpp"

#ifdef WOS_UMA_ARM_HOST
extern void uma_arm_host_request(uma_reference::PhaseRequest&);
#endif
namespace uma_reference {
void report_owner_failure(const PhaseRequest& request, const PhaseOutcome& outcome) {
    int const saved_error = errno;
    auto const& detail = outcome.inspection;
    const char* nonce = request.nonce ? request.nonce : "";
    const char* phase = request.phase ? request.phase : "";
    std::fprintf(stderr,
                 "UMA_OWNER_FAILURE v=1 nonce=%.32s phase=%.7s failure=%u check=%u errno_valid=%u error=%d old_error=%d "
                 "child=%lld raw=%d/%d admitted=%u consumed=%u zero=%u peek=%u reap=%u term=%u kill=%u\n",
                 nonce, phase, unsigned(outcome.failure), unsigned(detail.check), unsigned(detail.syscall_error_valid), detail.system_error,
                 outcome.system_error, static_cast<long long>(outcome.child), outcome.observed_raw, outcome.consumed_raw,
                 unsigned(outcome.admitted), unsigned(outcome.consumed), unsigned(outcome.raw_phase_zero), outcome.nonconsuming_completions,
                 outcome.consuming_completions, outcome.term_calls, outcome.kill_calls);
    std::fprintf(stderr,
                 "UMA_OWNER_INSPECTION v=1 nonce=%.32s phase=%.7s fd=%d depth=%u result=%d value=%llu limit=%llu "
                 "metadata_valid=%u device=%llu inode=%llu mode=%llu uid=%llu gid=%llu links=%llu size=%lld\n",
                 nonce, phase, detail.directory_fd, detail.depth, detail.syscall_result, static_cast<unsigned long long>(detail.observed),
                 static_cast<unsigned long long>(detail.limit), unsigned(detail.metadata_valid),
                 static_cast<unsigned long long>(detail.device), static_cast<unsigned long long>(detail.inode),
                 static_cast<unsigned long long>(detail.mode), static_cast<unsigned long long>(detail.uid),
                 static_cast<unsigned long long>(detail.gid), static_cast<unsigned long long>(detail.links),
                 static_cast<long long>(detail.size));
    constexpr char HEX[] = "0123456789abcdef";
    std::array<char, 255> escaped{};
    size_t bytes{};
    while (bytes < detail.entry.size() - 1 && detail.entry[bytes]) {
        auto value = static_cast<unsigned char>(detail.entry[bytes]);
        escaped[2 * bytes] = HEX[value >> 4];
        escaped[2 * bytes + 1] = HEX[value & 15];
        ++bytes;
    }
    bool const truncated = detail.entry_truncated || (bytes == detail.entry.size() - 1 && detail.entry[bytes]);
    std::fprintf(stderr, "UMA_OWNER_ENTRY v=1 nonce=%.32s phase=%.7s hex=%s truncated=%u\n", nonce, phase, escaped.data(),
                 unsigned(truncated));
    errno = saved_error;
}
}  // namespace uma_reference
namespace {
using namespace uma_reference;
namespace arm = uma_reference::arm;
auto directories() -> bool {
    // The package creates /var/lib, never this per-arm root. EEXIST is refusal.
    // Open the known parent without following its final component; common save
    // and the owner additionally walk every run-root component no-follow.
    const char* root = arm::root_path();
    if (!root || root[0] != '/' || std::strlen(root) >= 512) return false;
    std::array<char, 512> parent{};
    std::strcpy(parent.data(), root);
    char* slash = std::strrchr(parent.data(), '/');
    if (!slash || !slash[1]) return false;
    std::array<char, 256> name{};
    if (std::strlen(slash + 1) >= name.size()) return false;
    std::strcpy(name.data(), slash + 1);
    *slash = 0;
    int base = ::open(parent[0] ? parent.data() : "/", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (base < 0) return false;
    bool good = ::mkdirat(base, name.data(), 0700) == 0;
    int fd = good ? ::openat(base, name.data(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC) : -1;
    if (::close(base)) good = false;
    if (fd < 0) return false;
    for (const char* phase : std::array<const char*, 2>{"warmup", "measure"}) {
        if (!good) break;
        good = ::mkdirat(fd, phase, 0700) == 0;
        int p = good ? ::openat(fd, phase, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC) : -1;
        if (p < 0) {
            good = false;
            break;
        }
        for (const char* dir : std::array<const char*, 3>{"logs", "work", "timing"})
            if (::mkdirat(p, dir, 0700)) good = false;
        if (::fsync(p) || ::close(p)) good = false;
    }
    if (::fsync(fd) || ::close(fd)) good = false;
    return good;
}
auto run(const char* nonce) -> int {
    arm::Cancellation cancellation;
    if (!arm::valid_nonce(nonce) || !cancellation.install() || !arm::environment_valid() || !arm::process_admitted(1)) return 70;
    uint64_t start{}, clock{};
    if (!arm::now(start) || start > UINT64_MAX - arm::MARGIN) return 70;
    arm::init::StatusSnapshot initial{};
    uint64_t deadline{}, sequence{}, published{};
    for (;;) {
        if (cancellation.value || !arm::now(clock) || clock >= start + arm::MARGIN) return 71;
        auto rc = arm::snapshot_read(initial);
        if (rc != 0 && rc != -EAGAIN) return 71;
        if (!rc) {
            arm::init::ServiceStatus row{};
            if (!arm::snapshot_valid(initial, row) || initial.sequence < sequence || initial.published_mono_ns < published) return 71;
            sequence = initial.sequence;
            published = initial.published_mono_ns;
            if (arm::running(initial, ::getpid(), deadline)) break;
            if (row.state != arm::init::ServiceState::STARTING) return 71;
        }
        timespec delay{0, 10'000'000};
        if (::nanosleep(&delay, nullptr) && errno != EINTR) return 71;
    }
    if (!arm::now(clock) || deadline <= arm::MARGIN || clock >= deadline - arm::MARGIN || cancellation.value) return 71;
    uint64_t const stop = deadline - arm::MARGIN;
    if (!directories() || !arm::save("init-start.bin", &initial, sizeof(initial))) return 72;
    uint64_t resolution{};
    if (!arm::clock_capture("clock-before.txt", resolution)) return 72;
    arm::Completion complete{};
    std::strcpy(complete.nonce.data(), nonce);
    complete.controller = uint64_t(::getpid());
    complete.deadline = deadline;
    complete.work_stop = stop;
    uint64_t outputs{}, transport{};
    for (size_t index = 0; index < 2; ++index) {
        const char* phase = index == 0 ? "warmup" : "measure";
        if (cancellation.value || !arm::now(clock) || clock >= stop) return 73;
        PhaseRequest request{};
        request.phase = phase;
        request.nonce = nonce;
        request.cancellation = &cancellation.value;
        request.work_stop_ns = stop;
        request.prior_output_bytes = outputs;
        request.prior_transport_bytes = transport;
#ifdef WOS_UMA_ARM_HOST
        uma_arm_host_request(request);
#endif
        auto outcome = own_suite_phase(request);
        // These fields are actual wait/ack observations, not the raw0 projection
        // alone. A cancellation after consumption still prevents all successors.
        if (cancellation.value || outcome.failure != OwnerFailure::NONE || !outcome.admitted || !outcome.consumed ||
            outcome.consuming_completions != 1 || !outcome.nonconsuming_completions || outcome.observed_raw || outcome.consumed_raw ||
            !outcome.raw_phase_zero || !arm::now(clock) || clock >= stop) {
            report_owner_failure(request, outcome);
            return 73;
        }
        if (!arm::audit_phase(phase, nonce, stop, outcome.child, complete.phases[index]) || cancellation.value) return 74;
        auto const& audit = complete.phases[index];
        if (audit.resolution != resolution || audit.cases != 117 || audit.metrics != 11 ||
            (index && complete.phases[0].last >= audit.first))
            return 74;
        outputs += audit.output;
        transport += audit.transport;
        if (outputs > 64 * 1024 * 1024 || transport + 32768 > 512 * 1024) return 74;
        if (index == 0)
            complete.warm = uint64_t(outcome.child);
        else
            complete.measure = uint64_t(outcome.child);
    }
    uint64_t after_res{};
    if (cancellation.value || !arm::now(clock) || clock >= stop || !arm::clock_capture("clock-after.txt", after_res) ||
        after_res != resolution || !arm::audit_completion(complete) || cancellation.value || !arm::now(clock) || clock >= stop)
        return 75;
    std::array<char, arm::COMPLETION_LIMIT> record{};
    int bytes = arm::format_completion(record.data(), record.size(), complete);
    arm::Completion checked{};
    if (bytes <= 0 || size_t(bytes) >= record.size() || !arm::parse_completion(record.data(), size_t(bytes), nonce, checked) ||
        !arm::save("completion.txt", record.data(), size_t(bytes)) || cancellation.value)
        return 75;
    // A durable controller record is not init consuming0 or runtime success.
    std::printf("UMA_CONTROLLER v=1 nonce=%s pid=%llu cases=234 metrics=11 work_stop=%llu\n", nonce,
                (unsigned long long)complete.controller, (unsigned long long)stop);
    return 0;
}
}  // namespace
auto main(int argc, char** argv) -> int {
    if (argc != 4 || std::strcmp(argv[1], "--run") || std::strcmp(argv[2], "--nonce")) return 70;
    int result = run(argv[3]);
    if (result) std::fprintf(stderr, "UMA_CONTROLLER_FAILED v=1 phase_code=%d\n", result);
    return result;
}
