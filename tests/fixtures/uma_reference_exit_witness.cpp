#include <signal.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>

#include "uma_reference_arm.hpp"

namespace {
namespace arm = uma_reference::arm;
auto observe(const char* nonce) -> int {
    if (!arm::valid_nonce(nonce) || !arm::process_admitted(1, true)) return 80;
    uint64_t start{}, now{};
    if (!arm::now(start) || start > UINT64_MAX - arm::MARGIN) return 80;
    std::array<char, arm::COMPLETION_LIMIT> bytes{};
    size_t size{};
    arm::Completion complete{};
    if (!arm::read("completion.txt", bytes.data(), bytes.size(), size) || !arm::parse_completion(bytes.data(), size, nonce, complete) ||
        !arm::audit_completion(complete))
        return 81;
    arm::init::StatusSnapshot initial{};
    size_t initial_bytes{};
    uint64_t deadline{};
    if (!arm::read("init-start.bin", &initial, sizeof(initial), initial_bytes) || initial_bytes != sizeof(initial) ||
        !arm::running(initial, pid_t(complete.controller), deadline) || deadline != complete.deadline)
        return 81;
    uint64_t sequence = initial.sequence, published = initial.published_mono_ns;
    arm::init::StatusSnapshot final{};
    for (;;) {
        if (!arm::now(now) || now >= start + arm::MARGIN) return 82;
        auto rc = arm::snapshot_read(final);
        if (!arm::now(now) || now >= start + arm::MARGIN || (rc && rc != -EAGAIN)) return 82;
        if (!rc) {
            arm::init::ServiceStatus selected{};
            if (!arm::snapshot_valid(final, selected) || final.sequence < sequence || final.published_mono_ns < published) return 82;
            sequence = final.sequence;
            published = final.published_mono_ns;
            if (arm::consumed(final, ::getpid())) break;
            if (!arm::awaiting_exec(final, ::getpid()) && selected.state != arm::init::ServiceState::RUNNING &&
                selected.state != arm::init::ServiceState::STARTING)
                return 82;
        }
        timespec delay{0, 10'000'000};
        if (::nanosleep(&delay, nullptr) && errno != EINTR) return 82;
    }
    if (final.sequence <= initial.sequence || final.published_mono_ns < complete.phases[1].last || !arm::process_admitted(1, true) ||
        !arm::save("init-exit.bin", &final, sizeof(final)))
        return 83;
    std::array<char, 512> status{};
    int n =
        std::snprintf(status.data(), status.size(),
                      "UMA_INIT_EXIT v=1 nonce=%s controller=%llu observer=%llu sequence=%llu "
                      "state=6 readiness=2 wait=0 error=0 generation=1 restarts=0 history_exit=1 snapshot_bytes=4160\n",
                      nonce, (unsigned long long)complete.controller, (unsigned long long)::getpid(), (unsigned long long) final.sequence);
    if (n <= 0 || size_t(n) >= status.size() || !arm::save("init-exit.txt", status.data(), size_t(n)) || !arm::process_admitted(1, true))
        return 83;
    auto const& witness = final.services[7];
    uint64_t const original_deadline = start + arm::MARGIN < witness.deadline_mono_ns ? start + arm::MARGIN : witness.deadline_mono_ns;
    if (!arm::now(now) || now >= original_deadline) return 83;
    std::array<char, 512> window{};
    int count = std::snprintf(window.data(), window.size(), "UMA_WITNESS_WINDOW v=2 nonce=%s observer=%llu entered=%llu deadline=%llu\n",
                              nonce, (unsigned long long)::getpid(), (unsigned long long)start, (unsigned long long)original_deadline);
    if (count <= 0 || size_t(count) >= window.size() || !arm::save("witness-window.txt", window.data(), size_t(count)) || !arm::now(now) ||
        now >= original_deadline)
        return 83;
    // Init must consume this original witness0 before the dependent finalizer
    // can start. This observer never requests shutdown of its own live group.
    return 0;
}
auto finalize(const char* nonce) -> int {
    if (!arm::valid_nonce(nonce) || !arm::process_admitted(1, true)) return 90;
    uint64_t now{};
    if (!arm::now(now)) return 90;
    std::array<char, arm::COMPLETION_LIMIT> bytes{};
    size_t count{};
    arm::Completion complete{};
    if (!arm::read("completion.txt", bytes.data(), bytes.size(), count) || !arm::parse_completion(bytes.data(), count, nonce, complete))
        return 91;
    arm::init::StatusSnapshot earlier{};
    size_t snapshot_bytes{};
    if (!arm::read("init-exit.bin", &earlier, sizeof(earlier), snapshot_bytes) || snapshot_bytes != sizeof(earlier)) return 91;
    auto const& witness = earlier.services[7];
    if (witness.pid <= 1 || witness.pid == ::getpid() || !arm::consumed(earlier, pid_t(witness.pid))) return 91;
    std::array<char, 512> window{}, expected{};
    size_t window_bytes{};
    if (!arm::read("witness-window.txt", window.data(), window.size(), window_bytes) || !window_bytes || window_bytes >= window.size() ||
        window[window_bytes - 1] != '\n' || std::memchr(window.data(), 0, window_bytes))
        return 91;
    std::array<char, 33> window_nonce{};
    unsigned long long window_pid{}, entered{}, saved_deadline{};
    int used{};
    int parsed = std::sscanf(window.data(), "UMA_WITNESS_WINDOW v=2 nonce=%32[0-9a-f] observer=%llu entered=%llu deadline=%llu\n%n",
                             window_nonce.data(), &window_pid, &entered, &saved_deadline, &used);
    if (parsed != 4 || used != int(window_bytes) || std::strcmp(window_nonce.data(), nonce) || window_pid != uint64_t(witness.pid) ||
        !entered || entered > UINT64_MAX - arm::MARGIN || entered <= complete.phases[1].last || !arm::now(now) || entered > now)
        return 91;
    uint64_t const local_deadline = uint64_t(entered) + arm::MARGIN;
    uint64_t const deadline = local_deadline < witness.deadline_mono_ns ? local_deadline : witness.deadline_mono_ns;
    if (!deadline || saved_deadline != deadline || now >= deadline) return 91;
    int n = std::snprintf(expected.data(), expected.size(), "UMA_WITNESS_WINDOW v=2 nonce=%s observer=%llu entered=%llu deadline=%llu\n",
                          nonce, (unsigned long long)witness.pid, entered, (unsigned long long)deadline);
    if (n <= 0 || size_t(n) >= expected.size() || window_bytes != size_t(n) || std::memcmp(window.data(), expected.data(), window_bytes))
        return 91;
    arm::init::StatusSnapshot initial{};
    size_t initial_bytes{};
    uint64_t controller_deadline{};
    if (!arm::read("init-start.bin", &initial, sizeof(initial), initial_bytes) || initial_bytes != sizeof(initial) ||
        !arm::running(initial, pid_t(complete.controller), controller_deadline) || controller_deadline != complete.deadline)
        return 91;
    std::array<char, 512> observed{}, observed_expected{};
    size_t observed_bytes{};
    n = std::snprintf(observed_expected.data(), observed_expected.size(),
                      "UMA_INIT_EXIT v=1 nonce=%s controller=%llu observer=%llu sequence=%llu "
                      "state=6 readiness=2 wait=0 error=0 generation=1 restarts=0 history_exit=1 snapshot_bytes=4160\n",
                      nonce, (unsigned long long)complete.controller, (unsigned long long)witness.pid,
                      (unsigned long long)earlier.sequence);
    if (n <= 0 || size_t(n) >= observed_expected.size() || !arm::read("init-exit.txt", observed.data(), observed.size(), observed_bytes) ||
        observed_bytes != size_t(n) || std::memcmp(observed.data(), observed_expected.data(), observed_bytes))
        return 91;
    uint64_t sequence = earlier.sequence, published = earlier.published_mono_ns;
    arm::init::StatusSnapshot final{};
    for (;;) {
        if (!arm::now(now) || now >= deadline) return 92;
        auto rc = arm::snapshot_read(final);
        if (!arm::now(now) || now >= deadline || (rc && rc != -EAGAIN)) return 92;
        if (!rc) {
            if (final.sequence < sequence || final.published_mono_ns < published) return 92;
            sequence = final.sequence;
            published = final.published_mono_ns;
            if (arm::witness_consumed(final, earlier, ::getpid(), deadline)) break;
            if (!arm::witness_awaiting_exec(final, earlier, ::getpid(), deadline)) return 92;
        }
        timespec delay{0, 10'000'000};
        if (::nanosleep(&delay, nullptr) && errno != EINTR) return 92;
    }
    // Reuse the ORIGINAL witness deadline, not this new service's fresh30s.
    // witness_consumed also checks the finalizer's own service deadline.
    if (!arm::save("init-final.bin", &final, sizeof(final))) return 93;
    std::array<char, 512> status{};
    n = std::snprintf(status.data(), status.size(),
                      "UMA_INIT_FINAL v=2 nonce=%s controller=%llu witness=%llu finalizer=%llu sequence=%llu deadline=%llu "
                      "controller_wait=0 witness_wait=0 snapshot_bytes=4160\n",
                      nonce, (unsigned long long)complete.controller, (unsigned long long)witness.pid, (unsigned long long)::getpid(),
                      (unsigned long long) final.sequence, (unsigned long long)deadline);
    if (n <= 0 || size_t(n) >= status.size() || !arm::save("init-final.txt", status.data(), size_t(n)) || !arm::process_admitted(1, true) ||
        !arm::now(now) || now >= deadline || now >= final.services[8].deadline_mono_ns || !arm::request_poweroff())
        return 93;
    // Only the original controller and witness consuming0 are proof claims.
    // This finalizer is a shutdown actuator; its own exit status is not claimed.
    return 0;
}
}  // namespace
auto main(int argc, char** argv) -> int {
    if (argc != 4 || std::strcmp(argv[2], "--nonce")) return 80;
    if (!std::strcmp(argv[1], "--observe")) return observe(argv[3]);
    if (!std::strcmp(argv[1], "--finalize")) return finalize(argv[3]);
    return 80;
}
