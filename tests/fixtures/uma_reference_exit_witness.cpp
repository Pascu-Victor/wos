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
    // Independent original-init observer only, after actual consuming0 and all
    // raw234 records. Guest signal is ordinary shutdown, never QMP fallback.
    if (!arm::request_poweroff()) return 83;
    return 0;
}
}  // namespace
auto main(int argc, char** argv) -> int {
    if (argc != 4 || std::strcmp(argv[1], "--observe") || std::strcmp(argv[2], "--nonce")) return 80;
    return observe(argv[3]);
}
