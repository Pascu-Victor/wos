#include "init_control.hpp"

#include <abi/init_control.hpp>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <platform/sys/usercopy.hpp>

namespace ker::syscall::process {
namespace {

namespace init_abi = ker::abi::init_control;

ker::mod::sys::Spinlock g_init_control_lock;
struct InitControlMailbox {
    std::array<init_abi::Request, init_abi::MAILBOX_CAPACITY> requests{};
    size_t head{};
    size_t count{};
    bool receive_claimed{};
};
InitControlMailbox g_mailbox;
init_abi::StatusSnapshot g_status{};
uint64_t g_status_sequence{};
bool g_status_available{};

// The caller holds g_init_control_lock for the live mailbox. Keeping these
// fixed-memory transitions separate lets tests use an isolated mailbox.
auto enqueue_request(InitControlMailbox& mailbox, const init_abi::Request& request) -> int {
    if (mailbox.count == init_abi::MAILBOX_CAPACITY) return -EAGAIN;
    size_t const TAIL = (mailbox.head + mailbox.count) % init_abi::MAILBOX_CAPACITY;
    mailbox.requests[TAIL] = request;
    ++mailbox.count;
    return 0;
}

auto claim_request(InitControlMailbox& mailbox, init_abi::Request& request) -> int {
    if (mailbox.receive_claimed) return -EBUSY;
    if (mailbox.count == 0) return -EAGAIN;
    mailbox.receive_claimed = true;
    request = mailbox.requests[mailbox.head];
    return 0;
}

void finish_request_copy(InitControlMailbox& mailbox, bool copied) {
    if (copied) {
        mailbox.requests[mailbox.head] = {};
        mailbox.head = (mailbox.head + 1) % init_abi::MAILBOX_CAPACITY;
        --mailbox.count;
    }
    mailbox.receive_claimed = false;
}

auto current_task() -> ker::mod::sched::task::Task* { return ker::mod::sched::get_current_task(); }

auto is_init_process(const ker::mod::sched::task::Task& task) -> bool { return ker::mod::sched::task::process_pid(task) == 1; }

auto action_valid(init_abi::Action action) -> bool {
    switch (action) {
        case init_abi::Action::INVALID:
            return false;
        case init_abi::Action::START:
        case init_abi::Action::STOP:
        case init_abi::Action::RESTART:
            return true;
    }
    return false;
}

auto supervisor_state_valid(init_abi::SupervisorState state) -> bool {
    switch (state) {
        case init_abi::SupervisorState::UNKNOWN:
        case init_abi::SupervisorState::STARTING:
        case init_abi::SupervisorState::RUNNING:
        case init_abi::SupervisorState::STOPPING:
        case init_abi::SupervisorState::STOPPED:
        case init_abi::SupervisorState::FAILED:
            return true;
    }
    return false;
}

auto service_state_valid(init_abi::ServiceState state) -> bool {
    switch (state) {
        case init_abi::ServiceState::UNKNOWN:
        case init_abi::ServiceState::WAITING:
        case init_abi::ServiceState::STARTING:
        case init_abi::ServiceState::RUNNING:
        case init_abi::ServiceState::STOPPING:
        case init_abi::ServiceState::BACKOFF:
        case init_abi::ServiceState::EXITED:
        case init_abi::ServiceState::FAILED:
        case init_abi::ServiceState::DISABLED:
            return true;
    }
    return false;
}

auto readiness_state_valid(init_abi::ReadinessState state) -> bool {
    switch (state) {
        case init_abi::ReadinessState::UNKNOWN:
        case init_abi::ReadinessState::NOT_READY:
        case init_abi::ReadinessState::READY:
        case init_abi::ReadinessState::FAILED:
            return true;
    }
    return false;
}

auto transition_reason_valid(init_abi::TransitionReason reason) -> bool {
    switch (reason) {
        case init_abi::TransitionReason::NONE:
        case init_abi::TransitionReason::DEPENDENCY:
        case init_abi::TransitionReason::START_REQUEST:
        case init_abi::TransitionReason::SPAWN:
        case init_abi::TransitionReason::READINESS:
        case init_abi::TransitionReason::EXIT:
        case init_abi::TransitionReason::RESTART:
        case init_abi::TransitionReason::STOP_REQUEST:
        case init_abi::TransitionReason::SHUTDOWN:
        case init_abi::TransitionReason::TIMEOUT:
            return true;
    }
    return false;
}

auto service_name_char_valid(char value) -> bool {
    return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') || (value >= '0' && value <= '9') || value == '-' ||
           value == '_' || value == '.' || value == '@';
}

auto canonicalize_service_name(char (&name)[init_abi::SERVICE_NAME_CAPACITY]) -> bool {
    if (name[0] == '\0') {
        return false;
    }
    for (size_t index = 0; index < init_abi::SERVICE_NAME_CAPACITY; ++index) {
        if (name[index] == '\0') {
            for (size_t tail = index + 1; tail < init_abi::SERVICE_NAME_CAPACITY; ++tail) {
                name[tail] = '\0';
            }
            return true;
        }
        if (!service_name_char_valid(name[index])) {
            return false;
        }
    }
    return false;
}

template <size_t N>
auto all_zero(const uint64_t (&values)[N]) -> bool {
    for (uint64_t value : values) {
        if (value != 0) {
            return false;
        }
    }
    return true;
}

auto names_equal(const char (&left)[init_abi::SERVICE_NAME_CAPACITY], const char (&right)[init_abi::SERVICE_NAME_CAPACITY]) -> bool {
    for (size_t index = 0; index < init_abi::SERVICE_NAME_CAPACITY; ++index) {
        if (left[index] != right[index]) {
            return false;
        }
        if (left[index] == '\0') {
            return true;
        }
    }
    return true;
}

auto validate_request(init_abi::Request& request) -> bool {
    if (request.size != sizeof(request) || request.version != init_abi::ABI_VERSION || !action_valid(request.action) ||
        request.flags != 0 || request.request_id == 0 || request.sender_pid != 0 || request.sender_euid != 0 || request.reserved0 != 0 ||
        !all_zero(request.reserved)) {
        return false;
    }
    return canonicalize_service_name(request.service);
}

auto validate_status(init_abi::StatusSnapshot& status) -> bool {
    constexpr uint32_t SNAPSHOT_FLAGS = init_abi::SNAPSHOT_FLAG_SHUTTING_DOWN;
    constexpr uint32_t SERVICE_FLAGS =
        init_abi::SERVICE_FLAG_ENABLED | init_abi::SERVICE_FLAG_REQUIRED | init_abi::SERVICE_FLAG_RESTART_PENDING;

    if (status.size != sizeof(status) || status.version != init_abi::ABI_VERSION || !supervisor_state_valid(status.state) ||
        (status.flags & ~SNAPSHOT_FLAGS) != 0 || status.sequence != 0 || status.service_count > init_abi::MAX_STATUS_SERVICES ||
        status.reserved0 != 0 || !all_zero(status.reserved)) {
        return false;
    }

    for (size_t index = 0; index < status.service_count; ++index) {
        auto& service = status.services[index];
        if (!canonicalize_service_name(service.service) || !service_state_valid(service.state) ||
            !readiness_state_valid(service.readiness) || (service.flags & ~SERVICE_FLAGS) != 0 || service.reserved0 != 0 ||
            service.pid < -1 || service.pgid < -1 || service.history_count > init_abi::MAX_TRANSITION_HISTORY || service.reserved1 != 0 ||
            !all_zero(service.reserved)) {
            return false;
        }
        uint64_t previous_timestamp = 0;
        for (size_t history_index = 0; history_index < service.history_count; ++history_index) {
            auto const& transition = service.history[history_index];
            if (!service_state_valid(transition.from_state) || !service_state_valid(transition.to_state) ||
                !transition_reason_valid(transition.reason) || transition.reserved0 != 0 ||
                transition.timestamp_mono_ns < previous_timestamp) {
                return false;
            }
            previous_timestamp = transition.timestamp_mono_ns;
        }
        for (size_t history_index = service.history_count; history_index < init_abi::MAX_TRANSITION_HISTORY; ++history_index) {
            service.history[history_index] = {};
        }
        for (size_t previous = 0; previous < index; ++previous) {
            if (names_equal(service.service, status.services[previous].service)) {
                return false;
            }
        }
    }
    for (size_t index = status.service_count; index < init_abi::MAX_STATUS_SERVICES; ++index) {
        status.services[index] = {};
    }
    return true;
}

}  // namespace

auto wos_proc_init_control_submit(uint64_t request_addr) -> uint64_t {
    auto* task = current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    auto const CREDENTIALS = task->credentials();
    if (!CREDENTIALS.allows(ker::mod::security::Authority::INIT_CONTROL)) {
        return static_cast<uint64_t>(-EPERM);
    }
    if (request_addr == 0) {
        return static_cast<uint64_t>(-EFAULT);
    }

    init_abi::Request request{};
    if (!ker::mod::sys::usercopy::copy_value_from_task(*task, request_addr, request)) {
        return static_cast<uint64_t>(-EFAULT);
    }
    if (!validate_request(request)) {
        return static_cast<uint64_t>(-EINVAL);
    }
    request.sender_pid = ker::mod::sched::task::process_pid(*task);
    request.sender_euid = CREDENTIALS.user().effective;

    // The lock protects only fixed-size memory. No allocation, blocking,
    // logging, usercopy, VFS, scheduler, network, or WKI call occurs here.
    g_init_control_lock.lock();
    int const RESULT = enqueue_request(g_mailbox, request);
    g_init_control_lock.unlock();
    return static_cast<uint64_t>(RESULT);
}

auto wos_proc_init_control_receive(uint64_t request_addr) -> uint64_t {
    auto* task = current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (!is_init_process(*task)) {
        return static_cast<uint64_t>(-EPERM);
    }
    if (request_addr == 0) {
        return static_cast<uint64_t>(-EFAULT);
    }

    init_abi::Request request{};
    g_init_control_lock.lock();
    int const CLAIM = claim_request(g_mailbox, request);
    if (CLAIM != 0) {
        g_init_control_lock.unlock();
        return static_cast<uint64_t>(CLAIM);
    }
    g_init_control_lock.unlock();

    // Faulting usercopy is deliberately outside the spinlock. A failed copy
    // leaves the request queued so PID 1 can retry with a valid destination.
    bool const COPIED = ker::mod::sys::usercopy::copy_value_to_task(*task, request_addr, request);

    g_init_control_lock.lock();
    finish_request_copy(g_mailbox, COPIED);
    g_init_control_lock.unlock();
    return COPIED ? 0 : static_cast<uint64_t>(-EFAULT);
}

auto wos_proc_init_status_publish(uint64_t status_addr) -> uint64_t {
    auto* task = current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (!is_init_process(*task)) {
        return static_cast<uint64_t>(-EPERM);
    }
    if (status_addr == 0) {
        return static_cast<uint64_t>(-EFAULT);
    }

    init_abi::StatusSnapshot status{};
    if (!ker::mod::sys::usercopy::copy_value_from_task(*task, status_addr, status)) {
        return static_cast<uint64_t>(-EFAULT);
    }
    if (!validate_status(status)) {
        return static_cast<uint64_t>(-EINVAL);
    }

    g_init_control_lock.lock();
    ++g_status_sequence;
    if (g_status_sequence == 0) {
        ++g_status_sequence;
    }
    status.sequence = g_status_sequence;
    g_status = status;
    g_status_available = true;
    g_init_control_lock.unlock();
    return 0;
}

auto wos_proc_init_status_read(uint64_t status_addr) -> uint64_t {
    auto* task = current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (status_addr == 0) {
        return static_cast<uint64_t>(-EFAULT);
    }

    init_abi::StatusSnapshot status{};
    g_init_control_lock.lock();
    if (!g_status_available) {
        g_init_control_lock.unlock();
        return static_cast<uint64_t>(-EAGAIN);
    }
    status = g_status;
    g_init_control_lock.unlock();

    return ker::mod::sys::usercopy::copy_value_to_task(*task, status_addr, status) ? 0 : static_cast<uint64_t>(-EFAULT);
}

#ifdef WOS_SELFTEST
auto init_control_selftest_mailbox() -> bool {
    InitControlMailbox mailbox;
    init_abi::Request output{};
    std::memset(&output, 0xA5, sizeof(output));
    auto const POISON = output;
    bool ok = claim_request(mailbox, output) == -EAGAIN && std::memcmp(&output, &POISON, sizeof(output)) == 0;
    for (uint64_t id = 1; id <= init_abi::MAILBOX_CAPACITY; ++id) {
        init_abi::Request request{};
        request.request_id = id;
        request.sender_pid = id + 100;
        request.sender_euid = static_cast<uint32_t>(id + 200);
        ok &= enqueue_request(mailbox, request) == 0;
    }
    auto const FULL = mailbox.requests;
    ok &= enqueue_request(mailbox, POISON) == -EAGAIN && mailbox.count == init_abi::MAILBOX_CAPACITY && mailbox.head == 0 &&
          std::memcmp(mailbox.requests.data(), FULL.data(), sizeof(FULL)) == 0;
    ok &= claim_request(mailbox, output) == 0 && output.request_id == 1;
    output = POISON;
    ok &= claim_request(mailbox, output) == -EBUSY && std::memcmp(&output, &POISON, sizeof(output)) == 0;
    ok &= enqueue_request(mailbox, POISON) == -EAGAIN;
    finish_request_copy(mailbox, false);
    ok &= !mailbox.receive_claimed && mailbox.count == init_abi::MAILBOX_CAPACITY && mailbox.head == 0 &&
          std::memcmp(mailbox.requests.data(), FULL.data(), sizeof(FULL)) == 0;
    ok &= claim_request(mailbox, output) == 0 && output.request_id == 1;
    finish_request_copy(mailbox, true);
    init_abi::Request wrapped{};
    wrapped.request_id = init_abi::MAILBOX_CAPACITY + 1;
    wrapped.sender_pid = wrapped.request_id + 100;
    wrapped.sender_euid = static_cast<uint32_t>(wrapped.request_id + 200);
    ok &= enqueue_request(mailbox, wrapped) == 0;
    for (uint64_t id = 2; id <= wrapped.request_id; ++id) {
        ok &= claim_request(mailbox, output) == 0 && output.request_id == id && output.sender_pid == id + 100 &&
              output.sender_euid == id + 200;
        finish_request_copy(mailbox, true);
    }
    output = POISON;
    return ok && mailbox.count == 0 && !mailbox.receive_claimed && claim_request(mailbox, output) == -EAGAIN &&
           std::memcmp(&output, &POISON, sizeof(output)) == 0;
}

auto init_control_selftest_validation() -> bool {
    init_abi::Request valid{};
    valid.size = sizeof(valid);
    valid.version = init_abi::ABI_VERSION;
    valid.action = init_abi::Action::START;
    valid.request_id = 1;
    std::memcpy(valid.service, "service-test", sizeof("service-test"));
    bool ok = validate_request(valid);
    for (unsigned field = 0; field < 12; ++field) {
        auto request = valid;
        switch (field) {
            case 0:
                --request.size;
                break;
            case 1:
                ++request.version;
                break;
            case 2:
                request.action = init_abi::Action::INVALID;
                break;
            case 3:
                request.flags = 1;
                break;
            case 4:
                request.request_id = 0;
                break;
            case 5:
                request.sender_pid = 1;
                break;
            case 6:
                request.sender_euid = 1;
                break;
            case 7:
                request.reserved0 = 1;
                break;
            case 8:
                request.reserved[2] = 1;
                break;
            case 9:
                request.service[0] = '\0';
                break;
            case 10:
                request.service[0] = '/';
                break;
            case 11:
                std::memset(request.service, 'a', sizeof(request.service));
                break;
        }
        ok &= !validate_request(request);
    }
    for (auto action : {init_abi::Action::START, init_abi::Action::STOP, init_abi::Action::RESTART}) {
        auto request = valid;
        request.action = action;
        request.service[sizeof("service-test")] = 'x';
        ok &= validate_request(request) && request.service[sizeof("service-test")] == '\0';
    }
    init_abi::StatusSnapshot status{};
    status.size = sizeof(status);
    status.version = init_abi::ABI_VERSION;
    status.state = init_abi::SupervisorState::RUNNING;
    status.service_count = 1;
    status.services[0].service[0] = 's';
    status.services[0].state = init_abi::ServiceState::RUNNING;
    status.services[0].readiness = init_abi::ReadinessState::READY;
    status.services[0].history_count = 2;
    status.services[0].history[0].timestamp_mono_ns = 1;
    status.services[0].history[1].timestamp_mono_ns = 2;
    ok &= validate_status(status);
    for (unsigned field = 0; field < 21; ++field) {
        auto candidate = status;
        auto& service = candidate.services[0];
        switch (field) {
            case 0:
                --candidate.size;
                break;
            case 1:
                ++candidate.version;
                break;
            case 2:
                candidate.state = static_cast<init_abi::SupervisorState>(999);
                break;
            case 3:
                candidate.flags = UINT32_MAX;
                break;
            case 4:
                candidate.sequence = 1;
                break;
            case 5:
                candidate.service_count = init_abi::MAX_STATUS_SERVICES + 1;
                break;
            case 6:
                candidate.reserved0 = 1;
                break;
            case 7:
                candidate.reserved[1] = 1;
                break;
            case 8:
                service.state = static_cast<init_abi::ServiceState>(999);
                break;
            case 9:
                service.readiness = static_cast<init_abi::ReadinessState>(999);
                break;
            case 10:
                service.flags = UINT32_MAX;
                break;
            case 11:
                service.pid = -2;
                break;
            case 12:
                service.pgid = -2;
                break;
            case 13:
                service.history_count = init_abi::MAX_TRANSITION_HISTORY + 1;
                break;
            case 14:
                service.reserved1 = 1;
                break;
            case 15:
                service.history[0].from_state = static_cast<init_abi::ServiceState>(999);
                break;
            case 16:
                service.history[0].to_state = static_cast<init_abi::ServiceState>(999);
                break;
            case 17:
                service.history[0].reason = static_cast<init_abi::TransitionReason>(999);
                break;
            case 18:
                service.history[0].reserved0 = 1;
                break;
            case 19:
                service.history[1].timestamp_mono_ns = 0;
                break;
            case 20:
                candidate.service_count = 2;
                candidate.services[1] = service;
                break;
        }
        ok &= !validate_status(candidate);
    }
    std::memset(&status.services[1], 0xA5, sizeof(status.services[1]));
    std::memset(&status.services[0].history[2], 0xA5, sizeof(status.services[0].history[2]));
    status.services[0].service[2] = 'x';
    ok &= validate_status(status) && status.services[0].service[2] == '\0';
    init_abi::ServiceStatus const EMPTY_SERVICE{};
    init_abi::Transition const EMPTY_TRANSITION{};
    return ok && std::memcmp(&status.services[1], &EMPTY_SERVICE, sizeof(EMPTY_SERVICE)) == 0 &&
           std::memcmp(&status.services[0].history[2], &EMPTY_TRANSITION, sizeof(EMPTY_TRANSITION)) == 0;
}

auto init_control_selftest_process_role() -> bool {
    ker::mod::sched::task::Task task;
    task.pid = 0;
    bool ok = !is_init_process(task);
    task.pid = 1;
    ok &= is_init_process(task);
    task.pid = 2;
    ok &= !is_init_process(task);
    task.owner_pid = 1;
    ok &= is_init_process(task);
    task.pid = 1;
    task.owner_pid = 2;
    return ok && !is_init_process(task);
}
#endif

}  // namespace ker::syscall::process
