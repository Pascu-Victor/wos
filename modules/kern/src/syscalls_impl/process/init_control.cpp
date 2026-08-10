#include "init_control.hpp"

#include <abi/init_control.hpp>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <platform/sys/usercopy.hpp>

namespace ker::syscall::process {
namespace {

namespace init_abi = ker::abi::init_control;

ker::mod::sys::Spinlock g_init_control_lock;
std::array<init_abi::Request, init_abi::MAILBOX_CAPACITY> g_requests{};
size_t g_request_head{};
size_t g_request_count{};
bool g_receive_claimed{};
init_abi::StatusSnapshot g_status{};
uint64_t g_status_sequence{};
bool g_status_available{};

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
    if (task->euid != 0) {
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
    request.sender_euid = task->euid;

    // The lock protects only fixed-size memory. No allocation, blocking,
    // logging, usercopy, VFS, scheduler, network, or WKI call occurs here.
    g_init_control_lock.lock();
    if (g_request_count == init_abi::MAILBOX_CAPACITY) {
        g_init_control_lock.unlock();
        return static_cast<uint64_t>(-EAGAIN);
    }
    size_t const TAIL = (g_request_head + g_request_count) % init_abi::MAILBOX_CAPACITY;
    g_requests[TAIL] = request;
    ++g_request_count;
    g_init_control_lock.unlock();
    return 0;
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
    if (g_receive_claimed) {
        g_init_control_lock.unlock();
        return static_cast<uint64_t>(-EBUSY);
    }
    if (g_request_count == 0) {
        g_init_control_lock.unlock();
        return static_cast<uint64_t>(-EAGAIN);
    }
    g_receive_claimed = true;
    request = g_requests[g_request_head];
    g_init_control_lock.unlock();

    // Faulting usercopy is deliberately outside the spinlock. A failed copy
    // leaves the request queued so PID 1 can retry with a valid destination.
    bool const COPIED = ker::mod::sys::usercopy::copy_value_to_task(*task, request_addr, request);

    g_init_control_lock.lock();
    if (COPIED) {
        g_requests[g_request_head] = {};
        g_request_head = (g_request_head + 1) % init_abi::MAILBOX_CAPACITY;
        --g_request_count;
    }
    g_receive_claimed = false;
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

}  // namespace ker::syscall::process
