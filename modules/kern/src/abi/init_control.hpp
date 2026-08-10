#pragma once

#include <stdint.h>

namespace ker::abi::init_control {

constexpr uint32_t ABI_VERSION = 1;
constexpr uint32_t SERVICE_NAME_CAPACITY = 64;
constexpr uint32_t MAX_STATUS_SERVICES = 16;
constexpr uint32_t MAX_TRANSITION_HISTORY = 4;
constexpr uint32_t MAILBOX_CAPACITY = 16;

// Values are part of the kernel/userspace ABI and must remain append-only.
enum class Action : uint32_t {
    INVALID = 0,
    START = 1,
    STOP = 2,
    RESTART = 3,
};

enum class SupervisorState : uint32_t {
    UNKNOWN = 0,
    STARTING = 1,
    RUNNING = 2,
    STOPPING = 3,
    STOPPED = 4,
    FAILED = 5,
};

enum class ServiceState : uint32_t {
    UNKNOWN = 0,
    WAITING = 1,
    STARTING = 2,
    RUNNING = 3,
    STOPPING = 4,
    BACKOFF = 5,
    EXITED = 6,
    FAILED = 7,
    DISABLED = 8,
};

enum class ReadinessState : uint32_t {
    UNKNOWN = 0,
    NOT_READY = 1,
    READY = 2,
    FAILED = 3,
};

enum class TransitionReason : uint32_t {
    NONE = 0,
    DEPENDENCY = 1,
    START_REQUEST = 2,
    SPAWN = 3,
    READINESS = 4,
    EXIT = 5,
    RESTART = 6,
    STOP_REQUEST = 7,
    SHUTDOWN = 8,
    TIMEOUT = 9,
};

constexpr uint32_t SNAPSHOT_FLAG_SHUTTING_DOWN = 1U << 0;
constexpr uint32_t SERVICE_FLAG_ENABLED = 1U << 0;
constexpr uint32_t SERVICE_FLAG_REQUIRED = 1U << 1;
constexpr uint32_t SERVICE_FLAG_RESTART_PENDING = 1U << 2;

// The submitter must zero sender_* and reserved fields. The kernel fills the
// authenticated sender identity before making the request visible to PID 1.
struct Request {
    uint32_t size;
    uint32_t version;
    Action action;
    uint32_t flags;
    uint64_t request_id;
    uint64_t sender_pid;
    uint32_t sender_euid;
    uint32_t reserved0;
    char service[SERVICE_NAME_CAPACITY];
    uint64_t reserved[3];
};

struct Transition {
    uint64_t timestamp_mono_ns;
    ServiceState from_state;
    ServiceState to_state;
    TransitionReason reason;
    uint32_t reserved0;
};

struct ServiceStatus {
    char service[SERVICE_NAME_CAPACITY];
    ServiceState state;
    ReadinessState readiness;
    uint32_t flags;
    uint32_t reserved0;
    int64_t pid;
    int64_t pgid;
    uint64_t generation;
    uint64_t restart_count;
    int32_t last_wait_status;
    int32_t last_error;
    uint64_t state_since_mono_ns;
    uint64_t deadline_mono_ns;
    uint32_t history_count;
    uint32_t reserved1;
    uint64_t reserved[2];
    Transition history[MAX_TRANSITION_HISTORY];
};

// PID 1 publishes a complete snapshot. The kernel assigns sequence when it
// commits the snapshot, allowing readers to detect replacement atomically.
struct StatusSnapshot {
    uint32_t size;
    uint32_t version;
    SupervisorState state;
    uint32_t flags;
    uint64_t sequence;
    uint64_t published_mono_ns;
    uint32_t service_count;
    uint32_t reserved0;
    uint64_t reserved[3];
    ServiceStatus services[MAX_STATUS_SERVICES];
};

static_assert(sizeof(Request) == 128);
static_assert(sizeof(Transition) == 24);
static_assert(sizeof(ServiceStatus) == 256);
static_assert(sizeof(StatusSnapshot) == 4160);

}  // namespace ker::abi::init_control
