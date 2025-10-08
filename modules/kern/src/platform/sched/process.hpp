#pragma once

#include <platform/sched/task.hpp>
#include <std/string.hpp>
#include <std/vector.hpp>

namespace ker::mod::sched::process {

// Globally unique process identifier
struct ProcessID {
    uint64_t globalID;  // Unique across all systems
    uint64_t systemID;  // Origin system ID
    uint64_t localID;   // Local identifier on origin system
};

// Resource reference - may be remote
struct ResourceReference {
    enum Type { MEMORY, FILE, DEVICE, IPC, NETWORK, OTHER } type;

    uint64_t systemID;    // Where the resource lives
    uint64_t resourceID;  // System-local resource identifier
    void* localHandle;    // Direct pointer if local, nullptr if remote
};

// Distributed process structure
struct Process {
    ProcessID pid;
    std::string name;

    // Process state
    enum State { CREATED, RUNNING, WAITING, SUSPENDED, TERMINATED } state;
    int exitCode;

    // Ownership relationships
    ProcessID parentPID;
    std::vector<ProcessID> childPIDs;

    // Tasks executing within this process (distributed)
    struct TaskReference {
        uint64_t systemID;
        uint64_t taskID;
        task::Task* localTask;  // Direct pointer if local, nullptr if remote
    };
    std::vector<TaskReference> tasks;

    // Resource management
    std::map<uint64_t, ResourceReference> resources;

    // Remote access support
    struct RemoteProxy {
        // Handles transparent access to remote resources
        void* requestResource(uint64_t resourceID);
        bool releaseResource(uint64_t resourceID);
    } remoteProxy;

    // State synchronization
    uint64_t stateVersion;
    uint64_t lastSyncTimestamp;

    // Methods
    bool createTask(const char* name, uint64_t entryPoint, uint64_t systemID = 0);
    bool terminateTask(uint64_t taskID);
    void* accessResource(uint64_t resourceID);
    bool migrateToSystem(uint64_t targetSystemID);
};

}  // namespace ker::mod::sched::process
