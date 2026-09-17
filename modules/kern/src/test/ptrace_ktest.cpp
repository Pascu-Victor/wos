#include <abi/ptrace.hpp>
#include <cstddef>
#include <cstdint>
#include <platform/debug/ptrace.hpp>
#include <platform/sched/task.hpp>
#include <test/ktest.hpp>

KTEST(PtraceSignals, FatalAdmissionBeforeAndAfterStop) {
    ker::mod::sched::task::Task task{};
    constexpr uint64_t KILL = uint64_t{1} << 8;
    for (bool stop_first : {false, true}) {
        task.ptrace_stopped = stop_first;
        task.signal_pending_store(0);
        task.exit_in_progress = false;
        KEXPECT_EQ(task.ptrace_stop_blocks_execution(), stop_first);
        task.signal_add_pending_mask(KILL);
        task.ptrace_stopped = true;
        KEXPECT_FALSE(task.ptrace_stop_blocks_execution());
        // Exit cleanup clears signals only after publishing exit_in_progress.
        task.exit_in_progress = true;
        task.signal_pending_store(0);
        KEXPECT_FALSE(task.ptrace_stop_blocks_execution());
    }
    task.exit_in_progress = false;
    task.process_exit_requested = true;
    KEXPECT_FALSE(task.ptrace_stop_blocks_execution());
}

KTEST(PtraceSignals, FatalReleasePreservesClaimedStopAndExit) {
    KEXPECT_TRUE(ker::mod::debug::ptrace::ptrace_selftest_fatal_release_preserves_claims());
}

KTEST(PtraceObservation, ChangedCredentialsPreserveOutputsAndClaimsAreRetryable) {
    KEXPECT_TRUE(ker::mod::debug::ptrace::ptrace_selftest_observation_snapshot_and_claims());
}

KTEST(PtraceRegisters, ChangedCredentialsPreserveRegistersAndHardwareState) {
    KEXPECT_TRUE(ker::mod::debug::ptrace::ptrace_selftest_register_snapshot_rejects_changed_credentials());
}

KTEST(PtraceMemory, ChangedCredentialsPreserveDeniedCopyAndWord) {
    KEXPECT_TRUE(ker::mod::debug::ptrace::ptrace_selftest_memory_snapshot_rejects_changed_credentials());
}

KTEST(PtraceImages, ChangedCredentialsPreserveDeniedSnapshot) {
    KEXPECT_TRUE(ker::mod::debug::ptrace::ptrace_selftest_image_snapshot_rejects_changed_credentials());
}

KTEST(PtraceAbi, RequestNumbers) {
    KEXPECT_EQ(static_cast<uint64_t>(ker::abi::ptrace::request::TRACEME), static_cast<uint64_t>(0));
    KEXPECT_EQ(static_cast<uint64_t>(ker::abi::ptrace::request::PEEKDATA), static_cast<uint64_t>(2));
    KEXPECT_EQ(static_cast<uint64_t>(ker::abi::ptrace::request::POKEDATA), static_cast<uint64_t>(5));
    KEXPECT_EQ(static_cast<uint64_t>(ker::abi::ptrace::request::GETREGSET), static_cast<uint64_t>(12));
    KEXPECT_EQ(static_cast<uint64_t>(ker::abi::ptrace::request::ATTACH), static_cast<uint64_t>(16));
    KEXPECT_EQ(static_cast<uint64_t>(ker::abi::ptrace::request::GET_REMOTE_INFO), static_cast<uint64_t>(0x5705));
    KEXPECT_EQ(static_cast<uint64_t>(ker::abi::ptrace::request::SYSCALL_WAIT), static_cast<uint64_t>(0x5708));
    KEXPECT_EQ(static_cast<uint64_t>(ker::abi::ptrace::request::GET_IMAGE_CATALOG), static_cast<uint64_t>(0x5709));
}

KTEST(PtraceAbi, StructSizes) {
    KEXPECT_EQ(sizeof(ker::abi::ptrace::X86_64GprState), static_cast<size_t>(176));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::RegsetIo), static_cast<size_t>(24));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::MemIo), static_cast<size_t>(32));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::ThreadList), static_cast<size_t>(24));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::StopInfo), static_cast<size_t>(224));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::ImageRecord), static_cast<size_t>(296));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::ImageList), static_cast<size_t>(24));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::ImageCatalogRecord), static_cast<size_t>(352));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::ImageCatalogList), static_cast<size_t>(40));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::Event), static_cast<size_t>(40));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::RemoteInfo), static_cast<size_t>(104));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::HwBreak), static_cast<size_t>(32));
}

KTEST(PtraceAbi, RemoteInfoOffsets) {
    KEXPECT_EQ(offsetof(ker::abi::ptrace::RemoteInfo, is_proxy), static_cast<size_t>(0));
    KEXPECT_EQ(offsetof(ker::abi::ptrace::RemoteInfo, proxy_pid), static_cast<size_t>(8));
    KEXPECT_EQ(offsetof(ker::abi::ptrace::RemoteInfo, task_id), static_cast<size_t>(16));
    KEXPECT_EQ(offsetof(ker::abi::ptrace::RemoteInfo, target_node), static_cast<size_t>(24));
    KEXPECT_EQ(offsetof(ker::abi::ptrace::RemoteInfo, remote_pid), static_cast<size_t>(32));
    KEXPECT_EQ(offsetof(ker::abi::ptrace::RemoteInfo, target_hostname), static_cast<size_t>(40));
}

KTEST(PtraceSyscallStop, PublishesLiveSysretState) {
    KEXPECT_TRUE(ker::mod::debug::ptrace::ptrace_selftest_syscall_snapshot_patches_live_sysret_state());
}

KTEST(PtraceSyscallStop, SuppressesDeferredExitStop) {
    KEXPECT_TRUE(ker::mod::debug::ptrace::ptrace_selftest_deferred_syscall_exit_stop_suppression());
}

KTEST(PtraceDetach, PreservesWkiExecveProxyWait) {
    KEXPECT_TRUE(ker::mod::debug::ptrace::ptrace_selftest_detach_preserves_wki_execve_proxy_wait());
}

KTEST(PtraceRegisters, RejectionPreservesState) {
    KEXPECT_TRUE(ker::mod::debug::ptrace::ptrace_selftest_register_rejection_preserves_state());
}
