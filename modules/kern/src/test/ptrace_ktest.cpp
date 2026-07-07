#include <abi/ptrace.hpp>
#include <cstddef>
#include <cstdint>
#include <platform/debug/ptrace.hpp>
#include <test/ktest.hpp>

KTEST(PtraceAbi, RequestNumbers) {
    KEXPECT_EQ(static_cast<uint64_t>(ker::abi::ptrace::request::TRACEME), static_cast<uint64_t>(0));
    KEXPECT_EQ(static_cast<uint64_t>(ker::abi::ptrace::request::PEEKDATA), static_cast<uint64_t>(2));
    KEXPECT_EQ(static_cast<uint64_t>(ker::abi::ptrace::request::POKEDATA), static_cast<uint64_t>(5));
    KEXPECT_EQ(static_cast<uint64_t>(ker::abi::ptrace::request::GETREGSET), static_cast<uint64_t>(12));
    KEXPECT_EQ(static_cast<uint64_t>(ker::abi::ptrace::request::ATTACH), static_cast<uint64_t>(16));
    KEXPECT_EQ(static_cast<uint64_t>(ker::abi::ptrace::request::GET_REMOTE_INFO), static_cast<uint64_t>(0x5705));
    KEXPECT_EQ(static_cast<uint64_t>(ker::abi::ptrace::request::SYSCALL_WAIT), static_cast<uint64_t>(0x5708));
}

KTEST(PtraceAbi, StructSizes) {
    KEXPECT_EQ(sizeof(ker::abi::ptrace::X86_64GprState), static_cast<size_t>(176));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::RegsetIo), static_cast<size_t>(24));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::MemIo), static_cast<size_t>(32));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::ThreadList), static_cast<size_t>(24));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::StopInfo), static_cast<size_t>(224));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::ImageRecord), static_cast<size_t>(296));
    KEXPECT_EQ(sizeof(ker::abi::ptrace::ImageList), static_cast<size_t>(24));
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

KTEST(PtraceExit, NonParentTracerDoesNotConsumeParentWaitStatus) {
    KEXPECT_TRUE(ker::mod::debug::ptrace::ptrace_selftest_nonparent_exit_observer_preserves_parent_wait_status());
}

KTEST(PtraceExit, ParentTracerConsumesWaitStatus) {
    KEXPECT_TRUE(ker::mod::debug::ptrace::ptrace_selftest_parent_exit_observer_consumes_wait_status());
}
