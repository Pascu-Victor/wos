#pragma once

// Nyx Fuzzer Hypercall Interface for WOS.
//
// QEMU-Nyx uses VMCALL/VMMCALL instructions to communicate between the
// guest agent and the fuzzer host.  The guest signals "ready for snapshot",
// requests fuzz input, and signals "done — reset now".
//
// Reference: https://nyx-fuzz.com/

#include <cstddef>
#include <cstdint>

namespace nyx {

// Hypercall numbers (QEMU-Nyx convention)
constexpr uint32_t HC_NYX_GET_HOST_CONFIG = 0;
constexpr uint32_t HC_NYX_AGENT_DONE = 1;
constexpr uint32_t HC_NYX_REQ_TIMEOUT = 2;
constexpr uint32_t HC_NYX_GET_INPUT = 3;
constexpr uint32_t HC_NYX_AGENT_READY = 4;
constexpr uint32_t HC_NYX_PANIC = 5;
constexpr uint32_t HC_NYX_CREATE_SNAPSHOT = 6;

// Shared memory region for fuzz input (mapped by QEMU-Nyx)
struct NyxInput {
    uint32_t size;            // Fuzz input size (filled by host)
    uint8_t data[];           // Fuzz input bytes (flexible array)
} __attribute__((packed));

// Issue a Nyx hypercall via VMCALL.
// a0 = hypercall number, a1 = optional argument (e.g., pointer to shared mem)
inline uint64_t nyx_hypercall(uint32_t hc_num, uint64_t arg) {
    uint64_t result;
    // VMCALL encoding: EAX = 'NYX\0' magic, ECX = hc_num, RDX = arg
    asm volatile(
        "movl $0x4e595800, %%eax\n\t"  // 'NYX\0' magic
        "movl %1, %%ecx\n\t"
        "movq %2, %%rdx\n\t"
        "vmcall\n\t"
        "movq %%rax, %0\n\t"
        : "=r"(result)
        : "r"(hc_num), "r"(arg)
        : "rax", "rcx", "rdx", "memory"
    );
    return result;
}

// Signal to the fuzzer that the guest is ready — take a snapshot now.
inline void nyx_agent_ready() {
    nyx_hypercall(HC_NYX_AGENT_READY, 0);
}

// Create a snapshot at this exact point.  After this call returns, the
// fuzzer has restored the snapshot and injected new fuzz input.
inline void nyx_create_snapshot() {
    nyx_hypercall(HC_NYX_CREATE_SNAPSHOT, 0);
}

// Request the host to fill the shared input buffer.
// Returns pointer to NyxInput struct (already mapped in guest memory).
inline NyxInput* nyx_get_input(void* shared_buf) {
    nyx_hypercall(HC_NYX_GET_INPUT, reinterpret_cast<uint64_t>(shared_buf));
    return static_cast<NyxInput*>(shared_buf);
}

// Signal that this test case is done — reset to snapshot.
inline void nyx_agent_done() {
    nyx_hypercall(HC_NYX_AGENT_DONE, 0);
}

// Signal a guest panic (gives the fuzzer a crash trace).
inline void nyx_panic(const char* msg) {
    nyx_hypercall(HC_NYX_PANIC, reinterpret_cast<uint64_t>(msg));
}

}  // namespace nyx
