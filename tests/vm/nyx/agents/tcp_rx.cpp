// Nyx snapshot fuzzer agent: TCP RX path.
//
// This program runs inside WOS.  It:
//   1. Opens a raw socket or debug interface
//   2. Signals "ready" to QEMU-Nyx (snapshot taken)
//   3. Receives fuzz input (a raw Ethernet frame with TCP payload)
//   4. Injects it into the network stack
//   5. Signals "done" (snapshot restored)
//
// Note: This agent requires either:
//   a) A debug syscall (sys_inject_packet) that feeds raw frames into the stack
//   b) Or a raw socket with appropriate privileges
//
// Build: cross-compile for x86_64-pc-wos, install to rootfs.

#include "../harness/nyx_hypercall.hpp"

#include <cstdio>
#include <cstring>
#include <unistd.h>

// Shared input buffer
static uint8_t shared_buf[64 * 1024] __attribute__((aligned(4096)));

int main() {
    // Phase 1: Set up a listening TCP socket to create state
    // (The fuzzer will inject malformed packets that interact with this state)

    // TODO: When sys_inject_packet debug syscall is added:
    //   - Open /dev/netinject or use a dedicated debug ioctl
    //   - Pass raw ethernet frames from fuzz input directly to the NIC driver

    // For now, create a passive listener to establish TCP state
    // that the fuzzer can interact with via injected packets.

    // Signal readiness — Nyx takes a snapshot here
    nyx::nyx_agent_ready();

    // Snapshot loop
    while (true) {
        auto* input = nyx::nyx_get_input(shared_buf);
        if (input->size == 0 || input->size > 9000) {
            nyx::nyx_agent_done();
            continue;
        }

        // TODO: Inject input->data as a raw ethernet frame into the network stack.
        // This requires a kernel-side debug interface:
        //
        //   int fd = open("/dev/netinject", O_WRONLY);
        //   write(fd, input->data, input->size);
        //   close(fd);
        //
        // The injected frame goes through eth_rx -> ip_rx -> tcp_rx,
        // exercising the entire network stack parsing path.

        nyx::nyx_agent_done();
    }

    return 0;
}
