// Nyx snapshot fuzzer agent: VFS mount path.
//
// This program runs inside WOS.  It:
//   1. Boots to a stable state
//   2. Signals "ready" to QEMU-Nyx (snapshot taken)
//   3. Receives fuzz input (a malformed filesystem image)
//   4. Writes the fuzz input to a ramdisk / tmpfs file
//   5. Attempts to mount it
//   6. Signals "done" (snapshot restored)
//
// Build: cross-compile for x86_64-pc-wos, install to rootfs.

#include "../harness/nyx_hypercall.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/mount.h>
#include <unistd.h>

// Shared input buffer (page-aligned for QEMU-Nyx shared memory)
static uint8_t shared_buf[64 * 1024] __attribute__((aligned(4096)));

int main() {
    // Phase 1: Signal readiness — Nyx takes a snapshot here
    nyx::nyx_agent_ready();

    // Phase 2: Snapshot loop — each iteration gets fresh fuzz input
    while (true) {
        // Get fuzz input from the host
        auto* input = nyx::nyx_get_input(shared_buf);
        if (input->size == 0 || input->size > sizeof(shared_buf) - sizeof(nyx::NyxInput)) {
            nyx::nyx_agent_done();
            continue;
        }

        // Write fuzz input to a file that we'll try to mount as a filesystem
        int fd = open("/tmp/fuzz_disk.img", O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0) {
            write(fd, input->data, input->size);
            close(fd);

            // Try to mount it as various filesystem types
            // The kernel VFS/XFS/FAT32 parser will exercise all parsing code
            mkdir("/tmp/fuzz_mnt", 0755);

            // Try XFS first (primary FS)
            int ret = mount("/tmp/fuzz_disk.img", "/tmp/fuzz_mnt", "xfs", 0, nullptr);
            if (ret == 0) {
                // Successful mount — try some operations
                // (These exercise post-mount VFS code paths)
                umount("/tmp/fuzz_mnt");
            }

            // Try FAT32
            ret = mount("/tmp/fuzz_disk.img", "/tmp/fuzz_mnt", "fat32", 0, nullptr);
            if (ret == 0) {
                umount("/tmp/fuzz_mnt");
            }

            unlink("/tmp/fuzz_disk.img");
        }

        // Signal done — Nyx restores the snapshot
        nyx::nyx_agent_done();
    }

    return 0;
}
