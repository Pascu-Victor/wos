# Implementation Plan for mlibc System Functions

This implementation plan focuses on creating the fundamental system call handlers needed for mlibc in a new operating system without a VFS. I'll organize this by functionality group with progressive implementation phases.

## Phase 1: Core Memory Management

### 1. `sys_anon_allocate(unsigned long size, void** addr)`
- **Purpose**: Allocates anonymous memory (equivalent to mmap with MAP_ANONYMOUS)
- **Implementation Steps**:
  1. Create a physical memory manager to track free physical frames
  2. Implement a virtual memory manager with page tables
  3. Add function to find contiguous virtual address space of requested size
  4. Allocate physical frames and map them to virtual addresses
  5. Zero out allocated pages for security
  6. Return the base virtual address through the `addr` parameter

### 2. `sys_anon_free(void* addr, unsigned long size)`
- **Purpose**: Frees previously allocated anonymous memory
- **Implementation Steps**:
  1. Round size up to page boundary
  2. Unmap the virtual memory region
  3. Return physical frames to the free list
  4. Invalidate TLB entries for the freed region

### 3. `sys_vm_map(void* hint, unsigned long size, int prot, int flags, int fd, long offset, void** addr)`
- **Purpose**: Maps memory with specific protections (more general than anon_allocate)
- **Implementation Steps**:
  1. Extend anon_allocate with protection flags (read/write/execute)
  2. For `fd != -1`, implement a simplified file-backed mapping
  3. Store mapping metadata for future reference
  4. Implement copy-on-write if flags include MAP_PRIVATE
  5. Honor the hint address if possible
  6. Return actual mapped address

## Phase 2: Process Control and Thread Management

### 4. `sys_tcb_set(void* tcb)`
- **Purpose**: Sets the Thread Control Block for TLS support
- **Implementation Steps**:
  1. Create a per-thread structure to store thread-local data
  2. Set up the architecture-specific register (e.g., FS/GS on x86_64)
  3. Store the provided TCB pointer in the current thread structure
  4. Update the CPU register to point to the TCB

### 5. `sys_exit(int status)`
- **Purpose**: Terminates the current process
- **Implementation Steps**:
  1. Implement basic process management structures
  2. Add cleanup routine for thread resources
  3. Free all memory mappings of the process
  4. Store exit status for parent process to retrieve
  5. Notify parent process if waitpid functionality is implemented
  6. Schedule a different process to run

## Phase 3: Synchronization Primitives

### 6. `sys_futex_wait(int* addr, int expected, timespec const* timeout)`
- **Purpose**: Atomic wait on a futex address until woken or timeout
- **Implementation Steps**:
  1. Implement a wait queue data structure
  2. Check if the value at `addr` equals `expected`
  3. If not equal, return immediately with EAGAIN
  4. If equal, add current thread to wait queue for this futex address
  5. Set up timeout using the system timer if timeout is not NULL
  6. Block the thread until woken or timeout expires

### 7. `sys_futex_wake(int* addr)`
- **Purpose**: Wakes threads waiting on a futex address
- **Implementation Steps**:
  1. Look up the wait queue for the given futex address
  2. Wake at least one thread from the queue
  3. Move threads from waiting to ready state
  4. Return the number of threads woken

## Phase 4: Minimal I/O (Without VFS)

### 8. `sys_write(int fd, void const* buffer, unsigned long size, long* written)`
- **Purpose**: Writes data to a file descriptor
- **Implementation Steps**:
  1. Create a file descriptor table per process
  2. Handle special FDs: 0 (stdin), 1 (stdout), 2 (stderr)
  3. For stdin/stdout/stderr, implement console/serial output
  4. Verify buffer points to readable memory
  5. Copy data from user buffer to kernel
  6. For debug console, write data to serial port or framebuffer
  7. Set `written` to bytes successfully written

### 9. `sys_read(int fd, void* buffer, unsigned long size, long* bytes_read)`
- **Purpose**: Reads data from a file descriptor
- **Implementation Steps**:
  1. For stdin, implement keyboard buffer or serial input
  2. Verify buffer points to writable memory
  3. For special device files, implement direct device reading
  4. Copy data from kernel to user buffer
  5. Set `bytes_read` to actual bytes read

### 10. `sys_open(char const* path, int flags, unsigned int mode, int* fd)`
- **Purpose**: Opens a file and returns a file descriptor
- **Implementation Steps**:
  1. Implement a stub filesystem with a fixed set of device files
  2. Support special devices (e.g., "/dev/zero", "/dev/null", "/dev/random")
  3. Validate path string from user space
  4. Find an unused file descriptor number
  5. Create file object with the appropriate mode
  6. Store in the process's file descriptor table
  7. Return the new descriptor number through `fd`

### 11. `sys_close(int fd)`
- **Purpose**: Closes a file descriptor
- **Implementation Steps**:
  1. Validate file descriptor is within range and open
  2. Release any resources associated with the descriptor
  3. Mark the descriptor as available in the FD table

### 12. `sys_seek(int fd, long offset, int whence, long* new_offset)`
- **Purpose**: Repositions read/write offset in an open file
- **Implementation Steps**:
  1. Validate file descriptor
  2. Maintain position pointer for each open file
  3. Update pointer based on `whence` (SEEK_SET, SEEK_CUR, SEEK_END)
  4. For devices, implement device-specific seek behavior
  5. Return new position through `new_offset`

## Phase 5: System Information and Logging

### 13. `sys_clock_get(int clock_id, long* seconds, long* nanoseconds)`
- **Purpose**: Gets current time from specified clock
- **Implementation Steps**:
  1. Implement a system timer using hardware timer interrupts
  2. Track uptime since boot in ticks
  3. Convert ticks to seconds and nanoseconds
  4. Support at least CLOCK_MONOTONIC and CLOCK_REALTIME
  5. For CLOCK_REALTIME, add RTC hardware support later

### 14. `sys_libc_log(char const* message)`
- **Purpose**: Logs a message for debugging
- **Implementation Steps**:
  1. Create a kernel logging subsystem
  2. Route messages to debug console/serial port
  3. Copy string safely from user space
  4. Add optional log levels or filtering
  5. Consider storing recent logs in a kernel ring buffer

### 15. `sys_libc_panic()`
- **Purpose**: Handles critical errors in libc
- **Implementation Steps**:
  1. Print panic message and current call stack
  2. Halt the current process or entire system
  3. In debug builds, enter a debug mode if available
  4. In production, cleanly terminate the process

## Implementation Sequence Recommendation

1. Start with memory management (anon_allocate, anon_free)
2. Implement basic process control (exit, tcb_set)
3. Add debug output (libc_log, libc_panic)
4. Implement basic I/O for console (read/write for stdin/stdout)
5. Add synchronization primitives (futex_wait, futex_wake)
6. Implement simplified file operations without full VFS
7. Finally add clock and more advanced features

## Next Steps After Basic Implementation

1. Design and implement a proper VFS layer
2. Add support for actual filesystems (e.g., ext2, FAT)
3. Implement proper process/thread creation (fork, clone)
4. Add signal handling
5. Implement more POSIX compatibility

This plan provides a solid foundation for implementing the required mlibc system functions while acknowledging the early stage of your OS development.
