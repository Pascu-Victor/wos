# Thread backing during image creation

Automatic stack and TLS/SafeStack pages were cleared by the physical allocator
and then cleared again by their caller before mapping. The allocator's first
clear could also switch from the process CR3 to the kernel CR3 and back on each
page. Thread backing now uses the existing fallible full-overwrite allocators
and retains the complete caller clear before any user PTE is published.

Page owners, reclaim policy, layout reservations, guards, stack backing sizes,
TLS offsets, resource admission, and FPU/accounting behavior are unchanged.
Stack allocations still stage all new pages before mapping; allocation failure
returns every staged page. Pending links are erased by the final full clear.
TLS partial-allocation failure retains its existing caller cleanup contract.

The host test executes the actual backing functions with dirty pages, checks
every byte at mapping publication, and exercises allocation failures, existing
mapping reuse, injected rollback, and partial TLS cleanup. It substitutes
hardware/allocator endpoints and does not measure CR3 cost. Fresh diagnostic
selftests and ordinary four-node measurements are required before claiming
runtime qualification or a launch-time gain.
