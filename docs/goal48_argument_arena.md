# Exec argument snapshot arenas

Common exec and spawn inputs now start with a separately owned 1KiB kernel
string arena. If a string exceeds the remaining capacity, the snapshot allocates
the existing 2MiB arena once, copies only that unfinished string prefix, and
continues copying from its next user address. Completed strings remain in the
small arena until snapshot destruction, preserving every argv/envp pointer.

The combined argument string and pointer budget remains exactly 2MiB, including
both vector terminators. Maximum arena backing on fallback is 2MiB plus 1KiB.
Large inputs have one additional allocation failure point, returning ENOMEM.
Neither user vectors nor captured string prefixes are reread. Partial copies
containing a NUL complete before considering a fault, as in generic usercopy.

Both buffers become snapshot-owned before faulting usercopy. Native exec adopts
its request before the initial copy; spawn retains its existing synchronous
scope. Page pins and SMAP guards retire before allocating fallback storage.
Generic usercopy, admission, credentials, image cutover, address-space limits,
placement, trust and syscall/wire interfaces retain their behavior.

The host test executes the actual snapshot code with the real SmallVec and
controlled allocation/usercopy endpoints. It checks budget boundaries, unaligned
pages, prefix mutation, input retirement, partial faults, every modeled allocation
and copy failure, and modeled fatal ownership recovery. Old-source and mutation
failures establish meaningful assertions. A kernel test uses real mapped user
pages and Task usercopy to cover short strings, cross-page fallback, stable owned
outputs, faults, budget guards and pin/root cleanup. These tests establish safety
and reduced common allocation work; guest latency requires fresh qualification.
