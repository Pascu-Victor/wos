# Initial stack residency

Automatic initial-thread stacks now back 32 KiB at the top instead of 256 KiB.
The existing bottom compatibility page remains backed. For a default stack,
this removes 56 physical page allocations, complete clears and PTE installs
from fresh image construction.

The full stack reservation, four guard pages, resource admission, TLS/TCB,
64 KiB SafeStack, scratch layout and fork metadata are unchanged. Exec explicitly
backs every argument range, including ranges larger than the initial window.
Existing lazy faults still grow in the same larger batches, with page-rounded
resource limits, zero-before-publication and transactional allocation rollback.
Workloads exceeding the smaller initial window may fault earlier. Interrupt
signal delivery retains mapped-only copies and its existing deferral rules.

The native host test executes actual thread creation, backing and growth code
against dirty physical pages and controlled page-table, entropy and lock
endpoints. It checks startup allocation counts, full reserved capacity, guards,
TCB initialization, larger argument ranges, resource limits and allocation
failure cleanup. It does not prove real faults, interrupts, COW behavior or an
elapsed-time improvement. Fresh diagnostic selftests and ordinary runtime
measurements must qualify this source change.
