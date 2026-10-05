# Kernel wait CPU accounting and EEVDF

Kernel wait boundaries charge execution before blocking and exclude blocked
time before restoring the previous task state. This fixes both lost execution
before a wait and blocked time appearing in user CPU totals or EEVDF budget.

The four wait wrappers mask interrupts while publishing wait metadata and
applying these boundaries. The scheduler uses its existing runqueue timestamp
to update vruntime, weighted sums, slice usage, deadlines, heap ordering and the
minimum floor together. Only execution advances EEVDF budget. Explicit syscall
pause flushes preceding execution while leaving later exec-service work as
running kernel work.

`KernelWaitAccounting` retains the previous blocked state and whether syscall
accounting was paused. Completion restores that state after excluding the
blocked interval. CPU selection for an explicit pause uses `PreemptGuard`.
Preemption-disabled, transition-active and early-context fallback paths avoid
reentering a runqueue lock; complete corrected accounting is not claimed for
those fallback contexts.

The existing task CPU counters, rusage and EEVDF semantics remain coupled.
Committed CPU snapshots back aggregate procfs CPU statistics, preventing
independently sampled live intervals from making a summed view inconsistent.
Live task observations remain available for their existing callers. Syscall and
wire formats, placement, authentication, cancellation, continuation and incoming
interrupt-state conventions are preserved.

The host harness executes actual accounting functions, wait wrappers and the
real run heap under ASan/UBSan. It covers timer placement, IRQ debt, weights,
cancellation, finalized and precharged counters, paused exec work, migration,
nesting and detached heap membership. Kernel selftests exercise wait boundaries
and blocked runtime with explicit fixture initialization.

The final normal and diagnostic builds were exercised during the completed
launch-overhead goal. Its fresh isolated KTEST reported 66,705 assertions and
zero failures, with three existing skipped cases. The four-node checks included
ordinary launches, SSH activity and visible idle task accounting. These checks
do not establish exclusive host CPU cost or correct accounting in every unsafe
fallback context. The compact historical receipt remains in
`build/goal48-launch-performance-result-v87/goal-result-proof.json`; superseded
handoffs and raw experiment archives were pruned on 2026-10-05.
