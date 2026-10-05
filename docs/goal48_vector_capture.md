# Page-bounded exec vector capture

The syscall argument snapshot captures up to sixteen pointer words per usercopy
call during its inline prefix. Each group fits the current user page and the
remaining combined argument budget. Unaligned page crossings and every spilled
entry use the original one-pointer primitive. A group ends at the first possible
SmallVec growth failure; its public capacity is not used to predict that boundary.

Only complete captured words are consumed. A complete null terminator ends the
vector even if a later byte in the same copy faults. Every visited pointer,
including the terminator, counts against the same exact 2MiB strings/pointers
budget. Extra bytes within the current mapped RAM page are discarded after a
terminator; later user pages are never required for them.

The temporary capture owns no heap storage. Existing usercopy primitives retire
page pins, root accesses and SMAP guards before the request-owned SmallVec can
grow. Native request adoption precedes faulting copies, and spawn keeps its
existing synchronous scope. Argument string ownership, admission, credentials,
resource accounting, layouts, FPU state, placement and trust remain unchanged.

The strengthened host harness executes the actual snapshot with the real
SmallVec. Independent read ledgers cover exact budgets, page boundaries,
unaligned words, partial faults, source mutation, input retirement, every
allocation failure and modeled fatal cleanup. The old code passes the functional
checks but fails the common-case copy-count assertion. Seven mutations fail
semantic assertions. Common fourteen-pointer inputs take two copy calls in this
harness; this is removed work, not a measured guest latency gain.

The existing kernel test also uses real mapped pages to cover final-page
terminators, unaligned crossings, incomplete words at an unmapped boundary,
forty-pointer spills and an exhausted budget before another page. Fresh normal
and isolated builds, KTEST and the ordinary four-node matrix qualify the changed
kernel; evidence and launch measurements are tracked in the stability handoff.
