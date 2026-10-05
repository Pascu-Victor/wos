# Exact leaf-count arithmetic

The complete address-space walker counts occupied 4KiB leaves and multiplies
their total by the page size once. Every leaf in the original clipped range is
still read. Present and reserved leaves contribute one page independently of
the user bit; unrelated software flags contribute nothing. The occupancy-only
specialization still returns immediately at its first occupied leaf.

Each leaf table contributes at most 512 pages. Public and recursive bounds are
page aligned, so the final multiplication is exact and bounded. Higher-level
traversal and huge-page clipping are unchanged. Root pins, preemption guards,
publication serialization, complete lazy-range union counting, admission and
current/peak usage recording retain their existing behavior.

The existing actual-source host harness checks 3,000 randomized ranges, mixed
leaves across 32 tables, whole lower-half counts, clipped huge pages, failed pins
and invalid bounds. Added prefix-byte and exact-read oracles cover every leaf
range length from 1 through 512. Both the old and new walkers pass; six deliberate
counting mutations fail semantic assertions.

A detached alternating host benchmark found approximately 7µs for the old warm
32-table walk and 4µs for the new one across reserved, mixed and empty leaves.
Those measurements use modeled raw-load/address endpoints and exclude root
pins, syscalls and guest execution. They do not establish a launch-time gain.
Fresh normal and isolated builds, KTEST and the four-node matrix must qualify
the changed kernel; evidence is tracked in the stability handoff.
