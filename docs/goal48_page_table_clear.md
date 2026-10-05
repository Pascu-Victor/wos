# Clearing fresh page tables

Fresh page-table allocations use the existing full-overwrite reclaim allocator,
then clear the entire private 4 KiB page before kind tagging or returning it for
hierarchy publication. This keeps the current pagemap while clearing the HHDM
page, avoiding the physical zero allocator's CR3 round trip from a process map.

The page owner, tag, allocation size, default reclaim retries, fatal exhaustion
policy, KASan unpoisoning and alignment remain unchanged. Root-pool lookup,
zeroing on root release, ownership/refcount checks and reuse are unchanged.
Fork/COW, mapping publication, page-table reclamation, shootdowns, resource
accounting, user layout, FPU state and trust policy retain their existing rules.

The host test executes the actual allocation helper with dirty fallback pages
and checks every byte before kind tagging. It also checks pool passthrough and
the null-return branch. Its allocator and CR3 endpoints are models. Fresh
diagnostic selftests and ordinary four-node runtime measurements are required
before claiming hardware qualification or launch-time improvement.
