# Lazy-page backing

Anonymous lazy faults and private file faults used a zeroing physical allocator
and then cleared the same page again. They now use the existing full-overwrite
allocator and keep the complete caller clear before IO or PTE publication. This
removes the allocator's redundant clear and its possible CR3 round trip.

Anonymous faults retain fallible allocation without yielding reclaim or a fatal
OOM dump. Private file faults retain fallible allocation with the same bounded
reclaim policy and default retries. Page owners, tags, size, KASan unpoisoning,
references, EOF padding, exact reads, readonly file-cache selection, protection
revalidation, competing-mapping handling, lock order and TLB behavior are
unchanged. No syscall, wire, layout, resource, credential or trust policy changes.

The host test executes the actual fault, file materialization, range checks,
install, allocator wrappers and preparation functions against dirty pages and
controlled endpoints. It covers every EOF tail length, zero-before-IO and
publication, partial-read failure, allocation failure, stale protection and
metadata, competing mappings, ownership, lock order and KASan preparation.
The existing kernel anonymous lazy test also checks the entire page before its
first write. Host models do not measure hardware CR3 cost. Fresh isolated
diagnostic and four-node ordinary qualification is required for runtime claims.
