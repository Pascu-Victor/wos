# Synchronous exec input views

When an exec runs on its canonical process owner, remote placement now borrows
the immutable kernel argument snapshot and the current loader's ELF bytes.
The same syscall retains those inputs until synchronous submission and its
callbacks return. This avoids three argument-storage allocations and one ELF
copy allocation, including when placement returns local.

Worker exec still copies its input into the owner-held plan: acceptance can
release the worker before the owner finishes submission. Borrowed views occupy
separate fields; plan destruction frees only its owned storage. Both paths use
the same combined argument budget and preserve nonnull empty vectors.

Placement, peer admission, credentials, filesystem capture, resource limits,
strict and one-shot policy, cancellation, signal cutover and proxy publication
retain their existing order and behavior. No WKI API, wire format or trust
policy changes. The current image stays unchanged until accepted cutover.

The host test executes the actual plan, preparation and owner submission with
controlled allocator and submission endpoints. It checks all outcomes,
allocation failures, worker input retirement, budgets, empty vectors, refcounts
and original-image preservation. Its allocation counts are algorithmic evidence,
not guest timing or accepted remote-execution qualification. The existing kernel
plan-binding selftest also checks borrowed views and teardown. Fresh isolated
KTEST and the ordinary four-node matrix are required for runtime qualification.
