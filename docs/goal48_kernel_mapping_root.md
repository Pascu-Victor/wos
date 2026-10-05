# Private exec root initialization

Exec refreshes the kernel half of its newly allocated private page table through
`copy_kernel_mappings_to(PageTable*)`. This uses the same internal refresh as
the existing task wrapper, before TLS inspection and image mapping.

The change avoids clearing and constructing a 16,024-byte temporary `Task`
solely to pass its page-table pointer. It also avoids that temporary object's
destructor taking the process-population lock for an unlinked charge. Real
task construction, process charges and existing task-wrapper callers stay intact.

The refresh still copies current upper-half entries, including dynamic kernel
mappings, and preserves every lower-half entry. Null roots, a missing kernel
root, kernel self-copy and diagnostic permanent-table protection retain their
existing behavior. Callers must retain the root and serialize publication or
own it privately. Root allocation, zeroing, failure cleanup and later CR3
switches retain their existing order and behavior.

The host test executes the actual refresh, wrappers and exec staging fragment
with the real paging layout. It checks randomized tables, repeated updates,
lower-half preservation, null/self cases and a modeled diagnostic guard. The
old staging fragment fails the temporary-task assertion; copy, boundary and
guard mutations fail independent assertions. These checks establish behavior
and removed work, not guest timing. The existing kernel pool selftest also
checks a fresh root copy and a repeated refresh. Fresh isolated KTEST and the
ordinary four-node matrix are required to qualify the changed kernel.
