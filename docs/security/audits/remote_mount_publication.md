# Goal 19 remote mount publication

9LW established a supported mixed-mount layout and exposed a separate
transactional mount failure. 9LX moves remote preparation before the local
publication commit. This does not change declared LOCAL `/` routing or complete
the broader traversal, mount or WKI domains.

## Baseline evidence

On normal kernel `39370c03f20ce569e587b8b0e5a7d300e02b476f`, an ordinary fixture
first mounted a native TMPFS child, then a remote filesystem at its parent.
Pathname STAT/ACCESS/REALPATH/OPEN selected the remote parent's objects. The
retained native directory FD continued to select the old child. Its mode000
marker was readable by root and denied with EACCES for UID1002, while that
user could read the separate mode0644 peer marker through the covering parent.
The peer marker's bytes, mode and directory contents remained unchanged.

This demonstrates supported cover and retained-directory behavior. It is not
evidence that a new native child can be mounted under an already-remote parent:
that public path is refused by retained mount-target consumption with EXDEV.
No pathname-routing change is justified by the original source observation.

The subsequent assertion fixture passed its five initial native controls, then
failed at remote-parent mounting. At source uptime319.750, auxiliary lane2
returned `-ENOENT` after lane zero's mount row had already been published.
Rollback at319.752 returned `-EBUSY` because the native child was still mounted.
The helper observed a failed mount result and incomplete immediate cleanup.
It did not print the userspace mount errno; the log's exact error is an
auxiliary-attach result, not a measured userspace errno.

After the helper removed its child mount, deferred cleanup removed the parent
row. A later mount-table observation confirmed its absence; the exact empty
fixture directories were then removed. This was not an indefinitely stuck
mount. The cause of the owner's auxiliary `ENOENT` remains unqualified.

Source inspection also showed that this failed candidate called
`wki_remote_vfs_unmount_resource_generation`, which retires all configured
groups for that resource generation. The later mount table lacked the existing
automatic `/wki/wos-3/tmp` mount. The new native regression directly checks
isolation between two groups sharing the same resource and generation.

Baseline records, both live symbol matches, failure logs, cleanup and stopped
process receipts are in `build/goal19-mixed-mount-routing/validation.json`.
The failed assertion run is retained; no passing retry replaces it.

## Publication and lifetime contract

`mount_vfs_proxy_lane` now builds lane zero and all admitted auxiliary lanes
privately. A private ready table permits contiguous auxiliary preparation;
`mount_configured` remains false until the VFS publisher succeeds. Existing
partial-lane capacity policy and definitive ENOENT/ESTALE refusal remain intact.
Peer/channel/resource validation and all fallible transport preparation precede
publication. The public callback still revalidates its retained mount target.

Before commit, failure retires only the candidate group, including private
auxiliary lanes. It does not publish a VFS row or invoke resource-wide rollback.
The construction reference remains held throughout the callback and teardown.
Registry locks never span a VFS publisher or a remote wait.

A successful VFS publication is the mount commit. A peer/resource retirement
racing that callback may make the committed mount unavailable, as retirement
can immediately follow any successful mount. It triggers exact-group cleanup
without changing the successful syscall result. Actual concurrent VFS removal
owns its `mount_released` state; the constructor neither resets it nor claims
that teardown twice. Ordinary resource withdrawal remains generation-wide.

The publisher contract is explicit: return success after publishing one row,
or fail without publishing a row. Both current publishers use VFS's
prepare/publish/finish path. No credential representation, syscall/wire layout,
lane count, mount-target authorization or declared route changes.

## Verification scope

The native regression uses real registry objects and mount rows. It verifies
that private candidate failure changes no mount generation, leaves a native
child intact and preserves a separate active mount of the same resource.
It also checks a committed group's busy-child retirement and exact release,
including final registry removal. Existing manual teardown cases cover
publication-time VFS removal. Source guards require auxiliary admission before
the one publication site, prohibit broad candidate rollback and prohibit a
failure return after a successful commit.

The ordinary mount-cover fixture checks pathname versus retained-FD selection,
UID1002 denial, rejected follow-up mount/unmount operations, unchanged owner
contents/mode and cleanup. Its initial baseline fails at mounting as described
above. On the fixed kernel, the first assertion run and an errno-only probe
passed 22 checks but incorrectly expected EXDEV at the already occupied native
child. The probe measured EBUSY (16), consistent with exact-path mount
reservation. Both failed runs and their binaries remain archived. The final
fixture retains that EBUSY assertion and adds a distinct empty peer directory
for the EXDEV (18) check; all 24 checks pass. No kernel change was needed for
this test correction. Owner data/mode and fixture inventories are unchanged.

Normal Build WOS, the isolated diagnostic build, scoped formatting/tidy and
source/policy checks pass. Full KTEST passes 59,758 assertions with zero failures
and automatic poweroff, including the candidate-isolation native regression.
All 11 established traversal fixtures pass 1,568 checks on the fixed normal
kernel. The broader traversal domain remains pending, as does the underlying
owner auxiliary ENOENT cause. Results and process receipts are recorded in
`build/goal19-mount-publication/validation.json`.
