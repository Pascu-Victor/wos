# Goal19 9OD: block attachment access authority

Block bindings now retain their issued read permission. Scalar READ and ring
READ/BULK_READ reject a write-only attachment before allocating a read buffer,
calling the device or transferring data. Read-only bindings reject FLUSH in both
message and ring paths before the device callback. Existing write denials remain.

The attach classifier now requires the union of requested BLOCK_READ and
BLOCK_WRITE permissions, plus DEVICE_ATTACH. Legacy mode zero requests both
rights and therefore requires both permissions. Explicit read-only and write-only
requests continue to require only their respective permission.

## Defect and scope

The resource review found that the previous classifier chose BLOCK_WRITE whenever
write access was requested, including read/write requests. `DevServerBinding`
stored a `block_read_only` restriction but discarded requested read access.
Consequently the message and shared-ring read implementations had no way to
enforce a write-only issued grant. The ring's FLUSH branch also lacked the write
restriction implied by the message policy classifier.

This is a source-backed authorization defect. No packet alteration, remote
memory modification or penetration testing was used to demonstrate it. Native
functional tests call the actual handlers with a retained fixture binding and
counting block device.

`block_read_allowed` is initialized from the attach request before publication
and copied by both binding move operations. It defaults false. The existing
writer lease and read-only downgrade remain independent: a read/write request
may still receive a read-only binding when the backing device cannot admit a
remote writer. A write-only request cannot take that downgrade.

Denied reads return `-EACCES`; denied write/flush operations return `-EROFS`.
Existing malformed framing and range/geometry checks retain precedence. Ring
denials consume the request and publish an error completion with zero transferred
bytes; they do not publish device data. The unchanged channel, binding and zone
references continue to own admitted work and retirement.

No UID/GID ABI, wire layout, descriptor identity or declared VFS routing changes.
Configurations granting only BLOCK_WRITE can no longer admit a read/write or
legacy attachment. That refusal is intentional; it does not silently grant read
authority or widen the requested binding.

## Validation record

Artifacts and command receipts are under `build/goal19-wki-resource-audit/`.
The stage's `before/` files and `patch-contract.md` preserve the reviewed baseline
and scope. Final runtime results belong in `validation.json`; a build or source
check alone does not qualify the native cases.

The new scalar KTEST exercises actual deferred READ/WRITE/FLUSH callbacks with
denied and allowed bindings and checks that moves preserve access restrictions.
The existing retained-zone test now exercises actual ring READ/BULK_READ refusal,
zero result lengths, untouched data slots, a successful read control, read-only
FLUSH refusal, and the previous successful flush/retirement controls.

Fresh host tests cover explicit read-only/write-only, combined and legacy attach
permission masks. The earlier two-node 9OD baseline passed 124 IPC descriptor
checks and 216 main/worker resource-limit checks on the unchanged 9OB kernel;
those results must not be described as validation of this patch.

The patched normal and isolated diagnostic builds both pass. Four host suites,
seven focused WKI source suites, the policy audit and scoped formatting/tidy
pass. The initial formatting failure is retained alongside its corrected result.
Freshly packaged isolated KTEST passes **60,036/0**, including the new scalar
case and extended retained-zone/ring case, then powers off at484.516s.

The patched normal kernel on a fresh private two-node rootfs passes all ten IPC
commands (124 assertions, including nested child reports),108 main-thread and
108 worker-thread resource assertions. Filesystem readiness and strict remote
uname both pass in this run; this does not resolve the earlier four-node failure.
Temporary fixture files are absent on both physical nodes. Both guests synced
and stopped; the final host receipt at2026-09-17T02:19:26.345957UTC records no WOS
VM. Source/kernel hashes and exact command receipts are in `validation.json`.
These functional IPC/resource runs are regressions, not live block/RDMA tests.

The separate 9OC stale remote-root `EINVAL`/retirement `EBUSY` observation remains
unfixed. It is not explained by this block access change. Broader WKI resource
qualification and the full goal remain open until their remaining evidence is
reconciled.
