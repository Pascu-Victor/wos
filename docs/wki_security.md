# WKI v3 authenticated sessions

WKI v3 is secure-only. A node without a valid `opt/wos/wki-auth` fw_cfg file does not initialize WKI, and a v3 node never accepts a v2 frame or falls back after an authentication failure.

## Threat and trust model

The protected identity is the stable node number in the private boot credential table. The hostname, MAC address, capabilities, channel and boot epochs, protocol version, algorithm suite, and 256-bit boot nonce are authenticated claims; none of them is an identity by itself.

The construction is HMAC-SHA-256 with 128-bit frame tags and RFC 5869 HKDF-SHA-256. Every node pair has an independent 256-bit PSK. Session derivation binds the ordered node identities, key ID, protocol version, algorithm suite, both capability masks, both boot epochs, both channel epochs, and both 256-bit boot nonces. Direction-specific HKDF labels produce independent low-to-high and high-to-low keys. A separate label produces the public 128-bit session ID. This PSK construction provides mutual authentication but not forward secrecy or payload confidentiality.

The attacker may capture, delay, duplicate, reorder, corrupt, inject, and route WKI traffic and may know every public wire value. The attacker is not assumed to read a node's mode-0600 host credential file or kernel memory. A peer holding its own authorized PSK is trusted only for the permissions in its local policy row; it is not implicitly authorized for every remote service.

End destinations authenticate source and destination node, version and flags, message type, channel, sequence, ACK and credits, ports, reserved fields, payload, session ID, and counter before liveness, reliability, routing, or service state changes. `hop_ttl` and CRC are normalized to zero in the end-to-end MAC so a transit router can decrement TTL and repair CRC. Transit routers therefore remain trusted for topology and availability, not endpoint identity or payload integrity. Broadcast HELLO has no pairwise tag and is only a configured-identity discovery hint; it cannot create a peer, refresh liveness, process reliability state, or reach a service.

Denial of service is bounded rather than eliminated. Credential lookup is bounded by 256 fixed entries, SHA/HMAC has no allocation, replay admission uses a 64-bit bitmap and peer-local spinlock, and bad tags do not log on the RX hot path. Routed forwarding uses a fixed 32-frame no-grow pool and drops under pressure rather than allocating or blocking.

## Provisioning

`scripts/cluster/cluster_setup.py` creates a fresh key for every unordered node pair at launch. Each node receives only its own table through `-fw_cfg name=opt/wos/wki-auth,file=...`. Files are created atomically with mode 0600. Key bytes never enter JSON, QEMU string arguments, guest disks, logs, procfs, or `wkictl` output.

The little-endian v1 file is:

```text
header: magic[8]="WKIAUTH\0", version:u16, local_node:u16, peer_count:u16, reserved:u16
entry:  peer_node:u16, key_id:u16, policy:u64, key[32]
```

The parser requires the exact file size and rejects missing files, truncation, unknown bits, duplicate peers, zero/reserved identities, zero key IDs, and empty policies. Cluster topology ID `N` maps to stable WKI node ID `N+1`; WKI IDs zero and `0xffff` remain reserved.

Policy bits are independently composable:

| Bit | Permission |
|---:|---|
| 0 | Compute operation (also requires explicit identity trust) |
| 1 | VFS export admission |
| 2 | VFS reads |
| 3 | VFS writes |
| 4 | IPC and event operations |
| 5 | Remote network operations |
| 6 | Zone/RDMA operations |
| 7 | Generic device attachment |
| 8 | Block reads |
| 9 | Block writes |
| 10 | Shared nonroot UID/GID namespace for compute and explicit VFS reads |
| 11 | Include UID/GID 0 in that shared namespace |

Identity trust is configured independently for each receiver and peer:

```json
"wki_auth": {
  "peer_identity": {"1": "same-domain", "2": "same-domain-root"}
}
```

Keys are canonical cluster topology IDs (before the WKI `+1` conversion).
Missing entries and `"deny"` grant no identity trust. `"same-domain"` maps every
nonzero numeric UID/GID to the same local number; it rejects zero in any real,
effective, saved or supplementary-group slot, and rejects active service grants.
`"same-domain-root"` explicitly includes those zero identities and active service
grants. Stored grants made ineffective by an irreversible ceiling drop can pass
nonroot trust without regaining authority. Use these choices only for nodes whose numeric
account namespaces and authority are intentionally shared. The reverse direction
requires its own entry. Identity trust never grants separate compute or VFS operation permissions.
`peer_policy` continues to accept only operation bits 0–9; identity bits come only
from `peer_identity`. Invalid rows, unknown/self peers, unknown policy fields and
unknown identity modes reject provisioning before any credential file is written.

Default operation-only provisioning now rejects normal and fragmented task
submission at authenticated reliable admission. The deferred receiver checks the
mapping again on the decoded credential before executable fetch or process
reservation, using the authenticated policy copied under the peer lifecycle lease
into the compute session token. Deferred work never rereads shutdown-cleared boot
configuration; retirement still invalidates publication. It retains the mapped
snapshot for publication. Denials
return the existing `UNAUTHORIZED`/`EACCES` result. Saved IDs and all snapshot
restrictions remain unchanged by this identity mapping. Legacy credential transport
still rejects snapshots it cannot represent; negotiated complete records preserve
the restrictions described below. The fw_cfg file
layout is unchanged; older kernels reject provisioning with the new unknown bits.

VFS attachment requires generic attach, export, and the requested read/write permission. Block attachment requires generic attach and the requested block access. The authoritative gate is immediately before reliable request admission; denied in-order requests are consumed without service allocation and return `UNAUTHORIZED`, `ACCESS_DENIED`, `-EACCES`, or `REJECTED_POLICY` in the existing response family.

## Complete compute credential identity

`WKI_CAP_COMPUTE_CREDENTIALS` (`0x0400`) selects the canonical version-1,
80-byte credential record for task submissions. It follows the unchanged
368-byte identity prefix and resource snapshot. The 20-byte submit header,
prefix layout, and resource offset remain unchanged. All six duplicated IDs must
match the canonical record. Unknown versions, malformed fields, truncated data,
and capability/length mismatches reject before executable fetch or reservation.

The sender captures one snapshot for both copies of the IDs and the complete
record. The receiver retains negotiated capabilities with the authenticated
identity policy in its immutable compute session token, including deferred and
fragmented work. Mapping preserves supplementary groups, no-new-privileges,
dumpability, authority ceiling, and sealed service grants; it does not derive
authority from the worker's credentials.

If the capability is absent, only an exact legacy six-ID round trip is eligible.
Restricted snapshots return ENOTSUP for strict placement; optional placement
can execute locally with its restrictions intact. A rejected full record never
retries as a legacy record. `wki.compute_credentials_off` suppresses this one
capability for mixed-version diagnostics while preserving local enforcement and
resource negotiation.

## Explicit regular-file read identity

`WKI_CAP_VFS_READ_IDENTITY` (`0x0200`) negotiates `OP_VFS_OPEN_READ_AS`
(`0x0418`). Its body is the canonical 80-byte credential record followed by
legacy OPEN fields (`flags:u32`, `mode:u32`, `path_len:u16`, path bytes). Both
flags and mode must be zero and no prefetch suffix is accepted. The owner
validates the full record, requires VFS-read and separately configured identity
trust, then walks and opens the regular file beneath its export using the mapped
snapshot through TMPFS, XFS or FAT32. Devices, directories, and recursive
remote exports are rejected. FAT32 retains fixed root ownership and
attribute-derived POSIX modes; no extra owner/group metadata is invented.

Reliable RX copies the authenticated policy under its existing peer lifecycle
lease into fixed deferred storage alongside the exact channel identity. Workers
use that admitted value instead of reading a reusable peer slot. Responses and
File lookup retain the existing exact channel-generation checks and failed-send
cleanup. Legacy framing still lacks a binding nonce; the new body does not claim
to solve delayed numeric-channel reuse across the same authenticated session.

The explicit VFS reader fails with ENOTSUP when the capability is absent and
never retries through ambient legacy OPEN. PT_INTERP construction uses the
child's installed snapshot with the worker's path context. ELF source loading
opens with the mapped snapshot before every cache admission and retains that
File for metadata, buffered reads or file-backed construction. Submitted root
and routing policy are installed before source loading. Complete compute records use the separately negotiated capability below. Explicit credential read Files now bypass data/stat caches for content
verification. VFS CLOSE releases only an existing channel-owned
handle and requires no additional operation permission, allowing read-only
peers to clean up their references. Peer authentication and exact FD/channel
ownership still apply.

## Ordinary file-open identity

`WKI_CAP_VFS_OPEN_IDENTITY` (`0x0800`) negotiates `OP_VFS_OPEN_AS`
(`0x0419`). Its body is the canonical 80-byte credential record followed by
unchanged OPEN fields and the optional prefetch suffix. VFS read/write policy
bits follow the requested access mode; creation and truncation also require
write permission. Identity trust is independent of these operation permissions.
The owner maps the complete snapshot before native TMPFS/XFS/FAT open, creation,
truncation, FD publication or optional read prefetch. The client's original VFS
snapshot is used throughout; serving-worker credentials are never substituted.

RX captures both authenticated policy and negotiated capabilities under the
peer lifecycle lease and copies them into the deferred operation. Dedicated
regular-only reads retain their separate opcode and restrictions. Ordinary
clients require the new capability and return ENOTSUP without fallback; new
owners reject legacy OPEN with ENOTSUP if it reaches the handler. The diagnostic
`wki.vfs_open_identity_off` permits testing this refusal without disabling
local credential enforcement.

Policy denials reply on the admitted channel and generation, including dynamic
VFS channels; the resource-only send helper is not used for these replies.
Transport failures are translated to syscall errno at the OPEN boundary.

Owner scalar/RDMA/bulk reads, writes and directory reads also check the retained
File access mode before file I/O; invalid direction returns EBADF. Write-only
OPEN cannot request prefetch. CLOSE still releases the channel-owned reference
without requiring a new grant. Mapped scalar and batch namespace mutations are
described below; descriptor metadata and directory-listing policy remain separate
authorization surfaces.

## Credential-bearing path operations

`WKI_CAP_VFS_PATH_IDENTITY` (`0x4000`) negotiates the additive
`OP_VFS_PATH_AS` (`0x041c`) envelope. Its first supported selector is READLINK.
The request is the canonical 80-byte credential record followed by this packed
8-byte header and exactly `data_len` operation bytes:

```text
operation:u16 | flags:u16 | data_len:u16 | reserved:u16=0
```

For READLINK (`operation=1`), the bytes are a safe relative pathname of up to
511 bytes, without a terminator. An empty pathname probes the export root.
Flag bit 0 requires a directory; every other flag is rejected. The owner maps
the decoded snapshot under policy and capabilities captured at authenticated
RX admission, then checks physical ancestor search and export confinement
before reading a retained native symlink. Worker credentials are never the
request identity. PROCFS remains unsupported pending process-identity mapping.

A successful response uses the same header with READLINK echoed, zero flags
and reserved fields, followed by exactly `data_len` target bytes (at most 512).
An error carries no body. Exact outer and inner framing, bounded response size,
selector, status and reserved fields are validated before copying to the
kernel candidate; the caller buffer is published only on complete success.
Short buffers truncate text without appending a terminator. Reading absolute
link text does not authorize following its target outside the export.

The sender serializes its admitted snapshot once across retries, bypasses the
credentialless readlink caches, and never retries the legacy READLINK opcode.
Missing negotiation and legacy READLINK return ENOTSUP. Future selectors need
explicit policy and owner implementation; unknown selectors cannot dispatch to
legacy ambient handlers. The diagnostic `wki.vfs_path_identity_off` suppresses
only this capability for mixed-peer validation.

`WKI_CAP_VFS_COMPONENT_PATHS` (`0x8000`) additionally negotiates preserved `.` and
`..` components for mapped OPEN, regular-file read, STAT, READLINK and version-2
batch STAT. It changes no payload layout. Requests remain bounded relative byte
strings without embedded NULs; only the confined owner walker interprets their
components. Owners use capabilities captured at request admission. Senders return
`ENOTSUP` for dot paths when this capability is absent and do not normalize or
retry a legacy operation. Version-3 mutation batches preserve these components;
legacy scalar paths and version-1 mutation batches are refused. `wki.vfs_component_paths_off` disables this negotiation for testing.

The owner checks search permission before `..` and rejects stepping above an
export, including attempts to leave and reenter it. Absolute symlink targets use
physical owner paths and must begin inside the export before traversal. Final
confinement and namespace checks still precede output or mutation.

Retained-FD FSTAT and scalar/batch pathname STAT use their separate negotiated
operations. FSTAT preserves the channel-scoped descriptor grant; pathname
STAT maps the supplied identity and checks ancestor search. Atomic snapshots
against concurrent writers and versioned file contents remain separate work.

## Handshake and lifecycle

A broadcast HELLO may cause only the lower configured node ID to send a targeted HELLO. Targeted HELLO and HELLO_ACK are HMAC-authenticated under the pairwise PSK and bind their full v3 payload. Each contains the sender's hardware-generated 256-bit boot nonce, boot/channel epochs, suite, key ID, stable identity, hostname, MAC, and capabilities.

The exchange is three messages. The initiator sends `HELLO` with zero challenge-echo fields. The responder verifies its PSK tag, stages derived keys without publishing a connected peer, and returns `HELLO_ACK` echoing the initiator's nonce and channel epoch. The initiator verifies that challenge, installs the same transcript-derived session, and sends `HELLO_CONFIRM` authenticated by the new directional traffic key. Confirmation uses the normal nonzero session counter: a retry after loss, or a concurrent sender reserving a counter first, must not strand the responder. Only after verifying that confirmation does the responder promote the exact pending session identified by its verified trailer and publish topology and resources. Promotion seeds the receive replay window with that actual confirmation counter; zero counters, changed pending identities, and duplicate promotion are rejected. This proves possession of the derived key before either side accepts service traffic.

Repeating the current transcript is idempotent and does not reset counters. A different nonce cannot replace an active session. Exclusive peer teardown first closes authentication admission, remembers the non-secret retired session ID, erases active and pending traffic keys, then performs existing subsystem cleanup. The retired session ID prevents a recorded old HELLO from reinstalling the erased session after fencing. Reconnect requires a fresh boot nonce/session ID; there is no downgrade transition.

## Frame and replay contract

The packed 32-byte WKI header remains at offset zero. Authenticated v3 frames append:

```text
session_id[16] | counter:u64 | tag[16]
```

The 40-byte trailer reduces application payload from 8954 to 8914 bytes while keeping Ethernet frames inside the existing 9000-byte envelope. Exact frame length is required. The tag is computed once before a reliable frame enters retransmit storage; retransmissions reuse identical bytes, counter, and tag.

The counter bitmap accepts bounded reordering, identifies exact duplicates, and rejects zero or counters more than 63 behind the high-water mark. Reliable channel sequence/generation checks remain the authority for duplicate service suppression; an authenticated duplicate cannot process a piggybacked ACK, but may reach duplicate-sequence handling to regenerate the current ACK. Session IDs fence all old counters and channel sequences after reconnect.

## Routing, RDMA, and transport rollout

Ethernet and ivshmem message-ring WKI frames use the same endpoint tag. Routed frames preserve all authenticated fields and change only TTL/CRC. Rootless `ivshmem-plain` has no interrupt doorbell, so the WKI timer polls the same fixed-budget, allocation-free single-drainer used by the IRQ path. Cluster provisioning supplies each endpoint's stable role and direct peer identity through non-secret fw_cfg fields; receive metadata can establish direct contact only when it matches the authenticated source of a TTL-one HELLO.

The ivshmem message receiver uses the common `WKI_MAX_FRAME_SIZE` capacity
(8,986 bytes including header and authentication trailer), and its MTU excludes
the WKI header. Its former 8,192-byte receive buffer truncated larger valid
VFS frames before authentication, leaving reliable requests unacknowledged.
The capacity fix preserves the fixed receive budget and single-drainer path;
all 42 native executable stability cases pass over authenticated message rings.

Raw RoCE and direct ivshmem RDMA/doorbell paths can bypass ordinary WKI RX. WKI v3 does not initialize RoCE, does not publish a dedicated RDMA transport on peers, and does not negotiate RDMA capability. RoCE receive admission checks transport publication before parsing or dispatching any opcode, including doorbells.

Current source does allow authenticated zone creation when the peer policy includes `ZONE_RDMA`; policy denial is conditional. Zone backing selection can also fall back to an RDMA-capable primary ivshmem message transport. IRQ forwarding registration has a similar primary-transport fallback, although no current C++ callers of its registration API were found in the Goal19 9GG review. These paths need further authorization and lifetime qualification; a null dedicated RDMA pointer does not establish that every direct shared-memory path is disabled. The earlier blanket zone-denial statement was stale.

Authenticated raw data access requires a separately reviewed session-bound MAC/replay record for RoCE headers, shared rings, staged data, block SQ/CQ records, and doorbells; authenticating only an rkey exchange is insufficient. Goal19 9GG records the current source map in `build/goal19-roce-receive-admission/next-recon.txt`; it does not claim complete RDMA authorization.

## Visibility and verification

`wkictl auth status` reads `/proc/wki/auth`. It reports only protocol, secure-only state, stable peer ID, key ID, policy mask, session generation, and counter high-water marks. It never exposes PSKs, derived keys, nonces, session IDs, or tags. `/proc/wki/peers` retains its exact seven-column ABI.

`tools/wireshark/wki.lua` decodes the v3 HELLO authentication and challenge fields, session ID, counter, and 128-bit tag, and marks broadcast HELLO as an unauthenticated discovery hint. It diagnoses legacy versions, oversized payloads, missing or truncated authentication trailers, and non-exact frame lengths; it does not have or display authentication keys.

Host tests cover RFC SHA/HMAC/HKDF vectors, direction and epoch/nonce/capability key separation, handshake/data domain separation, challenge confirmation, protected-field and payload tamper, mutable TTL/CRC, replay/reordering, malformed policy messages, per-service permission separation, private pairwise provisioning, and secret-free QEMU arguments. KTEST and multi-node rootless commands use `--no-setup`.

The 2026-09-01 live rootless evidence used `configs/cluster_wki_auth_rootless.json` for a three-node Ethernet topology with no direct node-1/node-3 link. Direct peers and the routed endpoint authenticated, discovered resources, mounted VFS exports, and recovered after a node-3 QMP reset; the direct peer reconnected after about 65 seconds and the routed endpoint after about 66.5 seconds. `configs/cluster_wki_auth_mixed_rootless.json` used Ethernet plus file-backed `ivshmem-plain`; after both Ethernet links were disabled and node 2 was reset, the old session was fenced and a fresh authenticated session, resource catalog, and VFS mount recovered over ivshmem alone. Raw RoCE and direct ivshmem RDMA remained disabled as required above.

The same host measured the per-frame primitive with an AMD Ryzen 9 5950X. Seven rounds of 500,000 64-byte frames averaged 136.11 ns for CRC and 1,399.97 ns for authentication (1,263.86 ns incremental, 10.29x, 43.60 MiB/s authenticated). Seven rounds of 100,000 1,400-byte frames averaged 2,316.47 ns for CRC and 6,022.60 ns for authentication (3,706.12 ns incremental, 2.60x, 221.69 MiB/s authenticated). These are microbenchmark costs, not end-to-end network throughput.

Reproduction commands:

```sh
bin/wos-cluster --config configs/cluster_wki_auth_rootless.json --launch --no-setup
bin/wos-cluster --config configs/cluster_wki_auth_mixed_rootless.json --launch --no-setup
bin/wos-ktest --no-setup
```

Final validation on 2026-09-01 produced:

- Auth crypto/protocol/policy targets: 14 tests passed, including terminal counter exhaustion.
- Cluster provisioning model: 21 tests passed.
- Complete host suite: 172 of 174 passed; every WKI/auth and Wireshark-dissector test passed. The two failures are unrelated existing syscall-usercopy audits: missing `GET_IMAGE_CATALOG` manifest coverage and raw VM access in `modules/kern/src/syscalls_impl/shm/shm.cpp`.
- Normal Build WOS: passed and packaged the current kernel.
- Isolated `bin/wos-ktest --no-setup`: 8,111 passed, 0 failed under KCFI, report-mode KUBSan, KASan, KCOV, selftests, network tracing, and allocation provenance diagnostics.


Goal 19 receiver identity validation used a four-node cluster with default-deny,
nonroot shared-namespace, and root-inclusive shared-namespace receivers. Eighteen
strict remote-exec cases passed, including 16 KiB arguments that require task
fragmentation: both restrictive receivers returned EACCES, and the trusted root
receiver executed the payload on its own node. Authentication, connected peer
state, and visible executable exports were checked before the matrix. The run
also verified bounded init retry of mount EAGAIN under concurrent WKI namespace
publication; all four nodes pivoted to their rootfs, including two that retried.
This does not qualify the still-pending full credential record or mapped VFS and
interpreter/cache access policy.


Executable cache reuse now additionally requires all candidate bytes and exact
EOF to match uncached reads from the newly admitted source File. A shared buffer
reference remains pinned across comparison outside the compute lock; cancellation
and deadlines abort validation. Metadata equality alone grants no byte reuse.
Explicit credential readers bypass per-open data/stat caches internally; this
does not change legacy OPEN wire flags. Retained owner-FD FSTAT is now negotiated separately. Atomic snapshots against
concurrent writes and remote file-version support remain separate requirements.


Mapped REALPATH uses `OP_VFS_PATH_AS` selector 5 with the unchanged 80-byte
credential record and 8-byte path header. PATH and STAT identity capabilities
are both required; dot components additionally require preserved-component
negotiation. The owner maps the admitted snapshot once and resolves the complete
raw remainder beneath its export, including symlink targets and search before
`.` or `..`. The successful reply contains canonical export-relative text
without a NUL, at most 511 bytes; empty text denotes the export root. Both sides
validate its shape. The client retains its mount during the RPC and validates
namespace, mount and task context before composing and publishing visible text.
Errors preserve output and never fall back to legacy READLINK/STAT traversal.
The diagnostic `wki.vfs_realpath_off` returns ENOTSUP for this selector; older
receivers may reject its unknown selector with EACCES. PROCFS identity, broader
routing/overmount races and remaining mutation mapping are still pending.

## Mapped chmod (Goal 19, 9DC)

`OP_VFS_PATH_AS` selector 6 carries the existing 80-byte immutable credential
record, an eight-byte `VfsPathIdentityHeader`, then `{mode:u32, directory_fd:i32}`
and up to 511 raw path bytes. Descriptor -1 addresses an export-relative path
(which may be empty for the export root); a positive descriptor addresses a
nonempty path relative to a retained directory on that exact channel. Success
returns the matching path header with zero result bytes; errors have no body.

The client requires `RESOURCE_FLAG_VFS_CHMOD_IDENTITY` (0x40) in an exact live
resource observation plus the path-identity capability. It submits once on the
selected lane, with no read-retry or legacy fallback. The receiver requires
VFS_WRITE and explicit same-domain mapping; root propagation still requires
IDENTITY_ROOT. The owner uses the supplied snapshot for every ancestor and
backend owner/group check. The lookup preserves raw components, final-symlink
and directory requirements, and retained directory identity across rename.

Legacy OP_VFS_CHMOD now returns ENOTSUP before filesystem work. Before 9DC,
a native owner/non-owner check showed that this callerless request borrowed
the serving worker's root credentials. Earlier notes describing remote chmod
as merely unavailable were stale. Other unmapped metadata operations remain
separate migration work.

### Goal 19 9DD: mapped timestamp changes

VFS resources advertise `RESOURCE_FLAG_VFS_UTIMENS_IDENTITY` (`0x80`) for
`VfsPathOperation::UTIMENS` (7) within `OP_VFS_PATH_AS`. The existing 80-byte
credential record and 8-byte path header precede a packed 40-byte prefix:
directory FD, zero reserved word, and signed 64-bit atime/mtime seconds and
nanoseconds. A -1 directory FD selects the export-relative path; a positive FD
retains that exact channel's directory description. Raw path length is 0..511,
with zero allowed only for the export root. FOLLOW_FINAL and REQUIRE_DIRECTORY
are the only path flags. The success reply is a matching path header with no
operation data; errors contain no data.

Null timestamp arguments encode NOW/NOW. NOW and OMIT markers survive transport,
so the owner chooses the clock and distinguishes write-access NOW from
owner-only explicit or mixed updates. Framing and mapped VFS_WRITE plus
IDENTITY_SAME_DOMAIN policy precede filesystem work; root propagation still
requires the separate root mapping policy. Exact resource generation and
incarnation support is checked before submission. The request is submitted once
on its retained lane, with no legacy fallback. `OP_VFS_UTIMENS` returns ENOTSUP.
The retained owner backend validates search, ownership, time intent and final
directory requirements under its existing locks. No worker credential store is
changed. Runtime qualification is recorded in the Goal19 ledger.

### Goal 19 9DE: mapped directory creation

`OP_VFS_FEATURES` (`0x041e`) queries the exact VFS channel binding with a
packed four-byte version/reserved request. Its sixteen-byte response contains
version, length, reserved and a 64-bit feature mask. Version 1 advertises
`WKI_VFS_FEATURE_MKDIR_IDENTITY` (bit 0). Both envelopes require exact framing;
the caller validates the response before submitting any mutation. This query
uses the existing authenticated VFS write policy and grants no caller identity.
It does not change the full HELLO capability field or resource flag byte.

`VfsPathOperation::MKDIR` (8) carries the caller's 80-byte credential record,
eight-byte path header, and an eight-byte mode/directory-FD prefix followed by
0..511 raw path bytes. FD -1 selects the export-relative path; a positive FD
retains that exact channel's directory description and requires a nonempty
path. REQUIRE_DIRECTORY is the only accepted flag. The owner maps VFS_WRITE
and IDENTITY_SAME_DOMAIN before confined lookup and applies existing parent
search/write and UID/GID/setgid inheritance policy. Caller umask is applied
once before submission. Worker credentials and umask are never substituted.

Support query and one mkdir submission use a captured complete channel
identity. Rebinding cannot reuse a prior support result. Missing support or
invalid replies prevent mutation; legacy `OP_VFS_MKDIR` returns ENOTSUP, with
no fallback or ambiguous mutation replay. Older owners may return ENOTCONN
from their generic dispatcher, which the client preserves. Success returns
only the matching path header. Runtime qualification is recorded in the
Goal19 ledger; other callerless namespace mutations remain pending.

### Goal 19 9DF: mapped symlink creation

The version-1 VFS feature response adds `WKI_VFS_FEATURE_SYMLINK_IDENTITY`
(bit 1, value 2). `VfsPathOperation::SYMLINK` (9) carries the existing 80-byte
credential record and eight-byte header, then a packed eight-byte prefix:
signed directory FD, unsigned 16-bit target length, and zero reserved field.
The remaining bytes contain literal target text followed by the raw destination
path. Each is independently bounded to 0..511 bytes. FD -1 permits an
export-relative destination; a positive exact-channel directory FD requires a
nonempty relative destination. REQUIRE_DIRECTORY is the only accepted flag.
Target text may contain slashes, dots and remote-looking paths; it is copied,
never resolved, and embedded NUL bytes are rejected.

The client copies credentials, target and destination before its read-only
feature query. Query and single mutation share one complete channel identity.
Missing symlink support returns ENOTSUP without sending the mutation. The owner
maps VFS_WRITE plus IDENTITY_SAME_DOMAIN before retaining the directory and
performing confined CREATE_SYMLINK lookup. Existing backend parent policy sets
caller UID and effective/parent-setgid GID; symlink mode stays0777 regardless of
umask. Legacy OP_VFS_SYMLINK is refused. Successful replies contain only the
matching path header; errors contain no data. There is no callerless fallback
or ambiguous mutation replay.

Deferred autoattach now publishes mount paths directly. It no longer calls
mkdir on host roots, which could wait for a feature response while the same
worker needed to drain a queued withdrawal. Mount-table children already
supply the visible directories. Runtime qualification is recorded in Goal19.

### Goal 19 9DG: mapped file and directory removal

`WKI_VFS_FEATURE_REMOVE_IDENTITY` is feature bit 2 (value 4). The REMOVE selector
(10) carries the credential record and path header, then an eight-byte packed
prefix: signed directory FD, 16-bit flags, 16-bit zero reserved field. Flag 1
selects directory removal; other prefix flags are invalid. The header allows
only REQUIRE_DIRECTORY, and the final symlink is never followed. Raw path
length is 0..511 bytes; positive directory FDs require a nonempty path, while
FD -1 addresses the export-relative path. Success contains the matching header
and no result bytes.

The feature query and single mutation use the same complete channel identity.
Missing support returns ENOTSUP before removal is submitted. The owner requires
VFS_WRITE and IDENTITY_SAME_DOMAIN, maps one immutable snapshot, retains any
exact-channel directory FD, and checks confined parent/target removal policy.
Legacy OP_VFS_UNLINK and OP_VFS_RMDIR return ENOTSUP. No callerless fallback or
ambiguous mutation retry is permitted. Mapped rename and batch mutations are
described below; descriptor-metadata identity migration remains pending.

### Goal 19 9DI: mapped rename across directory bindings

`WKI_VFS_FEATURE_RENAME_IDENTITY` is feature bit 3 (value 8). PATH_AS selector
RENAME (12) carries one credential record, a zero-flags path header and a
28-byte packed prefix: source and destination directory references, a 16-bit
source length and 16-bit directory requirements (bit 0 source, bit 1 destination).
Each reference is 12 bytes: signed 32-bit FD, 16-bit channel ID, zero 16-bit
reserved field and 32-bit owner channel generation. FD -1 requires zero channel
and generation and selects an export-relative path. A positive FD requires a
dynamic channel, nonzero generation and nonempty relative path. Both raw paths
are independently bounded to 511 bytes. Success returns only the matching header.

DIRECTORY_BINDING (11) takes a positive signed 32-bit FD on its exact channel
and returns that directory's 12-byte owner reference. Both selectors require
VFS_WRITE and IDENTITY_SAME_DOMAIN. The generation belongs to the owner; no
kernel pointer or consumer-local generation is serialized. The client copies
both paths and one snapshot before waiting, holds directory IO locks in a
stable order, and queries feature support on the exact mutation binding. It
then queries each retained directory binding and submits rename exactly once.
An unsupported owner returns ENOTSUP before submission. Legacy OP_VFS_RENAME
is refused, with no callerless fallback or uncertain mutation replay.

The accepted owner worker borrows only live sibling bindings with the same
consumer, resource incarnation, export device/path and publication revision.
It acquires binding references under the server lock, releases that lock, then
retains exact-channel directory Files under the VFS lock. Files and binding
pins survive the confined two-parent commit; Files release before binding pins.
Retirement, epoch reset and pending detach reject new sibling acquisition.
The supplied mapped snapshot authorizes both parents, sticky replacement and
moving-directory access through the existing native rename transaction.


### Goal 19 9DJ: mapped metadata mutation batches

OP_VFS_METADATA_BATCH keeps its opcode and eight-byte header. Mutation wire
version 3 adds the canonical 80-byte credential record immediately after the
header for CREATE_CLOSE, UNLINK and RENAME. The existing per-item path-length
encoding and signed status replies remain unchanged; STAT keeps version 2 and
its status/Stat replies. Version 1 is refused before item execution. Unknown
operations are rejected by reliable RX policy classification; all three mutation
operations require VFS_WRITE and IDENTITY_SAME_DOMAIN.

`WKI_VFS_FEATURE_BATCH_MUTATION_IDENTITY` is feature bit 4 (value 16). The client
copies its credential record and complete mutation request before querying this
feature on the exact channel used for submission. Missing support returns
EOPNOTSUPP before setting the mutation-attempted flag. Existing callers may use
mapped scalar operations only after such a pre-submit refusal; any EOPNOTSUPP
returned after mutation submission becomes EPROTO, preventing scalar replay.
Mutation sets are never split: all 64 or fewer items, including the new record,
must fit one request. Read-only STAT retains its existing 60-item reply chunks.

VFS captures one caller snapshot and preserves raw remote ancestor components
for each operation-specific lookup. The owner preflights every item's lengths,
components and export boundary, decodes and maps the snapshot once, then calls
the explicit mapped owner open/unlink/rename APIs. CREATE_CLOSE now preserves
the actual open errno and uses caller ownership, parent-setgid and supplied
umask-adjusted mode. The worker's identity is never installed as caller authority.

A batch has per-item outcomes rather than transaction-wide atomicity. An allowed
item can succeed alongside a denied item; the denied item's objects and parents
remain unchanged. Existing mount-generation checks, completion-ambiguous status
and cache invalidation remain. The syscall version/layout, usercopy-before-effect
ordering, and pre-submit fallback for unsupported paths are unchanged.


### Goal 19 9DK: retained descriptor truncation authority

OP_VFS_TRUNCATE retains its 12-byte FD/length request and existing response.
The owner requires that exact request length, resolves the File through the
accepted complete channel identity and pins it under the VFS lock. Outside the
lock it calls `vfs_ftruncate_file`, which checks the description's original
write grant before backend effects; it then releases the File. Missing FDs
return EBADF and malformed lengths return EINVAL. Backend errno is preserved.

The client performs the same write-grant check before growth accounting,
write-behind flush or cache invalidation. Later credential or pathname changes
do not revoke an established writable description. No caller identity record,
feature bit, wire layout change or mutation retry is introduced. Open-time
O_TRUNC is applied by mapped owner OPEN/OPEN_AT and is not repeated through the
returned description. Other descriptor metadata remains pending; phased xattr
caller identity is mapped by 9DM below.


### Goal 19 9DL: xattr owner API prerequisite

VFS now exposes explicit-snapshot confined path and retained native File xattr
APIs. They preserve current-caller metadata DAC and reject recursive remote
owner dispatch. These APIs supply the explicit caller authority used by the
versioned transfer below.


### Goal 19 9DM: mapped phased xattr authority

OP_VFS_XATTR uses version 2. Its fixed phase/BEGIN/DATA/result sizes remain
20/40/28/28 bytes. BEGIN is followed by a canonical 80-byte credential record,
then exact path and name bytes. Former BEGIN reserved byte 37 is
`require_directory`; bytes 38–39 remain zero. PATH uses FD -1 for the export
root, or a nonnegative exact-channel directory FD with a nonempty relative
path. FD targets require no path and no directory flag.

The client captures one Snapshot and one complete channel binding. A validated
FEATURES response with `WKI_VFS_FEATURE_XATTR_IDENTITY` (bit 5, value 32) must
precede operation-ID allocation and BEGIN. Every subsequent phase uses that
same binding. Missing support returns EOPNOTSUPP without an xattr request;
version-1 requests are refused by the new owner. No fallback or new-identity
mutation replay occurs after an uncertain result.

GET/LIST require VFS_READ; SET/REMOVE require VFS_WRITE. Every phase also
requires explicit same-domain identity policy. BEGIN decodes and maps the caller
using RX-admitted policy before replacing a stage. The stage retains the mapped
Snapshot, canonical record, and any exact-channel File pin. Duplicate BEGIN
comparison includes caller bytes, numeric FD, directory requirement, and all
existing request fields. Existing bounded staging, DATA replay and terminal
results remain.

Owner calls use explicit-caller confined VFS APIs. Paths retain raw ancestor
search and final directory requirements. SET/REMOVE resolve at COMMIT under the
BEGIN snapshot; GET size/read lookups independently authorize that snapshot.
No lookup lock is held across transfer waits. Existing 64 KiB value limits,
user.* remote namespace filtering, XFS symlink rules and errno remain unchanged.

Functional qualification: 290 current-owner checks pass, including 244 DAC and
trailing-directory controls and 46 path, transfer and retained-File checks.
Eight old-owner refusals and an owner-side unchanged-attribute control pass.
Remote cwd lifetime is a separate known gap: `service_wki_xattr.py
--cwd-regression` preserves four failing fchdir/rename/reuse checks. Directory
FD transport support does not imply that remote cwd state retains that FD.


### Goal 19 9DN: native prerequisite for retained remote cwd

The confined VFS owner API can now pin a TMPFS/XFS directory using the mapped
caller's search authority alone. Its pathless File retains the native inode and
mount without granting data reads or directory enumeration. Ordinary OPEN
continues to reject its internal access mode. Child operations use their own
explicit caller snapshot and recheck search/confinement on the retained object.

WKI does not yet publish or retire these directory pins. Integration still needs
an exact-binding mapped request, bounded cancellation and lost-reply cleanup,
current-path recovery, and cwd inheritance/exit lifetime handling. Existing
remote fchdir/rename/reuse failures remain pending; this native API is not a
remote cwd fix or a new wire capability.


### Goal 19 9DO: confined native current-name recovery

The native owner API `vfs_directory_path_beneath` derives an export-relative
current name from a retained directory inode, never from cached File path text.
Namespace publication covers parent recovery and the export-boundary check.
Rename/reuse preserves the selected object; moving it outside the export or
removing it produces an error with unchanged output. Read-only mounts are
supported. Like native getcwd, querying an already-held directory name does not
reauthorize search or grant child access after chmod.

This supplies the current-name primitive needed by remote cwd. The WKI worker
must retain and authorize the exact directory binding before calling it, outside
the VFS registry lock. Transport, durable pin cancellation and cwd lifecycle
integration are still pending. No new WKI capability is advertised in 9DO.


### Goal 19 9DP: mapped search-only directory references

`OP_VFS_DIRECTORY_REFERENCE` (0x041f), version 1, is advertised by FEATURES bit
6 (value 64). Its packed 24-byte request is version:u16, operation:u16,
directory_fd:i32, token:u64, path_length:u16, reserved:u16, reserved2:u32.
All reserved fields are zero. PIN=1 and PATH=2 append the canonical 80-byte
caller record. PIN then appends up to 511 raw relative path bytes: FD -1 starts
at the export, while a positive exact-channel directory FD requires a nonempty
relative path. PATH requires a positive pin FD and no path. CANCEL=3 requires
FD 0 and no caller record or path; its nonzero token identifies existing ownership.

PIN/PATH require VFS_READ and explicit same-domain identity policy. The worker
decodes/maps the caller under RX-admitted policy before native work. PIN calls
the confined search-only owner API; PATH queries the exact retained pin's current
export-relative name without granting new search/read access. An active duplicate
PIN token returns EALREADY without publishing or retargeting another reference.
The client never retries PIN, and generated tokens are not reused. Reliable
ordered worker admission places cancellation after its corresponding request.

The packed 24-byte successful reply is version:u16, reserved:u16, fd:i32,
token:u64, path_length:u16, reserved2:u16, reserved3:u32, followed by 1–511
canonical absolute export-relative name bytes, without a NUL. The client checks
version, token, positive FD, all reserved bytes, exact length and canonical name
before copying caller output. PATH additionally checks its existing FD. CANCEL
and errors have no payload. Request/reply framing is exact at RX and deferred
worker admission; no recursive REMOTE owner lookup is used.

The client captures one Snapshot and channel binding for feature negotiation and
PIN. File/context allocation and cleanup-slot ownership precede PIN publication.
Unknown or malformed replies retire by the known token, never reply FD bytes.
Directory and execution-view tokens occupy separate owner fields. Their client
cleanup records share the existing bounded capacity of 1024, with a purpose tag
and separate diagnostic totals. Existing execution-view wire behavior is unchanged.
A failed cancellation retains its proxy reference and retry record until an ACK
or proven exact-binding/boot-epoch retirement. CANCEL needs no new data authority,
and its ACK follows release of the registry reference outside the lock.
Independent admitted operation/stage references remain valid; the last release
closes the native File. Directory cancellation, ordinary CLOSE, and binding
teardown all use this reference-aware retirement for search-only pins.

Returned directory Files are pathless and search-only. Client read/write/readdir/
truncate callbacks reject them with EBADF before cache, growth or network work.
Native owner callbacks retain their matching guards. Connected idle descriptions
are already excluded by stale-FD GC; disconnected cleanup drains binding work
before close. No Task/cwd publication is added here: those integration and live
multi-node transport checks were pending in 9DP; see 9DQ below.


### Goal 19 9DQ: Task cwd integration

VFS now retains directory-reference Files in Task cwd anchors. PIN, PATH and
final cancellation run outside namespace publication and Task path locks. Each
chdir/fchdir attempt uses the operation's single captured caller Snapshot;
relative operations map their own current caller against the retained directory.
Owner-issued directory identity survives rename/reuse and source-FD closure.
The existing 294-check xattr fixture, including four cwd regressions, and the
new 25-check cwd lifetime/permission fixture pass on the live two-node cluster.

The registered export is still the owner confinement boundary. VFS refuses a
remote cwd or directory-relative request if its export mount is not wholly
beneath the Task root. Supporting a root narrower than an export requires an
owner-enforced protocol boundary, not a preceding name query. Older owners must
refuse cwd PIN without fallback. Full fence/lost-reply/concurrency qualification
and remaining VFS authorization migration are still pending.

The mixed-version fixture also passes six old-owner checks: both chdir and
fchdir fail with EOPNOTSUPP without changing cwd or contents, and the ordinary
source descriptor remains closable. The full isolated KTEST run passes45,090
checks with zero failures, including cwd retirement outside publication.


### Goal 19 9DR: retained-directory REALPATH

PATH_AS selector13 REALPATH_AT requires VFS_READ plus same-domain identity
mapping, and negotiated PATH/STAT identity capabilities. FEATURES bit128 is
advertised only while both realpath and directory-path support are enabled.
The request is canonical80byte caller record + existing8byte path header +
positive4byte directory FD +1..511 raw relative path bytes. Header flags allow
only REQUIRE_DIRECTORY. Replies use the existing canonical0..511byte
export-relative text format; errors carry no output.

The owner maps the supplied caller before retaining the exact channel File and
uses the confined native realpath adapter with that File. Rename/reuse and
ancestor permission changes cannot substitute a pathname lookup. Client feature
query and operation share one captured binding, with no reconnect/path fallback;
output is copied only after exact reply and canonical-name validation. Existing
scalar REALPATH remains compatible. Narrower-root confinement, remote Task cwd
transfer and broader loss/fence/concurrency qualification are still pending.

The mixed-version fixture confirms six refusal/lifetime cases against an older
owner that supports directory pins but lacks REALPATH_AT. The full isolated
rerun passes51,284checks with zero failures. A prior run ended with two failures
while /tmp quota exhaustion cut off its serial evidence; the unchanged rerun
used workspace-backed overlays and logs. See Goal19 9DR for the preserved record.


### Admitted File lifetime during registry retirement (Goal19 9DT)

An owner-side registry row and an admitted transfer stage hold independent File
references. Exact-binding CLOSE, channel/peer cleanup, executable cancellation,
and cleanup after a failed published OPEN reply must release only the registry
reference. The row becomes unavailable immediately, while existing admitted
operations retain their inode and credential snapshot through completion.
Executable cancellation likewise leaves writer exclusion active while an
admitted reader still owns the immutable view. No operation regains FD admission
by retaining a reference, and no cleanup falls back to pathname selection.

The last reference runs backend close exactly once outside the registry lock.
Synchronous final CLOSE returns that backend result. Nonfinal retirement returns
zero without waiting for later phased requests; an eventual close error is logged
by existing VFS destruction and is not retroactively returned to an earlier CLOSE.
No wire format, capability, feature bit, credential mapping or token scope changes.

### Mapped descriptor metadata (Goal19 9DU)

PATH_AS adds FCHMOD14, FCHOWN15 and FUTIMENS16 under resource feature256
(WKI_VFS_FEATURE_FILE_METADATA_IDENTITY). Existing selector values, capability
bits and layouts are unchanged. Requests contain the canonical80-byte caller,
the existing8-byte identity header and an exact8/12/40-byte operation body.
Bodies carry a positive owner FD and mode, owner/group or two raw timespecs.
Flags and reserved fields must be zero; no pathname suffix is accepted.
Success returns the same selector in an otherwise empty identity header;
failure returns the existing negative status without a result body.

All three operations require VFS_WRITE and explicit same-domain identity
mapping. Deferred admission supplies the binding and peer policy; the worker
never borrows ambient task credentials. After decoding and mapping, the owner
retains the exact binding-scoped registry File, drops the registry lock, and
calls its native explicit-Snapshot metadata adapter. Recursive remote Files
return EXDEV and private executable views return EROFS. Native owner, group,
authority-ceiling, read-only-mount and timestamp rules remain authoritative.

The client holds its File and serializes its remote context through one
read-only feature query and one mutation on the same complete channel identity.
Unsupported owners are refused before mutation. There is no path reconstruction,
lane reselection or ambiguous-outcome replay. Timestamp input is checked before
transport while NOW/OMIT intent is preserved for owner-side clock conversion.
Only successful changes invalidate the exact context and File metadata cache.
Independent owner references remain valid through concurrent registry retirement
under the9DT lifetime contract; no reference grants new registry admission.


### Mapped pathname ownership (Goal19 9DV)

PATH_AS CHOWN17 requires resource feature512 (WKI_VFS_FEATURE_CHOWN_IDENTITY)
and VFS_WRITE plus IDENTITY_SAME_DOMAIN admission. The existing canonical80-byte
caller and8-byte identity header precede a packed12-byte prefix: owner:uint32,
group:uint32, directory_fd:int32, then0..511 raw path bytes. UID/GID UINT32_MAX
sentinels are preserved. Only FOLLOW_FINAL and REQUIRE_DIRECTORY flags are
allowed; reserved must be zero. Directory FD is -1 for export-relative paths
or positive with a nonempty relative path. Success has an empty identity result
with selector17; errors have no result body. Existing selectors/layouts are unchanged.

The client retains its chosen proxy or directory context through a feature query
and one mutation on the same complete binding. Missing support, generation loss
or malformed replies cannot trigger a callerless/pathname fallback or mutation
replay. At the owner, exact framing and canonical credentials are validated and
mapped using admission-time peer policy before any directory lookup. A retained
registry File supplies directory identity across rename/reuse; the registry lock
is released before confined native lookup/commit. Worker task credentials supply
no authority. Existing owner/group, ceiling, symlink, mount and cache policy
applies through the shared native ownership consumer.


### Scalar chmod/timestamp binding continuity (Goal19 9DW)

Existing mapped chmod and timestamp senders now capture the complete channel
identity before resource support admission, then require that exact identity
for the single PATH_AS send. Retained-directory operations keep the directory's
I/O lock; scalar operations keep the chosen proxy lifetime reference. The VFS
registry lock is held only while copying the identity and is released before
support checks or network waits. A changed binding fails through the existing
exact-identity send contract. Existing resource support flags, caller record,
selectors, layouts, timestamp intent and result validation are unchanged.
Public pathname chmod also retains one caller through native lookup retries and
returns remote results directly instead of retrying a mutation on EAGAIN.


### Mapped remote NIC OPEN (Goal19 9EJ)

NET attach ACKs advertise DEV_ATTACH_NET_OPEN_IDENTITY (0x0040) in the
NET-specific rdma_flags field. The existing 36-byte ACK and HELLO capability
layout stay unchanged. An older owner without this bit returns ENOTSUP locally
before an OPEN request is sent; there is no credentialless fallback.

OP_NET_OPEN_AS (0x0308) carries exactly 88 data bytes: a packed 8-byte header
(cookie:uint16, version:uint16=1, reserved:uint32=0), then the canonical 80-byte
credential record. The cookie must be nonzero. The consumer passes the syscall's
original admitted snapshot through NetDeviceOps::open_as. Native driver open
callbacks retain their existing signature.

The owner requires REMOTE_NET and IDENTITY_SAME_DOMAIN peer policy, maps the
record using that admitted policy, then checks NETWORK_ADMIN before invoking
the native driver. Existing SAME_DOMAIN_ROOT trust is still required for root
identities or active service privileges; SAME_DOMAIN does not gain those grants.
The fixed shared device worker retains the exact NET binding and copies the
request and admitted policy before reliable ACK. Driver calls run in task
context, with no ambient worker credential authority. Exact operation cookies
are echoed on both worker results and peer-policy refusal.

Legacy owner OP_NET_OPEN now returns ENOTSUP before driver or subscription
mutation. Only successful mapped OPEN publishes opened subscription, local UP
state and initial RX credits. Caller-owned completion and transport/owner errno
separation remain intact. CLOSE, SET_MAC and combined multi-operation transaction
semantics remain separate pending authorization work.


### Mapped remote NIC CLOSE (Goal19 9EK)

NET attach ACKs separately advertise DEV_ATTACH_NET_CLOSE_IDENTITY (0x0080).
OPEN support alone does not authorize or imply mapped CLOSE support.
OP_NET_CLOSE_AS (0x0309) uses the same exact 88-byte canonical caller frame and
cookie/version/reserved layout as OPEN. Existing ACK and HELLO layouts remain
unchanged. Missing support returns ENOTSUP before sending; legacy owner CLOSE
returns ENOTSUP without invoking a driver or altering a subscription.

The optional internal NetDeviceOps::close_as callback receives the same snapshot
already admitted by NETCTL or legacy IOCTL. Both local and owner checks use
NETWORK_ADMIN. OPEN and CLOSE share map_net_control_identity, retaining the
existing REMOTE_NET/IDENTITY_SAME_DOMAIN requirements and SAME_DOMAIN_ROOT trust
for root or active service grants. The fixed device worker retains the exact NET
binding and copied admission-time peer policy before reliable ACK. No ambient
worker identity grants device authority and native close runs outside RX.

CLOSE preparation/send/wait errors retain their transport-to-errno conversion;
owner status remains a distinct errno result in caller-owned completion. The
syscall and proxy publish DOWN only after success; authorization or unsupported
refusal does not change local fields or owner subscription. Native void close
callbacks retain their existing signature. Successful owner close explicitly
publishes DOWN even for drivers whose native close callback is a no-op.
A timeout after send can still have an ambiguous owner outcome; there is no
automatic mutation replay. MAC mapping, combined-driver transactions and
concurrent post-success publication remain pending.


### Mapped combined NIC link control (Goal19 9EL)

OP_NET_LINK_AS (0x030A) is independently negotiated through NET attach flag
DEV_ATTACH_NET_LINK_IDENTITY (0x0100). OPEN/CLOSE support alone is insufficient.
The exact104-byte request data consists of the existing88-byte canonical caller
frame, followed by packed NetLinkControlPayload: fields:uint16, reserved:uint16,
up:uint32, mac:6bytes, reserved_tail:uint16. Fields1 selects UP/DOWN and2 selects
MAC; at least one is required. Unknown fields, nonzero reserved words, up>1,
nonzero up without STATE, and nonzero MAC bytes without MAC are invalid.

The same retained pre-ACK NET worker owns the exact binding, copied request and
admission-time peer policy. It maps the canonical caller and applies NETWORK_ADMIN
once before checking native MAC support. The only fallible native driver phase,
OPEN, precedes MAC commit; no fallible phase remains after OPEN succeeds.
Successful state intent updates the subscription and initial RX credits; denied
requests do neither. Owner errno stays in caller-owned completion and is not
translated as a transport error. Legacy SET_MAC returns ENOTSUP and the callerless
proxy callback has no effects. There is no fallback or automatic mutation replay.

The syscall sends combined UP/DOWN+MAC through one apply_link_as callback before
publishing local flags, MTU, queue length or a reserved name. Native combined
requests likewise check MAC support before flags/driver effects. Standalone
legacy MAC IOCTL passes its original admitted Snapshot through the same callback.
The public syscall ABI is unchanged; new hooks are internal NetDeviceOps fields.

Native check_mac is pure admission for an infallible native MAC commit while the
device and immutable driver configuration remain retained. Fixed-address drivers
accept unchanged values and reject changes. Current WOS modern virtio lacks
CTRL_MAC_ADDR negotiation, so changed MAC values return ENOTSUP rather than
writing through an uninitialized legacy I/O base. Supported legacy virtio keeps
its configuration-space MAC write path. The transport distinction follows the
[virtio1.2 network device configuration rules](https://docs.oasis-open.org/virtio/virtio/v1.2/virtio-v1.2.html).
Concurrent control writers/readers and post-send ambiguous outcomes still need
separate serialization/receipt work; one combined owner decision is not a claim
of distributed rollback or coherent concurrent observations.


Mapped NIC OPEN/CLOSE/LINK control requests now share per-device try-admission
with local link syscalls (Goal19 9EM). Identity mapping and NETWORK_ADMIN checks
precede contention; an authorized competing request receives EBUSY before any
native callback. The handler retains admission through subscription publication
and notifications. Admission uses a lock-free atomic gate and leaves interrupts
and preemption enabled during existing waits. Asynchronous owner state notifications
and public field reads do not use this gate and still require coherent publication
work. Wire layouts, negotiated feature bits and peer trust requirements are unchanged.


Goal19 9EN adds a short publication lock to proxy control admission/release and
owner notification publication. A notification received during an admitted control
request retains its latest validated owner values and is projected at completion;
zero MTU means no MTU update. Retirement fences operations immediately and wins
final DOWN publication over any late successful control or pending UP notification.
No lock spans driver work or remote waits. Notification projection, attach cache
consumption and retirement use device publication lock before proxy state lock;
retirement may enter with the proxy registry lock held. Public read snapshots and
cross-node commit ordering remain separate unfinished work. No wire or caller-mapping
change is implied by this internal publication mechanism.


Goal19 9EO makes WKI owner link metadata and resource-advertisement name/link
fields use one owned per-device configuration. An in-flight control exposes its
pre-control record. Existing notification helpers queue their own notification
types while control is active; completion releases ownership and the publication
lock before flushing those types. Owner OPEN/CLOSE/LINK retain state-only
notifications and finish before sending their response. Generic retirement makes the visible record DOWN
and prevents late native completion from restoring UP. Public interface queries
use the same record. L3 cache coherence, packet metadata and global remote commit
ordering are separate pending work; no wire or caller-mapping change is introduced.


Goal19 9EP gives post-attach sparse/all NIC policy one owned owner-address
record. The helper retains the exact live proxy and copies IPv4 address/mask
and the complete IPv6 suffix under its existing state lock. Missing or retired
proxies return invalid empty records. The policy no longer reads owner cache
fields through private_data. This preserves receive-order metadata semantics,
peer/resource lifecycle admission and the separation from local address state;
it changes no wire format or authorization policy. Native address tables and
global remote commit ordering remain separate unfinished work.


### Goal19 9FV: current process-group receiver discovery

Negotiated COMPUTE resource `0x53494703` (`process-group-receivers-v1`) adds
request/reply message types `0x60`/`0x61`. Requests require the existing
COMPUTE and same-domain identity policy and map the original caller before
enumerating canonical current receivers. Discovery carries full immutable
home identity and a separate bounded receiver cursor; it does not authorize
delivery. Exact current credential/group admission and the first selected
home endpoint remain authoritative. Existing wire layouts are unchanged,
and peers without the capability retain legacy home discovery and its limits.

The original differing-policy B→C→B regression now passes on current kernels
and with an intentionally older B. Normal/isolated builds, KTEST52195/0, five
host suites and existing current-kernel functional regressions pass. A separate
mixed-version UID100 worker completion stall remains unresolved despite a
passing same-guest repeat; preserve its pending-signal capture and netd restart
evidence. All task guests and fixture files were cleaned, synced and stopped.
See `docs/remote_signal_authorization.md` and
`build/goal19-current-remote-discovery/validation.txt`.


## Receiver-owned terminal events (Goal19 9MU)

`TASK_TERMINAL_SIGNAL` (0x62, packed 48-byte version1 payload) carries an accepted
task ID, signal, SID/PGID, full session/group origins and a bounded forwarding
count. `process-terminal-signals-v1` is advertised as COMPUTE resource0x53494704.
The operation requires COMPUTE and same-domain identity policy. Reliable RX
validates the exact payload and retains peer/channel/session identity in the
bounded compute queue. The receiver retains the accepted Task and checks current
job membership under signal admission before effects. Terminal object authority
survives UID changes; it does not survive leaving the selected group/session.

Unsupported protocol, stale route and send refusal never fall back to lifecycle
TASK_CANCEL or the parked source frame. ACCEPT/CANCEL layouts and ordinary user
signal negotiation are unchanged. Older peers without the advertisement do not
receive the new terminal events. See [the implementation and evidence](security/audits/remote_terminal_admission.md).

## Issued block access (Goal19 9OD)

BLOCK attachment requires DEVICE_ATTACH and every requested access permission.
Read/write and legacy mode-zero attachments require both BLOCK_READ and
BLOCK_WRITE. Each published binding retains its issued read permission and
existing write restriction independently of writer-lease arbitration.

Scalar/ring READ and ring BULK_READ reject write-only bindings with EACCES before
device callbacks or data transfer. Scalar/ring FLUSH requires writable access,
returning EROFS otherwise. Existing write denials, geometry validation, exact
channel/zone lifetime and wire layouts remain unchanged. See the
[implementation and native functional evidence](security/audits/block_binding_authority.md).
