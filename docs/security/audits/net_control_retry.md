# Goal19: bounded remote NIC control send admission (9OA–9OB)

## Reproduced failure and scope

9OA started from the unchanged 9NT kernel and a private sparse two-node topology:
node0 owns the LAN NIC and node3 reaches it over a separate WKI link. Existing
six-profile OPEN fixtures were used without weakening any expected errno.
The first capture passed. A subsequent bounded series stopped on its fourth
iteration: sealed netd NETCTL OPEN returned EHOSTUNREACH, while the following
legacy OPEN succeeded. The send-return breakpoint recorded WKI_ERR_NO_ROUTE
(-2), before reliable acceptance. No channel-reservation refusal was recorded.

A second capture identified the existing origin-admission branch: a connected,
direct, authenticated peer had lifecycle_state=0x80000000, owned by another task.
Even root OPEN was refused at that boundary. The copied route remained empty;
no request had been sent or owner policy consulted. Broad route breakpoints
perturbed that run enough for its host fixture to time out. That timeout is
preserved and is not counted as a successful matrix. A later attempt to identify
the writer could not exercise OPEN because the proxy was DOWN; no writer stack
or stronger causal explanation is claimed.

Without breakpoints, CLOSE succeeded for root and netd, and all exercised denied
profiles preserved state. The final netd reopen failed; the remaining profile
could not obtain its UP baseline. This observation is preserved separately from
the captured OPEN cause. Source review also found that completed NIC control
responses were attempted once and discarded on send refusal.

All original 9OA artifacts remain in `build/goal19-net-open-diagnosis/`, including
`send-capture/`, `route-capture/`, failed fixture reports and debugger cleanup.
Both exact private VMs were synced and stopped; the final host audit records
no WOS VM at 2026-09-17T01:14:42.077899+00:00.

## Implementation and invariants

The 9OB change adds a task-context send-admission helper for mapped OPEN, CLOSE
and combined LINK requests and their completed responses. It retains the same
encoded caller/request bytes, cookie and channel generation. Only errors that
occur before reliable sequence publication may retry. WKI_OK is terminal even
if its initial hardware transmit failed: WKI already owns retransmission.

Admission retries sleep 1ms and share a single 100ms deadline. They check the
retained proxy or binding retirement flag and exact channel generation before
and after sleeping. A teardown waiting for that reference can therefore stop
the retry promptly. The helper does not borrow another task's lifecycle gate.
The existing final transport errno mapping and subsequent response wait remain
unchanged; the added admission budget is at most100ms, plus scheduler delay.

Owner handlers map/authorize once and execute the driver operation once. They
release device control admission before retrying their already-completed response.
The fixed pre-ACK worker retains its exact binding until the handler returns;
it now passes that binding's retirement flag. No spinlock spans retry sleeps,
no new allocation is introduced, and IRQ/NAPI or fire-and-forget paths do not
use this helper. WKI wire layout, peer policy, authority grants, credential ABI,
declared VFS routing and waitpid collision behavior remain unchanged.

This is bounded admission recovery, not an exactly-once distributed transaction
protocol. A completed operation whose response cannot reach the caller may still
have an unknown outcome. No driver operation is replayed to resolve that ambiguity.
Authorization denial continues to precede all driver effects.

## Validation

The stage contract, before files and command receipts are under
`build/goal19-net-control-retry/`. Normal Build WOS and the isolated diagnostic
build passed, as did formatting/clang-tidy, four credential/WKI host suites,
NET and device-server source guards, and the credential policy audit/unit suite.
An initial source guard still required the old direct-send spelling; it was
updated to check that control admission ends before the bounded send helper and
that responses use the retained binding retirement flag. Its failure is retained.

The full isolated KTEST run passed **60,035 checks / 0 failures**, including the
new production-helper test. It covers
transient admission sequences, single successful acceptance, permanent errors,
a fixed deadline, and retirement before/after sleeping. Existing owner-denial,
completion-ownership and transport errno tests remain in the isolated suite.

Ten unchanged six-profile OPEN matrices passed on the rebuilt private two-node
cluster, followed by the six-profile CLOSE/reopen and combined LINK matrices:
**150 primary NIC checks**, plus restorations. Every driver report passed and
owner interface snapshots matched before/after. All temporary fixture files
were removed. These passing runs qualify the bounded recovery change; they do
not establish a cause for every earlier timeout or failed restoration.

The child-wait fixture passed all **26 checks**, for UID0 and UID104, including
positive-PID EOVERFLOW without reaping, wait-any reaping, WNOWAIT, unchanged
accounting on refusal, and EFAULT without consumption. Its sparse-topology
adapter then failed while trying to OPEN a remote procfs file, which is not a
supported way to inspect that receiver. The wrapper failure is preserved.
A separate physical-node observation used ordinary remote execution to list
/proc and check all eight recorded PIDs; all were absent, as were both helper
files. This completed cleanup verification without rerunning or changing the
functional assertions.

Both guests synced and stopped. Final host audit at
2026-09-17T01:33:09.335937+00:00 found no WOS VM. The normal kernel SHA256 is
`b2d38ae690918ba5bf0a44f5bf550aa73579ec99e8276c01075283a504a91797`;
the diagnostic kernel SHA256 is
`48a06cec6c07845d0916d896773da1950d8dbd8d928745206741f3fc78f0b32f`.

## Network/hostname requirement audit

The finite Goal19 authorization requirements for this domain are now enforced.
This reconciles earlier pending notes against current code and evidence; it does
not add a general distributed transaction or network availability requirement.

| Surface | Current authority and denial boundary | Evidence |
| --- | --- | --- |
| Hostname | One captured HOSTNAME authority; bounded input validation before whole-name publication under its IRQ-safe lock. Owned query snapshots cannot alias mutable storage. | Current Hostname KTEST; 9NZ two-node identity and 24,000-write/48,000-read matrices per node with restoration. |
| Raw IPv4/IPv6 sockets | NETWORK_ADMIN before FD reservation or socket allocation. Ordinary UDP creation remains public. | Current credential KTEST and 9NZ 156 IPv6/socket checks. |
| IPv4/IPv6 addresses and routes | NETCTL and mutating legacy IOCTL selectors capture one caller and authorize before lookup/table mutation. IPv4 uses owned locked configuration; IPv6 connected-route admission precedes address commit with exact rollback of its own reference. | Current native IPv4/IPv6 tests; 9NZ 60 IPv4 and 156 IPv6/socket checks with denied-state preservation and cleanup. |
| Link flags, name, MTU, queue length and MAC | Shared NETWORK_ADMIN precedes per-device admission. Name and native MAC admission precede fallible OPEN; local fields publish only after success. Coherent snapshots hide intermediate writes and retirement wins. | Current NetLinkPolicy and WkiRemoteNetState KTESTs; 9OB OPEN/CLOSE/LINK matrices. |
| Queue affinity | One NETWORK_ADMIN snapshot; whole-mask admission under sorted runqueue locks, no partial move on later-worker refusal; IRQ routing updates after successful commit. | 9LJ original regression and transactional KTESTs, rerun in the current full suite; 9NZ 24 live queue cases. |
| Mapped NIC control | Negotiated complete caller records and retained pre-ACK binding/policy; REMOTE_NET plus same-domain trust, root-inclusive trust for active service grants, then shared NETWORK_ADMIN before callbacks. Unsupported legacy control refuses ENOTSUP. | Current owner-mapping/denial KTESTs; unchanged six-profile live matrices cover root, user, discarded-root authority, journal, sealed netd and cleared netd. |

The 9NZ source audit and functional receipts are reused explicitly where the
underlying code is unchanged; those live address/hostname/queue fixtures were
not rerun in9OB. Current full KTEST requalifies the compiled native paths.
The new live evidence closes the previously reproduced NIC admission failure.
Historical broad claims of pending coherent link/L3 publication and queue
transactionality are superseded by their current implementations and listed
receipts. Post-send unknown outcomes remain an explicit limitation, rather than
being mislabeled as authorization denial or prompting a replay of driver effects.

The manifest changes only `network-hostname` from pending to enforced:
**19 enforced / 2 pending**. WKI identity and resource domains still require their
own current-source requirement audits. The original goal string is unchanged.
