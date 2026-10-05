# Goal19 9NZ: IPv6 control and socket authority qualification

Previous turn9NY completed the signal authorization audit. Goal19 is active at
18 enforced/3 pending. Network/hostname is the next pending domain.

## Reconnaissance and test contract

Current `abi/callnums/net.h` exposes both IPv6 address APIs and route set/delete,
in addition to the older IPv4/link/queue controls. `sys_net.cpp` copies each
request and captures one credential snapshot; every mutating branch calls
`authorize_network_administration` before looking up a device or changing state.
Raw IPv4/IPv6 socket creation checks that policy before FD reservation or socket
allocation. Ordinary UDP socket creation and interface/address/route queries are
public. Hostname uses the distinct HOSTNAME authority and an atomic owned name.

Source read: network syscall enum/dispatch, local `wos/netctl.h` ABI, current
IPv4/IPv6 interface setters and snapshots, route6 publication/reference helpers,
netdevice control/name admission, remote NIC mapped OPEN/CLOSE/LINK controls,
credential policy, hostname syscall/store, and the existing live IPv4/hostname/
queue fixtures and kernel IPv6/link/remote-NIC tests. The network manifest's
older evidence does not explicitly qualify the newer public IPv6 control APIs.

Flow: copied request -> stable caller snapshot -> central policy -> exact retained
device -> interface/route validation and commit -> owned query observation.
Address generation detects conflicting updates; a refused IPv6 commit releases
only its connected-route reference. Route publication orders registry lifetime
before its route lock. Notifications run after address locks. Remote control
maps authenticated peer identity before native driver callbacks; transport
uncertainty is separate from an owner authorization denial.

This stage changes standalone fixtures and evidence only. No production patch,
new lock or ABI/wire change is planned. Use six existing authority profiles:
root, ordinary UID1000, root with a discarded ceiling, journal grant, network
grant, and network grant with a discarded ceiling. The root and sealed network
grant may change local network configuration; the other four must receive EPERM.
Each profile creates/closes ordinary UDP and raw sockets without sending traffic,
then exercises IPv6 v1/v2 address and route controls on loopback. Exact snapshots
must show no denied effects. Unique documentation-prefix /128 entries have no
automatic prefix route or duplicate-address probes; parent cleanup removes only
those entries and verifies the original loopback address/route records.

Run locally on source and after ordinary mapped exec on the private receiver.
Record child identities and verify their absence, reports/helper removal and
exact private VM shutdown. Reuse the verified9NT kernel/private boot image;
compile/format the fixture and run relevant host/source/policy checks. No new
KTEST is necessary unless a production change or new concern requires it. The
prior60034/0 run contains actual IPv6 transaction and network policy tests.

Rollback: retain before copies and a stage-only patch under
`build/goal19-network-ipv6-authority/`. Failures remain evidence and require
source diagnosis before changes. This is a functional OS authorization fixture,
not a packet or penetration test. Declared VFS routing and UID/GID ABI remain.

## Results

The new fixture passes **156 checks**, 78 on each node, on the unchanged9NT
normal kernel. Six profiles each cover ordinary UDP and raw IPv4/IPv6 socket
creation, v1 address add/duplicate/delete, v2 refresh/invalid lifetime, route
add/invalid/delete, and absent-address refusal. No traffic is sent by the socket
checks. Root and the sealed network grant succeed; all other profiles receive
EPERM for privileged operations. Exact loopback snapshots are restored after
every profile. All14 recorded parent/child process probes are absent and all
three helper/report paths are confirmed removed.

Fresh existing regressions also pass: **60 IPv4 checks**, **24 queue-affinity
checks** with no busy retries, and hostname checks on each node (four identity
profiles,24,000 writes and48,000 reads per node, original names restored). Queue
affinity restores its explicitly normalized disposable-VM baseline, not the
initial descending CPU layout; the existing fixture records both.

Mapped NIC OPEN/CLOSE/LINK did not pass. Their initial run returned97 before any
profile because the two-LAN topology had no eligible UP owner proxy. Receiver
serial output explicitly records subnet-overlap rejection. These are preserved
precondition failures, not operation-level authorization results. A second,
private sparse cluster reproduces the earlier fixture topology: source LAN plus
a separate WKI link, receiver WKI link only. It obtains `wki-wos-0-e0` and reaches
the actual operations:

| Fixture | Observed result |
| --- | --- |
| OPEN | Root's two APIs succeed. Four denied profiles return EPERM with unchanged records. The network-grant profile receives EHOSTUNREACH113 for both APIs, also with unchanged records. Fixture exits1. |
| CLOSE/reopen | Root's NETCTL call returns ETIMEDOUT110 and its expected-state check fails, followed by a successful restoration observation. Root's legacy call succeeds. The next three denied profiles preserve/restore state. The network-grant NETCTL call returns ENOTCONN107 and restoration fails; that child and the final profile subsequently exit91. Fixture exits1. |
| Combined LINK/MAC | Stops at97 because an UP proxy is no longer available after CLOSE. It provides no new link-operation result. |

The runners' failure cleanup removed their temporary files. The final read-only
peer snapshots show both nodes connected. Network diagnostics show source eth0
DOWN, its separate eth1 WKI transport UP, and receiver proxy ifindex5 DOWN. The
source serial log records detach/re-attach to channel114 after the CLOSE failure.
Neither unchanged denied snapshots nor eventual peer connectivity explains the
allowed-operation failures. The sparse network configuration was not fully
restored; its private VMs were synced and discarded, with no base-image mutation.

The original two-LAN QEMU processes3688276/3688279 are gone at
`2026-09-17T00:47:47.521987+00:00`. Sparse processes3690123/3690143 are gone at
`2026-09-17T00:52:38.339908+00:00`; the final host audit finds no WOS VM. Both
launchers exit0. Exact process start times, private backing-chain checks, guest
sync and QMP quit receipts are retained.

Normal Build WOS (`scripts/dev/build_wos.sh` with the current task environment)
passes after shutdown. The normal and diagnostic kernel hashes remain those of
9NT; no new kernel compilation or KTEST run is claimed. The previous60034/0 KTEST
contains network policy, link transactions, mapped identity/completion ownership
and IPv6 connected-reference/full-table refusal tests. Fresh credential/wire/auth
host suites, network usercopy, remote NIC, table lifetime and virtio source guards,
fixture formatting/compilation/Python syntax and policy gates pass.

Initial fixture compilation lacked `<initializer_list>` and used the wrong
printf width for WOS pid_t; both were corrected before execution. Initial format
output is retained. The small-output diagnostic collector refused netdiag's size;
the existing bounded2MiB collector then saved both full reports. `/proc/net/route`
does not exist in this WOS build; those two ENOENT observations are recorded, not
presented as routing snapshots. No diagnostic failure was treated as VM exit.

## Next diagnostic boundary

`net_open_transport_errno` maps WKI_ERR_NO_ROUTE and WKI_ERR_NOT_FOUND to113.
`open_retained_net_proxy` can return that conversion from preparation/send, while
owner results are consumed separately. `ResolvedRoute::resolve` requires a current
authenticated origin/next-hop and routing lease; final TX rechecks publication
invalidation/expiry. These are candidate refusal boundaries, not a diagnosis of
this run. Nothing in the captured result proves a credential-mapping denial.

Next reproduce OPEN alone on the sparse topology and capture the exact retained
proxy/channel, route lease and source of113 at the existing return boundaries.
Investigate CLOSE only with a known restored UP baseline and a separate record of
owner commit, reply ownership and proxy retirement. Do not add blind retries,
weaken peer policy or relabel113/110/107 as success. A source-backed fix requires
its own critical-path patch contract and corresponding build/KTEST/live evidence.

Goal19 remains **18 enforced / 3 pending**. This stage closes the missing IPv6
functional evidence and identifies a concrete remaining mapped-NIC failure;
network/hostname is not promoted. Evidence is indexed in
`build/goal19-network-ipv6-authority/validation.json`.
