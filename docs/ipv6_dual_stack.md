# IPv6 dual-stack networking

WOS supports IPv4 and IPv6 concurrently for TCP, UDP, and ICMPv6. IPv6 is
available on loopback and Ethernet netdevices, and `netd` can learn global
addresses, on-link prefixes, a default router, and an advertised MTU from
Router Advertisements.

## Socket behavior

`AF_INET6` stream and datagram sockets use the normal POSIX entrypoints:
`bind`, `connect`, `listen`, `accept`, `send`, `sendto`, `recv`, `recvfrom`,
`shutdown`, `getsockname`, and `getpeername`. A complete IPv6 endpoint is a
28-byte `sockaddr_in6`; ports are in network byte order.

An IPv6 socket is dual-stack by default. `IPV6_V6ONLY` may be set or queried at
`IPPROTO_IPV6`; changing it is allowed only before bind, connect, or listen.
When it is false, IPv4-mapped IPv6 addresses use the IPv4 transport path while
endpoint queries retain the mapped `sockaddr_in6` representation. Native IPv6
wildcard binds accept native IPv6, and mapped IPv4 unless `IPV6_V6ONLY` is set.

`sin6_scope_id` is required for link-local unicast and interface/link-local
multicast destinations. It must name a live interface and must agree with
`SO_BINDTODEVICE` when both are present. WOS retains scope for link-local
unicast and multicast scope 1 or 2, and normalizes it to zero for global,
loopback, and wider-scope addresses.

## Input and fragmentation policy

The IPv6 parser validates the fixed header and declared payload before walking
extensions. It accepts one Hop-by-Hop header in first position and one
Destination Options header, with at most eight extension headers and 256
extension bytes. Padding options are accepted; unknown options whose action
bits require ICMP processing are rejected.

Routing, AH, and ESP extensions are rejected. Fragment headers and IPv6
jumbograms are intentionally unsupported and are rejected deterministically;
WOS does not reassemble IPv6 fragments. Truncated, repeated, misordered, or
oversized extension chains are dropped before reaching ICMPv6, TCP, or UDP.
Transport multicast is not exposed until multicast membership APIs exist, so
the admitted all-nodes, all-routers, and solicited-node groups are restricted
to ICMPv6 control traffic.

## Addresses and routes

Each interface has at most eight IPv6 addresses. Address state is one of
tentative, preferred, deprecated, or DAD-failed. Preferred and valid lifetimes
are absolute monotonic deadlines inside the kernel; the versioned userspace
ABI supplies relative seconds. `UINT32_MAX` means infinite and zero takes
effect immediately. Source selection excludes tentative and DAD-failed
addresses, prefers non-deprecated addresses, requires a compatible address
scope, and then chooses the longest common prefix.

Unless `WOS_NET_ADDR_F_NOPREFIXROUTE` is set, a usable address publishes a
connected route after Duplicate Address Detection succeeds. `NODAD` promotes
an address immediately and is intended for controlled cases such as loopback.
Physical interfaces perform DAD for their automatic link-local address.

The IPv6 route table has 64 bounded slots. Lookup selects the longest matching
prefix, then the lowest metric, and retains the selected netdevice generation
until transmission finishes. Routes may be connected, on-link, or use a
gateway. Address expiry and interface teardown remove associated routes before
the device can become stale.

The public address flags are:

| Flag | Value | Meaning |
| --- | ---: | --- |
| `WOS_NET_ADDR_F_DADFAILED` | `0x00000008` | Observed DAD conflict |
| `WOS_NET_ADDR_F_DEPRECATED` | `0x00000020` | Preferred lifetime expired |
| `WOS_NET_ADDR_F_TENTATIVE` | `0x00000040` | DAD is in progress |
| `WOS_NET_ADDR_F_PERMANENT` | `0x00000080` | Administratively permanent |
| `WOS_NET_ADDR_F_NOPREFIXROUTE` | `0x00000200` | Do not derive a connected route |
| `WOS_NET_ADDR_F_AUTOCONF` | `0x00010000` | Managed by RA/SLAAC |
| `WOS_NET_ADDR_F_NODAD` | `0x00020000` | Skip DAD |

`wos_net_addr_set_v2`, `wos_net_route_list`, `wos_net_route_set`, and
`wos_net_route_del` use version 1 fixed-size records from `<wos/netctl.h>`.
Legacy NETCTL operation ordinals and record layouts remain unchanged. The new
socket `SENDTO_EX` and `RECVFROM_EX` operations carry address lengths through a
versioned 32-byte descriptor while preserving the legacy operations.

## Neighbor Discovery and ICMPv6

The NDP cache contains at most 64 entries. An unresolved neighbor may queue at
most eight packets, with 128 queued packets globally. Resolution sends at most
three solicitations one second apart. Reachable entries age after 30 seconds;
stale entries transition through a five-second delay and probe state. Timeouts,
eviction, and device removal release every queued packet and retained device
reference.

ICMPv6 validates checksum, hop limit, source/destination constraints, message
lengths, NDP option lengths, and duplicate link-layer options. It handles echo,
Router Solicitation/Advertisement, Neighbor Solicitation/Advertisement,
Destination Unreachable, Packet Too Big, and Time Exceeded. Error quotes are
bounded and reparsed before a TCP or UDP error is delivered. ICMP errors are
never generated in response to another ICMP error, multicast traffic, or an
unspecified source.

## Router Advertisement and SLAAC policy

An `/etc/netdevs` interface with policy `dhcp` runs DHCPv4 and the IPv6 RS/RA
worker. The kernel first derives a link-local address from the interface MAC and
runs DAD. Once that address is usable, `netd` sends up to three Router
Solicitations four seconds apart and continues receiving advertisements on a
nonblocking raw ICMPv6 socket bound to that interface.

Advertisements must come from a link-local source with hop limit 255. Parsing
is bounded to 32 options and four prefix-information records. For the first
usable autonomous prefix, `netd` derives an EUI-64 SLAAC address and installs
its lifetimes. The on-link bit controls a separately managed prefix route, the
router lifetime controls the default route, and an advertised MTU is accepted
only between 1280 and the interface's configured ceiling. If the interface
identity disappears or changes, only the state managed by that worker is
withdrawn before discovery restarts.

WKI peers negotiate `WKI_CAP_NET_IPV6_STATE` (`0x0020`). Negotiated NET resource
advertisements, attach acknowledgements, and state notifications append one
strictly validated 168-byte version-1 suffix containing at most eight address
records. Peers without the capability continue using the exact legacy lengths.
The suffix is informational and contributes to readiness; it does not install
a proxy IPv6 address or route.

## Validation and rootless runtime workflow

Run host coverage and the normal build from the repository root:

```sh
WOS_TEST_BUILD_DIR=/tmp/wos-ipv6-tests scripts/test/run_tests.sh test
source .vscode/wos-env.sh && scripts/dev/build_wos.sh
```

Build the isolated diagnostic artifacts, then launch them without topology
setup or root access. `--no-setup` applies to the launch path and is not a
valid `--build-only` option:

```sh
bin/wos-ktest --config configs/node_ktest_rootless.json --fast --build-only --reset-sysroot
bin/wos-ktest --config configs/node_ktest_rootless.json --fast --no-build --no-setup
```

For a two-node run, use the pre-existing rootless tap/bridge topology:

```sh
bin/wos-cluster --config configs/cluster_ipv6_test.json --launch --no-setup
```

`testprog ipv6-net` supplies bounded TCP/UDP server and client traffic. A
link-local address must include `--scope IFINDEX`:

```sh
testprog ipv6-net server --protocol tcp --address fe80::5054:ff:fe00:100 --port 46000 --scope 2 --sessions 8
testprog ipv6-net client --protocol tcp --address fe80::5054:ff:fe00:100 --port 46000 --scope 2 --sessions 8
```

Use `--protocol udp` for datagrams. `--payload-bytes` is bounded to 16..1400,
`--sessions` to 1..64, and `--timeout-ms` to 1..60000. `testprog ipv6-ra`
advertises the documentation prefix `2001:db8:6::/64`; `--withdraw`,
`--malformed-zero-length`, and `--malformed-truncated` exercise lifecycle and
rejection paths. Capture the `packet_pool`, `backlog`, and `netdev` rows from
`/proc/wki/netdiag`, `memacc summary`, serial logs, and the test helpers' exit
status before and after loss/recovery tests when collecting runtime evidence.
