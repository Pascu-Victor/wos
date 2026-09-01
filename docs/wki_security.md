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
| 0 | Compute submission |
| 1 | VFS export admission |
| 2 | VFS reads |
| 3 | VFS writes |
| 4 | IPC and event operations |
| 5 | Remote network operations |
| 6 | Zone/RDMA operations |
| 7 | Generic device attachment |
| 8 | Block reads |
| 9 | Block writes |

VFS attachment requires generic attach, export, and the requested read/write permission. Block attachment requires generic attach and the requested block access. The authoritative gate is immediately before reliable request admission; denied in-order requests are consumed without service allocation and return `UNAUTHORIZED`, `ACCESS_DENIED`, `-EACCES`, or `REJECTED_POLICY` in the existing response family.

## Handshake and lifecycle

A broadcast HELLO may cause only the lower configured node ID to send a targeted HELLO. Targeted HELLO and HELLO_ACK are HMAC-authenticated under the pairwise PSK and bind their full v3 payload. Each contains the sender's hardware-generated 256-bit boot nonce, boot/channel epochs, suite, key ID, stable identity, hostname, MAC, and capabilities.

The exchange is three messages. The initiator sends `HELLO` with zero challenge-echo fields. The responder verifies its PSK tag, stages derived keys without publishing a connected peer, and returns `HELLO_ACK` echoing the initiator's nonce and channel epoch. The initiator verifies that challenge, installs the same transcript-derived session, and sends `HELLO_CONFIRM` authenticated by the new directional traffic key with counter one. Only after verifying that confirmation does the responder promote its pending keys and publish topology and resources. This proves possession of the derived key before either side accepts service traffic.

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

Raw RoCE and direct ivshmem RDMA/doorbell paths can bypass ordinary WKI RX. WKI v3 therefore does not initialize RoCE, does not publish an RDMA transport on peers, and does not negotiate RDMA capability. Zone creation receives a stable policy denial. Re-enabling these paths requires a separately reviewed session-bound MAC/replay record for RoCE headers, shared rings, staged data, block SQ/CQ records, and doorbells; authenticating only an rkey exchange is insufficient.

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
