# WKI bootstrap replay and bounded retry ownership

A completed direct authentication session queues an own-origin LSA replay to
its peer. RX only publishes bounded work; the routing worker constructs and
sends the replay. The pending LSDB slot binds the replay to the authenticated
session generation and retains retry state through transient transmit refusal.
Lease-response work follows the same bounded ownership principle.

Discovery hints do not grant service authority. Hint receipt and revision times
are distinct, so repeated copies cannot extend a topology revision indefinitely.
Authentication, lease freshness, fencing, replay protection and service-routing
admission retain their existing checks.

## Control ACK bootstrap

The standalone cumulative control ACK can follow an authenticated discovery
route before the LSA has restored service routing. The exception requires an
empty `HEARTBEAT_ACK` on the control channel, zero sequence and reserved fields,
and the exact ACK flag combination apart from priority. It retains origin
admission while sending. Payload traffic, other channels and piggyback ACKs
continue through ordinary service admission. The wire layout is unchanged.

## Discovery and deferred work

Validated discovery hints publish targeted HELLO retries with bounded expiry,
backoff and per-pass transmit budgets. Each retry retains the transport's local
registration generation and the advertised boot epoch and nonce. Transport
retirement cancels matching retries; reused addresses cannot inherit them.
Reciprocal authenticated progress suppresses further HELLO publication.

Protocol maintenance sleeps until a deadline or notification rather than
polling continuously. Its event-before-park token preserves work arriving
between an empty-queue observation and sleep. The independent auto-attach
worker may park while the protocol worker services ACKs and timers.

Authenticated compute submits denied by policy reserve an owned negative reply
before reliable RX consumes or ACKs the request. Queue refusal returns RETRY;
it does not discard the decision. Workers retain transiently refused replies
until reliable enqueue or exact session retirement. Denied payloads never enter
execution admission. Notifications occur after dropping the channel lock.

## Diagnostics and qualification

Origin diagnostics copy bounded LSDB observations without authentication keys
or nonces. Opt-in `wki.routing_rx_timing` retains stage checkpoints and the
scheduler wake/timer window for the same winning receive sample. These are
elapsed observations, including possible IRQ, handoff and host delay; equal
endpoint CPUs do not exclude migration. Normal OFF paths avoid diagnostic
clock reads and bulk trace copying.

The aggregate diagnostic view includes the complete fixed channel pool,
explicit subsystem caps and a completion footer. Snapshot collection is
sequential across subsystems. Consumers must reject truncation and allocation
failure markers even when the footer is present. See
[diagnostic capacity](wki_netdiag.md) and
[kernel debug flags](kernel_debug_flags.md).

The stability work qualified the private four-node chain with selective drop,
reordering, endpoint restart, idle checks, startup captures and bounded receive
and SPF gates. A separate NAPI campaign exercised missed IRQ publication and
recovery. The completed replacement goal reused those qualified stability
scopes and freshly checked ordinary launches, SSH, idle and isolated KTEST.
The final KTEST reported 66,705 assertions, zero failures and three existing
skipped cases.

This evidence does not qualify positive trusted remote compute, physical
hardware, every lease topology or every host scheduling condition. Earlier
failed experiments are not counted as successful qualification. The broader
stability contract was replaced by the user; it is not an active completion
gate for the completed launch-overhead goal. Final evidence remains local at
`build/goal48-launch-performance-result-v87/goal-result-proof.json`, with the
historical result summary. Superseded handoffs and raw experiment archives
were pruned during worktree cleanup on 2026-10-05.
