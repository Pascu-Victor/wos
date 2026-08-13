# WOS structured telemetry interchange

WOS telemetry is an additive userspace interchange for journal, perf, strace,
and WOSDBG. Human-readable output remains the default. Kernel journal and perf
producers, syscall/ptrace ABIs, `/dev/journal`, `/proc/kperf`, and existing raw
records are unchanged.

## JSON envelope version 1

Each JSONL record is one bounded UTF-8 JSON object. The canonical top-level
members are:

```json
{
  "format": "wos.telemetry",
  "version": 1,
  "source": "journal",
  "source_version": 1,
  "kind": "journal.record",
  "identity": {
    "boot_id": "14188014900327801334",
    "node_id": "vm0",
    "pid": "42",
    "tid": "43",
    "cpu": 2
  },
  "clock": {
    "domain": "boot_monotonic",
    "value": "987654321",
    "unit": "ns",
    "quality": "local"
  },
  "correlation": {},
  "payload": {}
}
```

`version` is the envelope major version. Readers reject an unsupported major
version rather than guessing. `source_version` versions the producer's typed
payload independently. Unknown object members are retained by the shared
parser and deterministic re-serialization, which orders object keys bytewise.

Potentially 64-bit integers, including IDs and timestamps, are decimal JSON
strings. This prevents loss through JSON implementations backed by IEEE-754
doubles. Small enum, version, boolean, and CPU fields may be JSON numbers. A
missing identity or clock field means unavailable; it must not be inferred.

Clock domains describe what a timestamp means, not merely its unit.
`boot_monotonic` records are comparable within one boot. WOSDBG keeps records
in separate lanes unless node/boot/domain metadata or an incident manifest
proves comparability. It never turns local monotonic timestamps into a claimed
global cross-node order.

The common implementation is in `shared/telemetry`. Parsing is bounded by
input bytes, nesting depth, node/member/array counts, and decoded string bytes.
It rejects duplicate keys, invalid UTF-8, invalid escapes, malformed numbers,
trailing data, and unsupported envelope versions. Payload content is data and
is never executed.

## Journal

Use structured query or follow output explicitly:

```sh
journalctl --structured
journalctl -f --structured
journalctl -k -p warn --structured
```

Without `--structured`, journal text is unchanged. `journald` continues to
persist packed 608-byte `JournalRecord` v1 records unchanged; structured mode
is rejected in daemon mode. Each `journal.record` payload includes every v1
logical field, reserved fields, exact message/module byte encodings, and
`raw_record_hex`, the complete packed record. Valid UTF-8 text is also exposed
as `module` and `message`; those fields are `null` for non-UTF-8 data. A future
or malformed packed record aborts structured export explicitly instead of
being silently reinterpreted.

## Perf

Legacy `perf.data` remains the default. Opt in when recording or running a
command:

```sh
perf record --structured 1000
perf run --structured -- command arg
```

Inspect, convert, or export an existing input with:

```sh
perf data-info perf.data
perf data-convert perf.data
perf data-export perf.data > perf.jsonl
```

The `WOSPERF\0` container uses major/minor versioning, a fixed little-endian
header, bounded 24-byte section descriptors, 64-bit payload lengths, and CRC32C
for the header and every section. Version 1 contains a required exact legacy
snapshot plus required typed-event JSONL. Existing perf report/sched/WKI/map
readers transparently receive the exact legacy snapshot. Corrupt input,
truncated or partial magic, unsupported required sections, bad checksums, and
length/count overflow are errors; they never fall back to legacy parsing.

Conversion writes a same-directory temporary file, syncs and closes the
complete container, then atomically renames it over the legacy input. A failed
conversion leaves the legacy input in place. `data-export` validates the
container before emitting JSONL and can derive the same typed records from a
legacy input without modifying it. Perf records retain `/proc/kperf`'s subject
PID and CPU; they identify metadata unavailable from that ABI as missing rather
than inventing boot, thread, or container-instance identity.

## Strace

Use typed output explicitly:

```sh
strace --structured command arg
strace --structured -p 42
strace --structured -f -o trace.jsonl command
strace --structured -ff -o trace.jsonl command
```

The option is propagated through WOS remote attach/command helpers and fork
followers. Default decoded text, timestamps, files, attach behavior, syscall
entry/exit pairing, signal delivery, and resume semantics are unchanged.
Structured kinds cover syscall, signal, observed successful fork, observed
exec lifecycle, and termination. Ptrace does not publish tracee boot ID or CPU,
so strace omits them. A deferred or unpaired syscall is labeled explicitly.
Decoded display text and each serialized record are bounded; shared append
output emits one complete JSONL record per write and reports partial writes.

## WOSDBG

WOSDBG recognizes `.jsonl` files and valid `wos.telemetry` records through the
normal `wosdbg.load_log` interface. The backend retains both the full canonical
JSON string and a query projection, including unknown fields and exact decimal
ID/timestamp strings. Existing legacy log worker parsing and the GUI QDataStream
`LogEntry` layout are unchanged.

`wosdbg.build_distributed_timeline` uses structured identity, clock, and
correlation members before any legacy text fallback. Its result reports
`globalOrderAvailable`, clock partitions, missing metadata, and correlation
provenance. A mixed or unproven clock set is returned as separately ordered
lanes with no fabricated global array.

To inspect perf records in WOSDBG:

```sh
perf data-export perf.data > perf.jsonl
wosdbg --tool wosdbg.load_log --arguments '{"path":"perf.jsonl"}'
```

## Compatibility and limits

- JSON envelope v1 accepts unknown members and preserves them on round trip.
- Unsupported envelope/container major versions fail explicitly.
- Unknown optional container sections are reported and skipped by perf;
  unknown required sections fail.
- Journal raw v1, legacy delimiter-based `perf.data`, default strace text, and
  legacy WOSDBG text logs remain supported.
- Current default JSON record limit is 1 MiB. Perf container limits are 64 MiB
  total, 16 sections, 8 MiB legacy snapshot, 48 MiB typed events, and 262144
  typed records. WOSDBG additionally bounds file bytes, line bytes, and record
  count before retaining entries.

For live rootless validation, append `--no-setup` to `bin/wos-ktest` or
`bin/wos-cluster`; this reuses an existing topology without requiring root
setup access.
