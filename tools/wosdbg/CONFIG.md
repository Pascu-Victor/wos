# WOSDBG Configuration

WOSDBG loads `wosdbg.json`, searching the current working directory and up to
four parent directories. Relative paths are resolved against the directory
containing the configuration file.

Use [wosdbg.json.example](wosdbg.json.example) as the complete starting point:

```json
{
  "lookups": [
    {
      "from": "0x400000",
      "to": "0x700000",
      "path": "./build/modules/init/init"
    },
    {
      "from": "0xffffffff80000000",
      "to": "0xffffffffffffffff",
      "path": "./build/modules/kern/wos"
    }
  ],
  "coredumpDirectory": "./coredumps",
  "binaries": [
    {"name": "init", "path": "./build/modules/init/init"},
    {"name": "httpd", "path": "./build/modules/httpd/httpd"}
  ],
  "mcp": {
    "bindAddress": "127.0.0.1",
    "port": 12346,
    "allowedCidrs": ["127.0.0.1/32", "::1/128"],
    "allowedRoots": ["."],
    "maxEntries": 200,
    "maxMemoryBytes": 4096,
    "maxHits": 200,
    "maxStringLength": 160,
    "sourceWindowLines": 8,
    "maxDisassemblyInstructions": 48,
    "maxIncidentMembers": 512,
    "maxIncidentMemberBytes": "134217728",
    "maxIncidentTotalBytes": "536870912",
    "maxIncidentArchiveBytes": "268435456",
    "maxIncidentPathLength": 512,
    "maxIncidentPathDepth": 24
  },
  "live": {
    "enabled": false,
    "allowMutations": false,
    "allowedHosts": ["127.0.0.1", "::1"],
    "runtimeDescriptors": [
      "cluster-overlays/live-debug.json",
      "ktest-data/live-debug.json"
    ],
    "maxTargets": 16,
    "maxSessions": 8,
    "operationTimeoutMs": 3000,
    "leaseMs": 30000,
    "maxMemoryBytes": 4096,
    "maxTranscriptBytes": 1048576,
    "targets": []
  }
}
```

## Symbol and binary mappings

`lookups` maps runtime address ranges to ELF files:

- `from`, `to`: inclusive hexadecimal runtime range.
- `path`: ELF/symbol file.
- `offset`: optional runtime load base subtracted before symbol lookup, useful
  for PIE/shared objects.

`binaries` maps the executable name encoded in coredump filenames to its local
ELF. WOSDBG also discovers the embedded ELF, kernel mappings, interpreter, and
loaded modules when available.

`coredumpDirectory` is used by GUI browsing, extraction, CLI, and MCP.

## MCP and analysis safety bounds

The `mcp` object also supplies bounds and roots to the shared analysis backend,
so they apply to MCP, CLI, and the GUI Analysis Tools dock:

- `bindAddress`, `port`: default MCP listener.
- `allowedCidrs`: clients permitted to reach MCP. Loopback is the safe default.
- `allowedRoots`: additional filesystem roots that tools may read. The current
  workspace, configured coredump directory, and configured symbol/binary
  directories are also implicit roots.
- `maxEntries`, `maxHits`, `maxMemoryBytes`, `maxStringLength`,
  `sourceWindowLines`, `maxDisassemblyInstructions`: response and scan bounds.
- `maxIncidentMembers`, `maxIncidentMemberBytes`, `maxIncidentTotalBytes`,
  `maxIncidentArchiveBytes`, `maxIncidentPathLength`, and
  `maxIncidentPathDepth`: hostile bundle snapshot, expansion, and path bounds.
  Byte limits may be JSON strings so values are not narrowed by a frontend.

`validate_incident` and `load_incident` first require the input path itself to
be under an effective `allowedRoots` entry. Archive members are then copied to
a private snapshot only after rejecting traversal, links, devices, conflicts,
oversized input/expansion, bad checksums, and a mismatched content-derived
incident identity. Offline incident replay never searches the host for a
replacement executable; it uses the exact bundle-local binary association or
reports missing/mismatched symbols.

Do not expose MCP beyond loopback without deliberately configuring both
`bindAddress` and `allowedCidrs`. Files outside the effective roots described
above are rejected by analysis operations.

## Live targets and leases

Live debugging is disabled unless `live.enabled` is true. Runtime descriptors
are local mode-0600 files produced while `wos-cluster` or `wos-ktest` owns the
corresponding VMs. WOSDBG accepts only version 1 descriptors below an effective
allowed root, validates the recorded process identity, and connects only to
literal loopback RSP endpoints and local QMP sockets. Static `targets` use the
same schema and checks and are useful for an explicitly tunneled guest
debugserver. Opening a session is itself an audited pause operation and requires
`authority=pause-read`, a non-empty `auditId`, and
`confirmation=PAUSE_ALLOWLISTED_TARGETS`. No register/memory write, breakpoint,
step, continue, raw RSP/QMP, or command-execution tool is exposed;
`allowMutations=false` remains the default and reserved gate for any future
bounded mutation capability. A static target uses this shape:

```json
{
  "id": "debugserver-httpd",
  "nodeId": "0",
  "transport": "debugserver",
  "host": "127.0.0.1",
  "port": 2159,
  "symbolPath": "build/modules/httpd/httpd",
  "expectedBuildId": "0123456789abcdef",
  "logPaths": ["serial-vm0.log"]
}
```

Tool arguments select an `id`; they cannot supply a host, port, socket, symbol
file, or raw QMP/RSP command. QEMU sessions query run state through QMP and
resume only a running-to-paused transition owned by that lease. Session close,
frontend-owner cleanup, timeout, process exit, and partial multi-node failure
all unwind owned leases. Memory and raw transcript payloads require explicit
sensitive-data confirmations and remain bounded. `allowMutations` is reserved
and currently grants no write, breakpoint, kill, shell, or arbitrary-command
capability.

See [README.md](README.md) for GUI, CLI, MCP, batch workflows, and the full
capability map.
