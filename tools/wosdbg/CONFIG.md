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

See [README.md](README.md) for GUI, CLI, MCP, batch workflows, and the full
capability map.
