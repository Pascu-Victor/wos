# WOSDBG

WOSDBG is the host-side WOS log and coredump debugger. It has one stateful
analysis backend and three equivalent tool interfaces:

- **GUI:** interactive log/coredump views plus the schema-driven **Analysis
  Tools** dock.
- **CLI:** JSON commands and state-preserving batch workflows for agents or
  scripts that do not have MCP connected.
- **MCP:** HTTP MCP endpoint for an MCP-capable agent.

`DebugAnalysisService::tool_catalog()` and
`DebugAnalysisService::invoke_tool()` are the canonical capability contract.
MCP, CLI, and GUI enumerate and invoke that same contract. A backend tool added
to the catalog appears in all three interfaces without another frontend
dispatcher.

The normal host artifact is `tools/build/bin/wosdbg`. Build it with the
workspace **Build wosdbg** task or:

```sh
cmake --build build/tools --target wosdbg
```

WOSDBG reads `wosdbg.json`, searching the current directory and up to four
parents. See [CONFIG.md](CONFIG.md) and [wosdbg.json.example](wosdbg.json.example).

## GUI

Run:

```sh
tools/build/bin/wosdbg
```

The existing log table and coredump panels remain available. Open **Tools** in
the toolbar to show the **Analysis Tools** dock. It lists the complete shared
catalog, displays each JSON input schema, prepares required arguments, runs the
selected tool asynchronously against the active backend, and shows copyable
structured JSON. Successful `load_log`, `open_coredump`, and `load_incident`
calls are remembered so later tools receive the active `logId`, `dumpId`, or
`incidentId`.

The GUI can also start or stop MCP with **MCP On/Off**. In remote GUI mode, tool
calls run on the connected WOSDBG server, so its configured roots and sessions
apply.

## CLI

List the authoritative catalog:

```sh
tools/build/bin/wosdbg --list-tools
```

Invoke any tool; the `wosdbg.` prefix is optional:

```sh
tools/build/bin/wosdbg \
  --tool open_coredump \
  --arguments '{"path":"coredumps/core.httpd.42"}'
```

Tools such as coredump analysis require a session created by a previous call.
Use `--batch FILE` (or `--batch -` for stdin) to preserve sessions and reference
earlier results with `$id.field`:

```json
{
  "calls": [
    {
      "id": "node0",
      "tool": "load_log",
      "arguments": {"path": "serial-vm0.log"}
    },
    {
      "id": "node1",
      "tool": "load_log",
      "arguments": {"path": "serial-vm1.log"}
    },
    {
      "id": "incident",
      "tool": "build_distributed_timeline",
      "arguments": {
        "logIds": ["$node0.logId", "$node1.logId"],
        "query": "cookie=0x1234",
        "context": 4
      }
    }
  ]
}
```

The batch result is JSON and returns nonzero if a call fails. Set
`"continueOnError": true` on the top-level object when independent calls should
continue. Use `${id.field}` inside a larger string, for example a resource URI.
Batch calls may also use `"operation": "resources/list"`,
`"resources/templates/list"`, or `"resources/read"` with a `uri`; this permits
opening a session and reading its resources in one process. CLI resource parity
is also available directly with:

```sh
tools/build/bin/wosdbg --list-resources
tools/build/bin/wosdbg --list-resource-templates
tools/build/bin/wosdbg --read-resource 'wosdbg://coredump/DUMP_ID/summary'
```

Validate and replay a portable incident in one stateful batch:

```json
{
  "calls": [
    {"id": "load", "tool": "load_incident",
     "arguments": {"path": "failure.wosincident"}},
    {"id": "summary", "tool": "summarize_incident",
     "arguments": {"incidentId": "$load.incidentId", "maxEvents": 200}},
    {"id": "inventory", "tool": "get_incident_inventory",
     "arguments": {"incidentId": "$load.incidentId"}}
  ]
}
```

The loader snapshots bundle content, verifies its content-derived identity and
member checksums, and uses only bundle-local binaries during replay. See
[WOS incident bundles](../../docs/wosincident.md) for capture, redaction,
sharing, and replay guidance.

## MCP

Start a headless log server and MCP endpoint:

```sh
tools/build/bin/wosdbg \
  --server 127.0.0.1:12345 \
  --mcp \
  --mcp-host 127.0.0.1 \
  --mcp-port 12346
```

The MCP URL is `http://127.0.0.1:12346/mcp`. MCP provides the same tool catalog
as `--list-tools`, plus resource listing, templates, and reads. Binding,
allowed CIDRs, allowed filesystem roots, and response bounds come from the
`mcp` section of `wosdbg.json`. Keep the default loopback/CIDR restrictions
unless remote access is intentional.

## Capabilities

Always use `--list-tools` or MCP `tools/list` as the precise current schema.
The catalog is grouped here by debugging job:

- **Sessions and acquisition:** `status`, `list_logs`, `load_log`,
  `get_log_entries`, `search_log`, `get_log_context`, `extract_coredumps`,
  `list_coredumps`, `open_coredump`, `validate_incident`, `load_incident`,
  `get_incident_inventory`, and `summarize_incident`.
- **Crash triage:** `get_crash_summary`, `analyze_coredump`,
  `backtrace_coredump`, `decode_fault_instruction`, `describe_registers`,
  `follow_register`, `annotate_stack`, `inspect_pte`, and
  `recognize_startup_stack`.
- **Memory and code:** `search_coredump_memory`, `find_pointers`,
  `get_memory_context`, `disassemble_coredump`, `resolve_address`, and
  `get_source_context`.
- **ELF/image-corruption analysis:** `verify_embedded_elf`,
  `check_elf_mapping`, `find_duplicate_pages`, `analyze_elf_integrity`,
  `elf_layout_summary`, `compare_expected_disassembly`,
  `scan_chunk_corruption`, and `audit_executable_ptes`.
- **Distributed WOS/WKI incidents:** `correlate_coredump_logs`,
  `reconstruct_wki_trace`, `build_distributed_timeline`,
  `explain_remote_exec_path`, and `diagnose_remote_exec_corruption`.

`load_log` also accepts bounded `wos.telemetry` JSONL records from journal,
perf, and strace. It retains the canonical envelope and exact decimal identity,
timestamp, and correlation strings while preserving the legacy text loader.

`build_distributed_timeline` accepts multiple loaded logs, selects direct query
matches, optionally includes nearby context, expands normalized distributed
identifiers (`cookie`, request/task/resource IDs, peer, PID, FD, channel, and
sequence), and reports:

- per-log lanes and stable row positions;
- timestamp-ordered events within explicit clock partitions;
- explicit clock quality when only per-log ordering is trustworthy;
- identifiers observed across more than one log/resource;
- bounded/truncated status, with fair sampling across lanes.

This distinction matters: WOSDBG does not invent a global order for logs that
do not contain comparable timestamps.

## Bounded live debugging

Live targets are disabled by default and configured under `live` in
`wosdbg.json`; see [CONFIG.md](CONFIG.md). Launchers publish a local runtime
descriptor while debug VMs are alive. Rootless launches use the existing
topology and append `--no-setup`:

```sh
bin/wos-ktest --no-build --no-package --no-setup --debug-node
bin/wos-cluster --config configs/cluster_wki_auth_rootless.json \
  --launch --no-setup --debug-node 0 --debug-node 1 --debug-node 2
```

After enabling the matching runtime descriptor, use one stateful batch (or the
same MCP/GUI session) to discover, open, inspect, and close a lease:

```json
{
  "calls": [
    {"id":"targets","tool":"discover_live_targets","arguments":{}},
    {"id":"open","tool":"open_live_session","arguments":{
      "targetIds":["qemu-node-0"],"authority":"pause-read",
      "auditId":"incident-2026-09-03","confirmation":"PAUSE_ALLOWLISTED_TARGETS"}},
    {"id":"regs","tool":"read_live_registers","arguments":{
      "sessionId":"$open.sessionId","leaseToken":"$open.leaseToken","targetId":"qemu-node-0"}},
    {"id":"close","tool":"close_live_session","arguments":{
      "sessionId":"$open.sessionId","leaseToken":"$open.leaseToken"}}
  ]
}
```

QMP is the pause authority for QEMU: WOSDBG records initial state and never
resumes a VM that was already paused. Reads use a typed GDB-RSP subset; no tool
accepts raw packets, raw QMP, a guest command, or a host shell. Symbols are used
only when the target image catalog and configured local ELF build IDs match.
Every operation, memory response, transcript, target count, and lease has a
configured bound. Transcript payloads are hashed/redacted unless explicitly
requested with the documented sensitive-data confirmation.

`get_live_source` and `inspect_live_pte` use only build-ID-verified kernel
symbols; the PTE walk reads at most one x86-64 paging chain through WOS's HHDM.
`capture_live_incident` takes `confirmation=CAPTURE_LIVE_INCIDENT`, records
per-target host timestamps without claiming guest-global ordering, atomically
publishes the bounded evidence, and reopens it through the ordinary incident
loader. `verify_live_transcript` recomputes the canonical transcript digest and,
when payloads were explicitly exported, every record payload digest.
