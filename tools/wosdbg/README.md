# WOSDBG

WOSDBG is the host-side WOS log and coredump debugger. It has one stateful
analysis backend and three equivalent tool interfaces:

- **GUI:** a debugger workspace with linked machine-state views, an evidence
  browser, and a guided live-attach screen. Specialist tools live in Advanced Analysis.
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

Start in **Debugger**. Double-click a file in the evidence sidebar, or use
**File → Open coredump**, **Open incident**, or **Open log**. Coredumps open
directly into a crash summary, call stack, registers, disassembly, memory, source,
and modules. Double-click a register or frame to follow its address into code
and memory; right-click for navigation and copy actions. Back/forward buttons
retain address history. Choose **Fault context** or **Saved context** to inspect
the corresponding captured register set. No tool names, session IDs, or JSON
are required for these workflows.

**Debug → Attach to target** opens target discovery and selection. If live access
is off, choose **Configured targets**, **Running WOS cluster**, or **Isolated
KTEST VM**, then **Enable live debugging**. The latter two use the standard
runtime descriptors relative to the backend configuration directory; they do
not scan endpoints or launch VMs. Enablement is an explicit, process-local
choice shared by all connected frontends. An already-enabled manager and its
leases are never replaced; restart the backend to select another preset.

Select a target and choose **Attach and pause**. Confirm with an inspection
reason. The debugger shows registers and a call stack, the remaining pause
lease, **Extend pause**, and **Detach / release pause**. Detach releases only
pauses acquired by WOSDBG. Lease expiry and disconnect clear live context.
Raw memory reads require separate confirmation. Credentials are held internally,
not displayed as form fields. Stepping, writes, and breakpoints are not supported.

**Advanced analysis** (Ctrl+Shift+P) retains the complete tool catalog for
specialist operations, including executable integrity, WKI and remote-exec
analysis, incident validation, and full structured exports. It opens separately
from the main debugger layout, with the current evidence context preselected.

Inputs use file/folder pickers, text and numeric fields, choice menus, toggles,
and add/remove controls for lists and memory ranges. Fields marked `*` are
required. Check an optional field to include it; unchecked fields are omitted
so backend defaults remain intact, including booleans whose default is true.
Live pause, sensitive memory, transcript export, and incident capture use
explicit confirmation checkboxes, never preselected confirmations. Exported
transcript objects can be imported from a document without editing JSON.

Results show a summary and expandable evidence. Select an evidence list for a
sortable, filterable table; select rows to reuse their session, path, target,
or address in a follow-up action. The evidence selector remembers up to 32
session entries in memory, including paired live session IDs and lease tokens.
**Clear context** forgets suggestions; it does not close backend sessions.
In Advanced Analysis, use **Close live session** to release its live session. Disconnection clears form
context. Tables page through returned data in groups of 500 rows with up to 32
columns; double-click a row to inspect all its fields. Tree list previews show
up to 500 items. **JSON / export** preserves every field returned by the backend. Backend scan
and response limits still apply. **Tool schema** is available for developers.

The **Execution log / local coredump** workspace retains the dedicated register, memory,
disassembly, ELF, interrupt, and execution-log panels. **View → Panels** restores
closed panels; **Reset viewer layout** restores their layout. Search and
interrupt controls have separate toolbar rows. Appearance and window geometry
are remembered locally. **Analyze → Analyze coredump from viewer** prepares the
current viewer file for backend inspection (the path must also exist on the backend).
Workspace navigation stays outside the dock layout, so switching pages does not
move its tabs. Log scrolling uses aligned 1,024-row chunks, at most four in-flight
requests, several thousand rows of read-ahead, and a bounded cache. Cold jumps
retain the previous complete view with a loading banner until the requested rows
are ready; retained rows cannot be clicked as if they belonged to the new offset.
Formatted rows are cached only when complete, and scrolling never suspends loading.
Shortcuts: **Ctrl+1** Debugger, **Ctrl+2** Viewer,
**Ctrl+O** Open coredump, **Ctrl+F** search, **Alt+Left/Right** address history,
**Ctrl+Shift+P** Advanced analysis, **Ctrl+Enter** run an advanced analysis.

Use **Agent** to start/stop MCP, copy its endpoint, or read connection guidance.
In remote GUI mode, analysis paths and sessions belong to the connected backend.
File pickers browse the local machine; enter the backend path when those hosts
differ. **Open local coredump in viewer** explicitly uses the local viewer.

## CLI

List the authoritative catalog:

```sh
tools/build/bin/wosdbg --list-tools
```

For concise discovery, list names and short descriptions first, then request
only the schema you need. Neither command invokes an analysis operation:

```sh
tools/build/bin/wosdbg --list-tools --brief
tools/build/bin/wosdbg --tool-help analyze_coredump
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

MCP initialization includes a short workflow guide. Tool definitions include
readable titles, category metadata, effect annotations, parameter guidance, and
enumerated choices. Each call returns a bounded text summary (at most 2,500
characters) and the complete original object in `structuredContent`; `isError`
still follows the backend's `ok` field. Text no longer duplicates the full JSON
payload. Clients that previously parsed JSON from `content[0].text` must read
`structuredContent` instead. CLI JSON results and GUI wire results are unchanged.

For first-pass crash triage, use `get_crash_summary` or
`analyze_coredump` with `mode=compact`, then request specific registers, memory,
source, or integrity evidence. Reuse opened session IDs, request bounded pages,
and inspect degradation, truncation, and clock-quality fields before drawing
conclusions. Pair live `sessionId` and `leaseToken` from the same open operation
and close the lease when finished.

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
