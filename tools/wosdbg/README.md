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
structured JSON. Successful `load_log` and `open_coredump` calls are remembered
so later tools receive the active `logId` or `dumpId`.

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
  `list_coredumps`, and `open_coredump`.
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

`build_distributed_timeline` accepts multiple loaded logs, selects direct query
matches, optionally includes nearby context, expands normalized distributed
identifiers (`cookie`, request/task/resource IDs, peer, PID, FD, channel, and
sequence), and reports:

- per-log lanes and stable row positions;
- timestamp-ordered events when log clocks are present;
- explicit clock quality when only per-log ordering is trustworthy;
- identifiers observed across more than one log/resource;
- bounded/truncated status, with fair sampling across lanes.

This distinction matters: WOSDBG does not invent a global order for logs that
do not contain comparable timestamps.
