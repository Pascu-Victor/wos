#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WOSDBG = ROOT / "tools" / "wosdbg"
INCIDENT_TOOLS = {
    "wosdbg.load_incident": "path",
    "wosdbg.validate_incident": "path",
    "wosdbg.get_incident_inventory": "incidentId",
    "wosdbg.summarize_incident": "incidentId",
}


def fail(message: str) -> None:
    raise AssertionError(message)


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def between(source: str, start: str, end: str) -> str:
    start_index = source.find(start)
    if start_index < 0:
        fail(f"missing section start: {start}")
    end_index = source.find(end, start_index)
    if end_index < 0:
        fail(f"missing section end: {end}")
    return source[start_index:end_index]


def test_backend_catalog_is_the_only_tool_dispatch_contract() -> None:
    service_header = (WOSDBG / "debug_analysis_service.h").read_text()
    mcp_source = (WOSDBG / "mcp_http_server.cpp").read_text()
    mcp_header = (WOSDBG / "mcp_http_server.h").read_text()
    backend_sources = "\n".join(path.read_text() for path in sorted(WOSDBG.glob("*.cpp")))

    require_tokens(
        service_header,
        [
            "static QJsonObject tool_catalog()",
            "QJsonObject invoke_tool(const QString& name, const QJsonObject& args)",
        ],
        "frontend-neutral backend contract",
    )
    if "tool_list()" in mcp_header:
        fail("MCP must not own a second tool catalog")

    catalog = between(
        backend_sources,
        "auto DebugAnalysisService::tool_catalog()",
        "auto McpHttpServer::tool_result",
    )
    dispatch = between(
        backend_sources,
        "auto DebugAnalysisService::invoke_tool",
        "auto McpHttpServer::call_tool",
    )
    catalog_names = set(re.findall(r'\{"name", "(wosdbg\.[^"]+)"\}', catalog))
    dispatch_names = set(re.findall(r'name == "(wosdbg\.[^"]+)"', dispatch))
    if len(catalog_names) < 37:
        fail(f"unexpectedly small WOSDBG catalog: {len(catalog_names)} tools")
    if catalog_names != dispatch_names:
        fail(
            "catalog/dispatch parity failure: "
            f"catalog-only={sorted(catalog_names - dispatch_names)}, "
            f"dispatch-only={sorted(dispatch_names - catalog_names)}"
        )
    missing_incident_tools = set(INCIDENT_TOOLS) - catalog_names
    if missing_incident_tools:
        fail(f"incident catalog is incomplete: missing={sorted(missing_incident_tools)}")
    for name, required_argument in INCIDENT_TOOLS.items():
        start = catalog.find(f'{{"name", "{name}"}}')
        if start < 0:
            fail(f"incident catalog entry is not source-visible: {name}")
        next_entry = catalog.find('QJsonObject{{"name", "wosdbg.', start + 1)
        entry = catalog[start : next_entry if next_entry >= 0 else len(catalog)]
        require_tokens(entry, ['"inputSchema"', "schema(", f'"{required_argument}"'], f"{name} schema")
    require_tokens(
        mcp_source,
        [
            'return DebugAnalysisService::tool_catalog();',
            'analysis->invoke_tool(params["name"].toString(), params["arguments"].toObject())',
        ],
        "thin MCP adapter",
    )


def test_cli_and_gui_use_the_shared_contract() -> None:
    main = (WOSDBG / "main.cpp").read_text()
    cli = (WOSDBG / "debug_cli.cpp").read_text()
    panel = (WOSDBG / "debug_tool_panel.cpp").read_text()
    client = (WOSDBG / "log_client.cpp").read_text()
    server = (WOSDBG / "log_server.cpp").read_text()
    protocol = (WOSDBG / "protocol.h").read_text()

    require_tokens(
        main,
        [
            '"list-tools"',
            '"tool"',
            '"arguments"',
            '"batch"',
            '"list-resources"',
            '"list-resource-templates"',
            '"read-resource"',
            "run_debug_cli",
        ],
        "CLI options",
    )
    require_tokens(
        cli,
        [
            "DebugAnalysisService::tool_catalog()",
            "analysis.invoke_tool(name, ARGUMENTS)",
            "analysis.list_resources()",
            "DebugAnalysisService::list_resource_templates()",
            "analysis.read_resource(options.resource_uri)",
            "resolve_references",
            "continueOnError",
            '"resources/list"',
            '"resources/templates/list"',
            '"resources/read"',
        ],
        "CLI parity and stateful workflow",
    )
    require_tokens(
        protocol,
        [
            "TOOL_CATALOG_REQUEST = 24",
            "TOOL_CATALOG_RESPONSE = 25",
            "TOOL_CALL_REQUEST = 26",
            "TOOL_CALL_RESPONSE = 27",
        ],
        "append-only GUI protocol",
    )
    require_tokens(panel, ["tool_catalog_received", "tool_result_received", "suggested_arguments"], "schema-driven GUI")
    remembered_context = between(
        panel,
        "void DebugToolPanel::remember_context",
        "void DebugToolPanel::on_tool_result",
    )
    require_tokens(remembered_context, ['"dumpId"', '"logId"'], "GUI tool-result context")
    backend_sources = "\n".join(path.read_text() for path in sorted(WOSDBG.glob("*.cpp")))
    require_tokens(remembered_context, ['"incidentId"'], "GUI incident context")
    require_tokens(client, ["request_tool_catalog()", "call_tool(const QString& name"], "GUI client")
    require_tokens(
        server,
        [
            "DebugAnalysisService::tool_catalog()",
            "analysis_service->invoke_tool(name, DOCUMENT.object())",
        ],
        "GUI server adapter",
    )


def test_telemetry_metadata_does_not_change_legacy_gui_wire_layout() -> None:
    entry = (WOSDBG / "log_entry.h").read_text()
    protocol = (WOSDBG / "protocol.h").read_text()
    service = (WOSDBG / "debug_analysis_service.cpp").read_text()
    require_tokens(
        entry,
        ["has_telemetry_envelope", "telemetry_envelope"],
        "backend-local telemetry metadata",
    )
    writer = between(protocol, "inline auto operator<<", "inline auto operator>>")
    reader = between(protocol, "inline auto operator>>", "// Serialization helpers for AddressLookup")
    if "telemetry" in writer or "telemetry" in reader:
        fail("telemetry metadata must not change the existing QDataStream LogEntry layout")
    require_tokens(
        service,
        [
            'obj["telemetryEnvelope"] = entry.telemetry_envelope',
            'obj["telemetryJson"] = QString::fromStdString(entry.telemetry_json)',
            'obj["missingIdentityFields"] = missing_identity',
        ],
        "backend telemetry envelope exposure",
    )


def test_telemetry_jsonl_uses_shared_bounded_parser_and_preserves_legacy_path() -> None:
    parser = (WOSDBG / "telemetry_log.cpp").read_text()
    service = (WOSDBG / "debug_analysis_service.cpp").read_text()
    cmake = (WOSDBG / "CMakeLists.txt").read_text()
    semantic = (WOSDBG / "tests" / "telemetry_jsonl_semantic_test.py").read_text()
    require_tokens(
        parser,
        [
            "wos::telemetry::parse",
            "wos::telemetry::validate_envelope",
            "wos::telemetry::serialize",
            "MAX_TELEMETRY_FILE_BYTES",
            "MAX_TELEMETRY_LINE_BYTES",
            "MAX_TELEMETRY_RECORDS",
            "entry.telemetry_json",
            '"node_id"',
        ],
        "bounded shared telemetry JSONL parser",
    )
    require_tokens(
        service,
        [
            "load_telemetry_jsonl(RESOLVED)",
            "if (telemetry.recognized)",
            "LogProcessor processor(RESOLVED)",
            "Structured telemetry rejected",
        ],
        "structured-first and legacy-compatible log loading",
    )
    require_tokens(
        cmake,
        ["telemetry_log.cpp", "wos::telemetry", "wosdbg_telemetry_jsonl_semantic_test"],
        "compiled telemetry ingestion assurance",
    )
    require_tokens(
        semantic,
        [
            '"18446744073709551611"',
            '"future_extension"',
            '"mixed_timeline"',
            '"missing_timeline"',
            '"legacy-text"',
            '"duplicate.jsonl"',
            '"oversized.jsonl"',
        ],
        "telemetry JSONL semantic cases",
    )


def test_distributed_timeline_is_bounded_and_clock_honest() -> None:
    service = (WOSDBG / "debug_analysis_service.cpp").read_text()
    timeline = between(
        service,
        "auto DebugAnalysisService::build_distributed_timeline",
        "auto DebugAnalysisService::explain_remote_exec_path",
    )
    require_tokens(
        timeline,
        [
            'bounded_int(args, "maxEvents", 512, 1, 4096)',
            'bounded_int(args, "context", 0, 0, 32)',
            "cookie|request",
            "candidate.correlation_values",
            "crossLogCorrelations",
            "clockOrderedEvents",
            "per-log-order-only",
            "partial-timestamps",
            "all-events-timestamped",
            "partitioned-clock-domains",
            "clockPartitions",
            "globalOrderAvailable",
            "clock_partition_for",
            "structured_correlation_fields",
            "structured_timestamp_ns",
            "structured_clock_evidence",
            "unproven-clock",
            "unrecognized-quality",
            "std::vector<std::vector<Candidate>> by_lane",
        ],
        "distributed timeline bounds/correlation/clock semantics",
    )


def test_coredump_module_identity_requires_dump_evidence() -> None:
    service = (WOSDBG / "debug_analysis_service.cpp").read_text()
    matcher = between(
        service,
        "auto runtime_base_matches_dump",
        "auto source_with_llvm_symbolizer",
    )
    require_tokens(
        matcher,
        [
            "wosdbg::read_va_bytes",
            "RUNTIME_START",
            "FILE_OFFSET",
            "std::memcmp",
            "AVAILABLE < 32",
        ],
        "coredump module byte identity",
    )
    if "matched = !wosdbg::elf_bytes_at_runtime_va" in service:
        fail("WOSDBG must not compare an interpreter candidate with its own file bytes")
    require_tokens(
        service,
        [
            "matched = runtime_base_matches_dump(*session.dump, ELF, info, base)",
            "SYMBOL_SOURCE_TRUSTED",
            "if (SYMBOL_SOURCE_TRUSTED)",
        ],
        "unmatched module symbol quarantine",
    )
def test_incident_assurance_exercises_compiled_frontend_adapters() -> None:
    semantic_test = (WOSDBG / "tests" / "incident_cli_semantic_test.py").read_text()
    cmake = (WOSDBG / "CMakeLists.txt").read_text()
    require_tokens(
        semantic_test,
        [
            "class GuiWireClient",
            "GUI_TOOL_CATALOG_REQUEST = 24",
            "GUI_TOOL_CALL_REQUEST = 26",
            '"tools/list"',
            '"tools/call"',
            "check_runtime_interface_parity",
            "summary_projection(cli_summary)",
            "summary_projection(mcp_summary)",
            "summary_projection(gui_summary)",
            "check_relocated_host_config_independence",
            "check_cache_policy_upgrade",
            "run_seeded_property_smoke",
            '"semanticProjectionVersion"',
            '"semantic"',
        ],
        "compiled CLI/MCP/GUI incident assurance",
    )
    require_tokens(
        cmake,
        [
            "wosdbg_incident_cli_semantic_test",
            "tests/incident_cli_semantic_test.py",
            "--require-incident-tools",
        ],
        "mandatory incident semantic CTest registration",
    )


def test_agent_and_user_docs_cover_all_interfaces() -> None:
    agents = (ROOT / "AGENTS.md").read_text()
    readme = (WOSDBG / "README.md").read_text()
    config = (WOSDBG / "CONFIG.md").read_text()
    require_tokens(
        agents,
        [
            "## WOSDBG host debugger",
            "MCP at `/mcp`",
            "JSON CLI",
            "Analysis Tools",
            "build_distributed_timeline",
            "tools/wosdbg/README.md",
        ],
        "agent WOSDBG guidance",
    )
    require_tokens(
        readme,
        [
            "## GUI",
            "## CLI",
            "## MCP",
            "--list-tools",
            "--batch FILE",
            "--list-resources",
            "## Capabilities",
            "Distributed WOS/WKI incidents",
        ],
        "WOSDBG interface/capability documentation",
    )
    require_tokens(config, ["wosdbg.json", "allowedCidrs", "allowedRoots"], "WOSDBG configuration documentation")


def main() -> None:
    test_backend_catalog_is_the_only_tool_dispatch_contract()
    test_cli_and_gui_use_the_shared_contract()
    test_telemetry_metadata_does_not_change_legacy_gui_wire_layout()
    test_telemetry_jsonl_uses_shared_bounded_parser_and_preserves_legacy_path()
    test_distributed_timeline_is_bounded_and_clock_honest()
    test_coredump_module_identity_requires_dump_evidence()
    test_incident_assurance_exercises_compiled_frontend_adapters()
    test_agent_and_user_docs_cover_all_interfaces()
    print("WOSDBG MCP, CLI, and GUI share one bounded analysis contract")


if __name__ == "__main__":
    main()
