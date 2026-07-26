#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WOSDBG = ROOT / "tools" / "wosdbg"


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
    require_tokens(
        panel,
        [
            "tool_catalog_received",
            "tool_result_received",
            "suggested_arguments",
            'for (const QString& key : {"dumpId", "logId"})',
        ],
        "schema-driven GUI",
    )
    require_tokens(client, ["request_tool_catalog()", "call_tool(const QString& name"], "GUI client")
    require_tokens(
        server,
        [
            "DebugAnalysisService::tool_catalog()",
            "analysis_service->invoke_tool(name, DOCUMENT.object())",
        ],
        "GUI server adapter",
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
            "std::vector<std::vector<Candidate>> by_lane",
        ],
        "distributed timeline bounds/correlation/clock semantics",
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
    test_distributed_timeline_is_bounded_and_clock_honest()
    test_agent_and_user_docs_cover_all_interfaces()
    print("WOSDBG MCP, CLI, and GUI share one bounded analysis contract")


if __name__ == "__main__":
    main()
