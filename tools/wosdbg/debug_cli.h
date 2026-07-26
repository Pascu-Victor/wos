#pragma once

#include <QString>

class DebugAnalysisService;

struct DebugCliOptions {
    bool list_tools = false;
    bool list_resources = false;
    bool list_resource_templates = false;
    QString tool_name;
    QString arguments_json;
    QString batch_path;
    QString resource_uri;
};

// Runs the frontend-neutral analysis contract without starting MCP or a GUI.
// Returns a conventional process exit status and writes JSON to stdout.
auto run_debug_cli(DebugAnalysisService& analysis, const DebugCliOptions& options) -> int;
