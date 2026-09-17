#include "tool_presentation.h"

#include <QJsonArray>
#include <QJsonDocument>
#include <QRegularExpression>
#include <QSet>
#include <QStringList>

namespace wosdbg {
QString display_label(QString name) {
    name.remove(QRegularExpression("^wosdbg\\."));
    name.replace('_', ' ');
    name.replace(QRegularExpression("([a-z])([A-Z])"), "\\1 \\2");
    const QStringList acronyms{"id", "ids", "elf", "pte", "ptes", "wki", "cpu", "rsp", "rip", "cr2", "rdma"};
    QStringList words = name.split(' ');
    for (auto& word : words) {
        if (acronyms.contains(word.toLower())) {
            word = word.toUpper();
        }
    }
    name = words.join(' ');
    if (!name.isEmpty()) {
        name[0] = name[0].toUpper();
    }
    return name;
}

QJsonObject describe_tool(QJsonObject tool) {
    const QString name = tool["name"].toString().section('.', -1);
    QString category = "Coredumps";
    if (name == "status") {
        category = "Workspace";
    } else if (name.contains("live")) {
        category = "Live targets";
    } else if (name.contains("incident")) {
        category = "Incidents";
    } else if (name.contains("wki") || name.contains("remote_exec") || name.contains("distributed") || name == "correlate_coredump_logs") {
        category = "Distributed analysis";
    } else if (name.contains("log")) {
        category = "Logs";
    } else if (name.contains("elf") || name.contains("corruption") || name.contains("duplicate") || name.contains("executable") ||
               name == "compare_expected_disassembly") {
        category = "Executable integrity";
    } else if (name.contains("memory") || name.contains("pointer") || name.contains("pte") || name.contains("address") ||
               name.contains("source")) {
        category = "Memory and source";
    }
    const QString title = display_label(name);
    tool["title"] = title;
    tool["_meta"] = QJsonObject{{"org.wosdbg/category", category}};
    const QSet<QString> changes_state{"enable_live_debugging", "discover_live_targets", "open_live_session", "renew_live_session",
                                      "close_live_session",    "capture_live_incident", "extract_coredumps", "load_log",
                                      "open_coredump",         "load_incident",         "validate_incident"};
    tool["annotations"] = QJsonObject{{"title", title},
                                      {"readOnlyHint", !changes_state.contains(name)},
                                      {"destructiveHint", false},
                                      {"openWorldHint", name.contains("live") && name != "verify_live_transcript"}};

    QJsonObject schema = tool["inputSchema"].toObject();
    QJsonObject properties = schema["properties"].toObject();
    const QJsonObject descriptions{{"dumpId", "Coredump session ID returned by open_coredump or incident inventory."},
                                   {"logId", "Log session ID returned by load_log or incident inventory."},
                                   {"incidentId", "Session ID returned by load_incident."},
                                   {"sessionId", "Live session ID returned by open_live_session; belongs to this frontend connection."},
                                   {"leaseToken", "Lease token returned by open_live_session. Use with its matching sessionId."},
                                   {"targetId", "Allowlisted target ID from list_live_targets or the active session."},
                                   {"targetIds", "Allowlisted target IDs to pause together. Supply targetId or targetIds."},
                                   {"path", "Path on the WOSDBG backend host, within its configured allowed roots."},
                                   {"output", "New incident destination on the backend host; existing destinations are rejected."},
                                   {"address", "Address as a decimal or 0x-prefixed hexadecimal string; strings preserve all 64 bits."},
                                   {"start", "Zero-based page offset. Omit to start at the beginning."},
                                   {"count", "Maximum rows to return, capped by the configured backend limit."},
                                   {"maxHits", "Maximum matches to return, capped by the configured backend limit."},
                                   {"leaseMs", "Requested lease duration in milliseconds, capped by the configured maximum."},
                                   {"auditId", "Brief reason or ticket identifying this pause operation (1–160 UTF-8 bytes)."},
                                   {"length", "Number of bytes to read, capped by the configured live memory limit."}};
    for (auto it = properties.begin(); it != properties.end(); ++it) {
        QJsonObject field = it.value().toObject();
        const QString key = it.key();
        field["title"] = display_label(key);
        if (!field.contains("description") && descriptions.contains(key)) {
            field["description"] = descriptions[key];
        }
        if (key == "frame") {
            field["enum"] = QJsonArray{"trap", "saved"};
        } else if ((key == "mode" || key == "format") && name == "analyze_coredump") {
            field["enum"] = QJsonArray{"full", "default", "compact", "text", "human"};
        } else if (key == "scope" || key == "scanScope") {
            field["enum"] = QJsonArray{"all", "around", "active_stack", "stack", "all_stack", "fault"};
        } else if (key == "kind" && name == "search_coredump_memory") {
            field["enum"] = QJsonArray{"pointer", "symbol", "qword", "u64", "uint64", "bytes", "ascii", "nonzero"};
        } else if (key == "authority") {
            field["enum"] = QJsonArray{"pause-read"};
        } else if (key == "confirmation") {
            QString token;
            QString label;
            if (name == "enable_live_debugging") {
                token = "ENABLE_CONFIGURED_LIVE_TARGETS";
                label = "Enable access to configured live targets for this backend process";
            } else if (name == "open_live_session") {
                token = "PAUSE_ALLOWLISTED_TARGETS";
                label = "Pause the selected targets until this lease closes or expires";
            } else if (name == "read_live_memory") {
                token = "READ_SENSITIVE_LIVE_MEMORY";
                label = "Read live memory, which may contain secrets";
            } else if (name == "get_live_transcript") {
                token = "INCLUDE_SENSITIVE_LIVE_DATA";
                label = "Include sensitive live payloads in this transcript";
            } else if (name == "capture_live_incident") {
                token = "CAPTURE_LIVE_INCIDENT";
                label = "Write an incident bundle containing the selected live evidence";
            }
            if (!token.isEmpty()) {
                field["enum"] = QJsonArray{token};
                field["description"] = label + ". Requires confirmation=" + token + ".";
                field["x-wosdbg-confirmation"] = label;
            }
        }
        if (key == "elfPath" || key == "kernelElfPath" || key == "path" || key == "output") {
            field["x-wosdbg-path"] = key == "output" ? "save" : "open";
        }
        if (key == "chunkSizes") {
            field["items"] = QJsonObject{{"type", "integer"}, {"minimum", 1}};
        } else if (key == "coredumpPaths") {
            field["items"] = QJsonObject{{"type", "string"}, {"x-wosdbg-path", "open"}};
            field["maxItems"] = 32;
        } else if (key == "memoryRanges") {
            field["maxItems"] = 16;
            field["items"] = QJsonObject{
                {"type", "object"},
                {"required", QJsonArray{"targetId", "address", "length"}},
                {"properties", QJsonObject{{"targetId", QJsonObject{{"type", "string"}, {"description", descriptions["targetId"]}}},
                                           {"address", QJsonObject{{"type", "string"}, {"description", descriptions["address"]}}},
                                           {"length", QJsonObject{{"type", "integer"}, {"minimum", 1}}}}}};
        }
        it.value() = field;
    }
    schema["properties"] = properties;
    tool["inputSchema"] = schema;
    return tool;
}

QString result_summary(const QJsonObject& result) {
    QStringList lines;
    lines << (result["ok"].toBool(true) ? "Completed." : "Failed.");
    // Keep IDs, errors and uncertainty visible even when the evidence is large.
    const QStringList priority{"errorCode", "error",    "message", "incidentId",    "dumpId",       "logId",     "sessionId",      "state",
                               "valid",     "degraded", "warning", "symbolWarning", "clockQuality", "truncated", "issuesTruncated"};
    auto append = [&lines](const QString& key, const QJsonValue& value) {
        if (value.isObject()) {
            lines << QString("%1: %2 fields").arg(key).arg(value.toObject().size());
        } else if (value.isArray()) {
            lines << QString("%1: %2 items").arg(key).arg(value.toArray().size());
        } else {
            const QString text = value.isString()
                                     ? value.toString()
                                     : QString::fromUtf8(QJsonDocument(QJsonArray{value}).toJson(QJsonDocument::Compact)).mid(1).chopped(1);
            lines << QString("%1: %2").arg(key, text.size() > 240 ? text.left(240) + "…" : text);
        }
    };
    for (const auto& key : priority) {
        if (result.contains(key)) {
            append(key, result[key]);
        }
    }
    int remaining = 8;
    for (auto it = result.begin(); it != result.end() && remaining > 0; ++it) {
        if (it.key() == "ok" || it.key() == "leaseToken" || priority.contains(it.key())) {
            continue;
        }
        append(it.key(), it.value());
        --remaining;
    }
    return lines.join('\n').left(2400) + "\nComplete evidence and any omitted fields are in structuredContent.";
}
}  // namespace wosdbg
