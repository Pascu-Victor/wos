#pragma once

#include <QString>
#include <vector>

#include "log_entry.h"

struct TelemetryLogResult {
    bool recognized{false};
    QString error;
    QString node_id;
    QString clock_domain;
    std::vector<LogEntry> entries;
};

[[nodiscard]] auto load_telemetry_jsonl(const QString& path) -> TelemetryLogResult;
