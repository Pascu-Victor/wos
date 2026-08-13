#include "telemetry_log.h"

#include <QFile>
#include <QFileInfo>
#include <QJsonArray>
#include <QJsonObject>
#include <QJsonValue>
#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <wos/telemetry.hpp>

namespace {

constexpr qint64 MAX_TELEMETRY_FILE_BYTES = 256LL * 1024 * 1024;
constexpr qsizetype MAX_TELEMETRY_LINE_BYTES = 1024 * 1024;
constexpr size_t MAX_TELEMETRY_RECORDS = 1'000'000;
constexpr int64_t MAX_EXACT_QJSON_INTEGER = (INT64_C(1) << 53) - 1;

auto qstring(std::string_view value) -> QString { return QString::fromUtf8(value.data(), static_cast<qsizetype>(value.size())); }

auto json_projection(const wos::telemetry::Value& value) -> QJsonValue {
    if (value.is_null()) {
        return QJsonValue(QJsonValue::Null);
    }
    if (const auto* boolean = value.as_bool()) {
        return *boolean;
    }
    if (const auto* text = value.as_string()) {
        return qstring(*text);
    }
    if (const auto* number = value.as_number()) {
        int64_t integer = 0;
        const auto [end, error] = std::from_chars(number->lexeme.data(), number->lexeme.data() + number->lexeme.size(), integer);
        if (error == std::errc{} && end == number->lexeme.data() + number->lexeme.size() && integer >= -MAX_EXACT_QJSON_INTEGER &&
            integer <= MAX_EXACT_QJSON_INTEGER) {
            return static_cast<double>(integer);
        }
        // QJson stores numbers as doubles. Retain an exact lexeme as a string
        // whenever conversion could change the telemetry value.
        return qstring(number->lexeme);
    }
    if (const auto* array = value.as_array()) {
        QJsonArray result;
        for (const auto& child : *array) {
            result.append(json_projection(child));
        }
        return result;
    }
    QJsonObject result;
    if (const auto* object = value.as_object()) {
        for (const auto& [key, child] : *object) {
            result.insert(qstring(key), json_projection(child));
        }
    }
    return result;
}

auto member_string(const wos::telemetry::Value& object, std::string_view member) -> QString {
    const auto value = wos::telemetry::object_string(object, member);
    return value ? qstring(*value) : QString{};
}

auto envelope_member_string(const wos::telemetry::Value& envelope, std::string_view object_name, std::string_view member) -> QString {
    const auto* object = wos::telemetry::object_member(envelope, object_name);
    return object == nullptr ? QString{} : member_string(*object, member);
}

auto first_nonempty_line(QFile& file, QString& error) -> QByteArray {
    while (!file.atEnd()) {
        QByteArray line = file.readLine(MAX_TELEMETRY_LINE_BYTES + 2);
        if (line.size() > MAX_TELEMETRY_LINE_BYTES || (!line.endsWith('\n') && !file.atEnd())) {
            error = "Telemetry JSONL record exceeds the 1 MiB line limit";
            return {};
        }
        if (!line.trimmed().isEmpty()) {
            return line;
        }
    }
    return {};
}

auto line_view(const QByteArray& line) -> std::string_view {
    qsizetype size = line.size();
    if (size > 0 && line[size - 1] == '\n') {
        --size;
    }
    if (size > 0 && line[size - 1] == '\r') {
        --size;
    }
    return {line.constData(), static_cast<size_t>(size)};
}

auto parse_error_message(size_t line_number, const wos::telemetry::Error& error) -> QString {
    return QString("Invalid telemetry JSONL at line %1, byte %2: %3").arg(line_number).arg(error.offset).arg(qstring(error.message));
}

}  // namespace

auto load_telemetry_jsonl(const QString& path) -> TelemetryLogResult {
    TelemetryLogResult result;
    QFile file(path);
    if (!file.open(QIODevice::ReadOnly)) {
        return result;
    }
    if (file.size() > MAX_TELEMETRY_FILE_BYTES) {
        result.recognized = QFileInfo(path).suffix().compare("jsonl", Qt::CaseInsensitive) == 0;
        if (result.recognized) {
            result.error = "Telemetry JSONL input exceeds the 256 MiB file limit";
        }
        return result;
    }

    QString probe_error;
    const QByteArray FIRST_LINE = first_nonempty_line(file, probe_error);
    const bool JSONL_EXTENSION = QFileInfo(path).suffix().compare("jsonl", Qt::CaseInsensitive) == 0;
    bool valid_probe = false;
    if (!FIRST_LINE.isEmpty()) {
        const auto parsed = wos::telemetry::parse(line_view(FIRST_LINE));
        valid_probe = parsed && wos::telemetry::validate_envelope(*parsed.value);
    }
    result.recognized = JSONL_EXTENSION || valid_probe || FIRST_LINE.contains("wos.telemetry");
    if (!result.recognized) {
        return result;
    }
    if (!probe_error.isEmpty()) {
        result.error = probe_error;
        return result;
    }

    if (!file.seek(0)) {
        result.error = "Cannot rewind telemetry JSONL input";
        return result;
    }
    size_t physical_line = 0;
    std::optional<QString> common_node;
    std::optional<QString> common_clock_domain;
    while (!file.atEnd()) {
        QByteArray line = file.readLine(MAX_TELEMETRY_LINE_BYTES + 2);
        ++physical_line;
        if (line.size() > MAX_TELEMETRY_LINE_BYTES || (!line.endsWith('\n') && !file.atEnd())) {
            result.error = QString("Telemetry JSONL record at line %1 exceeds the 1 MiB line limit").arg(physical_line);
            return result;
        }
        if (line.trimmed().isEmpty()) {
            continue;
        }
        if (result.entries.size() >= MAX_TELEMETRY_RECORDS) {
            result.error = "Telemetry JSONL input exceeds the 1000000-record limit";
            return result;
        }

        wos::telemetry::Limits limits;
        limits.max_input_bytes = MAX_TELEMETRY_LINE_BYTES;
        const auto parsed = wos::telemetry::parse(line_view(line), limits);
        if (!parsed) {
            result.error = parse_error_message(physical_line, parsed.error);
            return result;
        }
        wos::telemetry::Error envelope_error;
        if (!wos::telemetry::validate_envelope(*parsed.value, &envelope_error)) {
            result.error = parse_error_message(physical_line, envelope_error);
            return result;
        }
        const auto canonical = wos::telemetry::serialize(*parsed.value, limits);
        if (!canonical) {
            result.error = parse_error_message(physical_line, canonical.error);
            return result;
        }

        LogEntry entry;
        entry.line_number = static_cast<int>(std::min(physical_line, static_cast<size_t>(std::numeric_limits<int>::max())));
        entry.type = EntryType::OTHER;
        entry.function = member_string(*parsed.value, "source").toStdString();
        entry.assembly = member_string(*parsed.value, "kind").toStdString();
        entry.original_line = *canonical.text;
        entry.has_telemetry_envelope = true;
        entry.telemetry_json = *canonical.text;
        entry.telemetry_envelope = json_projection(*parsed.value).toObject();
        result.entries.push_back(std::move(entry));

        QString NODE = envelope_member_string(*parsed.value, "identity", "node_id");
        if (NODE.isEmpty()) {
            // Explicit compatibility alias for early development envelopes.
            NODE = envelope_member_string(*parsed.value, "identity", "node");
        }
        const QString CLOCK_DOMAIN = envelope_member_string(*parsed.value, "clock", "domain");
        if (!common_node) {
            common_node = NODE;
        } else if (*common_node != NODE) {
            common_node = QString{};
        }
        if (!common_clock_domain) {
            common_clock_domain = CLOCK_DOMAIN;
        } else if (*common_clock_domain != CLOCK_DOMAIN) {
            common_clock_domain = QString{};
        }
    }
    result.node_id = common_node.value_or(QString{});
    result.clock_domain = common_clock_domain.value_or(QString{});
    return result;
}
