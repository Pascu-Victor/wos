#include "debug_cli.h"

#include <QByteArray>
#include <QFile>
#include <QHash>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QJsonParseError>
#include <QJsonValue>
#include <QRegularExpression>
#include <QStringList>
#include <QTextStream>
#include <QtTypes>
#include <cstdio>

#include "debug_analysis_service.h"

namespace {

auto write_json(const QJsonValue& value, FILE* stream) -> void {
    QJsonDocument document;
    if (value.isArray()) {
        document = QJsonDocument(value.toArray());
    } else {
        document = QJsonDocument(value.toObject());
    }
    QTextStream output(stream);
    output << document.toJson(QJsonDocument::Indented);
    output.flush();
}

auto fail(const QString& message) -> int {
    write_json(QJsonObject{{"ok", false}, {"error", message}}, stderr);
    return 2;
}

auto parse_json(const QByteArray& bytes, QJsonParseError* error) -> QJsonValue {
    const QJsonDocument DOCUMENT = QJsonDocument::fromJson(bytes, error);
    if (DOCUMENT.isArray()) {
        return DOCUMENT.array();
    }
    if (DOCUMENT.isObject()) {
        return DOCUMENT.object();
    }
    return {};
}

auto lookup_reference(QString reference, const QHash<QString, QJsonObject>& results) -> QJsonValue {
    if (reference.startsWith("${") && reference.endsWith('}')) {
        reference = reference.sliced(2, reference.size() - 3);
    } else if (reference.startsWith('$')) {
        reference.remove(0, 1);
    } else {
        return {QJsonValue::Undefined};
    }
    const QStringList PARTS = reference.split('.', Qt::SkipEmptyParts);
    if (PARTS.isEmpty() || !results.contains(PARTS.first())) {
        return {QJsonValue::Undefined};
    }
    QJsonValue value = results.value(PARTS.first());
    for (qsizetype i = 1; i < PARTS.size(); ++i) {
        if (value.isObject()) {
            if (!value.toObject().contains(PARTS[i])) {
                return {QJsonValue::Undefined};
            }
            value = value.toObject().value(PARTS[i]);
        } else if (value.isArray()) {
            bool ok = false;
            const int INDEX = PARTS[i].toInt(&ok);
            if (!ok || INDEX < 0 || INDEX >= value.toArray().size()) {
                return {QJsonValue::Undefined};
            }
            value = value.toArray().at(INDEX);
        } else {
            return {QJsonValue::Undefined};
        }
    }
    return value;
}

auto resolve_references(const QJsonValue& value, const QHash<QString, QJsonObject>& results, QString* error) -> QJsonValue {
    if (value.isString()) {
        const QString TEXT = value.toString();
        static const QRegularExpression WHOLE_REFERENCE(R"(^(\$[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*|\$\{[^}]+\})$)");
        if (WHOLE_REFERENCE.match(TEXT).hasMatch()) {
            QJsonValue resolved = lookup_reference(TEXT, results);
            if (resolved.isUndefined()) {
                *error = QString("Unresolved batch reference: %1").arg(TEXT);
                return {};
            }
            return resolved;
        }
        static const QRegularExpression EMBEDDED_REFERENCE(R"(\$\{([^}]+)\})");
        QString resolved_text = TEXT;
        auto matches = EMBEDDED_REFERENCE.globalMatch(TEXT);
        while (matches.hasNext()) {
            const auto MATCH = matches.next();
            const QJsonValue RESOLVED = lookup_reference(QString("${%1}").arg(MATCH.captured(1)), results);
            if (RESOLVED.isUndefined() || RESOLVED.isArray() || RESOLVED.isObject()) {
                *error = QString("Unresolved or non-scalar batch reference: %1").arg(MATCH.captured(0));
                return {};
            }
            resolved_text.replace(MATCH.captured(0), RESOLVED.toVariant().toString());
        }
        return resolved_text;
    }
    if (value.isArray()) {
        QJsonArray resolved;
        for (const auto& item : value.toArray()) {
            resolved.append(resolve_references(item, results, error));
            if (!error->isEmpty()) {
                return {};
            }
        }
        return resolved;
    }
    if (value.isObject()) {
        const QJsonObject OBJECT = value.toObject();
        QJsonObject resolved;
        for (auto it = OBJECT.constBegin(); it != OBJECT.constEnd(); ++it) {
            resolved[it.key()] = resolve_references(it.value(), results, error);
            if (!error->isEmpty()) {
                return {};
            }
        }
        return resolved;
    }
    return value;
}

auto read_batch(const QString& path, QString* error) -> QByteArray {
    if (path == "-") {
        QFile input;
        if (!input.open(stdin, QIODevice::ReadOnly)) {
            *error = "Could not read batch JSON from stdin";
            return {};
        }
        return input.readAll();
    }
    QFile input(path);
    if (!input.open(QIODevice::ReadOnly)) {
        *error = QString("Could not open batch file: %1").arg(path);
        return {};
    }
    return input.readAll();
}

auto run_batch(DebugAnalysisService& analysis, const QString& path) -> int {
    QString read_error;
    const QByteArray DATA = read_batch(path, &read_error);
    if (!read_error.isEmpty()) {
        return fail(read_error);
    }
    QJsonParseError parse_error;
    const QJsonValue ROOT = parse_json(DATA, &parse_error);
    if (parse_error.error != QJsonParseError::NoError) {
        return fail(QString("Invalid batch JSON: %1").arg(parse_error.errorString()));
    }

    QJsonArray calls;
    bool continue_on_error = false;
    if (ROOT.isArray()) {
        calls = ROOT.toArray();
    } else {
        const QJsonObject OBJECT = ROOT.toObject();
        calls = OBJECT["calls"].toArray();
        continue_on_error = OBJECT["continueOnError"].toBool(false);
    }
    if (calls.isEmpty()) {
        return fail("Batch input must be an array of calls or an object containing a non-empty 'calls' array");
    }

    QHash<QString, QJsonObject> named_results;
    QJsonArray outputs;
    bool all_ok = true;
    int index = 0;
    for (const auto& value : calls) {
        const QJsonObject CALL = value.toObject();
        const QString OPERATION = CALL["operation"].toString();
        QString name = CALL["tool"].toString(CALL["name"].toString());
        if (!name.startsWith("wosdbg.") && !name.isEmpty()) {
            name.prepend("wosdbg.");
        }
        const QString ID = CALL["id"].toString(QString::number(index));
        QString reference_error;
        const QJsonObject ARGUMENTS =
            resolve_references(CALL["arguments"].isObject() ? CALL["arguments"] : CALL["args"], named_results, &reference_error).toObject();
        QJsonObject result;
        if (!reference_error.isEmpty()) {
            result = QJsonObject{{"ok", false}, {"error", reference_error}};
        } else if (!name.isEmpty()) {
            result = analysis.invoke_tool(name, ARGUMENTS);
        } else if (OPERATION == "resources/list" || OPERATION == "list_resources") {
            result = QJsonObject{{"ok", true}, {"resources", analysis.list_resources()}};
        } else if (OPERATION == "resources/templates/list" || OPERATION == "list_resource_templates") {
            result = QJsonObject{{"ok", true}, {"resourceTemplates", DebugAnalysisService::list_resource_templates()}};
        } else if (OPERATION == "resources/read" || OPERATION == "read_resource") {
            const QJsonValue URI =
                resolve_references(CALL["uri"].isString() ? CALL["uri"] : ARGUMENTS["uri"], named_results, &reference_error);
            result =
                reference_error.isEmpty() ? analysis.read_resource(URI.toString()) : QJsonObject{{"ok", false}, {"error", reference_error}};
        } else {
            result = QJsonObject{{"ok", false}, {"error", "Batch call requires 'tool' or a supported resource 'operation'"}};
        }
        named_results[ID] = result;
        outputs.append(QJsonObject{{"id", ID}, {"tool", name}, {"operation", OPERATION}, {"result", result}});
        ++index;
        if (!result["ok"].toBool(true)) {
            all_ok = false;
            if (!continue_on_error) {
                break;
            }
        }
    }
    write_json(QJsonObject{{"ok", all_ok}, {"calls", outputs}}, stdout);
    return all_ok ? 0 : 1;
}

}  // namespace

auto run_debug_cli(DebugAnalysisService& analysis, const DebugCliOptions& options) -> int {
    if (options.list_tools) {
        write_json(DebugAnalysisService::tool_catalog(), stdout);
        return 0;
    }
    if (options.list_resources) {
        write_json(QJsonObject{{"resources", analysis.list_resources()}}, stdout);
        return 0;
    }
    if (options.list_resource_templates) {
        write_json(QJsonObject{{"resourceTemplates", DebugAnalysisService::list_resource_templates()}}, stdout);
        return 0;
    }
    if (!options.resource_uri.isEmpty()) {
        const QJsonObject RESULT = analysis.read_resource(options.resource_uri);
        write_json(RESULT, stdout);
        return RESULT["ok"].toBool(true) ? 0 : 1;
    }
    if (!options.batch_path.isEmpty()) {
        return run_batch(analysis, options.batch_path);
    }
    if (options.tool_name.isEmpty()) {
        return fail("No CLI operation selected");
    }

    QJsonObject arguments;
    if (!options.arguments_json.trimmed().isEmpty()) {
        QJsonParseError parse_error;
        const QJsonDocument DOCUMENT = QJsonDocument::fromJson(options.arguments_json.toUtf8(), &parse_error);
        if (parse_error.error != QJsonParseError::NoError || !DOCUMENT.isObject()) {
            return fail(QString("--arguments must be a JSON object: %1").arg(parse_error.errorString()));
        }
        arguments = DOCUMENT.object();
    }
    QString name = options.tool_name;
    if (!name.startsWith("wosdbg.")) {
        name.prepend("wosdbg.");
    }
    const QJsonObject RESULT = analysis.invoke_tool(name, arguments);
    write_json(RESULT, stdout);
    return RESULT["ok"].toBool(true) ? 0 : 1;
}
