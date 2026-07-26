#include "debug_tool_panel.h"

#include <QApplication>
#include <QClipboard>
#include <QComboBox>
#include <QFont>
#include <QFontDatabase>
#include <QHBoxLayout>
#include <QJsonDocument>
#include <QJsonParseError>
#include <QLabel>
#include <QLineEdit>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QSplitter>
#include <QVBoxLayout>
#include <QWidget>
#include <QtTypes>

#include "log_client.h"

DebugToolPanel::DebugToolPanel(LogClient* client, QWidget* parent)
    : QWidget(parent),
      client(client),
      filter_edit(new QLineEdit(this)),
      tool_combo(new QComboBox(this)),
      description_label(new QLabel(this)),
      schema_edit(new QPlainTextEdit(this)),
      arguments_edit(new QPlainTextEdit(this)),
      result_edit(new QPlainTextEdit(this)),
      execute_button(new QPushButton("Run tool", this)) {
    auto* layout = new QVBoxLayout(this);
    auto* selector_layout = new QHBoxLayout();
    filter_edit->setPlaceholderText("Filter the shared MCP / CLI / GUI tool catalog...");
    tool_combo->setMinimumWidth(320);
    selector_layout->addWidget(filter_edit);
    selector_layout->addWidget(tool_combo);
    layout->addLayout(selector_layout);

    description_label->setWordWrap(true);
    description_label->setTextInteractionFlags(Qt::TextSelectableByMouse);
    layout->addWidget(description_label);

    auto* editors = new QSplitter(Qt::Horizontal, this);
    auto* input_widget = new QWidget(editors);
    auto* input_layout = new QVBoxLayout(input_widget);
    input_layout->setContentsMargins(0, 0, 0, 0);
    input_layout->addWidget(new QLabel("Arguments (JSON)", input_widget));
    input_layout->addWidget(arguments_edit);
    input_layout->addWidget(new QLabel("Input schema", input_widget));
    input_layout->addWidget(schema_edit);
    editors->addWidget(input_widget);

    auto* result_widget = new QWidget(editors);
    auto* result_layout = new QVBoxLayout(result_widget);
    result_layout->setContentsMargins(0, 0, 0, 0);
    result_layout->addWidget(new QLabel("Structured result", result_widget));
    result_layout->addWidget(result_edit);
    auto* result_buttons = new QHBoxLayout();
    auto* copy_button = new QPushButton("Copy result", result_widget);
    result_buttons->addWidget(execute_button);
    result_buttons->addWidget(copy_button);
    result_buttons->addStretch();
    result_layout->addLayout(result_buttons);
    editors->addWidget(result_widget);
    editors->setStretchFactor(0, 1);
    editors->setStretchFactor(1, 2);
    layout->addWidget(editors);

    const QFont FIXED_FONT = QFontDatabase::systemFont(QFontDatabase::FixedFont);
    schema_edit->setFont(FIXED_FONT);
    arguments_edit->setFont(FIXED_FONT);
    result_edit->setFont(FIXED_FONT);
    schema_edit->setReadOnly(true);
    result_edit->setReadOnly(true);
    arguments_edit->setPlaceholderText("{}");

    connect(filter_edit, &QLineEdit::textChanged, this, &DebugToolPanel::rebuild_tool_list);
    connect(tool_combo, &QComboBox::currentTextChanged, this, &DebugToolPanel::show_selected_tool);
    connect(execute_button, &QPushButton::clicked, this, &DebugToolPanel::execute_selected_tool);
    connect(copy_button, &QPushButton::clicked, this, &DebugToolPanel::copy_result);
    connect(client, &LogClient::tool_catalog_received, this, &DebugToolPanel::on_catalog_received);
    connect(client, &LogClient::tool_result_received, this, &DebugToolPanel::on_tool_result);
    connect(client, &LogClient::connected, this, [this]() { this->client->request_tool_catalog(); });

    execute_button->setEnabled(false);
    result_edit->setPlainText("Loading the shared tool catalog...");
    if (client->is_connected()) {
        client->request_tool_catalog();
    }
}

void DebugToolPanel::on_catalog_received(const QJsonArray& tools) {
    catalog = tools;
    rebuild_tool_list();
}

void DebugToolPanel::rebuild_tool_list() {
    const QString PREVIOUS = tool_combo->currentData().toString();
    const QString FILTER = filter_edit->text();
    tool_combo->blockSignals(true);
    tool_combo->clear();
    for (const auto& value : catalog) {
        const QJsonObject TOOL = value.toObject();
        const QString NAME = TOOL["name"].toString();
        const QString DESCRIPTION = TOOL["description"].toString();
        if (!FILTER.isEmpty() && !NAME.contains(FILTER, Qt::CaseInsensitive) && !DESCRIPTION.contains(FILTER, Qt::CaseInsensitive)) {
            continue;
        }
        tool_combo->addItem(NAME, NAME);
    }
    const int PREVIOUS_INDEX = tool_combo->findData(PREVIOUS);
    if (PREVIOUS_INDEX >= 0) {
        tool_combo->setCurrentIndex(PREVIOUS_INDEX);
    }
    tool_combo->blockSignals(false);
    show_selected_tool();
}

auto DebugToolPanel::selected_tool() const -> QJsonObject {
    const QString NAME = tool_combo->currentData().toString();
    for (const auto& value : catalog) {
        if (value.toObject()["name"].toString() == NAME) {
            return value.toObject();
        }
    }
    return {};
}

auto DebugToolPanel::suggested_arguments(const QJsonObject& tool) const -> QJsonObject {
    const QJsonObject SCHEMA = tool["inputSchema"].toObject();
    const QJsonObject PROPERTIES = SCHEMA["properties"].toObject();
    const QJsonArray REQUIRED = SCHEMA["required"].toArray();
    QJsonObject arguments;
    for (const auto& value : REQUIRED) {
        const QString NAME = value.toString();
        if (context.contains(NAME)) {
            arguments[NAME] = context[NAME];
            continue;
        }
        const QString TYPE = PROPERTIES[NAME].toObject()["type"].toString();
        if (TYPE == "integer" || TYPE == "number") {
            arguments[NAME] = 0;
        } else if (TYPE == "boolean") {
            arguments[NAME] = false;
        } else if (TYPE == "array") {
            arguments[NAME] = QJsonArray{};
        } else if (TYPE == "object") {
            arguments[NAME] = QJsonObject{};
        } else {
            arguments[NAME] = "";
        }
    }
    return arguments;
}

void DebugToolPanel::show_selected_tool() {
    const QJsonObject TOOL = selected_tool();
    execute_button->setEnabled(!TOOL.isEmpty());
    description_label->setText(TOOL["description"].toString());
    schema_edit->setPlainText(QString::fromUtf8(QJsonDocument(TOOL["inputSchema"].toObject()).toJson(QJsonDocument::Indented)));
    arguments_edit->setPlainText(QString::fromUtf8(QJsonDocument(suggested_arguments(TOOL)).toJson(QJsonDocument::Indented)));
}

void DebugToolPanel::execute_selected_tool() {
    const QJsonObject TOOL = selected_tool();
    if (TOOL.isEmpty()) {
        return;
    }
    QJsonParseError error;
    const QJsonDocument ARGUMENTS = QJsonDocument::fromJson(arguments_edit->toPlainText().toUtf8(), &error);
    if (error.error != QJsonParseError::NoError || !ARGUMENTS.isObject()) {
        result_edit->setPlainText(QString("Arguments must be a JSON object:\n%1").arg(error.errorString()));
        return;
    }
    const quint64 REQUEST_ID = client->call_tool(TOOL["name"].toString(), ARGUMENTS.object());
    pending_requests.insert(REQUEST_ID);
    execute_button->setEnabled(false);
    result_edit->setPlainText(QString("Running %1 (request %2)...").arg(TOOL["name"].toString()).arg(REQUEST_ID));
}

void DebugToolPanel::remember_context(const QJsonObject& result) {
    for (const QString& key : {"dumpId", "logId"}) {
        if (!result[key].toString().isEmpty()) {
            context[key] = result[key];
        }
    }
}

void DebugToolPanel::on_tool_result(quint64 request_id, const QJsonObject& result) {
    if (!pending_requests.remove(request_id)) {
        return;
    }
    remember_context(result);
    result_edit->setPlainText(QString::fromUtf8(QJsonDocument(result).toJson(QJsonDocument::Indented)));
    execute_button->setEnabled(true);
}

void DebugToolPanel::copy_result() const { QApplication::clipboard()->setText(result_edit->toPlainText()); }
