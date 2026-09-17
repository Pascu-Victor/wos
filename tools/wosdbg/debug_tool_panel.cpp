#include "debug_tool_panel.h"

#include <QComboBox>
#include <QHBoxLayout>
#include <QJsonDocument>
#include <QLabel>
#include <QLineEdit>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QScrollArea>
#include <QSplitter>
#include <QTreeWidget>
#include <QVBoxLayout>

#include "log_client.h"
#include "tool_arguments_form.h"
#include "tool_presentation.h"
#include "tool_result_view.h"

DebugToolPanel::DebugToolPanel(LogClient* client, QWidget* parent) : QWidget(parent), client(client) {
    auto* layout = new QVBoxLayout(this);
    layout->setContentsMargins(12, 12, 12, 12);
    auto* header = new QHBoxLayout();
    context_combo = new QComboBox(this);
    context_combo->setObjectName("analysis_context");
    context_combo->addItem("No active evidence — open an incident, log, or coredump");
    context_combo->setSizeAdjustPolicy(QComboBox::AdjustToMinimumContentsLengthWithIcon);
    context_combo->setMinimumContentsLength(24);
    header->addWidget(new QLabel("Evidence context", this));
    header->addWidget(context_combo, 1);
    auto* clear = new QPushButton("Clear context", this);
    clear->setToolTip("Forget suggestions; this does not close backend sessions or live leases.");
    header->addWidget(clear);
    layout->addLayout(header);
    auto* split = new QSplitter(Qt::Horizontal, this);
    auto* navigation = new QWidget(split);
    auto* nav_layout = new QVBoxLayout(navigation);
    nav_layout->setContentsMargins(0, 0, 8, 0);
    filter_edit = new QLineEdit(navigation);
    filter_edit->setObjectName("analysis_search");
    filter_edit->setPlaceholderText("Find an action…");
    filter_edit->setClearButtonEnabled(true);
    nav_layout->addWidget(filter_edit);
    tool_tree = new QTreeWidget(navigation);
    tool_tree->setObjectName("analysis_actions");
    tool_tree->setHeaderHidden(true);
    tool_tree->setMinimumWidth(180);
    tool_tree->setIndentation(12);
    nav_layout->addWidget(tool_tree);
    split->addWidget(navigation);
    auto* work = new QSplitter(Qt::Vertical, split);
    auto* inputs = new QWidget(work);
    auto* inputs_layout = new QVBoxLayout(inputs);
    inputs_layout->setContentsMargins(0, 0, 0, 0);
    title_label = new QLabel(inputs);
    QFont font = title_label->font();
    font.setPointSize(font.pointSize() + 3);
    font.setBold(true);
    title_label->setFont(font);
    title_label->setTextFormat(Qt::PlainText);
    inputs_layout->addWidget(title_label);
    description_label = new QLabel(inputs);
    description_label->setWordWrap(true);
    description_label->setTextFormat(Qt::PlainText);
    description_label->setTextInteractionFlags(Qt::TextSelectableByMouse);
    inputs_layout->addWidget(description_label);
    auto* scroll = new QScrollArea(inputs);
    scroll->setWidgetResizable(true);
    scroll->setFrameShape(QFrame::NoFrame);
    arguments_form = new ToolArgumentsForm(scroll);
    scroll->setWidget(arguments_form);
    inputs_layout->addWidget(scroll, 1);
    auto* run_row = new QHBoxLayout();
    execute_button = new QPushButton("Run analysis", inputs);
    execute_button->setObjectName("run_analysis");
    execute_button->setShortcut(QKeySequence("Ctrl+Return"));
    execute_button->setToolTip("Run the selected action (Ctrl+Enter)");
    run_row->addWidget(execute_button);
    status_label = new QLabel(inputs);
    status_label->setObjectName("analysis_status");
    status_label->setTextFormat(Qt::PlainText);
    status_label->setWordWrap(true);
    run_row->addWidget(status_label, 1);
    auto* schema_button = new QPushButton("Tool schema", inputs);
    schema_button->setCheckable(true);
    run_row->addWidget(schema_button);
    inputs_layout->addLayout(run_row);
    auto* schema_view = new QPlainTextEdit(inputs);
    schema_view->setReadOnly(true);
    schema_view->setMaximumHeight(180);
    schema_view->hide();
    inputs_layout->addWidget(schema_view);
    connect(schema_button, &QPushButton::toggled, this, [this, schema_view](bool visible) {
        schema_view->setPlainText(QString::fromUtf8(QJsonDocument(selected_tool()).toJson(QJsonDocument::Indented)));
        schema_view->setVisible(visible);
    });
    connect(tool_tree, &QTreeWidget::currentItemChanged, this, [this, schema_view]() {
        if (schema_view->isVisible()) {
            schema_view->setPlainText(QString::fromUtf8(QJsonDocument(selected_tool()).toJson(QJsonDocument::Indented)));
        }
    });
    work->addWidget(inputs);
    auto* results = new QWidget(work);
    auto* results_layout = new QVBoxLayout(results);
    results_layout->setContentsMargins(0, 0, 0, 0);
    followups = new QHBoxLayout();
    results_layout->addLayout(followups);
    result_view = new ToolResultView(results);
    results_layout->addWidget(result_view, 1);
    work->addWidget(results);
    work->setSizes({340, 420});
    split->addWidget(work);
    split->setStretchFactor(1, 4);
    split->setSizes({240, 900});
    layout->addWidget(split, 1);

    connect(filter_edit, &QLineEdit::textChanged, this, &DebugToolPanel::rebuild_tool_list);
    connect(tool_tree, &QTreeWidget::currentItemChanged, this, &DebugToolPanel::show_selected_tool);
    connect(arguments_form, &ToolArgumentsForm::changed, this, &DebugToolPanel::update_run_state);
    connect(execute_button, &QPushButton::clicked, this, &DebugToolPanel::execute_selected_tool);
    connect(client, &LogClient::tool_catalog_received, this, [this](const QJsonArray& tools) {
        catalog = tools;
        rebuild_tool_list();
        select_tool(requested_tool, requested_arguments);
    });
    connect(client, &LogClient::tool_result_received, this, &DebugToolPanel::on_tool_result);
    connect(client, &LogClient::connected, this, [this]() { this->client->request_tool_catalog(); });
    auto forget = [this]() {
        context = {};
        context_combo->clear();
        context_combo->addItem("No active evidence");
        form_tool.clear();
        show_selected_tool();
        rebuild_followups();
    };
    connect(clear, &QPushButton::clicked, this, forget);
    connect(client, &LogClient::disconnected, this, [this, forget]() {
        pending_request = 0;
        forget();
        update_run_state();
    });
    connect(client, &LogClient::connection_error, this, [this](const QString& error) {
        update_run_state();
        status_label->setText("Connection error: " + error);
    });
    connect(context_combo, &QComboBox::activated, this, [this]() {
        const QJsonObject selected = context_combo->currentData().toJsonObject();
        if (selected.contains("sessionId")) {
            context.remove("leaseToken");
            context.remove("targetId");
        }
        for (auto it = selected.begin(); it != selected.end(); ++it) {
            context[it.key()] = it.value();
        }
        form_tool.clear();
        show_selected_tool();
        rebuild_followups();
    });
    connect(result_view, &ToolResultView::context_selected, this, [this](QJsonObject selected) {
        const QString collection = selected["_collection"].toString();
        const QMap<QString, QString> identities{
            {"logSessions", "logId"}, {"coredumpSessions", "dumpId"}, {"incidentSessions", "incidentId"}, {"targets", "targetId"}};
        if (identities.contains(collection) && selected["id"].isString()) {
            selected[identities[collection]] = selected["id"];
        }
        // Coredump metadata has a process sessionId; only live tool results
        // may contribute live debugger session identities.
        if (!result_tool.contains("live")) {
            selected.remove("sessionId");
        }
        remember_context(selected);
        for (const QString& key : {"path", "address", "register", "targetId", "row", "line"}) {
            if (selected.contains(key)) {
                context[key] = selected[key];
            }
        }
        context["_collection"] = collection;
        rebuild_followups();
    });
    update_run_state();
    rebuild_followups();
    if (client->is_connected()) {
        client->request_tool_catalog();
    }
}

void DebugToolPanel::rebuild_tool_list() {
    const QString previous = selected_tool()["name"].toString();
    const QString filter = filter_edit->text();
    tool_tree->blockSignals(true);
    tool_tree->clear();
    const QStringList categories{
        "Workspace", "Incidents", "Logs", "Coredumps", "Memory and source", "Executable integrity", "Distributed analysis", "Live targets"};
    QMap<QString, QTreeWidgetItem*> groups;
    for (const auto& category : categories) {
        auto* item = new QTreeWidgetItem(tool_tree, {category});
        item->setFlags(item->flags() & ~Qt::ItemIsSelectable);
        groups.insert(category, item);
    }
    QTreeWidgetItem* selection = nullptr;
    for (const auto& value : catalog) {
        const QJsonObject tool = value.toObject();
        const QString name = tool["name"].toString();
        const QString title = tool["title"].toString(wosdbg::display_label(name));
        const QString category = tool["_meta"].toObject()["org.wosdbg/category"].toString("Workspace");
        if (!filter.isEmpty() && !(name + title + category + tool["description"].toString()).contains(filter, Qt::CaseInsensitive)) {
            continue;
        }
        auto* item = new QTreeWidgetItem(groups.value(category, groups["Workspace"]), {title});
        item->setData(0, Qt::UserRole, name);
        item->setToolTip(0, tool["description"].toString());
        if (!selection || name == previous) {
            selection = item;
        }
    }
    for (auto* group : groups) {
        group->setHidden(group->childCount() == 0);
        group->setExpanded(true);
    }
    tool_tree->setCurrentItem(selection);
    tool_tree->blockSignals(false);
    show_selected_tool();
}

QJsonObject DebugToolPanel::selected_tool() const {
    const auto* item = tool_tree->currentItem();
    const QString name = item ? item->data(0, Qt::UserRole).toString() : QString();
    for (const auto& value : catalog) {
        if (value.toObject()["name"].toString() == name) {
            return value.toObject();
        }
    }
    return {};
}

QJsonObject DebugToolPanel::suggested_arguments(const QJsonObject& tool) const {
    const QJsonObject schema = tool["inputSchema"].toObject();
    const QJsonObject properties = schema["properties"].toObject();
    QJsonObject arguments;
    for (const auto& value : schema["required"].toArray()) {
        const QString key = value.toString();
        if (context.contains(key) && key != "confirmation") {
            arguments[key] = context[key];
        }
    }
    for (const QString& key : {"address", "targetId"}) {
        if (properties.contains(key) && context.contains(key)) {
            arguments[key] = context[key];
        }
    }
    return arguments;
}

void DebugToolPanel::show_selected_tool() {
    const QJsonObject tool = selected_tool();
    const QString name = tool["name"].toString();
    if (form_tool != name) {
        form_tool = name;
        arguments_form->set_schema(tool["inputSchema"].toObject(), suggested_arguments(tool));
    }
    title_label->setText(tool["title"].toString(wosdbg::display_label(name)));
    description_label->setText(tool.isEmpty() ? "No matching actions. Try a different search." : tool["description"].toString());
    update_run_state();
}

void DebugToolPanel::select_tool(const QString& name, const QJsonObject& arguments) {
    requested_tool = name.startsWith("wosdbg.") ? name : "wosdbg." + name;
    requested_arguments = arguments;
    filter_edit->clear();
    for (int group = 0; group < tool_tree->topLevelItemCount(); ++group) {
        auto* category = tool_tree->topLevelItem(group);
        for (int index = 0; index < category->childCount(); ++index) {
            auto* item = category->child(index);
            if (item->data(0, Qt::UserRole).toString() != requested_tool) {
                continue;
            }
            form_tool.clear();
            tool_tree->setCurrentItem(item);
            show_selected_tool();
            QJsonObject suggestions = suggested_arguments(selected_tool());
            for (auto it = arguments.begin(); it != arguments.end(); ++it) {
                suggestions[it.key()] = it.value();
            }
            arguments_form->set_schema(selected_tool()["inputSchema"].toObject(), suggestions);
            tool_tree->scrollToItem(item);
            update_run_state();
            return;
        }
    }
}

void DebugToolPanel::update_run_state() {
    QString error;
    const auto arguments = arguments_form->arguments(&error);
    Q_UNUSED(arguments);
    const bool connected = client->is_connected();
    execute_button->setEnabled(connected && pending_request == 0 && !selected_tool().isEmpty() && error.isEmpty());
    if (!connected) {
        status_label->setText("Disconnected. Connect to a WOSDBG backend to run analysis.");
    } else if (pending_request != 0) {
        status_label->setText("Running " + wosdbg::display_label(pending_tool) + "… You can browse other actions while it completes.");
    } else {
        status_label->setText(error.isEmpty() ? "Ready · * required · unchecked options use backend defaults" : error);
    }
}

void DebugToolPanel::execute_selected_tool() {
    if (pending_request != 0 || !client->is_connected()) {
        return;
    }
    const QJsonObject tool = selected_tool();
    QString error;
    const QJsonObject arguments = arguments_form->arguments(&error);
    if (tool.isEmpty() || !error.isEmpty()) {
        update_run_state();
        return;
    }
    pending_tool = tool["name"].toString();
    pending_arguments = arguments;
    pending_request = client->call_tool(pending_tool, arguments);
    update_run_state();
}

void DebugToolPanel::remember_context(const QJsonObject& result) {
    for (const QString& key : {"incidentId", "dumpId", "logId", "sessionId"}) {
        const QString id = result[key].toString();
        if (id.isEmpty()) {
            continue;
        }
        const QString label = wosdbg::display_label(key) + ": " + id;
        int index = context_combo->findText(label);
        QJsonObject saved = index < 0 ? QJsonObject{} : context_combo->itemData(index).toJsonObject();
        saved[key] = id;
        if (key == "sessionId") {
            if (!result["leaseToken"].toString().isEmpty()) {
                saved["leaseToken"] = result["leaseToken"];
            }
            if (context[key] != result[key]) {
                context.remove("targetId");
            }
            context.remove("leaseToken");
        }
        for (auto it = saved.begin(); it != saved.end(); ++it) {
            context[it.key()] = it.value();
        }
        if (index < 0) {
            if (context_combo->count() >= 32) {
                context_combo->removeItem(0);
            }
            context_combo->addItem(label, saved);
            index = context_combo->count() - 1;
        } else {
            context_combo->setItemData(index, saved);
        }
        context_combo->setCurrentIndex(index);
    }
    for (const QString& key : {"targetId", "transcript", "digest"}) {
        if (result.contains(key)) {
            context[key] = result[key];
        }
    }
}

void DebugToolPanel::rebuild_followups() {
    while (auto* item = followups->takeAt(0)) {
        delete item->widget();
        delete item;
    }
    auto add = [this](const QString& label, const QString& tool) {
        auto* button = new QPushButton(label, this);
        followups->addWidget(button);
        connect(button, &QPushButton::clicked, this, [this, tool]() { select_tool(tool); });
    };
    const auto selected = context_combo->currentData().toJsonObject();
    const QString collection = context["_collection"].toString();
    if (context.contains("path") && (collection == "logs" || collection == "coredumps")) {
        const QString tool = collection == "logs" ? "load_log" : "open_coredump";
        auto* open = new QPushButton("Open selected " + QString(collection == "logs" ? "log" : "coredump"), this);
        followups->addWidget(open);
        connect(open, &QPushButton::clicked, this, [this, tool]() { select_tool(tool, {{"path", context["path"]}}); });
    } else if (collection == "targets" && context.contains("targetId")) {
        add("Open live lease", "open_live_session");
        if (context.contains("sessionId")) {
            add("Read registers", "read_live_registers");
            add("Close live lease", "close_live_session");
        }
    } else if (selected.contains("sessionId")) {
        add("Session status", "get_live_session");
        add("Close live lease", "close_live_session");
    } else if (selected.contains("dumpId")) {
        add("Crash summary", "get_crash_summary");
        add("Backtrace", "backtrace_coredump");
        add("Inspect memory", "get_memory_context");
    } else if (selected.contains("incidentId")) {
        add("Incident summary", "summarize_incident");
        add("Browse inventory", "get_incident_inventory");
    } else if (selected.contains("logId")) {
        add("Search log", "search_log");
        add("Browse entries", "get_log_entries");
    } else {
        add("Open incident", "load_incident");
        add("Open coredump", "open_coredump");
        add("Open log", "load_log");
    }
    followups->addStretch();
}

void DebugToolPanel::on_tool_result(quint64 request_id, const QJsonObject& result) {
    if (request_id != pending_request || pending_request == 0) {
        return;
    }
    pending_request = 0;
    result_tool = pending_tool;
    context.remove("_collection");
    context.remove("path");
    context.remove("address");
    remember_context(result);
    if (pending_tool == "wosdbg.close_live_session" && result["ok"].toBool()) {
        const QString closed_id = pending_arguments["sessionId"].toString();
        for (int index = context_combo->count() - 1; index >= 0; --index) {
            if (context_combo->itemData(index).toJsonObject()["sessionId"].toString() == closed_id) {
                context_combo->removeItem(index);
            }
        }
        if (context["sessionId"].toString() == closed_id) {
            context.remove("sessionId");
            context.remove("leaseToken");
            context.remove("targetId");
        }
    }
    result_view->set_result(result, wosdbg::display_label(result_tool));
    rebuild_followups();
    update_run_state();
    status_label->setText(wosdbg::display_label(pending_tool) +
                          (result["ok"].toBool(true) ? " completed." : " failed: " + result["error"].toString()));
}
