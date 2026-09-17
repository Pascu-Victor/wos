#include "debugger_workspace.h"

#include <QApplication>
#include <QClipboard>
#include <QComboBox>
#include <QDialog>
#include <QDialogButtonBox>
#include <QFileDialog>
#include <QFileInfo>
#include <QFontDatabase>
#include <QHeaderView>
#include <QInputDialog>
#include <QJsonArray>
#include <QLabel>
#include <QLineEdit>
#include <QMenu>
#include <QMessageBox>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QSplitter>
#include <QStackedWidget>
#include <QStyle>
#include <QTabWidget>
#include <QTableWidget>
#include <QTimer>
#include <QVBoxLayout>

#include "log_client.h"

namespace {
QString text(const QJsonValue& value) {
    if (value.isArray()) {
        QStringList parts;
        for (const auto& item : value.toArray()) {
            parts << text(item);
        }
        return parts.join(" · ");
    }
    if (value.isObject()) {
        const auto obj = value.toObject();
        return obj["symbol"].toString(obj["name"].toString(obj["description"].toString()));
    }
    return value.toVariant().toString();
}

QTableWidget* table(const QString& name, const QStringList& columns, QWidget* parent) {
    auto* result = new QTableWidget(0, columns.size(), parent);
    result->setObjectName(name);
    result->setHorizontalHeaderLabels(columns);
    result->setEditTriggers(QAbstractItemView::NoEditTriggers);
    result->setSelectionBehavior(QAbstractItemView::SelectRows);
    result->setSelectionMode(QAbstractItemView::SingleSelection);
    result->setAlternatingRowColors(true);
    result->setShowGrid(false);
    result->setWordWrap(false);
    result->verticalHeader()->hide();
    result->verticalHeader()->setDefaultSectionSize(26);
    result->horizontalHeader()->setStretchLastSection(true);
    result->horizontalHeader()->setMinimumSectionSize(48);
    result->setFont(QFontDatabase::systemFont(QFontDatabase::FixedFont));
    result->setContextMenuPolicy(Qt::CustomContextMenu);
    QObject::connect(result, &QWidget::customContextMenuRequested, result, [result](QPoint point) {
        const auto* item = result->itemAt(point);
        if (!item) {
            return;
        }
        result->setCurrentCell(item->row(), item->column());
        QMenu menu(result);
        menu.addActions(result->actions());
        if (!result->actions().isEmpty()) {
            menu.addSeparator();
        }
        menu.addAction("Copy value", [item]() { QApplication::clipboard()->setText(item->text()); });
        menu.addAction("Copy row", [result, item]() {
            QStringList values;
            for (int col = 0; col < result->columnCount(); ++col) {
                values << result->item(item->row(), col)->text();
            }
            QApplication::clipboard()->setText(values.join('\t'));
        });
        menu.exec(result->viewport()->mapToGlobal(point));
    });
    return result;
}

void fill(QTableWidget* table, const QJsonArray& rows, const QStringList& keys) {
    // Build every cell before exposing the new snapshot to the paint loop.
    table->setUpdatesEnabled(false);
    table->setSortingEnabled(false);
    table->setRowCount(rows.size());
    int row = 0;
    for (const auto& value : rows) {
        const auto obj = value.toObject();
        for (int col = 0; col < keys.size(); ++col) {
            QString value_text = text(obj[keys[col]]);
            if (keys[col] == "instruction") {
                value_text = obj["mnemonic"].toString() + " " + obj["operands"].toString();
            }
            if (value_text.trimmed().isEmpty()) {
                value_text = "—";
            }
            auto* item = new QTableWidgetItem(value_text);
            item->setData(Qt::UserRole, obj);
            item->setToolTip(item->text());
            if (!obj["marker"].toString().isEmpty()) {
                item->setBackground(table->palette().highlight());
                item->setForeground(table->palette().highlightedText());
                item->setToolTip(obj["marker"].toString() + " · " + obj["byteSource"].toString());
            }
            table->setItem(row, col, item);
        }
        ++row;
    }
    table->setUpdatesEnabled(true);
}

QWidget* pane(const QString& heading, QWidget* content) {
    auto* widget = new QWidget;
    auto* layout = new QVBoxLayout(widget);
    layout->setContentsMargins(0, 0, 0, 0);
    auto* label = new QLabel(heading);
    label->setObjectName("paneHeading");
    layout->addWidget(label);
    layout->addWidget(content, 1);
    return widget;
}
QJsonObject selected(QTableWidget* table) {
    auto* item = table->item(table->currentRow(), 0);
    return item ? item->data(Qt::UserRole).toJsonObject() : QJsonObject{};
}
}  // namespace

DebuggerWorkspace::DebuggerWorkspace(LogClient* connection, QWidget* parent) : QWidget(parent), client(connection) {
    setObjectName("debuggerWorkspace");
    auto* layout = new QVBoxLayout(this);
    layout->setContentsMargins(14, 12, 14, 12);
    layout->setSpacing(10);
    auto* actions = new QHBoxLayout;
    title = new QLabel("WOS debugger");
    title->setSizePolicy(QSizePolicy::Ignored, QSizePolicy::Preferred);
    title->setObjectName("debuggerTitle");
    actions->addWidget(title, 1);
    for (const auto& kind : {QString("coredump"), QString("incident"), QString("log")}) {
        auto* button = new QPushButton("Open " + kind + "…");
        button->setIcon(style()->standardIcon(QStyle::SP_DialogOpenButton));
        connect(button, &QPushButton::clicked, this, [this, kind]() { choose_file(kind); });
        actions->addWidget(button);
    }
    auto* live = new QPushButton("Attach…");
    live->setObjectName("showTargets");
    connect(live, &QPushButton::clicked, this, &DebuggerWorkspace::show_targets);
    actions->addWidget(live);
    layout->addLayout(actions);
    notice = new QLabel;
    notice->setObjectName("debuggerNotice");
    notice->setTextFormat(Qt::PlainText);
    notice->setWordWrap(true);
    notice->hide();
    layout->addWidget(notice);
    auto* horizontal = new QSplitter;
    layout->addWidget(horizontal, 1);
    auto* sidebar = new QWidget;
    auto* side = new QVBoxLayout(sidebar);
    side->setContentsMargins(0, 0, 0, 0);
    auto* side_actions = new QHBoxLayout;
    side_actions->addWidget(new QLabel("EVIDENCE ON BACKEND"), 1);
    auto* refresh = new QPushButton;
    refresh->setIcon(style()->standardIcon(QStyle::SP_BrowserReload));
    refresh->setToolTip("Refresh available logs and coredumps");
    refresh->setAccessibleName("Refresh evidence");
    connect(refresh, &QPushButton::clicked, this, &DebuggerWorkspace::refresh_evidence);
    side_actions->addWidget(refresh);
    side->addLayout(side_actions);
    auto* filter = new QLineEdit;
    filter->setObjectName("evidenceFilter");
    filter->setPlaceholderText("Filter evidence…");
    filter->setClearButtonEnabled(true);
    side->addWidget(filter);
    evidence = table("evidenceFiles", {"Kind", "File"}, this);
    evidence->setColumnWidth(0, 78);
    side->addWidget(evidence, 1);
    connect(filter, &QLineEdit::textChanged, this, [this](const QString& value) {
        for (int row = 0; row < evidence->rowCount(); ++row) {
            evidence->setRowHidden(row, !evidence->item(row, 1)->text().contains(value, Qt::CaseInsensitive));
        }
    });
    auto* side_hint = new QLabel("Double-click evidence to open it.\nPaths belong to the connected backend.");
    side_hint->setWordWrap(true);
    side->addWidget(side_hint);
    auto* advanced = new QPushButton("Advanced analysis…");
    advanced->setToolTip("Specialist analyses and the complete backend capability catalog");
    connect(advanced, &QPushButton::clicked, this, [this]() { emit advanced_requested("", context()); });
    side->addWidget(advanced);
    horizontal->addWidget(sidebar);
    pages = new QStackedWidget;
    horizontal->addWidget(pages);
    horizontal->setStretchFactor(1, 1);
    horizontal->setSizes({280, 1050});

    auto* welcome = new QWidget;
    auto* welcome_layout = new QVBoxLayout(welcome);
    welcome_layout->setContentsMargins(48, 32, 48, 32);
    welcome_layout->addStretch();
    auto* welcome_title = new QLabel("Start with the evidence");
    welcome_title->setObjectName("welcomeTitle");
    welcome_layout->addWidget(welcome_title);
    auto* description = new QLabel(
        "Open a crash to follow its call stack, inspect registers, and navigate memory and code.\n"
        "Open an incident to bring related node evidence together, or attach to a running WOS target.");
    description->setWordWrap(true);
    welcome_layout->addWidget(description);
    for (const auto& entry : {qMakePair(QString("coredump"), QString("Inspect a crash →")),
                              qMakePair(QString("incident"), QString("Investigate an incident →")),
                              qMakePair(QString("log"), QString("Explore an execution log →"))}) {
        auto* button = new QPushButton(entry.second);
        button->setMinimumHeight(44);
        connect(button, &QPushButton::clicked, this, [this, kind = entry.first]() { choose_file(kind); });
        welcome_layout->addWidget(button);
    }
    auto* hint =
        new QLabel("READ-ONLY INSPECTION\nCaptured evidence is never modified. Live targets require a confirmed, expiring pause lease.");
    hint->setWordWrap(true);
    welcome_layout->addWidget(hint);
    welcome_layout->addStretch();
    pages->addWidget(welcome);

    auto* inspector = new QWidget;
    auto* inspection = new QVBoxLayout(inspector);
    inspection->setContentsMargins(0, 0, 0, 0);
    summary = new QLabel;
    summary->setObjectName("crashSummary");
    summary->setTextFormat(Qt::PlainText);
    summary->setWordWrap(true);
    summary->setTextInteractionFlags(Qt::TextSelectableByMouse);
    inspection->addWidget(summary);
    auto* navigation = new QHBoxLayout;
    back = new QPushButton;
    back->setIcon(style()->standardIcon(QStyle::SP_ArrowBack));
    back->setToolTip("Previous address (Alt+Left)");
    back->setAccessibleName("Previous address");
    back->setShortcut(QKeySequence("Alt+Left"));
    back->setEnabled(false);
    forward = new QPushButton;
    forward->setIcon(style()->standardIcon(QStyle::SP_ArrowForward));
    forward->setToolTip("Next address (Alt+Right)");
    forward->setAccessibleName("Next address");
    forward->setShortcut(QKeySequence("Alt+Right"));
    forward->setEnabled(false);
    navigation->addWidget(back);
    navigation->addWidget(forward);
    connect(back, &QPushButton::clicked, this, [this]() {
        if (history_index <= 0) {
            return;
        }
        traversing_history = true;
        seek(address_history[--history_index]);
        traversing_history = false;
    });
    connect(forward, &QPushButton::clicked, this, [this]() {
        if (history_index + 1 >= address_history.size()) {
            return;
        }
        traversing_history = true;
        seek(address_history[++history_index]);
        traversing_history = false;
    });
    frame = new QComboBox;
    frame->addItem("Fault context", "trap");
    frame->addItem("Saved context", "saved");
    frame->setToolTip("Select the captured register context");
    connect(frame, &QComboBox::activated, this, [this]() {
        ++generation;
        inspect();
    });
    navigation->addWidget(frame);
    address = new QLineEdit;
    address->setObjectName("debuggerAddress");
    address->setPlaceholderText("Go to address (0x…) or register (rip, rsp)…");
    address->setClearButtonEnabled(true);
    navigation->addWidget(address, 1);
    auto* go = new QPushButton("Go");
    connect(go, &QPushButton::clicked, this, [this]() { seek(address->text()); });
    connect(address, &QLineEdit::returnPressed, go, &QPushButton::click);
    navigation->addWidget(go);
    auto* pc = new QPushButton("Program counter");
    connect(pc, &QPushButton::clicked, this, [this]() { seek("rip"); });
    navigation->addWidget(pc);
    read_memory = new QPushButton("Read live memory…");
    connect(read_memory, &QPushButton::clicked, this, &DebuggerWorkspace::read_live_memory);
    read_memory->hide();
    navigation->addWidget(read_memory);
    inspection->addLayout(navigation);
    auto* columns = new QSplitter;
    auto* machine = new QSplitter(Qt::Vertical);
    stack = table("debuggerStack", {"#", "Address", "Function", "Confidence"}, this);
    stack->setColumnWidth(0, 36);
    stack->setColumnWidth(1, 164);
    registers = table("debuggerRegisters", {"Register", "Value", "Symbol"}, this);
    registers->setColumnWidth(0, 76);
    registers->setColumnWidth(1, 164);
    machine->addWidget(pane("CALL STACK · double-click to follow", stack));
    machine->addWidget(pane("REGISTERS · double-click to follow", registers));
    columns->addWidget(machine);
    auto* listings = new QSplitter(Qt::Vertical);
    code_tabs = new QTabWidget;
    code = table("debuggerCode", {"Address", "Bytes", "Instruction", "Symbol"}, this);
    code->setColumnWidth(0, 164);
    code->setColumnWidth(1, 150);
    code->setColumnWidth(2, 240);
    code_tabs->addTab(code, "Disassembly");
    source = new QPlainTextEdit;
    source->setReadOnly(true);
    source->setFont(QFontDatabase::systemFont(QFontDatabase::FixedFont));
    source->setPlaceholderText("Select a stack frame with source information to inspect its source.");
    code_tabs->addTab(source, "Source");
    listings->addWidget(code_tabs);
    auto* bottom = new QTabWidget;
    memory = table("debuggerMemory", {"Address", "Value / bytes", "Symbol", "Notes"}, this);
    memory->setColumnWidth(0, 164);
    memory->setColumnWidth(1, 210);
    bottom->addTab(memory, "Memory / stack");
    modules = table("debuggerModules", {"Module", "Base", "Build ID", "Symbols"}, this);
    bottom->addTab(modules, "Modules");
    listings->addWidget(bottom);
    listings->setSizes({440, 240});
    columns->addWidget(listings);
    columns->setSizes({420, 660});
    inspection->addWidget(columns, 1);
    pages->addWidget(inspector);

    auto* target_page = new QWidget;
    auto* target_layout = new QVBoxLayout(target_page);
    target_layout->setContentsMargins(24, 16, 24, 16);
    auto* target_title = new QLabel("Attach to a WOS target");
    target_title->setObjectName("welcomeTitle");
    target_layout->addWidget(target_title);
    target_hint = new QLabel("Discover configured targets. Discovery does not connect to or pause a VM.");
    target_hint->setTextFormat(Qt::PlainText);
    target_hint->setWordWrap(true);
    target_layout->addWidget(target_hint);
    auto* target_actions = new QHBoxLayout;
    auto* profile = new QComboBox;
    profile->setObjectName("liveProfile");
    profile->addItem("Configured targets", "configured");
    profile->addItem("Running WOS cluster", "cluster");
    profile->addItem("Isolated KTEST VM", "ktest");
    profile->setToolTip("Select a configured target set or a standard WOS runtime descriptor. No endpoint scanning.");
    target_actions->addWidget(profile);
    enable = new QPushButton("Enable live debugging…");
    enable->setObjectName("enableLiveDebugging");
    enable->hide();
    connect(enable, &QPushButton::clicked, this, [this, profile]() {
        if (QMessageBox::question(this, "Enable live debugging?",
                                  "Enable " + profile->currentText() +
                                      " for this backend process?\n\n"
                                      "No VM will be connected or paused yet. Endpoint allowlists and memory limits stay in force. "
                                      "This choice is not saved to the configuration file.",
                                  QMessageBox::Yes | QMessageBox::Cancel, QMessageBox::Cancel) != QMessageBox::Yes) {
            return;
        }
        enable->setEnabled(false);
        send("enable_live_debugging", {{"confirmation", "ENABLE_CONFIGURED_LIVE_TARGETS"}, {"profile", profile->currentData().toString()}},
             [this](const QJsonObject& result) {
                 enable->setEnabled(true);
                 if (accept(result, "Enable live debugging")) {
                     discover();
                 }
             });
    });
    target_actions->addWidget(enable);
    auto* rediscover = new QPushButton("Refresh targets");
    connect(rediscover, &QPushButton::clicked, this, &DebuggerWorkspace::discover);
    target_actions->addWidget(rediscover);
    target_actions->addStretch();
    attach = new QPushButton("Attach and pause…");
    attach->setObjectName("attachTarget");
    attach->setEnabled(false);
    connect(attach, &QPushButton::clicked, this, &DebuggerWorkspace::attach_target);
    target_actions->addWidget(attach);
    target_layout->addLayout(target_actions);
    targets = table("liveTargets", {"Target", "Node", "Transport", "Host", "Port", "Symbols"}, this);
    target_layout->addWidget(targets, 1);
    connect(targets, &QTableWidget::itemSelectionChanged, this,
            [this]() { attach->setEnabled(!selected(targets).isEmpty() && session_id.isEmpty() && !attach_pending); });
    auto* capability_hint =
        new QLabel("WOSDBG supports read-only inspection under a bounded pause lease. Stepping and breakpoints are not supported.");
    capability_hint->setWordWrap(true);
    target_layout->addWidget(capability_hint);
    pages->addWidget(target_page);

    auto* incident_page = new QWidget;
    auto* incident_layout = new QVBoxLayout(incident_page);
    inventory = table("incidentInventory", {"Evidence", "Kind", "Symbols", "Build ID"}, this);
    timeline = table("incidentTimeline", {"Time", "Node", "Event", "Message"}, this);
    auto* incident_split = new QSplitter(Qt::Vertical);
    incident_split->addWidget(pane("INCIDENT EVIDENCE · double-click a captured coredump to inspect", inventory));
    incident_split->addWidget(pane("TIMELINE · retain node-local order when clocks cannot be compared", timeline));
    incident_layout->addWidget(incident_split);
    pages->addWidget(incident_page);
    auto* lease_actions = new QHBoxLayout;
    lease_label = new QLabel("Offline evidence · read-only");
    lease_label->setTextFormat(Qt::PlainText);
    lease_actions->addWidget(lease_label, 1);
    renew = new QPushButton("Extend pause");
    detach = new QPushButton("Detach / release pause");
    renew->hide();
    detach->hide();
    lease_actions->addWidget(renew);
    lease_actions->addWidget(detach);
    layout->addLayout(lease_actions);
    connect(renew, &QPushButton::clicked, this, [this]() {
        send("renew_live_session", context(), [this](const QJsonObject& result) {
            if (accept(result, "Extend pause")) {
                update_lease(result);
            }
        });
    });
    connect(detach, &QPushButton::clicked, this, [this]() {
        send("close_live_session", context(), [this](const QJsonObject& result) {
            if (accept(result, "Detach")) {
                clear_live();
                show_targets();
            }
        });
    });
    auto* timer = new QTimer(this);
    timer->setInterval(250);
    connect(timer, &QTimer::timeout, this, [this]() {
        if (session_id.isEmpty()) {
            return;
        }
        const auto remaining = lease_ms - lease_clock.elapsed();
        lease_label->setText(
            QString("LIVE · %1 · pause lease %2 s remaining").arg(target_id).arg(std::max<qint64>(0, (remaining + 999) / 1000)));
        if (remaining <= 0) {
            clear_live();
            notice->setText("The pause lease expired. Reattach to read fresh state; captured values are no longer live.");
            notice->show();
            pages->setCurrentIndex(2);
        }
    });
    timer->start();
    connect(evidence, &QTableWidget::cellDoubleClicked, this, [this]() {
        const auto obj = selected(evidence);
        open_path(obj["kind"].toString(), obj["path"].toString());
    });
    connect(inventory, &QTableWidget::cellDoubleClicked, this, [this]() {
        const auto obj = selected(inventory);
        if (!obj["dumpId"].toString().isEmpty()) {
            show_dump(obj["dumpId"].toString(), {});
        } else {
            notice->setText("This member is available through Advanced analysis → Incident inventory and log tools.");
            notice->show();
        }
    });
    connect(registers, &QTableWidget::cellDoubleClicked, this, [this]() { seek(selected(registers)["value"].toString()); });
    registers->addAction("Follow value in code and memory", this, [this]() { seek(selected(registers)["value"].toString()); });
    stack->addAction("Follow frame in code and memory", this, [this]() {
        const auto row = selected(stack);
        seek(row["rip"].toString(row["address"].toString()));
    });
    memory->addAction("Follow pointer", this, [this]() { seek(selected(memory)["value"].toString()); });
    connect(stack, &QTableWidget::cellDoubleClicked, this, [this]() {
        const auto row = selected(stack);
        seek(row["rip"].toString(row["address"].toString()));
        const QString path = row["sourcePath"].toString();
        if (!path.isEmpty()) {
            const int epoch = generation;
            const int navigation_epoch = seek_generation;
            send("get_source_context", {{"path", path}, {"line", row["sourceLine"]}},
                 [this, epoch, navigation_epoch, path](const QJsonObject& result) {
                     if (epoch != generation || navigation_epoch != seek_generation || !accept(result, "Read source")) {
                         return;
                     }
                     QStringList lines{path, ""};
                     for (const auto& line : result["lines"].toArray()) {
                         const auto obj = line.toObject();
                         lines << QString("%1  %2").arg(text(obj["line"]), obj["text"].toString());
                     }
                     source->setPlainText(lines.join('\n'));
                     code_tabs->setCurrentWidget(source);
                 });
        }
    });
    connect(client, &LogClient::tool_result_received, this, [this](quint64 id, const QJsonObject& result) {
        auto it = requests.find(id);
        if (it == requests.end()) {
            return;
        }
        auto reply = std::move(it.value());
        requests.erase(it);
        reply(result);
    });
    connect(client, &LogClient::connected, this, [this]() {
        notice->hide();
        refresh_evidence();
    });
    connect(client, &LogClient::disconnected, this, [this]() {
        requests.clear();
        ++generation;
        attach_pending = false;
        clear_live();
        dump_id.clear();
        incident_id.clear();
        notice->setText("Backend disconnected. Live leases are released by the backend; reconnect before requesting new data.");
        notice->show();
        attach->setEnabled(false);
    });
    if (client->is_connected()) {
        QTimer::singleShot(0, this, &DebuggerWorkspace::refresh_evidence);
    }
}

void DebuggerWorkspace::send(const QString& tool, const QJsonObject& args, Reply reply) {
    if (!client->is_connected()) {
        reply({{"ok", false}, {"error", "The backend is disconnected. Connect before continuing."}});
        return;
    }
    requests.insert(client->call_tool("wosdbg." + tool, args), std::move(reply));
}

bool DebuggerWorkspace::accept(const QJsonObject& result, const QString& action) {
    if (result["ok"].toBool(true)) {
        return true;
    }
    notice->setText(action + ": " + result["error"].toString("The backend could not complete this request."));
    notice->show();
    return false;
}

QJsonObject DebuggerWorkspace::context() const {
    if (!session_id.isEmpty()) {
        return {{"sessionId", session_id}, {"leaseToken", lease_token}, {"targetId", target_id}};
    }
    if (!dump_id.isEmpty()) {
        return {{"dumpId", dump_id}, {"frame", frame->currentData().toString()}};
    }
    if (!incident_id.isEmpty()) {
        return {{"incidentId", incident_id}};
    }
    return {};
}

void DebuggerWorkspace::refresh_evidence() {
    send("list_coredumps", {}, [this](const QJsonObject& dumps) {
        if (!accept(dumps, "List coredumps")) {
            return;
        }
        send("list_logs", {}, [this, dumps](const QJsonObject& logs) {
            if (!accept(logs, "List logs")) {
                return;
            }
            QJsonArray rows;
            for (const auto& kind : {QString("coredump"), QString("log")}) {
                const auto items = (kind == "coredump" ? dumps["coredumps"] : logs["logs"]).toArray();
                for (const auto& item : items) {
                    auto obj = item.toObject();
                    obj["kind"] = kind;
                    rows.append(obj);
                }
            }
            fill(evidence, rows, {"kind", "name"});
            if (rows.isEmpty()) {
                notice->setText("No evidence found in the configured directories. Use Open to choose a backend file.");
                notice->show();
            }
        });
    });
}

void DebuggerWorkspace::choose_file(const QString& kind) {
    QDialog dialog(this);
    dialog.setWindowTitle("Open " + kind);
    dialog.resize(620, 160);
    auto* layout = new QVBoxLayout(&dialog);
    auto* hint = new QLabel("Choose from the evidence sidebar, or enter a path on the connected WOSDBG backend.");
    hint->setWordWrap(true);
    layout->addWidget(hint);
    auto* row = new QHBoxLayout;
    auto* path = new QLineEdit;
    path->setPlaceholderText("Backend file path");
    path->setAccessibleName("Backend file path");
    row->addWidget(path, 1);
    auto* browse = new QPushButton("Browse local files…");
    row->addWidget(browse);
    layout->addLayout(row);
    connect(browse, &QPushButton::clicked, &dialog, [&]() {
        const auto selected = QFileDialog::getOpenFileName(&dialog, "Choose " + kind);
        if (!selected.isEmpty()) {
            path->setText(selected);
        }
    });
    if (kind == "incident") {
        auto* folder = new QPushButton("Choose folder…");
        row->addWidget(folder);
        connect(folder, &QPushButton::clicked, &dialog, [&dialog, path]() {
            const auto selected = QFileDialog::getExistingDirectory(&dialog, "Choose incident bundle");
            if (!selected.isEmpty()) {
                path->setText(selected);
            }
        });
    }
    auto* buttons = new QDialogButtonBox(QDialogButtonBox::Open | QDialogButtonBox::Cancel);
    buttons->button(QDialogButtonBox::Open)->setEnabled(false);
    connect(path, &QLineEdit::textChanged, &dialog,
            [buttons](const QString& value) { buttons->button(QDialogButtonBox::Open)->setEnabled(!value.trimmed().isEmpty()); });
    connect(buttons, &QDialogButtonBox::accepted, &dialog, &QDialog::accept);
    connect(buttons, &QDialogButtonBox::rejected, &dialog, &QDialog::reject);
    layout->addWidget(buttons);
    if (dialog.exec() == QDialog::Accepted) {
        open_path(kind, path->text().trimmed());
    }
}

void DebuggerWorkspace::open_path(const QString& kind, const QString& path) {
    if (path.isEmpty()) {
        return;
    }
    if (!session_id.isEmpty() || attach_pending) {
        notice->setText("Detach from the live target before opening another inspection session.");
        notice->show();
        return;
    }
    if (kind == "log") {
        emit log_requested(path);
        return;
    }
    const int epoch = ++generation;
    notice->setText("Opening " + QFileInfo(path).fileName() + "…");
    notice->show();
    send(kind == "incident" ? "load_incident" : "open_coredump", {{"path", path}}, [this, epoch, kind, path](const QJsonObject& result) {
        if (epoch != generation || !accept(result, "Open " + kind)) {
            return;
        }
        notice->hide();
        title->setText(QFileInfo(path).fileName());
        if (kind == "coredump") {
            show_dump(result["dumpId"].toString(), result["summary"].toObject());
            return;
        }
        dump_id.clear();
        incident_id = result["incidentId"].toString();
        pages->setCurrentIndex(3);
        send("get_incident_inventory", {{"incidentId", incident_id}}, [this, epoch](const QJsonObject& inventory_result) {
            if (epoch != generation || !accept(inventory_result, "Read incident evidence")) {
                return;
            }
            fill(inventory, inventory_result["evidence"].toArray(), {"memberPath", "kind", "symbolStatus", "buildIdStatus"});
            const auto page = inventory_result["inventory"].toObject();
            const int total = text(page["total"]).toInt();
            if (total > inventory->rowCount()) {
                notice->setText(
                    QString(
                        "Showing %1 of %2 incident members. Advanced analysis → Incident inventory provides paged access to all members.")
                        .arg(inventory->rowCount())
                        .arg(total));
                notice->show();
            }
        });
        send("summarize_incident", {{"incidentId", incident_id}}, [this, epoch](const QJsonObject& summary_result) {
            if (epoch != generation || !accept(summary_result, "Read incident timeline")) {
                return;
            }
            const auto events = summary_result["timeline"].toObject();
            QHash<QString, QString> nodes;
            for (const auto& value : events["lanes"].toArray()) {
                const auto lane = value.toObject();
                nodes[lane["logId"].toString()] = text(lane["nodeId"]);
            }
            QJsonArray rows;
            for (const auto& value : events["events"].toArray()) {
                auto row = value.toObject();
                const auto entry = row["entry"].toObject();
                row["time"] = row["timestampNs"].toString("Not timestamped");
                row["node"] = nodes.value(row["logId"].toString(), row["logPath"].toString());
                row["event"] = entry["type"];
                row["message"] = entry["text"].toString(entry["assembly"].toString());
                rows.append(row);
            }
            fill(timeline, rows, {"time", "node", "event", "message"});
            lease_label->setText("Incident · clock quality: " + events["clockQuality"].toString("not available"));
            if (summary_result["degraded"].toBool() || summary_result["responseTruncated"].toBool()) {
                notice->setText(
                    "This incident is incomplete or the displayed analysis is bounded. Open Advanced analysis → Incident summary to "
                    "inspect all returned warnings and limits.");
                notice->show();
            }
        });
    });
}

void DebuggerWorkspace::show_dump(const QString& id, const QJsonObject& info) {
    ++generation;
    dump_id = id;
    pages->setCurrentIndex(1);
    frame->setEnabled(true);
    read_memory->hide();
    address_history.clear();
    history_index = -1;
    source->clear();
    const int epoch = generation;
    auto show_summary = [this](const QJsonObject& value) {
        summary->setText(QString("%1  ·  PID %2  ·  CPU %3\nInstruction %4    Fault address %5\nSymbols: %6")
                             .arg(value["interruptName"].toString(), text(value["pid"]), text(value["cpu"]), value["trapRip"].toString(),
                                  value["cr2"].toString(), value["symbols"].toObject()["symbolStatus"].toString("unavailable")));
        fill(modules, value["modules"].toArray(), {"name", "base", "buildId", "identityStatus"});
    };
    if (!info.isEmpty()) {
        show_summary(info);
    } else {
        send("get_crash_summary", context(), [this, epoch, show_summary](const QJsonObject& result) {
            if (epoch == generation && accept(result, "Inspect crash")) {
                show_summary(result["summary"].toObject());
            }
        });
    }
    inspect();
}

void DebuggerWorkspace::inspect() {
    const bool live = !session_id.isEmpty();
    if (!live && dump_id.isEmpty()) {
        return;
    }
    const int epoch = generation;
    source->clear();
    send(live ? "read_live_registers" : "describe_registers", context(), [this, epoch](const QJsonObject& result) {
        if (epoch == generation && accept(result, "Read registers")) {
            fill(registers, result["registers"].toArray(), {"name", "value", "address"});
        }
    });
    send(live ? "backtrace_live" : "backtrace_coredump", context(), [this, epoch, live](const QJsonObject& result) {
        if (epoch == generation && accept(result, "Read call stack")) {
            fill(stack, result["frames"].toArray(), {"index", live ? "address" : "rip", "symbol", "confidence"});
        }
    });
    if (!live) {
        seek("rip");
    } else {
        code->setRowCount(0);
        memory->setRowCount(0);
    }
}

void DebuggerWorkspace::seek(const QString& location) {
    if (location.trimmed().isEmpty()) {
        return;
    }
    address->setText(location);
    if (!traversing_history && (history_index < 0 || address_history[history_index] != location)) {
        address_history = address_history.mid(0, history_index + 1);
        address_history.append(location);
        if (address_history.size() > 128) {
            address_history.removeFirst();
        }
        history_index = address_history.size() - 1;
    }
    back->setEnabled(history_index > 0);
    forward->setEnabled(history_index + 1 < address_history.size());
    if (!session_id.isEmpty()) {
        notice->setText(
            "Address selected. Use Read live memory to authorize a bounded read. Live disassembly is not provided by this backend.");
        notice->show();
        return;
    }
    if (dump_id.isEmpty()) {
        return;
    }
    auto args = context();
    bool numeric = false;
    location.toULongLong(&numeric, 0);
    args[numeric ? "address" : "register"] = location;
    const int epoch = generation;
    const int seek_epoch = ++seek_generation;
    send("disassemble_coredump", args, [this, epoch, seek_epoch](const QJsonObject& result) {
        if (epoch != generation || seek_epoch != seek_generation || !accept(result, "Read code")) {
            return;
        }
        fill(code, result["instructions"].toArray(), {"address", "bytes", "instruction", "symbol"});
    });
    send("get_memory_context", args, [this, epoch, seek_epoch](const QJsonObject& result) {
        if (epoch != generation || seek_epoch != seek_generation || !accept(result, "Read memory")) {
            return;
        }
        fill(memory, result["rows"].toArray(), {"address", "value", "symbol", "notes"});
    });
}

void DebuggerWorkspace::show_targets() {
    pages->setCurrentIndex(2);
    discover();
}

void DebuggerWorkspace::discover() {
    attach->setEnabled(false);
    send("discover_live_targets", {}, [this](const QJsonObject& result) {
        if (result["errorCode"].toString() == "disabled") {
            notice->hide();
            enable->show();
            targets->setRowCount(0);
            findChild<QComboBox*>("liveProfile")->setEnabled(true);
            target_hint->setText(
                "Live debugging is off. Enable access to configured targets, then select one to attach. Nothing is paused during "
                "discovery.");
            return;
        }
        if (!accept(result, "Discover targets")) {
            return;
        }
        enable->hide();
        findChild<QComboBox*>("liveProfile")->setEnabled(false);
        findChild<QComboBox*>("liveProfile")
            ->setToolTip("Live configuration is active for this backend process. Restart the backend to choose another preset.");
        const auto rows = result["targets"].toArray();
        fill(targets, rows, {"id", "nodeId", "transport", "host", "port", "symbolStatus"});
        target_hint->setText(
            rows.isEmpty()
                ? "No live targets are registered in this configuration. Start the corresponding WOS debug VM, then Refresh targets. To "
                  "choose the standard cluster or KTEST preset, restart the backend and select that profile before enabling."
                : "Select a target, then Attach and pause. The confirmation explains what will pause and how it is released.");
        if (!rows.isEmpty()) {
            targets->selectRow(0);
        }
    });
}

void DebuggerWorkspace::attach_target() {
    if (attach_pending || !session_id.isEmpty()) {
        return;
    }
    const auto target = selected(targets);
    if (target.isEmpty()) {
        return;
    }
    bool confirmed = false;
    const QString reason =
        QInputDialog::getText(this, "Attach and pause " + target["id"].toString(),
                              "This will acquire a read-only pause lease on the selected target.\n"
                              "WOSDBG releases its own pause on detach, failure, disconnect, or expiry.\n"
                              "Already-paused targets remain paused. No stepping or writes are enabled.\n\nReason for this inspection:",
                              QLineEdit::Normal, "", &confirmed)
            .trimmed();
    if (!confirmed || reason.isEmpty()) {
        return;
    }
    if (reason.toUtf8().size() > 160) {
        notice->setText("Keep the inspection reason under 160 UTF-8 bytes.");
        notice->show();
        return;
    }
    attach_pending = true;
    attach->setEnabled(false);
    ++generation;
    send("open_live_session",
         {{"targetId", target["id"]}, {"authority", "pause-read"}, {"confirmation", "PAUSE_ALLOWLISTED_TARGETS"}, {"auditId", reason}},
         [this, target](const QJsonObject& result) {
             attach_pending = false;
             if (!accept(result, "Attach")) {
                 attach->setEnabled(!selected(targets).isEmpty());
                 return;
             }
             session_id = result["sessionId"].toString();
             lease_token = result["leaseToken"].toString();
             target_id = target["id"].toString();
             dump_id.clear();
             incident_id.clear();
             title->setText("Live · " + target_id);
             address_history.clear();
             history_index = -1;
             back->setEnabled(false);
             forward->setEnabled(false);
             frame->setEnabled(false);
             read_memory->show();
             pages->setCurrentIndex(1);
             notice->hide();
             summary->setText("Paused inspection · " + target_id +
                              "\nRead-only registers and call stack. Live memory requires a separate confirmed read.");
             modules->setRowCount(0);
             update_lease(result);
             inspect();
         });
}

void DebuggerWorkspace::update_lease(const QJsonObject& result) {
    lease_ms = result["leaseMsRemaining"].toInt();
    lease_clock.restart();
    renew->show();
    detach->show();
}

void DebuggerWorkspace::clear_live() {
    if (!session_id.isEmpty()) {
        ++generation;
    }
    session_id.clear();
    lease_token.clear();
    target_id.clear();
    renew->hide();
    detach->hide();
    read_memory->hide();
    lease_label->setText("Offline evidence · read-only");
}

void DebuggerWorkspace::focus_view(const QString& name) {
    if (dump_id.isEmpty() && session_id.isEmpty()) {
        notice->setText("Open a coredump or attach to a target to inspect its machine state.");
        notice->show();
        return;
    }
    pages->setCurrentIndex(1);
    if (name == "backtrace_coredump") {
        stack->setFocus();
    } else if (name == "get_memory_context") {
        address->setFocus();
        address->selectAll();
    } else {
        summary->setFocus();
    }
}

void DebuggerWorkspace::read_live_memory() {
    if (session_id.isEmpty()) {
        return;
    }
    bool valid = false;
    address->text().toULongLong(&valid, 0);
    if (!valid) {
        notice->setText("Enter an exact memory address, or double-click a register value first.");
        notice->show();
        return;
    }
    if (QMessageBox::question(this, "Read live memory?",
                              "Read up to 64 bytes at " + address->text() +
                                  "?\nLive memory may contain secrets. The backend's configured byte limit still applies.",
                              QMessageBox::Yes | QMessageBox::Cancel, QMessageBox::Cancel) != QMessageBox::Yes) {
        return;
    }
    auto args = context();
    args["address"] = address->text();
    args["length"] = 64;
    args["confirmation"] = "READ_SENSITIVE_LIVE_MEMORY";
    const int epoch = generation;
    send("read_live_memory", args, [this, epoch](const QJsonObject& result) {
        if (epoch != generation || !accept(result, "Read live memory")) {
            return;
        }
        const auto bytes = QByteArray::fromHex(result["bytesHex"].toString().toLatin1());
        const auto base = result["address"].toString().toULongLong(nullptr, 0);
        QJsonArray rows;
        for (int offset = 0; offset < bytes.size(); offset += 16) {
            rows.append(QJsonObject{{"address", "0x" + QString::number(base + offset, 16)},
                                    {"value", QString::fromLatin1(bytes.mid(offset, 16).toHex(' '))},
                                    {"notes", "Live snapshot"}});
        }
        fill(memory, rows, {"address", "value", "symbol", "notes"});
    });
}
