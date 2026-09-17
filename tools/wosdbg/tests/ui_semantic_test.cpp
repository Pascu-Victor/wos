#include <QApplication>
#include <QCheckBox>
#include <QComboBox>
#include <QDataStream>
#include <QElapsedTimer>
#include <QJsonArray>
#include <QJsonDocument>
#include <QLabel>
#include <QLineEdit>
#include <QProcess>
#include <QPushButton>
#include <QTableWidget>
#include <QTcpServer>
#include <QTcpSocket>
#include <QTreeWidget>
#include <functional>

#include "debug_tool_panel.h"
#include "log_client.h"
#include "tool_arguments_form.h"
#include "tool_presentation.h"
#include "tool_result_view.h"

namespace {
void require(bool value, const char* message) {
    if (!value) {
        qFatal("%s", message);
    }
}

void until(const std::function<bool()>& condition) {
    QElapsedTimer elapsed;
    elapsed.start();
    while (!condition() && elapsed.elapsed() < 3000) {
        QApplication::processEvents(QEventLoop::AllEvents, 10);
    }
    require(condition(), "Timed out waiting for GUI event");
}

template <class T>
T* widget(QObject& parent, const char* name) {
    auto* result = parent.findChild<T*>(name);
    require(result != nullptr, name);
    return result;
}
}  // namespace

int main(int argc, char** argv) {
    QApplication app(argc, argv);
    require(argc >= 2, "Pass the WOSDBG binary whose catalog should be exercised");
    QProcess catalog_process;
    catalog_process.start(QString::fromLocal8Bit(argv[1]), {"--list-tools"});
    require(catalog_process.waitForFinished(30000) && catalog_process.exitCode() == 0, "Cannot read built catalog");
    const QJsonArray catalog = QJsonDocument::fromJson(catalog_process.readAllStandardOutput()).object()["tools"].toArray();
    require(catalog.size() >= 40, "Incomplete catalog");
    catalog_process.start(QString::fromLocal8Bit(argv[1]), {"--tool-help", "read_live_memory"});
    require(catalog_process.waitForFinished(30000) && catalog_process.exitCode() == 0, "Targeted tool help failed");
    const QJsonObject help = QJsonDocument::fromJson(catalog_process.readAllStandardOutput()).object();
    require(help["name"].toString() == "wosdbg.read_live_memory" && help.contains("inputSchema"), "Tool help lacks the exact schema");
    catalog_process.start(QString::fromLocal8Bit(argv[1]), {"--list-tools", "--brief"});
    require(catalog_process.waitForFinished(30000) && catalog_process.exitCode() == 0, "Brief discovery failed");
    const QJsonArray brief = QJsonDocument::fromJson(catalog_process.readAllStandardOutput()).object()["tools"].toArray();
    require(brief.size() == catalog.size() && !brief.first().toObject().contains("inputSchema"),
            "Brief discovery omitted tools or retained full schemas");
    QMap<QString, QJsonObject> tools;
    ToolArgumentsForm form;
    for (const auto& value : catalog) {
        const auto tool = value.toObject();
        require(!tool["title"].toString().isEmpty(), "Tool lacks a readable title");
        require(!tool["_meta"].toObject()["org.wosdbg/category"].toString().isEmpty(), "Tool lacks a category");
        tools[tool["name"].toString()] = tool;
        form.set_schema(tool["inputSchema"].toObject());
        QString error;
        const auto arguments = form.arguments(&error);
        const auto required = tool["inputSchema"].toObject()["required"].toArray();
        require(!required.isEmpty() || (arguments.isEmpty() && error.isEmpty()), "Optional inputs changed backend defaults");
    }
    form.set_schema(tools["wosdbg.find_duplicate_pages"]["inputSchema"].toObject(), {{"dumpId", "dump-a"}});
    QString error;
    require(form.arguments(&error) == QJsonObject{{"dumpId", "dump-a"}} && error.isEmpty(), "Boolean defaults were submitted");
    widget<QCheckBox>(form, "include_executableOnly")->setChecked(true);
    require(form.arguments(&error)["executableOnly"].isBool() && !form.arguments()["executableOnly"].toBool(),
            "Cannot explicitly send false");
    widget<QCheckBox>(form, "include_executableOnly")->setChecked(false);
    require(!form.arguments().contains("executableOnly"), "Cannot restore omission");

    form.set_schema(tools["wosdbg.read_live_memory"]["inputSchema"].toObject(), {{"sessionId", "live-a"},
                                                                                 {"leaseToken", "secret-a"},
                                                                                 {"targetId", "node-0"},
                                                                                 {"address", "0xffffffffffffffff"},
                                                                                 {"length", 64},
                                                                                 {"confirmation", "READ_SENSITIVE_LIVE_MEMORY"}});
    require(form.arguments(&error).isEmpty() && !error.isEmpty(), "Confirmation was preselected");
    widget<QCheckBox>(form, "confirm_confirmation")->setChecked(true);
    const auto memory = form.arguments(&error);
    require(error.isEmpty() && memory["address"].toString() == "0xffffffffffffffff", "Address lost precision");
    require(memory["length"].isDouble() && memory["length"].toInt() == 64, "Numeric input is not numeric");
    require(widget<QLineEdit>(form, "input_leaseToken")->echoMode() == QLineEdit::Password, "Lease token should be concealed in form");
    widget<QLineEdit>(form, "input_length")->setText("999999999999999999999");
    require(form.arguments(&error).isEmpty() && !error.isEmpty(), "Out-of-range integer was silently narrowed");

    const QJsonArray ranges{QJsonObject{{"targetId", "node-0"}, {"address", "0xffffffff80000000"}, {"length", 4096}}};
    form.set_schema(tools["wosdbg.capture_live_incident"]["inputSchema"].toObject(),
                    {{"sessionId", "live-a"},
                     {"leaseToken", "secret-a"},
                     {"output", "/tmp/new.wosincident"},
                     {"memoryRanges", ranges},
                     {"coredumpPaths", QJsonArray{"/tmp/core with spaces"}}});
    widget<QCheckBox>(form, "confirm_confirmation")->setChecked(true);
    require(form.arguments(&error)["memoryRanges"].toArray() == ranges && error.isEmpty(),
            "Nested range controls changed request semantics");
    require(form.arguments()["coredumpPaths"].toArray().first().toString() == "/tmp/core with spaces", "Path list lost spaces");

    ToolResultView result;
    QJsonArray rows;
    for (int i = 0; i < 600; ++i) {
        rows.append(QJsonObject{{"dumpId", "dump-" + QString::number(i)}, {"row", i}, {"address", "0xffffffffffffffff"}});
    }
    result.set_result({{"ok", true}, {"entries", rows}, {"degraded", true}});
    auto* table = widget<QTableWidget>(result, "result_table");
    require(table->rowCount() == 500 && result.result()["entries"].toArray().size() == 600, "Preview bound lost complete result");
    QJsonObject selected;
    QObject::connect(&result, &ToolResultView::context_selected, &result, [&selected](const QJsonObject& value) { selected = value; });
    table->sortItems(2, Qt::DescendingOrder);
    table->selectRow(0);
    require(selected["row"].toInt() == 499, "Sorted row selection changed evidence identity");
    result.findChild<QLineEdit*>()->setText("dump-499");
    require(!table->isRowHidden(0) && table->isRowHidden(1), "Evidence filtering failed");
    widget<QPushButton>(result, "next_result_page")->click();
    require(table->rowCount() == 100, "Cannot inspect returned rows beyond the preview bound");
    result.findChild<QLineEdit*>()->clear();
    table->selectRow(0);
    require(selected["row"].toInt() == 599, "Paging changed sorted row identity");
    table->itemDoubleClicked(table->item(0, 0));
    require(widget<QTreeWidget>(result, "result_tree")->topLevelItemCount() == 4, "Cannot inspect all fields of a paged row");
    const QString summary = wosdbg::result_summary({{"ok", false},
                                                    {"errorCode", "invalid_argument"},
                                                    {"error", "Wrong session"},
                                                    {"entries", rows},
                                                    {"degraded", true},
                                                    {"leaseToken", "must-not-appear"}});
    require(summary.size() < 2500 && summary.contains("Wrong session") && summary.contains("degraded: true") &&
                !summary.contains("must-not-appear"),
            "Agent summary hid errors or leaked the token");

    // Exercise actual asynchronous GUI request serialization against a fixture server.
    QTcpServer server;
    require(server.listen(QHostAddress::LocalHost, 0), "Cannot start local GUI fixture server");
    LogClient client;
    DebugToolPanel panel(&client);
    panel.resize(1200, 900);
    panel.show();
    client.connect_to_host("127.0.0.1", server.serverPort());
    until([&]() { return client.is_connected() && server.hasPendingConnections(); });
    auto* peer = server.nextPendingConnection();
    client.tool_catalog_received(catalog);
    panel.select_tool("load_log", {{"path", "serial-vm0.log"}});
    auto* run = widget<QPushButton>(panel, "run_analysis");
    require(run->isEnabled(), "Valid form cannot run");
    run->click();
    panel.select_tool("list_logs");
    require(!run->isEnabled(), "Switching tools allowed overlapping requests");
    QDataStream in(peer);
    in.setVersion(QDataStream::Qt_6_0);
    quint64 request = 0;
    until([&]() {
        while (peer->bytesAvailable() > 0) {
            in.startTransaction();
            quint32 size;
            quint8 type;
            in >> size;
            if (in.status() != QDataStream::Ok || peer->bytesAvailable() < size) {
                in.rollbackTransaction();
                break;
            }
            in >> type;
            if (static_cast<MessageType>(type) == MessageType::TOOL_CALL_REQUEST) {
                QString name;
                QByteArray arguments;
                in >> request >> name >> arguments;
                require(name == "wosdbg.load_log" && QJsonDocument::fromJson(arguments).object() == QJsonObject{{"path", "serial-vm0.log"}},
                        "GUI sent wrong tool arguments");
            } else {
                in.skipRawData(static_cast<int>(size) - 1);
            }
            require(in.commitTransaction(), "Malformed GUI fixture frame");
        }
        return request != 0;
    });
    require(request != 0, "No GUI tool request observed");
    client.tool_result_received(request + 99, {{"ok", true}, {"logId", "unrelated"}});
    require(!run->isEnabled(), "Unrelated response completed pending request");
    client.tool_result_received(request, {{"ok", true}, {"logId", "log-a"}, {"entries", rows}});
    require(run->isEnabled(), "Response left Run disabled");
    panel.select_tool("search_log", {{"query", "panic"}});
    require(widget<QLineEdit>(panel, "input_logId")->text() == "log-a", "Follow-up lost log session");
    auto* search = widget<QLineEdit>(panel, "analysis_search");
    search->setText("search_log");
    require(widget<QLineEdit>(panel, "input_query")->text() == "panic", "Filtering the current action erased input");
    search->clear();
    QApplication::processEvents();
    require(widget<QLineEdit>(panel, "input_query")->isVisible(), "Required form fields are not visible");
    if (argc >= 3) {
        require(panel.grab().save(QString::fromLocal8Bit(argv[2])), "Cannot save GUI preview");
    }
    for (const QString& session : {"a", "b"}) {
        panel.select_tool("open_live_session", {{"auditId", "UI fixture"}, {"targetId", "node-0"}});
        require(!run->isEnabled(), "Pause operation did not require confirmation");
        widget<QCheckBox>(panel, "confirm_confirmation")->setChecked(true);
        require(run->isEnabled(), "Confirmed pause form did not become runnable");
        run->click();
        client.tool_result_received(++request, {{"ok", true}, {"sessionId", "live-" + session}, {"leaseToken", "token-" + session}});
    }
    auto* contexts = widget<QComboBox>(panel, "analysis_context");
    const int first_session = contexts->findText("Session ID: live-a");
    require(first_session >= 0, "First live session was not retained");
    contexts->setCurrentIndex(first_session);
    contexts->activated(first_session);
    panel.select_tool("get_live_session");
    require(widget<QLineEdit>(panel, "input_sessionId")->text() == "live-a" &&
                widget<QLineEdit>(panel, "input_leaseToken")->text() == "token-a",
            "Session switch paired an incorrect token");
    panel.select_tool("search_log", {{"query", "panic"}});
    peer->disconnectFromHost();
    until([&]() { return !client.is_connected(); });
    require(!run->isEnabled() && widget<QLineEdit>(panel, "input_logId")->text().isEmpty(), "Disconnect retained stale input or Run state");
    qInfo(
        "PASS: full catalog forms, defaults, confirmations, address precision, nested inputs, bounded/sorted results, request identity and "
        "disconnect recovery");
    return 0;
}
