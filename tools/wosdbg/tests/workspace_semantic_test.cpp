#include <QApplication>
#include <QDialog>
#include <QElapsedTimer>
#include <QInputDialog>
#include <QJsonArray>
#include <QJsonDocument>
#include <QLabel>
#include <QLineEdit>
#include <QMessageBox>
#include <QPushButton>
#include <QScrollBar>
#include <QSettings>
#include <QTabBar>
#include <QTabWidget>
#include <QTableWidget>
#include <QTcpServer>
#include <QTcpSocket>
#include <QTemporaryDir>
#include <QTimer>
#include <functional>

#include "debug_analysis_service.h"
#include "debugger_workspace.h"
#include "log_client.h"
#include "virtual_table.h"
#include "virtual_table_integration.h"
#include "wosdbg.h"

namespace {
void check(bool value, const char* message) {
    if (!value) {
        qFatal("%s", message);
    }
}
void until(const std::function<bool()>& predicate) {
    QElapsedTimer timer;
    timer.start();
    while (!predicate() && timer.elapsed() < 5000) {
        QApplication::processEvents(QEventLoop::AllEvents, 10);
    }
    check(predicate(), "Timed out waiting for workspace fixture");
}
template <class T>
T* child(QObject& parent, const char* name) {
    auto* value = parent.findChild<T*>(name);
    check(value != nullptr, name);
    return value;
}
void transmit(QTcpSocket* socket, MessageType type, const std::function<void(QDataStream&)>& write) {
    QByteArray bytes;
    QDataStream stream(&bytes, QIODevice::WriteOnly);
    stream.setVersion(QDataStream::Qt_6_0);
    stream << quint32(0) << static_cast<quint8>(type);
    write(stream);
    stream.device()->seek(0);
    stream << static_cast<quint32>(bytes.size() - sizeof(quint32));
    socket->write(bytes);
}

struct Fixture : QObject {
    QTcpServer server;
    QTcpSocket* peer = nullptr;
    bool enabled = false;
    int memory_reads = 0;
    int data_requests = 0;
    int outstanding = 0;
    int high_water = 0;
    int data_delay = 30;
    QJsonObject last_open;
    QJsonObject last_seek;
    Fixture() {
        check(server.listen(QHostAddress::LocalHost, 0), "Cannot listen on local fixture socket");
        connect(&server, &QTcpServer::newConnection, this, [this]() {
            peer = server.nextPendingConnection();
            connect(peer, &QTcpSocket::readyRead, this, [this]() { receive(); });
        });
    }
    QJsonObject answer(const QString& name, const QJsonObject& args) {
        if (name == "list_logs") {
            return {{"ok", true}, {"logs", QJsonArray{QJsonObject{{"name", "serial-vm0.log"}, {"path", "/fixture/serial-vm0.log"}}}}};
        }
        if (name == "list_coredumps") {
            return {{"ok", true},
                    {"coredumps", QJsonArray{QJsonObject{{"name", "kernel_coredump.bin"}, {"path", "/fixture/kernel_coredump.bin"}}}}};
        }
        if (name == "enable_live_debugging") {
            check(args["confirmation"].toString() == "ENABLE_CONFIGURED_LIVE_TARGETS", "Enable lacked confirmation");
            enabled = true;
        }
        if (name == "discover_live_targets" || name == "enable_live_debugging") {
            if (!enabled) {
                return {{"ok", false}, {"errorCode", "disabled"}, {"error", "live debugging is disabled by configuration"}};
            }
            return {{"ok", true},
                    {"targets", QJsonArray{QJsonObject{{"id", "vm0"},
                                                       {"nodeId", "0"},
                                                       {"transport", "qemu"},
                                                       {"host", "127.0.0.1"},
                                                       {"port", 1234},
                                                       {"symbolStatus", "verified"}}}}};
        }
        if (name == "open_coredump" || name == "get_crash_summary") {
            return {{"ok", true},
                    {"dumpId", "dump-1"},
                    {"summary", QJsonObject{{"interruptName", "Page fault"},
                                            {"pid", "42"},
                                            {"cpu", "0"},
                                            {"trapRip", "0xffffffff80001234"},
                                            {"cr2", "0x10"},
                                            {"symbols", QJsonObject{{"symbolStatus", "verified"}}}}}};
        }
        if (name == "describe_registers" || name == "read_live_registers") {
            return {{"ok", true},
                    {"registers", QJsonArray{QJsonObject{{"name", "rip"}, {"value", "0xffffffff80001234"}},
                                             QJsonObject{{"name", "rsp"}, {"value", "0xffff800010001000"}}}}};
        }
        if (name == "backtrace_coredump" || name == "backtrace_live") {
            return {{"ok", true},
                    {"frames", QJsonArray{QJsonObject{{"index", 0},
                                                      {"rip", "0xffffffff80001234"},
                                                      {"address", "0xffffffff80001234"},
                                                      {"symbol", "kernel::page_fault"},
                                                      {"confidence", "likely"}}}}};
        }
        if (name == "disassemble_coredump") {
            last_seek = args;
            return {{"ok", true},
                    {"instructions", QJsonArray{QJsonObject{{"address", "0xffffffff80001234"},
                                                            {"bytes", "48 8b 00"},
                                                            {"mnemonic", "mov"},
                                                            {"operands", "rax, qword ptr [rax]"},
                                                            {"symbol", "kernel::page_fault"},
                                                            {"marker", "trap_rip"}}}}};
        }
        if (name == "get_memory_context") {
            return {{"ok", true},
                    {"rows", QJsonArray{QJsonObject{{"address", "0xffff800010001000"},
                                                    {"value", "0xffffffff80001234"},
                                                    {"symbol", "kernel::page_fault"},
                                                    {"notes", QJsonArray{"return address"}}}}}};
        }
        if (name == "open_live_session" || name == "renew_live_session") {
            if (name == "open_live_session") {
                last_open = args;
                check(args["authority"].toString() == "pause-read" && args["confirmation"].toString() == "PAUSE_ALLOWLISTED_TARGETS",
                      "Attach lacked authority");
            }
            return {{"ok", true}, {"sessionId", "live-1"}, {"leaseToken", "secret-fixture-token"}, {"leaseMsRemaining", 30000}};
        }
        if (name == "read_live_memory") {
            ++memory_reads;
        }
        return {{"ok", true}};
    }
    void receive() {
        QDataStream in(peer);
        in.setVersion(QDataStream::Qt_6_0);
        while (peer->bytesAvailable()) {
            in.startTransaction();
            quint32 size;
            quint8 type;
            in >> size;
            if (in.status() != QDataStream::Ok || peer->bytesAvailable() < size) {
                in.rollbackTransaction();
                return;
            }
            in >> type;
            switch (static_cast<MessageType>(type)) {
                case MessageType::TOOL_CALL_REQUEST: {
                    quint64 id;
                    QString name;
                    QByteArray args;
                    in >> id >> name >> args;
                    auto result = answer(name.section('.', -1), QJsonDocument::fromJson(args).object());
                    transmit(peer, MessageType::TOOL_CALL_RESPONSE,
                             [=](QDataStream& stream) { stream << id << QJsonDocument(result).toJson(QJsonDocument::Compact); });
                    break;
                }
                case MessageType::REQUEST_DATA: {
                    int first, count;
                    in >> first >> count;
                    ++data_requests;
                    ++outstanding;
                    high_water = std::max(high_water, outstanding);
                    QTimer::singleShot(data_delay, this, [this, first, count]() {
                        --outstanding;
                        transmit(peer, MessageType::DATA_RESPONSE, [=](QDataStream& stream) {
                            stream << first << static_cast<quint32>(count);
                            for (int index = first; index < first + count; ++index) {
                                LogEntry entry;
                                entry.line_number = index + 1;
                                entry.type = EntryType::INSTRUCTION;
                                entry.address = "0xffffffff80001234";
                                entry.function = "kernel::worker";
                                entry.hex_bytes = "90";
                                entry.assembly = "nop";
                                stream << entry;
                            }
                        });
                    });
                    break;
                }
                default:
                    in.skipRawData(static_cast<int>(size) - 1);
                    break;
            }
            check(in.commitTransaction(), "Malformed fixture request");
        }
    }
};
}  // namespace

int main(int argc, char** argv) {
    QApplication app(argc, argv);
    QTemporaryDir settings;
    QSettings::setPath(QSettings::NativeFormat, QSettings::UserScope, settings.path());
    ConfigService::instance().get_mutable_config().set_coredump_directory(settings.path() + "/coredumps");
    {
        Config config;
        config.get_mutable_mcp_settings().allowed_roots = QStringList{settings.path()};
        DebugAnalysisService service;
        service.set_config(config);
        check(service.list_live_targets()["errorCode"].toString() == "disabled", "Live access wasn't disabled by default");
        check(!service.enable_live_debugging({})["ok"].toBool(), "Live enabling did not require explicit confirmation");
        check(service.list_live_targets()["errorCode"].toString() == "disabled", "Denied enable changed live state");
        const auto result = service.enable_live_debugging({{"confirmation", "ENABLE_CONFIGURED_LIVE_TARGETS"}});
        check(result["ok"].toBool() && result["targetCount"].toInt() == 0, "Configured-only opt-in failed or invented a target");
        check(!config.get_live_settings().enabled, "Process-local enable mutated persistent settings");
    }
    Fixture fixture;
    LogClient client;
    QemuLogViewer window(&client);
    window.resize(1440, 960);
    window.show();
    client.connect_to_host("127.0.0.1", fixture.server.serverPort());
    auto* workspace = child<DebuggerWorkspace>(window, "debuggerWorkspace");
    until([&]() { return child<QTableWidget>(window, "evidenceFiles")->rowCount() == 2; });
    auto* evidence_table = child<QTableWidget>(window, "evidenceFiles");
    check(evidence_table->palette().base().color().lightness() < 128 && evidence_table->palette().text().color().lightness() > 128,
          "Dark stylesheet used a light application palette and unreadable text");
    auto* tabs = child<QTabWidget>(window, "workspaceTabs");
    const QRect navigation(tabs->tabBar()->mapToGlobal(QPoint()), tabs->tabBar()->size());
    for (int index : {1, 0, 1, 0}) {
        tabs->setCurrentIndex(index);
        QApplication::processEvents();
        check(QRect(tabs->tabBar()->mapToGlobal(QPoint()), tabs->tabBar()->size()) == navigation,
              "Workspace navigation moved when docks changed");
    }
    if (argc > 1) {
        check(window.grab().save(QString::fromLocal8Bit(argv[1]) + "-welcome.png"), "Cannot save welcome preview");
    }
    window.resize(1024, 768);
    QApplication::processEvents();
    check(window.width() == 1024, "Hidden workspace panes forced the application beyond laptop width");
    if (argc > 1) {
        check(window.grab().save(QString::fromLocal8Bit(argv[1]) + "-compact.png"), "Cannot save compact preview");
    }
    window.resize(1440, 960);
    QApplication::processEvents();
    workspace->open_path("coredump", "/fixture/kernel_coredump.bin");
    auto* code = child<QTableWidget>(window, "debuggerCode");
    until([&]() { return code->rowCount() == 1 && child<QTableWidget>(window, "debuggerRegisters")->rowCount() == 2; });
    check(code->item(0, 2)->text() == "mov rax, qword ptr [rax]", "Disassembly wasn't projected into instructions");
    auto* registers = child<QTableWidget>(window, "debuggerRegisters");
    registers->setCurrentCell(1, 0);
    registers->cellDoubleClicked(1, 0);
    until([&]() { return fixture.last_seek["address"].toString() == "0xffff800010001000"; });
    if (argc > 1) {
        check(window.grab().save(QString::fromLocal8Bit(argv[1]) + "-crash.png"), "Cannot save crash preview");
    }
    workspace->show_targets();
    auto* enable = child<QPushButton>(window, "enableLiveDebugging");
    until([&]() { return enable->isVisible(); });
    check(!child<QLabel>(window, "debuggerNotice")->isVisible(), "Disabled live debugging was rendered as a failed analysis");
    QTimer::singleShot(0, &window, []() {
        auto* dialog = qobject_cast<QMessageBox*>(QApplication::activeModalWidget());
        check(dialog, "No enable confirmation");
        dialog->button(QMessageBox::Yes)->click();
    });
    enable->click();
    auto* attach = child<QPushButton>(window, "attachTarget");
    until([&]() { return attach->isEnabled(); });
    if (argc > 1) {
        check(window.grab().save(QString::fromLocal8Bit(argv[1]) + "-attach.png"), "Cannot save attach preview");
    }
    QTimer::singleShot(0, &window, []() {
        auto* dialog = qobject_cast<QInputDialog*>(QApplication::activeModalWidget());
        check(dialog, "No attach confirmation");
        dialog->setTextValue("Host UI fixture");
        dialog->accept();
    });
    attach->click();
    until([&]() { return workspace->context()["sessionId"].toString() == "live-1"; });
    check(fixture.memory_reads == 0, "Attach implicitly read sensitive live memory");
    for (auto* label : window.findChildren<QLabel*>()) {
        check(!label->text().contains("secret-fixture-token"), "Lease credential leaked into label");
    }
    fixture.peer->disconnectFromHost();
    until([&]() { return workspace->context().isEmpty(); });

    Fixture log_fixture;
    LogClient log_client;
    log_client.connect_to_host("127.0.0.1", log_fixture.server.serverPort());
    until([&]() { return log_client.is_connected() && log_fixture.peer; });
    auto* view = VirtualTableIntegration::initialize_virtual_table(nullptr, &log_client);
    view->resize(1100, 600);
    view->show();
    auto* model = static_cast<VirtualTableModel*>(view->model());
    QObject::connect(&log_client, &LogClient::file_ready, model, &VirtualTableModel::setRowCount);
    QObject::connect(&log_client, &LogClient::data_received, view,
                     [=](int first, int count) { model->invalidateRows(first, first + count - 1); });
    transmit(log_fixture.peer, MessageType::FILE_READY, [](QDataStream& stream) { stream << 100000; });
    until([&]() { return log_client.rows_ready(0, 4096) && model->rowCount() == 100000; });
    view->scrollToLogicalRow(2500, QAbstractItemView::PositionAtTop);
    QApplication::processEvents();
    check(view->getViewportStartRow() == 2500, "Viewport index mixed pixel and item scroll units");
    check(model->data(model->index(2500, 5)).toString() == "nop", "Warm scrolling lost populated cells");
    log_fixture.data_delay = 100;
    view->scrollToLogicalRow(70000, QAbstractItemView::PositionAtTop);
    QApplication::processEvents();
    check(model->data(model->index(70000, 5)).isNull(), "Missing data became a cached placeholder");
    if (argc > 1) {
        check(view->grab().save(QString::fromLocal8Bit(argv[1]) + "-buffered-jump.png"), "Cannot save scroll preview");
    }
    until([&]() { return log_client.rows_ready(70000, 128); });
    check(model->data(model->index(70000, 5)).toString() == "nop", "Cold jump never populated");
    for (int index : {100, 50000, 1000, 90000, 3000, 70000}) {
        view->scrollToLogicalRow(index, QAbstractItemView::PositionAtTop);
        QApplication::processEvents();
    }
    until([&]() { return log_client.rows_ready(70000, 128) && log_fixture.outstanding == 0; });
    check(log_fixture.high_water <= 4, "Scrolling created an unbounded request backlog");
    check(log_fixture.data_requests < 35, "Scrolling repeatedly requested already in-flight chunks");
    check(!log_client.rows_ready(0, 128), "Distant log windows were retained indefinitely");
    bool filter_ready = false;
    QObject::connect(&log_client, &LogClient::filter_applied, view, [&](int total) {
        check(total == 10 && log_client.rows_ready(0, 10), "Filter exposed rows before their data was ready");
        filter_ready = true;
    });
    log_client.set_filter(true, "");
    transmit(log_fixture.peer, MessageType::DATA_RESPONSE, [](QDataStream& stream) {
        LogEntry stale;
        stale.assembly = "STALE";
        stream << 0 << quint32(1) << stale;
    });
    transmit(log_fixture.peer, MessageType::SET_FILTER_RESPONSE, [](QDataStream& stream) { stream << 10; });
    until([&]() { return filter_ready; });
    check(log_client.get_entry(0)->assembly == "nop", "Old view data contaminated a filter result");
    model->setCacheSize(1);
    (void)model->data(model->index(0, 0));
    (void)model->data(model->index(1, 0));
    size_t cached, hits, misses;
    model->getCache().getStats(cached, hits, misses);
    check(cached == 1, "Formatted-row cache size control had no effect");
    delete view;
    qInfo(
        "PASS: stable whole-window navigation; linked crash views; guided enable/attach; credential isolation; buffered scrolling and "
        "bounded requests");
}
