#include <qcontainerfwd.h>
#include <qdir.h>
#include <qlogging.h>
#include <qtypes.h>

#include <QApplication>
#include <QCommandLineOption>
#include <QCommandLineParser>
#include <QCoreApplication>
#include <QDebug>
#include <QHostAddress>
#include <memory>

#include "config.h"
#include "log_client.h"
#include "log_server.h"
#include "wosdbg.h"

int main(int argc, char* argv[]) {
    // Check for server mode to decide whether to instantiate QApplication or QCoreApplication
    bool is_server = false;
    for (int i = 1; i < argc; ++i) {
        if (QString::fromLocal8Bit(argv[i]).startsWith("--server")) {
            is_server = true;
            break;
        }
    }

    std::unique_ptr<QCoreApplication> app;
    if (is_server) {
        app = std::make_unique<QCoreApplication>(argc, argv);
    } else {
        app = std::make_unique<QApplication>(argc, argv);
    }

    QCoreApplication::setApplicationName("wosdbg");
    QCoreApplication::setApplicationVersion("2.0");
    QCoreApplication::setOrganizationName("WOS Kernel Project");

    QCommandLineParser parser;
    parser.setApplicationDescription("WOS Debugger - Execution Log Viewer & Coredump Analyzer");
    parser.addHelpOption();
    parser.addVersionOption();

    QCommandLineOption server_option("server", "Run in server mode", "host:port");
    parser.addOption(server_option);

    QCommandLineOption remote_option("remote", "Run in remote client mode", "host:port");
    parser.addOption(remote_option);

    QCommandLineOption mcp_option("mcp", "Start the MCP server with the wosdbg backend");
    parser.addOption(mcp_option);

    QCommandLineOption mcp_host_option("mcp-host", "MCP bind address", "host");
    parser.addOption(mcp_host_option);

    QCommandLineOption mcp_port_option("mcp-port", "MCP port", "port");
    parser.addOption(mcp_port_option);

    parser.process(*app);

    // Initialize config service - search CWD and upward for wosdbg.json
    {
        QString config_path = "wosdbg.json";
        QDir dir = QDir::current();
        for (int i = 0; i < 5; ++i) {
            if (QFile::exists(dir.filePath("wosdbg.json"))) {
                config_path = dir.absoluteFilePath("wosdbg.json");
                break;
            }
            if (!dir.cdUp()) {
                break;
            }
        }
        ConfigService::instance().initialize(config_path);
    }

    if (parser.isSet(server_option)) {
        // Server Mode
        QString host_port = parser.value(server_option);
        QStringList parts = host_port.split(":");
        QString host = "127.0.0.1";
        int port = 12345;

        if (parts.size() == 2) {
            host = parts[0];
            port = parts[1].toInt();
        } else if (parts.size() == 1 && !parts[0].isEmpty()) {
            // Handle case where only port is provided or only host?
            // Assuming strict host:port as per instruction, but let's be flexible
            if (parts[0].contains(".")) {
                host = parts[0];
            } else {
                port = parts[0].toInt();
            }
        }

        LogServer server(port);
        if (!server.is_listening()) {
            qCritical() << "Failed to start server on" << host << ":" << port;
            return 1;
        }

        if (parser.isSet(mcp_option)) {
            QString mcp_host = parser.value(mcp_host_option);
            quint16 mcp_port = static_cast<quint16>(parser.value(mcp_port_option).toUInt());
            if (!server.start_mcp_server(mcp_host, mcp_port)) {
                return 1;
            }
        }

        qInfo() << "Server started on" << host << ":" << port;
        return QCoreApplication::exec();
    }

    if (parser.isSet(remote_option)) {
        // Remote Client Mode
        QString host_port = parser.value(remote_option);
        QStringList parts = host_port.split(":");
        QString host = "127.0.0.1";
        int port = 12345;

        if (parts.size() == 2) {
            host = parts[0];
            port = parts[1].toInt();
        } else if (parts.size() == 1 && !parts[0].isEmpty()) {
            if (parts[0].contains(".")) {
                host = parts[0];
            } else {
                port = parts[0].toInt();
            }
        }

        auto* client = new LogClient(app.get());
        client->connect_to_host(host, port);

        QemuLogViewer viewer(client);
        viewer.show();

        return QCoreApplication::exec();
    }

    // Standalone Mode (Local Pair)
    // Start server on localhost with ephemeral port
    auto* server = new LogServer(0, app.get());
    if (!server->is_listening()) {  // 0 = ephemeral port
        qCritical() << "Failed to start internal server";
        return 1;
    }

    quint16 port = server->server_port();
    qInfo() << "Internal server started on port" << port;

    auto* client = new LogClient(app.get());
    client->connect_to_host("127.0.0.1", port);

    QemuLogViewer viewer(client);
    viewer.show();

    return QCoreApplication::exec();
}
