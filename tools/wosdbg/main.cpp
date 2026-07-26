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
#include "debug_analysis_service.h"
#include "debug_cli.h"
#include "log_client.h"
#include "log_server.h"
#include "wosdbg.h"

int main(int argc, char* argv[]) {
    // Check for server mode to decide whether to instantiate QApplication or QCoreApplication
    bool is_server = false;
    bool is_cli = false;
    for (int i = 1; i < argc; ++i) {
        const QString ARGUMENT = QString::fromLocal8Bit(argv[i]);
        if (ARGUMENT.startsWith("--server")) {
            is_server = true;
        }
        if (ARGUMENT == "--list-tools" || ARGUMENT == "--list-resources" || ARGUMENT == "--list-resource-templates" ||
            ARGUMENT.startsWith("--tool") || ARGUMENT.startsWith("--batch") || ARGUMENT.startsWith("--read-resource")) {
            is_cli = true;
        }
    }

    std::unique_ptr<QCoreApplication> app;
    if (is_server || is_cli) {
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

    QCommandLineOption list_tools_option("list-tools", "List the canonical analysis tool catalog as JSON");
    parser.addOption(list_tools_option);

    QCommandLineOption tool_option("tool", "Invoke one analysis tool (the wosdbg. prefix is optional)", "name");
    parser.addOption(tool_option);

    QCommandLineOption arguments_option("arguments", "JSON object passed to --tool", "json", "{}");
    parser.addOption(arguments_option);

    QCommandLineOption batch_option(
        "batch", "Run a JSON workflow while preserving sessions; path may be '-' for stdin and later calls may reference $id.field",
        "path");
    parser.addOption(batch_option);

    QCommandLineOption list_resources_option("list-resources", "List resources from the local analysis session as JSON");
    parser.addOption(list_resources_option);

    QCommandLineOption list_resource_templates_option("list-resource-templates", "List resource templates as JSON");
    parser.addOption(list_resource_templates_option);

    QCommandLineOption read_resource_option("read-resource", "Read a wosdbg resource URI as JSON", "uri");
    parser.addOption(read_resource_option);

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

    if (is_cli) {
        DebugAnalysisService analysis;
        analysis.set_config(ConfigService::instance().get_config());
        return run_debug_cli(analysis, DebugCliOptions{.list_tools = parser.isSet(list_tools_option),
                                                       .list_resources = parser.isSet(list_resources_option),
                                                       .list_resource_templates = parser.isSet(list_resource_templates_option),
                                                       .tool_name = parser.value(tool_option),
                                                       .arguments_json = parser.value(arguments_option),
                                                       .batch_path = parser.value(batch_option),
                                                       .resource_uri = parser.value(read_resource_option)});
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
