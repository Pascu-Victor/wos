#include <QApplication>
#include <QCommandLineOption>
#include <QCommandLineParser>
#include <QCoreApplication>
#include <QDebug>
#include <QHostAddress>
#include <memory>

#include "log_client.h"
#include "log_server.h"
#include "qemu_log_viewer.h"

int main(int argc, char* argv[]) {
    // Check for server mode to decide whether to instantiate QApplication or QCoreApplication
    bool isServer = false;
    for (int i = 1; i < argc; ++i) {
        if (QString::fromLocal8Bit(argv[i]).startsWith("--server")) {
            isServer = true;
            break;
        }
    }

    std::unique_ptr<QCoreApplication> app;
    if (isServer) {
        app = std::make_unique<QCoreApplication>(argc, argv);
    } else {
        app = std::make_unique<QApplication>(argc, argv);
    }

    QCoreApplication::setApplicationName("QEMU Log Viewer");
    QCoreApplication::setApplicationVersion("2.0");
    QCoreApplication::setOrganizationName("WOS Kernel Project");

    QCommandLineParser parser;
    parser.setApplicationDescription("QEMU Log Viewer with Client-Server Architecture");
    parser.addHelpOption();
    parser.addVersionOption();

    QCommandLineOption serverOption("server", "Run in server mode", "host:port");
    parser.addOption(serverOption);

    QCommandLineOption remoteOption("remote", "Run in remote client mode", "host:port");
    parser.addOption(remoteOption);

    parser.process(*app);

    if (parser.isSet(serverOption)) {
        // Server Mode
        QString hostPort = parser.value(serverOption);
        QStringList parts = hostPort.split(":");
        QString host = "127.0.0.1";
        int port = 12345;

        if (parts.size() == 2) {
            host = parts[0];
            port = parts[1].toInt();
        } else if (parts.size() == 1 && !parts[0].isEmpty()) {
            // Handle case where only port is provided or only host?
            // Assuming strict host:port as per instruction, but let's be flexible
            if (parts[0].contains("."))
                host = parts[0];
            else
                port = parts[0].toInt();
        }

        LogServer server(port);
        if (!server.isListening()) {
            qCritical() << "Failed to start server on" << host << ":" << port;
            return 1;
        }

        qInfo() << "Server started on" << host << ":" << port;
        return app->exec();
    } else if (parser.isSet(remoteOption)) {
        // Remote Client Mode
        QString hostPort = parser.value(remoteOption);
        QStringList parts = hostPort.split(":");
        QString host = "127.0.0.1";
        int port = 12345;

        if (parts.size() == 2) {
            host = parts[0];
            port = parts[1].toInt();
        } else if (parts.size() == 1 && !parts[0].isEmpty()) {
            if (parts[0].contains("."))
                host = parts[0];
            else
                port = parts[0].toInt();
        }

        LogClient* client = new LogClient(app.get());
        client->connectToHost(host, port);

        QemuLogViewer viewer(client);
        viewer.show();

        return app->exec();
    } else {
        // Standalone Mode (Local Pair)
        // Start server on localhost with ephemeral port
        LogServer* server = new LogServer(0, app.get());
        if (!server->isListening()) {  // 0 = ephemeral port
            qCritical() << "Failed to start internal server";
            return 1;
        }

        quint16 port = server->serverPort();
        qInfo() << "Internal server started on port" << port;

        LogClient* client = new LogClient(app.get());
        client->connectToHost("127.0.0.1", port);

        QemuLogViewer viewer(client);
        viewer.show();

        return app->exec();
    }
}
