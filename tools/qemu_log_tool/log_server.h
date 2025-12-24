#pragma once

#include <QDir>
#include <QObject>
#include <QTcpServer>
#include <QTcpSocket>

#include "config.h"
#include "protocol.h"
#include "qemu_log_viewer.h"

class LogServer : public QObject {
    Q_OBJECT

   public:
    explicit LogServer(quint16 port, QObject* parent = nullptr);
    ~LogServer();

    bool isListening() const { return tcpServer->isListening(); }
    quint16 serverPort() const { return tcpServer->serverPort(); }

   private slots:
    void onNewConnection();
    void onReadyRead();
    void onClientDisconnected();
    void onProcessingProgress(int percentage);
    void onProcessingComplete();
    void onProcessingError(const QString& error);

   private:
    QTcpServer* tcpServer;
    QTcpSocket* clientSocket;
    LogProcessor* processor;
    Config config;
    QString currentFilename;
    std::vector<size_t> filteredIndices;
    bool filterActive;

    void processMessage(MessageType type, QDataStream& in);
    void sendFileList();
    void sendConfig();
    void sendError(const QString& message);
    void sendProgress(int percentage);
    void sendFileReady(int totalLines);
    void sendDataResponse(int startLine, const std::vector<LogEntry>& entries);
    void sendSearchResponse(const std::vector<int>& matches);
    void sendInterruptsResponse(const std::vector<LogEntry>& interrupts);
    void sendSetFilterResponse(int totalLines);
    void sendRowForLineResponse(int row);
    void sendFileListResponse(const QStringList& files);

    void applyFilter(bool hideStructural, const QString& interruptFilter);
};
