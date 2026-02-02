#pragma once

#include <QObject>
#include <QTcpSocket>
#include <QTimer>
#include <map>
#include <vector>

#include "config.h"
#include "protocol.h"

class LogClient : public QObject {
    Q_OBJECT

   public:
    explicit LogClient(QObject* parent = nullptr);
    ~LogClient();

    void connectToHost(const QString& host, quint16 port);
    void selectFile(const QString& filename);

    // Returns pointer to entry if available, nullptr otherwise.
    // If nullptr, it triggers a fetch for a chunk around this line.
    const LogEntry* getEntry(int lineIndex);

    const QStringList& getFileList() const { return fileList; }
    const Config& getConfig() const { return config; }
    int getTotalLines() const { return totalLines; }
    bool isConnected() const { return socket->state() == QAbstractSocket::ConnectedState; }

    void search(const QString& text, bool isRegex);
    void requestInterrupts();
    void setFilter(bool hideStructural, const QString& interruptFilter);
    void requestRowForLine(int lineNumber);
    void requestOpenSourceFile(const QString& file, int line);
    void requestFileList();

   signals:
    void searchResults(const std::vector<int>& matches);
    void interruptsReceived(const std::vector<LogEntry>& interrupts);
    void filterApplied(int totalLines);
    void rowForLineReceived(int row);
    void connected();
    void disconnected();
    void connectionError(const QString& error);
    void fileListReceived(const QStringList& files);
    void configReceived();
    void fileReady(int totalLines);
    void progress(int percentage);
    void errorOccurred(const QString& error);
    void dataReceived(int startLine, int count);  // Signal to repaint

   private slots:
    void onConnected();
    void onReadyRead();
    void onSocketError(QAbstractSocket::SocketError socketError);
    void processPendingRequests();

   private:
    QTcpSocket* socket;
    Config config;
    QStringList fileList;
    int totalLines;

    // Cache: lineIndex -> LogEntry
    std::map<int, LogEntry> cache;

    // Request management
    std::vector<std::pair<int, int>> pendingRequests;  // start, count
    QTimer requestTimer;
    bool initialLoadPending;

    void processMessage(MessageType type, QDataStream& in);
    void requestData(int startLine, int count);
    void sendSelectFile(const QString& filename);
};
