#include "log_client.h"

#include <QDataStream>
#include <QDebug>

LogClient::LogClient(QObject* parent) : QObject(parent), socket(new QTcpSocket(this)), totalLines(0), initialLoadPending(false) {
    connect(socket, &QTcpSocket::connected, this, &LogClient::onConnected);
    connect(socket, &QTcpSocket::readyRead, this, &LogClient::onReadyRead);
    connect(socket, &QTcpSocket::errorOccurred, this, &LogClient::onSocketError);

    requestTimer.setSingleShot(true);
    requestTimer.setInterval(50);  // Debounce requests
    connect(&requestTimer, &QTimer::timeout, this, &LogClient::processPendingRequests);
}

LogClient::~LogClient() { socket->disconnectFromHost(); }

void LogClient::connectToHost(const QString& host, quint16 port) { socket->connectToHost(host, port); }

void LogClient::onConnected() { emit connected(); }

void LogClient::onSocketError(QAbstractSocket::SocketError socketError) {
    Q_UNUSED(socketError);
    emit connectionError(socket->errorString());
}

void LogClient::onReadyRead() {
    QDataStream in(socket);
    in.setVersion(QDataStream::Qt_6_0);

    while (true) {
        in.startTransaction();

        quint32 size;
        in >> size;

        if (in.status() != QDataStream::Ok) {
            in.rollbackTransaction();
            return;
        }

        if (socket->bytesAvailable() < size) {
            in.rollbackTransaction();
            return;
        }

        // We have the full message
        quint8 typeInt;
        in >> typeInt;
        MessageType type = static_cast<MessageType>(typeInt);

        processMessage(type, in);

        in.commitTransaction();

        if (socket->bytesAvailable() == 0) break;
    }
}

void LogClient::processMessage(MessageType type, QDataStream& in) {
    switch (type) {
        case MessageType::Welcome: {
            // Config
            quint32 lookupCount;
            in >> lookupCount;
            config.clearAddressLookups();
            for (quint32 i = 0; i < lookupCount; ++i) {
                AddressLookup lookup;
                in >> lookup;
                config.addAddressLookup(lookup);
            }
            emit configReceived();

            // File List
            in >> fileList;
            emit fileListReceived(fileList);
            break;
        }
        case MessageType::FileReady: {
            in >> totalLines;
            qDebug() << "Client received FileReady: totalLines=" << totalLines;
            cache.clear();

            if (totalLines > 0) {
                initialLoadPending = true;
                // Request first chunk immediately to avoid flashing
                requestData(0, 2000);
            } else {
                emit fileReady(totalLines);
            }
            break;
        }
        case MessageType::Progress: {
            int pct;
            in >> pct;
            emit progress(pct);
            break;
        }
        case MessageType::Error: {
            QString msg;
            in >> msg;
            emit errorOccurred(msg);
            break;
        }
        case MessageType::DataResponse: {
            int startLine;
            quint32 count;
            in >> startLine >> count;

            for (quint32 i = 0; i < count; ++i) {
                LogEntry entry;
                in >> entry;
                cache[startLine + i] = entry;
                // if (i == 0) qDebug() << "First entry assembly:" << QString::fromStdString(entry.assembly);
            }

            if (initialLoadPending) {
                initialLoadPending = false;
                emit fileReady(totalLines);
            }

            emit dataReceived(startLine, count);
            break;
        }
        case MessageType::SearchResponse: {
            quint32 count;
            in >> count;
            std::vector<int> matches;
            matches.reserve(count);
            for (quint32 i = 0; i < count; ++i) {
                int line;
                in >> line;
                matches.push_back(line);
            }
            emit searchResults(matches);
            break;
        }
        case MessageType::GetInterruptsResponse: {
            quint32 count;
            in >> count;
            qDebug() << "Received GetInterruptsResponse: count=" << count;
            std::vector<LogEntry> interrupts;
            interrupts.reserve(count);
            for (quint32 i = 0; i < count; ++i) {
                LogEntry entry;
                in >> entry;
                interrupts.push_back(entry);
                // qDebug() << "Interrupt:" << QString::fromStdString(entry.interruptNumber);
            }
            emit interruptsReceived(interrupts);
            break;
        }
        case MessageType::SetFilterResponse: {
            in >> totalLines;
            cache.clear();
            emit filterApplied(totalLines);
            break;
        }
        case MessageType::RowForLineResponse: {
            int row;
            in >> row;
            emit rowForLineReceived(row);
            break;
        }
        case MessageType::FileListResponse: {
            in >> fileList;
            emit fileListReceived(fileList);
            break;
        }
        default:
            break;
    }
}

void LogClient::selectFile(const QString& filename) { sendSelectFile(filename); }

void LogClient::setFilter(bool hideStructural, const QString& interruptFilter) {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::SetFilterRequest << hideStructural << interruptFilter;
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    socket->write(block);
}

void LogClient::requestRowForLine(int lineNumber) {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::RequestRowForLine << lineNumber;
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    socket->write(block);
}

void LogClient::search(const QString& text, bool isRegex) {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::SearchRequest << text << isRegex;
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    socket->write(block);
}

void LogClient::requestInterrupts() {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::GetInterruptsRequest;
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    socket->write(block);
}

void LogClient::requestOpenSourceFile(const QString& file, int line) {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::OpenSourceFile << file << line;
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    socket->write(block);
}

void LogClient::sendSelectFile(const QString& filename) {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::SelectFile << filename;
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    socket->write(block);
}

const LogEntry* LogClient::getEntry(int lineIndex) {
    auto it = cache.find(lineIndex);
    if (it != cache.end()) {
        return &it->second;
    }

    // Request data
    // Check if already pending?
    // For simplicity, just add to pending requests and debounce
    // Fetch a larger chunk to handle scrolling (500 before, 1500 after)
    int start = std::max(0, lineIndex - 500);
    int count = 2000;
    pendingRequests.emplace_back(start, count);
    if (!requestTimer.isActive()) {
        requestTimer.start();
    }

    return nullptr;
}

void LogClient::processPendingRequests() {
    if (pendingRequests.empty()) return;

    // Merge overlapping requests?
    // For now, just send them.
    // Optimization: Merge contiguous ranges.

    std::sort(pendingRequests.begin(), pendingRequests.end());

    std::vector<std::pair<int, int>> merged;
    if (!pendingRequests.empty()) {
        merged.push_back(pendingRequests[0]);
        for (size_t i = 1; i < pendingRequests.size(); ++i) {
            auto& last = merged.back();
            auto& curr = pendingRequests[i];

            if (curr.first <= last.first + last.second) {
                // Overlap or adjacent
                int newEnd = std::max(last.first + last.second, curr.first + curr.second);
                last.second = newEnd - last.first;
            } else {
                merged.push_back(curr);
            }
        }
    }

    for (const auto& req : merged) {
        requestData(req.first, req.second);
    }

    pendingRequests.clear();
}

void LogClient::requestData(int startLine, int count) {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::RequestData << startLine << count;
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    socket->write(block);
}

void LogClient::requestFileList() {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::RequestFileList;
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    socket->write(block);
}
