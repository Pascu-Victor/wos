#include "log_server.h"

#include <QCoreApplication>
#include <QDebug>
#include <QFileInfo>
#include <QProcess>
#include <QRegularExpression>
#include <numeric>

#include "log_processor.h"

LogServer::LogServer(quint16 port, QObject* parent)
    : QObject(parent), tcpServer(new QTcpServer(this)), clientSocket(nullptr), processor(nullptr), filterActive(false) {
    // Load config
    config.loadFromFile();

    if (!tcpServer->listen(QHostAddress::Any, port)) {
        qCritical() << "Server failed to start:" << tcpServer->errorString();
    } else {
        qInfo() << "Server listening on port" << port;
    }

    connect(tcpServer, &QTcpServer::newConnection, this, &LogServer::onNewConnection);
}

LogServer::~LogServer() {
    if (clientSocket) {
        clientSocket->disconnectFromHost();
    }
    if (processor) {
        delete processor;
    }
}

void LogServer::onNewConnection() {
    if (clientSocket) {
        QTcpSocket* newSocket = tcpServer->nextPendingConnection();
        newSocket->disconnectFromHost();
        newSocket->deleteLater();
        return;
    }

    clientSocket = tcpServer->nextPendingConnection();
    connect(clientSocket, &QTcpSocket::readyRead, this, &LogServer::onReadyRead);
    connect(clientSocket, &QTcpSocket::disconnected, this, &LogServer::onClientDisconnected);

    qInfo() << "Client connected from" << clientSocket->peerAddress().toString();

    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);

    QDir dir(QDir::currentPath());
    QStringList filters;
    filters << "*.log" << "*.txt";
    QStringList files = dir.entryList(filters, QDir::Files, QDir::Name);

    std::sort(files.begin(), files.end(), [](const QString& a, const QString& b) {
        bool aModified = a.contains(".modified.");
        bool bModified = b.contains(".modified.");
        if (aModified != bModified) {
            return aModified > bModified;
        }
        return a < b;
    });

    out << (quint32)0;
    out << (quint8)MessageType::Welcome;

    const auto& lookups = config.getAddressLookups();
    out << (quint32)lookups.size();
    for (const auto& lookup : lookups) {
        out << lookup;
    }

    out << files;

    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));

    clientSocket->write(block);
}

void LogServer::onClientDisconnected() {
    qInfo() << "Client disconnected";
    clientSocket->deleteLater();
    clientSocket = nullptr;
}

void LogServer::onReadyRead() {
    if (!clientSocket) return;

    QDataStream in(clientSocket);
    in.setVersion(QDataStream::Qt_6_0);

    while (true) {
        in.startTransaction();

        quint32 size;
        in >> size;

        if (in.status() != QDataStream::Ok) {
            in.rollbackTransaction();
            return;
        }

        if (clientSocket->bytesAvailable() < size) {
            in.rollbackTransaction();
            return;
        }

        quint8 typeInt;
        in >> typeInt;
        MessageType type = static_cast<MessageType>(typeInt);

        processMessage(type, in);

        in.commitTransaction();

        if (clientSocket->bytesAvailable() == 0) break;
    }
}

void LogServer::processMessage(MessageType type, QDataStream& in) {
    switch (type) {
        case MessageType::SelectFile: {
            QString filename;
            in >> filename;

            if (filename == currentFilename && processor) {
                size_t total = filterActive ? filteredIndices.size() : processor->getEntries().size();
                sendFileReady(total);
                return;
            }

            if (processor) {
                delete processor;
                processor = nullptr;
            }

            currentFilename = filename;
            filterActive = false;
            filteredIndices.clear();

            processor = new LogProcessor(filename);
            connect(processor, &LogProcessor::progressUpdate, this, &LogServer::onProcessingProgress);
            connect(processor, &LogProcessor::processingComplete, this, &LogServer::onProcessingComplete);
            connect(processor, &LogProcessor::errorOccurred, this, &LogServer::onProcessingError);

            processor->startProcessing();
            break;
        }
        case MessageType::RequestData: {
            int startLine, count;
            in >> startLine >> count;

            if (!processor) {
                sendError("No file loaded");
                return;
            }

            const auto& entries = processor->getEntries();
            std::vector<LogEntry> result;

            size_t total = filterActive ? filteredIndices.size() : entries.size();

            if (startLine < 0 || startLine >= static_cast<int>(total)) {
                sendDataResponse(startLine, result);
                return;
            }

            int endLine = std::min(startLine + count, static_cast<int>(total));
            result.reserve(endLine - startLine);

            for (int i = startLine; i < endLine; ++i) {
                size_t idx = filterActive ? filteredIndices[i] : i;
                result.push_back(entries[idx]);
            }

            sendDataResponse(startLine, result);
            break;
        }
        case MessageType::SearchRequest: {
            QString text;
            bool isRegex;
            in >> text >> isRegex;

            if (!processor) {
                sendError("No file loaded");
                return;
            }

            const auto& entries = processor->getEntries();
            std::vector<int> matches;

            QRegularExpression regex;
            if (isRegex) {
                regex.setPattern(text);
                regex.setPatternOptions(QRegularExpression::CaseInsensitiveOption);
            } else {
                QString pattern = text;
                pattern.replace(QRegularExpression("([\\^\\$\\*\\+\\?\\{\\}\\[\\]\\(\\)\\|\\\\])"), "\\\\1");
                regex.setPattern(pattern);
                regex.setPatternOptions(QRegularExpression::CaseInsensitiveOption);
            }

            if (!regex.isValid()) {
                sendError("Invalid regex");
                return;
            }

            size_t total = filterActive ? filteredIndices.size() : entries.size();

            for (size_t i = 0; i < total; ++i) {
                size_t idx = filterActive ? filteredIndices[i] : i;
                const auto& entry = entries[idx];

                QString combined = QString::fromStdString(entry.address) + " " + QString::fromStdString(entry.function) + " " +
                                   QString::fromStdString(entry.hexBytes) + " " + QString::fromStdString(entry.assembly);

                if (regex.match(combined).hasMatch()) {
                    matches.push_back(i);
                }
            }

            sendSearchResponse(matches);
            break;
        }
        case MessageType::GetInterruptsRequest: {
            if (!processor) {
                sendError("No file loaded");
                return;
            }

            const auto& entries = processor->getEntries();
            std::vector<LogEntry> interrupts;

            for (const auto& entry : entries) {
                if (entry.type == EntryType::INTERRUPT) {
                    interrupts.push_back(entry);
                }
            }

            sendInterruptsResponse(interrupts);
            break;
        }
        case MessageType::SetFilterRequest: {
            bool hideStructural;
            QString interruptFilter;
            in >> hideStructural >> interruptFilter;
            applyFilter(hideStructural, interruptFilter);
            break;
        }
        case MessageType::RequestRowForLine: {
            int lineNumber;
            in >> lineNumber;

            if (!processor) {
                sendError("No file loaded");
                return;
            }

            const auto& entries = processor->getEntries();
            int row = -1;

            // Binary search could be used if entries are sorted by line number,
            // but they might not be strictly monotonic due to out-of-order execution or other factors.
            // However, they are generally sorted.
            // For now, linear search on the visible entries.

            size_t total = filterActive ? filteredIndices.size() : entries.size();

            // Optimization: Start search from an estimated position?
            // Or just linear search. Since we need exact match.

            for (size_t i = 0; i < total; ++i) {
                size_t idx = filterActive ? filteredIndices[i] : i;
                if (entries[idx].lineNumber == lineNumber) {
                    row = static_cast<int>(i);
                    break;
                }
                // Optimization: if entries are sorted by line number and we passed it
                if (entries[idx].lineNumber > lineNumber) {
                    // Assuming sorted.
                    break;
                }
            }

            sendRowForLineResponse(row);
            break;
        }
        case MessageType::OpenSourceFile: {
            QString file;
            int line;
            in >> file >> line;

            // Use 'code' command to open file in VS Code
            // -g file:line
            QStringList args;
            args << "-g" << QString("%1:%2").arg(file).arg(line);

            if (!QProcess::startDetached("code", args)) {
                qDebug() << "Failed to start 'code' process";
                // Try 'code-insiders' as fallback?
                if (!QProcess::startDetached("code-insiders", args)) {
                    qDebug() << "Failed to start 'code-insiders' process";
                }
            }
            break;
        }
        case MessageType::RequestFileList: {
            QDir dir(QDir::currentPath());
            QStringList filters;
            filters << "*.log" << "*.txt";
            QStringList files = dir.entryList(filters, QDir::Files, QDir::Name);

            std::sort(files.begin(), files.end(), [](const QString& a, const QString& b) {
                bool aModified = a.contains(".modified.");
                bool bModified = b.contains(".modified.");
                if (aModified != bModified) {
                    return aModified > bModified;
                }
                return a < b;
            });

            sendFileListResponse(files);
            break;
        }
        default:
            break;
    }
}

void LogServer::sendRowForLineResponse(int row) {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::RowForLineResponse << row;
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    clientSocket->write(block);
}

void LogServer::applyFilter(bool hideStructural, const QString& interruptFilter) {
    if (!processor) return;

    const auto& entries = processor->getEntries();
    filteredIndices.clear();
    filteredIndices.reserve(entries.size());

    bool hasInterruptFilter = !interruptFilter.isEmpty() && interruptFilter != "All Interrupts";

    for (size_t i = 0; i < entries.size(); ++i) {
        const auto& entry = entries[i];

        if (hideStructural) {
            if (entry.type == EntryType::SEPARATOR || entry.type == EntryType::BLOCK) {
                continue;
            }
        }

        if (hasInterruptFilter) {
            if (entry.type == EntryType::INTERRUPT) {
                if (QString::fromStdString(entry.interruptNumber) != interruptFilter) {
                    continue;
                }
            }
        }

        filteredIndices.push_back(i);
    }

    filterActive = true;
    sendSetFilterResponse(filteredIndices.size());
}

void LogServer::onProcessingProgress(int percentage) { sendProgress(percentage); }

void LogServer::onProcessingComplete() {
    qDebug() << "LogServer::onProcessingComplete";
    if (processor) {
        filterActive = false;
        filteredIndices.clear();
        sendFileReady(processor->getEntries().size());
    } else {
        qWarning() << "LogServer::onProcessingComplete: No processor!";
    }
}

void LogServer::onProcessingError(const QString& error) { sendError(error); }

void LogServer::sendProgress(int percentage) {
    if (!clientSocket) return;
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::Progress << percentage;
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    clientSocket->write(block);
}

void LogServer::sendError(const QString& message) {
    if (!clientSocket) return;
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::Error << message;
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    clientSocket->write(block);
}

void LogServer::sendFileReady(int totalLines) {
    if (!clientSocket) {
        qWarning() << "LogServer::sendFileReady: No client socket!";
        return;
    }
    qDebug() << "LogServer::sendFileReady: Sending FileReady with totalLines=" << totalLines;
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::FileReady << totalLines;
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    clientSocket->write(block);
    clientSocket->flush();  // Ensure data is written
}

void LogServer::sendDataResponse(int startLine, const std::vector<LogEntry>& entries) {
    if (!clientSocket) return;
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::DataResponse << startLine << (quint32)entries.size();
    for (const auto& entry : entries) {
        out << entry;
    }
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    clientSocket->write(block);
}

void LogServer::sendSearchResponse(const std::vector<int>& matches) {
    if (!clientSocket) return;
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::SearchResponse << (quint32)matches.size();
    for (int line : matches) {
        out << line;
    }
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    clientSocket->write(block);
}

void LogServer::sendInterruptsResponse(const std::vector<LogEntry>& interrupts) {
    if (!clientSocket) return;
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::GetInterruptsResponse << (quint32)interrupts.size();
    for (const auto& entry : interrupts) {
        out << entry;
    }
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    clientSocket->write(block);
}

void LogServer::sendSetFilterResponse(int totalLines) {
    if (!clientSocket) return;
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::SetFilterResponse << totalLines;
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    clientSocket->write(block);
}

void LogServer::sendFileListResponse(const QStringList& files) {
    if (!clientSocket) return;
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << (quint32)0 << (quint8)MessageType::FileListResponse << files;
    out.device()->seek(0);
    out << (quint32)(block.size() - sizeof(quint32));
    clientSocket->write(block);
}
