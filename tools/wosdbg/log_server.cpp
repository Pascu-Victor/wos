#include "log_server.h"

#include <qcontainerfwd.h>
#include <qhostaddress.h>
#include <qlogging.h>
#include <qobject.h>
#include <qstringview.h>
#include <qtcpserver.h>
#include <qtcpsocket.h>
#include <qtypes.h>

#include <QCoreApplication>
#include <QDebug>
#include <QFileInfo>
#include <QProcess>
#include <QRegularExpression>
#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

#include "debug_analysis_service.h"
#include "log_entry.h"
#include "log_processor.h"
#include "mcp_http_server.h"
#include "protocol.h"

LogServer::LogServer(quint16 port, QObject* parent)
    : QObject(parent),
      tcp_server(new QTcpServer(this)),

      analysis_service(new DebugAnalysisService(this)),
      mcp_server(new McpHttpServer(analysis_service, this)) {
    // Load config
    config.load_from_file();
    analysis_service->set_config(config);
    mcp_server->set_allowed_cidrs(config.get_mcp_settings().allowed_cidrs);

    if (!tcp_server->listen(QHostAddress::Any, port)) {
        qCritical() << "Server failed to start:" << tcp_server->errorString();
    } else {
        qInfo() << "Server listening on port" << port;
    }

    connect(tcp_server, &QTcpServer::newConnection, this, &LogServer::on_new_connection);
}

LogServer::~LogServer() {
    if (clientSocket) {
        clientSocket->disconnectFromHost();
    }

    delete processor;

    stop_mcp_server();
}

bool LogServer::start_mcp_server(const QString& bind_address, quint16 port) {
    const auto& settings = config.get_mcp_settings();
    QString host = bind_address.isEmpty() ? settings.bind_address : bind_address;
    quint16 listen_port = port == 0 ? settings.port : port;
    mcp_server->set_allowed_cidrs(settings.allowed_cidrs);
    bool ok = mcp_server->start(host, listen_port);
    if (ok) {
        qInfo() << "MCP server listening at" << mcp_server->endpoint();
    } else {
        qWarning() << "Failed to start MCP server at" << host << listen_port;
    }
    return ok;
}

void LogServer::stop_mcp_server() { mcp_server->stop(); }

bool LogServer::is_mcp_listening() const { return mcp_server->is_listening(); }

QString LogServer::mcp_endpoint() const { return mcp_server->endpoint(); }

void LogServer::on_new_connection() {
    if (clientSocket) {
        QTcpSocket* new_socket = tcp_server->nextPendingConnection();
        new_socket->disconnectFromHost();
        new_socket->deleteLater();
        return;
    }

    clientSocket = tcp_server->nextPendingConnection();
    connect(clientSocket, &QTcpSocket::readyRead, this, &LogServer::on_ready_read);
    connect(clientSocket, &QTcpSocket::disconnected, this, &LogServer::on_client_disconnected);

    qInfo() << "Client connected from" << clientSocket->peerAddress().toString();

    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);

    QDir dir(QDir::currentPath());
    QStringList filters;
    filters << "*.log" << "*.txt";
    QStringList files = dir.entryList(filters, QDir::Files, QDir::Name);

    std::ranges::sort(files, [](const QString& a, const QString& b) {
        bool a_modified = a.contains(".modified.");
        bool b_modified = b.contains(".modified.");
        if (a_modified != b_modified) {
            return a_modified > b_modified;
        }
        return a < b;
    });

    out << static_cast<quint32>(0);
    out << static_cast<quint8>(MessageType::WELCOME);

    const auto& lookups = config.get_address_lookups();
    out << static_cast<quint32>(lookups.size());
    for (const auto& lookup : lookups) {
        out << lookup;
    }

    out << files;

    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));

    clientSocket->write(block);
}

void LogServer::on_client_disconnected() {
    qInfo() << "Client disconnected";
    clientSocket->deleteLater();
    clientSocket = nullptr;
}

void LogServer::on_ready_read() {
    if (!clientSocket) {
        return;
    }

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

        quint8 type_int;
        in >> type_int;
        auto type = static_cast<MessageType>(type_int);

        process_message(type, in);

        in.commitTransaction();

        if (clientSocket->bytesAvailable() == 0) {
            break;
        }
    }
}

void LogServer::process_message(MessageType type, QDataStream& in) {
    switch (type) {
        case MessageType::SELECT_FILE: {
            QString filename;
            in >> filename;

            if (filename == current_filename && processor) {
                size_t total = filterActive ? filtered_indices.size() : processor->get_entries().size();
                send_file_ready(total);
                return;
            }

            if (processor) {
                delete processor;
                processor = nullptr;
            }

            current_filename = filename;
            filterActive = false;
            filtered_indices.clear();

            processor = new LogProcessor(filename);

            // Pass config path to processor for symbol resolution
            // Use absolute path based on current working directory
            QString config_path = QDir::currentPath() + "/wosdbg.json";
            processor->set_config_path(config_path);

            connect(processor, &LogProcessor::progress_update, this, &LogServer::on_processing_progress);
            connect(processor, &LogProcessor::processing_complete, this, &LogServer::on_processing_complete);
            connect(processor, &LogProcessor::error_occurred, this, &LogServer::on_processing_error);

            processor->start_processing();
            break;
        }
        case MessageType::REQUEST_DATA: {
            int start_line;
            int count;
            in >> start_line >> count;

            if (!processor) {
                send_error("No file loaded");
                return;
            }

            const auto& entries = processor->get_entries();
            std::vector<LogEntry> result;

            size_t total = filterActive ? filtered_indices.size() : entries.size();

            if (start_line < 0 || std::cmp_greater_equal(start_line, total)) {
                send_data_response(start_line, result);
                return;
            }

            int end_line = std::min(start_line + count, static_cast<int>(total));
            result.reserve(end_line - start_line);

            for (int i = start_line; i < end_line; ++i) {
                size_t idx = filterActive ? filtered_indices[i] : i;
                result.push_back(entries[idx]);
            }

            send_data_response(start_line, result);
            break;
        }
        case MessageType::SEARCH_REQUEST: {
            QString text;
            bool is_regex;
            in >> text >> is_regex;

            if (!processor) {
                send_error("No file loaded");
                return;
            }

            const auto& entries = processor->get_entries();
            std::vector<int> matches;

            QRegularExpression regex;
            if (is_regex) {
                regex.setPattern(text);
                regex.setPatternOptions(QRegularExpression::CaseInsensitiveOption);
            } else {
                QString pattern = text;
                pattern.replace(QRegularExpression(R"(([\^\$\*\+\?\{\}\[\]\(\)\|\\]))"), "\\\\1");
                regex.setPattern(pattern);
                regex.setPatternOptions(QRegularExpression::CaseInsensitiveOption);
            }

            if (!regex.isValid()) {
                send_error("Invalid regex");
                return;
            }

            size_t total = filterActive ? filtered_indices.size() : entries.size();

            for (size_t i = 0; i < total; ++i) {
                size_t idx = filterActive ? filtered_indices[i] : i;
                const auto& entry = entries[idx];

                QString combined = QString::fromStdString(entry.address) + " " + QString::fromStdString(entry.function) + " " +
                                   QString::fromStdString(entry.hex_bytes) + " " + QString::fromStdString(entry.assembly);

                if (regex.match(combined).hasMatch()) {
                    matches.push_back(i);
                }
            }

            send_search_response(matches);
            break;
        }
        case MessageType::GET_INTERRUPTS_REQUEST: {
            if (!processor) {
                send_error("No file loaded");
                return;
            }

            const auto& entries = processor->get_entries();
            std::vector<LogEntry> interrupts;

            for (const auto& entry : entries) {
                if (entry.type == EntryType::INTERRUPT) {
                    interrupts.push_back(entry);
                }
            }

            send_interrupts_response(interrupts);
            break;
        }
        case MessageType::SET_FILTER_REQUEST: {
            bool hide_structural;
            QString interrupt_filter;
            in >> hide_structural >> interrupt_filter;
            apply_filter(hide_structural, interrupt_filter);
            break;
        }
        case MessageType::REQUEST_ROW_FOR_LINE: {
            int line_number;
            in >> line_number;

            if (!processor) {
                send_error("No file loaded");
                return;
            }

            const auto& entries = processor->get_entries();
            int row = -1;

            // Binary search could be used if entries are sorted by line number,
            // but they might not be strictly monotonic due to out-of-order execution or other factors.
            // However, they are generally sorted.
            // For now, linear search on the visible entries.

            size_t total = filterActive ? filtered_indices.size() : entries.size();

            // Optimization: Start search from an estimated position?
            // Or just linear search. Since we need exact match.

            for (size_t i = 0; i < total; ++i) {
                size_t idx = filterActive ? filtered_indices[i] : i;
                if (entries[idx].line_number == line_number) {
                    row = static_cast<int>(i);
                    break;
                }
                // Optimization: if entries are sorted by line number and we passed it
                if (entries[idx].line_number > line_number) {
                    // Assuming sorted.
                    break;
                }
            }

            send_row_for_line_response(row);
            break;
        }
        case MessageType::OPEN_SOURCE_FILE: {
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
        case MessageType::REQUEST_FILE_LIST: {
            QDir dir(QDir::currentPath());
            QStringList filters;
            filters << "*.log" << "*.txt";
            QStringList files = dir.entryList(filters, QDir::Files, QDir::Name);

            std::ranges::sort(files, [](const QString& a, const QString& b) {
                bool a_modified = a.contains(".modified.");
                bool b_modified = b.contains(".modified.");
                if (a_modified != b_modified) {
                    return a_modified > b_modified;
                }
                return a < b;
            });

            send_file_list_response(files);
            break;
        }
        case MessageType::START_MCP_SERVER: {
            QString bind_address;
            quint16 port;
            in >> bind_address >> port;
            bool ok = start_mcp_server(bind_address, port);
            send_mcp_server_status(ok ? "MCP server started" : "Failed to start MCP server");
            break;
        }
        case MessageType::STOP_MCP_SERVER: {
            stop_mcp_server();
            send_mcp_server_status("MCP server stopped");
            break;
        }
        case MessageType::MCP_SERVER_STATUS_REQUEST: {
            send_mcp_server_status();
            break;
        }
        default:
            break;
    }
}

void LogServer::send_row_for_line_response(int row) {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::ROW_FOR_LINE_RESPONSE) << row;
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    clientSocket->write(block);
}

void LogServer::apply_filter(bool hide_structural, const QString& interrupt_filter) {
    if (!processor) {
        return;
    }

    const auto& entries = processor->get_entries();
    filtered_indices.clear();
    filtered_indices.reserve(entries.size());

    bool has_interrupt_filter = !interrupt_filter.isEmpty() && interrupt_filter != "All Interrupts";

    for (size_t i = 0; i < entries.size(); ++i) {
        const auto& entry = entries[i];

        if (hide_structural) {
            if (entry.type == EntryType::SEPARATOR || entry.type == EntryType::BLOCK) {
                continue;
            }
        }

        if (has_interrupt_filter) {
            if (entry.type == EntryType::INTERRUPT) {
                if (QString::fromStdString(entry.interrupt_number) != interrupt_filter) {
                    continue;
                }
            }
        }

        filtered_indices.push_back(i);
    }

    filterActive = true;
    send_set_filter_response(filtered_indices.size());
}

void LogServer::on_processing_progress(int percentage) { send_progress(percentage); }

void LogServer::on_processing_complete() {
    qDebug() << "LogServer::onProcessingComplete";
    if (processor) {
        filterActive = false;
        filtered_indices.clear();
        send_file_ready(processor->get_entries().size());
    } else {
        qWarning() << "LogServer::onProcessingComplete: No processor!";
    }
}

void LogServer::on_processing_error(const QString& error) { send_error(error); }

void LogServer::send_progress(int percentage) {
    if (!clientSocket) {
        return;
    }
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::PROGRESS) << percentage;
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    clientSocket->write(block);
}

void LogServer::send_error(const QString& message) {
    if (!clientSocket) {
        return;
    }
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::ERROR) << message;
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    clientSocket->write(block);
}

void LogServer::send_file_ready(int total_lines) {
    if (!clientSocket) {
        qWarning() << "LogServer::sendFileReady: No client socket!";
        return;
    }
    qDebug() << "LogServer::sendFileReady: Sending FileReady with totalLines=" << total_lines;
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::FILE_READY) << total_lines;
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    clientSocket->write(block);
    clientSocket->flush();  // Ensure data is written
}

void LogServer::send_data_response(int start_line, const std::vector<LogEntry>& entries) {
    if (!clientSocket) {
        return;
    }
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::DATA_RESPONSE) << start_line << static_cast<quint32>(entries.size());
    for (const auto& entry : entries) {
        out << entry;
    }
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    clientSocket->write(block);
}

void LogServer::send_search_response(const std::vector<int>& matches) {
    if (!clientSocket) {
        return;
    }
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::SEARCH_RESPONSE) << static_cast<quint32>(matches.size());
    for (int line : matches) {
        out << line;
    }
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    clientSocket->write(block);
}

void LogServer::send_interrupts_response(const std::vector<LogEntry>& interrupts) {
    if (!clientSocket) {
        return;
    }
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::GET_INTERRUPTS_RESPONSE) << static_cast<quint32>(interrupts.size());
    for (const auto& entry : interrupts) {
        out << entry;
    }
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    clientSocket->write(block);
}

void LogServer::send_set_filter_response(int total_lines) {
    if (!clientSocket) {
        return;
    }
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::SET_FILTER_RESPONSE) << total_lines;
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    clientSocket->write(block);
}

void LogServer::send_file_list_response(const QStringList& files) {
    if (!clientSocket) {
        return;
    }
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::FILE_LIST_RESPONSE) << files;
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    clientSocket->write(block);
}

void LogServer::send_mcp_server_status(const QString& message) {
    if (!clientSocket) {
        return;
    }
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::MCP_SERVER_STATUS_RESPONSE) << is_mcp_listening() << mcp_endpoint()
        << message;
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    clientSocket->write(block);
}
