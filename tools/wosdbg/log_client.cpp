#include "log_client.h"

#include <qabstractsocket.h>
#include <qlogging.h>
#include <qobject.h>
#include <qtcpsocket.h>
#include <qtimer.h>
#include <qtmetamacros.h>
#include <qtpreprocessorsupport.h>
#include <qtypes.h>

#include <QDataStream>
#include <QDebug>
#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

#include "config.h"
#include "log_entry.h"
#include "protocol.h"

LogClient::LogClient(QObject* parent) : QObject(parent), socket(new QTcpSocket(this)) {
    connect(socket, &QTcpSocket::connected, this, &LogClient::on_connected);
    connect(socket, &QTcpSocket::readyRead, this, &LogClient::on_ready_read);
    connect(socket, &QTcpSocket::errorOccurred, this, &LogClient::on_socket_error);

    request_timer.setSingleShot(true);
    request_timer.setInterval(50);  // Debounce requests
    connect(&request_timer, &QTimer::timeout, this, &LogClient::process_pending_requests);
}

LogClient::~LogClient() { socket->disconnectFromHost(); }

void LogClient::connect_to_host(const QString& host, quint16 port) { socket->connectToHost(host, port); }

void LogClient::on_connected() { emit connected(); }

void LogClient::on_socket_error(QAbstractSocket::SocketError socket_error) {
    Q_UNUSED(socket_error);
    emit connection_error(socket->errorString());
}

void LogClient::on_ready_read() {
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
        quint8 type_int;
        in >> type_int;
        auto type = static_cast<MessageType>(type_int);

        process_message(type, in);

        in.commitTransaction();

        if (socket->bytesAvailable() == 0) {
            break;
        }
    }
}

void LogClient::process_message(MessageType type, QDataStream& in) {
    switch (type) {
        case MessageType::WELCOME: {
            // Config
            quint32 lookup_count;
            in >> lookup_count;
            config.clear_address_lookups();
            for (quint32 i = 0; i < lookup_count; ++i) {
                AddressLookup lookup;
                in >> lookup;
                config.add_address_lookup(lookup);
            }
            emit config_received();

            // File List
            in >> file_list;
            emit file_list_received(file_list);
            break;
        }
        case MessageType::FILE_READY: {
            in >> total_lines;
            qDebug() << "Client received FileReady: totalLines=" << total_lines;
            cache.clear();

            if (total_lines > 0) {
                initial_load_pending = true;
                // Request first chunk immediately to avoid flashing
                request_data(0, 2000);
            } else {
                emit file_ready(total_lines);
            }
            break;
        }
        case MessageType::PROGRESS: {
            int pct;
            in >> pct;
            emit progress(pct);
            break;
        }
        case MessageType::ERROR: {
            QString msg;
            in >> msg;
            emit error_occurred(msg);
            break;
        }
        case MessageType::DATA_RESPONSE: {
            int start_line;
            quint32 count;
            in >> start_line >> count;

            for (quint32 i = 0; i < count; ++i) {
                LogEntry entry;
                in >> entry;
                cache[start_line + i] = entry;
                // if (i == 0) qDebug() << "First entry assembly:" << QString::fromStdString(entry.assembly);
            }

            if (initial_load_pending) {
                initial_load_pending = false;
                emit file_ready(total_lines);
            }

            emit data_received(start_line, count);
            break;
        }
        case MessageType::SEARCH_RESPONSE: {
            quint32 count;
            in >> count;
            std::vector<int> matches;
            matches.reserve(count);
            for (quint32 i = 0; i < count; ++i) {
                int line;
                in >> line;
                matches.push_back(line);
            }
            emit search_results(matches);
            break;
        }
        case MessageType::GET_INTERRUPTS_RESPONSE: {
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
            emit interrupts_received(interrupts);
            break;
        }
        case MessageType::SET_FILTER_RESPONSE: {
            in >> total_lines;
            cache.clear();
            emit filter_applied(total_lines);
            break;
        }
        case MessageType::ROW_FOR_LINE_RESPONSE: {
            int row;
            in >> row;
            emit row_for_line_received(row);
            break;
        }
        case MessageType::FILE_LIST_RESPONSE: {
            in >> file_list;
            emit file_list_received(file_list);
            break;
        }
        case MessageType::MCP_SERVER_STATUS_RESPONSE: {
            bool running;
            QString endpoint;
            QString message;
            in >> running >> endpoint >> message;
            emit mcp_server_status(running, endpoint, message);
            break;
        }
        default:
            break;
    }
}

void LogClient::select_file(const QString& filename) { send_select_file(filename); }

void LogClient::set_filter(bool hide_structural, const QString& interrupt_filter) {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::SET_FILTER_REQUEST) << hide_structural << interrupt_filter;
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    socket->write(block);
}

void LogClient::request_row_for_line(int line_number) {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::REQUEST_ROW_FOR_LINE) << line_number;
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    socket->write(block);
}

void LogClient::search(const QString& text, bool is_regex) {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::SEARCH_REQUEST) << text << is_regex;
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    socket->write(block);
}

void LogClient::request_interrupts() {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::GET_INTERRUPTS_REQUEST);
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    socket->write(block);
}

void LogClient::request_open_source_file(const QString& file, int line) {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::OPEN_SOURCE_FILE) << file << line;
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    socket->write(block);
}

void LogClient::send_select_file(const QString& filename) {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::SELECT_FILE) << filename;
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    socket->write(block);
}

const LogEntry* LogClient::get_entry(int line_index) {
    auto it = cache.find(line_index);
    if (it != cache.end()) {
        return &it->second;
    }

    // Request data
    // Check if already pending?
    // For simplicity, just add to pending requests and debounce
    // Fetch a larger chunk to handle scrolling (500 before, 1500 after)
    int start = std::max(0, line_index - 500);
    int count = 2000;
    pending_requests.emplace_back(start, count);
    if (!request_timer.isActive()) {
        request_timer.start();
    }

    return nullptr;
}

void LogClient::process_pending_requests() {
    if (pending_requests.empty()) {
        return;
    }

    // Merge overlapping requests?
    // For now, just send them.
    // Optimization: Merge contiguous ranges.

    std::ranges::sort(pending_requests);

    std::vector<std::pair<int, int>> merged;
    if (!pending_requests.empty()) {
        merged.push_back(pending_requests[0]);
        for (size_t i = 1; i < pending_requests.size(); ++i) {
            auto& last = merged.back();
            auto& curr = pending_requests[i];

            if (curr.first <= last.first + last.second) {
                // Overlap or adjacent
                int new_end = std::max(last.first + last.second, curr.first + curr.second);
                last.second = new_end - last.first;
            } else {
                merged.push_back(curr);
            }
        }
    }

    for (const auto& req : merged) {
        request_data(req.first, req.second);
    }

    pending_requests.clear();
}

void LogClient::request_data(int start_line, int count) {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::REQUEST_DATA) << start_line << count;
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    socket->write(block);
}

void LogClient::request_file_list() {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::REQUEST_FILE_LIST);
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    socket->write(block);
}

void LogClient::start_mcp_server(const QString& bind_address, quint16 port) {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::START_MCP_SERVER) << bind_address << port;
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    socket->write(block);
}

void LogClient::stop_mcp_server() {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::STOP_MCP_SERVER);
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    socket->write(block);
}

void LogClient::request_mcp_server_status() {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::MCP_SERVER_STATUS_REQUEST);
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    socket->write(block);
}
