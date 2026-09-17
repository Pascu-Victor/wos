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
#include <QJsonDocument>
#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

#include "config.h"
#include "log_entry.h"
#include "protocol.h"

LogClient::LogClient(QObject* parent) : QObject(parent), socket(new QTcpSocket(this)) {
    connect(socket, &QTcpSocket::connected, this, &LogClient::on_connected);
    connect(socket, &QTcpSocket::disconnected, this, [this]() {
        cache.clear();
        in_flight.clear();
        total_lines = 0;
        view_changes = 0;
        initial_load_pending = false;
        initial_filter_pending = false;
        emit file_ready(0);
        emit disconnected();
    });
    connect(socket, &QTcpSocket::readyRead, this, &LogClient::on_ready_read);
    connect(socket, &QTcpSocket::errorOccurred, this, &LogClient::on_socket_error);
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
            in_flight.clear();
            view_changes = std::max(0, view_changes - 1);
            if (view_changes) {
                break;
            }

            if (total_lines > 0) {
                initial_load_pending = true;
                // Request first chunk immediately to avoid flashing
                prefetch_rows(0, 1);
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
            if (view_changes) {
                view_changes = 0;
                in_flight.clear();
                cache.clear();
                total_lines = 0;
                initial_load_pending = false;
                initial_filter_pending = false;
                emit file_ready(0);
            }
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
                if (!view_changes && std::abs(start_line + static_cast<int>(i) - viewport_first) < 8192) {
                    cache[start_line + i] = std::move(entry);
                }
                // if (i == 0) qDebug() << "First entry assembly:" << QString::fromStdString(entry.assembly);
            }

            in_flight.erase(start_line);
            if (view_changes) {
                break;
            }
            if (initial_load_pending && rows_ready(0, std::min(total_lines, 128))) {
                initial_load_pending = false;
                emit file_ready(total_lines);
            }
            if (initial_filter_pending && rows_ready(0, std::min(total_lines, 128))) {
                initial_filter_pending = false;
                emit filter_applied(total_lines);
            }

            emit data_received(start_line, count);
            prefetch_rows(viewport_first, viewport_count);
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
            in_flight.clear();
            view_changes = std::max(0, view_changes - 1);
            if (view_changes) {
                break;
            }
            initial_filter_pending = total_lines > 0;
            if (initial_filter_pending) {
                prefetch_rows(0, 1);
            } else {
                emit filter_applied(total_lines);
            }
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
        case MessageType::TOOL_CATALOG_RESPONSE: {
            QByteArray json;
            in >> json;
            emit tool_catalog_received(QJsonDocument::fromJson(json).object()["tools"].toArray());
            break;
        }
        case MessageType::TOOL_CALL_RESPONSE: {
            quint64 request_id;
            QByteArray json;
            in >> request_id >> json;
            emit tool_result_received(request_id, QJsonDocument::fromJson(json).object());
            break;
        }
        default:
            break;
    }
}

void LogClient::select_file(const QString& filename) { send_select_file(filename); }

void LogClient::set_filter(bool hide_structural, const QString& interrupt_filter) {
    ++view_changes;
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
    ++view_changes;
    initial_load_pending = false;
    initial_filter_pending = false;
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

    prefetch_rows(line_index, 1);

    return nullptr;
}

bool LogClient::rows_ready(int first, int count) const {
    if (view_changes) {
        return false;
    }
    for (int row = first; row < first + count; ++row) {
        if (!cache.contains(row)) {
            return false;
        }
    }
    return true;
}

void LogClient::prefetch_rows(int first, int count) {
    if (view_changes || total_lines <= 0 || !is_connected()) {
        return;
    }
    viewport_first = std::clamp(first, 0, total_lines - 1);
    viewport_count = std::clamp(count, 1, 1024);
    // Aligned, deduplicated windows. Visible data takes priority over read-ahead.
    constexpr int chunk = 1024;
    const int anchor = viewport_first / chunk * chunk;
    for (int offset : {0, 1, -1, 2, -2, 3}) {
        const int start = anchor + offset * chunk;
        if (start < 0 || start >= total_lines || in_flight.size() >= 4) {
            continue;
        }
        const int length = std::min(chunk, total_lines - start);
        if (!in_flight.contains(start) && !rows_ready(start, length)) {
            in_flight.insert(start);
            request_data(start, length);
        }
    }
    cache.erase(cache.begin(), cache.lower_bound(std::max(0, viewport_first - 8191)));
    cache.erase(cache.lower_bound(viewport_first + 8192), cache.end());
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

void LogClient::request_tool_catalog() {
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::TOOL_CATALOG_REQUEST);
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    socket->write(block);
}

auto LogClient::call_tool(const QString& name, const QJsonObject& arguments) -> quint64 {
    const quint64 REQUEST_ID = next_tool_request_id++;
    const QByteArray JSON = QJsonDocument(arguments).toJson(QJsonDocument::Compact);
    QByteArray block;
    QDataStream out(&block, QIODevice::WriteOnly);
    out.setVersion(QDataStream::Qt_6_0);
    out << static_cast<quint32>(0) << static_cast<quint8>(MessageType::TOOL_CALL_REQUEST) << REQUEST_ID << name << JSON;
    out.device()->seek(0);
    out << static_cast<quint32>(block.size() - sizeof(quint32));
    socket->write(block);
    return REQUEST_ID;
}
