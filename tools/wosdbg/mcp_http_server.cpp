#include "mcp_http_server.h"

#include <qabstractsocket.h>
#include <qcontainerfwd.h>
#include <qhostaddress.h>
#include <qjsonparseerror.h>
#include <qlist.h>
#include <qlogging.h>
#include <qnamespace.h>
#include <qobject.h>
#include <qtcpserver.h>
#include <qtcpsocket.h>
#include <qtypes.h>

#include <QDebug>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonValue>
#include <QUuid>
#include <algorithm>
#include <cstdlib>
#include <optional>
#include <string_view>

#include "debug_analysis_service.h"

namespace {

auto status_text(int status) -> QByteArray {
    switch (status) {
        case 200:
            return "OK";
        case 202:
            return "Accepted";
        case 400:
            return "Bad Request";
        case 406:
            return "Not Acceptable";
        case 403:
            return "Forbidden";
        case 404:
            return "Not Found";
        case 405:
            return "Method Not Allowed";
        default:
            return "Internal Server Error";
    }
}

auto ipv4_to_int(const QHostAddress& address, quint32* out) -> bool {
    if (address.protocol() != QAbstractSocket::IPv4Protocol) {
        return false;
    }
    *out = address.toIPv4Address();
    return true;
}

auto text_content(const QString& text) -> QJsonObject { return QJsonObject{{"type", "text"}, {"text", text}}; }

auto default_protocol_version() -> QByteArray { return "2025-11-25"; }

auto supported_protocol_versions() -> const QStringList& {
    static const QStringList VERSIONS = {"2025-11-25", "2025-06-18", "2025-03-26"};
    return VERSIONS;
}

auto negotiated_protocol_version(const QString& requested) -> QString {
    for (const auto& version : supported_protocol_versions()) {
        if (version == requested || requested.isEmpty()) {
            return requested.isEmpty() ? supported_protocol_versions().first() : version;
        }
    }
    return {};
}

auto mcp_trace_enabled() -> bool {
    static const bool ENABLED = [] {
        const char* env = std::getenv("WOSDBG_MCP_TRACE");
        if (!env) {
            return false;
        }
        const std::string_view VALUE(env);
        return !VALUE.empty() && VALUE != "0" && VALUE != "false" && VALUE != "False" && VALUE != "FALSE";
    }();
    return ENABLED;
}

}  // namespace

McpHttpServer::McpHttpServer(DebugAnalysisService* analysis, QObject* parent) : QObject(parent), analysis(analysis) {
    connect(&tcp_server, &QTcpServer::newConnection, this, &McpHttpServer::on_new_connection);
}

McpHttpServer::~McpHttpServer() { stop(); }

auto McpHttpServer::start(const QString& bind_address, quint16 port) -> bool {
    stop();
    server_bind_address = bind_address.isEmpty() ? "127.0.0.1" : bind_address;
    QHostAddress host(bind_address);
    if (host.isNull() && bind_address != "0.0.0.0") {
        host = QHostAddress::LocalHost;
        server_bind_address = "127.0.0.1";
    }
    return tcp_server.listen(host, port);
}

void McpHttpServer::stop() {
    for (auto* socket : buffers.keys()) {
        unregister_socket(socket);
        socket->disconnectFromHost();
        socket->deleteLater();
    }
    sessions.clear();
    socket_to_session.clear();
    buffers.clear();
    tcp_server.close();
}

auto McpHttpServer::endpoint() const -> QString {
    if (!is_listening()) {
        return {};
    }
    return QString("http://%1:%2/mcp").arg(server_bind_address).arg(port());
}

void McpHttpServer::on_new_connection() {
    while (tcp_server.hasPendingConnections()) {
        auto* socket = tcp_server.nextPendingConnection();
        if (mcp_trace_enabled()) {
            qInfo().noquote() << "[MCP] new connection from" << socket->peerAddress().toString() << ":" << socket->peerPort();
        }
        if (!peer_allowed(socket->peerAddress())) {
            if (mcp_trace_enabled()) {
                qInfo().noquote() << "[MCP] forbidden connection from" << socket->peerAddress().toString();
            }
            send_raw(socket, "Forbidden", "text/plain", 403, default_protocol_version());
            continue;
        }
        buffers.insert(socket, {});
        connect(socket, &QTcpSocket::readyRead, this, &McpHttpServer::on_ready_read);
        connect(socket, &QTcpSocket::disconnected, this, &McpHttpServer::on_socket_disconnected);
        connect(socket, &QTcpSocket::destroyed, this, [this, socket]() { unregister_socket(socket); });
    }
}

void McpHttpServer::on_socket_disconnected() {
    auto* socket = qobject_cast<QTcpSocket*>(sender());
    if (!socket) {
        return;
    }
    unregister_socket(socket);
}

auto McpHttpServer::create_session_id() const -> QByteArray {
    QByteArray id;
    do {
        id = QUuid::createUuid().toString(QUuid::WithoutBraces).toUtf8().replace('-', '_');
    } while (has_session(id));
    return id;
}

auto McpHttpServer::has_session(const QByteArray& session_id) const -> bool { return sessions.contains(session_id); }

auto McpHttpServer::ensure_valid_session(const QString& method, const QByteArray& session_id, QTcpSocket* socket) -> bool {
    if (!socket) {
        return false;
    }
    if (method == "initialize") {
        if (session_id.isEmpty()) {
            return true;
        }
        if (!has_session(session_id)) {
            send_json(socket, json_rpc_error(QJsonValue(), -32001, "Session not found"), 404, default_protocol_version(), session_id);
            return false;
        }
        return true;
    }
    if (session_id.isEmpty()) {
        send_json(socket, json_rpc_error(QJsonValue(), -32602, "Missing MCP-Session-Id"), 400, default_protocol_version(), session_id);
        return false;
    }
    if (!has_session(session_id)) {
        send_json(socket, json_rpc_error(QJsonValue(), -32001, "Session not found"), 404, default_protocol_version(), session_id);
        return false;
    }
    return true;
}

auto McpHttpServer::register_listener(const QByteArray& session_id, QTcpSocket* socket) -> bool {
    auto session_it = sessions.find(session_id);
    if (session_it == sessions.end() || !socket) {
        return false;
    }

    if (socket_to_session.contains(socket) && socket_to_session[socket] != session_id) {
        unregister_socket(socket);
    }
    socket_to_session[socket] = session_id;
    if (!session_it->listeners.contains(socket)) {
        session_it->listeners.push_back(socket);
    }
    return true;
}

void McpHttpServer::unregister_socket(QTcpSocket* socket) {
    if (!socket) {
        return;
    }
    buffers.remove(socket);
    const QByteArray SESSION_ID = socket_to_session.take(socket);
    if (SESSION_ID.isEmpty()) {
        return;
    }
    auto session_it = sessions.find(SESSION_ID);
    if (session_it == sessions.end()) {
        return;
    }
    session_it->listeners.removeAll(socket);
    if (session_it->listeners.isEmpty()) {
        return;
    }
}

void McpHttpServer::remove_session(const QByteArray& session_id) {
    auto session_it = sessions.find(session_id);
    if (session_it == sessions.end()) {
        return;
    }
    for (auto* listener : session_it->listeners) {
        socket_to_session.remove(listener);
        listener->disconnectFromHost();
        listener->deleteLater();
    }
    sessions.erase(session_it);
}

[[nodiscard]] auto McpHttpServer::negotiated_protocol(const QByteArray& header) -> QByteArray {
    if (header.isEmpty()) {
        return default_protocol_version();
    }
    const QStringList REQUESTED = QString::fromUtf8(header).split(',', Qt::SkipEmptyParts);
    for (const auto& value : REQUESTED) {
        const QString NORMALIZED = value.trimmed();
        if (NORMALIZED.isEmpty()) {
            continue;
        }
        if (supported_protocol_versions().contains(NORMALIZED)) {
            return NORMALIZED.toUtf8();
        }
    }
    return {};
}

auto McpHttpServer::peer_allowed(const QHostAddress& address) const -> bool {
    if (allowed_cidrs.isEmpty()) {
        return address.isLoopback();
    }
    return std::ranges::any_of(allowed_cidrs, [&](const QString& cidr) -> bool { return cidr_allows(cidr, address); });
}

auto McpHttpServer::cidr_allows(const QString& cidr, const QHostAddress& address) -> bool {
    if (cidr == "*") {
        return true;
    }
    if (!cidr.contains('/')) {
        QHostAddress exact(cidr);
        return exact == address;
    }
    const QStringList PARTS = cidr.split('/');
    if (PARTS.size() != 2) {
        return false;
    }
    QHostAddress network(PARTS[0]);
    bool ok = false;
    int prefix = PARTS[1].toInt(&ok);
    if (!ok) {
        return false;
    }
    quint32 network_v4 = 0;
    quint32 address_v4 = 0;
    if (ipv4_to_int(network, &network_v4) && ipv4_to_int(address, &address_v4)) {
        prefix = std::clamp(prefix, 0, 32);
        quint32 mask = prefix == 0 ? 0 : (0xffffffffU << (32 - prefix));
        return (network_v4 & mask) == (address_v4 & mask);
    }
    return network == address || (network.isLoopback() && address.isLoopback());
}

void McpHttpServer::on_ready_read() {
    auto* socket = qobject_cast<QTcpSocket*>(sender());
    if (!socket) {
        return;
    }
    buffers[socket].append(socket->readAll());
    while (buffers.contains(socket)) {
        auto request = parse_http_request(socket);
        if (!request) {
            return;
        }

        if (mcp_trace_enabled()) {
            qInfo().nospace() << "[MCP] request method=" << request->method.toUtf8() << " path=" << request->path
                              << " peer=" << socket->peerAddress().toString() << ":" << socket->peerPort()
                              << " session=" << request->headers.value("mcp-session-id");
        }

        const QByteArray RESPONSE_PROTOCOL = negotiated_protocol(request->headers.value("mcp-protocol-version"));
        const QByteArray EFFECTIVE_PROTOCOL = RESPONSE_PROTOCOL.isNull() ? default_protocol_version() : RESPONSE_PROTOCOL;

        if (request->path == "/.well-known/oauth-authorization-server" && request->method == "GET") {
            const auto ENDPOINT = this->endpoint();
            QJsonObject discovery = {
                {"issuer", "wosdbg"},
                {"mcpEndpoint", ENDPOINT.isEmpty() ? "unknown://localhost/mcp" : ENDPOINT},
                {"supported_protocols", QJsonArray::fromStringList(supported_protocol_versions())},
            };
            send_json(socket, discovery, 200, EFFECTIVE_PROTOCOL, {});
            return;
        }

        if (request->path == "/" && request->method == "GET") {
            const QByteArray TEXT = "wosdbg MCP endpoint: /mcp";
            send_raw(socket, TEXT, "text/plain", 200, EFFECTIVE_PROTOCOL);
            return;
        }

        if (request->path != "/mcp") {
            if (mcp_trace_enabled()) {
                qInfo().nospace() << "[MCP] request rejected: path mismatch " << request->path;
            }
            send_raw(socket, "Not found", "text/plain", 404, default_protocol_version());
            return;
        }

        if (RESPONSE_PROTOCOL.isNull()) {
            if (mcp_trace_enabled()) {
                qInfo().nospace() << "[MCP] request rejected: unsupported protocol version";
            }
            const QString REQUESTED_PROTOCOL = QString::fromUtf8(request->headers.value("mcp-protocol-version").trimmed());
            const auto UNSUPPORTED = QJsonObject{
                {"jsonrpc", "2.0"},
                {"error", QJsonObject{{"code", -32602},
                                      {"message", "Unsupported protocol version"},
                                      {"data", QJsonObject{{"supported", QJsonArray::fromStringList(supported_protocol_versions())},
                                                           {"requested", REQUESTED_PROTOCOL}}}}},
            };
            send_raw(socket, QJsonDocument(UNSUPPORTED).toJson(QJsonDocument::Compact), "application/json", 400,
                     default_protocol_version());
            return;
        }
        const QByteArray REQUEST_SESSION_ID = request->headers.value("mcp-session-id");

        if (request->method == "GET") {
            if (!REQUEST_SESSION_ID.isEmpty() && !has_session(REQUEST_SESSION_ID)) {
                send_json(socket, json_rpc_error(QJsonValue(), -32001, "Session not found"), 404, RESPONSE_PROTOCOL, QByteArray());
                return;
            }
            if (!REQUEST_SESSION_ID.isEmpty()) {
                register_listener(REQUEST_SESSION_ID, socket);
                send_sse_headers(socket, RESPONSE_PROTOCOL, REQUEST_SESSION_ID);
                send_sse_event(socket, "0", "open", {});
                return;
            }

            const QByteArray CREATED = create_session_id();
            sessions.insert(CREATED, SessionState{.id = CREATED, .last_event_id = {}, .next_event_id = 1, .listeners = {}});
            register_listener(CREATED, socket);
            send_sse_headers(socket, RESPONSE_PROTOCOL, CREATED);
            send_sse_event(socket, "0", "open", {});
            return;
        }

        if (request->method == "DELETE") {
            if (REQUEST_SESSION_ID.isEmpty()) {
                send_json(socket, json_rpc_error(QJsonValue(), -32602, "Missing MCP-Session-Id"), 400, RESPONSE_PROTOCOL);
                return;
            }
            if (!has_session(REQUEST_SESSION_ID)) {
                send_json(socket, json_rpc_error(QJsonValue(), -32001, "Session not found"), 404, RESPONSE_PROTOCOL, REQUEST_SESSION_ID);
                return;
            }
            remove_session(REQUEST_SESSION_ID);
            send_raw(socket, "{}", "application/json", 200, RESPONSE_PROTOCOL, REQUEST_SESSION_ID);
            return;
        }

        if (request->method != "POST") {
            send_raw(socket, "Only POST is supported", "text/plain", 405, RESPONSE_PROTOCOL);
            return;
        }

        QJsonParseError error;
        QJsonDocument doc = QJsonDocument::fromJson(request->body, &error);
        if (error.error != QJsonParseError::NoError || (!doc.isObject() && !doc.isArray())) {
            if (mcp_trace_enabled()) {
                qInfo() << "[MCP] request rejected: invalid JSON payload";
            }
            send_json(socket, json_rpc_error(QJsonValue(), -32700, "Parse error"), 400, RESPONSE_PROTOCOL);
            return;
        }

        QByteArray response_session;
        auto validate_session_for_method = [&](const QString& method) -> std::optional<QByteArray> {
            if (method == "initialize") {
                if (has_session(REQUEST_SESSION_ID)) {
                    return REQUEST_SESSION_ID;
                }
                return QByteArray{};
            }
            if (REQUEST_SESSION_ID.isEmpty()) {
                return QByteArray{};
            }
            if (has_session(REQUEST_SESSION_ID) && response_session.isEmpty()) {
                response_session = REQUEST_SESSION_ID;
            }
            return QByteArray{};
        };

        auto handle_request = [&](const QJsonValue& request_value, QJsonArray& responses) -> void {
            if (!request_value.isObject()) {
                if (mcp_trace_enabled()) {
                    qInfo() << "[MCP] invalid request item (not object)";
                }
                responses.append(json_rpc_error(QJsonValue(), -32600, "Invalid Request"));
                return;
            }

            QJsonObject request_object = request_value.toObject();
            const QString METHOD = request_object.value("method").toString();
            const QJsonValue REQUEST_ID = request_object.value("id");
            if (mcp_trace_enabled()) {
                if (REQUEST_ID.isString()) {
                    qInfo().nospace() << "[MCP] rpc method=" << METHOD << " id=\"" << REQUEST_ID.toString() << "\"";
                } else if (REQUEST_ID.isDouble()) {
                    qInfo().nospace() << "[MCP] rpc method=" << METHOD << " id=" << REQUEST_ID.toInt();
                } else if (REQUEST_ID.isUndefined()) {
                    qInfo().nospace() << "[MCP] rpc method=" << METHOD << " (notification)";
                } else {
                    qInfo().nospace() << "[MCP] rpc method=" << METHOD << " id=<non-scalar>";
                }
            }
            if (METHOD.isEmpty()) {
                if (mcp_trace_enabled()) {
                    qInfo() << "[MCP] request missing method";
                }
                responses.append(json_rpc_error(request_object.value("id"), -32600, "Invalid Request"));
                return;
            }

            const auto ACTIVE_SESSION = validate_session_for_method(METHOD);
            if (!ACTIVE_SESSION) {
                if (!REQUEST_SESSION_ID.isEmpty() || METHOD != "initialize") {
                    if (mcp_trace_enabled()) {
                        qInfo().nospace() << "[MCP] request denied for method=" << METHOD
                                          << " session=" << (REQUEST_SESSION_ID.isEmpty() ? "<missing>" : REQUEST_SESSION_ID);
                    }
                    responses.append(json_rpc_error(request_object.value("id"), -32001, "Session not found"));
                } else {
                    if (mcp_trace_enabled()) {
                        qInfo() << "[MCP] request denied: initialize without session id";
                    }
                    responses.append(json_rpc_error(request_object.value("id"), -32602, "Missing MCP-Session-Id"));
                }
                return;
            }
            if (request_object.value("method").toString() == "initialize") {
                response_session = *ACTIVE_SESSION;
                if (!response_session.isEmpty()) {
                    register_listener(response_session, socket);
                }
                if (!request_object.contains("id")) {
                    return;
                }
            }

            auto response = handle_json_rpc(request_object);
            if (!response.isEmpty()) {
                responses.append(response);
            }
            if (request_object.value("method").toString() != "initialize" && response_session.isEmpty() && !REQUEST_SESSION_ID.isEmpty()) {
                response_session = REQUEST_SESSION_ID;
            }
        };

        if (doc.isArray()) {
            QJsonArray responses;
            for (const auto& value : doc.array()) {
                handle_request(value, responses);
            }
            const auto& response_session_id = response_session;
            if (responses.isEmpty()) {
                send_raw(socket, QByteArray(), "application/json", 202, RESPONSE_PROTOCOL, response_session_id, false);
            } else {
                send_raw(socket, QJsonDocument(responses).toJson(QJsonDocument::Compact), "application/json", 200, RESPONSE_PROTOCOL,
                         response_session_id, false);
            }
        } else {
            QJsonArray responses;
            handle_request(doc.object(), responses);
            QJsonObject response;
            if (!responses.isEmpty()) {
                response = responses.first().toObject();
            }
            const auto& response_session_id = response_session;
            if (response.isEmpty()) {
                send_raw(socket, QByteArray(), "application/json", 202, RESPONSE_PROTOCOL, response_session_id, false);
            } else {
                send_json(socket, response, 200, RESPONSE_PROTOCOL, response_session_id, false);
            }
        }
    }
}

auto McpHttpServer::parse_http_request(QTcpSocket* socket) -> std::optional<McpHttpServer::HttpRequest> {
    QByteArray& buffer = buffers[socket];
    int header_end = buffer.indexOf("\r\n\r\n");
    if (header_end < 0) {
        return std::nullopt;
    }
    QByteArray header = buffer.left(header_end);
    QList<QByteArray> lines = header.split('\n');
    if (lines.isEmpty()) {
        return std::nullopt;
    }
    QList<QByteArray> request_line = lines[0].trimmed().split(' ');
    if (request_line.size() < 2) {
        return std::nullopt;
    }
    HttpRequest request;
    request.method = QString::fromLatin1(request_line[0]);
    request.path = QString::fromLatin1(request_line[1]).split('?').first();
    int content_length = 0;
    for (int i = 1; i < lines.size(); ++i) {
        QByteArray line = lines[i].trimmed();
        if (line.isEmpty()) {
            continue;
        }
        int colon = line.indexOf(':');
        if (colon < 0) {
            continue;
        }
        QByteArray key = line.left(colon).trimmed().toLower();
        QByteArray value = line.mid(colon + 1).trimmed();
        if (key == "content-length") {
            content_length = value.toInt();
        }
        request.headers[QString::fromLatin1(key)] = value;
    }
    int body_start = header_end + 4;
    if (buffer.size() < body_start + content_length) {
        return std::nullopt;
    }
    request.body = buffer.mid(body_start, content_length);
    buffer.remove(0, body_start + content_length);
    return request;
}

void McpHttpServer::send_json(QTcpSocket* socket, const QJsonObject& object, int status, const QByteArray& protocol_version,
                              const QByteArray& session_id, bool close_connection) {
    send_raw(socket, QJsonDocument(object).toJson(QJsonDocument::Compact), "application/json", status, protocol_version, session_id,
             close_connection);
}

void McpHttpServer::send_raw(QTcpSocket* socket, const QByteArray& body, const QByteArray& content_type, int status,
                             const QByteArray& protocol_version, const QByteArray& session_id, bool close_connection) {
    if (mcp_trace_enabled()) {
        qInfo().nospace() << "[MCP] response status=" << status << " content-type=" << content_type
                          << " session=" << (session_id.isEmpty() ? "<none>" : session_id);
    }
    QByteArray response;
    response += "HTTP/1.1 " + QByteArray::number(status) + " " + status_text(status) + "\r\n";
    response += "Content-Type: " + content_type + "\r\n";
    if (!protocol_version.isEmpty()) {
        response += "MCP-Protocol-Version: " + protocol_version + "\r\n";
    }
    if (!session_id.isEmpty()) {
        response += "MCP-Session-Id: " + session_id + "\r\n";
    }
    response += "Content-Length: " + QByteArray::number(body.size()) + "\r\n";
    response += close_connection ? "Connection: close\r\n\r\n" : "Connection: keep-alive\r\n\r\n";
    response += body;
    socket->write(response);
    if (close_connection && socket->state() == QTcpSocket::ConnectedState) {
        socket->disconnectFromHost();
    }
}

void McpHttpServer::send_sse_headers(QTcpSocket* socket, const QByteArray& protocol_version, const QByteArray& session_id) {
    QByteArray headers;
    headers += "HTTP/1.1 200 OK\r\n";
    headers += "Content-Type: text/event-stream\r\n";
    headers += "Cache-Control: no-cache\r\n";
    headers += "Connection: keep-alive\r\n";
    headers += "X-Accel-Buffering: no\r\n";
    if (!protocol_version.isEmpty()) {
        headers += "MCP-Protocol-Version: " + protocol_version + "\r\n";
    }
    if (!session_id.isEmpty()) {
        headers += "MCP-Session-Id: " + session_id + "\r\n";
    }
    headers += "\r\n";
    socket->write(headers);
}

void McpHttpServer::send_sse_event(QTcpSocket* socket, const QByteArray& event_id, const QByteArray& event, const QByteArray& data) {
    QByteArray frame;
    frame += "id: " + event_id + "\r\n";
    if (!event.isEmpty()) {
        frame += "event: " + event + "\r\n";
    }
    if (data.isEmpty()) {
        frame += "data:\r\n";
    } else {
        const QList<QByteArray> LINES = data.split('\n');
        for (const auto& line : LINES) {
            frame += "data: " + line + "\r\n";
        }
    }
    frame += "\r\n";
    socket->write(frame);
}

auto McpHttpServer::json_rpc_error(const QJsonValue& id, int code, const QString& message) -> QJsonObject {
    return QJsonObject{{"jsonrpc", "2.0"}, {"id", id}, {"error", QJsonObject{{"code", code}, {"message", message}}}};
}

auto McpHttpServer::json_rpc_result(const QJsonValue& id, const QJsonObject& result) -> QJsonObject {
    return QJsonObject{{"jsonrpc", "2.0"}, {"id", id}, {"result", result}};
}

auto McpHttpServer::handle_json_rpc(const QJsonObject& request) -> QJsonObject {
    QJsonValue id = request.value("id");
    QString method = request.value("method").toString();
    if (method.isEmpty()) {
        return json_rpc_error(id, -32600, "Invalid Request");
    }
    QJsonObject params = request.value("params").toObject();
    QJsonObject result = handle_method(method, params);
    if (!request.contains("id")) {
        return {};
    }
    if (result.contains("_jsonrpcError")) {
        QJsonObject err = result["_jsonrpcError"].toObject();
        return json_rpc_error(id, err["code"].toInt(-32603), err["message"].toString("Internal error"));
    }
    return json_rpc_result(id, result);
}

auto McpHttpServer::handle_method(const QString& method, const QJsonObject& params) -> QJsonObject {
    if (method == "initialize") {
        const QString REQUESTED_PROTOCOL = params.value("protocolVersion").toString();
        const QString PROTOCOL = negotiated_protocol_version(REQUESTED_PROTOCOL);
        if (PROTOCOL.isEmpty()) {
            return QJsonObject{
                {"_jsonrpcError", QJsonObject{{"code", -32602},
                                              {"message", "Unsupported protocol version"},
                                              {"data", QJsonObject{{"supported", QJsonArray::fromStringList(supported_protocol_versions())},
                                                                   {"requested", REQUESTED_PROTOCOL}}}}}};
        }
        return QJsonObject{{"protocolVersion", PROTOCOL},
                           {"serverInfo", QJsonObject{{"name", "wosdbg"}, {"version", "2.0"}}},
                           {"capabilities", QJsonObject{{"tools", QJsonObject{{"listChanged", false}}},
                                                        {"resources", QJsonObject{{"subscribe", false}, {"listChanged", false}}}}}};
    }
    if (method == "ping" || method == "notifications/initialized") {
        return QJsonObject{};
    }
    if (method == "tools/list") {
        return tool_list();
    }
    if (method == "tools/call") {
        return call_tool(params);
    }
    if (method == "resources/list") {
        return QJsonObject{{"resources", analysis->list_resources()}};
    }
    if (method == "resources/templates/list") {
        return QJsonObject{{"resourceTemplates", DebugAnalysisService::list_resource_templates()}};
    }
    if (method == "resources/read") {
        QString uri = params["uri"].toString();
        QJsonObject payload = analysis->read_resource(uri);
        return QJsonObject{
            {"contents", QJsonArray{QJsonObject{{"uri", uri},
                                                {"mimeType", "application/json"},
                                                {"text", QString::fromUtf8(QJsonDocument(payload).toJson(QJsonDocument::Indented))}}}}};
    }
    return QJsonObject{{"_jsonrpcError", QJsonObject{{"code", -32601}, {"message", QString("Method not found: %1").arg(method)}}}};
}

auto McpHttpServer::tool_list() -> QJsonObject {
    auto schema = [](const QJsonObject& properties, const QJsonArray& required = {}) {
        return QJsonObject{{"type", "object"}, {"properties", properties}, {"required", required}};
    };
    QJsonArray tools{
        QJsonObject{{"name", "wosdbg.status"}, {"description", "Show server status and active sessions."}, {"inputSchema", schema({})}},
        QJsonObject{{"name", "wosdbg.extract_coredumps"},
                    {"description", "Run the existing WOS coredump extraction flow."},
                    {"inputSchema", schema({{"cluster", QJsonObject{{"type", "boolean"}}}})}},
        QJsonObject{{"name", "wosdbg.list_logs"}, {"description", "List server-visible TCG execution logs."}, {"inputSchema", schema({})}},
        QJsonObject{{"name", "wosdbg.load_log"},
                    {"description", "Parse a TCG execution log."},
                    {"inputSchema", schema({{"path", QJsonObject{{"type", "string"}}}}, {"path"})}},
        QJsonObject{{"name", "wosdbg.get_log_entries"},
                    {"description", "Return a bounded page of structured log entries."},
                    {"inputSchema", schema({{"logId", QJsonObject{{"type", "string"}}},
                                            {"start", QJsonObject{{"type", "integer"}}},
                                            {"count", QJsonObject{{"type", "integer"}}}},
                                           {"logId"})}},
        QJsonObject{{"name", "wosdbg.search_log"},
                    {"description", "Search parsed log entries."},
                    {"inputSchema", schema({{"logId", QJsonObject{{"type", "string"}}},
                                            {"query", QJsonObject{{"type", "string"}}},
                                            {"regex", QJsonObject{{"type", "boolean"}}},
                                            {"maxHits", QJsonObject{{"type", "integer"}}}},
                                           {"logId", "query"})}},
        QJsonObject{{"name", "wosdbg.get_log_context"},
                    {"description", "Return nearby log context around row, line, or address."},
                    {"inputSchema", schema({{"logId", QJsonObject{{"type", "string"}}},
                                            {"row", QJsonObject{{"type", "integer"}}},
                                            {"line", QJsonObject{{"type", "integer"}}},
                                            {"address", QJsonObject{{"type", "string"}}}},
                                           {"logId"})}},
        QJsonObject{{"name", "wosdbg.list_coredumps"},
                    {"description", "List extracted WOS coredumps with parsed metadata."},
                    {"inputSchema", schema({})}},
        QJsonObject{{"name", "wosdbg.open_coredump"},
                    {"description", "Parse and cache a WOS coredump session."},
                    {"inputSchema", schema({{"path", QJsonObject{{"type", "string"}}}}, {"path"})}},
        QJsonObject{{"name", "wosdbg.get_crash_summary"},
                    {"description",
                     "Quick JSON crash summary for an opened coredump: fault metadata, suspicious registers, decoded fault "
                     "instruction, and nearby disassembly. For full/compact/text reports use wosdbg.analyze_coredump."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}}}, {"dumpId"})}},
        QJsonObject{
            {"name", "wosdbg.analyze_coredump"},
            {"description",
             "One-shot coredump report. Default mode returns full structured JSON: compact diagnosis, fault/trap/saved "
             "contexts, decoded instruction, backtrace, red-zone, stack, PTE, and disassembly. compact=true, mode=compact, "
             "or format=compact returns only {compact}. mode=text or format=text returns only {text}, a concise human "
             "first-pass report. mode=human or format=human returns both {compact} JSON and {human} text. maxFrames caps both the "
             "full backtrace and compact.topBacktrace."},
            {"inputSchema",
             schema({{"dumpId", QJsonObject{{"type", "string"}}},
                     {"maxFrames", QJsonObject{{"type", "integer"}, {"description", "Caps full backtrace and compact.topBacktrace."}}},
                     {"stackBeforeBytes", QJsonObject{{"type", "integer"}}},
                     {"stackAfterBytes", QJsonObject{{"type", "integer"}}},
                     {"compact", QJsonObject{{"type", "boolean"}, {"description", "Alias for mode=compact; returns only compact JSON."}}},
                     {"mode", QJsonObject{{"type", "string"}, {"description", "One of full/default, compact, text, or human."}}},
                     {"format", QJsonObject{{"type", "string"}, {"description", "Same semantics as mode; useful for output preference."}}}},
                    {"dumpId"})}},
        QJsonObject{
            {"name", "wosdbg.backtrace_coredump"},
            {"description",
             "Best-effort userspace backtrace. frame=trap unwinds from the fault-time trap registers; frame=saved unwinds from the "
             "scheduler/syscall saved task context. Uses frame pointers first with stack-scan fallback."},
            {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                    {"frame", QJsonObject{{"type", "string"},
                                                          {"description", "trap for fault-time registers, or saved for task context."}}},
                                    {"maxFrames", QJsonObject{{"type", "integer"}}}},
                                   {"dumpId"})}},
        QJsonObject{
            {"name", "wosdbg.decode_fault_instruction"},
            {"description", "Decode the faulting instruction at trap RIP and compute memory effective addresses from trap registers."},
            {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}}}, {"dumpId"})}},
        QJsonObject{
            {"name", "wosdbg.inspect_pte"},
            {"description",
             "Show coredump mapping/PTE information for an address or register. Defaults to CR2 when no address/register is "
             "given. Reports mapped/unmapped, physical address, segment, and user/rw/nx/cow/shared bits."},
            {"inputSchema",
             schema({{"dumpId", QJsonObject{{"type", "string"}}},
                     {"address", QJsonObject{{"type", "string"}}},
                     {"register", QJsonObject{{"type", "string"}}},
                     {"frame", QJsonObject{{"type", "string"}, {"description", "Register frame for register lookup: trap or saved."}}}},
                    {"dumpId"})}},
        QJsonObject{
            {"name", "wosdbg.annotate_stack"},
            {"description",
             "Return annotated stack qwords around RSP/RBP. frame=trap uses the fault-time stack; frame=saved uses the "
             "scheduler/syscall task stack. Highlights return addresses, pointers, red-zone slots, and suspicious small values."},
            {"inputSchema",
             schema({{"dumpId", QJsonObject{{"type", "string"}}},
                     {"frame", QJsonObject{{"type", "string"}, {"description", "trap for fault-time stack, or saved for task context."}}},
                     {"beforeBytes", QJsonObject{{"type", "integer"}}},
                     {"afterBytes", QJsonObject{{"type", "integer"}}}},
                    {"dumpId"})}},
        QJsonObject{
            {"name", "wosdbg.describe_registers"},
            {"description",
             "Classify registers and describe likely pointed-to memory. frame=trap means fault-time trap registers; frame=saved means "
             "scheduler/syscall saved task registers."},
            {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                    {"frame", QJsonObject{{"type", "string"},
                                                          {"description", "trap for fault-time registers, or saved for task context."}}}},
                                   {"dumpId"})}},
        QJsonObject{
            {"name", "wosdbg.follow_register"},
            {"description",
             "Return the best next view for a register in a chosen frame: disassembly for code-like values, memory/stack "
             "context for mapped pointers, or explanation for scalar/unmapped values."},
            {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                    {"register", QJsonObject{{"type", "string"}}},
                                    {"frame", QJsonObject{{"type", "string"},
                                                          {"description", "trap for fault-time registers, or saved for task context."}}}},
                                   {"dumpId", "register"})}},
        QJsonObject{
            {"name", "wosdbg.search_coredump_memory"},
            {"description",
             "Bounded search over present coredump segments. kind defaults to pointer; use qword/u64/uint64 with needle or value for "
             "scalar qword matches, bytes for hex bytes, ascii for text, symbol for symbolized pointers, or nonzero. scope/scanScope "
             "controls scan region: all scans every captured segment; around scans beforeBytes/afterBytes around address/register; "
             "active_stack scans the trap/saved RSP page; stack scans active stack first then remaining stack pages; all_stack scans all "
             "stack pages in address order; fault scans fault-page segments. Passing address or register without scope implies around. "
             "Results include scannedRegions so truncation/scope are auditable."},
            {"inputSchema",
             schema({{"dumpId", QJsonObject{{"type", "string"}}},
                     {"kind",
                      QJsonObject{{"type", "string"}, {"description", "pointer, symbol, qword/u64/uint64, bytes, ascii, or nonzero."}}},
                     {"needle", QJsonObject{{"description", "Search value: qword address/scalar, hex bytes, or ASCII text."}}},
                     {"value", QJsonObject{{"description", "Alias for needle for qword/u64/uint64 searches."}}},
                     {"maxHits", QJsonObject{{"type", "integer"}}},
                     {"scope", QJsonObject{{"type", "string"}, {"description", "all, around, active_stack, stack, all_stack, or fault."}}},
                     {"scanScope", QJsonObject{{"type", "string"}, {"description", "Alias for scope."}}},
                     {"maxScanBytes", QJsonObject{{"type", "integer"}}},
                     {"address", QJsonObject{{"type", "string"}}},
                     {"register", QJsonObject{{"type", "string"}}},
                     {"beforeBytes", QJsonObject{{"type", "integer"}}},
                     {"afterBytes", QJsonObject{{"type", "integer"}}}},
                    {"dumpId"})}},
        QJsonObject{
            {"name", "wosdbg.find_pointers"},
            {"description",
             "Find qwords that look like pointers. Uses the same scope/scanScope behavior as search_coredump_memory: address "
             "or register implies around; stack is active-stack-first; all_stack is literal full-stack order. target filters "
             "matched pointer destinations by class or module text, e.g. code, stack, kernel, mapped, libc, ld.so."},
            {"inputSchema",
             schema({{"dumpId", QJsonObject{{"type", "string"}}},
                     {"target",
                      QJsonObject{{"type", "string"},
                                  {"description", "Optional destination filter: code, stack, kernel, mapped, or module/path text."}}},
                     {"maxHits", QJsonObject{{"type", "integer"}}},
                     {"scope", QJsonObject{{"type", "string"}, {"description", "all, around, active_stack, stack, all_stack, or fault."}}},
                     {"scanScope", QJsonObject{{"type", "string"}, {"description", "Alias for scope."}}},
                     {"maxScanBytes", QJsonObject{{"type", "integer"}}},
                     {"address", QJsonObject{{"type", "string"}}},
                     {"register", QJsonObject{{"type", "string"}}},
                     {"beforeBytes", QJsonObject{{"type", "integer"}}},
                     {"afterBytes", QJsonObject{{"type", "integer"}}}},
                    {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.get_memory_context"},
                    {"description",
                     "Return bounded annotated memory around an address or register. Defaults to RSP when no address/register is given. "
                     "Rows annotate symbols, pointers, stack markers, suspicious small values, and redZone=true for trap red-zone slots."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"address", QJsonObject{{"type", "string"}}},
                                            {"register", QJsonObject{{"type", "string"}}}},
                                           {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.disassemble_coredump"},
                    {"description",
                     "Disassemble from the containing symbol boundary when available, otherwise from the exact address/register. Defaults "
                     "to trap RIP. The trap RIP instruction is marked in the returned rows."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"address", QJsonObject{{"type", "string"}}},
                                            {"register", QJsonObject{{"type", "string"}}},
                                            {"instructions", QJsonObject{{"type", "integer"}}}},
                                           {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.resolve_address"},
                    {"description",
                     "Explain an address/register using module discovery, symbols, source, sections, coredump segment data, and PTE "
                     "mapping. moduleFilter/module can restrict results to kernel, userspace/user, or a module name substring."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"address", QJsonObject{{"type", "string"}}},
                                            {"register", QJsonObject{{"type", "string"}}},
                                            {"moduleFilter", QJsonObject{{"type", "string"}}},
                                            {"module", QJsonObject{{"type", "string"}}}},
                                           {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.verify_embedded_elf"},
                    {"description",
                     "Compare the coredump embedded ELF buffer against a local ELF chosen by path/elfPath, the opened binary path, or "
                     "build-id lookup. Reports build IDs, hashes, first mismatch, mismatch ranges, and whether differing embedded bytes "
                     "also occur elsewhere in the local file."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"path", QJsonObject{{"type", "string"}}},
                                            {"elfPath", QJsonObject{{"type", "string"}}},
                                            {"maxRanges", QJsonObject{{"type", "integer"}}}},
                                           {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.check_elf_mapping"},
                    {"description",
                     "For a runtime VA/register, compute PIE load base, ELF vaddr, PT_LOAD segment, and expected file offset, then "
                     "compare captured coredump bytes with embedded/local ELF bytes. Good for detecting stale/wrong executable pages, "
                     "for example a page at VA X whose bytes match another ELF file offset."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"address", QJsonObject{{"type", "string"}}},
                                            {"register", QJsonObject{{"type", "string"}}},
                                            {"bytes", QJsonObject{{"type", "integer"}}},
                                            {"page", QJsonObject{{"type", "boolean"}}},
                                            {"maxRanges", QJsonObject{{"type", "integer"}}}},
                                           {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.find_duplicate_pages"},
                    {"description",
                     "Hash captured pages and report duplicate page-content groups. Defaults to executable user pages only; use "
                     "executableOnly=false or userOnly=false for broader scans. Reports regular address deltas for stale/chunk "
                     "repetition clues."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"executableOnly", QJsonObject{{"type", "boolean"}}},
                                            {"userOnly", QJsonObject{{"type", "boolean"}}},
                                            {"maxGroups", QJsonObject{{"type", "integer"}}}},
                                           {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.analyze_elf_integrity"},
                    {"description",
                     "One-shot executable-image integrity report. Runs embedded ELF verification, mapped-page-vs-ELF comparison for "
                     "RIP or a supplied address/register, and duplicate executable-page detection. Intended for remote exec/WKI VFS_REF "
                     "or stale page corruption triage."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"address", QJsonObject{{"type", "string"}}},
                                            {"register", QJsonObject{{"type", "string"}}},
                                            {"bytes", QJsonObject{{"type", "integer"}}}},
                                           {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.elf_layout_summary"},
                    {"description",
                     "Readelf-style summary for discovered modules: ELF type, entrypoint, interpreter, PT_LOAD runtime ranges, file "
                     "offsets, sizes, flags, and PIE base conversions. Optional module filters by module/role substring."},
                    {"inputSchema",
                     schema({{"dumpId", QJsonObject{{"type", "string"}}}, {"module", QJsonObject{{"type", "string"}}}}, {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.compare_expected_disassembly"},
                    {"description",
                     "Objdump-like side-by-side disassembly for a runtime VA/register: actual captured coredump bytes versus expected "
                     "module ELF bytes at the PT_LOAD-derived file offset, plus byte comparison metadata."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"address", QJsonObject{{"type", "string"}}},
                                            {"register", QJsonObject{{"type", "string"}}},
                                            {"instructions", QJsonObject{{"type", "integer"}}},
                                            {"bytes", QJsonObject{{"type", "integer"}}}},
                                           {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.scan_chunk_corruption"},
                    {"description",
                     "Scan mapped user image pages against their PT_LOAD source bytes and look for stale/wrong-page signatures aligned "
                     "to common chunk sizes such as 4K, 64K, 256K, and 2M. Reports pages whose actual bytes match another ELF offset."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"chunkSizes", QJsonObject{{"type", "array"}}},
                                            {"maxHits", QJsonObject{{"type", "integer"}}}},
                                           {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.audit_executable_ptes"},
                    {"description",
                     "Audit executable-image PTE permissions against ELF PT_LOAD flags. Flags executable pages that are NX or writable, "
                     "read-only segments mapped writable, and userspace image pages missing the user bit."},
                    {"inputSchema",
                     schema({{"dumpId", QJsonObject{{"type", "string"}}}, {"maxIssues", QJsonObject{{"type", "integer"}}}}, {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.recognize_startup_stack"},
                    {"description",
                     "Inspect stack slots near RSP for startup-shape hints such as _start and __mlibc_entry return/code pointers. Useful "
                     "for early process startup crashes."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"frame", QJsonObject{{"type", "string"}}},
                                            {"address", QJsonObject{{"type", "string"}}},
                                            {"register", QJsonObject{{"type", "string"}}}},
                                           {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.correlate_coredump_logs"},
                    {"description",
                     "Correlate an opened coredump with already-loaded logs by PID, RIP, CR2, and executable path. Call load_log first "
                     "or pass logId. Returns matching log rows for nearby fault context."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"logId", QJsonObject{{"type", "string"}}},
                                            {"maxHits", QJsonObject{{"type", "integer"}}}},
                                           {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.reconstruct_wki_trace"},
                    {"description",
                     "Search already-loaded logs for WKI/VFS/RDMA operation traces: OP_VFS_READ, OP_VFS_READ_BULK, OP_VFS_READ_RDMA, "
                     "VFS_REF, remote_vfs, remote_compute, and RDMA lines. Optional filter narrows by path, peer, cookie, or op text."},
                    {"inputSchema", schema({{"logId", QJsonObject{{"type", "string"}}},
                                            {"filter", QJsonObject{{"type", "string"}}},
                                            {"maxHits", QJsonObject{{"type", "integer"}}}})}},
        QJsonObject{
            {"name", "wosdbg.explain_remote_exec_path"},
            {"description",
             "Explain the likely source-level path for a /wki/<peer>/... remote executable: remote_compute VFS_REF, exec/VFS "
             "reads, remote_vfs/WKI operations, and ELF loader mapping. Returns next-tool suggestions."},
            {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}}, {"path", QJsonObject{{"type", "string"}}}}, {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.diagnose_remote_exec_corruption"},
                    {"description",
                     "Canned remote executable corruption analysis. Combines remote path explanation, ELF integrity checks, chunk/stale "
                     "page scan, and executable PTE audit; intended for WKI VFS_REF/RDMA/cache corruption triage."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"address", QJsonObject{{"type", "string"}}},
                                            {"register", QJsonObject{{"type", "string"}}},
                                            {"maxHits", QJsonObject{{"type", "integer"}}}},
                                           {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.get_source_context"},
                    {"description", "Return bounded source around a file and line."},
                    {"inputSchema", schema({{"path", QJsonObject{{"type", "string"}}},
                                            {"line", QJsonObject{{"type", "integer"}}},
                                            {"contextLines", QJsonObject{{"type", "integer"}}}},
                                           {"path", "line"})}},
    };
    return QJsonObject{{"tools", tools}};
}

auto McpHttpServer::tool_result(const QJsonObject& payload) -> QJsonObject {
    QString text = QString::fromUtf8(QJsonDocument(payload).toJson(QJsonDocument::Indented));
    return QJsonObject{
        {"content", QJsonArray{text_content(text)}}, {"structuredContent", payload}, {"isError", !payload["ok"].toBool(true)}};
}

auto McpHttpServer::call_tool(const QJsonObject& params) const -> QJsonObject {
    QString name = params["name"].toString();
    QJsonObject args = params["arguments"].toObject();
    QJsonObject payload;
    if (name == "wosdbg.status") {
        payload = analysis->status();
    } else if (name == "wosdbg.extract_coredumps") {
        payload = analysis->extract_coredumps(args);
    } else if (name == "wosdbg.list_logs") {
        payload = DebugAnalysisService::list_logs();
    } else if (name == "wosdbg.load_log") {
        payload = analysis->load_log(args);
    } else if (name == "wosdbg.get_log_entries") {
        payload = analysis->get_log_entries(args);
    } else if (name == "wosdbg.search_log") {
        payload = analysis->search_log(args);
    } else if (name == "wosdbg.get_log_context") {
        payload = analysis->get_log_context(args);
    } else if (name == "wosdbg.list_coredumps") {
        payload = analysis->list_coredumps();
    } else if (name == "wosdbg.open_coredump") {
        payload = analysis->open_coredump(args);
    } else if (name == "wosdbg.get_crash_summary") {
        payload = analysis->get_crash_summary(args);
    } else if (name == "wosdbg.analyze_coredump") {
        payload = analysis->analyze_coredump(args);
    } else if (name == "wosdbg.backtrace_coredump") {
        payload = analysis->backtrace_coredump(args);
    } else if (name == "wosdbg.decode_fault_instruction") {
        payload = analysis->decode_fault_instruction(args);
    } else if (name == "wosdbg.inspect_pte") {
        payload = analysis->inspect_page_table(args);
    } else if (name == "wosdbg.annotate_stack") {
        payload = analysis->annotate_stack(args);
    } else if (name == "wosdbg.describe_registers") {
        payload = analysis->describe_registers(args);
    } else if (name == "wosdbg.follow_register") {
        payload = analysis->follow_register(args);
    } else if (name == "wosdbg.search_coredump_memory") {
        payload = analysis->search_coredump_memory(args);
    } else if (name == "wosdbg.find_pointers") {
        payload = analysis->find_pointers(args);
    } else if (name == "wosdbg.get_memory_context") {
        payload = analysis->get_memory_context(args);
    } else if (name == "wosdbg.disassemble_coredump") {
        payload = analysis->disassemble_coredump(args);
    } else if (name == "wosdbg.resolve_address") {
        payload = analysis->resolve_address_tool(args);
    } else if (name == "wosdbg.verify_embedded_elf") {
        payload = analysis->verify_embedded_elf(args);
    } else if (name == "wosdbg.check_elf_mapping") {
        payload = analysis->check_elf_mapping(args);
    } else if (name == "wosdbg.find_duplicate_pages") {
        payload = analysis->find_duplicate_pages(args);
    } else if (name == "wosdbg.analyze_elf_integrity") {
        payload = analysis->analyze_elf_integrity(args);
    } else if (name == "wosdbg.elf_layout_summary") {
        payload = analysis->elf_layout_summary(args);
    } else if (name == "wosdbg.compare_expected_disassembly") {
        payload = analysis->compare_expected_disassembly(args);
    } else if (name == "wosdbg.scan_chunk_corruption") {
        payload = analysis->scan_chunk_corruption(args);
    } else if (name == "wosdbg.audit_executable_ptes") {
        payload = analysis->audit_executable_ptes(args);
    } else if (name == "wosdbg.recognize_startup_stack") {
        payload = analysis->recognize_startup_stack(args);
    } else if (name == "wosdbg.correlate_coredump_logs") {
        payload = analysis->correlate_coredump_logs(args);
    } else if (name == "wosdbg.reconstruct_wki_trace") {
        payload = analysis->reconstruct_wki_trace(args);
    } else if (name == "wosdbg.explain_remote_exec_path") {
        payload = analysis->explain_remote_exec_path(args);
    } else if (name == "wosdbg.diagnose_remote_exec_corruption") {
        payload = analysis->diagnose_remote_exec_corruption(args);
    } else if (name == "wosdbg.get_source_context") {
        payload = analysis->get_source_context(args);
    } else {
        payload = QJsonObject{{"ok", false}, {"error", QString("Unknown tool: %1").arg(name)}};
    }
    return tool_result(payload);
}
