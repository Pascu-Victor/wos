#include "mcp_http_server.h"

#include <qabstractsocket.h>
#include <qcontainerfwd.h>
#include <qhostaddress.h>
#include <qjsonparseerror.h>
#include <qlist.h>
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
#include <ranges>
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
    static const bool enabled = [] {
        const char* env = std::getenv("WOSDBG_MCP_TRACE");
        if (!env) {
            return false;
        }
        const std::string_view value(env);
        return !value.empty() && value != "0" && value != "false" && value != "False" && value != "FALSE";
    }();
    return enabled;
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

[[nodiscard]] auto McpHttpServer::negotiated_protocol(const QByteArray& header) const -> QByteArray {
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

auto McpHttpServer::cidr_allows(const QString& cidr, const QHostAddress& address) const -> bool {
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
        quint32 mask = prefix == 0 ? 0 : (0xffffffffu << (32 - prefix));
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
            const auto endpoint = this->endpoint();
            QJsonObject discovery = {
                {"issuer", "wosdbg"},
                {"mcpEndpoint", endpoint.isEmpty() ? "unknown://localhost/mcp" : endpoint},
                {"supported_protocols", QJsonArray::fromStringList(supported_protocol_versions())},
            };
            send_json(socket, discovery, 200, EFFECTIVE_PROTOCOL, {});
            return;
        }

        if (request->path == "/" && request->method == "GET") {
            const QByteArray text = "wosdbg MCP endpoint: /mcp";
            send_raw(socket, text, "text/plain", 200, EFFECTIVE_PROTOCOL);
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
            sessions.insert(CREATED, SessionState{CREATED, {}, 1, {}});
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

auto McpHttpServer::json_rpc_error(const QJsonValue& id, int code, const QString& message) const -> QJsonObject {
    return QJsonObject{{"jsonrpc", "2.0"}, {"id", id}, {"error", QJsonObject{{"code", code}, {"message", message}}}};
}

auto McpHttpServer::json_rpc_result(const QJsonValue& id, const QJsonObject& result) const -> QJsonObject {
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
        return QJsonObject{{"resources", analysis->listResources()}};
    }
    if (method == "resources/templates/list") {
        return QJsonObject{{"resourceTemplates", analysis->listResourceTemplates()}};
    }
    if (method == "resources/read") {
        QString uri = params["uri"].toString();
        QJsonObject payload = analysis->readResource(uri);
        return QJsonObject{
            {"contents", QJsonArray{QJsonObject{{"uri", uri},
                                                {"mimeType", "application/json"},
                                                {"text", QString::fromUtf8(QJsonDocument(payload).toJson(QJsonDocument::Indented))}}}}};
    }
    return QJsonObject{{"_jsonrpcError", QJsonObject{{"code", -32601}, {"message", QString("Method not found: %1").arg(method)}}}};
}

auto McpHttpServer::tool_list() const -> QJsonObject {
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
                    {"description", "Summarize fault, RIP, CR2, suspicious registers, and nearby disassembly."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}}}, {"dumpId"})}},
        QJsonObject{
            {"name", "wosdbg.describe_registers"},
            {"description", "Classify trap/saved registers and describe likely pointed-to memory."},
            {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}}, {"frame", QJsonObject{{"type", "string"}}}}, {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.follow_register"},
                    {"description", "Return the best next view for a register."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"register", QJsonObject{{"type", "string"}}},
                                            {"frame", QJsonObject{{"type", "string"}}}},
                                           {"dumpId", "register"})}},
        QJsonObject{{"name", "wosdbg.search_coredump_memory"},
                    {"description", "Bounded search over present coredump segments."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"kind", QJsonObject{{"type", "string"}}},
                                            {"needle", QJsonObject{}},
                                            {"maxHits", QJsonObject{{"type", "integer"}}}},
                                           {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.find_pointers"},
                    {"description", "Find qwords that look like pointers, optionally filtered by target class."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"target", QJsonObject{{"type", "string"}}},
                                            {"maxHits", QJsonObject{{"type", "integer"}}}},
                                           {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.get_memory_context"},
                    {"description", "Return bounded annotated memory around an address or register."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"address", QJsonObject{{"type", "string"}}},
                                            {"register", QJsonObject{{"type", "string"}}}},
                                           {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.disassemble_coredump"},
                    {"description", "Disassemble around RIP, saved RIP, address, or register."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"address", QJsonObject{{"type", "string"}}},
                                            {"register", QJsonObject{{"type", "string"}}},
                                            {"instructions", QJsonObject{{"type", "integer"}}}},
                                           {"dumpId"})}},
        QJsonObject{{"name", "wosdbg.resolve_address"},
                    {"description", "Explain an address using symbols, sections, segments, and heuristics."},
                    {"inputSchema", schema({{"dumpId", QJsonObject{{"type", "string"}}},
                                            {"address", QJsonObject{{"type", "string"}}},
                                            {"register", QJsonObject{{"type", "string"}}}},
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

auto McpHttpServer::tool_result(const QJsonObject& payload) const -> QJsonObject {
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
        payload = analysis->extractCoredumps(args);
    } else if (name == "wosdbg.list_logs") {
        payload = analysis->listLogs();
    } else if (name == "wosdbg.load_log") {
        payload = analysis->loadLog(args);
    } else if (name == "wosdbg.get_log_entries") {
        payload = analysis->getLogEntries(args);
    } else if (name == "wosdbg.search_log") {
        payload = analysis->searchLog(args);
    } else if (name == "wosdbg.get_log_context") {
        payload = analysis->getLogContext(args);
    } else if (name == "wosdbg.list_coredumps") {
        payload = analysis->listCoredumps();
    } else if (name == "wosdbg.open_coredump") {
        payload = analysis->openCoredump(args);
    } else if (name == "wosdbg.get_crash_summary") {
        payload = analysis->getCrashSummary(args);
    } else if (name == "wosdbg.describe_registers") {
        payload = analysis->describeRegisters(args);
    } else if (name == "wosdbg.follow_register") {
        payload = analysis->followRegister(args);
    } else if (name == "wosdbg.search_coredump_memory") {
        payload = analysis->searchCoredumpMemory(args);
    } else if (name == "wosdbg.find_pointers") {
        payload = analysis->findPointers(args);
    } else if (name == "wosdbg.get_memory_context") {
        payload = analysis->getMemoryContext(args);
    } else if (name == "wosdbg.disassemble_coredump") {
        payload = analysis->disassembleCoredump(args);
    } else if (name == "wosdbg.resolve_address") {
        payload = analysis->resolveAddressTool(args);
    } else if (name == "wosdbg.get_source_context") {
        payload = analysis->getSourceContext(args);
    } else {
        payload = QJsonObject{{"ok", false}, {"error", QString("Unknown tool: %1").arg(name)}};
    }
    return tool_result(payload);
}
