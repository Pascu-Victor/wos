#pragma once

#include <qcontainerfwd.h>
#include <qtmetamacros.h>
#include <qtypes.h>

#include <QByteArray>
#include <QHash>
#include <QHostAddress>
#include <QJsonObject>
#include <QObject>
#include <QTcpServer>
#include <QTcpSocket>
#include <optional>

class DebugAnalysisService;

class McpHttpServer : public QObject {
    Q_OBJECT

   public:
    explicit McpHttpServer(DebugAnalysisService* analysis, QObject* parent = nullptr);
    ~McpHttpServer() override;

    auto start(const QString& server_bind_address, quint16 port) -> bool;
    void stop();
    [[nodiscard]] auto is_listening() const -> bool { return tcp_server.isListening(); }
    [[nodiscard]] auto port() const -> quint16 { return tcp_server.serverPort(); }
    [[nodiscard]] auto bind_address() const -> QString { return server_bind_address; }
    [[nodiscard]] auto endpoint() const -> QString;
    void set_allowed_cidrs(const QStringList& cidrs) { allowed_cidrs = cidrs; }

   private slots:
    void on_new_connection();
    void on_ready_read();

   private:
    struct HttpRequest {
        QString method;
        QString path;
        QHash<QString, QByteArray> headers;
        QByteArray body;
    };

    struct SessionState {
        QByteArray id;
        QByteArray last_event_id;
        qint64 next_event_id = 1;
        QList<QTcpSocket*> listeners;
    };

    [[nodiscard]] auto peer_allowed(const QHostAddress& address) const -> bool;
    [[nodiscard]] auto cidr_allows(const QString& cidr, const QHostAddress& address) const -> bool;
    [[nodiscard]] auto parse_http_request(QTcpSocket* socket) -> std::optional<HttpRequest>;
    void on_socket_disconnected();
    void send_json(QTcpSocket* socket, const QJsonObject& object, int status = 200, const QByteArray& protocol_version = {},
                   const QByteArray& session_id = {}, bool close_connection = true);
    void send_raw(QTcpSocket* socket, const QByteArray& body, const QByteArray& content_type, int status,
                  const QByteArray& protocol_version = {}, const QByteArray& session_id = {}, bool close_connection = true);
    void send_sse_headers(QTcpSocket* socket, const QByteArray& protocol_version = {}, const QByteArray& session_id = {});
    void send_sse_event(QTcpSocket* socket, const QByteArray& event_id, const QByteArray& event, const QByteArray& data = {});
    [[nodiscard]] auto handle_json_rpc(const QJsonObject& request) -> QJsonObject;
    [[nodiscard]] auto handle_method(const QString& method, const QJsonObject& params) -> QJsonObject;
    [[nodiscard]] auto tool_list() const -> QJsonObject;
    [[nodiscard]] auto call_tool(const QJsonObject& params) const -> QJsonObject;
    [[nodiscard]] auto json_rpc_error(const QJsonValue& id, int code, const QString& message) const -> QJsonObject;
    [[nodiscard]] auto json_rpc_result(const QJsonValue& id, const QJsonObject& result) const -> QJsonObject;
    [[nodiscard]] auto tool_result(const QJsonObject& payload) const -> QJsonObject;
    [[nodiscard]] auto create_session_id() const -> QByteArray;
    [[nodiscard]] auto has_session(const QByteArray& session_id) const -> bool;
    auto ensure_valid_session(const QString& method, const QByteArray& session_id, QTcpSocket* socket) -> bool;
    auto register_listener(const QByteArray& session_id, QTcpSocket* socket) -> bool;
    void unregister_socket(QTcpSocket* socket);
    void remove_session(const QByteArray& session_id);
    [[nodiscard]] auto negotiated_protocol(const QByteArray& header) const -> QByteArray;

    DebugAnalysisService* analysis;
    QTcpServer tcp_server;
    QString server_bind_address;
    QStringList allowed_cidrs;
    QHash<QTcpSocket*, QByteArray> buffers;
    QHash<QByteArray, SessionState> sessions;
    QHash<QTcpSocket*, QByteArray> socket_to_session;
};
