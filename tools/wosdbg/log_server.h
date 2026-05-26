#pragma once

#include <QDir>
#include <QObject>
#include <QTcpServer>
#include <QTcpSocket>

#include "config.h"
#include "protocol.h"
#include "wosdbg.h"

class DebugAnalysisService;
class LogServer : public QObject {
    Q_OBJECT

   public:
    explicit LogServer(quint16 port, QObject* parent = nullptr);
    ~LogServer();

    [[nodiscard]] bool is_listening() const { return tcp_server->isListening(); }
    [[nodiscard]] quint16 server_port() const { return tcp_server->serverPort(); }
    bool start_mcp_server(const QString& bind_address = QString(), quint16 port = 0);
    void stop_mcp_server();
    [[nodiscard]] bool is_mcp_listening() const;
    [[nodiscard]] QString mcp_endpoint() const;

   private slots:
    void on_new_connection();
    void on_ready_read();
    void on_client_disconnected();
    void on_processing_progress(int percentage);
    void on_processing_complete();
    void on_processing_error(const QString& error);

   private:
    QTcpServer* tcp_server;
    QTcpSocket* clientSocket{nullptr};
    LogProcessor* processor{nullptr};
    DebugAnalysisService* analysis_service;
    class McpHttpServer* mcp_server;
    Config config;
    QString current_filename;
    std::vector<size_t> filtered_indices;
    bool filterActive{false};

    void process_message(MessageType type, QDataStream& in);
    void send_file_list();
    void send_config();
    void send_error(const QString& message);
    void send_progress(int percentage);
    void send_file_ready(int total_lines);
    void send_data_response(int start_line, const std::vector<LogEntry>& entries);
    void send_search_response(const std::vector<int>& matches);
    void send_interrupts_response(const std::vector<LogEntry>& interrupts);
    void send_set_filter_response(int total_lines);
    void send_row_for_line_response(int row);
    void send_file_list_response(const QStringList& files);
    void send_mcp_server_status(const QString& message = QString());

    void apply_filter(bool hide_structural, const QString& interrupt_filter);
};
