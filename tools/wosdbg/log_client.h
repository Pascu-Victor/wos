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

    void connect_to_host(const QString& host, quint16 port);
    void select_file(const QString& filename);

    // Returns pointer to entry if available, nullptr otherwise.
    // If nullptr, it triggers a fetch for a chunk around this line.
    const LogEntry* get_entry(int line_index);

    [[nodiscard]] const QStringList& get_file_list() const { return file_list; }
    [[nodiscard]] const Config& get_config() const { return config; }
    [[nodiscard]] int get_total_lines() const { return total_lines; }
    [[nodiscard]] bool is_connected() const { return socket->state() == QAbstractSocket::ConnectedState; }

    void search(const QString& text, bool is_regex);
    void request_interrupts();
    void set_filter(bool hide_structural, const QString& interrupt_filter);
    void request_row_for_line(int line_number);
    void request_open_source_file(const QString& file, int line);
    void request_file_list();
    void start_mcp_server(const QString& bind_address = QString(), quint16 port = 0);
    void stop_mcp_server();
    void request_mcp_server_status();

   signals:
    void search_results(const std::vector<int>& matches);
    void interrupts_received(const std::vector<LogEntry>& interrupts);
    void filter_applied(int total_lines);
    void row_for_line_received(int row);
    void connected();
    void disconnected();
    void connection_error(const QString& error);
    void file_list_received(const QStringList& files);
    void config_received();
    void file_ready(int total_lines);
    void progress(int percentage);
    void error_occurred(const QString& error);
    void data_received(int start_line, int count);  // Signal to repaint
    void mcp_server_status(bool running, const QString& endpoint, const QString& message);

   private slots:
    void on_connected();
    void on_ready_read();
    void on_socket_error(QAbstractSocket::SocketError socket_error);
    void process_pending_requests();

   private:
    QTcpSocket* socket;
    Config config;
    QStringList file_list;
    int total_lines{0};

    // Cache: lineIndex -> LogEntry
    std::map<int, LogEntry> cache;

    // Request management
    std::vector<std::pair<int, int>> pending_requests;  // start, count
    QTimer request_timer;
    bool initial_load_pending{false};

    void process_message(MessageType type, QDataStream& in);
    void request_data(int start_line, int count);
    void send_select_file(const QString& filename);
};
