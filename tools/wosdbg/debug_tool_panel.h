#pragma once

#include <QJsonArray>
#include <QJsonObject>
#include <QSet>
#include <QWidget>

class QLabel;
class QComboBox;
class QLineEdit;
class QPlainTextEdit;
class QPushButton;
class LogClient;

// Schema-driven GUI for the same catalog used by MCP and CLI. Adding a backend
// tool automatically makes it available here without frontend code changes.
class DebugToolPanel final : public QWidget {
    Q_OBJECT

   public:
    explicit DebugToolPanel(LogClient* client, QWidget* parent = nullptr);

   private slots:
    void on_catalog_received(const QJsonArray& tools);
    void on_tool_result(quint64 request_id, const QJsonObject& result);
    void rebuild_tool_list();
    void show_selected_tool();
    void execute_selected_tool();
    void copy_result() const;

   private:
    [[nodiscard]] QJsonObject selected_tool() const;
    [[nodiscard]] QJsonObject suggested_arguments(const QJsonObject& tool) const;
    void remember_context(const QJsonObject& result);

    LogClient* client;
    QJsonArray catalog;
    QJsonObject context;
    QSet<quint64> pending_requests;
    QLineEdit* filter_edit;
    QComboBox* tool_combo;
    QLabel* description_label;
    QPlainTextEdit* schema_edit;
    QPlainTextEdit* arguments_edit;
    QPlainTextEdit* result_edit;
    QPushButton* execute_button;
};
