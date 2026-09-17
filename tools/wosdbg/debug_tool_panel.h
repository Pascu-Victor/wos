#pragma once

#include <QJsonArray>
#include <QJsonObject>
#include <QWidget>

class QLabel;
class QComboBox;
class QLineEdit;
class QPushButton;
class QTreeWidget;
class QHBoxLayout;
class LogClient;
class ToolArgumentsForm;
class ToolResultView;

// Human workflow over the same catalog/dispatcher used by MCP and CLI.
class DebugToolPanel final : public QWidget {
    Q_OBJECT
   public:
    explicit DebugToolPanel(LogClient* client, QWidget* parent = nullptr);
    void select_tool(const QString& name, const QJsonObject& arguments = {});

   private:
    void rebuild_tool_list();
    void show_selected_tool();
    void execute_selected_tool();
    void update_run_state();
    void remember_context(const QJsonObject& result);
    void rebuild_followups();
    void on_tool_result(quint64 request_id, const QJsonObject& result);
    [[nodiscard]] QJsonObject selected_tool() const;
    [[nodiscard]] QJsonObject suggested_arguments(const QJsonObject& tool) const;

    LogClient* client;
    QJsonArray catalog;
    QJsonObject context;
    quint64 pending_request = 0;
    QString pending_tool;
    QString result_tool;
    QJsonObject pending_arguments;
    QString requested_tool = "wosdbg.load_incident";
    QJsonObject requested_arguments;
    QString form_tool;
    QLineEdit* filter_edit;
    QTreeWidget* tool_tree;
    QComboBox* context_combo;
    QLabel* title_label;
    QLabel* description_label;
    QLabel* status_label;
    ToolArgumentsForm* arguments_form;
    ToolResultView* result_view;
    QPushButton* execute_button;
    QHBoxLayout* followups;
};
