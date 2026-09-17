#pragma once

#include <QJsonArray>
#include <QJsonObject>
#include <QWidget>

class QLabel;
class QLineEdit;
class QPlainTextEdit;
class QTabWidget;
class QTableWidget;
class QTreeWidget;
class QTreeWidgetItem;
class QPushButton;

class ToolResultView final : public QWidget {
    Q_OBJECT
   public:
    explicit ToolResultView(QWidget* parent = nullptr);
    void set_result(const QJsonObject& result, const QString& action_title = {});
    [[nodiscard]] QJsonObject result() const { return payload; }

   signals:
    void context_selected(const QJsonObject& context);

   private:
    void populate(QTreeWidgetItem* parent, const QJsonValue& value);
    void show_table(const QJsonArray& rows);
    void render_page();
    void filter_rows();
    QJsonObject payload;
    QJsonArray table_rows;
    QString table_collection;
    int page_start = 0;
    QPushButton* previous_page;
    QPushButton* next_page;
    QTreeWidgetItem* inspected_row = nullptr;
    QTreeWidget* tree;
    QTableWidget* table;
    QTabWidget* tabs;
    QLineEdit* filter;
    QLabel* table_status;
    QPlainTextEdit* raw;
    QPlainTextEdit* detail;
    QLabel* summary;
};
