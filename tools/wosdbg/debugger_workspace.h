#pragma once

#include <QElapsedTimer>
#include <QHash>
#include <QJsonObject>
#include <QWidget>
#include <functional>

class LogClient;
class QLabel;
class QPushButton;
class QLineEdit;
class QComboBox;
class QTableWidget;
class QTabWidget;
class QStackedWidget;
class QPlainTextEdit;

// Human debugger workflows. Protocol/session details stay in this controller, not in forms.
class DebuggerWorkspace : public QWidget {
    Q_OBJECT
   public:
    explicit DebuggerWorkspace(LogClient* client, QWidget* parent = nullptr);
    void open_path(const QString& kind, const QString& path);
    void choose_file(const QString& kind);
    void show_targets();
    void refresh_evidence();
    void focus_view(const QString& name);
    QJsonObject context() const;
   signals:
    void log_requested(const QString& path);
    void advanced_requested(const QString& tool, const QJsonObject& context);

   private:
    using Reply = std::function<void(const QJsonObject&)>;
    LogClient* client;
    QHash<quint64, Reply> requests;
    QStackedWidget* pages;
    QTableWidget* evidence;
    QTableWidget* targets;
    QTableWidget* registers;
    QTableWidget* stack;
    QTableWidget* code;
    QTableWidget* memory;
    QTableWidget* modules;
    QTableWidget* inventory;
    QTableWidget* timeline;
    QPlainTextEdit* source;
    QTabWidget* code_tabs;
    QLabel* notice;
    QLabel* title;
    QLabel* summary;
    QLabel* target_hint;
    QLabel* lease_label;
    QPushButton* enable;
    QPushButton* attach;
    QPushButton* detach;
    QPushButton* renew;
    QPushButton* read_memory;
    QPushButton* back;
    QPushButton* forward;
    QLineEdit* address;
    QComboBox* frame;
    QString dump_id;
    QString incident_id;
    QString session_id;
    QString lease_token;
    QString target_id;
    QElapsedTimer lease_clock;
    int lease_ms = 0;
    int generation = 0;
    int seek_generation = 0;
    bool attach_pending = false;
    QStringList address_history;
    int history_index = -1;
    bool traversing_history = false;

    void send(const QString& tool, const QJsonObject& args, Reply reply);
    bool accept(const QJsonObject& result, const QString& action);
    void show_dump(const QString& id, const QJsonObject& info);
    void inspect();
    void seek(const QString& location);
    void read_live_memory();
    void discover();
    void attach_target();
    void update_lease(const QJsonObject& result);
    void clear_live();
};
