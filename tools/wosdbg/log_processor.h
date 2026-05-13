#pragma once

#include <qtmetamacros.h>

#include <QJsonObject>
#include <QObject>
#include <QProcess>
#include <QString>
#include <QTemporaryDir>
#include <cstddef>
#include <vector>

#include "log_entry.h"

class LogProcessor : public QObject {
    Q_OBJECT

   public:
    LogProcessor(QString filename, QObject* parent = nullptr);
    void start_processing();

    // Set the config file path for symbol resolution
    void set_config_path(const QString& path) { config_path = path; }
    [[nodiscard]] std::vector<LogEntry> get_entries() const { return entries; }

    // Filtering support
    void set_filter(bool hide_structural, const QString& interrupt_filter);
    [[nodiscard]] size_t get_visible_entry_count() const { return visible_entries.size(); }
    [[nodiscard]] const LogEntry* get_visible_entry(size_t index) const {
        if (index < visible_entries.size()) {
            return visible_entries[index];
        }
        return nullptr;
    }
    [[nodiscard]] auto get_visible_entries() const -> const std::vector<const LogEntry*>& { return visible_entries; }

   signals:
    void progress_update(int percentage);
    void processing_complete();
    void error_occurred(const QString& error);

   private slots:
    void on_worker_finished(int exit_code, QProcess::ExitStatus exit_status);
    void on_worker_error(QProcess::ProcessError error);

   private:
    QString filename;
    QString config_path;  // Path to config file for symbol resolution
    std::vector<LogEntry> entries;
    std::vector<const LogEntry*> visible_entries;
    QTemporaryDir temp_dir;
    std::vector<QProcess*> workers;
    int completed_workers{0};
    int total_workers{0};

    void split_file_into_chunks();
    void start_worker_processes();
    void merge_results();
    auto parse_log_entry_from_json(const QJsonObject& json) -> LogEntry;
};
