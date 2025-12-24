#pragma once

#include <QJsonObject>
#include <QObject>
#include <QProcess>
#include <QString>
#include <QTemporaryDir>
#include <vector>

#include "log_entry.h"

class LogProcessor : public QObject {
    Q_OBJECT

   public:
    LogProcessor(const QString& filename, QObject* parent = nullptr);
    void startProcessing();
    std::vector<LogEntry> getEntries() const { return entries; }

    // Filtering support
    void setFilter(bool hideStructural, const QString& interruptFilter);
    size_t getVisibleEntryCount() const { return visibleEntries.size(); }
    const LogEntry* getVisibleEntry(size_t index) const {
        if (index < visibleEntries.size()) return visibleEntries[index];
        return nullptr;
    }
    const std::vector<const LogEntry*>& getVisibleEntries() const { return visibleEntries; }

   signals:
    void progressUpdate(int percentage);
    void processingComplete();
    void errorOccurred(const QString& error);

   private slots:
    void onWorkerFinished(int exitCode, QProcess::ExitStatus exitStatus);
    void onWorkerError(QProcess::ProcessError error);

   private:
    QString filename;
    std::vector<LogEntry> entries;
    std::vector<const LogEntry*> visibleEntries;
    QTemporaryDir tempDir;
    std::vector<QProcess*> workers;
    int completedWorkers;
    int totalWorkers;

    void splitFileIntoChunks();
    void startWorkerProcesses();
    void mergeResults();
    LogEntry parseLogEntryFromJson(const QJsonObject& json);
};
