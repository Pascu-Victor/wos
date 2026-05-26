// Work around BFD config.h requirement
#include <qcontainerfwd.h>
#include <qcoreapplication.h>
#include <qlogging.h>
#include <qobject.h>
#include <qoverload.h>
#include <qprocess.h>
#include <qthread.h>
#include <qtmetamacros.h>
#include <qtypes.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "log_entry.h"
#define PACKAGE "wosdbg"
#define PACKAGE_VERSION "1.0"
extern "C" {
#include <bfd.h>
}

#include <QtCore/QDebug>
#include <QtCore/QFile>
#include <QtCore/QIODevice>
#include <QtCore/QJsonArray>
#include <QtCore/QJsonDocument>
#include <QtCore/QJsonObject>
#include <QtCore/QRegularExpression>
#include <QtCore/QStandardPaths>
#include <QtCore/QTextStream>
#include <QtWidgets/QApplication>
#include <algorithm>

#include "log_processor.h"
#include "wosdbg.h"

// LogProcessor implementation
LogProcessor::LogProcessor(QString filename, QObject* parent) : QObject(parent), filename(std::move(filename)) {
    if (!temp_dir.isValid()) {
        qDebug() << "Failed to create temporary directory";
    }
}

void LogProcessor::start_processing() {
    emit progress_update(0);

    split_file_into_chunks();

    start_worker_processes();
}

void LogProcessor::split_file_into_chunks() {
    QFile file(filename);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) {
        emit error_occurred(QString("Cannot open file: %1").arg(filename));
        return;
    }

    // Determine number of worker processes
    total_workers = std::max(4, QThread::idealThreadCount());

    // Get file size for better chunk estimation
    qint64 file_size = file.size();
    qint64 target_chunk_size = file_size / total_workers;

    // Pre-allocate chunk files
    std::vector<QFile*> chunk_files;
    std::vector<QTextStream*> chunk_streams;
    chunk_files.reserve(total_workers);
    chunk_streams.reserve(total_workers);

    for (int i = 0; i < total_workers; ++i) {
        QString chunk_file = temp_dir.filePath(QString("chunk_%1.txt").arg(i));
        auto* out_file = new QFile(chunk_file);
        if (out_file->open(QIODevice::WriteOnly | QIODevice::Text)) {
            chunk_files.push_back(out_file);
            chunk_streams.push_back(new QTextStream(out_file));
        }
    }

    // Stream-based chunk distribution with smart boundary detection
    QTextStream in(&file);
    QString line;
    int current_chunk = 0;
    qint64 current_chunk_bytes = 0;
    bool in_interrupt_block = false;

    // Pre-compile regex patterns for better performance
    static const QRegularExpression INSTRUCTION_PATTERN(R"(^0x[0-9a-fA-F]+:\s+)");
    static const QRegularExpression INTERRUPT_PATTERN(R"(^(Servicing hardware INT=|check_exception))");
    static const QRegularExpression CPU_STATE_PATTERN(
        R"(RAX=|RBX=|RCX=|RDX=|RSI=|RDI=|RBP=|RSP=|R\d+=|RIP=|RFL=|[CEDFGS]S =|LDT=|TR =|[GI]DT=|CR[0234]=|DR[0-7]=|CC[CDs]=|EFER=|^\s*\d+:\s+v=)");

    while (!in.atEnd()) {
        line = in.readLine();
        if (line.isEmpty()) {
            continue;
        }

        QString trimmed = line.trimmed();
        qint64 line_bytes = line.length() + 1;  // +1 for newline

        // Detect interrupt/exception blocks
        if (INTERRUPT_PATTERN.match(trimmed).hasMatch()) {
            in_interrupt_block = true;
        }

        // End interrupt block detection
        if (in_interrupt_block && !trimmed.isEmpty()) {
            if (!CPU_STATE_PATTERN.match(trimmed).hasMatch() && !INTERRUPT_PATTERN.match(trimmed).hasMatch() &&
                !trimmed.startsWith("IN:") && !trimmed.startsWith("----")) {
                in_interrupt_block = false;
            }
        }

        // Smart chunk boundary: complete instruction boundary
        bool is_instruction_start = INSTRUCTION_PATTERN.match(trimmed).hasMatch();
        bool should_switch_chunk =
            (current_chunk < total_workers - 1 && current_chunk_bytes >= target_chunk_size && !in_interrupt_block && is_instruction_start);

        if (should_switch_chunk) {
            current_chunk++;
            current_chunk_bytes = 0;
        }

        // Write to current chunk
        *chunk_streams[current_chunk] << line << "\n";
        current_chunk_bytes += line_bytes;
    }

    // Cleanup
    for (auto* stream : chunk_streams) {
        delete stream;
    }
    for (auto* file : chunk_files) {
        file->close();
        delete file;
    }

    file.close();
}

void LogProcessor::start_worker_processes() {
    workers.clear();
    completed_workers = 0;

    for (int i = 0; i < total_workers; ++i) {
        QString chunk_file = temp_dir.filePath(QString("chunk_%1.txt").arg(i));
        QString result_file = temp_dir.filePath(QString("result_%1.json").arg(i));

        auto* worker = new QProcess(this);
        workers.push_back(worker);

        connect(worker, QOverload<int, QProcess::ExitStatus>::of(&QProcess::finished), this, &LogProcessor::on_worker_finished);
        connect(worker, &QProcess::errorOccurred, this, &LogProcessor::on_worker_error);

        // Forward worker output to debug console
        connect(worker, &QProcess::readyReadStandardOutput, [worker]() {
            QByteArray data = worker->readAllStandardOutput();
            qDebug() << "Worker stdout:" << data;
        });
        connect(worker, &QProcess::readyReadStandardError, [worker]() {
            QByteArray data = worker->readAllStandardError();
            qDebug() << "Worker stderr:" << data;
        });

        // Start the worker process
        QString worker_path = QCoreApplication::applicationDirPath() + "/wosdbg_worker";
        QStringList args = {chunk_file, result_file};

        // Pass config path to worker if available
        if (!config_path.isEmpty()) {
            args << config_path;
        }

        worker->start(worker_path, args);
    }
}

void LogProcessor::on_worker_finished(int exit_code, QProcess::ExitStatus exit_status) {
    (void)exit_code;
    (void)exit_status;
    completed_workers++;

    // Update progress
    int progress = (completed_workers * 90) / total_workers;  // Reserve 10% for merging
    emit progress_update(progress);

    if (completed_workers == total_workers) {
        // All workers finished, merge results
        qDebug() << "All workers finished. Merging results...";
        merge_results();
    }
}

void LogProcessor::on_worker_error(QProcess::ProcessError error) {
    emit error_occurred(QString("Worker process error: %1").arg(static_cast<int>(error)));
}

void LogProcessor::merge_results() {
    entries.clear();
    entries.reserve(100000);  // Pre-allocate for typical log sizes

    // Process chunks in order with efficient line renumbering
    int global_line_number = 1;

    for (int i = 0; i < total_workers; ++i) {
        QString result_file = temp_dir.filePath(QString("result_%1.json").arg(i));
        QFile file(result_file);

        if (!file.open(QIODevice::ReadOnly)) {
            continue;
        }

        QByteArray data = file.readAll();
        QJsonDocument doc = QJsonDocument::fromJson(data);
        QJsonArray array = doc.array();

        qDebug() << "Chunk" << i << "entries:" << array.size();

        file.close();

        // Process entries from this chunk
        std::vector<LogEntry> chunk_entries;
        chunk_entries.reserve(array.size());

        for (const auto& value : array) {
            chunk_entries.push_back(parse_log_entry_from_json(value.toObject()));
        }

        // Sort by worker-local line numbers only if needed (optimization: typically already ordered)
        if (!chunk_entries.empty() && chunk_entries.size() > 1) {
            bool needs_sort = false;
            for (size_t j = 1; j < chunk_entries.size(); ++j) {
                if (chunk_entries[j].line_number < chunk_entries[j - 1].line_number) {
                    needs_sort = true;
                    break;
                }
            }

            if (needs_sort) {
                std::ranges::sort(chunk_entries,
                                  [](const LogEntry& a, const LogEntry& b) -> bool { return a.line_number < b.line_number; });
            }
        }

        // Renumber with global line numbers (move semantics to avoid copying)
        for (auto& entry : chunk_entries) {
            entry.line_number = global_line_number++;

            // Renumber child entries
            for (auto& child : entry.child_entries) {
                child.line_number = global_line_number++;
            }

            entries.push_back(std::move(entry));
        }
    }

    // Initialize visible entries (default: show all)
    visible_entries.clear();
    visible_entries.reserve(entries.size());
    for (const auto& entry : entries) {
        visible_entries.push_back(&entry);
    }

    emit progress_update(100);
    qDebug() << "Processing complete. Total entries:" << entries.size();
    emit processing_complete();
}

void LogProcessor::set_filter(bool hide_structural, const QString& interrupt_filter) {
    visible_entries.clear();
    visible_entries.reserve(entries.size());

    std::string interrupt_filter_std = interrupt_filter.toStdString();
    bool filter_interrupts = !interrupt_filter.isEmpty();

    for (const auto& entry : entries) {
        // Structural filtering
        if (hide_structural) {
            if (entry.type == EntryType::SEPARATOR || entry.type == EntryType::BLOCK || entry.type == EntryType::OTHER) {
                continue;
            }
        }

        // Interrupt filtering (if enabled)
        if (filter_interrupts) {
            // If filtering by interrupt, we only show entries that match the interrupt
            // Note: This changes behavior from original viewer which only navigated.
            // But for a remote viewer, filtering is more bandwidth efficient.
            if (entry.type != EntryType::INTERRUPT || entry.interrupt_number != interrupt_filter_std) {
                continue;
            }
        }

        visible_entries.push_back(&entry);
    }
}

auto LogProcessor::parse_log_entry_from_json(const QJsonObject& json) -> LogEntry {
    LogEntry entry;

    entry.line_number = json["lineNumber"].toInt();
    entry.type = static_cast<EntryType>(json["type"].toInt());
    entry.address = json["address"].toString().toStdString();
    entry.function = json["function"].toString().toStdString();
    entry.hex_bytes = json["hexBytes"].toString().toStdString();
    entry.assembly = json["assembly"].toString().toStdString();
    entry.original_line = json["originalLine"].toString().toStdString();
    entry.source_file = json["sourceFile"].toString().toStdString();
    entry.source_line = json["sourceLine"].toInt();
    entry.address_value = static_cast<uint64_t>(json["addressValue"].toVariant().toULongLong());
    entry.is_expanded = json["isExpanded"].toBool();
    entry.is_child = json["isChild"].toBool();
    entry.interrupt_number = json["interruptNumber"].toString().toStdString();
    entry.cpu_state_info = json["cpuStateInfo"].toString().toStdString();

    // Parse child entries
    QJsonArray child_array = json["childEntries"].toArray();
    for (const auto& child_value : child_array) {
        LogEntry child = parse_log_entry_from_json(child_value.toObject());
        entry.child_entries.push_back(child);
    }

    return entry;
}
