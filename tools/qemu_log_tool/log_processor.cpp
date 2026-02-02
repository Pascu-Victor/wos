// Work around BFD config.h requirement
#include <qcoreapplication.h>
#include <qoverload.h>
#include <qprocess.h>
#include <qthread.h>
#include <qtmetamacros.h>
#include <qtypes.h>

#include <vector>
#define PACKAGE "qemu_log_viewer"
#define PACKAGE_VERSION "1.0"
extern "C" {
#include <bfd.h>
}
#include <capstone/capstone.h>
#include <cxxabi.h>

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
#include "qemu_log_viewer.h"

// LogProcessor implementation
LogProcessor::LogProcessor(const QString& filename, QObject* parent)
    : QObject(parent), filename(filename), completedWorkers(0), totalWorkers(0) {
    if (!tempDir.isValid()) {
        qDebug() << "Failed to create temporary directory";
    }
}

void LogProcessor::startProcessing() {
    emit progressUpdate(0);

    splitFileIntoChunks();

    startWorkerProcesses();
}

void LogProcessor::splitFileIntoChunks() {
    QFile file(filename);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) {
        emit errorOccurred(QString("Cannot open file: %1").arg(filename));
        return;
    }

    // Determine number of worker processes
    totalWorkers = std::max(4, QThread::idealThreadCount());

    // Get file size for better chunk estimation
    qint64 fileSize = file.size();
    qint64 targetChunkSize = fileSize / totalWorkers;

    // Pre-allocate chunk files
    std::vector<QFile*> chunkFiles;
    std::vector<QTextStream*> chunkStreams;
    chunkFiles.reserve(totalWorkers);
    chunkStreams.reserve(totalWorkers);

    for (int i = 0; i < totalWorkers; ++i) {
        QString chunkFile = tempDir.filePath(QString("chunk_%1.txt").arg(i));
        QFile* outFile = new QFile(chunkFile);
        if (outFile->open(QIODevice::WriteOnly | QIODevice::Text)) {
            chunkFiles.push_back(outFile);
            chunkStreams.push_back(new QTextStream(outFile));
        }
    }

    // Stream-based chunk distribution with smart boundary detection
    QTextStream in(&file);
    QString line;
    int currentChunk = 0;
    qint64 currentChunkBytes = 0;
    bool inInterruptBlock = false;

    // Pre-compile regex patterns for better performance
    static const QRegularExpression instructionPattern(R"(^0x[0-9a-fA-F]+:\s+)");
    static const QRegularExpression interruptPattern(R"(^(Servicing hardware INT=|check_exception))");
    static const QRegularExpression cpuStatePattern(
        R"(RAX=|RBX=|RCX=|RDX=|RSI=|RDI=|RBP=|RSP=|R\d+=|RIP=|RFL=|[CEDFGS]S =|LDT=|TR =|[GI]DT=|CR[0234]=|DR[0-7]=|CC[CDs]=|EFER=|^\s*\d+:\s+v=)");

    while (!in.atEnd()) {
        line = in.readLine();
        if (line.isEmpty()) {
            continue;
        }

        QString trimmed = line.trimmed();
        qint64 lineBytes = line.length() + 1;  // +1 for newline

        // Detect interrupt/exception blocks
        if (interruptPattern.match(trimmed).hasMatch()) {
            inInterruptBlock = true;
        }

        // End interrupt block detection
        if (inInterruptBlock && !trimmed.isEmpty()) {
            if (!cpuStatePattern.match(trimmed).hasMatch() && !interruptPattern.match(trimmed).hasMatch() && !trimmed.startsWith("IN:") &&
                !trimmed.startsWith("----")) {
                inInterruptBlock = false;
            }
        }

        // Smart chunk boundary: complete instruction boundary
        bool isInstructionStart = instructionPattern.match(trimmed).hasMatch();
        bool shouldSwitchChunk =
            (currentChunk < totalWorkers - 1 && currentChunkBytes >= targetChunkSize && !inInterruptBlock && isInstructionStart);

        if (shouldSwitchChunk) {
            currentChunk++;
            currentChunkBytes = 0;
        }

        // Write to current chunk
        *chunkStreams[currentChunk] << line << "\n";
        currentChunkBytes += lineBytes;
    }

    // Cleanup
    for (auto* stream : chunkStreams) {
        delete stream;
    }
    for (auto* file : chunkFiles) {
        file->close();
        delete file;
    }

    file.close();
}

void LogProcessor::startWorkerProcesses() {
    workers.clear();
    completedWorkers = 0;

    for (int i = 0; i < totalWorkers; ++i) {
        QString chunkFile = tempDir.filePath(QString("chunk_%1.txt").arg(i));
        QString resultFile = tempDir.filePath(QString("result_%1.json").arg(i));

        auto* worker = new QProcess(this);
        workers.push_back(worker);

        connect(worker, QOverload<int, QProcess::ExitStatus>::of(&QProcess::finished), this, &LogProcessor::onWorkerFinished);
        connect(worker, &QProcess::errorOccurred, this, &LogProcessor::onWorkerError);

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
        QString workerPath = QCoreApplication::applicationDirPath() + "/log_worker";
        QStringList args = {chunkFile, resultFile};

        // Pass config path to worker if available
        if (!configPath.isEmpty()) {
            args << configPath;
        }

        worker->start(workerPath, args);
    }
}

void LogProcessor::onWorkerFinished(int exitCode, QProcess::ExitStatus exitStatus) {
    (void)exitCode;
    (void)exitStatus;
    completedWorkers++;

    // Update progress
    int progress = (completedWorkers * 90) / totalWorkers;  // Reserve 10% for merging
    emit progressUpdate(progress);

    if (completedWorkers == totalWorkers) {
        // All workers finished, merge results
        qDebug() << "All workers finished. Merging results...";
        mergeResults();
    }
}

void LogProcessor::onWorkerError(QProcess::ProcessError error) {
    emit errorOccurred(QString("Worker process error: %1").arg(static_cast<int>(error)));
}

void LogProcessor::mergeResults() {
    entries.clear();
    entries.reserve(100000);  // Pre-allocate for typical log sizes

    // Process chunks in order with efficient line renumbering
    int globalLineNumber = 1;

    for (int i = 0; i < totalWorkers; ++i) {
        QString resultFile = tempDir.filePath(QString("result_%1.json").arg(i));
        QFile file(resultFile);

        if (!file.open(QIODevice::ReadOnly)) {
            continue;
        }

        QByteArray data = file.readAll();
        QJsonDocument doc = QJsonDocument::fromJson(data);
        QJsonArray array = doc.array();

        qDebug() << "Chunk" << i << "entries:" << array.size();

        file.close();

        // Process entries from this chunk
        std::vector<LogEntry> chunkEntries;
        chunkEntries.reserve(array.size());

        for (const QJsonValue& value : array) {
            chunkEntries.push_back(parseLogEntryFromJson(value.toObject()));
        }

        // Sort by worker-local line numbers only if needed (optimization: typically already ordered)
        if (!chunkEntries.empty() && chunkEntries.size() > 1) {
            bool needsSort = false;
            for (size_t j = 1; j < chunkEntries.size(); ++j) {
                if (chunkEntries[j].lineNumber < chunkEntries[j - 1].lineNumber) {
                    needsSort = true;
                    break;
                }
            }

            if (needsSort) {
                std::sort(chunkEntries.begin(), chunkEntries.end(),
                          [](const LogEntry& a, const LogEntry& b) { return a.lineNumber < b.lineNumber; });
            }
        }

        // Renumber with global line numbers (move semantics to avoid copying)
        for (auto& entry : chunkEntries) {
            entry.lineNumber = globalLineNumber++;

            // Renumber child entries
            for (auto& child : entry.childEntries) {
                child.lineNumber = globalLineNumber++;
            }

            entries.push_back(std::move(entry));
        }
    }

    // Initialize visible entries (default: show all)
    visibleEntries.clear();
    visibleEntries.reserve(entries.size());
    for (const auto& entry : entries) {
        visibleEntries.push_back(&entry);
    }

    emit progressUpdate(100);
    qDebug() << "Processing complete. Total entries:" << entries.size();
    emit processingComplete();
}

void LogProcessor::setFilter(bool hideStructural, const QString& interruptFilter) {
    visibleEntries.clear();
    visibleEntries.reserve(entries.size());

    std::string interruptFilterStd = interruptFilter.toStdString();
    bool filterInterrupts = !interruptFilter.isEmpty();

    for (const auto& entry : entries) {
        // Structural filtering
        if (hideStructural) {
            if (entry.type == EntryType::SEPARATOR || entry.type == EntryType::BLOCK || entry.type == EntryType::OTHER) {
                continue;
            }
        }

        // Interrupt filtering (if enabled)
        if (filterInterrupts) {
            // If filtering by interrupt, we only show entries that match the interrupt
            // Note: This changes behavior from original viewer which only navigated.
            // But for a remote viewer, filtering is more bandwidth efficient.
            if (entry.type != EntryType::INTERRUPT || entry.interruptNumber != interruptFilterStd) {
                continue;
            }
        }

        visibleEntries.push_back(&entry);
    }
}

auto LogProcessor::parseLogEntryFromJson(const QJsonObject& json) -> LogEntry {
    LogEntry entry;

    entry.lineNumber = json["lineNumber"].toInt();
    entry.type = static_cast<EntryType>(json["type"].toInt());
    entry.address = json["address"].toString().toStdString();
    entry.function = json["function"].toString().toStdString();
    entry.hexBytes = json["hexBytes"].toString().toStdString();
    entry.assembly = json["assembly"].toString().toStdString();
    entry.originalLine = json["originalLine"].toString().toStdString();
    entry.sourceFile = json["sourceFile"].toString().toStdString();
    entry.sourceLine = json["sourceLine"].toInt();
    entry.addressValue = static_cast<uint64_t>(json["addressValue"].toVariant().toULongLong());
    entry.isExpanded = json["isExpanded"].toBool();
    entry.isChild = json["isChild"].toBool();
    entry.interruptNumber = json["interruptNumber"].toString().toStdString();
    entry.cpuStateInfo = json["cpuStateInfo"].toString().toStdString();

    // Parse child entries
    QJsonArray childArray = json["childEntries"].toArray();
    for (const auto& childValue : childArray) {
        LogEntry child = parseLogEntryFromJson(childValue.toObject());
        entry.childEntries.push_back(child);
    }

    return entry;
}
