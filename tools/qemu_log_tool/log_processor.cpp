// Work around BFD config.h requirement
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
#include <QtCore/QRegularExpression>
#include <QtCore/QTextStream>
#include <QtCore/QJsonArray>
#include <QtCore/QJsonDocument>
#include <QtCore/QJsonObject>
#include <QtCore/QStandardPaths>
#include <QtWidgets/QApplication>
#include <fstream>
#include <iostream>
#include <sstream>

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
    
    // Split file into chunks
    splitFileIntoChunks();
    
    // Start worker processes
    startWorkerProcesses();
}

void LogProcessor::splitFileIntoChunks() {
    QFile file(filename);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) {
        emit errorOccurred(QString("Cannot open file: %1").arg(filename));
        return;
    }

    // Determine number of worker processes (use CPU cores)
    totalWorkers = QThread::idealThreadCount();
    if (totalWorkers <= 0) totalWorkers = 4;

    QTextStream in(&file);
    QStringList allLines;
    
    // Read all lines
    while (!in.atEnd()) {
        allLines.append(in.readLine());
    }
    
    // Split into contiguous chunks, but avoid splitting interrupt/exception blocks
    int totalLines = allLines.size();
    int baseChunkSize = totalLines / totalWorkers;
    
    std::vector<QStringList> chunks(totalWorkers);
    int currentWorker = 0;
    int currentChunkSize = 0;
    bool inInterruptBlock = false;
    
    for (int i = 0; i < allLines.size(); ++i) {
        const QString& line = allLines[i];
        chunks[currentWorker].append(line);
        currentChunkSize++;
        
        // Check if this line starts an interrupt/exception block
        if (line.trimmed().startsWith("Servicing hardware INT=") || 
            line.trimmed().contains("check_exception")) {
            inInterruptBlock = true;
        }
        
        // Check if this line ends an interrupt block (line that doesn't look like CPU state)
        if (inInterruptBlock && !line.trimmed().isEmpty()) {
            QString trimmed = line.trimmed();
            bool isCpuState = (trimmed.contains("RAX=") || trimmed.contains("RBX=") || 
                              trimmed.contains("ES =") || trimmed.contains("CS =") ||
                              trimmed.contains("CR0=") || trimmed.contains("DR0=") ||
                              trimmed.contains("CCS=") || trimmed.contains("EFER=") ||
                              trimmed.contains(QRegularExpression(R"(^\s*\d+:\s+v=[0-9a-fA-F]+)")));
            
            if (!isCpuState && !trimmed.startsWith("Servicing hardware INT=") && 
                !trimmed.contains("check_exception")) {
                inInterruptBlock = false;
            }
        }
        
        // Move to next chunk if we've reached the target size and we're not in an interrupt block
        if (currentWorker < totalWorkers - 1 && currentChunkSize >= baseChunkSize && !inInterruptBlock) {
            currentWorker++;
            currentChunkSize = 0;
        }
    }
    
    // Write chunks to temporary files
    for (int i = 0; i < totalWorkers; ++i) {
        QString chunkFile = tempDir.filePath(QString("chunk_%1.txt").arg(i));
        QFile outFile(chunkFile);
        if (outFile.open(QIODevice::WriteOnly | QIODevice::Text)) {
            QTextStream out(&outFile);
            for (const QString& line : chunks[i]) {
                out << line << "\n";
            }
        }
    }
}

void LogProcessor::startWorkerProcesses() {
    workers.clear();
    completedWorkers = 0;
    
    for (int i = 0; i < totalWorkers; ++i) {
        QString chunkFile = tempDir.filePath(QString("chunk_%1.txt").arg(i));
        QString resultFile = tempDir.filePath(QString("result_%1.json").arg(i));
        
        QProcess* worker = new QProcess(this);
        workers.push_back(worker);
        
        connect(worker, QOverload<int, QProcess::ExitStatus>::of(&QProcess::finished),
                this, &LogProcessor::onWorkerFinished);
        connect(worker, &QProcess::errorOccurred, this, &LogProcessor::onWorkerError);
        
        // Start the worker process
        QString workerPath = QCoreApplication::applicationDirPath() + "/log_worker";
        worker->start(workerPath, {chunkFile, resultFile});
    }
}

void LogProcessor::onWorkerFinished(int exitCode, QProcess::ExitStatus exitStatus) {
    completedWorkers++;
    
    // Update progress
    int progress = (completedWorkers * 90) / totalWorkers;  // Reserve 10% for merging
    emit progressUpdate(progress);
    
    if (completedWorkers == totalWorkers) {
        // All workers finished, merge results
        mergeResults();
    }
}

void LogProcessor::onWorkerError(QProcess::ProcessError error) {
    emit errorOccurred(QString("Worker process error: %1").arg(static_cast<int>(error)));
}

void LogProcessor::mergeResults() {
    entries.clear();
    
    // We need to process chunks in order and renumber lines sequentially
    // since each worker started counting from 1
    
    int globalLineNumber = 1;  // Global line counter across all chunks
    
    for (int i = 0; i < totalWorkers; ++i) {
        QString resultFile = tempDir.filePath(QString("result_%1.json").arg(i));
        QFile file(resultFile);
        
        if (file.open(QIODevice::ReadOnly)) {
            QByteArray data = file.readAll();
            QJsonDocument doc = QJsonDocument::fromJson(data);
            QJsonArray array = doc.array();
            
            // Process entries from this chunk in order
            std::vector<LogEntry> chunkEntries;
            for (const QJsonValue& value : array) {
                LogEntry entry = parseLogEntryFromJson(value.toObject());
                chunkEntries.push_back(entry);
            }
            
            // Sort chunk entries by their worker-local line numbers
            std::sort(chunkEntries.begin(), chunkEntries.end(), 
                     [](const LogEntry& a, const LogEntry& b) { 
                         return a.lineNumber < b.lineNumber; 
                     });
            
            // Renumber the entries with global line numbers
            for (auto& entry : chunkEntries) {
                entry.lineNumber = globalLineNumber++;
                
                // Also renumber child entries if they exist
                for (auto& child : entry.childEntries) {
                    child.lineNumber = globalLineNumber++;
                }
                
                entries.push_back(entry);
            }
        }
    }
    
    emit progressUpdate(100);
    emit processingComplete();
}

LogEntry LogProcessor::parseLogEntryFromJson(const QJsonObject& json) {
    LogEntry entry;
    
    entry.lineNumber = json["lineNumber"].toInt();
    entry.type = static_cast<EntryType>(json["type"].toInt());
    entry.address = json["address"].toString().toStdString();
    entry.function = json["function"].toString().toStdString();
    entry.hexBytes = json["hexBytes"].toString().toStdString();
    entry.assembly = json["assembly"].toString().toStdString();
    entry.originalLine = json["originalLine"].toString().toStdString();
    entry.addressValue = static_cast<uint64_t>(json["addressValue"].toVariant().toULongLong());
    entry.isExpanded = json["isExpanded"].toBool();
    entry.isChild = json["isChild"].toBool();
    entry.interruptNumber = json["interruptNumber"].toString().toStdString();
    entry.cpuStateInfo = json["cpuStateInfo"].toString().toStdString();
    
    // Parse child entries
    QJsonArray childArray = json["childEntries"].toArray();
    for (const QJsonValue& childValue : childArray) {
        LogEntry child = parseLogEntryFromJson(childValue.toObject());
        entry.childEntries.push_back(child);
    }
    
    return entry;
}

int main(int argc, char* argv[]) {
    QApplication app(argc, argv);

    app.setApplicationName("QEMU Log Viewer");
    app.setApplicationVersion("1.0");
    app.setOrganizationName("WOS Kernel Project");

    QemuLogViewer viewer;
    viewer.show();

    return app.exec();
}
