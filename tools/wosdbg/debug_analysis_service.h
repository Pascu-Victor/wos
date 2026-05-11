#pragma once

#include <QByteArray>
#include <QHash>
#include <QJsonArray>
#include <QJsonObject>
#include <QObject>
#include <QString>
#include <memory>
#include <optional>
#include <vector>

#include "coredump_parser.h"
#include "elf_symbol_resolver.h"
#include "log_entry.h"

class Config;

class DebugAnalysisService : public QObject {
    Q_OBJECT

   public:
    explicit DebugAnalysisService(QObject* parent = nullptr);

    void setConfig(const Config& config);
    void reloadConfig();

    [[nodiscard]] QJsonObject status() const;
    [[nodiscard]] QJsonObject listLogs() const;
    [[nodiscard]] QJsonObject loadLog(const QJsonObject& args);
    [[nodiscard]] QJsonObject getLogEntries(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject searchLog(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject getLogContext(const QJsonObject& args) const;

    [[nodiscard]] QJsonObject extractCoredumps(const QJsonObject& args);
    [[nodiscard]] QJsonObject listCoredumps() const;
    [[nodiscard]] QJsonObject openCoredump(const QJsonObject& args);
    [[nodiscard]] QJsonObject getCrashSummary(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject describeRegisters(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject followRegister(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject searchCoredumpMemory(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject findPointers(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject getMemoryContext(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject disassembleCoredump(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject resolveAddressTool(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject getSourceContext(const QJsonObject& args) const;

    [[nodiscard]] QJsonArray listResources() const;
    [[nodiscard]] QJsonArray listResourceTemplates() const;
    [[nodiscard]] QJsonObject readResource(const QString& uri) const;

   private:
    struct LogSession {
        QString id;
        QString path;
        QString displayName;
        std::vector<LogEntry> entries;
    };

    struct DumpSession {
        QString id;
        QString path;
        QString displayName;
        std::unique_ptr<wosdbg::CoreDump> dump;
        std::unique_ptr<wosdbg::SymbolTable> binarySymbols;
        std::unique_ptr<wosdbg::SectionMap> binarySections;
        std::unique_ptr<wosdbg::SymbolTable> embeddedSymbols;
        std::unique_ptr<wosdbg::SectionMap> embeddedSections;
        std::unique_ptr<wosdbg::SymbolTable> kernelSymbols;
        std::unique_ptr<wosdbg::SectionMap> kernelSections;
    };

    struct AddressDescription {
        uint64_t value = 0;
        QString hex;
        QString classification;
        QString confidence;
        QString description;
        QString symbol;
        QString section;
        QString source;
        int sourceLine = 0;
        int segmentIndex = -1;
        uint64_t segmentOffset = 0;
        QString segmentType;
        QString asciiPreview;
        QJsonArray qwordPreview;
        bool mapped = false;
        bool canonical = false;
    };

    const LogSession* findLogSession(const QString& id) const;
    const DumpSession* findDumpSession(const QString& id) const;
    DumpSession* findDumpSession(const QString& id);

    [[nodiscard]] QString resolvePathForRead(const QString& path, const QString& fallbackDir = QString()) const;
    [[nodiscard]] bool isPathAllowed(const QString& path) const;
    [[nodiscard]] QStringList allowedRoots() const;
    [[nodiscard]] QString makeSessionId(const QString& prefix, const QString& canonicalPath) const;

    [[nodiscard]] QJsonObject logEntryToJson(const LogEntry& entry, bool includeChildren = true) const;
    [[nodiscard]] QJsonObject logSummaryToJson(const LogSession& session) const;

    [[nodiscard]] QJsonObject coredumpSummaryToJson(const DumpSession& session) const;
    [[nodiscard]] QJsonArray coredumpRegistersToJson(const DumpSession& session, const QString& frame) const;
    [[nodiscard]] QJsonObject addressDescriptionToJson(const AddressDescription& desc) const;
    [[nodiscard]] AddressDescription describeAddress(const DumpSession& session, uint64_t value,
                                                     const QString& registerName = QString()) const;
    [[nodiscard]] QHash<QString, uint64_t> registerMap(const wosdbg::CoreDump& dump, const QString& frame) const;
    [[nodiscard]] std::vector<wosdbg::SymbolTable*> symbolTables(const DumpSession& session) const;
    [[nodiscard]] std::vector<wosdbg::SectionMap*> sectionMaps(const DumpSession& session) const;
    [[nodiscard]] std::optional<uint64_t> parseAddressValue(const QJsonValue& value) const;
    [[nodiscard]] std::optional<uint64_t> resolveAddressArgument(const DumpSession& session, const QJsonObject& args,
                                                                 const QString& defaultRegister = QString()) const;
    [[nodiscard]] QJsonObject sourceContextForPath(const QString& filePath, int line, int contextLines) const;
    [[nodiscard]] QString asciiPreview(const QByteArray& bytes, int maxLen) const;
    [[nodiscard]] QJsonArray qwordPreview(const wosdbg::CoreDump& dump, uint64_t address, int qwords) const;
    [[nodiscard]] std::optional<wosdbg::SymbolEntry> disassemblyStartSymbol(const DumpSession& session, uint64_t address) const;
    [[nodiscard]] QJsonArray disassembleAt(const DumpSession& session, uint64_t address, int instructionCount) const;

    QJsonObject toolError(const QString& message) const;
    int boundedInt(const QJsonObject& args, const QString& key, int fallback, int minValue, int maxValue) const;

    Config* config_ = nullptr;
    QHash<QString, std::shared_ptr<LogSession>> logSessions_;
    QHash<QString, std::shared_ptr<DumpSession>> dumpSessions_;
};
