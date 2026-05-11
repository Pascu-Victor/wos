#include "debug_analysis_service.h"

#include <capstone/capstone.h>
#include <qcontainerfwd.h>
#include <qhash.h>
#include <qhashfunctions.h>
#include <qnamespace.h>
#include <qobject.h>
#include <qtypes.h>

#include <QCoreApplication>
#include <QCryptographicHash>
#include <QDir>
#include <QDirIterator>
#include <QEventLoop>
#include <QFile>
#include <QFileInfo>
#include <QJsonDocument>
#include <QProcess>
#include <QRegularExpression>
#include <QTextStream>
#include <QTimer>
#include <QUrl>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "config.h"
#include "coredump_memory.h"
#include "coredump_parser.h"
#include "elf_symbol_resolver.h"
#include "log_entry.h"
#include "log_processor.h"

namespace {

constexpr int kDefaultMaxStringLength = 160;
constexpr qsizetype kMaxCapturedProcessOutput = 256 * 1024;
constexpr uint64_t kMaxMemorySearchBytes = 64ULL * 1024ULL * 1024ULL;
constexpr uint64_t kMaxDisassemblyFunctionBytes = 128ULL * 1024ULL;
constexpr uint64_t kDisassemblyBytesAfterTarget = 512;

auto formatHex(uint64_t value) -> QString { return wosdbg::formatU64(value); }

auto entryTypeName(EntryType type) -> QString {
    switch (type) {
        case EntryType::INSTRUCTION:
            return "instruction";
        case EntryType::INTERRUPT:
            return "interrupt";
        case EntryType::REGISTER:
            return "register";
        case EntryType::BLOCK:
            return "block";
        case EntryType::SEPARATOR:
            return "separator";
        case EntryType::OTHER:
            return "other";
    }
    return "other";
}

auto canonicalPathOrAbsolute(const QString& path) -> QString {
    QFileInfo info(path);
    QString canonical = info.canonicalFilePath();
    if (!canonical.isEmpty()) {
        return canonical;
    }
    return info.absoluteFilePath();
}

auto startsWithPathRoot(const QString& path, const QString& root) -> bool {
    QString cleanPath = QDir::cleanPath(path);
    QString cleanRoot = QDir::cleanPath(root);
    if (cleanPath == cleanRoot) {
        return true;
    }
    if (!cleanRoot.endsWith('/')) {
        cleanRoot += '/';
    }
    return cleanPath.startsWith(cleanRoot);
}

auto isCanonicalX86Address(uint64_t value) -> bool {
    const bool high = (value & (1ULL << 47)) != 0;
    const uint64_t top = value >> 48;
    return high ? top == 0xffffULL : top == 0;
}

auto readU64Le(const char* data) -> uint64_t {
    uint64_t value = 0;
    std::memcpy(&value, data, sizeof(value));
    return value;
}

auto bytesFromHex(const QString& hexText) -> QByteArray {
    QString hex = hexText;
    hex.remove(QRegularExpression(R"([^0-9a-fA-F])"));
    if (hex.size() % 2 != 0) {
        hex.prepend('0');
    }
    QByteArray bytes;
    bytes.reserve(hex.size() / 2);
    for (int i = 0; i + 1 < hex.size(); i += 2) {
        bool ok = false;
        auto byte = static_cast<char>(hex.mid(i, 2).toUInt(&ok, 16));
        if (!ok) {
            return {};
        }
        bytes.append(byte);
    }
    return bytes;
}

}  // namespace

DebugAnalysisService::DebugAnalysisService(QObject* parent) : QObject(parent) {}

void DebugAnalysisService::setConfig(const Config& config) { config_ = const_cast<Config*>(&config); }

void DebugAnalysisService::reloadConfig() {
    ConfigService::instance().reload();
    config_ = &ConfigService::instance().getMutableConfig();
}

auto DebugAnalysisService::toolError(const QString& message) const -> QJsonObject { return QJsonObject{{"ok", false}, {"error", message}}; }

auto DebugAnalysisService::status() const -> QJsonObject {
    const Config& cfg = config_ ? *config_ : ConfigService::instance().getConfig();
    QJsonArray logs;
    for (auto it = logSessions_.cbegin(); it != logSessions_.cend(); ++it) {
        logs.append(QJsonObject{{"id", it.key()}, {"path", it.value()->path}, {"entries", static_cast<int>(it.value()->entries.size())}});
    }

    QJsonArray dumps;
    for (auto it = dumpSessions_.cbegin(); it != dumpSessions_.cend(); ++it) {
        dumps.append(QJsonObject{{"id", it.key()}, {"path", it.value()->path}});
    }

    return QJsonObject{{"ok", true},
                       {"cwd", QDir::currentPath()},
                       {"coredumpDirectory", cfg.getCoredumpDirectory()},
                       {"logSessions", logs},
                       {"coredumpSessions", dumps},
                       {"limits", QJsonObject{{"maxEntries", cfg.getMcpSettings().maxEntries},
                                              {"maxHits", cfg.getMcpSettings().maxHits},
                                              {"maxMemoryBytes", cfg.getMcpSettings().maxMemoryBytes},
                                              {"sourceWindowLines", cfg.getMcpSettings().sourceWindowLines}}}};
}

auto DebugAnalysisService::allowedRoots() const -> QStringList {
    const Config& cfg = config_ ? *config_ : ConfigService::instance().getConfig();
    QStringList roots = cfg.getMcpSettings().allowedRoots;
    roots << QDir::currentPath();
    roots << cfg.getCoredumpDirectory();
    for (const auto& mapping : cfg.getBinaryMappings()) {
        roots << QFileInfo(cfg.resolvePath(mapping.elfPath)).absolutePath();
    }
    for (const auto& lookup : cfg.getAddressLookups()) {
        roots << QFileInfo(cfg.resolvePath(lookup.symbolFilePath)).absolutePath();
    }

    QStringList normalized;
    for (const auto& root : roots) {
        if (root.isEmpty()) {
            continue;
        }
        normalized << canonicalPathOrAbsolute(root);
    }
    normalized.removeDuplicates();
    return normalized;
}

auto DebugAnalysisService::isPathAllowed(const QString& path) const -> bool {
    QString canonical = canonicalPathOrAbsolute(path);
    for (const auto& root : allowedRoots()) {
        if (startsWithPathRoot(canonical, root)) {
            return true;
        }
    }
    return false;
}

auto DebugAnalysisService::resolvePathForRead(const QString& path, const QString& fallbackDir) const -> QString {
    const Config& cfg = config_ ? *config_ : ConfigService::instance().getConfig();
    QString resolved = path;
    if (resolved.isEmpty()) {
        return {};
    }
    QFileInfo info(resolved);
    if (!info.isAbsolute()) {
        const QString base = fallbackDir.isEmpty() ? QDir::currentPath() : fallbackDir;
        resolved = QDir(base).absoluteFilePath(resolved);
    }
    resolved = cfg.resolvePath(resolved);
    return canonicalPathOrAbsolute(resolved);
}

auto DebugAnalysisService::makeSessionId(const QString& prefix, const QString& canonicalPath) const -> QString {
    QFileInfo info(canonicalPath);
    const QString key = canonicalPath + ":" + QString::number(info.size()) + ":" + QString::number(info.lastModified().toMSecsSinceEpoch());
    QByteArray hash = QCryptographicHash::hash(key.toUtf8(), QCryptographicHash::Sha1).toHex().left(16);
    return prefix + "_" + QString::fromLatin1(hash);
}

auto DebugAnalysisService::listLogs() const -> QJsonObject {
    QDir dir(QDir::currentPath());
    QStringList files = dir.entryList({"*.log", "*.txt"}, QDir::Files, QDir::Name);
    std::sort(files.begin(), files.end(), [](const QString& a, const QString& b) {
        const bool aModified = a.contains(".modified.");
        const bool bModified = b.contains(".modified.");
        if (aModified != bModified) {
            return aModified > bModified;
        }
        return a < b;
    });

    QJsonArray out;
    for (const auto& file : files) {
        QFileInfo info(dir.absoluteFilePath(file));
        out.append(QJsonObject{{"name", file}, {"path", info.absoluteFilePath()}, {"size", QString::number(info.size())}});
    }
    return QJsonObject{{"ok", true}, {"logs", out}};
}

auto DebugAnalysisService::loadLog(const QJsonObject& args) -> QJsonObject {
    QString path = args["path"].toString(args["file"].toString());
    if (path.isEmpty()) {
        return toolError("load_log requires 'path' or 'file'");
    }

    QString resolved = resolvePathForRead(path);
    if (!QFileInfo::exists(resolved)) {
        return toolError(QString("Log file not found: %1").arg(path));
    }
    if (!isPathAllowed(resolved)) {
        return toolError(QString("Log path is outside allowed roots: %1").arg(resolved));
    }

    auto id = makeSessionId("log", resolved);
    if (logSessions_.contains(id)) {
        return QJsonObject{{"ok", true}, {"logId", id}, {"cached", true}, {"summary", logSummaryToJson(*logSessions_[id])}};
    }

    auto session = std::make_shared<LogSession>();
    session->id = id;
    session->path = resolved;
    session->displayName = QFileInfo(resolved).fileName();

    LogProcessor processor(resolved);
    processor.setConfigPath(QDir::current().absoluteFilePath("wosdbg.json"));

    QEventLoop loop;
    QString error;
    connect(&processor, &LogProcessor::processingComplete, &loop, &QEventLoop::quit);
    connect(&processor, &LogProcessor::errorOccurred, &loop, [&](const QString& message) {
        error = message;
        loop.quit();
    });

    QTimer timeout;
    timeout.setSingleShot(true);
    connect(&timeout, &QTimer::timeout, &loop, [&]() {
        error = "Timed out while parsing log";
        loop.quit();
    });
    timeout.start(args["timeoutMs"].toInt(120000));
    processor.startProcessing();
    loop.exec();

    if (!error.isEmpty()) {
        return toolError(error);
    }

    session->entries = processor.getEntries();
    auto* saved = session.get();
    logSessions_.insert(id, session);
    return QJsonObject{{"ok", true}, {"logId", id}, {"cached", false}, {"summary", logSummaryToJson(*saved)}};
}

auto DebugAnalysisService::findLogSession(const QString& id) const -> const LogSession* {
    auto it = logSessions_.find(id);
    return it == logSessions_.end() ? nullptr : it.value().get();
}

auto DebugAnalysisService::logEntryToJson(const LogEntry& entry, bool includeChildren) const -> QJsonObject {
    QJsonObject obj{{"lineNumber", entry.lineNumber},
                    {"type", entryTypeName(entry.type)},
                    {"address", QString::fromStdString(entry.address)},
                    {"addressValue", entry.addressValue == 0 ? QString() : formatHex(entry.addressValue)},
                    {"function", QString::fromStdString(entry.function)},
                    {"hexBytes", QString::fromStdString(entry.hexBytes)},
                    {"assembly", QString::fromStdString(entry.assembly)},
                    {"originalLine", QString::fromStdString(entry.originalLine)},
                    {"sourceFile", QString::fromStdString(entry.sourceFile)},
                    {"sourceLine", entry.sourceLine},
                    {"interruptNumber", QString::fromStdString(entry.interruptNumber)},
                    {"cpuStateInfo", QString::fromStdString(entry.cpuStateInfo)}};
    if (includeChildren && !entry.childEntries.empty()) {
        QJsonArray children;
        for (const auto& child : entry.childEntries) {
            children.append(logEntryToJson(child, false));
        }
        obj["children"] = children;
    }
    return obj;
}

auto DebugAnalysisService::logSummaryToJson(const LogSession& session) const -> QJsonObject {
    int instructions = 0;
    int interrupts = 0;
    for (const auto& entry : session.entries) {
        if (entry.type == EntryType::INSTRUCTION) {
            ++instructions;
        } else if (entry.type == EntryType::INTERRUPT) {
            ++interrupts;
        }
    }
    return QJsonObject{{"id", session.id},
                       {"path", session.path},
                       {"entries", static_cast<int>(session.entries.size())},
                       {"instructions", instructions},
                       {"interrupts", interrupts}};
}

int DebugAnalysisService::boundedInt(const QJsonObject& args, const QString& key, int fallback, int minValue, int maxValue) const {
    int value = args[key].toInt(fallback);
    return std::clamp(value, minValue, maxValue);
}

auto DebugAnalysisService::getLogEntries(const QJsonObject& args) const -> QJsonObject {
    const auto* session = findLogSession(args["logId"].toString());
    if (!session) {
        return toolError("Unknown logId");
    }
    const Config& cfg = config_ ? *config_ : ConfigService::instance().getConfig();
    int start = boundedInt(args, "start", 0, 0, static_cast<int>(session->entries.size()));
    int count = boundedInt(args, "count", 50, 1, cfg.getMcpSettings().maxEntries);
    int end = std::min(start + count, static_cast<int>(session->entries.size()));

    QJsonArray entries;
    for (int i = start; i < end; ++i) {
        entries.append(logEntryToJson(session->entries[static_cast<size_t>(i)]));
    }
    return QJsonObject{{"ok", true},
                       {"logId", session->id},
                       {"start", start},
                       {"count", entries.size()},
                       {"total", static_cast<int>(session->entries.size())},
                       {"entries", entries}};
}

auto DebugAnalysisService::searchLog(const QJsonObject& args) const -> QJsonObject {
    const auto* session = findLogSession(args["logId"].toString());
    if (!session) {
        return toolError("Unknown logId");
    }
    QString query = args["query"].toString();
    if (query.isEmpty()) {
        return toolError("search_log requires 'query'");
    }

    const Config& cfg = config_ ? *config_ : ConfigService::instance().getConfig();
    int maxHits = boundedInt(args, "maxHits", cfg.getMcpSettings().maxHits, 1, cfg.getMcpSettings().maxHits);
    int startRow = boundedInt(args, "start", 0, 0, static_cast<int>(session->entries.size()));

    QRegularExpression regex;
    if (args["regex"].toBool(false)) {
        regex = QRegularExpression(query, QRegularExpression::CaseInsensitiveOption);
    } else {
        regex = QRegularExpression(QRegularExpression::escape(query), QRegularExpression::CaseInsensitiveOption);
    }
    if (!regex.isValid()) {
        return toolError("Invalid regex");
    }

    QJsonArray hits;
    bool truncated = false;
    for (int i = startRow; i < static_cast<int>(session->entries.size()); ++i) {
        const auto& entry = session->entries[static_cast<size_t>(i)];
        QString combined = QString::fromStdString(entry.address + " " + entry.function + " " + entry.assembly + " " + entry.originalLine +
                                                  " " + entry.sourceFile);
        if (!regex.match(combined).hasMatch()) {
            continue;
        }
        hits.append(QJsonObject{{"row", i}, {"entry", logEntryToJson(entry, false)}});
        if (hits.size() >= maxHits) {
            truncated = (i + 1) < static_cast<int>(session->entries.size());
            break;
        }
    }
    return QJsonObject{{"ok", true}, {"logId", session->id}, {"hits", hits}, {"truncated", truncated}};
}

auto DebugAnalysisService::getLogContext(const QJsonObject& args) const -> QJsonObject {
    const auto* session = findLogSession(args["logId"].toString());
    if (!session) {
        return toolError("Unknown logId");
    }
    int row = args["row"].toInt(-1);
    if (row < 0 && args.contains("line")) {
        int line = args["line"].toInt();
        for (int i = 0; i < static_cast<int>(session->entries.size()); ++i) {
            if (session->entries[static_cast<size_t>(i)].lineNumber == line) {
                row = i;
                break;
            }
        }
    }
    if (row < 0 && args.contains("address")) {
        auto addr = parseAddressValue(args["address"]);
        if (addr) {
            for (int i = 0; i < static_cast<int>(session->entries.size()); ++i) {
                if (session->entries[static_cast<size_t>(i)].addressValue == *addr) {
                    row = i;
                    break;
                }
            }
        }
    }
    if (row < 0 || row >= static_cast<int>(session->entries.size())) {
        return toolError("No matching log row");
    }

    int before = boundedInt(args, "before", 8, 0, 100);
    int after = boundedInt(args, "after", 16, 0, 100);
    int start = std::max(0, row - before);
    int end = std::min(static_cast<int>(session->entries.size()), row + after + 1);
    QJsonArray entries;
    for (int i = start; i < end; ++i) {
        entries.append(QJsonObject{{"row", i}, {"entry", logEntryToJson(session->entries[static_cast<size_t>(i)])}});
    }
    return QJsonObject{{"ok", true}, {"logId", session->id}, {"row", row}, {"start", start}, {"entries", entries}};
}

auto DebugAnalysisService::extractCoredumps(const QJsonObject& args) -> QJsonObject {
    QString scriptPath;
    const QStringList candidates = {QDir::current().absoluteFilePath("scripts/extract_coredumps.sh"),
                                    QDir::current().absoluteFilePath("../scripts/extract_coredumps.sh")};
    for (const auto& candidate : candidates) {
        if (QFileInfo::exists(candidate)) {
            scriptPath = candidate;
            break;
        }
    }
    if (scriptPath.isEmpty()) {
        return toolError("scripts/extract_coredumps.sh not found");
    }
    if (!isPathAllowed(QFileInfo(scriptPath).absolutePath())) {
        return toolError("Extraction script is outside allowed roots");
    }

    QStringList processArgs;
    processArgs << scriptPath;
    if (args["cluster"].toBool(true)) {
        processArgs << "--cluster";
    }

    QProcess process;
    process.setWorkingDirectory(QDir::currentPath());
    process.start("bash", processArgs);
    if (!process.waitForStarted(5000)) {
        return toolError("Failed to start coredump extraction");
    }
    const int timeoutMs = args["timeoutMs"].toInt(300000);
    if (!process.waitForFinished(timeoutMs)) {
        process.kill();
        process.waitForFinished(3000);
        return toolError("Timed out while extracting coredumps");
    }

    QByteArray stdoutBytes = process.readAllStandardOutput().left(kMaxCapturedProcessOutput);
    QByteArray stderrBytes = process.readAllStandardError().left(kMaxCapturedProcessOutput);
    QJsonObject listed = listCoredumps();
    return QJsonObject{{"ok", process.exitStatus() == QProcess::NormalExit && process.exitCode() == 0},
                       {"exitCode", process.exitCode()},
                       {"stdout", QString::fromUtf8(stdoutBytes)},
                       {"stderr", QString::fromUtf8(stderrBytes)},
                       {"coredumps", listed["coredumps"].toArray()}};
}

auto DebugAnalysisService::listCoredumps() const -> QJsonObject {
    const Config& cfg = config_ ? *config_ : ConfigService::instance().getConfig();
    QDir root(cfg.getCoredumpDirectory());
    QJsonArray dumps;
    if (!root.exists()) {
        return QJsonObject{{"ok", true}, {"coredumpDirectory", root.absolutePath()}, {"coredumps", dumps}};
    }

    QDirIterator it(root.absolutePath(), {"*_coredump.bin"}, QDir::Files, QDirIterator::Subdirectories);
    while (it.hasNext()) {
        it.next();
        QFileInfo info = it.fileInfo();
        QJsonObject item{{"path", info.absoluteFilePath()},
                         {"name", info.fileName()},
                         {"size", QString::number(info.size())},
                         {"binary", wosdbg::parseBinaryNameFromFilename(info.fileName())}};
        QFile file(info.absoluteFilePath());
        if (file.open(QIODevice::ReadOnly)) {
            auto parsed = wosdbg::parseCoreDump(file.readAll());
            if (parsed) {
                item["pid"] = QString::number(parsed->pid);
                item["cpu"] = QString::number(parsed->cpu);
                item["interrupt"] = QString::number(parsed->int_num);
                item["interruptName"] = wosdbg::interruptName(parsed->int_num);
                item["rip"] = formatHex(parsed->trapFrame.rip);
                item["cr2"] = formatHex(parsed->cr2);
                item["timestamp"] = QString::number(parsed->timestamp);
            }
        }
        dumps.append(item);
    }
    return QJsonObject{{"ok", true}, {"coredumpDirectory", root.absolutePath()}, {"coredumps", dumps}};
}

auto DebugAnalysisService::openCoredump(const QJsonObject& args) -> QJsonObject {
    QString path = args["path"].toString(args["file"].toString());
    if (path.isEmpty()) {
        return toolError("open_coredump requires 'path' or 'file'");
    }

    const Config& cfg = config_ ? *config_ : ConfigService::instance().getConfig();
    QString resolved = resolvePathForRead(path, cfg.getCoredumpDirectory());
    if (!QFileInfo::exists(resolved)) {
        return toolError(QString("Coredump not found: %1").arg(path));
    }
    if (!isPathAllowed(resolved)) {
        return toolError(QString("Coredump path is outside allowed roots: %1").arg(resolved));
    }

    QString id = makeSessionId("dump", resolved);
    if (dumpSessions_.contains(id)) {
        return QJsonObject{{"ok", true}, {"dumpId", id}, {"cached", true}, {"summary", coredumpSummaryToJson(*dumpSessions_[id])}};
    }

    auto dump = wosdbg::parseCoreDump(resolved);
    if (!dump) {
        return toolError("Failed to parse coredump");
    }

    auto session = std::make_shared<DumpSession>();
    session->id = id;
    session->path = resolved;
    session->displayName = QFileInfo(resolved).fileName();
    session->dump = std::move(dump);

    QString binaryName = wosdbg::parseBinaryNameFromFilename(session->dump->sourceFilename);
    QString elfPath = cfg.findElfPathForBinary(binaryName);
    if (!elfPath.isEmpty()) {
        session->binarySymbols = wosdbg::loadSymbolsFromFile(elfPath);
        session->binarySections = wosdbg::loadSectionsFromFile(elfPath);
    }
    if (!session->dump->embeddedElf().isEmpty()) {
        session->embeddedSymbols = wosdbg::loadSymbolsFromCoreDump(*session->dump);
        session->embeddedSections = wosdbg::loadSectionsFromCoreDump(*session->dump);
    }
    for (const auto& lookup : cfg.getAddressLookups()) {
        if (lookup.symbolFilePath.contains("kern") || lookup.symbolFilePath.contains("wos")) {
            QString kernelPath = cfg.resolvePath(lookup.symbolFilePath);
            session->kernelSymbols = wosdbg::loadSymbolsFromFile(kernelPath);
            session->kernelSections = wosdbg::loadSectionsFromFile(kernelPath);
            break;
        }
    }

    auto* saved = session.get();
    dumpSessions_.insert(id, session);
    return QJsonObject{{"ok", true}, {"dumpId", id}, {"cached", false}, {"summary", coredumpSummaryToJson(*saved)}};
}

auto DebugAnalysisService::findDumpSession(const QString& id) const -> const DumpSession* {
    auto it = dumpSessions_.find(id);
    return it == dumpSessions_.end() ? nullptr : it.value().get();
}

auto DebugAnalysisService::findDumpSession(const QString& id) -> DumpSession* {
    auto it = dumpSessions_.find(id);
    return it == dumpSessions_.end() ? nullptr : it.value().get();
}

auto DebugAnalysisService::symbolTables(const DumpSession& session) const -> std::vector<wosdbg::SymbolTable*> {
    std::vector<wosdbg::SymbolTable*> tables;
    if (session.binarySymbols) tables.push_back(session.binarySymbols.get());
    if (session.embeddedSymbols) tables.push_back(session.embeddedSymbols.get());
    if (session.kernelSymbols) tables.push_back(session.kernelSymbols.get());
    return tables;
}

auto DebugAnalysisService::sectionMaps(const DumpSession& session) const -> std::vector<wosdbg::SectionMap*> {
    std::vector<wosdbg::SectionMap*> maps;
    if (session.binarySections) maps.push_back(session.binarySections.get());
    if (session.embeddedSections) maps.push_back(session.embeddedSections.get());
    if (session.kernelSections) maps.push_back(session.kernelSections.get());
    return maps;
}

auto DebugAnalysisService::coredumpSummaryToJson(const DumpSession& session) const -> QJsonObject {
    const auto& dump = *session.dump;
    return QJsonObject{{"id", session.id},
                       {"path", session.path},
                       {"pid", QString::number(dump.pid)},
                       {"cpu", QString::number(dump.cpu)},
                       {"interrupt", QString::number(dump.int_num)},
                       {"interruptName", wosdbg::interruptName(dump.int_num)},
                       {"errorCode", formatHex(dump.err_code)},
                       {"cr2", formatHex(dump.cr2)},
                       {"cr3", formatHex(dump.cr3)},
                       {"trapRip", formatHex(dump.trapFrame.rip)},
                       {"trapRsp", formatHex(dump.trapFrame.rsp)},
                       {"savedRip", formatHex(dump.savedFrame.rip)},
                       {"exePath", dump.exePath},
                       {"cwd", dump.cwd},
                       {"root", dump.root},
                       {"segments", static_cast<int>(dump.segments.size())},
                       {"embeddedElfBytes", QString::number(dump.elfSize)}};
}

auto DebugAnalysisService::parseAddressValue(const QJsonValue& value) const -> std::optional<uint64_t> {
    if (value.isString()) {
        QString text = value.toString().trimmed();
        bool ok = false;
        uint64_t parsed = text.startsWith("0x", Qt::CaseInsensitive) ? text.toULongLong(&ok, 16) : text.toULongLong(&ok, 10);
        if (ok) {
            return parsed;
        }
        return std::nullopt;
    }
    if (value.isDouble()) {
        double d = value.toDouble();
        if (d >= 0) {
            return static_cast<uint64_t>(d);
        }
    }
    return std::nullopt;
}

auto DebugAnalysisService::registerMap(const wosdbg::CoreDump& dump, const QString& frame) const -> QHash<QString, uint64_t> {
    const bool saved = frame.compare("saved", Qt::CaseInsensitive) == 0;
    const auto& f = saved ? dump.savedFrame : dump.trapFrame;
    const auto& r = saved ? dump.savedRegs : dump.trapRegs;
    QHash<QString, uint64_t> regs;
    regs.insert("rip", f.rip);
    regs.insert("rsp", f.rsp);
    regs.insert("cs", f.cs);
    regs.insert("ss", f.ss);
    regs.insert("rflags", f.rflags);
    regs.insert("rax", r.rax);
    regs.insert("rbx", r.rbx);
    regs.insert("rcx", r.rcx);
    regs.insert("rdx", r.rdx);
    regs.insert("rsi", r.rsi);
    regs.insert("rdi", r.rdi);
    regs.insert("rbp", r.rbp);
    regs.insert("r8", r.r8);
    regs.insert("r9", r.r9);
    regs.insert("r10", r.r10);
    regs.insert("r11", r.r11);
    regs.insert("r12", r.r12);
    regs.insert("r13", r.r13);
    regs.insert("r14", r.r14);
    regs.insert("r15", r.r15);
    regs.insert("cr2", dump.cr2);
    regs.insert("cr3", dump.cr3);
    regs.insert("fs_base", dump.threadFsBase);
    regs.insert("gs_base", dump.threadGsBase);
    return regs;
}

auto DebugAnalysisService::asciiPreview(const QByteArray& bytes, int maxLen) const -> QString {
    QString out;
    int limit = std::min(static_cast<int>(bytes.size()), maxLen);
    for (int i = 0; i < limit; ++i) {
        const auto ch = static_cast<unsigned char>(bytes[i]);
        if (ch == 0) {
            break;
        }
        out += (ch >= 32 && ch < 127) ? QChar(ch) : QChar('.');
    }
    return out;
}

auto DebugAnalysisService::qwordPreview(const wosdbg::CoreDump& dump, uint64_t address, int qwords) const -> QJsonArray {
    QJsonArray out;
    QByteArray bytes = wosdbg::readVaBytes(dump, address, static_cast<size_t>(qwords * 8));
    for (int i = 0; i < qwords && i * 8 + 7 < bytes.size(); ++i) {
        uint64_t value = readU64Le(bytes.constData() + i * 8);
        out.append(QJsonObject{{"address", formatHex(address + static_cast<uint64_t>(i * 8))}, {"value", formatHex(value)}});
    }
    return out;
}

auto DebugAnalysisService::describeAddress(const DumpSession& session, uint64_t value, const QString& registerName) const
    -> AddressDescription {
    const auto& dump = *session.dump;
    AddressDescription desc;
    desc.value = value;
    desc.hex = formatHex(value);
    desc.canonical = isCanonicalX86Address(value);
    desc.confidence = "heuristic";

    if (value == 0) {
        desc.classification = "null";
        desc.description = "zero/null value";
        return desc;
    }
    if (value < 0x1000) {
        desc.classification = "small integer";
        desc.description = "small scalar-looking value, not a usable pointer";
        return desc;
    }
    if (!desc.canonical) {
        desc.classification = "invalid/non-canonical";
        desc.description = "not a canonical x86-64 virtual address";
        return desc;
    }

    const auto syms = symbolTables(session);
    const auto sections = sectionMaps(session);
    auto resolved = wosdbg::resolveAddress(value, syms, sections);
    if (resolved) {
        desc.symbol = QString::fromStdString(*resolved);
    }
    for (const auto* sectionMap : sections) {
        if (!sectionMap) continue;
        auto section = sectionMap->lookup(value);
        if (section) {
            desc.section = QString::fromStdString(*section);
            break;
        }
    }

    const auto* seg = wosdbg::findSegmentForVa(dump, value);
    if (seg) {
        desc.mapped = true;
        auto begin = dump.segments.begin();
        auto it = std::find_if(begin, dump.segments.end(), [&](const wosdbg::CoreDumpSegment& candidate) { return &candidate == seg; });
        desc.segmentIndex = it == dump.segments.end() ? -1 : static_cast<int>(std::distance(begin, it));
        desc.segmentOffset = value - seg->vaddr;
        desc.segmentType = seg->typeName();
        QByteArray bytes = wosdbg::readVaBytes(dump, value, 64);
        desc.asciiPreview = asciiPreview(bytes, (config_ ? config_->getMcpSettings().maxStringLength : kDefaultMaxStringLength));
        desc.qwordPreview = qwordPreview(dump, value, 4);
    }

    const bool isStackRange =
        (dump.threadStackSize > 0 && value >= dump.threadStackBase && value < dump.threadStackBase + dump.threadStackSize) ||
        (seg && seg->type == static_cast<uint32_t>(wosdbg::SegmentType::StackPage)) ||
        registerName.compare("rsp", Qt::CaseInsensitive) == 0 || registerName.compare("rbp", Qt::CaseInsensitive) == 0;
    const bool isTlsRange = (dump.threadTlsSize > 0 && value >= dump.threadTlsBase && value < dump.threadTlsBase + dump.threadTlsSize) ||
                            value == dump.threadFsBase || value == dump.threadGsBase;

    if (value >= 0xffffffff80000000ULL) {
        desc.classification = desc.symbol.isEmpty() ? "kernel pointer" : "kernel code";
    } else if (isStackRange) {
        desc.classification = "stack pointer";
    } else if (isTlsRange) {
        desc.classification = "TLS/FS/GS-ish";
    } else if ((!desc.symbol.isEmpty() || desc.section.startsWith(".text")) && value < 0x0000800000000000ULL) {
        desc.classification = "user code";
    } else if (desc.mapped) {
        desc.classification = "mapped data";
    } else {
        desc.classification = "unmapped";
    }

    if (!desc.symbol.isEmpty()) {
        desc.description = QString("resolves to %1").arg(desc.symbol);
    } else if (desc.mapped) {
        desc.description = QString("falls inside %1 segment %2 at offset %3")
                               .arg(desc.segmentType)
                               .arg(desc.segmentIndex)
                               .arg(formatHex(desc.segmentOffset));
    } else {
        desc.description = "canonical address, but not present in the coredump segment table";
    }
    return desc;
}

auto DebugAnalysisService::addressDescriptionToJson(const AddressDescription& desc) const -> QJsonObject {
    return QJsonObject{{"value", desc.hex},
                       {"classification", desc.classification},
                       {"confidence", desc.confidence},
                       {"description", desc.description},
                       {"canonical", desc.canonical},
                       {"mapped", desc.mapped},
                       {"segmentIndex", desc.segmentIndex},
                       {"segmentOffset", desc.segmentIndex >= 0 ? formatHex(desc.segmentOffset) : QString()},
                       {"segmentType", desc.segmentType},
                       {"symbol", desc.symbol},
                       {"section", desc.section},
                       {"source", desc.source},
                       {"sourceLine", desc.sourceLine},
                       {"asciiPreview", desc.asciiPreview},
                       {"qwordPreview", desc.qwordPreview}};
}

auto DebugAnalysisService::coredumpRegistersToJson(const DumpSession& session, const QString& frame) const -> QJsonArray {
    QJsonArray out;
    QHash<QString, uint64_t> regs = registerMap(*session.dump, frame);
    QStringList keys = regs.keys();
    std::sort(keys.begin(), keys.end());
    for (const auto& key : keys) {
        AddressDescription desc = describeAddress(session, regs.value(key), key);
        out.append(QJsonObject{
            {"name", key}, {"frame", frame}, {"value", formatHex(regs.value(key))}, {"address", addressDescriptionToJson(desc)}});
    }
    return out;
}

auto DebugAnalysisService::getCrashSummary(const QJsonObject& args) const -> QJsonObject {
    const auto* session = findDumpSession(args["dumpId"].toString());
    if (!session) {
        return toolError("Unknown dumpId");
    }
    const auto& dump = *session->dump;
    QJsonArray suspicious;
    const auto regs = registerMap(dump, "trap");
    for (const auto* name : {"rip", "rsp", "rbp", "cr2", "rdi", "rsi", "rdx"}) {
        QString regName = QString::fromLatin1(name);
        AddressDescription desc = describeAddress(*session, regs.value(regName), regName);
        if (regName == "rip" || regName == "cr2" || desc.classification == "unmapped" || desc.classification == "invalid/non-canonical" ||
            desc.classification == "stack pointer") {
            suspicious.append(QJsonObject{{"register", regName}, {"address", addressDescriptionToJson(desc)}});
        }
    }
    QJsonArray disasm = disassembleAt(*session, dump.trapFrame.rip, 24);
    return QJsonObject{
        {"ok", true}, {"summary", coredumpSummaryToJson(*session)}, {"suspiciousRegisters", suspicious}, {"nearTrapRip", disasm}};
}

auto DebugAnalysisService::describeRegisters(const QJsonObject& args) const -> QJsonObject {
    const auto* session = findDumpSession(args["dumpId"].toString());
    if (!session) {
        return toolError("Unknown dumpId");
    }
    QString frame = args["frame"].toString("trap").toLower();
    if (frame != "trap" && frame != "saved") {
        frame = "trap";
    }
    return QJsonObject{{"ok", true}, {"dumpId", session->id}, {"frame", frame}, {"registers", coredumpRegistersToJson(*session, frame)}};
}

auto DebugAnalysisService::resolveAddressArgument(const DumpSession& session, const QJsonObject& args, const QString& defaultRegister) const
    -> std::optional<uint64_t> {
    if (args.contains("address")) {
        return parseAddressValue(args["address"]);
    }
    QString reg = args["register"].toString(defaultRegister).toLower();
    if (!reg.isEmpty()) {
        auto regs = registerMap(*session.dump, args["frame"].toString("trap"));
        if (regs.contains(reg)) {
            return regs.value(reg);
        }
    }
    return std::nullopt;
}

auto DebugAnalysisService::followRegister(const QJsonObject& args) const -> QJsonObject {
    const auto* session = findDumpSession(args["dumpId"].toString());
    if (!session) {
        return toolError("Unknown dumpId");
    }
    QString reg = args["register"].toString().toLower();
    if (reg.isEmpty()) {
        return toolError("follow_register requires 'register'");
    }
    auto regs = registerMap(*session->dump, args["frame"].toString("trap"));
    if (!regs.contains(reg)) {
        return toolError(QString("Unknown register: %1").arg(reg));
    }
    uint64_t value = regs.value(reg);
    AddressDescription desc = describeAddress(*session, value, reg);
    QJsonObject out{{"ok", true}, {"dumpId", session->id}, {"register", reg}, {"address", addressDescriptionToJson(desc)}};
    if (desc.classification.contains("code")) {
        out["suggestedView"] = "disassembly";
        out["disassembly"] = disassembleAt(*session, value, 32);
    } else if (desc.mapped) {
        out["suggestedView"] = (reg == "rsp" || reg == "rbp") ? "stack" : "memory";
        out["memory"] =
            getMemoryContext(QJsonObject{{"dumpId", session->id}, {"address", formatHex(value)}, {"beforeBytes", 64}, {"afterBytes", 192}});
    } else {
        out["suggestedView"] = "explanation";
    }
    return out;
}

auto DebugAnalysisService::getMemoryContext(const QJsonObject& args) const -> QJsonObject {
    const auto* session = findDumpSession(args["dumpId"].toString());
    if (!session) {
        return toolError("Unknown dumpId");
    }
    auto address = resolveAddressArgument(*session, args, "rsp");
    if (!address) {
        return toolError("get_memory_context requires an address or register");
    }
    const Config& cfg = config_ ? *config_ : ConfigService::instance().getConfig();
    int before = boundedInt(args, "beforeBytes", 64, 0, cfg.getMcpSettings().maxMemoryBytes);
    int after = boundedInt(args, "afterBytes", 192, 1, cfg.getMcpSettings().maxMemoryBytes);
    int total = std::min(before + after, cfg.getMcpSettings().maxMemoryBytes);
    uint64_t start = *address > static_cast<uint64_t>(before) ? *address - static_cast<uint64_t>(before) : 0;
    uint64_t end = start + static_cast<uint64_t>(total);
    auto rows = wosdbg::dumpRange(*session->dump, start, end, symbolTables(*session), sectionMaps(*session));
    QJsonArray outRows;
    for (const auto& row : rows) {
        outRows.append(QJsonObject{{"address", formatHex(row.va)},
                                   {"value", formatHex(row.value)},
                                   {"symbol", row.symbol},
                                   {"notes", row.notes},
                                   {"gutter", row.gutter}});
    }
    return QJsonObject{
        {"ok", true}, {"dumpId", session->id}, {"anchor", formatHex(*address)}, {"start", formatHex(start)}, {"rows", outRows}};
}

auto DebugAnalysisService::searchCoredumpMemory(const QJsonObject& args) const -> QJsonObject {
    const auto* session = findDumpSession(args["dumpId"].toString());
    if (!session) {
        return toolError("Unknown dumpId");
    }
    const Config& cfg = config_ ? *config_ : ConfigService::instance().getConfig();
    QString kind = args["kind"].toString("pointer").toLower();
    int maxHits = boundedInt(args, "maxHits", cfg.getMcpSettings().maxHits, 1, cfg.getMcpSettings().maxHits);
    int alignment = boundedInt(args, "alignment", (kind == "bytes" || kind == "ascii") ? 1 : 8, 1, 4096);
    int contextBytes = boundedInt(args, "contextBytes", 32, 0, 256);
    QString segmentType = args["segmentType"].toString();

    QByteArray byteNeedle;
    uint64_t u64Needle = 0;
    bool hasU64Needle = false;
    if (kind == "bytes") {
        byteNeedle = bytesFromHex(args["needle"].toString());
        if (byteNeedle.isEmpty()) {
            return toolError("bytes search requires a non-empty hex 'needle'");
        }
    } else if (kind == "ascii") {
        byteNeedle = args["needle"].toString().toUtf8();
        if (byteNeedle.isEmpty()) {
            return toolError("ascii search requires a non-empty 'needle'");
        }
    } else if (kind == "u64" || kind == "pointer" || kind == "symbol") {
        auto parsed = parseAddressValue(args["needle"]);
        if (parsed) {
            u64Needle = *parsed;
            hasU64Needle = true;
        }
    }

    std::optional<uint64_t> rangeStart;
    std::optional<uint64_t> rangeEnd;
    if (args["addressRange"].isObject()) {
        auto range = args["addressRange"].toObject();
        rangeStart = parseAddressValue(range["from"]);
        rangeEnd = parseAddressValue(range["to"]);
    }

    QJsonArray hits;
    bool truncated = false;
    uint64_t scanned = 0;
    const auto& dump = *session->dump;
    for (int segIndex = 0; segIndex < static_cast<int>(dump.segments.size()); ++segIndex) {
        const auto& seg = dump.segments[static_cast<size_t>(segIndex)];
        if (!seg.isPresent()) {
            continue;
        }
        if (!segmentType.isEmpty() && seg.typeName().compare(segmentType, Qt::CaseInsensitive) != 0) {
            continue;
        }
        uint64_t start = seg.vaddr;
        uint64_t end = seg.vaddrEnd();
        if (rangeStart) start = std::max(start, *rangeStart);
        if (rangeEnd) end = std::min(end, *rangeEnd);
        if (end <= start) {
            continue;
        }
        uint64_t length = end - start;
        if (scanned + length > kMaxMemorySearchBytes) {
            length = kMaxMemorySearchBytes - scanned;
            truncated = true;
        }
        QByteArray bytes = wosdbg::readVaBytes(dump, start, static_cast<size_t>(length));
        scanned += static_cast<uint64_t>(bytes.size());
        if (bytes.isEmpty()) {
            continue;
        }

        for (qsizetype off = 0; off < bytes.size();) {
            bool match = false;
            uint64_t hitValue = 0;
            if ((kind == "bytes" || kind == "ascii") && off + byteNeedle.size() <= bytes.size()) {
                match = std::memcmp(bytes.constData() + off, byteNeedle.constData(), static_cast<size_t>(byteNeedle.size())) == 0;
                hitValue = 0;
            } else if (off + 7 < bytes.size()) {
                hitValue = readU64Le(bytes.constData() + off);
                if (kind == "u64") {
                    match = hasU64Needle && hitValue == u64Needle;
                } else if (kind == "nonzero") {
                    match = hitValue != 0;
                } else if (kind == "pointer" || kind == "symbol") {
                    auto desc = describeAddress(*session, hitValue);
                    match = desc.canonical && (desc.mapped || !desc.symbol.isEmpty() || desc.classification.contains("code") ||
                                               desc.classification.contains("kernel"));
                    if (kind == "symbol") {
                        match = match && !desc.symbol.isEmpty();
                    }
                    if (hasU64Needle) {
                        match = match && hitValue == u64Needle;
                    }
                }
            }

            if (match) {
                uint64_t hitAddr = start + static_cast<uint64_t>(off);
                AddressDescription pointed = describeAddress(*session, hitValue);
                uint64_t ctxStart = hitAddr > static_cast<uint64_t>(contextBytes) ? hitAddr - static_cast<uint64_t>(contextBytes) : hitAddr;
                QByteArray preview = wosdbg::readVaBytes(dump, ctxStart, static_cast<size_t>(contextBytes * 2 + 8));
                hits.append(QJsonObject{{"dumpId", session->id},
                                        {"address", formatHex(hitAddr)},
                                        {"segmentIndex", segIndex},
                                        {"value", (kind == "bytes" || kind == "ascii") ? QString() : formatHex(hitValue)},
                                        {"pointsTo", addressDescriptionToJson(pointed)},
                                        {"preview", asciiPreview(preview, cfg.getMcpSettings().maxStringLength)},
                                        {"suggestedTool", "wosdbg.get_memory_context"}});
                if (hits.size() >= maxHits) {
                    truncated = true;
                    break;
                }
            }

            off += alignment;
        }
        if (hits.size() >= maxHits || scanned >= kMaxMemorySearchBytes) {
            break;
        }
    }
    return QJsonObject{{"ok", true},
                       {"dumpId", session->id},
                       {"kind", kind},
                       {"hits", hits},
                       {"scannedBytes", QString::number(scanned)},
                       {"truncated", truncated}};
}

auto DebugAnalysisService::findPointers(const QJsonObject& args) const -> QJsonObject {
    QJsonObject searchArgs = args;
    searchArgs["kind"] = "pointer";
    if (!searchArgs.contains("alignment")) {
        searchArgs["alignment"] = 8;
    }
    QJsonObject result = searchCoredumpMemory(searchArgs);
    QString target = args["target"].toString().toLower();
    if (target.isEmpty() || !result["ok"].toBool()) {
        return result;
    }

    QJsonArray filtered;
    for (const auto& hitValue : result["hits"].toArray()) {
        QJsonObject hit = hitValue.toObject();
        QString cls = hit["pointsTo"].toObject()["classification"].toString().toLower();
        bool keep = (target == "code" && cls.contains("code")) || (target == "stack" && cls.contains("stack")) ||
                    (target == "kernel" && cls.contains("kernel")) || (target == "mapped" && hit["pointsTo"].toObject()["mapped"].toBool());
        if (keep) {
            filtered.append(hit);
        }
    }
    result["hits"] = filtered;
    result["target"] = target;
    return result;
}

auto DebugAnalysisService::disassemblyStartSymbol(const DumpSession& session, uint64_t address) const
    -> std::optional<wosdbg::SymbolEntry> {
    auto matchInTable = [address](const wosdbg::SymbolTable* table) -> std::optional<wosdbg::SymbolEntry> {
        if (!table || table->entries().empty()) {
            return std::nullopt;
        }

        const auto& entries = table->entries();
        auto it = std::upper_bound(entries.begin(), entries.end(), address,
                                   [](uint64_t value, const wosdbg::SymbolEntry& entry) { return value < entry.addr; });
        if (it == entries.begin()) {
            return std::nullopt;
        }
        --it;

        const uint64_t offset = address - it->addr;
        uint64_t nextSymbolDistance = kMaxDisassemblyFunctionBytes + 1;
        if (std::next(it) != entries.end() && std::next(it)->addr > it->addr) {
            nextSymbolDistance = std::next(it)->addr - it->addr;
        }
        const uint64_t symbolExtent = it->size > 0 ? it->size : std::min<uint64_t>(nextSymbolDistance, kMaxDisassemblyFunctionBytes);
        if (offset >= symbolExtent || offset > kMaxDisassemblyFunctionBytes) {
            return std::nullopt;
        }
        return *it;
    };

    // The embedded ELF supplies the bytes we disassemble, so prefer its symbols
    // when present. File symbols remain useful for older dumps without symtabs.
    if (auto match = matchInTable(session.embeddedSymbols.get())) {
        return match;
    }
    if (auto match = matchInTable(session.binarySymbols.get())) {
        return match;
    }
    return matchInTable(session.kernelSymbols.get());
}

auto DebugAnalysisService::disassembleAt(const DumpSession& session, uint64_t address, int instructionCount) const -> QJsonArray {
    QJsonArray out;
    QByteArray elf = session.dump->embeddedElf();
    if (elf.isEmpty()) {
        out.append(QJsonObject{{"note", "no embedded ELF in coredump"}});
        return out;
    }

    csh capstone = 0;
    if (cs_open(CS_ARCH_X86, CS_MODE_64, &capstone) != CS_ERR_OK) {
        out.append(QJsonObject{{"note", "Capstone failed to initialize"}});
        return out;
    }
    cs_option(capstone, CS_OPT_SYNTAX, CS_OPT_SYNTAX_INTEL);

    uint64_t start = address > 64 ? address - 64 : 0;
    if (auto symbol = disassemblyStartSymbol(session, address)) {
        start = symbol->addr;
    }
    const uint64_t bytesNeeded = address >= start ? (address - start) + kDisassemblyBytesAfterTarget : kDisassemblyBytesAfterTarget;
    const size_t readSize = static_cast<size_t>(std::min<uint64_t>(bytesNeeded, kMaxDisassemblyFunctionBytes));
    auto bytes = wosdbg::elfBytesAtVA(elf, start, readSize);
    if (bytes.empty()) {
        cs_close(&capstone);
        out.append(QJsonObject{{"note", QString("no ELF bytes mapped at %1").arg(formatHex(start))}});
        return out;
    }

    cs_insn* insns = nullptr;
    size_t count = cs_disasm(capstone, bytes.data(), bytes.size(), start, 0, &insns);
    if (count == 0) {
        cs_close(&capstone);
        out.append(QJsonObject{{"note", "Capstone disassembly failed"}});
        return out;
    }

    int anchor = 0;
    for (size_t i = 0; i < count; ++i) {
        if (insns[i].address >= address) {
            anchor = static_cast<int>(i);
            break;
        }
    }
    if (anchor == 0 && count > 0 && insns[count - 1].address < address) {
        anchor = static_cast<int>(count - 1);
    }
    int begin = std::max(0, anchor - instructionCount / 3);
    int end = std::min(static_cast<int>(count), begin + instructionCount);
    const auto syms = symbolTables(session);
    const auto sections = sectionMaps(session);
    for (int i = begin; i < end; ++i) {
        const auto& ins = insns[static_cast<size_t>(i)];
        QString bytesText;
        for (int b = 0; b < static_cast<int>(ins.size); ++b) {
            bytesText += QString("%1 ").arg(static_cast<uint32_t>(ins.bytes[b]), 2, 16, QChar('0'));
        }
        auto sym = wosdbg::resolveAddress(ins.address, syms, sections);
        const bool targetInside = address > ins.address && address < ins.address + ins.size;
        QString marker;
        if (ins.address == session.dump->trapFrame.rip) {
            marker = "trap_rip";
        } else if (ins.address == session.dump->savedFrame.rip) {
            marker = "saved_rip";
        } else if (ins.address == address) {
            marker = "target";
        } else if (targetInside) {
            marker = "target_inside_instruction";
        }
        out.append(QJsonObject{{"address", formatHex(ins.address)},
                               {"marker", marker},
                               {"bytes", bytesText.trimmed()},
                               {"mnemonic", QString::fromLatin1(ins.mnemonic)},
                               {"operands", QString::fromLatin1(ins.op_str)},
                               {"symbol", sym ? QString::fromStdString(*sym) : QString()}});
    }
    cs_free(insns, count);
    cs_close(&capstone);
    return out;
}

auto DebugAnalysisService::disassembleCoredump(const QJsonObject& args) const -> QJsonObject {
    const auto* session = findDumpSession(args["dumpId"].toString());
    if (!session) {
        return toolError("Unknown dumpId");
    }
    auto address = resolveAddressArgument(*session, args, "rip");
    if (!address) {
        return toolError("disassemble_coredump requires an address or register");
    }
    const Config& cfg = config_ ? *config_ : ConfigService::instance().getConfig();
    int count = boundedInt(args, "instructions", 32, 1, cfg.getMcpSettings().maxDisassemblyInstructions);
    return QJsonObject{{"ok", true},
                       {"dumpId", session->id},
                       {"address", formatHex(*address)},
                       {"instructions", disassembleAt(*session, *address, count)}};
}

auto DebugAnalysisService::resolveAddressTool(const QJsonObject& args) const -> QJsonObject {
    const auto* session = findDumpSession(args["dumpId"].toString());
    if (!session) {
        return toolError("Unknown dumpId");
    }
    auto address = resolveAddressArgument(*session, args);
    if (!address) {
        return toolError("resolve_address requires an address or register");
    }
    return QJsonObject{{"ok", true}, {"dumpId", session->id}, {"address", addressDescriptionToJson(describeAddress(*session, *address))}};
}

auto DebugAnalysisService::sourceContextForPath(const QString& filePath, int line, int contextLines) const -> QJsonObject {
    QString resolved = resolvePathForRead(filePath);
    if (!QFileInfo::exists(resolved)) {
        return toolError(QString("Source file not found: %1").arg(filePath));
    }
    if (!isPathAllowed(resolved)) {
        return toolError(QString("Source path is outside allowed roots: %1").arg(resolved));
    }
    QFile file(resolved);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) {
        return toolError(QString("Could not open source file: %1").arg(resolved));
    }
    int start = std::max(1, line - contextLines);
    int end = line + contextLines;
    QJsonArray lines;
    QTextStream in(&file);
    int current = 1;
    while (!in.atEnd() && current <= end) {
        QString text = in.readLine();
        if (current >= start) {
            lines.append(QJsonObject{{"line", current}, {"text", text}, {"target", current == line}});
        }
        ++current;
    }
    return QJsonObject{{"ok", true}, {"path", resolved}, {"line", line}, {"start", start}, {"lines", lines}};
}

auto DebugAnalysisService::getSourceContext(const QJsonObject& args) const -> QJsonObject {
    QString path = args["path"].toString(args["sourceFile"].toString());
    int line = args["line"].toInt(args["sourceLine"].toInt(1));
    if (path.isEmpty()) {
        return toolError("get_source_context requires 'path'");
    }
    const Config& cfg = config_ ? *config_ : ConfigService::instance().getConfig();
    int context = boundedInt(args, "contextLines", cfg.getMcpSettings().sourceWindowLines, 0, 100);
    return sourceContextForPath(path, line, context);
}

auto DebugAnalysisService::listResources() const -> QJsonArray {
    QJsonArray resources;
    for (auto it = logSessions_.cbegin(); it != logSessions_.cend(); ++it) {
        resources.append(
            QJsonObject{{"uri", QString("wosdbg://log/%1/summary").arg(it.key())}, {"name", QString("Log %1 summary").arg(it.key())}});
    }
    for (auto it = dumpSessions_.cbegin(); it != dumpSessions_.cend(); ++it) {
        resources.append(QJsonObject{{"uri", QString("wosdbg://coredump/%1/summary").arg(it.key())},
                                     {"name", QString("Coredump %1 summary").arg(it.key())}});
        resources.append(QJsonObject{{"uri", QString("wosdbg://coredump/%1/registers").arg(it.key())},
                                     {"name", QString("Coredump %1 registers").arg(it.key())}});
    }
    return resources;
}

auto DebugAnalysisService::listResourceTemplates() const -> QJsonArray {
    return QJsonArray{
        QJsonObject{{"uriTemplate", "wosdbg://coredump/{dumpId}/summary"}, {"name", "Coredump summary"}},
        QJsonObject{{"uriTemplate", "wosdbg://coredump/{dumpId}/registers"}, {"name", "Coredump registers"}},
        QJsonObject{{"uriTemplate", "wosdbg://coredump/{dumpId}/register/{name}"}, {"name", "Coredump register follow-up"}},
        QJsonObject{{"uriTemplate", "wosdbg://coredump/{dumpId}/memory/{address}"}, {"name", "Coredump memory context"}},
        QJsonObject{{"uriTemplate", "wosdbg://coredump/{dumpId}/disasm/{address}"}, {"name", "Coredump disassembly"}},
        QJsonObject{{"uriTemplate", "wosdbg://log/{logId}/summary"}, {"name", "Log summary"}},
        QJsonObject{{"uriTemplate", "wosdbg://log/{logId}/entry/{row}"}, {"name", "Log entry"}},
        QJsonObject{{"uriTemplate", "wosdbg://source/{encodedPath}:{line}"}, {"name", "Source context"}},
    };
}

auto DebugAnalysisService::readResource(const QString& uri) const -> QJsonObject {
    QUrl url(uri);
    if (url.scheme() != "wosdbg") {
        return toolError("Unsupported resource URI scheme");
    }
    QStringList parts = url.path().split('/', Qt::SkipEmptyParts);
    QString host = url.host();
    if (host == "log" && parts.size() >= 2) {
        QString logId = parts[0];
        QString kind = parts[1];
        const auto* session = findLogSession(logId);
        if (!session) return toolError("Unknown logId");
        if (kind == "summary") return QJsonObject{{"ok", true}, {"summary", logSummaryToJson(*session)}};
        if (kind == "entry" && parts.size() >= 3)
            return getLogContext(QJsonObject{{"logId", logId}, {"row", parts[2].toInt()}, {"before", 0}, {"after", 0}});
    }
    if (host == "coredump" && parts.size() >= 2) {
        QString dumpId = parts[0];
        QString kind = parts[1];
        if (kind == "summary") return getCrashSummary(QJsonObject{{"dumpId", dumpId}});
        if (kind == "registers") return describeRegisters(QJsonObject{{"dumpId", dumpId}, {"frame", "trap"}});
        if (kind == "register" && parts.size() >= 3) return followRegister(QJsonObject{{"dumpId", dumpId}, {"register", parts[2]}});
        if (kind == "memory" && parts.size() >= 3) return getMemoryContext(QJsonObject{{"dumpId", dumpId}, {"address", parts[2]}});
        if (kind == "disasm" && parts.size() >= 3) return disassembleCoredump(QJsonObject{{"dumpId", dumpId}, {"address", parts[2]}});
    }
    if (host == "source") {
        QString payload = QUrl::fromPercentEncoding(url.path().mid(1).toUtf8());
        int split = payload.lastIndexOf(':');
        if (split > 0) {
            QString path = payload.left(split);
            int line = payload.mid(split + 1).toInt();
            return getSourceContext(QJsonObject{{"path", path}, {"line", line}});
        }
    }
    return toolError("Resource not found");
}
