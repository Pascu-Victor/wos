#pragma once

#include <QDataStream>
#include <QString>
#include <QVector>
#include <cstdint>

#include "config.h"
#include "log_entry.h"

enum class MessageType : uint8_t {
    Hello = 1,
    Welcome = 2,
    SelectFile = 3,
    FileReady = 4,
    RequestData = 5,
    DataResponse = 6,
    Error = 7,
    Progress = 8,
    SearchRequest = 9,
    SearchResponse = 10,
    GetInterruptsRequest = 11,
    GetInterruptsResponse = 12,
    SetFilterRequest = 13,
    SetFilterResponse = 14,
    RequestRowForLine = 15,
    RowForLineResponse = 16,
    OpenSourceFile = 17,
    RequestFileList = 18,
    FileListResponse = 19
};

// Serialization helpers for LogEntry
inline QDataStream& operator<<(QDataStream& out, const LogEntry& entry) {
    out << entry.lineNumber << static_cast<int>(entry.type) << QString::fromStdString(entry.address)
        << QString::fromStdString(entry.function) << QString::fromStdString(entry.hexBytes) << QString::fromStdString(entry.assembly)
        << QString::fromStdString(entry.originalLine) << static_cast<qulonglong>(entry.addressValue) << entry.isExpanded << entry.isChild
        << QString::fromStdString(entry.interruptNumber) << QString::fromStdString(entry.cpuStateInfo)
        << QString::fromStdString(entry.sourceFile) << entry.sourceLine;

    out << static_cast<quint32>(entry.childEntries.size());
    for (const auto& child : entry.childEntries) {
        out << child;
    }
    return out;
}

inline QDataStream& operator>>(QDataStream& in, LogEntry& entry) {
    int typeInt;
    QString address, function, hexBytes, assembly, originalLine, interruptNumber, cpuStateInfo, sourceFile;
    qulonglong addressValue;
    quint32 childCount;
    int sourceLine;

    in >> entry.lineNumber >> typeInt >> address >> function >> hexBytes >> assembly >> originalLine >> addressValue >> entry.isExpanded >>
        entry.isChild >> interruptNumber >> cpuStateInfo >> sourceFile >> sourceLine >> childCount;

    entry.type = static_cast<EntryType>(typeInt);
    entry.address = address.toStdString();
    entry.function = function.toStdString();
    entry.hexBytes = hexBytes.toStdString();
    entry.assembly = assembly.toStdString();
    entry.originalLine = originalLine.toStdString();
    entry.addressValue = static_cast<uint64_t>(addressValue);
    entry.interruptNumber = interruptNumber.toStdString();
    entry.cpuStateInfo = cpuStateInfo.toStdString();
    entry.sourceFile = sourceFile.toStdString();
    entry.sourceLine = sourceLine;

    entry.childEntries.resize(childCount);
    for (quint32 i = 0; i < childCount; ++i) {
        in >> entry.childEntries[i];
    }
    return in;
}

// Serialization helpers for AddressLookup (Config)
inline QDataStream& operator<<(QDataStream& out, const AddressLookup& lookup) {
    out << static_cast<qulonglong>(lookup.fromAddress) << static_cast<qulonglong>(lookup.toAddress) << lookup.symbolFilePath;
    return out;
}

inline QDataStream& operator>>(QDataStream& in, AddressLookup& lookup) {
    qulonglong from, to;
    in >> from >> to >> lookup.symbolFilePath;
    lookup.fromAddress = static_cast<uint64_t>(from);
    lookup.toAddress = static_cast<uint64_t>(to);
    return in;
}
