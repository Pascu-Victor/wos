#pragma once

#include <qtypes.h>

#include <QDataStream>
#include <QString>
#include <QVector>
#include <cstdint>

#include "config.h"
#include "log_entry.h"

enum class MessageType : uint8_t {
    HELLO = 1,
    WELCOME = 2,
    SELECT_FILE = 3,
    FILE_READY = 4,
    REQUEST_DATA = 5,
    DATA_RESPONSE = 6,
    ERROR = 7,
    PROGRESS = 8,
    SEARCH_REQUEST = 9,
    SEARCH_RESPONSE = 10,
    GET_INTERRUPTS_REQUEST = 11,
    GET_INTERRUPTS_RESPONSE = 12,
    SET_FILTER_REQUEST = 13,
    SET_FILTER_RESPONSE = 14,
    REQUEST_ROW_FOR_LINE = 15,
    ROW_FOR_LINE_RESPONSE = 16,
    OPEN_SOURCE_FILE = 17,
    REQUEST_FILE_LIST = 18,
    FILE_LIST_RESPONSE = 19,
    START_MCP_SERVER = 20,
    STOP_MCP_SERVER = 21,
    MCP_SERVER_STATUS_REQUEST = 22,
    MCP_SERVER_STATUS_RESPONSE = 23
};

// Serialization helpers for LogEntry
inline auto operator<<(QDataStream& out, const LogEntry& entry) -> QDataStream& {
    out << entry.line_number << static_cast<int>(entry.type) << QString::fromStdString(entry.address)
        << QString::fromStdString(entry.function) << QString::fromStdString(entry.hex_bytes) << QString::fromStdString(entry.assembly)
        << QString::fromStdString(entry.original_line) << static_cast<qulonglong>(entry.address_value) << entry.is_expanded
        << entry.is_child << QString::fromStdString(entry.interrupt_number) << QString::fromStdString(entry.cpu_state_info)
        << QString::fromStdString(entry.source_file) << entry.source_line;

    out << static_cast<quint32>(entry.child_entries.size());
    for (const auto& child : entry.child_entries) {
        out << child;
    }
    return out;
}

inline auto operator>>(QDataStream& in, LogEntry& entry) -> QDataStream& {
    int type_int;
    QString address;
    QString function;
    QString hex_bytes;
    QString assembly;
    QString original_line;
    QString interrupt_number;
    QString cpu_state_info;
    QString source_file;
    qulonglong address_value;
    quint32 child_count;
    int source_line;

    in >> entry.line_number >> type_int >> address >> function >> hex_bytes >> assembly >> original_line >> address_value >>
        entry.is_expanded >> entry.is_child >> interrupt_number >> cpu_state_info >> source_file >> source_line >> child_count;

    entry.type = static_cast<EntryType>(type_int);
    entry.address = address.toStdString();
    entry.function = function.toStdString();
    entry.hex_bytes = hex_bytes.toStdString();
    entry.assembly = assembly.toStdString();
    entry.original_line = original_line.toStdString();
    entry.address_value = static_cast<uint64_t>(address_value);
    entry.interrupt_number = interrupt_number.toStdString();
    entry.cpu_state_info = cpu_state_info.toStdString();
    entry.source_file = source_file.toStdString();
    entry.source_line = source_line;

    entry.child_entries.resize(child_count);
    for (quint32 i = 0; i < child_count; ++i) {
        in >> entry.child_entries[i];
    }
    return in;
}

// Serialization helpers for AddressLookup (Config)
inline auto operator<<(QDataStream& out, const AddressLookup& lookup) -> QDataStream& {
    out << static_cast<qulonglong>(lookup.from_address) << static_cast<qulonglong>(lookup.to_address)
        << static_cast<qulonglong>(lookup.load_offset) << lookup.symbol_file_path;
    return out;
}

inline auto operator>>(QDataStream& in, AddressLookup& lookup) -> QDataStream& {
    qulonglong from;
    qulonglong to;
    qulonglong load_offset;
    in >> from >> to >> load_offset >> lookup.symbol_file_path;
    lookup.from_address = static_cast<uint64_t>(from);
    lookup.to_address = static_cast<uint64_t>(to);
    lookup.load_offset = static_cast<uint64_t>(load_offset);
    return in;
}
