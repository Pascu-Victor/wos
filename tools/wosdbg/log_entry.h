#pragma once

#include <QJsonObject>
#include <cstdint>
#include <string>
#include <vector>

enum class EntryType : uint8_t { INSTRUCTION, INTERRUPT, REGISTER, BLOCK, SEPARATOR, OTHER };

struct LogEntry {
    int line_number{0};
    EntryType type{EntryType::OTHER};
    std::string address;
    std::string function;
    std::string hex_bytes;
    std::string assembly;
    std::string original_line;
    uint64_t address_value{0};

    // For grouped entries (like interrupts)
    bool is_expanded{false};
    std::vector<LogEntry> child_entries;
    bool is_child{false};

    // Interrupt-specific fields
    std::string interrupt_number;
    std::string cpu_state_info;

    // Source code mapping
    std::string source_file;
    int source_line{0};

    // Canonical telemetry metadata is retained by local/backend parsers. The
    // legacy GUI QDataStream projection intentionally does not serialize
    // these fields, keeping that protocol byte-for-byte compatible.
    bool has_telemetry_envelope{false};
    std::string telemetry_json;
    QJsonObject telemetry_envelope;

    LogEntry() = default;
};
