#pragma once

#include <cstdint>
#include <string>
#include <vector>

enum class EntryType { INSTRUCTION, INTERRUPT, REGISTER, BLOCK, SEPARATOR, OTHER };

struct LogEntry {
    int lineNumber;
    EntryType type;
    std::string address;
    std::string function;
    std::string hexBytes;
    std::string assembly;
    std::string originalLine;
    uint64_t addressValue;

    // For grouped entries (like interrupts)
    bool isExpanded;
    std::vector<LogEntry> childEntries;
    bool isChild;

    // Interrupt-specific fields
    std::string interruptNumber;
    std::string cpuStateInfo;

    // Source code mapping
    std::string sourceFile;
    int sourceLine;

    LogEntry() : lineNumber(0), type(EntryType::OTHER), addressValue(0), isExpanded(false), isChild(false), sourceLine(0) {}
};
