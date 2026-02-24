#pragma once
// Shared CapstoneDisassembler — AT&T→Intel x86-64 disassembly conversion.
// Used by both wosdbg (GUI) and wosdbg_worker (headless).

#include <capstone/capstone.h>

#include <QtCore/QRegularExpression>
#include <QtCore/QString>
#include <cstdint>
#include <string>
#include <vector>

class CapstoneDisassembler {
   public:
    CapstoneDisassembler();
    ~CapstoneDisassembler();

    // Non-copyable
    CapstoneDisassembler(const CapstoneDisassembler&) = delete;
    CapstoneDisassembler& operator=(const CapstoneDisassembler&) = delete;

    /// Convert an AT&T-syntax assembly line (possibly with hex bytes)
    /// to Intel syntax.  Falls back to manual regex conversion if
    /// Capstone cannot decode.
    [[nodiscard]] std::string convertToIntel(const std::string& atntAssembly) const;

   private:
    csh handle;
    static std::string extractHexBytes(const std::string& line);
    static std::vector<uint8_t> hexStringToBytes(const std::string& hex);
    static std::string manualATTToIntelConversion(const std::string& atntAssembly);
};
