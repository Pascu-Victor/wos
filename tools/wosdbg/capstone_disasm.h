#pragma once
// Shared CapstoneDisassembler - AT&T->Intel x86-64 disassembly conversion.
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
    auto operator=(const CapstoneDisassembler&) -> CapstoneDisassembler& = delete;

    /// Convert an AT&T-syntax assembly line (possibly with hex bytes)
    /// to Intel syntax.  Falls back to manual regex conversion if
    /// Capstone cannot decode.
    [[nodiscard]] auto convert_to_intel(const std::string& atnt_assembly) const -> std::string;

   private:
    csh handle;
    static auto extract_hex_bytes(const std::string& line) -> std::string;
    static auto hex_string_to_bytes(const std::string& hex) -> std::vector<uint8_t>;
    static auto manual_att_to_intel_conversion(const std::string& atnt_assembly) -> std::string;
};
