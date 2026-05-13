#pragma once

#include <qcontainerfwd.h>
#include <qjsonvalue.h>
#include <qtmetamacros.h>

#include <QByteArray>
#include <QHash>
#include <QJsonArray>
#include <QJsonObject>
#include <QObject>
#include <QString>
#include <cstdint>
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

    void set_config(const Config& new_config);
    void reload_config();

    [[nodiscard]] QJsonObject status() const;
    static auto list_logs() -> QJsonObject;
    [[nodiscard]] QJsonObject load_log(const QJsonObject& args);
    [[nodiscard]] QJsonObject get_log_entries(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject search_log(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject get_log_context(const QJsonObject& args) const;

    [[nodiscard]] QJsonObject extract_coredumps(const QJsonObject& args);
    [[nodiscard]] QJsonObject list_coredumps() const;
    [[nodiscard]] QJsonObject open_coredump(const QJsonObject& args);
    [[nodiscard]] QJsonObject get_crash_summary(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject describe_registers(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject follow_register(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject search_coredump_memory(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject find_pointers(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject get_memory_context(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject disassemble_coredump(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject resolve_address_tool(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject analyze_coredump(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject backtrace_coredump(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject inspect_page_table(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject annotate_stack(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject decode_fault_instruction(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject get_source_context(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject verify_embedded_elf(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject check_elf_mapping(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject find_duplicate_pages(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject analyze_elf_integrity(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject elf_layout_summary(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject compare_expected_disassembly(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject scan_chunk_corruption(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject audit_executable_ptes(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject recognize_startup_stack(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject correlate_coredump_logs(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject reconstruct_wki_trace(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject explain_remote_exec_path(const QJsonObject& args) const;
    [[nodiscard]] QJsonObject diagnose_remote_exec_corruption(const QJsonObject& args) const;

    [[nodiscard]] QJsonArray list_resources() const;
    static QJsonArray list_resource_templates();
    [[nodiscard]] QJsonObject read_resource(const QString& uri) const;

   private:
    struct LogSession {
        QString id;
        QString path;
        QString display_name;
        std::vector<LogEntry> entries;
    };

    struct DumpSession {
        struct LoadedModule {
            QString name;
            QString path;
            QString role;
            QString build_id;
            QString address_model;
            QStringList alternate_paths;
            uint64_t base = 0;
            uint64_t end = 0;
            uint64_t first_load_vaddr = 0;
            bool memory_matched = false;
            std::unique_ptr<wosdbg::SymbolTable> symbols;
            std::unique_ptr<wosdbg::SectionMap> sections;
        };

        QString id;
        QString path;
        QString display_name;
        std::unique_ptr<wosdbg::CoreDump> dump;
        std::unique_ptr<wosdbg::SymbolTable> binary_symbols;
        std::unique_ptr<wosdbg::SectionMap> binary_sections;
        std::unique_ptr<wosdbg::SymbolTable> embedded_symbols;
        std::unique_ptr<wosdbg::SectionMap> embedded_sections;
        std::unique_ptr<wosdbg::SymbolTable> kernel_symbols;
        std::unique_ptr<wosdbg::SectionMap> kernel_sections;
        QString binary_elf_path;
        QString binary_build_id;
        QString embedded_build_id;
        QString kernel_elf_path;
        QString kernel_build_id;
        QString symbol_warning;
        bool binary_build_id_matches = true;
        std::vector<LoadedModule> modules;
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
        QString source_path;
        QString source_clickable;
        QString object_path;
        QString object_name;
        uint64_t object_base = 0;
        uint64_t object_offset = 0;
        int source_line = 0;
        int source_column = 0;
        int segment_index = -1;
        uint64_t segment_offset = 0;
        QString segment_type;
        QString ascii_preview;
        QJsonArray qword_preview;
        bool mapped = false;
        bool canonical = false;
    };

    [[nodiscard]] const LogSession* find_log_session(const QString& id) const;
    [[nodiscard]] const DumpSession* find_dump_session(const QString& id) const;
    DumpSession* find_dump_session(const QString& id);

    [[nodiscard]] QString resolve_path_for_read(const QString& path, const QString& fallback_dir = QString()) const;
    [[nodiscard]] bool is_path_allowed(const QString& path) const;
    [[nodiscard]] QStringList allowed_roots() const;
    static QString make_session_id(const QString& prefix, const QString& canonical_path);

    [[nodiscard]] QJsonObject log_entry_to_json(const LogEntry& entry, bool include_children = true) const;
    static QJsonObject log_summary_to_json(const LogSession& session);

    static QJsonObject coredump_summary_to_json(const DumpSession& session);
    [[nodiscard]] QJsonArray coredump_registers_to_json(const DumpSession& session, const QString& frame) const;
    static QJsonObject address_description_to_json(const AddressDescription& desc);
    [[nodiscard]] AddressDescription describe_address(const DumpSession& session, uint64_t value,
                                                      const QString& register_name = QString()) const;
    static QHash<QString, uint64_t> register_map(const wosdbg::CoreDump& dump, const QString& frame);
    static std::vector<wosdbg::SymbolTable*> symbol_tables(const DumpSession& session);
    static std::vector<wosdbg::SectionMap*> section_maps(const DumpSession& session);
    static const DumpSession::LoadedModule* module_for_address(const DumpSession& session, uint64_t address);
    void add_module(DumpSession& session, const QString& path, const QString& role, uint64_t base, bool memory_matched);
    void discover_modules(DumpSession& session);
    static std::optional<uint64_t> parse_address_value(const QJsonValue& value);
    static std::optional<uint64_t> resolve_address_argument(const DumpSession& session, const QJsonObject& args,
                                                            const QString& default_register = QString());
    [[nodiscard]] QJsonObject source_context_for_path(const QString& file_path, int line, int context_lines) const;
    static QString ascii_preview(const QByteArray& bytes, int max_len);
    static QJsonArray qword_preview(const wosdbg::CoreDump& dump, uint64_t address, int qwords);
    static std::optional<wosdbg::SymbolEntry> disassembly_start_symbol(const DumpSession& session, uint64_t address);
    static QJsonArray disassemble_at(const DumpSession& session, uint64_t address, int instruction_count);
    static QJsonObject pte_info_for_address(const DumpSession& session, uint64_t address);
    [[nodiscard]] QJsonArray stack_window(const DumpSession& session, uint64_t rsp, uint64_t rbp, int before_bytes, int after_bytes) const;
    [[nodiscard]] QJsonObject red_zone_report(const DumpSession& session, uint64_t rsp) const;
    [[nodiscard]] QJsonArray frame_pointer_backtrace(const DumpSession& session, const QString& frame, int max_frames) const;
    [[nodiscard]] QJsonObject faulting_instruction_report(const DumpSession& session) const;
    static QJsonObject classify_fault(const DumpSession& session, const QJsonObject& instruction);
    [[nodiscard]] QJsonObject compact_crash_summary(const DumpSession& session, const QJsonObject& instruction,
                                                    const QJsonObject& classification, int max_frames) const;
    static QJsonObject module_json(const DumpSession::LoadedModule& module);
    static QJsonObject source_location_for_address(const DumpSession& session, uint64_t address, const DumpSession::LoadedModule* module);
    static QByteArray module_elf_bytes(const DumpSession& session, const DumpSession::LoadedModule& module);

    static QJsonObject tool_error(const QString& message);
    static int bounded_int(const QJsonObject& args, const QString& key, int fallback, int min_value, int max_value);

    Config* config = nullptr;
    QHash<QString, std::shared_ptr<LogSession>> log_sessions;
    QHash<QString, std::shared_ptr<DumpSession>> dump_sessions;
};
