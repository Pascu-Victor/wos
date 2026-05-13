#pragma once

#include <qcontainerfwd.h>
#include <qtypes.h>

#include <QJsonArray>
#include <QJsonObject>
#include <QString>
#include <QStringList>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

struct AddressLookup {
    uint64_t from_address;
    uint64_t to_address;
    uint64_t load_offset;  // Runtime load offset - subtract from runtime address to get file offset
    QString symbol_file_path;

    AddressLookup() : from_address(0), to_address(0), load_offset(0) {}

    AddressLookup(uint64_t from, uint64_t to, QString path, uint64_t offset = 0)
        : from_address(from), to_address(to), load_offset(offset), symbol_file_path(std::move(path)) {}

    // Check if an address falls within this lookup range
    [[nodiscard]] bool contains_address(uint64_t address) const { return address >= from_address && address <= to_address; }

    // Convert runtime address to file-relative address for BFD lookups
    [[nodiscard]] uint64_t to_file_address(uint64_t runtime_address) const { return runtime_address - load_offset; }
};

// Maps a binary name (as it appears in coredump filenames) to its ELF path
struct BinaryMapping {
    QString name;      // e.g. "httpd", "netd", "init"
    QString elf_path;  // e.g. "./build/modules/httpd/httpd"

    BinaryMapping() = default;
    BinaryMapping(QString n, QString p) : name(std::move(n)), elf_path(std::move(p)) {}
};

struct McpSettings {
    QString bind_address = "127.0.0.1";
    quint16 port = 12346;
    QStringList allowed_cidrs = {"127.0.0.1/32", "::1/128"};
    QStringList allowed_roots;
    int max_entries = 200;
    int max_memory_bytes = 4096;
    int max_hits = 200;
    int max_string_length = 160;
    int source_window_lines = 8;
    int max_disassembly_instructions = 48;
};

class Config {
   public:
    Config();

    // Load configuration from wosdbg.json if it exists
    bool load_from_file(const QString& file_path = "wosdbg.json");

    // Save current configuration to file
    [[nodiscard]] bool save_to_file(const QString& file_path = "wosdbg.json") const;

    // Get all address lookups
    [[nodiscard]] const std::vector<AddressLookup>& get_address_lookups() const { return address_lookups; }

    // Find symbol file path for a given address
    [[nodiscard]] QString find_symbol_file_for_address(uint64_t address) const;

    // Add a new address lookup
    void add_address_lookup(const AddressLookup& lookup);

    // Remove address lookup by index
    void remove_address_lookup(size_t index);

    // Clear all lookups
    void clear_address_lookups();

    // Coredump directory
    [[nodiscard]] QString get_coredump_directory() const { return resolve_path(coredump_directory); }
    void set_coredump_directory(const QString& dir) { coredump_directory = dir; }

    // Binary mappings for coredump symbol auto-resolution
    [[nodiscard]] const std::vector<BinaryMapping>& get_binary_mappings() const { return binary_mappings; }
    void add_binary_mapping(const BinaryMapping& mapping);
    void clear_binary_mappings();

    // Find ELF path for a binary name (from coredump filename)
    [[nodiscard]] QString find_elf_path_for_binary(const QString& binary_name) const;

    // Resolve a potentially-relative path against the config file's directory
    [[nodiscard]] QString resolve_path(const QString& path) const;

    [[nodiscard]] const McpSettings& get_mcp_settings() const { return mcp_settings; }
    McpSettings& get_mutable_mcp_settings() { return mcp_settings; }

    // Get default configuration
    void load_defaults();

    // Validate configuration
    [[nodiscard]] bool is_valid() const;

   private:
    std::vector<AddressLookup> address_lookups;
    QString coredump_directory = "./coredumps";
    std::vector<BinaryMapping> binary_mappings;
    McpSettings mcp_settings;
    QString config_base_dir;  // Directory containing the config file, for resolving relative paths

    // Helper functions for JSON parsing
    static uint64_t parse_address(const QString& address_str);
    static QString format_address(uint64_t address);
    [[nodiscard]] static AddressLookup parse_address_lookup(const QJsonObject& obj);
    [[nodiscard]] static QJsonObject serialize_address_lookup(const AddressLookup& lookup);
    [[nodiscard]] McpSettings parse_mcp_settings(const QJsonObject& obj) const;
    static QJsonObject serialize_mcp_settings(const McpSettings& settings);
};

// Global configuration instance
class ConfigService {
   public:
    static ConfigService& instance();

    // Initialize the configuration service
    void initialize(const QString& config_path = "wosdbg.json");

    // Get the configuration
    [[nodiscard]] const Config& get_config() const { return config; }
    Config& get_mutable_config() { return config; }

    // Reload configuration from file
    bool reload();

    // Save configuration to file
    bool save();

    // Check if config file exists
    [[nodiscard]] bool config_file_exists() const;

   private:
    ConfigService() = default;
    Config config;
    QString config_file_path;
};
