#pragma once

#include <QJsonArray>
#include <QJsonObject>
#include <QString>
#include <cstdint>
#include <vector>

struct AddressLookup {
    uint64_t fromAddress;
    uint64_t toAddress;
    uint64_t loadOffset;  // Runtime load offset - subtract from runtime address to get file offset
    QString symbolFilePath;

    AddressLookup() : fromAddress(0), toAddress(0), loadOffset(0) {}

    AddressLookup(uint64_t from, uint64_t to, const QString& path, uint64_t offset = 0)
        : fromAddress(from), toAddress(to), loadOffset(offset), symbolFilePath(path) {}

    // Check if an address falls within this lookup range
    bool containsAddress(uint64_t address) const { return address >= fromAddress && address <= toAddress; }

    // Convert runtime address to file-relative address for BFD lookups
    uint64_t toFileAddress(uint64_t runtimeAddress) const { return runtimeAddress - loadOffset; }
};

// Maps a binary name (as it appears in coredump filenames) to its ELF path
struct BinaryMapping {
    QString name;     // e.g. "httpd", "netd", "init"
    QString elfPath;  // e.g. "./build/modules/httpd/httpd"

    BinaryMapping() = default;
    BinaryMapping(const QString& n, const QString& p) : name(n), elfPath(p) {}
};

class Config {
   public:
    Config();

    // Load configuration from wosdbg.json if it exists
    bool loadFromFile(const QString& filePath = "wosdbg.json");

    // Save current configuration to file
    bool saveToFile(const QString& filePath = "wosdbg.json") const;

    // Get all address lookups
    const std::vector<AddressLookup>& getAddressLookups() const { return addressLookups; }

    // Find symbol file path for a given address
    QString findSymbolFileForAddress(uint64_t address) const;

    // Add a new address lookup
    void addAddressLookup(const AddressLookup& lookup);

    // Remove address lookup by index
    void removeAddressLookup(size_t index);

    // Clear all lookups
    void clearAddressLookups();

    // Coredump directory
    QString getCoredumpDirectory() const { return resolvePath(coredumpDirectory); }
    void setCoredumpDirectory(const QString& dir) { coredumpDirectory = dir; }

    // Binary mappings for coredump symbol auto-resolution
    const std::vector<BinaryMapping>& getBinaryMappings() const { return binaryMappings; }
    void addBinaryMapping(const BinaryMapping& mapping);
    void clearBinaryMappings();

    // Find ELF path for a binary name (from coredump filename)
    QString findElfPathForBinary(const QString& binaryName) const;

    // Resolve a potentially-relative path against the config file's directory
    QString resolvePath(const QString& path) const;

    // Get default configuration
    void loadDefaults();

    // Validate configuration
    bool isValid() const;

   private:
    std::vector<AddressLookup> addressLookups;
    QString coredumpDirectory = "./coredumps";
    std::vector<BinaryMapping> binaryMappings;
    QString configBaseDir;  // Directory containing the config file, for resolving relative paths

    // Helper functions for JSON parsing
    uint64_t parseAddress(const QString& addressStr) const;
    QString formatAddress(uint64_t address) const;
    AddressLookup parseAddressLookup(const QJsonObject& obj) const;
    QJsonObject serializeAddressLookup(const AddressLookup& lookup) const;
};

// Global configuration instance
class ConfigService {
   public:
    static ConfigService& instance();

    // Initialize the configuration service
    void initialize(const QString& configPath = "wosdbg.json");

    // Get the configuration
    const Config& getConfig() const { return config; }
    Config& getMutableConfig() { return config; }

    // Reload configuration from file
    bool reload();

    // Save configuration to file
    bool save();

    // Check if config file exists
    bool configFileExists() const;

   private:
    ConfigService() = default;
    Config config;
    QString configFilePath;
};
