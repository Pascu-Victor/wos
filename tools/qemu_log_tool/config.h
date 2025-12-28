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

class Config {
   public:
    Config();

    // Load configuration from logview.json if it exists
    bool loadFromFile(const QString& filePath = "logview.json");

    // Save current configuration to file
    bool saveToFile(const QString& filePath = "logview.json") const;

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

    // Get default configuration
    void loadDefaults();

    // Validate configuration
    bool isValid() const;

   private:
    std::vector<AddressLookup> addressLookups;

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
    void initialize(const QString& configPath = "logview.json");

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
