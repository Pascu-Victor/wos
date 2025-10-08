#pragma once

#include <QString>
#include <QJsonObject>
#include <QJsonArray>
#include <vector>
#include <cstdint>

struct AddressLookup {
    uint64_t fromAddress;
    uint64_t toAddress;
    QString symbolFilePath;
    
    AddressLookup() : fromAddress(0), toAddress(0) {}
    
    AddressLookup(uint64_t from, uint64_t to, const QString& path)
        : fromAddress(from), toAddress(to), symbolFilePath(path) {}
    
    // Check if an address falls within this lookup range
    bool containsAddress(uint64_t address) const {
        return address >= fromAddress && address <= toAddress;
    }
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
