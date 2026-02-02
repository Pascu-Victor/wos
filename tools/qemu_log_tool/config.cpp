#include "config.h"

#include <QDebug>
#include <QDir>
#include <QFile>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>

Config::Config() { loadDefaults(); }

bool Config::loadFromFile(const QString& filePath) {
    QFile file(filePath);
    if (!file.exists()) {
        qDebug() << "Config file" << filePath << "does not exist, using defaults";
        loadDefaults();
        return false;
    }

    if (!file.open(QIODevice::ReadOnly)) {
        qWarning() << "Could not open config file" << filePath << "for reading";
        loadDefaults();
        return false;
    }

    QByteArray data = file.readAll();
    QJsonParseError parseError;
    QJsonDocument doc = QJsonDocument::fromJson(data, &parseError);

    if (parseError.error != QJsonParseError::NoError) {
        qWarning() << "JSON parse error in config file:" << parseError.errorString();
        loadDefaults();
        return false;
    }

    if (!doc.isObject()) {
        qWarning() << "Config file root must be a JSON object";
        loadDefaults();
        return false;
    }

    QJsonObject root = doc.object();

    // Clear existing lookups
    addressLookups.clear();

    // Parse address lookups
    if (root.contains("lookups") && root["lookups"].isArray()) {
        QJsonArray lookupsArray = root["lookups"].toArray();

        for (const QJsonValue& value : lookupsArray) {
            if (value.isObject()) {
                try {
                    AddressLookup lookup = parseAddressLookup(value.toObject());
                    if (lookup.fromAddress <= lookup.toAddress && !lookup.symbolFilePath.isEmpty()) {
                        addressLookups.push_back(lookup);
                    } else {
                        qWarning() << "Invalid address lookup entry - skipping";
                    }
                } catch (const std::exception& e) {
                    qWarning() << "Error parsing address lookup:" << e.what();
                }
            }
        }
    }

    qDebug() << "Loaded" << addressLookups.size() << "address lookups from config file";
    return true;
}

bool Config::saveToFile(const QString& filePath) const {
    QJsonObject root;
    QJsonArray lookupsArray;

    // Serialize address lookups
    for (const auto& lookup : addressLookups) {
        lookupsArray.append(serializeAddressLookup(lookup));
    }

    root["lookups"] = lookupsArray;

    QJsonDocument doc(root);

    QFile file(filePath);
    if (!file.open(QIODevice::WriteOnly)) {
        qWarning() << "Could not open config file" << filePath << "for writing";
        return false;
    }

    file.write(doc.toJson());
    qDebug() << "Saved configuration to" << filePath;
    return true;
}

QString Config::findSymbolFileForAddress(uint64_t address) const {
    for (const auto& lookup : addressLookups) {
        if (lookup.containsAddress(address)) {
            return lookup.symbolFilePath;
        }
    }
    return QString();  // No matching lookup found
}

void Config::addAddressLookup(const AddressLookup& lookup) { addressLookups.push_back(lookup); }

void Config::removeAddressLookup(size_t index) {
    if (index < addressLookups.size()) {
        addressLookups.erase(addressLookups.begin() + index);
    }
}

void Config::clearAddressLookups() { addressLookups.clear(); }

void Config::loadDefaults() {
    addressLookups.clear();

    // Add some common default lookups that might be useful
    // These are examples and can be customized based on typical use cases

    // Example: User space applications (typical range)
    // addressLookups.emplace_back(0x400000, 0x800000, "./build/modules/init/init");

    // Example: Kernel space (x86_64 typical kernel range)
    addressLookups.emplace_back(0xffffffff80000000ULL, 0xffffffffffffffffULL, "./build/kernel/kernel.elf");

    // Example: Shared libraries (typical range)
    addressLookups.emplace_back(0x7f0000000000ULL, 0x7fffffffULL, "./build/lib/libc.so");

    qDebug() << "Loaded default configuration with" << addressLookups.size() << "address lookups";
}

bool Config::isValid() const {
    // Check for overlapping ranges
    for (size_t i = 0; i < addressLookups.size(); ++i) {
        const auto& lookup1 = addressLookups[i];

        // Check if range is valid
        if (lookup1.fromAddress > lookup1.toAddress) {
            return false;
        }

        // Check for overlaps with other ranges
        for (size_t j = i + 1; j < addressLookups.size(); ++j) {
            const auto& lookup2 = addressLookups[j];

            // Check if ranges overlap
            if (!(lookup1.toAddress < lookup2.fromAddress || lookup2.toAddress < lookup1.fromAddress)) {
                qWarning() << "Overlapping address ranges detected in configuration";
                return false;
            }
        }
    }

    return true;
}

uint64_t Config::parseAddress(const QString& addressStr) const {
    QString trimmed = addressStr.trimmed();
    bool ok = false;
    uint64_t address = 0;

    if (trimmed.startsWith("0x", Qt::CaseInsensitive)) {
        address = trimmed.toULongLong(&ok, 16);
    } else {
        address = trimmed.toULongLong(&ok, 10);
    }

    if (!ok) {
        throw std::runtime_error(QString("Invalid address format: %1").arg(addressStr).toStdString());
    }

    return address;
}

QString Config::formatAddress(uint64_t address) const { return QString("0x%1").arg(address, 0, 16); }

AddressLookup Config::parseAddressLookup(const QJsonObject& obj) const {
    AddressLookup lookup;

    if (!obj.contains("from") || !obj.contains("to") || !obj.contains("path")) {
        throw std::runtime_error("Address lookup must contain 'from', 'to', and 'path' fields");
    }

    lookup.fromAddress = parseAddress(obj["from"].toString());
    lookup.toAddress = parseAddress(obj["to"].toString());
    lookup.symbolFilePath = obj["path"].toString();

    // Load offset is optional - defaults to 0 (no offset, addresses match file)
    if (obj.contains("offset")) {
        lookup.loadOffset = parseAddress(obj["offset"].toString());
    } else {
        lookup.loadOffset = 0;
    }

    // Validate the path is not empty
    if (lookup.symbolFilePath.isEmpty()) {
        throw std::runtime_error("Symbol file path cannot be empty");
    }

    return lookup;
}

QJsonObject Config::serializeAddressLookup(const AddressLookup& lookup) const {
    QJsonObject obj;
    obj["from"] = formatAddress(lookup.fromAddress);
    obj["to"] = formatAddress(lookup.toAddress);
    obj["path"] = lookup.symbolFilePath;
    if (lookup.loadOffset != 0) {
        obj["offset"] = formatAddress(lookup.loadOffset);
    }
    return obj;
}

// ConfigService implementation
ConfigService& ConfigService::instance() {
    static ConfigService instance;
    return instance;
}

void ConfigService::initialize(const QString& configPath) {
    configFilePath = configPath;
    config.loadFromFile(configFilePath);
}

bool ConfigService::reload() { return config.loadFromFile(configFilePath); }

bool ConfigService::save() { return config.saveToFile(configFilePath); }

bool ConfigService::configFileExists() const { return QFile::exists(configFilePath); }
