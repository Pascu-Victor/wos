#include "config.h"

#include <qcborvalue.h>
#include <qjsonparseerror.h>
#include <qlogging.h>
#include <qnamespace.h>
#include <qtypes.h>

#include <QDebug>
#include <QDir>
#include <QFile>
#include <QFileInfo>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <stdexcept>

Config::Config() { load_defaults(); }

bool Config::load_from_file(const QString& file_path) {
    QFile file(file_path);

    // Store the base directory for relative path resolution
    config_base_dir = QFileInfo(file_path).absolutePath();

    if (!file.exists()) {
        qDebug() << "Config file" << file_path << "does not exist, using defaults";
        load_defaults();
        return false;
    }

    if (!file.open(QIODevice::ReadOnly)) {
        qWarning() << "Could not open config file" << file_path << "for reading";
        load_defaults();
        return false;
    }

    QByteArray data = file.readAll();
    QJsonParseError parse_error;
    QJsonDocument doc = QJsonDocument::fromJson(data, &parse_error);

    if (parse_error.error != QJsonParseError::NoError) {
        qWarning() << "JSON parse error in config file:" << parse_error.errorString();
        load_defaults();
        return false;
    }

    if (!doc.isObject()) {
        qWarning() << "Config file root must be a JSON object";
        load_defaults();
        return false;
    }

    QJsonObject root = doc.object();

    // Clear existing lookups
    address_lookups.clear();

    // Parse address lookups
    if (root.contains("lookups") && root["lookups"].isArray()) {
        QJsonArray lookups_array = root["lookups"].toArray();

        for (const auto& value : lookups_array) {
            if (value.isObject()) {
                try {
                    AddressLookup lookup = parse_address_lookup(value.toObject());
                    if (lookup.from_address <= lookup.to_address && !lookup.symbol_file_path.isEmpty()) {
                        address_lookups.push_back(lookup);
                    } else {
                        qWarning() << "Invalid address lookup entry - skipping";
                    }
                } catch (const std::exception& e) {
                    qWarning() << "Error parsing address lookup:" << e.what();
                }
            }
        }
    }

    qDebug() << "Loaded" << address_lookups.size() << "address lookups from config file";

    // Parse coredump directory
    if (root.contains("coredumpDirectory") && root["coredumpDirectory"].isString()) {
        coredump_directory = root["coredumpDirectory"].toString();
    }

    // Parse binary mappings
    binary_mappings.clear();
    if (root.contains("binaries") && root["binaries"].isArray()) {
        QJsonArray binaries_array = root["binaries"].toArray();
        for (const auto& value : binaries_array) {
            if (value.isObject()) {
                QJsonObject obj = value.toObject();
                if (obj.contains("name") && obj.contains("path")) {
                    binary_mappings.emplace_back(obj["name"].toString(), obj["path"].toString());
                }
            }
        }
    }
    qDebug() << "Loaded" << binary_mappings.size() << "binary mappings,"
             << "coredump directory:" << coredump_directory;

    if (root.contains("mcp") && root["mcp"].isObject()) {
        mcp_settings = parse_mcp_settings(root["mcp"].toObject());
    } else {
        mcp_settings = McpSettings{};
    }

    return true;
}

bool Config::save_to_file(const QString& file_path) const {
    QJsonObject root;
    QJsonArray lookups_array;

    // Serialize address lookups
    for (const auto& lookup : address_lookups) {
        lookups_array.append(serialize_address_lookup(lookup));
    }

    root["lookups"] = lookups_array;

    // Serialize coredump directory
    root["coredumpDirectory"] = coredump_directory;

    // Serialize binary mappings
    QJsonArray binaries_array;
    for (const auto& mapping : binary_mappings) {
        QJsonObject obj;
        obj["name"] = mapping.name;
        obj["path"] = mapping.elf_path;
        binaries_array.append(obj);
    }
    root["binaries"] = binaries_array;

    root["mcp"] = serialize_mcp_settings(mcp_settings);

    QJsonDocument doc(root);

    QFile file(file_path);
    if (!file.open(QIODevice::WriteOnly)) {
        qWarning() << "Could not open config file" << file_path << "for writing";
        return false;
    }

    file.write(doc.toJson());
    qDebug() << "Saved configuration to" << file_path;
    return true;
}

QString Config::find_symbol_file_for_address(uint64_t address) const {
    for (const auto& lookup : address_lookups) {
        if (lookup.contains_address(address)) {
            return resolve_path(lookup.symbol_file_path);
        }
    }
    return {};  // No matching lookup found
}

void Config::add_address_lookup(const AddressLookup& lookup) { address_lookups.push_back(lookup); }

void Config::remove_address_lookup(size_t index) {
    if (index < address_lookups.size()) {
        address_lookups.erase(address_lookups.begin() + static_cast<std::ptrdiff_t>(index));
    }
}

void Config::clear_address_lookups() { address_lookups.clear(); }

void Config::add_binary_mapping(const BinaryMapping& mapping) { binary_mappings.push_back(mapping); }

void Config::clear_binary_mappings() { binary_mappings.clear(); }

QString Config::find_elf_path_for_binary(const QString& binary_name) const {
    for (const auto& mapping : binary_mappings) {
        if (mapping.name == binary_name) {
            return resolve_path(mapping.elf_path);
        }
    }
    return {};
}

QString Config::resolve_path(const QString& path) const {
    if (path.isEmpty()) {
        return path;
    }
    QFileInfo fi(path);
    if (fi.isAbsolute()) {
        return path;
    }
    if (config_base_dir.isEmpty()) {
        return path;
    }
    return QDir(config_base_dir).absoluteFilePath(path);
}

void Config::load_defaults() {
    address_lookups.clear();
    mcp_settings = McpSettings{};

    // Add some common default lookups that might be useful
    // These are examples and can be customized based on typical use cases

    // Example: User space applications (typical range)
    // address_lookups.emplace_back(0x400000, 0x800000, "./build/modules/init/init");

    // Example: Kernel space (x86_64 typical kernel range)
    address_lookups.emplace_back(0xffffffff80000000ULL, 0xffffffffffffffffULL, "./build/modules/kern/wos");

    // Example: Shared libraries (typical range)
    address_lookups.emplace_back(0x7f0000000000ULL, 0x7fffffffULL, "./build/lib/libc.so");

    qDebug() << "Loaded default configuration with" << address_lookups.size() << "address lookups";
}

bool Config::is_valid() const {
    // Check for overlapping ranges
    for (size_t i = 0; i < address_lookups.size(); ++i) {
        const auto& lookup1 = address_lookups[i];

        // Check if range is valid
        if (lookup1.from_address > lookup1.to_address) {
            return false;
        }

        // Check for overlaps with other ranges
        for (size_t j = i + 1; j < address_lookups.size(); ++j) {
            const auto& lookup2 = address_lookups[j];

            // Check if ranges overlap
            if (lookup1.to_address >= lookup2.from_address && lookup2.to_address >= lookup1.from_address) {
                qWarning() << "Overlapping address ranges detected in configuration";
                return false;
            }
        }
    }

    return true;
}

uint64_t Config::parse_address(const QString& address_str) {
    QString trimmed = address_str.trimmed();
    bool ok = false;
    uint64_t address = 0;

    if (trimmed.startsWith("0x", Qt::CaseInsensitive)) {
        address = trimmed.toULongLong(&ok, 16);
    } else {
        address = trimmed.toULongLong(&ok, 10);
    }

    if (!ok) {
        throw std::runtime_error(QString("Invalid address format: %1").arg(address_str).toStdString());
    }

    return address;
}

auto Config::format_address(uint64_t address) -> QString { return QString("0x%1").arg(address, 0, 16); }

auto Config::parse_address_lookup(const QJsonObject& obj) -> AddressLookup {
    AddressLookup lookup;

    if (!obj.contains("from") || !obj.contains("to") || !obj.contains("path")) {
        throw std::runtime_error("Address lookup must contain 'from', 'to', and 'path' fields");
    }

    lookup.from_address = parse_address(obj["from"].toString());
    lookup.to_address = parse_address(obj["to"].toString());
    lookup.symbol_file_path = obj["path"].toString();

    // Load offset is optional - defaults to 0 (no offset, addresses match file)
    if (obj.contains("offset")) {
        lookup.load_offset = parse_address(obj["offset"].toString());
    } else {
        lookup.load_offset = 0;
    }

    // Validate the path is not empty
    if (lookup.symbol_file_path.isEmpty()) {
        throw std::runtime_error("Symbol file path cannot be empty");
    }

    return lookup;
}

auto Config::serialize_address_lookup(const AddressLookup& lookup) -> QJsonObject {
    QJsonObject obj;
    obj["from"] = format_address(lookup.from_address);
    obj["to"] = format_address(lookup.to_address);
    obj["path"] = lookup.symbol_file_path;
    if (lookup.load_offset != 0) {
        obj["offset"] = format_address(lookup.load_offset);
    }
    return obj;
}

McpSettings Config::parse_mcp_settings(const QJsonObject& obj) const {
    McpSettings settings;
    if (obj.contains("bindAddress")) {
        settings.bind_address = obj["bindAddress"].toString(settings.bind_address);
    }
    if (obj.contains("port")) {
        settings.port = static_cast<quint16>(std::clamp(obj["port"].toInt(settings.port), 1, 65535));
    }
    if (obj.contains("allowedCidrs") && obj["allowedCidrs"].isArray()) {
        settings.allowed_cidrs.clear();
        for (const auto& value : obj["allowedCidrs"].toArray()) {
            if (value.isString()) {
                settings.allowed_cidrs << value.toString();
            }
        }
    }
    if (obj.contains("allowedRoots") && obj["allowedRoots"].isArray()) {
        settings.allowed_roots.clear();
        for (const auto& value : obj["allowedRoots"].toArray()) {
            if (value.isString()) {
                settings.allowed_roots << resolve_path(value.toString());
            }
        }
    }
    settings.max_entries = std::clamp(obj["maxEntries"].toInt(settings.max_entries), 1, 5000);
    settings.max_memory_bytes = std::clamp(obj["maxMemoryBytes"].toInt(settings.max_memory_bytes), 256, 1024 * 1024);
    settings.max_hits = std::clamp(obj["maxHits"].toInt(settings.max_hits), 1, 10000);
    settings.max_string_length = std::clamp(obj["maxStringLength"].toInt(settings.max_string_length), 16, 4096);
    settings.source_window_lines = std::clamp(obj["sourceWindowLines"].toInt(settings.source_window_lines), 0, 200);
    settings.max_disassembly_instructions =
        std::clamp(obj["maxDisassemblyInstructions"].toInt(settings.max_disassembly_instructions), 1, 512);
    return settings;
}

QJsonObject Config::serialize_mcp_settings(const McpSettings& settings) {
    QJsonObject obj;
    obj["bindAddress"] = settings.bind_address;
    obj["port"] = static_cast<int>(settings.port);
    QJsonArray allowed_cidrs;
    for (const auto& cidr : settings.allowed_cidrs) {
        allowed_cidrs.append(cidr);
    }
    obj["allowedCidrs"] = allowed_cidrs;
    QJsonArray allowed_roots;
    for (const auto& root : settings.allowed_roots) {
        allowed_roots.append(root);
    }
    obj["allowedRoots"] = allowed_roots;
    obj["maxEntries"] = settings.max_entries;
    obj["maxMemoryBytes"] = settings.max_memory_bytes;
    obj["maxHits"] = settings.max_hits;
    obj["maxStringLength"] = settings.max_string_length;
    obj["sourceWindowLines"] = settings.source_window_lines;
    obj["maxDisassemblyInstructions"] = settings.max_disassembly_instructions;
    return obj;
}

// ConfigService implementation
ConfigService& ConfigService::instance() {
    static ConfigService instance;
    return instance;
}

void ConfigService::initialize(const QString& config_path) {
    config_file_path = config_path;
    config.load_from_file(config_file_path);
}

bool ConfigService::reload() { return config.load_from_file(config_file_path); }

bool ConfigService::save() { return config.save_to_file(config_file_path); }

bool ConfigService::config_file_exists() const { return QFile::exists(config_file_path); }
