#pragma once

#include <QJsonArray>
#include <QJsonObject>
#include <QString>
#include <QStringList>
#include <QTemporaryDir>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace wosdbg {

// Bounds applied while snapshotting either a directory-form incident or an
// archive-form incident. The archive source itself is bounded separately from
// its expanded regular-file bytes.
struct IncidentLimits {
    size_t max_members = 4096;
    uint64_t max_member_bytes = 128ULL * 1024ULL * 1024ULL;
    uint64_t max_total_bytes = 512ULL * 1024ULL * 1024ULL;
    uint64_t max_archive_bytes = 256ULL * 1024ULL * 1024ULL;
    uint64_t max_manifest_bytes = 1024ULL * 1024ULL;
    size_t max_path_length = 512;
    size_t max_path_depth = 24;
    uint64_t max_expansion_ratio = 200;
    QStringList allowed_roots;
};

// Stable machine-readable validation finding. Callers should branch on code,
// not message. Fatal issues make IncidentBundle::valid() false.
struct IncidentIssue {
    QString code;
    QString message;
    QString path;
    bool fatal = true;
    QJsonObject details;
};

// One manifest-declared evidence member. absolute_path always points into the
// loader-owned immutable snapshot and is empty for an intentionally missing
// optional member.
struct IncidentMember {
    QString id;
    QString path;
    QString kind;
    uint64_t size = 0;
    QString sha256;
    bool required = false;
    bool truncated = false;
    QString node_id;
    QString build_id;
    QString binary;
    QString source_name;
    QString state;
    QString absolute_path;
    QJsonObject metadata;
};

struct IncidentBundle {
    QString source_path;
    QString root_path;
    QJsonObject manifest;
    std::vector<IncidentMember> members;
    std::vector<IncidentIssue> issues;
    std::shared_ptr<QTemporaryDir> storage;
    bool archive = false;

    [[nodiscard]] bool valid() const;
    [[nodiscard]] QString incident_id() const;
    [[nodiscard]] const IncidentMember* find_member(const QString& path) const;
};

// When limits.allowed_roots is non-empty, the loader verifies the already-open
// source descriptor against those canonical roots before reading bundle bytes.
// It then treats every byte/member as hostile: it rejects unsafe names and node
// types, snapshots regular files into a private QTemporaryDir, validates
// manifest.json format "wosincident" version 1, checks declared sizes/SHA-256
// values and required members, sorts inventory by path, and never executes
// bundle content.
//
// A non-null bundle is returned even when validation fails so callers can
// expose stable issue codes. Its snapshot is owned by IncidentBundle::storage
// and remains valid for the bundle's lifetime.
[[nodiscard]] std::unique_ptr<IncidentBundle> load_incident_bundle(const QString& source_path, const IncidentLimits& limits = {});

[[nodiscard]] QJsonObject incident_issue_to_json(const IncidentIssue& issue);
[[nodiscard]] QJsonObject incident_member_to_json(const IncidentMember& member, bool include_absolute_path = false);
[[nodiscard]] QJsonArray incident_issues_to_json(const IncidentBundle& bundle, size_t count = 200);
[[nodiscard]] QJsonObject incident_inventory_to_json(const IncidentBundle& bundle, size_t start = 0, size_t count = 200,
                                                     bool include_absolute_paths = false);

}  // namespace wosdbg
