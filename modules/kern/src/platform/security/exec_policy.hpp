#pragma once

#include <array>
#include <cstdint>
#include <platform/security/credentials.hpp>

namespace ker::mod::security::exec_policy {

inline constexpr size_t IDENTITY_AUXV_WORDS = 10;

// Existing ID differences remain privileged execution even under NNP. Explicit
// service grants also require the loader to ignore caller-controlled settings;
// ordinary equal-ID root execution does not acquire authority through exec.
[[nodiscard]] constexpr auto requires_secure_loader(const Snapshot& credentials) -> bool {
    return credentials.user().real != credentials.user().effective || credentials.group().real != credentials.group().effective ||
           credentials.allows_service(ServicePrivilege::JOURNAL_READ) || credentials.allows_service(ServicePrivilege::NETWORK_ADMIN);
}

// System V auxiliary-vector keys. The caller appends AT_NULL after all entries.
[[nodiscard]] constexpr auto identity_auxv(const Snapshot& credentials) -> std::array<uint64_t, IDENTITY_AUXV_WORDS> {
    constexpr uint64_t UID_KEY = 11, EUID_KEY = 12, GID_KEY = 13, EGID_KEY = 14, SECURE_KEY = 23;
    return {UID_KEY,  credentials.user().real,       EUID_KEY,   credentials.user().effective,       GID_KEY, credentials.group().real,
            EGID_KEY, credentials.group().effective, SECURE_KEY, requires_secure_loader(credentials)};
}

}  // namespace ker::mod::security::exec_policy
