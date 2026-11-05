#include "cache.hpp"

#include <dev/firmware/archive.hpp>
#include <net/wki/auth_crypto.hpp>

namespace ker::dev::firmware::detail {
auto cache_policy_error(const Policy& policy, uint32_t required_revision) -> Error {
    if (policy.name.data() == nullptr || policy.name.size() >= MAX_NAME_BYTES || !canonical_name(policy.name)) return Error::policy;
    if (policy.length > policy.maximum || policy.maximum > MAX_BLOB_BYTES) return Error::length;
    if (policy.revision != required_revision) return Error::revision;
    return Error::none;
}

auto cache_index_error(std::span<const ArchiveEntry> entries) -> Error {
    if (entries.size() > MAX_INDEX_ENTRIES) return Error::limit;
    if (!entries.empty() && entries.data() == nullptr) return Error::unsafe_source;
    for (size_t i = 0; i < entries.size(); ++i) {
        auto const& entry = entries[i];
        if (entry.name.data() == nullptr || entry.name.size() >= MAX_NAME_BYTES || !canonical_name(entry.name)) return Error::path;
        if (entry.bytes.size() > MAX_BLOB_BYTES) return Error::length;
        if (!entry.bytes.empty() && entry.bytes.data() == nullptr) return Error::unsafe_source;
        for (size_t prior = 0; prior < i; ++prior)
            if (entries[prior].name == entry.name) return Error::duplicate;
    }
    return Error::none;
}

auto cache_digest_matches(std::span<const uint8_t> bytes, const Digest& expected) -> bool {
    auto const ACTUAL = ker::net::wki::auth_crypto::sha256(bytes.data(), bytes.size());
    return ker::net::wki::auth_crypto::constant_time_equal(ACTUAL.data(), expected.data(), ACTUAL.size());
}
}  // namespace ker::dev::firmware::detail
