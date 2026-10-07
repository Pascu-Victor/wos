#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::net::wki::rdma_crypto {

using Key = std::array<uint8_t, 32>;
using Nonce = std::array<uint8_t, 12>;
using Tag = std::array<uint8_t, 16>;

// RFC 8439 AEAD. The caller must never reuse a nonce with the same key.
// Inputs and metadata must remain stable throughout the call; callers must
// first snapshot untrusted shared memory into private storage. Buffers must be
// accessible for their declared lengths. Null is allowed only
// for an empty buffer. Exact input/output aliasing is supported; other overlap
// and writable overlap with key, nonce, AAD or tag are rejected. Rejection does
// not modify output or the seal tag. Open authenticates before writing output.
[[nodiscard]] auto seal(const Key& key, const Nonce& nonce, const void* aad, size_t aad_length, const void* input, size_t length,
                        void* output, Tag& tag) -> bool;
[[nodiscard]] auto open(const Key& key, const Nonce& nonce, const void* aad, size_t aad_length, const void* input, size_t length,
                        void* output, const Tag& tag) -> bool;

}  // namespace ker::net::wki::rdma_crypto
