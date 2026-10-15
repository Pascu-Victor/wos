#pragma once

#include <stddef.h>
#include <stdint.h>

#include <type_traits>

// Fixed-layout local WOS NUMA records. Node IDs are dense boot-local IDs;
// these records never contain physical, HHDM, Task or root addresses.
namespace ker::abi::vmem::numa {
inline constexpr uint32_t VERSION = 1;
inline constexpr uint32_t MAX_PAGE_NODES = 64;
inline constexpr uint32_t PAGE_BYTES = 4096;
inline constexpr uint32_t CPU_MASK_WORDS = 4;
inline constexpr uint16_t INVALID_NODE = UINT16_MAX;
inline constexpr uint64_t FEATURE_RESIDENT_PAGE_NODES = uint64_t{1} << 0;
inline constexpr uint64_t FEATURE_LOCAL_POLICY_SETTER = uint64_t{1} << 1;
inline constexpr uint64_t FEATURES_V1 = FEATURE_RESIDENT_PAGE_NODES | FEATURE_LOCAL_POLICY_SETTER;

enum class QueryKind : uint32_t { SUMMARY = 0, NODE = 1, SELF_POLICY = 2, PAGE_NODES = 3 };
enum class PolicyMode : uint32_t { LOCAL = 0, PREFERRED = 1, BIND = 2, INTERLEAVE = 3 };
// Rejected-firmware fallback is distinguished using permanent BootStatus; no
// firmware address, parser offset or rejected table bytes are exposed.
enum class Provenance : uint32_t { UMA_MISSING_SRAT = 0, SRAT_DEFAULT_DISTANCES = 1, SRAT_SLIT = 2, UMA_REJECTED_FIRMWARE = 3 };
enum class PageStatus : uint16_t { RESIDENT = 0, UNAVAILABLE = 1 };

struct QueryRequest {
    uint32_t version;
    uint32_t bytes;
    uint32_t kind;
    uint32_t flags;
    uint64_t index;
    uint64_t address;
    uint32_t count;
    uint32_t reserved32_a;
    uint64_t output_address;
    uint32_t output_bytes;
    uint32_t reserved32_b;
    uint64_t reserved64;
};
struct PolicyRequest {
    uint32_t version;
    uint32_t bytes;
    uint32_t mode;
    uint32_t flags;
    uint64_t nodes;
    uint64_t reserved64;
};
struct Summary {
    uint32_t version;
    uint32_t bytes;
    uint64_t features;
    uint32_t provenance;
    uint32_t node_count;
    uint32_t cpu_count;
    uint32_t page_bytes;
    uint32_t max_page_nodes;
    uint32_t cpu_mask_words;
    uint64_t reserved64[3];
};
struct Node {
    uint32_t version;
    uint32_t bytes;
    uint16_t node;
    uint16_t reserved16;
    uint32_t proximity_id;
    uint64_t managed_bytes;
    uint64_t cpu_words[4];
    uint64_t reserved64;
};
struct SelfPolicy {
    uint32_t version;
    uint32_t bytes;
    uint32_t mode;
    uint32_t reserved32;
    uint64_t nodes;
    uint64_t reserved64;
};
struct PageNode {
    uint16_t node;
    uint16_t status;
    uint32_t reserved32;
};

static_assert(sizeof(QueryKind) == 4 && sizeof(PolicyMode) == 4 && sizeof(Provenance) == 4 && sizeof(PageStatus) == 2);

static_assert(sizeof(QueryRequest) == 64 && alignof(QueryRequest) == 8);
static_assert(std::is_standard_layout_v<QueryRequest> && std::is_trivially_copyable_v<QueryRequest>);
static_assert(offsetof(QueryRequest, version) == 0);
static_assert(offsetof(QueryRequest, bytes) == 4);
static_assert(offsetof(QueryRequest, kind) == 8);
static_assert(offsetof(QueryRequest, flags) == 12);
static_assert(offsetof(QueryRequest, index) == 16);
static_assert(offsetof(QueryRequest, address) == 24);
static_assert(offsetof(QueryRequest, count) == 32);
static_assert(offsetof(QueryRequest, reserved32_a) == 36);
static_assert(offsetof(QueryRequest, output_address) == 40);
static_assert(offsetof(QueryRequest, output_bytes) == 48);
static_assert(offsetof(QueryRequest, reserved32_b) == 52);
static_assert(offsetof(QueryRequest, reserved64) == 56);

static_assert(sizeof(PolicyRequest) == 32 && alignof(PolicyRequest) == 8);
static_assert(std::is_standard_layout_v<PolicyRequest> && std::is_trivially_copyable_v<PolicyRequest>);
static_assert(offsetof(PolicyRequest, version) == 0);
static_assert(offsetof(PolicyRequest, bytes) == 4);
static_assert(offsetof(PolicyRequest, mode) == 8);
static_assert(offsetof(PolicyRequest, flags) == 12);
static_assert(offsetof(PolicyRequest, nodes) == 16);
static_assert(offsetof(PolicyRequest, reserved64) == 24);

static_assert(sizeof(Summary) == 64 && alignof(Summary) == 8);
static_assert(std::is_standard_layout_v<Summary> && std::is_trivially_copyable_v<Summary>);
static_assert(offsetof(Summary, version) == 0);
static_assert(offsetof(Summary, bytes) == 4);
static_assert(offsetof(Summary, features) == 8);
static_assert(offsetof(Summary, provenance) == 16);
static_assert(offsetof(Summary, node_count) == 20);
static_assert(offsetof(Summary, cpu_count) == 24);
static_assert(offsetof(Summary, page_bytes) == 28);
static_assert(offsetof(Summary, max_page_nodes) == 32);
static_assert(offsetof(Summary, cpu_mask_words) == 36);
static_assert(offsetof(Summary, reserved64) == 40);

static_assert(sizeof(Node) == 64 && alignof(Node) == 8);
static_assert(std::is_standard_layout_v<Node> && std::is_trivially_copyable_v<Node>);
static_assert(offsetof(Node, version) == 0);
static_assert(offsetof(Node, bytes) == 4);
static_assert(offsetof(Node, node) == 8);
static_assert(offsetof(Node, reserved16) == 10);
static_assert(offsetof(Node, proximity_id) == 12);
static_assert(offsetof(Node, managed_bytes) == 16);
static_assert(offsetof(Node, cpu_words) == 24);
static_assert(offsetof(Node, reserved64) == 56);

static_assert(sizeof(SelfPolicy) == 32 && alignof(SelfPolicy) == 8);
static_assert(std::is_standard_layout_v<SelfPolicy> && std::is_trivially_copyable_v<SelfPolicy>);
static_assert(offsetof(SelfPolicy, version) == 0);
static_assert(offsetof(SelfPolicy, bytes) == 4);
static_assert(offsetof(SelfPolicy, mode) == 8);
static_assert(offsetof(SelfPolicy, reserved32) == 12);
static_assert(offsetof(SelfPolicy, nodes) == 16);
static_assert(offsetof(SelfPolicy, reserved64) == 24);

static_assert(sizeof(PageNode) == 8 && alignof(PageNode) == 4);
static_assert(std::is_standard_layout_v<PageNode> && std::is_trivially_copyable_v<PageNode>);
static_assert(offsetof(PageNode, node) == 0);
static_assert(offsetof(PageNode, status) == 2);
static_assert(offsetof(PageNode, reserved32) == 4);

}  // namespace ker::abi::vmem::numa
