#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::vfs::lookup_model {

// A fixed-storage state model for the VFS lookup contract.  It deliberately
// has no dependency on VFS, scheduler, allocation, or locking primitives so
// the same transition model can be used by host tests and kernel KTEST.
constexpr uint16_t INVALID_NODE = UINT16_MAX;
constexpr uint8_t INVALID_MOUNT = UINT8_MAX;
constexpr uint16_t INVALID_NAME = 0;
constexpr size_t MAX_MOUNTS = 4;
constexpr size_t MAX_NODES = 24;
constexpr size_t MAX_DENTRIES = 48;
constexpr uint8_t MAX_SYMLINK_DEPTH = 8;
constexpr uint8_t DEFAULT_MAX_RESTARTS = 3;

using NodeId = uint16_t;
using MountId = uint8_t;
using NameId = uint16_t;

enum class NodeKind : uint8_t { FILE, DIRECTORY, SYMLINK };

enum class Result : uint8_t {
    OK,
    NOT_FOUND,
    NOT_DIRECTORY,
    NOT_SYMLINK,
    EXISTS,
    INVALID_ARGUMENT,
    INVALID_HANDLE,
    STALE_HANDLE,
    STALE_BINDING,
    MOUNT_RETIRED,
    BUSY,
    ROOT_ESCAPE,
    SYMLINK_DEPTH,
    RESTART,
    RETRY_LIMIT,
};

enum class ValidationUse : uint8_t { READ_OBJECT, CONTINUE_WALK, MUTATION_COMMIT };

struct MountIdentity {
    MountId id{INVALID_MOUNT};
    uint64_t generation{};

    [[nodiscard]] constexpr auto valid() const -> bool { return id != INVALID_MOUNT && generation != 0; }
    constexpr auto operator==(MountIdentity const&) const -> bool = default;
};

struct NodeIdentity {
    NodeId id{INVALID_NODE};
    uint64_t generation{};

    [[nodiscard]] constexpr auto valid() const -> bool { return id != INVALID_NODE && generation != 0; }
    constexpr auto operator==(NodeIdentity const&) const -> bool = default;
};

struct LookupToken {
    MountIdentity mount{};
    NodeIdentity object{};
    NodeIdentity parent{};
    NodeIdentity root{};
    NameId name{INVALID_NAME};
    uint64_t parent_directory_generation{};
    bool observes_binding{};
};

struct LookupHandle {
    LookupToken token{};
    bool retained{};
};

struct MountState {
    bool allocated{};
    bool accepting_lookups{};
    bool retiring{};
    uint64_t generation{};
    uint32_t references{};
    NodeId root{INVALID_NODE};
};

struct NodeState {
    bool allocated{};
    uint64_t generation{};
    MountIdentity mount{};
    NodeKind kind{NodeKind::FILE};
    NodeId parent{INVALID_NODE};
    uint32_t references{};
    uint32_t links{};
    uint64_t directory_generation{};
    NodeIdentity symlink_target{};
};

struct DentryState {
    bool allocated{};
    NodeId parent{INVALID_NODE};
    NameId name{INVALID_NAME};
    NodeIdentity child{};
};

struct Model {
    std::array<MountState, MAX_MOUNTS> mounts{};
    std::array<NodeState, MAX_NODES> nodes{};
    std::array<DentryState, MAX_DENTRIES> dentries{};
};

struct RestartBudget {
    uint8_t maximum_restarts{DEFAULT_MAX_RESTARTS};
    uint8_t restarts{};
    uint8_t attempts{};
};

struct ParentOrder {
    MountIdentity first_mount{};
    NodeIdentity first{};
    MountIdentity second_mount{};
    NodeIdentity second{};
    bool same_parent{};
};

[[nodiscard]] constexpr auto next_generation(uint64_t generation) -> uint64_t {
    ++generation;
    return generation == 0 ? 1 : generation;
}

[[nodiscard]] constexpr auto mount_identity(Model const& model, MountId mount) -> MountIdentity {
    if (mount >= MAX_MOUNTS || !model.mounts[mount].allocated) {
        return {};
    }
    return {.id = mount, .generation = model.mounts[mount].generation};
}

[[nodiscard]] constexpr auto node_identity(Model const& model, NodeId node) -> NodeIdentity {
    if (node >= MAX_NODES || !model.nodes[node].allocated) {
        return {};
    }
    return {.id = node, .generation = model.nodes[node].generation};
}

[[nodiscard]] constexpr auto node_matches(Model const& model, NodeIdentity identity) -> bool {
    return identity.valid() && identity.id < MAX_NODES && model.nodes[identity.id].allocated &&
           model.nodes[identity.id].generation == identity.generation;
}

[[nodiscard]] constexpr auto mount_matches(Model const& model, MountIdentity identity) -> bool {
    return identity.valid() && identity.id < MAX_MOUNTS && model.mounts[identity.id].allocated &&
           model.mounts[identity.id].generation == identity.generation;
}

constexpr auto install_mount(Model& model, MountId mount) -> Result {
    if (mount >= MAX_MOUNTS) {
        return Result::INVALID_ARGUMENT;
    }
    auto& state = model.mounts[mount];
    if (state.allocated) {
        return Result::EXISTS;
    }
    state.allocated = true;
    state.accepting_lookups = true;
    state.retiring = false;
    state.generation = next_generation(state.generation);
    state.references = 0;
    state.root = INVALID_NODE;
    return Result::OK;
}

constexpr auto create_node(Model& model, MountId mount, NodeId node, NodeKind kind, NodeIdentity* identity_out = nullptr) -> Result {
    if (mount >= MAX_MOUNTS || node >= MAX_NODES || !model.mounts[mount].allocated || model.nodes[node].allocated) {
        return Result::INVALID_ARGUMENT;
    }
    auto& state = model.nodes[node];
    state.allocated = true;
    state.generation = next_generation(state.generation);
    state.mount = mount_identity(model, mount);
    state.kind = kind;
    state.parent = INVALID_NODE;
    state.references = 0;
    state.links = 0;
    state.directory_generation = kind == NodeKind::DIRECTORY ? 1 : 0;
    state.symlink_target = {};
    if (model.mounts[mount].root == INVALID_NODE) {
        if (kind != NodeKind::DIRECTORY) {
            state.allocated = false;
            return Result::NOT_DIRECTORY;
        }
        model.mounts[mount].root = node;
        state.links = 1;  // The mount root owns one non-dentry namespace link.
    }
    if (identity_out != nullptr) {
        *identity_out = node_identity(model, node);
    }
    return Result::OK;
}

[[nodiscard]] constexpr auto find_dentry(Model const& model, NodeId parent, NameId name) -> size_t {
    for (size_t index = 0; index < model.dentries.size(); ++index) {
        auto const& entry = model.dentries[index];
        if (entry.allocated && entry.parent == parent && entry.name == name) {
            return index;
        }
    }
    return MAX_DENTRIES;
}

[[nodiscard]] constexpr auto free_dentry(Model const& model) -> size_t {
    for (size_t index = 0; index < model.dentries.size(); ++index) {
        if (!model.dentries[index].allocated) {
            return index;
        }
    }
    return MAX_DENTRIES;
}

constexpr auto bind(Model& model, NodeIdentity parent, NameId name, NodeIdentity child) -> Result {
    if (name == INVALID_NAME || !node_matches(model, parent) || !node_matches(model, child)) {
        return Result::INVALID_ARGUMENT;
    }
    auto& parent_state = model.nodes[parent.id];
    auto& child_state = model.nodes[child.id];
    if (parent_state.kind != NodeKind::DIRECTORY) {
        return Result::NOT_DIRECTORY;
    }
    if (parent_state.mount != child_state.mount) {
        return Result::INVALID_ARGUMENT;
    }
    if (find_dentry(model, parent.id, name) != MAX_DENTRIES) {
        return Result::EXISTS;
    }
    size_t const SLOT = free_dentry(model);
    if (SLOT == MAX_DENTRIES) {
        return Result::BUSY;
    }
    model.dentries[SLOT] = {.allocated = true, .parent = parent.id, .name = name, .child = child};
    child_state.links++;
    if (child_state.kind == NodeKind::DIRECTORY) {
        child_state.parent = parent.id;
    }
    parent_state.directory_generation = next_generation(parent_state.directory_generation);
    return Result::OK;
}

constexpr auto unlink_binding(Model& model, NodeIdentity parent, NameId name) -> Result {
    if (!node_matches(model, parent) || model.nodes[parent.id].kind != NodeKind::DIRECTORY) {
        return Result::INVALID_ARGUMENT;
    }
    size_t const SLOT = find_dentry(model, parent.id, name);
    if (SLOT == MAX_DENTRIES) {
        return Result::NOT_FOUND;
    }
    NodeIdentity const CHILD = model.dentries[SLOT].child;
    model.dentries[SLOT] = {};
    if (node_matches(model, CHILD)) {
        auto& child = model.nodes[CHILD.id];
        if (child.links != 0) {
            --child.links;
        }
        if (child.kind == NodeKind::DIRECTORY && child.links == 0) {
            child.parent = INVALID_NODE;
        }
    }
    auto& parent_state = model.nodes[parent.id];
    parent_state.directory_generation = next_generation(parent_state.directory_generation);
    return Result::OK;
}

constexpr auto replace_binding(Model& model, NodeIdentity parent, NameId name, NodeIdentity replacement) -> Result {
    if (!node_matches(model, parent) || !node_matches(model, replacement) || name == INVALID_NAME) {
        return Result::INVALID_ARGUMENT;
    }
    auto& parent_state = model.nodes[parent.id];
    auto& replacement_state = model.nodes[replacement.id];
    if (parent_state.kind != NodeKind::DIRECTORY) {
        return Result::NOT_DIRECTORY;
    }
    if (parent_state.mount != replacement_state.mount) {
        return Result::INVALID_ARGUMENT;
    }
    size_t const SLOT = find_dentry(model, parent.id, name);
    if (SLOT == MAX_DENTRIES) {
        return bind(model, parent, name, replacement);
    }
    NodeIdentity const OLD = model.dentries[SLOT].child;
    if (OLD == replacement) {
        return Result::OK;
    }
    if (node_matches(model, OLD) && model.nodes[OLD.id].links != 0) {
        --model.nodes[OLD.id].links;
    }
    model.dentries[SLOT].child = replacement;
    replacement_state.links++;
    if (replacement_state.kind == NodeKind::DIRECTORY) {
        replacement_state.parent = parent.id;
    }
    parent_state.directory_generation = next_generation(parent_state.directory_generation);
    return Result::OK;
}

constexpr auto collect_unlinked_node(Model& model, NodeIdentity identity) -> Result {
    if (!node_matches(model, identity)) {
        return Result::STALE_HANDLE;
    }
    auto& node = model.nodes[identity.id];
    if (node.links != 0 || node.references != 0) {
        return Result::BUSY;
    }
    node.allocated = false;
    node.parent = INVALID_NODE;
    node.symlink_target = {};
    return Result::OK;
}

constexpr auto set_symlink_target(Model& model, NodeIdentity link, NodeIdentity target) -> Result {
    if (!node_matches(model, link) || !node_matches(model, target) || model.nodes[link.id].kind != NodeKind::SYMLINK ||
        model.nodes[link.id].mount != model.nodes[target.id].mount) {
        return Result::INVALID_ARGUMENT;
    }
    model.nodes[link.id].symlink_target = target;
    return Result::OK;
}

constexpr void retain_node(Model& model, NodeIdentity identity) {
    if (node_matches(model, identity)) {
        model.nodes[identity.id].references++;
    }
}

constexpr void release_node(Model& model, NodeIdentity identity) {
    if (node_matches(model, identity) && model.nodes[identity.id].references != 0) {
        --model.nodes[identity.id].references;
    }
}

constexpr void retain_unique_nodes(Model& model, LookupToken const& token) {
    retain_node(model, token.object);
    if (token.parent.valid() && token.parent != token.object) {
        retain_node(model, token.parent);
    }
    if (token.root.valid() && token.root != token.object && token.root != token.parent) {
        retain_node(model, token.root);
    }
}

constexpr void release_unique_nodes(Model& model, LookupToken const& token) {
    release_node(model, token.object);
    if (token.parent.valid() && token.parent != token.object) {
        release_node(model, token.parent);
    }
    if (token.root.valid() && token.root != token.object && token.root != token.parent) {
        release_node(model, token.root);
    }
}

constexpr auto retain_token(Model& model, LookupToken token, LookupHandle& out) -> Result {
    if (!mount_matches(model, token.mount) || !node_matches(model, token.object) || !node_matches(model, token.root) ||
        (token.parent.valid() && !node_matches(model, token.parent))) {
        return Result::STALE_HANDLE;
    }
    auto& mount = model.mounts[token.mount.id];
    mount.references++;
    retain_unique_nodes(model, token);
    out = {.token = token, .retained = true};
    return Result::OK;
}

constexpr void release(Model& model, LookupHandle& handle) {
    if (!handle.retained) {
        return;
    }
    release_unique_nodes(model, handle.token);
    if (mount_matches(model, handle.token.mount) && model.mounts[handle.token.mount.id].references != 0) {
        --model.mounts[handle.token.mount.id].references;
    }
    handle.retained = false;
}

[[nodiscard]] constexpr auto validate_token(Model const& model, LookupToken const& token, ValidationUse use) -> Result {
    if (!mount_matches(model, token.mount) || !node_matches(model, token.object) || !node_matches(model, token.root) ||
        (token.parent.valid() && !node_matches(model, token.parent))) {
        return Result::STALE_HANDLE;
    }
    auto const& mount = model.mounts[token.mount.id];
    // A retained path walk pins its mount and may finish after retirement is
    // published.  New roots are fenced in acquire_mount_root(); mutation
    // commit is stricter and must not begin once retirement has started.
    if (use == ValidationUse::MUTATION_COMMIT && (!mount.accepting_lookups || mount.retiring)) {
        return Result::MOUNT_RETIRED;
    }
    if (model.nodes[token.object.id].mount != token.mount || model.nodes[token.root.id].mount != token.mount ||
        (token.parent.valid() && model.nodes[token.parent.id].mount != token.mount)) {
        return Result::STALE_HANDLE;
    }
    return Result::OK;
}

[[nodiscard]] constexpr auto validate_handle(Model const& model, LookupHandle const& handle, ValidationUse use) -> Result {
    if (!handle.retained) {
        return Result::INVALID_HANDLE;
    }
    return validate_token(model, handle.token, use);
}

constexpr auto acquire_mount_root(Model& model, MountId mount, LookupHandle& out) -> Result {
    if (mount >= MAX_MOUNTS || !model.mounts[mount].allocated || !model.mounts[mount].accepting_lookups || model.mounts[mount].retiring) {
        return Result::MOUNT_RETIRED;
    }
    NodeIdentity const ROOT = node_identity(model, model.mounts[mount].root);
    if (!ROOT.valid()) {
        return Result::NOT_FOUND;
    }
    return retain_token(model, {.mount = mount_identity(model, mount), .object = ROOT, .root = ROOT}, out);
}

[[nodiscard]] constexpr auto node_is_beneath_root(Model const& model, NodeIdentity node, NodeIdentity root) -> bool {
    if (!node_matches(model, node) || !node_matches(model, root) || model.nodes[node.id].mount != model.nodes[root.id].mount) {
        return false;
    }
    std::array<bool, MAX_NODES> reachable{};
    reachable[root.id] = true;
    for (size_t pass = 0; pass < MAX_NODES; ++pass) {
        bool changed = false;
        for (auto const& entry : model.dentries) {
            if (!entry.allocated || entry.parent >= MAX_NODES || !reachable[entry.parent] || !node_matches(model, entry.child)) {
                continue;
            }
            if (!reachable[entry.child.id]) {
                reachable[entry.child.id] = true;
                changed = true;
            }
        }
        if (reachable[node.id]) {
            return true;
        }
        if (!changed) {
            break;
        }
    }
    return reachable[node.id];
}

constexpr auto lookup_child(Model& model, LookupHandle const& parent, NameId name, LookupHandle& out) -> Result {
    Result const VALID = validate_handle(model, parent, ValidationUse::CONTINUE_WALK);
    if (VALID != Result::OK) {
        return VALID;
    }
    if (name == INVALID_NAME) {
        return Result::INVALID_ARGUMENT;
    }
    auto const& parent_node = model.nodes[parent.token.object.id];
    if (parent_node.kind != NodeKind::DIRECTORY) {
        return Result::NOT_DIRECTORY;
    }
    if (!node_is_beneath_root(model, parent.token.object, parent.token.root)) {
        return Result::ROOT_ESCAPE;
    }
    size_t const SLOT = find_dentry(model, parent.token.object.id, name);
    if (SLOT == MAX_DENTRIES || !node_matches(model, model.dentries[SLOT].child)) {
        return Result::NOT_FOUND;
    }
    NodeIdentity const CHILD = model.dentries[SLOT].child;
    LookupToken const TOKEN = {
        .mount = parent.token.mount,
        .object = CHILD,
        .parent = parent.token.object,
        .root = parent.token.root,
        .name = name,
        .parent_directory_generation = parent_node.directory_generation,
        .observes_binding = true,
    };
    return retain_token(model, TOKEN, out);
}

[[nodiscard]] constexpr auto revalidate_binding(Model const& model, LookupHandle const& handle) -> Result {
    Result const VALID = validate_handle(model, handle, ValidationUse::MUTATION_COMMIT);
    if (VALID != Result::OK) {
        return VALID;
    }
    auto const& token = handle.token;
    if (!token.observes_binding || !token.parent.valid()) {
        return Result::INVALID_ARGUMENT;
    }
    auto const& parent = model.nodes[token.parent.id];
    if (parent.directory_generation != token.parent_directory_generation) {
        return Result::STALE_BINDING;
    }
    size_t const SLOT = find_dentry(model, token.parent.id, token.name);
    if (SLOT == MAX_DENTRIES || model.dentries[SLOT].child != token.object) {
        return Result::STALE_BINDING;
    }
    return Result::OK;
}

constexpr auto walk_dotdot(Model& model, LookupHandle const& cursor, LookupHandle& out) -> Result {
    Result const VALID = validate_handle(model, cursor, ValidationUse::CONTINUE_WALK);
    if (VALID != Result::OK) {
        return VALID;
    }
    if (!node_is_beneath_root(model, cursor.token.object, cursor.token.root)) {
        return Result::ROOT_ESCAPE;
    }
    if (cursor.token.object == cursor.token.root) {
        return retain_token(model, {.mount = cursor.token.mount, .object = cursor.token.root, .root = cursor.token.root}, out);
    }
    NodeId const PARENT_ID = model.nodes[cursor.token.object.id].parent;
    NodeIdentity const PARENT = node_identity(model, PARENT_ID);
    if (!PARENT.valid() || !node_is_beneath_root(model, PARENT, cursor.token.root)) {
        return Result::ROOT_ESCAPE;
    }
    return retain_token(model, {.mount = cursor.token.mount, .object = PARENT, .root = cursor.token.root}, out);
}

constexpr auto follow_symlink(Model& model, LookupHandle const& link, uint8_t& depth, LookupHandle& out) -> Result {
    Result const VALID = validate_handle(model, link, ValidationUse::CONTINUE_WALK);
    if (VALID != Result::OK) {
        return VALID;
    }
    auto const& node = model.nodes[link.token.object.id];
    if (node.kind != NodeKind::SYMLINK) {
        return Result::NOT_SYMLINK;
    }
    if (depth >= MAX_SYMLINK_DEPTH) {
        return Result::SYMLINK_DEPTH;
    }
    ++depth;
    if (!node_matches(model, node.symlink_target)) {
        return Result::NOT_FOUND;
    }
    if (!node_is_beneath_root(model, node.symlink_target, link.token.root)) {
        return Result::ROOT_ESCAPE;
    }
    return retain_token(model, {.mount = link.token.mount, .object = node.symlink_target, .root = link.token.root}, out);
}

constexpr auto retire_mount(Model& model, MountIdentity mount) -> Result {
    if (!mount_matches(model, mount)) {
        return Result::STALE_HANDLE;
    }
    auto& state = model.mounts[mount.id];
    state.accepting_lookups = false;
    state.retiring = true;
    return Result::OK;
}

constexpr auto destroy_retired_mount(Model& model, MountIdentity mount) -> Result {
    if (!mount_matches(model, mount)) {
        return Result::STALE_HANDLE;
    }
    auto& state = model.mounts[mount.id];
    if (!state.retiring) {
        return Result::INVALID_ARGUMENT;
    }
    if (state.references != 0) {
        return Result::BUSY;
    }
    for (auto const& node : model.nodes) {
        if (node.allocated && node.mount == mount && node.references != 0) {
            return Result::BUSY;
        }
    }
    for (auto& entry : model.dentries) {
        if (entry.allocated && node_matches(model, entry.child) && model.nodes[entry.child.id].mount == mount) {
            entry = {};
        }
    }
    for (auto& node : model.nodes) {
        if (node.allocated && node.mount == mount) {
            node.allocated = false;
            node.references = 0;
            node.links = 0;
        }
    }
    state.allocated = false;
    state.accepting_lookups = false;
    state.retiring = false;
    state.root = INVALID_NODE;
    return Result::OK;
}

[[nodiscard]] constexpr auto revalidate_or_restart(Model const& model, LookupHandle const& handle, RestartBudget& budget) -> Result {
    budget.attempts++;
    Result const RESULT = revalidate_binding(model, handle);
    if (RESULT != Result::STALE_BINDING && RESULT != Result::STALE_HANDLE) {
        return RESULT;
    }
    if (budget.restarts >= budget.maximum_restarts) {
        return Result::RETRY_LIMIT;
    }
    budget.restarts++;
    return Result::RESTART;
}

[[nodiscard]] constexpr auto identity_less(MountIdentity left_mount, NodeIdentity left, MountIdentity right_mount, NodeIdentity right)
    -> bool {
    if (left_mount.id != right_mount.id) {
        return left_mount.id < right_mount.id;
    }
    if (left_mount.generation != right_mount.generation) {
        return left_mount.generation < right_mount.generation;
    }
    if (left.id != right.id) {
        return left.id < right.id;
    }
    return left.generation < right.generation;
}

[[nodiscard]] constexpr auto order_parents(LookupHandle const& left, LookupHandle const& right) -> ParentOrder {
    NodeIdentity const LEFT = left.token.object;
    NodeIdentity const RIGHT = right.token.object;
    if (left.token.mount == right.token.mount && LEFT == RIGHT) {
        return {.first_mount = left.token.mount, .first = LEFT, .second_mount = right.token.mount, .second = RIGHT, .same_parent = true};
    }
    if (identity_less(left.token.mount, LEFT, right.token.mount, RIGHT)) {
        return {.first_mount = left.token.mount, .first = LEFT, .second_mount = right.token.mount, .second = RIGHT, .same_parent = false};
    }
    return {.first_mount = right.token.mount, .first = RIGHT, .second_mount = left.token.mount, .second = LEFT, .same_parent = false};
}

}  // namespace ker::vfs::lookup_model
