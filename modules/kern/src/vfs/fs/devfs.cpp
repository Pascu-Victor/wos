#include "devfs.hpp"

#include <cerrno>
#include <cstring>
#include <dev/block_device.hpp>
#include <dev/device.hpp>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

#include "vfs/file_operations.hpp"
#include "vfs/vfs.hpp"

namespace ker::vfs::devfs {

namespace {

// ── DevFSNode tree root ──────────────────────────────────────────────

DevFSNode root_node;

constexpr size_t INITIAL_CHILDREN_CAPACITY = 16;

// ── Tree helpers ─────────────────────────────────────────────────────

void ensure_children_capacity(DevFSNode* node) {
    if (node->children_count < node->children_capacity) {
        return;
    }
    size_t new_cap = (node->children_capacity == 0)
                         ? INITIAL_CHILDREN_CAPACITY
                         : node->children_capacity * 2;
    auto** new_arr = static_cast<DevFSNode**>(
        ker::mod::mm::dyn::kmalloc::malloc(new_cap * sizeof(DevFSNode*)));
    if (new_arr == nullptr) {
        return;
    }
    if (node->children != nullptr && node->children_count > 0) {
        std::memcpy(static_cast<void*>(new_arr),
                    static_cast<const void*>(node->children),
                    node->children_count * sizeof(DevFSNode*));
    }
    node->children = new_arr;
    node->children_capacity = new_cap;
}

auto find_child(DevFSNode* dir, const char* name) -> DevFSNode* {
    if (dir == nullptr) {
        return nullptr;
    }
    for (size_t i = 0; i < dir->children_count; i++) {
        if (std::strcmp(dir->children[i]->name.data(), name) == 0) {
            return dir->children[i];
        }
    }
    return nullptr;
}

void add_child(DevFSNode* parent, DevFSNode* child) {
    if (parent == nullptr || child == nullptr) {
        return;
    }
    ensure_children_capacity(parent);
    if (parent->children_count >= parent->children_capacity) {
        return;  // allocation failed
    }
    child->parent = parent;
    parent->children[parent->children_count++] = child;
}

auto create_node(const char* name, DevFSNodeType type) -> DevFSNode* {
    auto* node = new DevFSNode();
    if (node == nullptr) {
        return nullptr;
    }
    size_t len = std::strlen(name);
    if (len >= DEVFS_NAME_MAX) {
        len = DEVFS_NAME_MAX - 1;
    }
    std::memcpy(node->name.data(), name, len);
    node->name[len] = '\0';
    node->type = type;
    return node;
}

// Walk a path relative to /dev root.
// If create_intermediate is true, missing directories are created.
auto walk_path(const char* path, bool create_intermediate = false)
    -> DevFSNode* {
    if (path == nullptr || path[0] == '\0') {
        return &root_node;
    }

    const char* p = path;
    if (*p == '/') {
        p++;
    }

    DevFSNode* current = &root_node;

    while (*p != '\0') {
        if (*p == '/') {
            p++;
            continue;
        }

        // Extract component
        const char* end = p;
        while (*end != '\0' && *end != '/') {
            end++;
        }
        auto comp_len = static_cast<size_t>(end - p);

        std::array<char, DEVFS_NAME_MAX> component{};
        if (comp_len >= DEVFS_NAME_MAX) {
            comp_len = DEVFS_NAME_MAX - 1;
        }
        std::memcpy(component.data(), p, comp_len);
        component[comp_len] = '\0';

        DevFSNode* child = find_child(current, component.data());
        if (child == nullptr) {
            if (!create_intermediate) {
                return nullptr;
            }
            child = create_node(component.data(), DevFSNodeType::DIRECTORY);
            if (child == nullptr) {
                return nullptr;
            }
            add_child(current, child);
        }

        current = child;
        p = (*end == '/') ? end + 1 : end;
    }

    return current;
}

// ── DevFSFile wrapper (stored in File->private_data) ─────────────────

struct DevFSFile {
    DevFSNode* node = nullptr;
    ker::dev::Device* device = nullptr;
    uint32_t magic = 0xDEADBEEF;
};

// ── File operations ──────────────────────────────────────────────────

auto devfs_close(File* f) -> int {
    if (f == nullptr) {
        vfs_debug_log("devfs_close: file is null\n");
        return -1;
    }
    if (f->private_data == nullptr) {
        vfs_debug_log("devfs_close: no private_data\n");
        return 0;
    }

    auto* devfs_file = static_cast<DevFSFile*>(f->private_data);

    // Validate pointer is in valid kernel memory range
    auto df_addr = reinterpret_cast<uintptr_t>(devfs_file);
    bool in_hhdm =
        (df_addr >= 0xffff800000000000ULL && df_addr < 0xffff900000000000ULL);
    bool in_kernel_static =
        (df_addr >= 0xffffffff80000000ULL && df_addr < 0xffffffffc0000000ULL);
    if (!in_hhdm && !in_kernel_static) {
        ker::mod::dbg::log(
            "devfs_close: devfs_file %p out of valid kernel range; "
            "skipping delete\n",
            devfs_file);
        f->private_data = nullptr;
        return 0;
    }

    if (devfs_file->magic != 0xDEADBEEF) {
        ker::mod::dbg::log(
            "devfs_close: devfs_file %p has invalid magic 0x%x; "
            "skipping delete\n",
            devfs_file, devfs_file->magic);
        f->private_data = nullptr;
        return 0;
    }

    // Call device close if available
    if (devfs_file->device != nullptr &&
        devfs_file->device->char_ops != nullptr &&
        devfs_file->device->char_ops->close != nullptr) {
        devfs_file->device->char_ops->close(f);
    }

    devfs_file->magic = 0;
    delete devfs_file;
    f->private_data = nullptr;
    return 0;
}

auto devfs_read(File* f, void* buf, size_t count, size_t /*offset*/)
    -> ssize_t {
    if (f == nullptr || f->private_data == nullptr) {
        return -1;
    }
    auto* devfs_file = static_cast<DevFSFile*>(f->private_data);
    if (devfs_file->device != nullptr &&
        devfs_file->device->char_ops != nullptr &&
        devfs_file->device->char_ops->read != nullptr) {
        return devfs_file->device->char_ops->read(f, buf, count);
    }
    return -1;
}

auto devfs_write(File* f, const void* buf, size_t count, size_t /*offset*/)
    -> ssize_t {
    if (f == nullptr || f->private_data == nullptr) {
        return -1;
    }
    auto* devfs_file = static_cast<DevFSFile*>(f->private_data);
    if (devfs_file->device != nullptr &&
        devfs_file->device->char_ops != nullptr &&
        devfs_file->device->char_ops->write != nullptr) {
        return devfs_file->device->char_ops->write(f, buf, count);
    }
    return -1;
}

auto devfs_lseek(File* /*f*/, off_t /*offset*/, int /*whence*/) -> off_t {
    return ESPIPE;  // devfs does not support seeking
}

auto devfs_isatty(File* f) -> bool {
    if (f == nullptr || f->private_data == nullptr) {
        return false;
    }
    auto* devfs_file = static_cast<DevFSFile*>(f->private_data);
    if (devfs_file->device != nullptr &&
        devfs_file->device->char_ops != nullptr &&
        devfs_file->device->char_ops->isatty != nullptr) {
        return devfs_file->device->char_ops->isatty(f);
    }
    return false;
}

auto devfs_readdir(File* f, DirEntry* entry, size_t index) -> int {
    if (f == nullptr || entry == nullptr || !f->is_directory) {
        return -1;
    }
    if (f->private_data == nullptr) {
        return -1;
    }

    auto* devfs_file = static_cast<DevFSFile*>(f->private_data);
    auto* dir_node = devfs_file->node;
    if (dir_node == nullptr ||
        dir_node->type != DevFSNodeType::DIRECTORY) {
        return -1;
    }
    if (index >= dir_node->children_count) {
        return -1;
    }

    auto* child = dir_node->children[index];

    entry->d_ino = index + 1;
    entry->d_off = index;
    entry->d_reclen = sizeof(DirEntry);

    switch (child->type) {
        case DevFSNodeType::DIRECTORY:
            entry->d_type = DT_DIR;
            break;
        case DevFSNodeType::DEVICE:
            if (child->device != nullptr &&
                child->device->type == ker::dev::DeviceType::CHAR) {
                entry->d_type = DT_CHR;
            } else {
                entry->d_type = DT_BLK;
            }
            break;
        case DevFSNodeType::SYMLINK:
            entry->d_type = DT_LNK;
            break;
    }

    size_t name_len = std::strlen(child->name.data());
    if (name_len >= DIRENT_NAME_MAX) {
        name_len = DIRENT_NAME_MAX - 1;
    }
    std::memcpy(entry->d_name.data(), child->name.data(), name_len);
    entry->d_name[name_len] = '\0';

    return 0;
}

auto devfs_fops_readlink(File* f, char* buf, size_t bufsize) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr || buf == nullptr ||
        bufsize == 0) {
        return -1;
    }

    auto* devfs_file = static_cast<DevFSFile*>(f->private_data);
    auto* node = devfs_file->node;
    if (node == nullptr || node->type != DevFSNodeType::SYMLINK) {
        return -EINVAL;
    }

    size_t target_len = std::strlen(node->symlink_target.data());
    size_t copy_len = (target_len < bufsize) ? target_len : bufsize;
    std::memcpy(buf, node->symlink_target.data(), copy_len);
    return static_cast<ssize_t>(copy_len);
}

FileOperations devfs_fops = {
    .vfs_open = nullptr,
    .vfs_close = devfs_close,
    .vfs_read = devfs_read,
    .vfs_write = devfs_write,
    .vfs_lseek = devfs_lseek,
    .vfs_isatty = devfs_isatty,
    .vfs_readdir = devfs_readdir,
    .vfs_readlink = devfs_fops_readlink,
};

}  // anonymous namespace

// ── Public interface ─────────────────────────────────────────────────

auto get_devfs_fops() -> FileOperations* { return &devfs_fops; }

auto devfs_open_path(const char* path, int /*flags*/, int /*mode*/) -> File* {
    if (path == nullptr) {
        return nullptr;
    }

    // Determine path relative to /dev root
    const char* rel_path = path;
    if (path[0] == '/' && path[1] == 'd' && path[2] == 'e' &&
        path[3] == 'v') {
        if (path[4] == '\0' || (path[4] == '/' && path[5] == '\0')) {
            rel_path = "";  // /dev itself
        } else if (path[4] == '/') {
            rel_path = path + 5;  // strip "/dev/"
        }
    }

    // Walk the tree
    DevFSNode* node = walk_path(rel_path);
    if (node == nullptr) {
        vfs_debug_log("devfs: path not found: ");
        vfs_debug_log(path);
        vfs_debug_log("\n");
        return nullptr;
    }

    // Allocate File + DevFSFile wrapper
    auto* file = static_cast<File*>(
        ker::mod::mm::dyn::kmalloc::malloc(sizeof(File)));
    if (file == nullptr) {
        return nullptr;
    }

    auto* devfs_file = new DevFSFile();
    if (devfs_file == nullptr) {
        ker::mod::mm::dyn::kmalloc::free(file);
        return nullptr;
    }
    devfs_file->node = node;

    file->fd = -1;
    file->fops = &devfs_fops;
    file->pos = 0;
    file->fs_type = FSType::DEVFS;
    file->refcount = 1;

    if (node->type == DevFSNodeType::DIRECTORY) {
        file->is_directory = true;
        file->private_data = devfs_file;
        vfs_debug_log("devfs: opened directory\n");
    } else if (node->type == DevFSNodeType::DEVICE) {
        file->is_directory = false;
        devfs_file->device = node->device;
        file->private_data = devfs_file;

        // Call device open if char device with open handler
        if (node->device != nullptr &&
            node->device->char_ops != nullptr &&
            node->device->char_ops->open != nullptr) {
            if (node->device->char_ops->open(file) != 0) {
                delete devfs_file;
                ker::mod::mm::dyn::kmalloc::free(file);
                return nullptr;
            }
        }

        vfs_debug_log("devfs: opened device: ");
        vfs_debug_log(node->name.data());
        vfs_debug_log("\n");
    } else if (node->type == DevFSNodeType::SYMLINK) {
        // Return a File representing the symlink (for readlink)
        file->is_directory = false;
        file->private_data = devfs_file;
        vfs_debug_log("devfs: opened symlink: ");
        vfs_debug_log(node->name.data());
        vfs_debug_log("\n");
    }

    return file;
}

auto devfs_create_directory(const char* path) -> DevFSNode* {
    return walk_path(path, true);
}

auto devfs_create_symlink(const char* path, const char* target)
    -> DevFSNode* {
    if (path == nullptr || target == nullptr) {
        return nullptr;
    }

    // Split path into parent directory + leaf name
    const char* last_slash = nullptr;
    for (const char* p = path; *p != '\0'; p++) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    DevFSNode* parent = &root_node;
    const char* symlink_name = path;

    if (last_slash != nullptr) {
        auto parent_len = static_cast<size_t>(last_slash - path);
        std::array<char, DEVFS_NAME_MAX> parent_path{};
        if (parent_len >= DEVFS_NAME_MAX) {
            parent_len = DEVFS_NAME_MAX - 1;
        }
        std::memcpy(parent_path.data(), path, parent_len);
        parent_path[parent_len] = '\0';

        parent = walk_path(parent_path.data(), true);
        if (parent == nullptr) {
            return nullptr;
        }
        symlink_name = last_slash + 1;
    }

    auto* node = create_node(symlink_name, DevFSNodeType::SYMLINK);
    if (node == nullptr) {
        return nullptr;
    }

    size_t target_len = std::strlen(target);
    if (target_len >= DEVFS_SYMLINK_MAX) {
        target_len = DEVFS_SYMLINK_MAX - 1;
    }
    std::memcpy(node->symlink_target.data(), target, target_len);
    node->symlink_target[target_len] = '\0';

    add_child(parent, node);

    vfs_debug_log("devfs: created symlink ");
    vfs_debug_log(path);
    vfs_debug_log(" -> ");
    vfs_debug_log(target);
    vfs_debug_log("\n");

    return node;
}

auto devfs_add_device_node(const char* name, ker::dev::Device* dev)
    -> DevFSNode* {
    auto* node = create_node(name, DevFSNodeType::DEVICE);
    if (node == nullptr) {
        return nullptr;
    }
    node->device = dev;
    add_child(&root_node, node);
    return node;
}

void devfs_populate_partition_symlinks() {
    // Create /dev/disk/by-partuuid/ directory hierarchy
    walk_path("disk/by-partuuid", true);

    size_t count = ker::dev::block_device_count();
    for (size_t i = 0; i < count; i++) {
        auto* bdev = ker::dev::block_device_at(i);
        if (bdev == nullptr) {
            continue;
        }

        // Add block device node at /dev root (e.g. sda, sda1)
        if (find_child(&root_node, bdev->name.data()) == nullptr) {
            auto* dnode =
                create_node(bdev->name.data(), DevFSNodeType::DEVICE);
            if (dnode != nullptr) {
                dnode->device = nullptr;  // block devices have no char_ops
                add_child(&root_node, dnode);
            }
        }

        // Create PARTUUID symlink for partition devices
        if (bdev->is_partition && bdev->partuuid_str[0] != '\0') {
            // symlink path: disk/by-partuuid/<UUID>
            std::array<char, DEVFS_SYMLINK_MAX> symlink_path{};
            const char* prefix = "disk/by-partuuid/";
            size_t prefix_len = std::strlen(prefix);
            std::memcpy(symlink_path.data(), prefix, prefix_len);
            size_t uuid_len = std::strlen(bdev->partuuid_str.data());
            std::memcpy(symlink_path.data() + prefix_len,
                        bdev->partuuid_str.data(), uuid_len);
            symlink_path[prefix_len + uuid_len] = '\0';

            // target: /dev/<name>
            std::array<char, DEVFS_SYMLINK_MAX> target_path{};
            const char* dev_prefix = "/dev/";
            size_t dev_prefix_len = std::strlen(dev_prefix);
            std::memcpy(target_path.data(), dev_prefix, dev_prefix_len);
            size_t name_len = std::strlen(bdev->name.data());
            std::memcpy(target_path.data() + dev_prefix_len,
                        bdev->name.data(), name_len);
            target_path[dev_prefix_len + name_len] = '\0';

            devfs_create_symlink(symlink_path.data(), target_path.data());
        }
    }

    vfs_debug_log("devfs: partition symlinks populated\n");
}

void register_devfs() { vfs_debug_log("devfs: registered\n"); }

void devfs_init() {
    vfs_debug_log("devfs: initializing\n");

    // Initialize root node
    root_node.name[0] = '\0';
    root_node.type = DevFSNodeType::DIRECTORY;

    // Populate with registered character devices
    size_t dev_count = ker::dev::dev_get_count();
    for (size_t i = 0; i < dev_count; i++) {
        auto* dev = ker::dev::dev_get_at_index(i);
        if (dev != nullptr) {
            devfs_add_device_node(dev->name, dev);
        }
    }

    register_devfs();
    vfs_debug_log("devfs: initialized with ");
    vfs_debug_log_hex(root_node.children_count);
    vfs_debug_log(" device nodes\n");
}

void devfs_populate_net_nodes() {
    // Create /dev/net/ directory
    walk_path("net", true);
    auto* net_dir = walk_path("net");
    if (net_dir == nullptr) {
        return;
    }

    size_t count = ker::net::netdev_count();
    for (size_t i = 0; i < count; i++) {
        auto* netdev = ker::net::netdev_at(i);
        if (netdev == nullptr) {
            continue;
        }

        // Skip if node already exists
        if (find_child(net_dir, netdev->name) != nullptr) {
            continue;
        }

        // Create device node (not a real char device, just stores netdev pointer)
        auto* node = create_node(netdev->name, DevFSNodeType::DEVICE);
        if (node == nullptr) {
            continue;
        }

        // Allocate a Device struct to hold the netdev pointer
        auto* dev = static_cast<ker::dev::Device*>(
            ker::mod::mm::dyn::kmalloc::calloc(1, sizeof(ker::dev::Device)));
        if (dev == nullptr) {
            continue;
        }

        // Net stats read handler
        static ker::dev::CharDeviceOps net_stats_ops = {
            .open = nullptr,
            .close = nullptr,
            .read = [](ker::vfs::File* f, void* buf, size_t count) -> ssize_t {
                if (f == nullptr || f->private_data == nullptr || buf == nullptr) {
                    return -EINVAL;
                }
                auto* df = static_cast<DevFSFile*>(f->private_data);
                if (df->device == nullptr || df->device->private_data == nullptr) {
                    return -EINVAL;
                }
                auto* nd = static_cast<ker::net::NetDevice*>(df->device->private_data);

                // Format stats into a temporary buffer
                char stats[512];
                size_t pos = 0;

                // Helper to append string
                auto append_str = [&](const char* s) {
                    while (*s != '\0' && pos < sizeof(stats) - 1) {
                        stats[pos++] = *s++;
                    }
                };
                // Helper to append uint64 as decimal
                auto append_u64 = [&](uint64_t val) {
                    if (val == 0) {
                        if (pos < sizeof(stats) - 1) stats[pos++] = '0';
                        return;
                    }
                    char tmp[21];
                    int ti = 0;
                    while (val > 0) {
                        tmp[ti++] = '0' + static_cast<char>(val % 10);
                        val /= 10;
                    }
                    for (int j = ti - 1; j >= 0 && pos < sizeof(stats) - 1; j--) {
                        stats[pos++] = tmp[j];
                    }
                };
                // Helper to append hex byte
                auto append_hex_byte = [&](uint8_t b) {
                    const char hex[] = "0123456789abcdef";
                    if (pos < sizeof(stats) - 2) {
                        stats[pos++] = hex[b >> 4];
                        stats[pos++] = hex[b & 0xF];
                    }
                };

                append_str("name: ");
                append_str(nd->name);
                append_str("\nstate: ");
                append_str(nd->state != 0 ? "up" : "down");
                append_str("\nmac: ");
                for (int m = 0; m < 6; m++) {
                    if (m > 0) append_str(":");
                    append_hex_byte(nd->mac[m]);
                }
                append_str("\nmtu: ");
                append_u64(nd->mtu);

                // Show IPv4 addresses if configured
                auto* nif = ker::net::netif_get(nd);
                if (nif != nullptr && nif->ipv4_addr_count > 0) {
                    append_str("\nipv4: ");
                    uint32_t addr = nif->ipv4_addrs[0].addr;
                    append_u64((addr >> 24) & 0xFF); append_str(".");
                    append_u64((addr >> 16) & 0xFF); append_str(".");
                    append_u64((addr >> 8) & 0xFF); append_str(".");
                    append_u64(addr & 0xFF);
                    append_str("/");
                    // Count prefix length from netmask
                    uint32_t mask = nif->ipv4_addrs[0].netmask;
                    int prefix = 0;
                    for (int b = 31; b >= 0; b--) {
                        if ((mask >> b) & 1) prefix++;
                        else break;
                    }
                    append_u64(static_cast<uint64_t>(prefix));
                }

                append_str("\nrx_packets: "); append_u64(nd->rx_packets);
                append_str("\ntx_packets: "); append_u64(nd->tx_packets);
                append_str("\nrx_bytes: "); append_u64(nd->rx_bytes);
                append_str("\ntx_bytes: "); append_u64(nd->tx_bytes);
                append_str("\n");
                stats[pos] = '\0';

                // Apply file position offset
                auto offset = static_cast<size_t>(f->pos);
                if (offset >= pos) {
                    return 0;  // EOF
                }
                size_t available = pos - offset;
                size_t to_copy = (count < available) ? count : available;
                std::memcpy(buf, stats + offset, to_copy);
                f->pos += static_cast<off_t>(to_copy);
                return static_cast<ssize_t>(to_copy);
            },
            .write = nullptr,
            .isatty = nullptr,
        };

        dev->major = 10;
        dev->minor = static_cast<unsigned>(200 + i);
        dev->name = netdev->name;
        dev->type = ker::dev::DeviceType::CHAR;
        dev->private_data = netdev;
        dev->char_ops = &net_stats_ops;

        node->device = dev;
        add_child(net_dir, node);
    }

    vfs_debug_log("devfs: net nodes populated (");
    vfs_debug_log_hex(count);
    vfs_debug_log(" devices)\n");
}

}  // namespace ker::vfs::devfs
