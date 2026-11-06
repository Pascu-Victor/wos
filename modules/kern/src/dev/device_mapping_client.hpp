#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <dev/device.hpp>
#include <dev/device_mapping_grants.hpp>
#include <memory>
#include <platform/security/credentials.hpp>
#include <string_view>

namespace ker::vfs::devfs {
struct MappingEndpointAccess;
}

namespace ker::dev::mapping {

class Endpoint;
class Client;

// Built-in kernel code only: this interface supplies no module-unload pin.
// The endpoint uniquely owns the complete callback context until its last ref.
class EndpointContext {
   public:
    virtual ~EndpointContext() = default;
};

namespace detail {
class ReferenceCount {
   public:
    explicit ReferenceCount(uint32_t initial = 1) : value_(initial) {}
    auto retain() noexcept -> bool;
    auto drop_last() noexcept -> bool;

   private:
    static_assert(std::atomic<uint32_t>::is_always_lock_free);
    std::atomic<uint32_t> value_;
};
}  // namespace detail

class EndpointRef {
   public:
    EndpointRef() = default;
    ~EndpointRef();
    EndpointRef(const EndpointRef&) = delete;
    auto operator=(const EndpointRef&) -> EndpointRef& = delete;
    EndpointRef(EndpointRef&& other) noexcept;
    auto operator=(EndpointRef&& other) noexcept -> EndpointRef&;
    [[nodiscard]] auto clone_into(EndpointRef& empty) const noexcept -> bool;
    void reset() noexcept;
    [[nodiscard]] auto get() const noexcept -> Endpoint* { return endpoint_; }
    explicit operator bool() const noexcept { return endpoint_ != nullptr; }

   private:
    friend class Endpoint;
    explicit EndpointRef(Endpoint* endpoint) : endpoint_(endpoint) {}
    Endpoint* endpoint_ = nullptr;
};

class ClientRef {
   public:
    ClientRef() = default;
    ~ClientRef();
    ClientRef(const ClientRef&) = delete;
    auto operator=(const ClientRef&) -> ClientRef& = delete;
    ClientRef(ClientRef&& other) noexcept;
    auto operator=(ClientRef&& other) noexcept -> ClientRef&;
    [[nodiscard]] auto clone_into(ClientRef& empty) const noexcept -> bool;
    void reset() noexcept;
    [[nodiscard]] auto get() const noexcept -> const Client* { return client_; }
    explicit operator bool() const noexcept { return client_ != nullptr; }

   private:
    friend class Client;
    explicit ClientRef(Client* client) : client_(client) {}
    Client* client_ = nullptr;
};

struct ClientAccess {
    unsigned open_flags{};
    bool read_only{};
    bool nosuid{};
};

class Endpoint {
   public:
    static constexpr size_t NAME_CAPACITY = 64;
    // No legacy open hooks: devfs creates the typed Client without allowing an
    // open callback to redirect its private wrapper to an unretained Device.
    [[nodiscard]] static auto create(unsigned major, unsigned minor, std::string_view name, const CharDeviceOps& operations,
                                     std::unique_ptr<EndpointContext> context, EndpointRef& empty) -> int;
    void close_admission() noexcept { accepting_.store(false, std::memory_order_release); }
    [[nodiscard]] auto accepting_clients() const noexcept -> bool { return accepting_.load(std::memory_order_acquire); }
    [[nodiscard]] auto context() const noexcept -> EndpointContext& { return *context_; }

   private:
    friend class EndpointRef;
    friend struct ker::vfs::devfs::MappingEndpointAccess;
    Endpoint(unsigned major, unsigned minor, std::string_view name, const CharDeviceOps& operations,
             std::unique_ptr<EndpointContext> context);
    ~Endpoint() = default;
    detail::ReferenceCount references_;
    std::atomic<bool> accepting_{true};
    std::array<char, NAME_CAPACITY> name_{};
    CharDeviceOps operations_{};
    std::unique_ptr<EndpointContext> context_;
    Device device_{};
};

class Client {
   public:
    [[nodiscard]] static auto open(const EndpointRef& endpoint, const ker::mod::security::Snapshot& credentials, ClientAccess access,
                                   ClientRef& empty) -> int;
    [[nodiscard]] auto credentials() const noexcept -> const ker::mod::security::Snapshot& { return credentials_; }
    [[nodiscard]] auto access() const noexcept -> ClientAccess { return access_; }
    [[nodiscard]] auto endpoint() const noexcept -> const Endpoint& { return *endpoint_.get(); }
    // Ordinary kernel provider API. User mmap resolves these grants through
    // the retained File; creating grants is never a raw user-pointer callback.
    [[nodiscard]] auto configure_grants(size_t capacity = GrantTable::MAX_CAPACITY) const -> int;
    [[nodiscard]] auto grant(const ObjectRef&, uint64_t object_offset, uint64_t length, uint64_t max_protections, Grant& empty) const
        -> int;
    [[nodiscard]] auto lookup(uint64_t fake_offset, uint64_t length, uint64_t protections, uint64_t flags, PreparedGrant& empty) const
        -> int;
    [[nodiscard]] auto remove_grant(Grant) const -> int;
    void close_grants() const noexcept;

   private:
    friend class ClientRef;
    Client(EndpointRef endpoint, const ker::mod::security::Snapshot& credentials, ClientAccess access);
    ~Client();
    [[nodiscard]] auto grant_table() const noexcept -> GrantTable*;
    detail::ReferenceCount references_;
    EndpointRef endpoint_;
    ker::mod::security::Snapshot credentials_;
    ClientAccess access_;
    mutable ker::mod::sys::Mutex grant_lock_;
    mutable std::unique_ptr<GrantTable> grants_;
    mutable bool grants_closed_{};
};

// Reference owners are movable only. Shared consumers first retain the exact
// File, then explicitly clone its immutable client. Final puts are scheduled
// work outside IRQ/devfs tree guards. An outer sleeping VFS publication mutex
// may still be held; callbacks/destructors must not recurse into VFS.
}  // namespace ker::dev::mapping
