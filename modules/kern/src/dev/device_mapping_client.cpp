#include "device_mapping_client.hpp"

#include <cerrno>
#include <limits>
#include <new>
#include <utility>
#include <vfs/file.hpp>

namespace ker::dev::mapping {

auto detail::ReferenceCount::retain() noexcept -> bool {
    uint32_t value = value_.load(std::memory_order_relaxed);
    while (value != 0 && value != std::numeric_limits<uint32_t>::max()) {
        if (value_.compare_exchange_weak(value, value + 1, std::memory_order_relaxed, std::memory_order_relaxed)) return true;
    }
    return false;
}

auto detail::ReferenceCount::drop_last() noexcept -> bool {
    uint32_t const previous = value_.fetch_sub(1, std::memory_order_acq_rel);
    if (previous == 0) __builtin_trap();
    return previous == 1;
}

EndpointRef::~EndpointRef() { reset(); }
EndpointRef::EndpointRef(EndpointRef&& other) noexcept : endpoint_(std::exchange(other.endpoint_, nullptr)) {}
auto EndpointRef::operator=(EndpointRef&& other) noexcept -> EndpointRef& {
    if (this != &other) {
        reset();
        endpoint_ = std::exchange(other.endpoint_, nullptr);
    }
    return *this;
}
auto EndpointRef::clone_into(EndpointRef& empty) const noexcept -> bool {
    if (empty || endpoint_ == nullptr || !endpoint_->references_.retain()) return false;
    empty.endpoint_ = endpoint_;
    return true;
}
void EndpointRef::reset() noexcept {
    auto* endpoint = std::exchange(endpoint_, nullptr);
    if (endpoint != nullptr && endpoint->references_.drop_last()) delete endpoint;
}

ClientRef::~ClientRef() { reset(); }
ClientRef::ClientRef(ClientRef&& other) noexcept : client_(std::exchange(other.client_, nullptr)) {}
auto ClientRef::operator=(ClientRef&& other) noexcept -> ClientRef& {
    if (this != &other) {
        reset();
        client_ = std::exchange(other.client_, nullptr);
    }
    return *this;
}
auto ClientRef::clone_into(ClientRef& empty) const noexcept -> bool {
    if (empty || client_ == nullptr || !client_->references_.retain()) return false;
    empty.client_ = client_;
    return true;
}
void ClientRef::reset() noexcept {
    auto* client = std::exchange(client_, nullptr);
    if (client != nullptr && client->references_.drop_last()) delete client;
}

Endpoint::Endpoint(unsigned major, unsigned minor, std::string_view name, const CharDeviceOps& operations,
                   std::unique_ptr<EndpointContext> context)
    : operations_(operations), context_(std::move(context)) {
    for (size_t index = 0; index < name.size(); ++index) name_.at(index) = name[index];
    device_ = {.major = major,
               .minor = minor,
               .name = name_.data(),
               .type = DeviceType::CHAR,
               .private_data = context_.get(),
               .char_ops = &operations_};
}

auto Endpoint::create(unsigned major, unsigned minor, std::string_view name, const CharDeviceOps& operations,
                      std::unique_ptr<EndpointContext> context, EndpointRef& empty) -> int {
    if (empty || context == nullptr || name.empty() || name.size() >= NAME_CAPACITY || name == "." || name == ".." ||
        operations.open != nullptr || operations.open_with_credentials != nullptr)
        return -EINVAL;
    for (char const character : name) {
        if (character == '/' || character == '\0' || static_cast<unsigned char>(character) < 0x20U) return -EINVAL;
    }
    auto* endpoint = new (std::nothrow) Endpoint(major, minor, name, operations, std::move(context));
    if (endpoint == nullptr) return -ENOMEM;
    empty = EndpointRef(endpoint);
    return 0;
}

Client::Client(EndpointRef endpoint, const ker::mod::security::Snapshot& credentials, ClientAccess access)
    : endpoint_(std::move(endpoint)), credentials_(credentials), access_(access) {}

auto Client::open(const EndpointRef& endpoint, const ker::mod::security::Snapshot& credentials, ClientAccess access, ClientRef& empty)
    -> int {
    if (empty || !endpoint || (access.open_flags & 3U) == 3U) return -EINVAL;
    if (access.read_only && ((access.open_flags & 3U) != 0 || (access.open_flags & ker::vfs::O_TRUNC) != 0)) return -EROFS;
    if (!endpoint.get()->accepting_clients()) return -ENODEV;
    EndpointRef retained;
    if (!endpoint.clone_into(retained)) return -EOVERFLOW;
    auto* client = new (std::nothrow) Client(std::move(retained), credentials, access);
    if (client == nullptr) return -ENOMEM;
    ClientRef result(client);
    // This acquire is new-client admission's linearization point. Retirement
    // can close future admission without destroying previously admitted clients.
    if (!endpoint.get()->accepting_clients()) return -ENODEV;
    empty = std::move(result);
    return 0;
}

}  // namespace ker::dev::mapping
