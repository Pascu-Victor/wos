#include "service_identity.h"

namespace wos::init {
namespace {
auto valid_name(std::string_view name) -> bool {
    if (name.empty() || name.size() >= MAX_SERVICE_NAME_BYTES) return false;
    auto alpha = [](char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'; };
    if (!alpha(name.front())) return false;
    for (char c : name)
        if (!alpha(c) && (c < '0' || c > '9') && c != '-') return false;
    return true;
}
auto number(std::string_view text, uint32_t& out) -> bool {
    if (text.empty()) return false;
    uint32_t value = 0;
    for (char c : text) {
        if (c < '0' || c > '9' || value > (UINT32_MAX - 1 - static_cast<uint32_t>(c - '0')) / 10) return false;
        value = value * 10 + static_cast<uint32_t>(c - '0');
    }
    out = value;
    return true;
}
template <size_t Count>
auto split(std::string_view line, std::array<std::string_view, Count>& fields) -> bool {
    for (size_t i = 0; i < Count; ++i) {
        size_t const END = line.find(':');
        if ((END == std::string_view::npos) != (i == Count - 1)) return false;
        fields[i] = line.substr(0, END);
        if (END != std::string_view::npos) line.remove_prefix(END + 1);
    }
    return true;
}
template <typename Consumer>
auto lines(std::string_view input, Consumer consume) -> bool {
    if (input.empty() || input.size() > MAX_ACCOUNT_FILE_BYTES || input.back() != '\n') return false;
    for (char c : input)
        if (c != '\n' && (c < 0x20 || c > 0x7e)) return false;
    while (!input.empty()) {
        size_t const END = input.find('\n');
        auto const LINE = input.substr(0, END);
        if (LINE.size() > MAX_MANIFEST_LINE_BYTES) return false;
        if (!LINE.empty() && LINE.front() != '#' && !consume(LINE)) return false;
        input.remove_prefix(END + 1);
    }
    return true;
}
}  // namespace

auto parse_service_accounts(std::string_view passwd, std::string_view group, ServiceAccounts& out) -> bool {
    ServiceAccounts next{};
    if (!lines(passwd, [&](std::string_view line) {
            std::array<std::string_view, 7> fields{};
            if (!split(line, fields) || !valid_name(fields[0]) || next.user_count == MAX_SERVICE_ACCOUNTS || fields[5].empty() ||
                fields[5].front() != '/' || fields[6].empty() || fields[6].front() != '/')
                return false;
            ServiceUser user{};
            if (!user.name.assign(fields[0]) || !number(fields[2], user.uid) || !number(fields[3], user.gid)) return false;
            for (size_t i = 0; i < next.user_count; ++i)
                if (next.users[i].name.view() == user.name.view() || next.users[i].uid == user.uid) return false;
            next.users[next.user_count++] = user;
            return true;
        }))
        return false;
    if (!lines(group, [&](std::string_view line) {
            std::array<std::string_view, 4> fields{};
            if (!split(line, fields) || !valid_name(fields[0]) || next.group_count == MAX_SERVICE_ACCOUNTS) return false;
            ServiceGroup entry{};
            if (!entry.name.assign(fields[0]) || !number(fields[2], entry.gid)) return false;
            for (size_t i = 0; i < next.group_count; ++i)
                if (next.groups[i].name.view() == entry.name.view() || next.groups[i].gid == entry.gid) return false;
            auto members = fields[3];
            while (!members.empty()) {
                size_t const END = members.find(',');
                auto const NAME = members.substr(0, END);
                bool known = false;
                for (size_t i = 0; i < next.user_count; ++i) known |= next.users[i].name.view() == NAME;
                if (!known) return false;
                if (END == std::string_view::npos) break;
                members.remove_prefix(END + 1);
                if (members.empty()) return false;
            }
            next.groups[next.group_count++] = entry;
            return true;
        }))
        return false;
    if (next.user_count == 0 || next.group_count == 0) return false;
    for (size_t i = 0; i < next.user_count; ++i) {
        bool found = false;
        for (size_t g = 0; g < next.group_count; ++g) found |= next.users[i].gid == next.groups[g].gid;
        if (!found) return false;
    }
    out = next;
    return true;
}

auto resolve_service_identity(const ServiceIdentityPolicy& policy, const ServiceAccounts& accounts, ResolvedServiceIdentity& out) -> bool {
    if (accounts.user_count > MAX_SERVICE_ACCOUNTS || accounts.group_count > MAX_SERVICE_ACCOUNTS ||
        policy.supplementary_count > MAX_SERVICE_GROUPS || policy.mask > 0777 || policy.authority_ceiling > SERVICE_AUTHORITY_ALL)
        return false;
    ResolvedServiceIdentity next{};
    bool found = false;
    for (size_t i = 0; i < accounts.user_count; ++i) {
        if (accounts.users[i].name.view() == policy.user.view()) {
            next.uid = accounts.users[i].uid;
            found = true;
        }
    }
    if (!found) return false;
    auto group_id = [&](std::string_view name, uint32_t& id) {
        for (size_t i = 0; i < accounts.group_count; ++i)
            if (accounts.groups[i].name.view() == name) {
                id = accounts.groups[i].gid;
                return true;
            }
        return false;
    };
    if (!group_id(policy.group.view(), next.gid)) return false;
    for (size_t i = 0; i < policy.supplementary_count; ++i) {
        if (!group_id(policy.supplementary_groups[i].view(), next.groups[i])) return false;
        for (size_t previous = 0; previous < i; ++previous)
            if (next.groups[previous] == next.groups[i]) return false;
    }
    if (!valid_service_authority(next.uid, policy.no_new_privileges, policy.authority_ceiling, policy.service_privileges)) return false;
    next.group_count = policy.supplementary_count;
    next.mask = policy.mask;
    next.authority_ceiling = policy.authority_ceiling;
    next.no_new_privileges = policy.no_new_privileges;
    next.service_privileges = policy.service_privileges;
    next.valid = true;
    out = next;
    return true;
}
}  // namespace wos::init
