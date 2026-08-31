#include "wkictl/peer_resolver.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>

namespace {

constexpr const char* WKI_PEERS_PATH = "/proc/wki/peers";
constexpr char WKI_PEERS_HEADER[] = "hostname node_id connected cpus load_pct last_update_us local\n";
constexpr std::size_t WKI_HOSTNAME_BYTES = 64;
constexpr std::size_t WKI_PEER_ROW_MAX = 257;  // One local row plus every bounded peer-table slot.
constexpr uint16_t WKI_NODE_INVALID = 0;
constexpr uint16_t WKI_NODE_BROADCAST = UINT16_MAX;

struct PeerSpan {
    uint16_t hostname_offset = 0;
    uint8_t hostname_length = 0;
    uint16_t node_id = WKI_NODE_INVALID;
};

struct TokenSpan {
    const char* data = nullptr;
    std::size_t size = 0;
};

auto hostname_valid(const char* hostname, std::size_t size) -> bool {
    if (hostname == nullptr || size == 0 || size >= WKI_HOSTNAME_BYTES) {
        return false;
    }
    for (std::size_t index = 0; index < size; ++index) {
        unsigned char const BYTE = static_cast<unsigned char>(hostname[index]);
        if (BYTE <= 0x20 || BYTE >= 0x7f) {
            return false;
        }
    }
    return true;
}

auto parse_decimal(TokenSpan token, uint64_t maximum, uint64_t* out) -> bool {
    if (token.data == nullptr || token.size == 0 || out == nullptr) {
        return false;
    }
    uint64_t value = 0;
    for (std::size_t index = 0; index < token.size; ++index) {
        unsigned char const BYTE = static_cast<unsigned char>(token.data[index]);
        if (BYTE < '0' || BYTE > '9') {
            return false;
        }
        uint64_t const DIGIT = BYTE - '0';
        if (value > (maximum - DIGIT) / 10) {
            return false;
        }
        value = (value * 10) + DIGIT;
    }
    *out = value;
    return true;
}

auto split_peer_row(const char* line, std::size_t line_size, std::array<TokenSpan, 7>* fields) -> bool {
    if (line == nullptr || line_size == 0 || fields == nullptr) {
        return false;
    }
    std::size_t field_index = 0;
    std::size_t start = 0;
    for (std::size_t index = 0; index <= line_size; ++index) {
        if (index != line_size && line[index] != ' ') {
            continue;
        }
        if (field_index >= fields->size() || index == start) {
            return false;
        }
        fields->at(field_index++) = {.data = line + start, .size = index - start};
        start = index + 1;
    }
    return field_index == fields->size();
}

auto peer_span_equals(const char* snapshot, const PeerSpan& span, TokenSpan hostname) -> bool {
    return span.hostname_length == hostname.size && std::memcmp(snapshot + span.hostname_offset, hostname.data, hostname.size) == 0;
}

auto read_peer_snapshot(std::array<char, wkictl::WKI_PEER_PROC_SNAPSHOT_CAPACITY>* snapshot, std::size_t* size) -> bool {
    if (snapshot == nullptr || size == nullptr) {
        errno = EINVAL;
        return false;
    }
    int const FD = open(WKI_PEERS_PATH, O_RDONLY);
    if (FD < 0) {
        return false;
    }

    std::size_t total = 0;
    bool ok = true;
    while (total < snapshot->size()) {
        ssize_t const READ = read(FD, snapshot->data() + total, snapshot->size() - total);
        if (READ < 0 && errno == EINTR) {
            continue;
        }
        if (READ < 0) {
            ok = false;
            break;
        }
        if (READ == 0) {
            break;
        }
        total += static_cast<std::size_t>(READ);
    }
    if (ok && total == snapshot->size()) {
        char extra = 0;
        ssize_t read_extra = -1;
        do {
            read_extra = read(FD, &extra, 1);
        } while (read_extra < 0 && errno == EINTR);
        if (read_extra != 0) {
            ok = false;
            errno = read_extra > 0 ? EOVERFLOW : errno;
        }
    }
    int saved_errno = ok ? 0 : errno;
    if (close(FD) != 0 && ok) {
        ok = false;
        saved_errno = errno;
    }
    if (!ok) {
        errno = saved_errno;
        return false;
    }
    *size = total;
    return true;
}

}  // namespace

namespace wkictl {

auto resolve_peer_hostname_snapshot(const char* snapshot, std::size_t size, const char* hostname, uint16_t* node_id) -> bool {
    if (snapshot == nullptr || hostname == nullptr || node_id == nullptr) {
        errno = EINVAL;
        return false;
    }
    std::size_t const HOSTNAME_SIZE = std::strlen(hostname);
    if (!hostname_valid(hostname, HOSTNAME_SIZE)) {
        errno = EINVAL;
        return false;
    }
    // Procfs currently allocates exactly 4096 bytes and reserves one byte for
    // NUL. Reaching that producer ceiling means a row may have been clipped,
    // even if the clipped byte happened to be a newline.
    if (size >= WKI_PEER_PROC_SNAPSHOT_CAPACITY - 1) {
        errno = EOVERFLOW;
        return false;
    }
    constexpr std::size_t HEADER_SIZE = sizeof(WKI_PEERS_HEADER) - 1;
    if (size <= HEADER_SIZE || std::memcmp(snapshot, WKI_PEERS_HEADER, HEADER_SIZE) != 0 || snapshot[size - 1] != '\n' ||
        std::memchr(snapshot, '\0', size) != nullptr) {
        errno = EBADMSG;
        return false;
    }

    std::array<PeerSpan, WKI_PEER_ROW_MAX> seen{};
    std::size_t seen_count = 0;
    std::size_t local_count = 0;
    std::size_t matches = 0;
    bool matching_connected = false;
    uint16_t matching_node = WKI_NODE_INVALID;
    std::size_t offset = HEADER_SIZE;
    while (offset < size) {
        const char* const NEWLINE = static_cast<const char*>(std::memchr(snapshot + offset, '\n', size - offset));
        if (NEWLINE == nullptr || NEWLINE == snapshot + offset || seen_count >= seen.size()) {
            errno = EBADMSG;
            return false;
        }
        std::size_t const LINE_SIZE = static_cast<std::size_t>(NEWLINE - (snapshot + offset));
        std::array<TokenSpan, 7> fields{};
        if (!split_peer_row(snapshot + offset, LINE_SIZE, &fields) || !hostname_valid(fields.at(0).data, fields.at(0).size)) {
            errno = EBADMSG;
            return false;
        }

        std::array<uint64_t, 6> numbers{};
        constexpr std::array<uint64_t, 6> MAXIMA{
            UINT16_MAX, 1, std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max(),
            1};
        for (std::size_t index = 0; index < numbers.size(); ++index) {
            if (!parse_decimal(fields.at(index + 1), MAXIMA.at(index), &numbers.at(index))) {
                errno = EBADMSG;
                return false;
            }
        }
        uint16_t const ROW_NODE = static_cast<uint16_t>(numbers.at(0));
        if (ROW_NODE == WKI_NODE_INVALID || ROW_NODE == WKI_NODE_BROADCAST) {
            errno = EBADMSG;
            return false;
        }
        for (std::size_t index = 0; index < seen_count; ++index) {
            if (seen.at(index).node_id == ROW_NODE || peer_span_equals(snapshot, seen.at(index), fields.at(0))) {
                errno = EEXIST;
                return false;
            }
        }
        if (offset > UINT16_MAX || fields.at(0).size > UINT8_MAX) {
            errno = EOVERFLOW;
            return false;
        }
        seen.at(seen_count++) = {.hostname_offset = static_cast<uint16_t>(offset),
                                 .hostname_length = static_cast<uint8_t>(fields.at(0).size),
                                 .node_id = ROW_NODE};
        local_count += numbers.at(5) != 0 ? 1 : 0;

        if (fields.at(0).size == HOSTNAME_SIZE && std::memcmp(fields.at(0).data, hostname, HOSTNAME_SIZE) == 0) {
            ++matches;
            matching_node = ROW_NODE;
            matching_connected = numbers.at(1) != 0;
        }
        offset = static_cast<std::size_t>(NEWLINE + 1 - snapshot);
    }

    if (local_count != 1) {
        errno = EBADMSG;
        return false;
    }
    if (matches == 0) {
        errno = ENOENT;
        return false;
    }
    if (matches != 1) {
        errno = EEXIST;
        return false;
    }
    if (!matching_connected) {
        errno = ENOTCONN;
        return false;
    }
    *node_id = matching_node;
    return true;
}

auto resolve_peer_hostname(const char* hostname, uint16_t* node_id) -> bool {
    std::array<char, WKI_PEER_PROC_SNAPSHOT_CAPACITY> snapshot{};
    std::size_t size = 0;
    if (!read_peer_snapshot(&snapshot, &size)) {
        return false;
    }
    return resolve_peer_hostname_snapshot(snapshot.data(), size, hostname, node_id);
}

}  // namespace wkictl
