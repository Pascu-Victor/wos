#include "transport.hpp"

#include <errno.h>
#include <poll.h>
#include <stdint.h>
#include <sys/sendfile.h>
#include <unistd.h>

#include <array>

namespace sftpserver {
namespace {

auto wait_for_fd(int fd, short events) -> bool {
    pollfd pfd{
        .fd = fd,
        .events = events,
        .revents = 0,
    };

    while (true) {
        int rc = poll(&pfd, 1, -1);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (rc == 0) {
            continue;
        }
        return (pfd.revents & (events | POLLHUP | POLLERR)) != 0;
    }
}

auto write_all(int fd, const void* data, size_t size) -> bool {
    const auto* bytes = static_cast<const uint8_t*>(data);
    size_t done = 0;
    while (done < size) {
        ssize_t written = write(fd, bytes + done, size - done);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (wait_for_fd(fd, POLLOUT)) {
                    continue;
                }
            }
            return false;
        }
        if (written == 0) {
            return false;
        }
        done += static_cast<size_t>(written);
    }
    return true;
}

auto read_all(int fd, void* data, size_t size) -> bool {
    auto* bytes = static_cast<uint8_t*>(data);
    size_t done = 0;
    while (done < size) {
        ssize_t count = read(fd, bytes + done, size - done);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (wait_for_fd(fd, POLLIN)) {
                    continue;
                }
            }
            return false;
        }
        if (count == 0) {
            return false;
        }
        done += static_cast<size_t>(count);
    }
    return true;
}

}  // namespace

auto read_packet(std::vector<uint8_t>& out) -> bool {
    std::array<uint8_t, 4> len_bytes{};
    if (!read_all(STDIN_FILENO, len_bytes.data(), len_bytes.size())) {
        return false;
    }
    uint32_t len = (static_cast<uint32_t>(len_bytes[0]) << 24) | (static_cast<uint32_t>(len_bytes[1]) << 16) |
                   (static_cast<uint32_t>(len_bytes[2]) << 8) | static_cast<uint32_t>(len_bytes[3]);
    if (len == 0 || len > MAX_PACKET_SIZE) {
        return false;
    }
    out.resize(len);
    return read_all(STDIN_FILENO, out.data(), out.size());
}

auto send_packet(const PacketWriter& packet) -> bool {
    uint32_t len = static_cast<uint32_t>(packet.body.size());
    std::array<uint8_t, 4> len_bytes = {
        static_cast<uint8_t>((len >> 24) & 0xffU),
        static_cast<uint8_t>((len >> 16) & 0xffU),
        static_cast<uint8_t>((len >> 8) & 0xffU),
        static_cast<uint8_t>(len & 0xffU),
    };
    return write_all(STDOUT_FILENO, len_bytes.data(), len_bytes.size()) && write_all(STDOUT_FILENO, packet.body.data(), packet.body.size());
}

auto send_data_packet(uint32_t id, std::span<const uint8_t> data) -> bool {
    uint32_t len = static_cast<uint32_t>(1U + 4U + 4U + data.size());
    std::array<uint8_t, 13> header = {
        static_cast<uint8_t>((len >> 24) & 0xffU),
        static_cast<uint8_t>((len >> 16) & 0xffU),
        static_cast<uint8_t>((len >> 8) & 0xffU),
        static_cast<uint8_t>(len & 0xffU),
        SSH_FXP_DATA,
        static_cast<uint8_t>((id >> 24) & 0xffU),
        static_cast<uint8_t>((id >> 16) & 0xffU),
        static_cast<uint8_t>((id >> 8) & 0xffU),
        static_cast<uint8_t>(id & 0xffU),
        static_cast<uint8_t>((data.size() >> 24) & 0xffU),
        static_cast<uint8_t>((data.size() >> 16) & 0xffU),
        static_cast<uint8_t>((data.size() >> 8) & 0xffU),
        static_cast<uint8_t>(data.size() & 0xffU),
    };
    return write_all(STDOUT_FILENO, header.data(), header.size()) && write_all(STDOUT_FILENO, data.data(), data.size());
}

auto send_file_data_packet(uint32_t id, int fd, uint64_t offset, size_t size) -> bool {
    auto const LEN = static_cast<uint32_t>(1U + 4U + 4U + size);
    std::array<uint8_t, 13> header = {
        static_cast<uint8_t>((LEN >> 24) & 0xffU),
        static_cast<uint8_t>((LEN >> 16) & 0xffU),
        static_cast<uint8_t>((LEN >> 8) & 0xffU),
        static_cast<uint8_t>(LEN & 0xffU),
        SSH_FXP_DATA,
        static_cast<uint8_t>((id >> 24) & 0xffU),
        static_cast<uint8_t>((id >> 16) & 0xffU),
        static_cast<uint8_t>((id >> 8) & 0xffU),
        static_cast<uint8_t>(id & 0xffU),
        static_cast<uint8_t>((size >> 24) & 0xffU),
        static_cast<uint8_t>((size >> 16) & 0xffU),
        static_cast<uint8_t>((size >> 8) & 0xffU),
        static_cast<uint8_t>(size & 0xffU),
    };
    if (!write_all(STDOUT_FILENO, header.data(), header.size())) {
        return false;
    }

    auto pos = static_cast<off_t>(offset);
    size_t done = 0;
    while (done < size) {
        ssize_t sent = sendfile(STDOUT_FILENO, fd, &pos, size - done);
        if (sent < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (wait_for_fd(STDOUT_FILENO, POLLOUT)) {
                    continue;
                }
            }
            return false;
        }
        if (sent == 0) {
            return false;
        }
        done += static_cast<size_t>(sent);
    }
    return true;
}

}  // namespace sftpserver
