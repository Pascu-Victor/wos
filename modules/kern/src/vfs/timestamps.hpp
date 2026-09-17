#pragma once

#include <array>
#include <platform/security/file_policy.hpp>
#include <vfs/stat.hpp>

namespace ker::vfs {

// Request-local validated value. NOW intent survives conversion to an absolute
// time; backends cannot accidentally grant write-access permission to an
// explicit or mixed NOW/OMIT request. No userspace pointer is retained.
class TimestampUpdate {
   public:
    static constexpr int64_t NOW = (1LL << 30) - 1;
    static constexpr int64_t OMIT = (1LL << 30) - 2;

    static constexpr auto prepare(const Timespec* requested, Timespec now, TimestampUpdate& out) -> int {
        if (now.tv_nsec < 0 || now.tv_nsec >= 1000000000) return -EINVAL;
        TimestampUpdate next;
        next.ctime_ = now;
        for (size_t i = 0; i < 2; ++i) {
            if (requested == nullptr || requested[i].tv_nsec == NOW) {
                next.kinds_[i] = Kind::NOW;
                next.times_[i] = now;
            } else if (requested[i].tv_nsec == OMIT) {
                next.kinds_[i] = Kind::OMIT;
            } else if (requested[i].tv_nsec < 0 || requested[i].tv_nsec >= 1000000000) {
                return -EINVAL;
            } else {
                next.kinds_[i] = Kind::EXPLICIT;
                next.times_[i] = requested[i];
            }
        }
        out = next;
        return 0;
    }

    constexpr auto atime() const -> Timespec { return times_[0]; }
    constexpr auto mtime() const -> Timespec { return times_[1]; }
    constexpr auto ctime() const -> Timespec { return ctime_; }
    constexpr auto set_atime() const -> bool { return kinds_[0] != Kind::OMIT; }
    constexpr auto set_mtime() const -> bool { return kinds_[1] != Kind::OMIT; }
    constexpr auto changed() const -> bool { return set_atime() || set_mtime(); }
    constexpr auto intent() const -> ker::mod::security::FileTimestampIntent {
        using Intent = ker::mod::security::FileTimestampIntent;
        if (!changed()) return Intent::PRESERVE;
        return kinds_[0] == Kind::NOW && kinds_[1] == Kind::NOW ? Intent::SET_TO_NOW : Intent::SET_EXPLICIT;
    }

   private:
    enum class Kind : uint8_t { OMIT, NOW, EXPLICIT };
    std::array<Timespec, 2> times_{};
    Timespec ctime_{};
    std::array<Kind, 2> kinds_{Kind::OMIT, Kind::OMIT};
};

}  // namespace ker::vfs
