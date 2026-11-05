#pragma once
#include <snapshot_fixture.hpp>
namespace ker::vfs::tmpfs {
// The selected production backing is PRIVATE_COPY. TMPFS backend acquisition
// is an explicit unsupported endpoint, never replacement mapping behavior.
class TmpfsReadSharedBacking {
   public:
    static auto prepare(File&, uint64_t, uint64_t, std::unique_ptr<TmpfsReadSharedBacking>&) -> int { return -EOPNOTSUPP; }
};
class TmpfsDemandSharedBacking {
   public:
    static auto prepare(File&, uint64_t, uint64_t, bool, std::unique_ptr<TmpfsDemandSharedBacking>&) -> int { return -EOPNOTSUPP; }
};
}  // namespace ker::vfs::tmpfs
