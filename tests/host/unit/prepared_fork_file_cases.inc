namespace fixture {
namespace actual = ker::syscall::vmem;
using File = ker::vfs::File;
struct FileWorld : Actors {
    std::array<File, 64> files;
    FileWorld() { check(actual::g_file_mmap_ranges.empty(), "empty registry at entry"); }
    void seed(unsigned n) {
        check(actual::g_file_mmap_ranges.reserve(n), "seed reserve");
        for (unsigned i = 0; i < n; ++i) {
            actual::FileMmapRange r{.pagemap = i % 3 ? &source : &foreign,
                                    .start = 0x400000 + 4096 * i,
                                    .length = 4096,
                                    .file_offset = 8192 * i,
                                    .file = &files[i % files.size()],
                                    .writeback = bool(i % 2)};
            check(actual::retain_prepared_legacy_file(r.file), "seed checked ref");
            check(actual::g_file_mmap_ranges.push_back_no_grow(r), "seed append");
        }
    }
    unsigned source_rows() {
        unsigned n = 0;
        for (auto const& r : actual::g_file_mmap_ranges) n += r.pagemap == &source;
        return n;
    }
    ~FileWorld() {
        for (auto const& r : actual::g_file_mmap_ranges) actual::return_prepared_legacy_file(r.file);
        actual::g_file_mmap_ranges = decltype(actual::g_file_mmap_ranges){};
        for (auto& f : files) check(f.refcount == 1, "one witness File owner after plan+registry cleanup");
        check(ker::test::numa_lifecycle::mm_debt::values[0] == 0 && ker::test::numa_lifecycle::mm_debt::values[1] == 0,
              "all actual File census debt returned");
    }
};
void file_tests() {
    for (unsigned n : {0u, 3u, 32u, 33u, 90u}) {
        ++cases;
        FileWorld w;
        w.seed(n);
        unsigned count = w.source_rows(), start = allocations;
        {
            actual::LegacyFileForkPreparation p;
            check(p.prepare(w.actor, w.child, w.scope) == 0, "prepare actual inline/spilled whole registry");
            if (count == 0) check(allocations == start, "zero source allocates nothing");
            unsigned prepared = allocations;
            {
                Pub pub;
                irq = false;
                check(p.matches(w.actor, w.child, w.scope, pub) == 0, "prepared rows match");
                check(p.commit(w.actor, w.child, w.scope, pub) == 0, "actual two vector transfers");
                check(allocations == prepared, "final transaction has no allocation");
                check(actual::g_file_mmap_ranges.size() == n + count, "all unrelated and child rows preserved");
                check(p.commit(w.actor, w.child, w.scope, pub) == -EAGAIN, "adoption single use");
                irq = true;
            }
        }
        for (auto const& r : actual::g_file_mmap_ranges)
            if (r.pagemap == &w.childroot) {
                bool found = false;
                for (auto const& s : actual::g_file_mmap_ranges)
                    if (s.pagemap == &w.source && s.start == r.start)
                        found = s.file == r.file && s.length == r.length && s.file_offset == r.file_offset && s.writeback == r.writeback;
                check(found, "child exact full geometry and File identity");
            }
    }
    for (int failure = 0; failure < 3; ++failure) {
        ++cases;
        FileWorld w;
        w.seed(60);
        unsigned old = actual::g_file_mmap_ranges.size();
        {
            actual::LegacyFileForkPreparation p;
            fail_after = failure;
            check(p.prepare(w.actor, w.child, w.scope) == -ENOMEM, "actual heap prefix OOM");
            fail_after = -1;
            check(actual::g_file_mmap_ranges.size() == old, "OOM no publication");
        }
    }
    for (unsigned mutation = 0; mutation < 10; ++mutation) {
        ++cases;
        FileWorld w;
        w.seed(40);
        actual::LegacyFileForkPreparation p;
        check(p.prepare(w.actor, w.child, w.scope) == 0, "prepare stale/refusal case");
        auto& row = actual::g_file_mmap_ranges.at(0);
        auto old = row;
        auto serial = w.source.serial;
        switch (mutation) {
            case 0:
                ++row.start;
                break;
            case 1:
                ++row.length;
                break;
            case 2:
                ++row.file_offset;
                break;
            case 3:
                row.writeback = !row.writeback;
                break;
            case 4:
                row.pagemap = &w.childroot;
                break;
            case 5:
                ++w.source.serial;
                break;
            case 6:
                try_busy = true;
                break;
            case 7:
                w.child.has_run = true;
                break;
            case 9:
                row.file = &w.files[63];
                break;
            case 8:
                check(p.storage_->retired.reserve(33), "inject real forbidden retired heap");
                break;
        }
        {
            Pub pub;
            irq = false;
            check(p.commit(w.actor, w.child, w.scope, pub) == -EAGAIN, "changed full row, ABA, busy, child or retired heap refuses");
            irq = true;
        }
        row = old;
        w.source.serial = serial;
        try_busy = false;
        w.child.has_run = false;
    }
    for (unsigned saturation = 0; saturation < 60; ++saturation) {
        ++cases;
        FileWorld w;
        w.seed(60);
        auto& f = w.files[saturation];
        int saved = f.refcount;
        f.refcount = INT_MAX;
        {
            actual::LegacyFileForkPreparation p;
            check(p.prepare(w.actor, w.child, w.scope) == -EOVERFLOW, "checked real File reference saturation");
            f.refcount = saved;
        }
    }
    for (unsigned i = 1; i < 60; ++i) {
        if (i % 3 == 0) continue;
        ++cases;
        FileWorld w;
        w.seed(60);
        auto& file = w.files[i];
        int const original = file.refcount;
        {
            actual::LegacyFileForkPreparation plan;
            file.refcount = INT_MAX - 1;
            check(plan.prepare(w.actor, w.child, w.scope) == -EOVERFLOW,
                  "each child retain prefix saturates after one global copied owner");
            file.refcount = original + 1;
        }
    }
    {
        ++cases;
        FileWorld w;
        w.seed(3);
        actual::LegacyFileForkPreparation plan;
        check(plan.prepare(w.actor, w.child, w.scope) == 0, "prepare before native registry frontier");
        actual::NativeMappingEntry native{.root = &w.source};
        actual::g_native_file_mmap_ranges = &native;
        {
            Pub pub;
            check(plan.commit(w.actor, w.child, w.scope, pub) == -EAGAIN, "native source remains separate registry");
        }
        actual::g_native_file_mmap_ranges = nullptr;
    }
    {
        ++cases;
        FileWorld w;
        actual::LegacyFileForkPreparation p;
        ready = false;
        check(p.prepare(w.actor, w.child, w.scope) == -EWOULDBLOCK, "actual current preparation required");
        ready = true;
        w.actor.pagemap = &ker::mod::mm::virt::kernel;
        check(p.prepare(w.actor, w.child, w.scope) == -EINVAL, "kernel root refused");
    }
    std::printf("PASS file cases=%u checks=%u retains=%u puts=%u\n", cases, checks, retains, puts);
}
}  // namespace fixture
int main() { fixture::file_tests(); }
