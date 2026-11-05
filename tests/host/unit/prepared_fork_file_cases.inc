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
using EmptyMatch = actual::LegacyFileForkPreparation::EmptyMatch;
static_assert(!std::is_copy_constructible_v<EmptyMatch> && !std::is_move_constructible_v<EmptyMatch>);
static_assert(!std::is_copy_assignable_v<EmptyMatch> && !std::is_move_assignable_v<EmptyMatch>);
void empty_match_tests() {
    for (bool remove_foreign : {false, true}) {
        ++cases;
        FileWorld w;
        w.seed(90);
        for (auto& row : actual::g_file_mmap_ranges) row.pagemap = &w.foreign;
        check(actual::g_file_mmap_ranges.is_spilled(), "foreign registry really exceeds inline capacity");
        std::array<actual::FileMmapRange, 90> original;
        for (unsigned i = 0; i < original.size(); ++i) original[i] = actual::g_file_mmap_ranges.at(i);
        unsigned const before_alloc = allocations, before_retains = retains, before_puts = puts;
        actual::LegacyFileForkPreparation plan;
        check(plan.prepare(w.actor, w.child, w.scope) == 0 && plan.storage_ == nullptr,
              "zero source with ninety foreign rows uses scalar preparation");
        EmptyMatch proof;
        actual::FileMmapRange retired{};
        {
            Pub pub;
            check(plan.matches(w.actor, w.child, w.scope, pub, nullptr, &proof) == 0 && proof.armed(),
                  "actual fresh locked match issues empty certificate");
            unsigned const matched_tries = registry_tries;
            auto* const buffer = &actual::g_file_mmap_ranges.at(32);
            if (remove_foreign) {
                retired = actual::g_file_mmap_ranges.at(89);
                actual::g_file_mmap_ranges.pop_back();  // Explicit foreign teardown endpoint, no File close under PUB.
            }
            try_busy = true;
            irq = false;
            check(plan.commit(w.actor, w.child, w.scope, pub, &proof) == 0, "certified no-op survives foreign registry busy/removal");
            check(!proof.armed() && registry_tries == matched_tries, "consume once without registry reacquisition");
            check(&actual::g_file_mmap_ranges.at(32) == buffer && actual::g_file_mmap_ranges.size() == (remove_foreign ? 89u : 90u),
                  "fast commit never moves unrelated spilled registry");
            for (unsigned i = 0; i < actual::g_file_mmap_ranges.size(); ++i)
                check(actual::legacy_row_equal(actual::g_file_mmap_ranges.at(i), original[i]),
                      "foreign geometry/File identity unchanged by commit");
            check(retains == before_retains && puts == before_puts && allocations == before_alloc,
                  "empty prepare/match/commit has no heap or foreign File ownership traffic");
            check(plan.commit(w.actor, w.child, w.scope, pub, &proof) == -EAGAIN && !proof.armed(), "certificate cannot be reused");
            irq = true;
        }
        try_busy = false;
        if (remove_foreign) actual::return_prepared_legacy_file(retired.file);
    }
    for (unsigned refusal = 0; refusal < 9; ++refusal) {
        ++cases;
        FileWorld w;
        actual::LegacyFileForkPreparation plan, other;
        check(plan.prepare(w.actor, w.child, w.scope) == 0 && other.prepare(w.actor, w.child, w.scope) == 0, "prepare empty identities");
        EmptyMatch proof;
        Task wrong_actor = {}, wrong_child = {};
        wrong_actor.pagemap = &w.source;
        wrong_child.pagemap = &w.childroot;
        {
            Pub pub;
            check(plan.matches(w.actor, w.child, w.scope, pub, nullptr, &proof) == 0 && proof.armed(), "fresh proof before refusal");
            if (refusal == 0) proof.reset();
            if (refusal == 1) {
                check(other.commit(w.actor, w.child, w.scope, pub, &proof) == -EAGAIN && !proof.armed(), "wrong plan consumes/refuses");
                continue;
            }
            if (refusal == 2) {
                check(plan.commit(wrong_actor, w.child, w.scope, pub, &proof) == -EAGAIN && !proof.armed(), "wrong actor consumes/refuses");
                continue;
            }
            if (refusal == 3) {
                check(plan.commit(w.actor, wrong_child, w.scope, pub, &proof) == -EAGAIN && !proof.armed(), "wrong Child consumes/refuses");
                continue;
            }
            if (refusal == 4) ++w.source.serial;
            if (refusal == 5) ++w.childroot.serial;
            if (refusal == 6) w.child.has_run = true;
            if (refusal == 7) ready = false;
            if (refusal == 8) preempt = 1;
            check(plan.commit(w.actor, w.child, w.scope, pub, &proof) == -EAGAIN && !proof.armed(),
                  "missing/root/context/Child refusal consumes proof");
            ready = true;
            preempt = 0;
            w.child.has_run = false;
        }
    }
    {
        ++cases;
        FileWorld w;
        actual::LegacyFileForkPreparation plan;
        check(plan.prepare(w.actor, w.child, w.scope) == 0, "prepare before Source added");
        actual::FileMmapRange row{.pagemap = &w.source, .start = 0x400000, .length = 4096, .file = &w.files[0]};
        check(actual::retain_prepared_legacy_file(row.file) && actual::g_file_mmap_ranges.push_back_no_grow(row),
              "actual Source addition before fresh match");
        EmptyMatch proof;
        {
            Pub pub;
            check(plan.matches(w.actor, w.child, w.scope, pub, nullptr, &proof) == -EAGAIN && !proof.armed(),
                  "storage-null alone cannot certify added Source");
        }
    }
    for (bool native_child : {false, true}) {
        ++cases;
        FileWorld w;
        actual::LegacyFileForkPreparation plan;
        check(plan.prepare(w.actor, w.child, w.scope) == 0, "prepare before Native addition");
        actual::NativeMappingEntry entry{.root = native_child ? &w.childroot : &w.source};
        actual::g_native_file_mmap_ranges = &entry;
        EmptyMatch proof;
        {
            Pub pub;
            check(plan.matches(w.actor, w.child, w.scope, pub, nullptr, &proof) == -EAGAIN && !proof.armed(),
                  "fresh Native Source/Child entry refuses");
        }
        actual::g_native_file_mmap_ranges = nullptr;
    }
    for (bool busy : {false, true}) {
        ++cases;
        FileWorld w;
        actual::LegacyFileForkPreparation plan;
        check(plan.prepare(w.actor, w.child, w.scope) == 0, "prepare reissuance");
        EmptyMatch proof;
        {
            Pub pub;
            check(plan.matches(w.actor, w.child, w.scope, pub, nullptr, &proof) == 0 && proof.armed(), "initial certificate issued");
            try_busy = busy;
            ready = busy;
            check(plan.matches(w.actor, w.child, w.scope, pub, nullptr, &proof) == -EAGAIN && !proof.armed(),
                  "failed fresh issuance clears old proof");
            ready = true;
            check(plan.commit(w.actor, w.child, w.scope, pub, &proof) == -EAGAIN && !proof.armed(),
                  "failed issuance leaves no fast authority");
        }
        try_busy = false;
    }
    {
        ++cases;
        FileWorld w;
        actual::LegacyFileForkPreparation first, second;
        check(first.prepare(w.actor, w.child, w.scope) == 0 && second.prepare(w.actor, w.child, w.scope) == 0,
              "prepare actual PUB ABA plans");
        EmptyMatch stale, fresh;
        alignas(Pub) unsigned char bytes[sizeof(Pub)];
        auto* pub = ::new (bytes) Pub;
        check(first.matches(w.actor, w.child, w.scope, *pub, nullptr, &stale) == 0 && stale.armed(), "old guard generation issued");
        auto const generation = stale.generation_;
        pub->~Pub();
        pub = ::new (bytes) Pub;
        check(second.matches(w.actor, w.child, w.scope, *pub, nullptr, &fresh) == 0 && fresh.armed() && fresh.generation_ != generation,
              "actual reconstruction at same address receives distinct lazy generation");
        try_busy = true;
        check(first.commit(w.actor, w.child, w.scope, *pub, &stale) == -EAGAIN && !stale.armed(),
              "pointer equality cannot authorize reconstructed PUB");
        check(second.commit(w.actor, w.child, w.scope, *pub, &fresh) == 0 && !fresh.armed(), "new actual guard certificate remains valid");
        pub->~Pub();
        try_busy = false;
    }
    {
        ++cases;
        FileWorld w;
        actual::LegacyFileForkPreparation plan;
        check(plan.prepare(w.actor, w.child, w.scope) == 0, "prepare default path");
        {
            Pub pub;
            try_busy = true;
            check(plan.commit(w.actor, w.child, w.scope, pub) == -EAGAIN, "no supplied proof preserves original late busy refusal");
        }
        try_busy = false;
    }
    // Last in this executable: fault-inject exhaustion of the actual plain
    // PUB-protected counter. Production never resets or wraps this counter.
    {
        ++cases;
        FileWorld w;
        actual::LegacyFileForkPreparation plan;
        check(plan.prepare(w.actor, w.child, w.scope) == 0, "prepare counter exhaustion");
        actual::g_empty_fork_publication_generation = UINT64_MAX;
        EmptyMatch proof;
        {
            Pub pub;
            check(plan.matches(w.actor, w.child, w.scope, pub, nullptr, &proof) == 0 && !proof.armed(),
                  "saturated actual issuer leaves match successful but unarmed");
            unsigned const tries = registry_tries;
            irq = false;
            check(plan.commit(w.actor, w.child, w.scope, pub, proof.armed() ? &proof : nullptr) == 0 && registry_tries == tries + 1,
                  "unarmed caller falls back to original locked commit");
            check(actual::g_empty_fork_publication_generation == UINT64_MAX, "counter never wraps");
            irq = true;
        }
    }
}
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
    empty_match_tests();
    std::printf("PASS file cases=%u checks=%u retains=%u puts=%u\n", cases, checks, retains, puts);
}
}  // namespace fixture
int main() { fixture::file_tests(); }
