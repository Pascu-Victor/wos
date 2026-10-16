namespace fixture {
namespace actual = ker::mod::mm::virt;
struct SwapWorld : Actors {
    unsigned n{};
    SwapWorld() {
        check(actual::anonymous_swap_mappings == nullptr, "previous descriptors fully purged");
        actual::fixture_pte::cells = {};
    }
    void seed(unsigned count, bool same_object = false) {
        n = count;
        actual::AnonymousSwapObject* shared = nullptr;
        for (unsigned i = 0; i < count; ++i) {
            auto* r = actual::allocate_swap_descriptor<actual::AnonymousSwapMapping>(actual::DebtField::SWAP_MAPPINGS);
            check(r != nullptr, "seed actual descriptor");
            r->pagemap = &source;
            r->vaddr = 0x400000 + 4096 * i;
            r->generation = 11 + i;
            r->original_raw = 2 | 4;
            r->state = actual::AnonymousSwapRuntimeState::SWAPPED;
            r->maintenance_valid = true;
            r->maintenance_placement.local_node = 1;
            if (!same_object || !shared) {
                shared = actual::allocate_swap_descriptor<actual::AnonymousSwapObject>(actual::DebtField::SWAP_OBJECTS);
                check(shared != nullptr, "seed actual object");
            }
            r->object = shared;
            ++shared->mapping_refs;
            actual::insert_anonymous_swap_mapping_locked(r);
            ++actual::anonymous_swap_stats.swapped_pages;
            uint64_t marker = actual::anonymous_swap_marker_raw(r->original_raw);
            actual::fixture_pte::cells[2 * i] = {&source, r->vaddr, marker};
            actual::fixture_pte::cells[2 * i + 1] = {&childroot, r->vaddr, marker};
        }
    }
    actual::AnonymousSwapMapping* first() {
        for (auto* r = actual::anonymous_swap_mappings; r; r = r->next)
            if (r->pagemap == &source) return r;
        return nullptr;
    }
    unsigned child_rows() {
        unsigned c = 0;
        for (auto* r = actual::anonymous_swap_mappings; r; r = r->next) c += r->pagemap == &childroot;
        return c;
    }
    ~SwapWorld() {
        actual::purge_anonymous_swap_mappings(&childroot);
        actual::purge_anonymous_swap_mappings(&source);
        check(actual::anonymous_swap_mappings == nullptr, "real purge unlinks every row");
        check(actual::anonymous_swap_stats.swapped_pages == 0, "real swapped row stats closure");
        for (long v : ker::test::numa_lifecycle::mm_debt::values) check(v == 0, "all real descriptor/slot/debt prefixes returned");
    }
};
void swap_tests() {
    for (unsigned n : {0u, 1u, 5u, 20u})
        for (bool aliases : {false, true}) {
            ++cases;
            SwapWorld w;
            w.seed(n, aliases);
            {
                actual::PreparedAnonymousSwapFork p;
                unsigned before = allocations;
                check(p.prepare(w.actor, w.child, w.scope) == 0, "actual descriptors prepared");
                check(allocations - before == (n ? n + 2 : 0), "no terminating descriptor, empty no heap");
                unsigned prepared = allocations;
                {
                    Pub pub;
                    irq = false;
                    check(actual::detail::PreparedSwapForkAccess::preflight(p, &w.source, &w.childroot) == 0,
                          "actual pre-split complete validation");
                    check(actual::detail::PreparedSwapForkAccess::commit(p, &w.source, &w.childroot) == 0,
                          "prepared descriptors adopted all");
                    check(allocations == prepared, "no allocation in final swap adoption");
                    check(w.child_rows() == n, "one actual child descriptor per source");
                    if (p.storage_)
                        for (size_t i = 0; i < p.storage_->capacity; ++i)
                            check(p.storage_->rows[i] == nullptr, "adopted descriptor leaves exact prepared owner");
                    check(actual::detail::PreparedSwapForkAccess::commit(p, &w.source, &w.childroot) == -EAGAIN, "swap single adoption");
                    irq = true;
                }
                for (auto* r = actual::anonymous_swap_mappings; r; r = r->next)
                    if (r->pagemap == &w.childroot) {
                        check(!r->pagein_queued && !r->waiters && !r->frame && !r->cancelled, "cloned transient owners reset");
                        check(r->object->mapping_refs == (aliases ? 2 * n : 2), "aggregate actual shared object refs");
                        check((r->original_raw & 512) != 0 && (r->original_raw & 2) == 0, "actual COW original flags");
                    }
            }
        }
    for (int f = 0; f < 6; ++f) {
        ++cases;
        SwapWorld w;
        w.seed(4);
        {
            actual::PreparedAnonymousSwapFork p;
            fail_after = f;
            check(p.prepare(w.actor, w.child, w.scope) == -ENOMEM, "every actual prepared allocation prefix OOM");
            fail_after = -1;
            check(w.child_rows() == 0, "OOM no descriptor publication");
        }
    }
    for (unsigned mutation = 0; mutation < 13; ++mutation) {
        ++cases;
        SwapWorld w;
        w.seed(2, true);
        actual::PreparedAnonymousSwapFork p;
        check(p.prepare(w.actor, w.child, w.scope) == 0, "prepare validation refusal");
        auto* r = w.first();
        auto original = *r;
        auto refs = r->object->mapping_refs;
        switch (mutation) {
            case 0:
                r->state = actual::AnonymousSwapRuntimeState::PAGEIN;
                break;
            case 1:
                r->pagein_queued = true;
                break;
            case 2:
                r->frame = &w;
                break;
            case 3:
                r->waiters = &w.child;
                break;
            case 4:
                r->cancelled = true;
                break;
            case 5:
                r->generation = 0;
                break;
            case 6:
                r->maintenance_valid = false;
                break;
            case 7:
                r->maintenance_placement.local_node = 2;
                break;
            case 8:
                r->object->mapping_refs = UINT32_MAX - 1;
                break;
            case 9:
                r->pagein_access_gate = 0;
                break;
            case 10:
                actual::fixture_pte::cells[1].raw = 0;
                break;
            case 11:
                ++w.source.serial;
                break;
            case 12:
                r->error = -ENOMEM;
                break;
        }
        {
            Pub pub;
            irq = false;
            int result = actual::detail::PreparedSwapForkAccess::commit(p, &w.source, &w.childroot);
            check(result == (mutation == 8 ? -EOVERFLOW : -EAGAIN), "full transaction refuses before first metadata store");
            irq = true;
        }
        check(w.child_rows() == 0, "no prefix on rejected adoption");
        r->object->mapping_refs = refs;
        *r = original;
    }
    {
        ++cases;
        SwapWorld w;
        w.seed(2, true);
        actual::PreparedAnonymousSwapFork plan;
        check(plan.prepare(w.actor, w.child, w.scope) == 0, "prepare aliased shared object");
        {
            Pub pub;
            irq = false;
            check(actual::detail::PreparedSwapForkAccess::preflight(plan, &w.source, &w.childroot) == 0, "early aggregate refs allowed");
            auto* object = w.first()->object;
            auto refs = object->mapping_refs;
            object->mapping_refs = UINT32_MAX - 1;
            check(actual::detail::PreparedSwapForkAccess::commit(plan, &w.source, &w.childroot) == -EOVERFLOW,
                  "final one-COW transaction repeats shared object bound");
            object->mapping_refs = refs;
            irq = true;
        }
        check(w.child_rows() == 0, "late shared-object change has no prefix");
    }
    {
        ++cases;
        SwapWorld w;
        w.seed(4);
        actual::PreparedAnonymousSwapFork plan;
        check(plan.prepare(w.actor, w.child, w.scope) == 0, "prepare retained empty descriptor capacity");
        actual::purge_anonymous_swap_mappings(&w.source);
        {
            Pub pub;
            check(actual::detail::PreparedSwapForkAccess::commit(plan, &w.source, &w.childroot) == 0, "fresh empty source adopts none");
        }
        check(w.child_rows() == 0, "unused descriptors remain private until ordinary destructor");
    }
    {
        ++cases;
        SwapWorld w;
        actual::PreparedAnonymousSwapFork plan;
        check(plan.prepare(w.actor, w.child, w.scope) == 0, "empty source plan");
        w.seed(1);
        {
            Pub pub;
            check(actual::detail::PreparedSwapForkAccess::preflight(plan, &w.source, &w.childroot) == -EAGAIN,
                  "fresh source growth refuses inadequate real descriptors");
        }
        check(w.child_rows() == 0, "source count growth has no child prefix");
    }
    for (bool retained : {false, true}) {
        ++cases;
        SwapWorld w;
        w.seed(1);
        auto* r = w.first();
        r->state = actual::AnonymousSwapRuntimeState::FAILED;
        r->error = -EIO;
        --actual::anonymous_swap_stats.swapped_pages;
        if (!retained) {
            auto* object = actual::detach_anonymous_swap_object_locked(r);
            actual::release_anonymous_swap_object(object);
        }
        actual::PreparedAnonymousSwapFork p;
        check(p.prepare(w.actor, w.child, w.scope) == 0, "both genuine FAILED forms prepare");
        {
            Pub pub;
            check(actual::detail::PreparedSwapForkAccess::commit(p, &w.source, &w.childroot) == 0,
                  "both FAILED forms inherit exact stored error");
        }
        check(w.child_rows() == 1, "FAILED descriptor clone");
        for (auto* c = actual::anonymous_swap_mappings; c; c = c->next)
            if (c->pagemap == &w.childroot)
                check(c->error == -EIO && (c->object != nullptr) == retained, "FAILED object ownership form preserved");
    }
    std::printf("PASS swap cases=%u checks=%u\n", cases, checks);
}
}  // namespace fixture
int main() { fixture::swap_tests(); }
