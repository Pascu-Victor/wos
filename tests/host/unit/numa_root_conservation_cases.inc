// Pure snapshot inputs are explicit Host provider values. Actual route/pool/
// operation-window/observation/conservation functions are extracted unchanged.
auto old_root_closure(const phys::PhysicalBalanceSnapshot& before, const phys::PhysicalBalanceSnapshot& after) -> bool {
    struct State {
        const phys::PhysicalBalanceSnapshot& root_owner_before;
        const phys::PhysicalBalanceSnapshot& root_owner_after;
    } state{before, after};
    auto check = [](bool value, const char*) { return value; };
    // Exact original109 loop: the genuine old control rejects retained roots.
    for (auto owner : {mm::PhysicalPageOwner::PAGE_TABLE, mm::PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE}) {
        auto const index = static_cast<size_t>(owner);
        if (!check(state.root_owner_after.owners[index].pages == state.root_owner_before.owners[index].pages &&
                       state.root_owner_after.owners[index].objects == state.root_owner_before.owners[index].objects,
                   "actual root page-table owner baseline restored"))
            return false;
    }
    return true;
}
void conservation_cases() {
    unsigned const initial_assertions = assertions;
    using Observation = virt::SelftestPageTablePoolObservation;
    virt::RootRouteDiagnostics routes{};
    auto capture = [&](size_t index) {
        auto& f = routes.frontiers[index];
        f.observed = true;
        virt::get_page_table_pool_stats_snapshot(f.pool);
        virt::selftest_observe_page_table_pool(0, f.proof);
        f.emergency.published = f.emergency.healthy = true;
        f.emergency.sequence = 1;
        f.emergency.members = 128;
        f.emergency.node_count = 2;
        for (unsigned n = 0; n < 2; ++n) f.emergency.nodes[n] = {64, 64, 0, 0, 0};
    };
    for (unsigned n = 0; n < 5; ++n) capture(n);
    auto& route = routes.roots[1];
    route.factory_called = true;
    auto* root = create(route.factory);
    check(root != nullptr, "actual default root allocated");
    capture(2);
    capture(3);
    bool probed = false;
    retag_probe = +[](void* pointer, PhysicalPageOwner) {
        Observation during{};
        virt::selftest_observe_page_table_pool(reinterpret_cast<uint64_t>(pointer), during);
        check(!during.valid && during.inflight != 0, "off-lock retag window cannot look clean");
    };
    route.release_called = true;
    virt::selftest_release_pagemap(root, route.release);
    retag_probe = nullptr;
    probed = route.release.insertion.inserted;
    check(probed && route.release.insertion.completed_sequence != 0, "only actual completed insertion issues receipt");
    capture(4);
    // Match the exact actual retained historical identity, with no frame read.
    virt::selftest_observe_page_table_pool(route.release.historical_root_bits, routes.frontiers[4].proof);
    Observation before{}, after{};
    virt::selftest_observe_page_table_pool(route.release.historical_root_bits, before);
    virt::selftest_observe_page_table_pool(route.release.historical_root_bits, after);
    phys::PhysicalBalanceSnapshot a{}, b{};
    constexpr auto POOL = static_cast<size_t>(PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE);
    constexpr auto PT = static_cast<size_t>(PhysicalPageOwner::PAGE_TABLE);
    b.owners[POOL] = {1, 1};
    check(virt::root_retained_pool_conservation(routes, before, after, a, b), "exact retained owner accepted");
    check(!old_root_closure(a, b), "actual old unconditional equality rejects genuine retained owner");
    auto refuses = [&](const char* text) { check(!virt::root_retained_pool_conservation(routes, before, after, a, b), text); };
    ++after.sequence;
    refuses("changed generation refused");
    --after.sequence;
    after.matching_entries = 0;
    refuses("wrong historical member refused");
    after.matching_entries = 1;
    after.inflight = 1;
    refuses("inflight refused");
    after.inflight = 0;
    after.valid = false;
    refuses("saturated invalid proof refused");
    after.valid = true;
    ++b.owners[POOL].pages;
    refuses("unexplained extra page refused");
    --b.owners[POOL].pages;
    ++b.owners[POOL].objects;
    refuses("unexplained extra object refused");
    --b.owners[POOL].objects;
    ++b.owners[PT].pages;
    refuses("active table nonclosure refused");
    --b.owners[PT].pages;
    routes.frontiers[4].emergency.sequence = 2;
    refuses("emergency semantic mutation refused");
    routes.frontiers[4].emergency.sequence = 1;
    route.release.insertion.inserted = false;
    refuses("descriptive disposition alone has no authority");
    route.release.insertion.inserted = true;
    ++route.release.historical_pa;
    refuses("wrong acquisition return PA refused");
    --route.release.historical_pa;
    routes.frontiers[1].proof.cached_pages = 1;
    refuses("nonempty original cache refused");
    routes.frontiers[1].proof.cached_pages = 0;
    // Even a forged clean MAX observation/receipt is never authority.
    auto saved_routes = routes;
    auto saved_before = before, saved_after = after;
    for (size_t i = 1; i <= 3; ++i) routes.frontiers[i].proof.sequence = UINT64_MAX - 2;
    routes.frontiers[4].proof.sequence = before.sequence = after.sequence = UINT64_MAX;
    route.release.insertion.completed_sequence = UINT64_MAX;
    refuses("saturated completed receipt refused by actual conservation predicate");
    routes = saved_routes;
    before = saved_before;
    after = saved_after;
    virt::SelftestRootPoolInsertion duplicate{1, true};
    auto const rejects = virt::page_table_pool_rejects.load();
    check(virt::try_release_page_table_to_pool(root, {}, &duplicate), "old duplicate true preserved");
    check(!duplicate.inserted && duplicate.completed_sequence == 0 && virt::page_table_pool_rejects.load() == rejects + 1,
          "duplicate does not claim actual insertion");
    // Pop and return the same pointer: PA membership alone is an ABA false proof.
    retag_probe = +[](void* pointer, PhysicalPageOwner) {
        Observation during{};
        virt::selftest_observe_page_table_pool(reinterpret_cast<uint64_t>(pointer), during);
        check(!during.valid && during.inflight != 0, "pop remains inflight through owner restoration");
    };
    auto* reused = virt::try_alloc_page_table_from_pool();
    retag_probe = nullptr;
    check(reused == root, "actual pop uses same retained pointer");
    virt::release_unregistered_user_root(reused);
    virt::selftest_observe_page_table_pool(route.release.historical_root_bits, after);
    refuses("actual pop reinsert ABA refused despite same PA");
    reset();
    // Actual raced-full path restores the root owner before proof completion.
    virt::SelftestRootFactoryOutput raced{};
    auto* raced_root = create(raced);
    retag_probe = +[](void*, PhysicalPageOwner desired) {
        if (desired != PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE) return;
        retag_probe = nullptr;
        for (size_t i = 0; i < virt::PAGE_TABLE_POOL_CAPACITY; ++i) {
            auto* page = static_cast<paging::PageTable*>(allocate(PhysicalPageOwner::PAGE_TABLE));
            phys::page_mark_kind(page, PageKind::PAGE_TABLE);
            virt::release_unregistered_user_root(page);
        }
    };
    virt::SelftestRootReleaseOutput rejected{};
    virt::selftest_release_pagemap(raced_root, rejected);
    virt::selftest_observe_page_table_pool(rejected.historical_root_bits, after);
    check(rejected.disposition == R::PHYSICAL_RELEASED && !rejected.insertion.inserted && after.valid && after.inflight == 0 &&
              after.matching_entries == 0 && after.cached_pages == 64,
          "raced-full rollback completes owner and counter tail before clean proof");
    reset();
    // Actual duplicate-after-retag path still returns true but lends no receipt.
    auto* duplicate_root = create(raced);
    retag_probe = +[](void* pointer, PhysicalPageOwner desired) {
        if (desired != PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE) return;
        retag_probe = nullptr;
        check(virt::try_release_page_table_to_pool(static_cast<paging::PageTable*>(pointer), {}), "fault-injected raced adoption");
    };
    virt::selftest_release_pagemap(duplicate_root, rejected);
    virt::selftest_observe_page_table_pool(rejected.historical_root_bits, after);
    check(rejected.disposition == R::LEGACY_CACHED && !rejected.insertion.inserted && after.valid && after.inflight == 0 &&
              after.matching_entries == 1,
          "raced duplicate true cannot issue insertion receipt");
    reset();
    // Both final sequence boundaries retire the tracked intent but issue no
    // receipt, without suppressing the real physical allocation/cache return.
    for (auto sequence : {UINT64_MAX - 2, UINT64_MAX - 1}) {
        virt::page_table_pool.proof_sequence = sequence;
        virt::page_table_pool.proof_inflight = 0;
        virt::page_table_pool.proof_invalid = false;
        virt::SelftestRootFactoryOutput acquired{};
        auto* boundary = create(acquired);
        virt::SelftestRootReleaseOutput released{};
        virt::selftest_release_pagemap(boundary, released);
        virt::selftest_observe_page_table_pool(released.historical_root_bits, after);
        check(!released.insertion.inserted && released.insertion.completed_sequence == 0 && !after.valid && after.sequence == UINT64_MAX &&
                  after.inflight == 0 && after.cached_pages == 1 && released.disposition == R::LEGACY_CACHED,
              "reaching sequence sentinel never issues insertion authority");
        reset();
    }
    virt::page_table_pool.proof_sequence = UINT64_MAX;
    virt::page_table_pool.proof_invalid = false;  // defensive scalar-observer vector
    virt::selftest_observe_page_table_pool(0, after);
    check(!after.valid, "observed MAX is never a clean bracket");
    virt::page_table_pool.proof_sequence = UINT64_MAX - 1;
    virt::page_table_pool.proof_inflight = 1;
    virt::page_table_pool.proof_invalid = false;
    check(virt::page_table_pool_proof_end(true) == 0 && virt::page_table_pool.proof_sequence == UINT64_MAX &&
              virt::page_table_pool.proof_inflight == 0 && virt::page_table_pool.proof_invalid,
          "actual final end overflow retires inflight and invalidates permanently");
    reset();
    // Sticky saturation never changes real ordinary allocation/return policy.
    virt::page_table_pool.proof_sequence = UINT64_MAX;
    virt::SelftestRootFactoryOutput overflow{};
    auto* ordinary = create(overflow);
    virt::SelftestRootReleaseOutput returned{};
    virt::selftest_release_pagemap(ordinary, returned);
    virt::selftest_observe_page_table_pool(returned.historical_root_bits, after);
    check(!returned.insertion.inserted && !after.valid && after.sequence == UINT64_MAX && after.cached_pages == 1,
          "saturation invalidates proof without suppressing cache insertion");
    reset();
    // Test-only reset of the explicitly modeled pool between isolated vectors;
    // no such reset API exists in production or fixture.
    virt::page_table_pool.proof_sequence = virt::page_table_pool.proof_inflight = 0;
    virt::page_table_pool.proof_invalid = false;
    phys::NodeCounterSnapshot nodes{}, baseline{};
    nodes.complete = baseline.complete = true;
    nodes.node_count = baseline.node_count = 2;
    nodes.allocator_count = baseline.allocator_count = 1;
    nodes.nodes[0] = {10, 5, 1, 4};
    nodes.nodes[1] = {20, 7, 2, 5};
    nodes.global = {30, 12, 3, 9};
    baseline = nodes;
    phys::PhysicalBalanceSnapshot physical{};
    physical.managed_pages = physical.identity_pages = 40;
    physical.free_pages = 12;
    physical.owner_pages = 28;
    check(virt::post_clear_accounting_complete(nodes, baseline, physical), "exact postclear node physical identities");
    ++physical.free_pages;
    check(!virt::post_clear_accounting_complete(nodes, baseline, physical), "sequential physical disagreement refuses");
    --physical.free_pages;
    ++nodes.nodes[1].cached;
    check(!virt::post_clear_accounting_complete(nodes, baseline, physical), "bad node buddy cache identity refuses");
    --nodes.nodes[1].cached;
    ++nodes.nodes[1].usable;
    check(!virt::post_clear_accounting_complete(nodes, baseline, physical), "changed immutable denominator refuses");
    --nodes.nodes[1].usable;
    physical.identity_mismatch_pages = 1;
    check(!virt::post_clear_accounting_complete(nodes, baseline, physical), "physical mismatch refuses");
    std::printf("PASS retained_root_conservation extra_assertions=%u explicit_host_endpoints=1\n", assertions - initial_assertions);
}
