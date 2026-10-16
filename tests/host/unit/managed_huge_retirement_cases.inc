// Actual raw/budget functions and RegistryCore are compiled above. Aligned Host
// buffers, physical metadata, locks, root gate and ACK are explicit seams.
struct ManagedFixture : Fixture {
    void* backing{};
    PageTable* directory{};
    huge::LeafHandle handle{};
    huge::LeafKey key{};
    uint64_t semantic{};
    static constexpr uint64_t VA = huge::LEAF_BYTES;
    ManagedFixture() : Fixture(0) {
        root_serial = 1;
        managed_huge_registry = HostRegistry{};
        huge::CapsInput caps{};
        caps.node_count = 1;
        caps.usable[0] = 32768;
        expect(managed_huge_registry.initialize(caps) == huge::Error::None, "real registry initialized from finite capacity input");
        backing = std::aligned_alloc(huge::LEAF_BYTES, huge::LEAF_BYTES);
        if (!backing) throw std::bad_alloc();
        std::fill_n(static_cast<unsigned char*>(backing), huge::LEAF_BYTES, 0xa5);
        auto* l3 = reinterpret_cast<PageTable*>(root->entries[0].frame << 12);
        directory = reinterpret_cast<PageTable*>(l3->entries[0].frame << 12);
        key = {{reinterpret_cast<uint64_t>(root), root_serial}, VA, reinterpret_cast<uint64_t>(backing)};
        semantic = key.pa_2m | 0x87;
        expect(managed_huge_registry.reserve_leaf(key.root, VA, 0, handle) == huge::Error::None, "real registry reserves exact root/VA");
        expect(managed_huge_registry.publish_mapped(handle, key, semantic) == huge::Error::None,
               "real registry adopts explicit fixture mapping ownership");
        *word() = semantic;
        observed_handoff = 0;
        for (uint16_t i = 0; i < 512; ++i) {
            auto* page = static_cast<unsigned char*>(backing) + 4096 * i;
            refs[page] = 1;
            kinds[reinterpret_cast<uint64_t>(page)] = PageKind::NORMAL;
            managed_index[page] = i;
            managed_extra[page] = 0;
        }
    }
    auto word() -> uint64_t* { return reinterpret_cast<uint64_t*>(&directory->entries[1]); }
    void retain_edges_and_alias() {
        auto* first = backing;
        auto* last = static_cast<unsigned char*>(backing) + 511 * 4096;
        ++refs[first];
        ++managed_extra[first];
        ++refs[last];
        ++managed_extra[last];
        // A genuine independent4K mapping ref aliases the first physical page.
        leaves[0]->entries[0] = entry(first);
        ++refs[first];
    }
    void expect_mapping_refs_returned() {
        for (auto const& [page, extra] : managed_extra)
            expect(refs[page] == extra, "all512 mapping references returned exactly once; retained aliases remain");
        huge::Snapshot snapshot{};
        managed_huge_registry.snapshot(snapshot);
        expect(snapshot.charged_pages == 0 && snapshot.retired_pages == 0 && snapshot.mapped_pages == 0,
               "actual registry cap closes after mapping references");
        expect(*word() == 0, "managed leaf is atomically absent, including old A/D");
    }
    ~ManagedFixture() {
        expect(held_gates.empty(), "no managed fixture leaves a root gate held");
        for (uint16_t i = 0; i < 512; ++i) {
            auto* p = static_cast<unsigned char*>(backing) + 4096 * i;
            refs.erase(p);
            kinds.erase(reinterpret_cast<uint64_t>(p));
        }
        managed_index.clear();
        managed_extra.clear();
        std::free(backing);
    }
};
void managed_raw_case(bool alias) {
    ManagedFixture f;
    if (alias) f.retain_edges_and_alias();
    *f.word() |= 0x60;  // Real detach allows hardware A/D and clears the whole PDE.
    destroy_user_space(f.root, 1, "managed-host", "raw");
    // Ancestor PTs were actually detached/freed; inspect root rather than freed
    // directory storage. Host buffers remain allocated solely for diagnostics.
    expect(f.root->entries[0].present == 0, "raw actual table collector tears down PS ancestors");
    f.expect_mapping_refs_returned();
}
void managed_dispose_case(uint16_t prefix) {
    ManagedFixture f;
    auto* late_table = f.table();
    auto* late_data = f.alloc(PageKind::NORMAL);
    f.directory->entries[2] = Fixture::entry(late_table);
    late_table->entries[0] = Fixture::entry(late_data);
    auto* state = new DestroyUserSpaceBudgetState{};
    expect(begin_prepared_destroy_user_space(*state, f.root, 1, "managed-host", "partial", true), "actual begin owns original root gate");
    DestroyUserSpaceCallStats stats{};
    unsigned bounded = 0;
    while (state->phase == DestroyUserSpacePhase::COLLECT_FRAMES && ++bounded < 1000) advance_collect_frames_budgeted(*state, stats);
    expect(bounded < 1000, "actual collector stops at known PS and retains exact VA");
    while (!state->managed_cursor.active && ++bounded < 2000) advance_free_data_budgeted(*state, stats);
    expect(state->managed_cursor.active && state->managed_cursor.next == 0, "detach boundary owns all512 unqueued references");
    expect(*f.word() == 0, "actual PS detached before any batch ownership");
    for (uint16_t i = 0; i < prefix; ++i) advance_free_data_budgeted(*state, stats);
    expect(state->managed_cursor.next == prefix, "each actual budget step hands off exactly one reference");
    destroy_user_space_budget_state_destroy(state);
    f.expect_mapping_refs_returned();
    expect(refs[late_data] == 1 && late_table->entries[0].present, "partial disposal preserves still-reachable remainder");
    destroy_user_space(f.root, 1, "managed-host", "reachable-remainder");
    expect(refs[late_data] == 0, "caller can later retire the original reachable remainder");
}
void managed_budget_case(unsigned steps) {
    ManagedFixture f;
    f.retain_edges_and_alias();
    auto* state = new DestroyUserSpaceBudgetState{};
    expect(begin_prepared_destroy_user_space(*state, f.root, 1, "managed-host", "budget", true), "actual budget begins");
    expect(!destroy_user_space_budgeted(state, 0), "budget zero neither detaches nor retires managed data");
    unsigned rounds = 0;
    while (!destroy_user_space_budgeted(state, steps) && ++rounds < 10000) {
    }
    expect(rounds < 10000 && state->phase == DestroyUserSpacePhase::DONE, "actual managed budget reaches DONE");
    expect(state->refdec_batch.count == 0 && !state->managed_cursor.active, "DONE owns no hidden detached prefix or tail");
    destroy_user_space_budget_state_destroy(state);
    f.expect_mapping_refs_returned();
}
void managed_refusal_case(unsigned mutation, bool budget) {
    ManagedFixture f;
    uint64_t* slot = f.word();
    uint64_t old = *slot;
    huge::LeafHandle stale = f.handle;
    ++stale.generation;
    expect(!managed_huge_registry.match_mapped(stale, f.key, old), "stale handle cannot acquire mapped ownership");
    switch (mutation) {
        case 0:
            ++root_serial;
            break;
        case 1:
            *slot ^= 2;
            break;
        case 2:
            *slot ^= huge::LEAF_BYTES;
            break;
        case 3: {
            auto* l3 = reinterpret_cast<PageTable*>(f.root->entries[0].frame << 12);
            slot = reinterpret_cast<uint64_t*>(&l3->entries[1]);
            old = *slot;
            *slot = 0x40000087;
            break;
        }
        case 4:
            slot = reinterpret_cast<uint64_t*>(&f.directory->entries[2]);
            old = *slot;
            *slot = f.semantic;
            break;
        case 5:
            *slot = 0x87;
            break;
    }
    uint64_t const invalid = *slot;
    bool refused = false;
    DestroyUserSpaceBudgetState* state = nullptr;
    try {
        if (budget) {
            state = new DestroyUserSpaceBudgetState{};
            begin_prepared_destroy_user_space(*state, f.root, 1, "host", "refusal", true);
            destroy_user_space_budgeted(state, 8192);
        } else
            destroy_user_space(f.root, 1, "host", "refusal");
    } catch (std::runtime_error const&) {
        refused = true;
    }
    expect(refused && *slot == invalid, "unknown/stale root/VA/PA/semantic/1G refused before mutation");
    for (auto const& [page, extra] : managed_extra) expect(refs[page] == 1 + extra, "refusal preserves every mapping reference");
    *slot = old;
    root_serial = 1;
    if (state) destroy_user_space_budget_state_destroy(state);
    destroy_user_space(f.root, 1, "host", "restored-valid-root");
    f.expect_mapping_refs_returned();
}
// Pure predicate vector: a local scalar registry has no physical/Task lease.
// Actual ref/ACK ownership is exercised separately by the real helper cases.
void retired_predicate_case() {
    huge::RegistryCore core;
    huge::CapsInput caps{};
    caps.node_count = 1;
    caps.usable[0] = 32768;
    expect(core.initialize(caps) == huge::Error::None, "predicate registry initialized");
    huge::RootKey root{0x1000, 7};
    huge::LeafHandle handle{};
    expect(core.reserve_leaf(root, 0x200000, 0, handle) == huge::Error::None, "predicate reserve");
    expect(core.publish_mapped(handle, {root, 0x200000, 0x400000}, 0x400087) == huge::Error::None, "predicate mapped row");
    expect(core.root_has_published_owners(root) && !core.root_has_retired_owners(root),
           "reachable Mapped row is not a detached disposal tail");
    expect(core.detach_mapped(handle, {root, 0x200000, 0x400000}) == huge::Error::None, "predicate detach");
    expect(core.root_has_retired_owners(root) && !core.root_has_retired_owners({root.address_bits, 8}),
           "retired query retains exact incarnation");
}
void run_managed_tests() {
    retired_predicate_case();
    managed_raw_case(false);
    managed_raw_case(true);
    for (uint16_t i = 0; i <= 512; ++i) managed_dispose_case(i);
    for (unsigned steps : {1U, 63U, 64U, 65U, 8192U}) managed_budget_case(steps);
    for (unsigned mutation = 0; mutation < 6; ++mutation) {
        managed_refusal_case(mutation, false);
        managed_refusal_case(mutation, true);
    }
}
