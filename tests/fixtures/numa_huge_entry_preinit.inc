// Genuine pre-initialization negative control from immutable36906 CPP.
// HISTORICAL extraction source CPP SHA256: 1b4bfb262953c69d01dfe11985939118bd11b1181aec15e2fc21661bfe5b2510
// HISTORICAL extracted run_numa_huge body SHA256: 1226d7d4ec25cc503ee8ed339114f1e86e1c4a306d73c137041efb38bb613e3a
// Intentionally retains the old complete=false defect; source-only Host control.
auto run_numa_huge(int argc, char** argv) -> int {
    HugeOptions options;
    if (!huge_options(argc, argv, options)) {
        std::fprintf(stderr, "numa-huge: invalid bounded options/scheduled holds\n");
        return 2;
    }
    HugeState s;
    initialize_transport(s);
    s.options = options;
    s.remaining = options.bench ? 40 : options.domains == 1 ? 46 : 50;
    if (!deadline_after(options.timeout_ms, s.deadline)) return 1;
    s.frontier = s.deadline - 10000 * NS_PER_MS;
    abi::SelfPolicy original{};
    rlimit limits{};
    s.tid = ker::multiproc::currentThreadId();
    s.original_affinity = ker::multiproc::getThreadAffinityMask(s.tid);
    bool const SAVED = s.tid == static_cast<uint64_t>(getpid()) && policy_read(s.io, original) == 0 && getrlimit(RLIMIT_AS, &limits) == 0;
    bool ok = SAVED && policy_topology(s.io, s.topo, options.domains) == 0 && pin_current_cpu(s.topo, s.selected, s.frontier) == 0 &&
              huge_admit(s);
    std::printf(
        "NUMA_HUGE_BEGIN nonce=%llu pid=%llu tid=%llu domains=%u bench=%u holds=%llu physical_closure=unqualified geometry=unobserved\n",
        static_cast<unsigned long long>(options.nonce), static_cast<unsigned long long>(getpid()), static_cast<unsigned long long>(s.tid),
        options.domains, static_cast<unsigned>(options.bench), static_cast<unsigned long long>(s.remaining));
    if (ok)
        ok = options.bench ? policy_set(2, uint64_t{1} << s.selected.node) == 0 &&
                                 policy_matches(s.io, 2, uint64_t{1} << s.selected.node) && huge_bench(s)
                           : huge_aliases(s);
    if (ok && !options.bench)
        ok = huge_mutations(s) && huge_child_case(s, 0) && huge_child_case(s, 1) && huge_usercopy(s) && huge_child_case(s, 4) &&
             huge_child_case(s, 2) && huge_child_case(s, 3);
    bool restored = false;
    if (SAVED && s.children == 0 && s.own.maps == 0 && s.own.fds == 0 && s.own.complete) {
        restored = policy_set(original.mode, original.nodes) == 0 && policy_matches(s.io, original.mode, original.nodes) &&
                   ker::multiproc::setThreadAffinityMask(s.tid, s.original_affinity) == 0 &&
                   static_cast<uint64_t>(ker::multiproc::getThreadAffinityMask(s.tid)) == s.original_affinity;
        rlimit final{};
        restored = restored && getrlimit(RLIMIT_AS, &final) == 0 && final.rlim_cur == limits.rlim_cur && final.rlim_max == limits.rlim_max;
    }
    ok = ok && s.remaining == 0 && s.children == 0 && restored && s.own.maps == 0 && s.own.fds == 0 && s.own.complete && before(s.deadline);
    std::printf(
        "NUMA_HUGE_RESULT nonce=%llu pass=%u remaining=%llu maps=%u fds=%u children=%u complete=%u restored=%u "
        "geometry=external_witness_required physical_closure=unqualified\n",
        static_cast<unsigned long long>(options.nonce), static_cast<unsigned>(ok), static_cast<unsigned long long>(s.remaining), s.own.maps,
        s.own.fds, s.children, static_cast<unsigned>(s.own.complete), static_cast<unsigned>(restored));
    return ok && before(s.deadline) ? 0 : 1;
}
