namespace activity_fixture {
using nl::MmActivityRelation;
using nl::MmActivitySnapshot;
using nl::MmDebtSnapshot;
static MmDebtSnapshot before_mm{};
static MmActivitySnapshot before_activity{};
static MmActivityRelation end_relation{};
static bool observe_end{}, observed_end{};
void sample(MmDebtSnapshot& mm, MmActivitySnapshot& activity) {
    debt::snapshot(mm);
    debt::activity_snapshot(activity);
}
void check_delta(const MmDebtSnapshot& a, const MmDebtSnapshot& b, const MmActivitySnapshot& aa, const MmActivitySnapshot& ab,
                 uint64_t windows, uint64_t calls) {
    MmActivityRelation r{};
    debt::interpret_activity(a, b, aa, ab, r);
    CHECK(r.reconciled == 1);
    CHECK(r.begin_delta == windows && r.end_delta == windows);
    CHECK(r.adjust_calls == calls && r.mm_delta == 2 * windows + 2 * calls && r.accounted_delta == r.mm_delta);
}
void scalar_tests() {
    static_assert(sizeof(MmDebtSnapshot) == 216 && sizeof(debt::CounterState) == 216);
    static_assert(sizeof(MmActivitySnapshot) == 448 && sizeof(MmActivityRelation) == 48 && sizeof(debt::ActivityState) == 400);
    MmDebtSnapshot a{}, b{};
    MmActivitySnapshot aa{}, ab{};
    MmActivityRelation r{};
    sample(a, aa);
    sample(b, ab);
    check_delta(a, b, aa, ab, 0, 0);
    // Genuine zero-net ABA on actual hooks; old quantities alone look identical.
    sample(a, aa);
    debt::add(DebtField::USER_PAGE_PINS);
    debt::drop(DebtField::USER_PAGE_PINS);
    sample(b, ab);
    CHECK(a.user_page_pins == b.user_page_pins && b.version_begin == a.version_begin + 4);
    check_delta(a, b, aa, ab, 0, 2);
    CHECK(ab.add_attempts[2] == aa.add_attempts[2] + 1 && ab.drop_attempts[2] == aa.drop_attempts[2] + 1);
    sample(a, aa);
    debt::begin_change();
    debt::end_change();
    sample(b, ab);
    check_delta(a, b, aa, ab, 1, 0);
    sample(a, aa);
    debt::begin_change();
    debt::add(DebtField::PIN_RETURNS);
    debt::drop(DebtField::PIN_RETURNS);
    debt::end_change();
    sample(b, ab);
    check_delta(a, b, aa, ab, 1, 2);
    sample(a, aa);
    for (size_t i = 0; i < 23; ++i) {
        debt::add(static_cast<DebtField>(i));
        debt::drop(static_cast<DebtField>(i));
    }
    sample(b, ab);
    check_delta(a, b, aa, ab, 0, 46);
    for (size_t i = 0; i < 23; ++i) CHECK(ab.add_attempts[i] == aa.add_attempts[i] + 1 && ab.drop_attempts[i] == aa.drop_attempts[i] + 1);
    // At the actual Epoch::end return seam every attempt is already visible.
    sample(before_mm, before_activity);
    observe_end = true;
    observed_end = false;
    debt::add(DebtField::USER_PAGE_PINS);
    observe_end = false;
    CHECK(observed_end && end_relation.reconciled == 1 && end_relation.adjust_calls == 1 && end_relation.mm_delta == 2);
    debt::drop(DebtField::USER_PAGE_PINS);
    // Open windows, unmatched stamps and missing observations stay UNKNOWN.
    sample(a, aa);
    debt::begin_change();
    sample(b, ab);
    debt::interpret_activity(a, b, aa, ab, r);
    CHECK(!r.reconciled);
    debt::end_change();
    sample(a, aa);
    debt::add(DebtField::PIN_RETURNS);
    debt::drop(DebtField::PIN_RETURNS);
    sample(b, ab);
    auto good = ab;
    ab.before.version--;
    debt::interpret_activity(a, b, aa, ab, r);
    CHECK(!r.reconciled);
    ab = good;
    ab.after.version++;
    debt::interpret_activity(a, b, aa, ab, r);
    CHECK(!r.reconciled);
    ab = good;
    ab.valid = 0;
    debt::interpret_activity(a, b, aa, ab, r);
    CHECK(!r.reconciled);
    ab = good;
    ab.before.inflight = 1;
    debt::interpret_activity(a, b, aa, ab, r);
    CHECK(!r.reconciled);
    ab = good;
    ab.after.valid = false;
    debt::interpret_activity(a, b, aa, ab, r);
    CHECK(!r.reconciled);
    ab = good;
    ab.drop_attempts[6]--;
    debt::interpret_activity(a, b, aa, ab, r);
    CHECK(!r.reconciled);
    ab = good;
    ab.explicit_begins = aa.explicit_begins - 1;
    debt::interpret_activity(a, b, aa, ab, r);
    CHECK(!r.reconciled);
    ab = good;
    ab.invalid_selectors = UINT64_MAX;
    debt::interpret_activity(a, b, aa, ab, r);
    CHECK(!r.reconciled);
    ab = good;
    ab.add_attempts[0] = UINT64_MAX - 1;
    ab.drop_attempts[0] = UINT64_MAX - 1;
    debt::interpret_activity(a, b, aa, ab, r);
    CHECK(!r.reconciled);
    ab = good;
    b.version_begin = UINT64_MAX;
    b.version_end = UINT64_MAX;
    ab.before.version = UINT64_MAX;
    ab.after.version = UINT64_MAX;
    debt::interpret_activity(a, b, aa, ab, r);
    CHECK(!r.reconciled);
    // Invalid selectors are attempted inside the actual failing local MM epoch.
    debt::CounterState bad{};
    MmActivitySnapshot bad_activity{};
    auto invalid_before = debt::activity.invalid_selectors.load();
    debt::adjust(bad, static_cast<DebtField>(999), true);
    debt::collect(bad, b);
    debt::collect_activity(bad, bad_activity);
    CHECK(!b.valid && debt::activity.invalid_selectors.load() == invalid_before + 1 && b.version_begin == 3);
    debt::interpret_activity(a, b, aa, bad_activity, r);
    CHECK(!r.reconciled);
    // Host-only finite saturation seeds; no production reset interface.
    std::atomic<uint64_t> boundary{UINT64_MAX - 2};
    debt::increment_activity(boundary);
    CHECK(boundary.load() == UINT64_MAX - 1 && !debt::activity.invalid.load());
    debt::increment_activity(boundary);
    CHECK(boundary.load() == UINT64_MAX && debt::activity.invalid.load());
    debt::activity_snapshot(ab);
    CHECK(!ab.valid);
    debt::increment_activity(boundary);
    CHECK(boundary.load() == UINT64_MAX && debt::activity.invalid.load());
    std::printf("PASS actual MM activity core ABA/explicit/internal/anchors/overflow/end-boundary; state400 outputs984 total1384\n");
}
}  // namespace activity_fixture
void mm_activity_epoch_ended() noexcept {
    if (!activity_fixture::observe_end) return;
    nl::MmDebtSnapshot mm{};
    nl::MmActivitySnapshot activity{};
    debt::snapshot(mm);
    debt::activity_snapshot(activity);
    if (mm.inflight == 0) {
        activity_fixture::observed_end = true;
        debt::interpret_activity(activity_fixture::before_mm, mm, activity_fixture::before_activity, activity,
                                 activity_fixture::end_relation);
    }
}
