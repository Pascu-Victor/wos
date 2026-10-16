namespace activity_fixture {
void resident_test() {
    nl::MmDebtSnapshot a{}, b{};
    nl::MmActivitySnapshot aa{}, ab{};
    nl::MmActivityRelation r{};
    debt::snapshot(a);
    debt::activity_snapshot(aa);
    fixture::virt::PageTable root;
    fixture::task::Task actor;
    actor.pagemap = &root;
    {
        fixture::uc::StableUserPage pin;
        CHECK(fixture::uc::pin_task_user_page(actor, 0x4013, true, false, pin));
        CHECK(pin.valid());
    }
    fixture::clean();
    debt::snapshot(b);
    debt::activity_snapshot(ab);
    debt::interpret_activity(a, b, aa, ab, r);
    CHECK(r.reconciled && r.mm_delta == 36 && r.accounted_delta == 36 && r.begin_delta == 6 && r.end_delta == 6 && r.adjust_calls == 12);
    for (size_t i = 0; i < 23; ++i) {
        uint64_t expected = i == 6 ? 2 : (i == 0 || i == 2 || i == 3 || i == 5 ? 1 : 0);
        CHECK(ab.add_attempts[i] - aa.add_attempts[i] == expected);
        CHECK(ab.drop_attempts[i] - aa.drop_attempts[i] == expected);
    }
    std::printf("PASS actual resident Task/root/pin/StableUserPage36 epoch increments, endpoint seams only; not producer attribution\n");
}
}  // namespace activity_fixture
void mm_activity_epoch_ended() noexcept {}
