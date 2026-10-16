namespace fixture {
namespace actual = ker::syscall::shm;
struct ShmWorld : Actors {
    ShmWorld() {
        actual::g_segments = {};
        actual::g_attachments = {};
        actual::g_segments[0].active = true;
        actual::g_segments[0].id = 17;
        actual::g_segments[0].size = 4096;
    }
    void seed(unsigned n) {
        auto& segment = actual::g_segments[0];
        for (unsigned i = 0; i < n; ++i) {
            actual::g_attachments[i] = {
                .active = true, .publishing = false, .pid = actor.pid, .shmid = 17, .addr = 0x400000 + 4096 * i, .size = 4096};
            ++segment.attach_count;
        }
    }
    unsigned child_count() {
        unsigned n = 0;
        for (auto const& r : actual::g_attachments) n += r.active && r.pid == child.pid;
        return n;
    }
};
void shm_tests() {
    for (unsigned n : {0u, 1u, 5u, 128u}) {
        ++cases;
        ShmWorld w;
        w.seed(n);
        unsigned start = allocations;
        {
            actual::ShmForkPreparation p;
            check(p.prepare(w.actor, w.child, w.scope) == 0, "prepare real fixed SHM table");
            if (!n) check(allocations == start, "empty SHM no heap");
            unsigned before = allocations;
            {
                Pub pub;
                irq = false;
                check(p.matches(w.actor, w.child, w.scope, pub) == 0, "SHM exact prepared rows");
                check(p.commit(w.actor, w.child, w.scope, pub) == 0, "infallible actual SHM adoption");
                check(allocations == before, "SHM commit no allocation");
                check(w.child_count() == n, "all child rows published once");
                check(actual::g_segments[0].attach_count == 2 * n, "true segment attachment count");
                check(p.commit(w.actor, w.child, w.scope, pub) == -EAGAIN, "SHM single use");
                irq = true;
            }
        }
    }
    {
        ++cases;
        ShmWorld w;
        w.seed(129);
        actual::ShmForkPreparation p;
        check(p.prepare(w.actor, w.child, w.scope) == -ENOSPC, "fixed table shortage exact errno");
        check(w.child_count() == 0 && actual::g_segments[0].attach_count == 129, "no partial adoption on shortage");
    }
    for (int f = 0; f < 2; ++f) {
        ++cases;
        ShmWorld w;
        w.seed(3);
        actual::ShmForkPreparation p;
        fail_after = f;
        check(p.prepare(w.actor, w.child, w.scope) == -ENOMEM, "real SHM plan allocation failure");
        fail_after = -1;
        check(w.child_count() == 0, "failed plan has no publishing reservation");
    }
    for (unsigned mutation = 0; mutation < 10; ++mutation) {
        ++cases;
        ShmWorld w;
        w.seed(3);
        actual::ShmForkPreparation p;
        check(p.prepare(w.actor, w.child, w.scope) == 0, "prepare SHM changing snapshot");
        switch (mutation) {
            case 0:
                actual::g_attachments[0].publishing = true;
                break;
            case 1:
                actual::g_attachments[0].addr += 4096;
                break;
            case 2:
                actual::g_attachments[0].size = UINT64_MAX;
                break;
            case 3:
                actual::g_attachments[3] = {.active = false, .publishing = true, .pid = w.child.pid};
                break;
            case 4:
                actual::g_segments[0].active = false;
                break;
            case 5:
                actual::g_segments[0].attach_count = UINT64_MAX - 1;
                break;
            case 6:
                actual::g_segments[0].attach_count = 1;
                break;
            case 7:
                ++w.childroot.serial;
                break;
            case 8:
                w.child.has_run = true;
                break;
            case 9:
                actual::g_attachments[1].shmid = 19;
                break;
        }
        {
            Pub pub;
            irq = false;
            int result = p.commit(w.actor, w.child, w.scope, pub);
            check(result == (mutation == 5 || mutation == 6 ? -EOVERFLOW : -EAGAIN), "SHM full prevalidation refuses before transfer");
            irq = true;
        }
        check(w.child_count() == 0, "all refusal leaves actual child without rows");
    }
    {
        ++cases;
        ShmWorld w;
        w.seed(2);
        actual::g_segments[0].marked_destroy = true;
        actual::ShmForkPreparation p;
        check(p.prepare(w.actor, w.child, w.scope) == 0, "retained marked-destroy capability inherits");
        {
            Pub pub;
            check(p.commit(w.actor, w.child, w.scope, pub) == 0, "destroy flag not namespace/name re-admission");
        }
        check(w.child_count() == 2, "marked destroy retained attachment rows");
    }
    {
        ++cases;
        ShmWorld w;
        w.seed(2);
        Task thread;
        thread.pagemap = &w.source;
        thread.pid = 99;
        thread.is_thread = true;
        thread.owner = &w.actor;
        ker::mod::sched::current = &thread;
        actual::ShmForkPreparation p;
        check(p.prepare(thread, w.child, w.scope) == 0, "initiating thread uses actual canonical physical process PID");
        {
            Pub pub;
            check(p.commit(thread, w.child, w.scope, pub) == 0, "canonical process SHM rows copied");
        }
        check(w.child_count() == 2, "thread numerical TID is not attachment owner");
    }
    std::printf("PASS shm cases=%u checks=%u\n", cases, checks);
}
}  // namespace fixture
int main() { fixture::shm_tests(); }
