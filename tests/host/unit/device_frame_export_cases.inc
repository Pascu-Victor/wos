namespace ef {
namespace uc = ker::mod::sys::usercopy;
namespace fm = ker::mod::mm::file_mapping;
namespace phys = ker::mod::mm::phys;
namespace mapping = ker::dev::mapping;
namespace of = object_fixture;
constexpr uint64_t PAGE = 4096, VA = 0x400000;
unsigned checks{}, cases{};
void check(bool value) {
    ++checks;
    if (!value) {
        std::fprintf(stderr, "device export check %u failed\n", checks);
        std::abort();
    }
}
fm::FileMappingBinding* selected{};
void* selected_page{};
uint64_t selected_physical{}, selected_user = VA;
mapping::Object* returning_object{};
unsigned ordered_returns{};
void observe_return(void* page) {
    check(page == selected_page && returning_object != nullptr && returning_object->owners().exports == 1);
    ++ordered_returns;
}
struct World {
    ker::vfs::File file;
    ker::mod::sched::task::Task task;
    ker::mod::mm::paging::PageTable root{};
    fm::FileMappingIndex index;
    mapping::QuotaAccountRef quota;
    mapping::ObjectRef object;
    fm::FileMappingLifetime* family{};
    fm::FileMappingBinding* binding{};
    void* ordinary{};
    explicit World(bool device = true, bool bound = true) {
        check(mapping::QuotaAccount::create({8, 4, 8}, quota) == 0);
        if (device) {
            check(mapping::Object::create(quota, 4 * PAGE, 3, object) == 0);
            int error = 0;
            family = fm::FileMappingLifetime::prepare_device_shared_wb(file, object, PAGE, 3 * PAGE, 3, error);
            check(family != nullptr && error == 0);
            selected_page = object.get()->cpu_page(PAGE);
            check(object.get()->frame_at(PAGE, selected_physical));
        } else {
            family = fm::FileMappingLifetime::prepare(file);
            check(family != nullptr);
            ordinary = phys::page_alloc_may_fail(ker::mod::mm::PhysicalPageOwner::USER_SHARED_MEMORY, PAGE, "old-frame");
            check(ordinary != nullptr);
            selected_page = ordinary;
            selected_physical = reinterpret_cast<uint64_t>(ordinary);
        }
        if (bound) {
            auto* prepared = fm::FileMappingBinding::prepare(*family, &root, 31, VA, VA + 2 * PAGE, device ? PAGE : 0);
            check(prepared != nullptr);
            binding = prepared;
            snapshot_fixture::IndexGuard guard;
            check(index.attach_locked(prepared, 31) && prepared == nullptr);
        }
        selected = binding;
        selected_user = VA;
    }
    void retire_mapping() {
        if (family == nullptr) return;
        fm::FileMappingTlbRetirement retired;
        {
            snapshot_fixture::IndexGuard guard;
            check(index.detach_root_locked(&root, 31, retired));
        }
        retired.release_after_completed_tlb();  // Host ACK endpoint only.
        std::exchange(family, nullptr)->release(fm::FamilyOwner::PREPARED);
        snapshot_fixture::drain();
        binding = selected = nullptr;
    }
    ~World() {
        check(task.accesses == 0);
        retire_mapping();
        object.reset();
        snapshot_fixture::drain();
        if (ordinary) check(phys::page_ref_dec(ordinary) == 0);
        quota.reset();
        snapshot_fixture::drain();
        ker::vfs::vfs_put_file(&file);
        check(snapshot_fixture::queue_empty() && of::live_pages() == 0);
        check(snapshot_fixture::lock_depth == 0 && snapshot_fixture::index_depth == 0 && of::irq_depth == 0);
    }
};
}  // namespace ef

namespace ker::mod::sys::usercopy {
// Finite incoming-pin port only. All retain/export/move/reset decisions below
// are actual production methods. This does not emulate a PTE or root gate.
auto pin_task_user_page(sched::task::Task& task, uint64_t address, bool, bool, StableUserPage& out) -> bool {
    out.reset();
    if (!mm::phys::page_ref_try_inc(ef::selected_page)) return false;
    if (ef::selected != nullptr && !ef::selected->try_acquire(mm::file_mapping::BindingOwner::PIN)) {
        mm::phys::page_ref_dec(ef::selected_page);
        return false;
    }
    ++task.accesses;
    out.m_task = &task;
    out.m_user_address = address;
    out.m_pin.user_page = ef::selected_user;
    out.m_pin.hhdm_page = ef::selected_page;
    out.m_pin.physical_page = ef::selected_physical;
    out.m_pin.file_binding = ef::selected;
    return true;
}
}  // namespace ker::mod::sys::usercopy
namespace ker::mod::mm::virt {
void unpin_user_page(UserPagePin& pin) {
    auto* page = std::exchange(pin.hhdm_page, nullptr);
    auto* binding = std::exchange(pin.file_binding, nullptr);
    if (page) phys::page_ref_dec(page);
    if (binding) binding->release(file_mapping::BindingOwner::PIN);
    pin = {};
}
}  // namespace ker::mod::mm::virt

namespace ef {
void lifetime_and_order() {
    World world;
    uc::UserPageFrameRef retained;
    {
        uc::StableUserPage pin;
        check(uc::pin_task_user_page(world.task, VA, false, false, pin));
        check(pin.retain_frame(retained) && retained.valid());
        check(world.object.get()->owners().exports == 1 && of::references(selected_page) == 3);
        check(world.binding->owner_counts().count(fm::BindingOwner::PIN) == 1);
    }
    check(world.binding->owner_counts().count(fm::BindingOwner::PIN) == 0 && world.task.accesses == 0);
    world.retire_mapping();
    returning_object = world.object.get();
    world.object.reset();
    check(of::references(selected_page) == 2 && world.quota.get()->usage().resident_pages == 4);
    phys::after_frame_return = observe_return;
    {
        of::IrqScope irq;
        retained.reset();
        check(ordered_returns == 1 && !retained.valid());
        check(of::references(selected_page) == 1 && world.quota.get()->usage().resident_pages == 4);
    }
    phys::after_frame_return = nullptr;
    returning_object = nullptr;
    snapshot_fixture::drain();
    check(of::live_pages() == 0 && world.quota.get()->usage().resident_pages == 0);
    ++cases;
}
void rollback_and_moves() {
    World world;
    uc::StableUserPage pin, invalid;
    uc::UserPageFrameRef first, second;
    check(!invalid.retain_frame(first));
    check(uc::pin_task_user_page(world.task, VA, false, false, pin));
    of::fail_pin = true;
    check(!pin.retain_frame(first) && !first.valid() && world.object.get()->owners().exports == 0);
    of::fail_pin = false;
    of::set_references(selected_page, UINT32_MAX);
    check(!pin.retain_frame(first) && !first.valid() && world.object.get()->owners().exports == 0);
    of::set_references(selected_page, 2);
    auto const saved_owners = world.object.get()->owners_;
    world.object.get()->owners_.exports = UINT64_MAX - saved_owners.total;
    world.object.get()->owners_.total = UINT64_MAX;
    check(!pin.retain_frame(first) && !first.valid() && of::references(selected_page) == 2);
    world.object.get()->owners_ = saved_owners;
    check(pin.retain_frame(first) && pin.retain_frame(second));
    check(!pin.retain_frame(first) && world.object.get()->owners().exports == 2 && of::references(selected_page) == 4);
    uc::UserPageFrameRef moved(std::move(first));
    check(!first.valid() && moved.valid() && world.object.get()->owners().exports == 2);
    second = std::move(moved);
    second = std::move(second);
    check(!moved.valid() && second.valid() && world.object.get()->owners().exports == 1 && of::references(selected_page) == 3);
    second.reset();
    second.reset();
    check(world.object.get()->owners().exports == 0 && of::references(selected_page) == 2);
    ++cases;
}
void nonzero_offset() {
    World world;
    selected_user = VA + PAGE;
    selected_page = world.object.get()->cpu_page(2 * PAGE);
    check(world.object.get()->frame_at(2 * PAGE, selected_physical));
    uc::StableUserPage pin;
    uc::UserPageFrameRef frame;
    check(uc::pin_task_user_page(world.task, VA + PAGE + 17, false, false, pin));
    check(pin.retain_frame(frame) && world.object.get()->owners().exports == 1 && of::references(selected_page) == 3);
    frame.reset();
    ++cases;
}
void malformed_and_identity() {
    World world;
    uc::StableUserPage pin;
    uc::UserPageFrameRef refused;
    check(uc::pin_task_user_page(world.task, VA, false, false, pin));
    auto const generation = world.family->device_generation_;
    world.family->device_generation_ = 0;
    check(!pin.retain_frame(refused));
    world.family->device_generation_ = generation + 1;
    check(!pin.retain_frame(refused));
    world.family->device_generation_ = generation;
    auto object = std::move(world.family->device_object_);
    check(!pin.retain_frame(refused));
    world.family->device_object_ = std::move(object);
    auto const physical = pin.m_pin.physical_page;
    pin.m_pin.physical_page += PAGE;
    check(!pin.retain_frame(refused));
    pin.m_pin.physical_page = physical + 1;
    check(!pin.retain_frame(refused));
    pin.m_pin.physical_page = physical;
    pin.m_pin.user_page = VA - PAGE;
    check(!pin.retain_frame(refused));
    pin.m_pin.user_page = VA + 2 * PAGE;
    check(!pin.retain_frame(refused));
    pin.m_pin.user_page = VA + PAGE;
    auto const offset = world.binding->file_offset_;
    world.binding->file_offset_ = UINT64_MAX - (PAGE - 1);
    check(!pin.retain_frame(refused));
    world.binding->file_offset_ = offset;
    pin.m_pin.user_page = VA;
    auto const end = world.family->device_window_end_;
    world.family->device_window_end_ = world.family->device_window_base_;
    check(!pin.retain_frame(refused));
    world.family->device_window_end_ = end;
    check(!refused.valid() && world.object.get()->owners().exports == 0 && of::references(selected_page) == 2);
    check(pin.retain_frame(refused));
    ++cases;
}
void ordinary_regression(bool bound) {
    World world(false, bound);
    uc::StableUserPage pin;
    uc::UserPageFrameRef frame;
    check(uc::pin_task_user_page(world.task, VA, false, false, pin));
    check(pin.retain_frame(frame) && frame.valid() && !frame.m_device_export && of::references(selected_page) == 3);
    frame.reset();
    check(of::references(selected_page) == 2);
    ++cases;
}
void admission_race() {
    World world;
    uc::StableUserPage pin;
    uc::UserPageFrameRef frame, refused;
    check(uc::pin_task_user_page(world.task, VA, false, false, pin));
    std::atomic<bool> go{};
    bool acquired = false, closed = false;
    std::thread exporter([&] {
        while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
        acquired = pin.retain_frame(frame);
    });
    std::thread revoker([&] {
        while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        closed = world.object.get()->begin_revoke(publication);
    });
    go.store(true, std::memory_order_release);
    exporter.join();
    revoker.join();
    check(closed && world.object.get()->owners().exports == static_cast<unsigned>(acquired));
    check(of::references(selected_page) == 2U + static_cast<unsigned>(acquired));
    check(!pin.retain_frame(refused) && !refused.valid());
    frame.reset();
    check(world.object.get()->owners().exports == 0);
    ++cases;
}
}  // namespace ef
int main() {
    ef::lifetime_and_order();
    ef::rollback_and_moves();
    ef::nonzero_offset();
    ef::malformed_and_identity();
    ef::ordinary_regression(false);
    ef::ordinary_regression(true);
    ef::admission_race();
    std::printf("PASS device_frame_exports cases=%u assertions=%u actual_usercopy=1 actual_object=1 actual_family=1 hardware_root=0\n",
                ef::cases, ef::checks);
}
