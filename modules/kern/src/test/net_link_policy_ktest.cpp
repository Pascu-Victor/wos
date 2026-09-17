#include <array>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <syscalls_impl/net/sys_net.hpp>
#include <test/ktest.hpp>

KTEST(NetLinkPolicy, RenameAdmissionPrecedesFieldEffectsAndDriverCallbacks) {
    KEXPECT_TRUE(ker::syscall::net::net_selftest_link_rename_admission_precedes_effects());
}

KTEST(NetLinkPolicy, DriverOpenErrorsPreserveAllRequestedFields) {
    KEXPECT_TRUE(ker::syscall::net::net_selftest_open_error_preserves_link_request());
}

KTEST(NetIPv4Config, OwnedCopiesPreservePrimaryUpdatesDenialsAndRetirement) {
    namespace net = ker::net;
    net::NetDevice dev{};
    dev.name = {'k', 't', 'i', 'p', 'v', '4'};
    int const REGISTERED = net::netdev_register(&dev);
    KEXPECT_EQ(REGISTERED, 0);
    if (REGISTERED != 0) return;
    KEXPECT_TRUE(net::netif_ipv4_snapshot(nullptr) == net::IPv4ConfigSnapshot{});
    KEXPECT_EQ(net::netif_set_primary_ipv4_netmask(&dev, 0xffff0000), -EADDRNOTAVAIL);
    KEXPECT_EQ(net::netif_ipv4_snapshot(&dev).count, 0U);
    KEXPECT_EQ(net::netif_set_primary_ipv4_address(&dev, 0xc6120001), 0);
    auto const INITIAL = net::netif_ipv4_snapshot(&dev);
    KEXPECT_EQ(INITIAL.count, 1U);
    KEXPECT_EQ(INITIAL.entries.front().addr.to_host_order(), 0xc6120001U);
    KEXPECT_EQ(INITIAL.entries.front().netmask.to_host_order(), 0xffffff00U);
    KEXPECT_EQ(net::netif_set_primary_ipv4_netmask(&dev, 0xffff0000), 0);
    KEXPECT_EQ(net::netif_set_primary_ipv4_address(&dev, 0xc6120002), 0);
    auto const UPDATED = net::netif_ipv4_snapshot(&dev);
    KEXPECT_EQ(UPDATED.entries.front().addr.to_host_order(), 0xc6120002U);
    KEXPECT_EQ(UPDATED.entries.front().netmask.to_host_order(), 0xffff0000U);
    KEXPECT_EQ(INITIAL.entries.front().addr.to_host_order(), 0xc6120001U);
    KEXPECT_EQ(INITIAL.entries.front().netmask.to_host_order(), 0xffffff00U);
    KEXPECT_EQ(net::netif_set_ipv4(&dev, 0xc6120002, 0xffff0000, false), -EEXIST);
    KEXPECT_TRUE(net::netif_ipv4_snapshot(&dev) == UPDATED);
    KEXPECT_EQ(net::netif_del_ipv4(&dev, 0xc6120002, 0xffffff00), -EADDRNOTAVAIL);
    KEXPECT_TRUE(net::netif_ipv4_snapshot(&dev) == UPDATED);
    KEXPECT_EQ(net::netif_set_ipv4(&dev, 0xc6120002, 0xffffff00, false), 0);
    for (uint32_t i = 1; i < net::MAX_ADDRS_PER_IF; ++i) KEXPECT_EQ(net::netif_add_ipv4(&dev, 0xc6120100 + i, 0xffffffff), 0);
    auto const FULL = net::netif_ipv4_snapshot(&dev);
    KEXPECT_EQ(FULL.count, net::MAX_ADDRS_PER_IF);
    KEXPECT_EQ(net::netif_add_ipv4(&dev, 0xc6120200, 0xffffffff), -1);
    KEXPECT_TRUE(net::netif_ipv4_snapshot(&dev) == FULL);
    KEXPECT_EQ(net::netif_set_ipv4(&dev, 0xc6120200, 0xffffffff, true), -1);
    KEXPECT_TRUE(net::netif_ipv4_snapshot(&dev) == FULL);
    KEXPECT_EQ(net::netif_del_ipv4(&dev, 0xc6120002, 0xffffff00), 0);
    auto const REMOVED = net::netif_ipv4_snapshot(&dev);
    KEXPECT_EQ(REMOVED.count, net::MAX_ADDRS_PER_IF - 1);
    KEXPECT_TRUE(REMOVED.entries.front() == FULL.entries.at(1));
    KEXPECT_TRUE(REMOVED.entries.back() == net::IPv4Addr{});
    KEXPECT_TRUE(net::netif_find_by_ipv4(0xc6120101) != nullptr);
    KEXPECT_EQ(net::netdev_unregister(&dev), 0);
    KEXPECT_TRUE(net::netif_ipv4_snapshot(&dev) == net::IPv4ConfigSnapshot{});
    KEXPECT_TRUE(net::netif_find_by_ipv4(0xc6120101) == nullptr);
    KEXPECT_EQ(FULL.count, net::MAX_ADDRS_PER_IF);
    KEXPECT_EQ(net::netif_set_primary_ipv4_address(&dev, 0xc6120003), -ENOMEM);
}

KTEST(NetLinkPolicy, NameReservationIsInvisibleAndAbortReleasesItsClaim) {
    namespace net = ker::net;
    net::NetDevice first{}, second{}, duplicate{};
    first.name = {'k', 't', 'c', 'l', 'a', 'i', 'm', 'a'};
    second.name = {'k', 't', 'c', 'l', 'a', 'i', 'm', 'b'};
    int const FIRST = net::netdev_register(&first);
    KEXPECT_EQ(FIRST, 0);
    if (FIRST != 0) return;
    int const SECOND = net::netdev_register(&second);
    KEXPECT_EQ(SECOND, 0);
    if (SECOND != 0) {
        static_cast<void>(net::netdev_unregister(&first));
        return;
    }
    auto a = net::netdev_retain_registered(&first);
    auto b = net::netdev_retain_registered(&second);
    auto const OLD = first.name;
    {
        net::NetDeviceNameChange change, competing;
        KEXPECT_EQ(change.prepare(a.identity(), "ktclaimnew"), 0);
        KEXPECT_TRUE(first.name == OLD);
        KEXPECT_FALSE(static_cast<bool>(net::netdev_find_by_name_ref("ktclaimnew")));
        KEXPECT_TRUE(net::netdev_find_by_name_ref(OLD.data()).identity() == a.identity());
        KEXPECT_EQ(net::netdev_rename(b.identity(), "ktclaimnew"), -EEXIST);
        KEXPECT_EQ(competing.prepare(a.identity(), "ktclaimother"), -EBUSY);
        KEXPECT_EQ(change.prepare(a.identity(), "ktclaimother"), -EBUSY);
        std::strcpy(duplicate.name.data(), "ktclaimnew");
        int const DUPLICATE = net::netdev_register(&duplicate);
        KEXPECT_EQ(DUPLICATE, -EEXIST);
        if (DUPLICATE == 0) static_cast<void>(net::netdev_unregister(&duplicate));
    }
    KEXPECT_TRUE(first.name == OLD);
    KEXPECT_EQ(net::netdev_rename(b.identity(), "ktclaimnew"), 0);
    {
        net::NetDeviceNameChange change;
        KEXPECT_EQ(change.prepare(a.identity(), "ktclaimother"), 0);
        change.commit();
        change.commit();
        change.cancel();
        KEXPECT_TRUE(net::netdev_find_by_name_ref("ktclaimother").identity() == a.identity());
    }
    a.reset();
    b.reset();
    KEXPECT_EQ(net::netdev_unregister(&second), 0);
    KEXPECT_EQ(net::netdev_unregister(&first), 0);
}

KTEST(NetLinkPolicy, RetiredClaimsRemainBoundedAndCannotRepublishDevices) {
    namespace net = ker::net;
    std::array<net::NetDevice, net::MAX_NET_DEVICES> devices{};
    std::array<net::NetDeviceNameChange, net::MAX_NET_DEVICES> claims{};
    std::array<net::NetDeviceRetireToken, net::MAX_NET_DEVICES> tokens{};
    size_t count = 0;
    for (; count < devices.size(); ++count) {
        auto& dev = devices.at(count);
        dev.name = {'k', 't', 'p', 'o', 'o', 'l'};
        static_cast<void>(std::to_chars(dev.name.data() + 6, dev.name.data() + dev.name.size() - 1, count));
        int const REGISTER = net::netdev_register(&dev);
        KEXPECT_EQ(REGISTER, 0);
        if (REGISTER != 0) break;
        auto retained = net::netdev_retain_registered(&dev);
        auto name = dev.name;
        name.front() = 'r';
        int const PREPARE = claims.at(count).prepare(retained.identity(), name.data());
        KEXPECT_EQ(PREPARE, 0);
        int const RETIRE = net::netdev_unregister_begin(&dev, tokens.at(count));
        KEXPECT_EQ(RETIRE, 0);
        retained.reset();
        if (PREPARE != 0 || RETIRE != 0) {
            claims.at(count).cancel();
            if (RETIRE == 0)
                net::netdev_unregister_wait(tokens.at(count));
            else
                static_cast<void>(net::netdev_unregister(&dev));
            break;
        }
    }
    KEXPECT_EQ(count, devices.size());
    net::NetDevice extra{};
    extra.name = {'k', 't', 'e', 'x', 't', 'r', 'a'};
    int const EXTRA = net::netdev_register(&extra);
    KEXPECT_EQ(EXTRA, 0);
    if (EXTRA == 0) {
        auto retained = net::netdev_retain_registered(&extra);
        net::NetDeviceNameChange change;
        if (count == devices.size()) KEXPECT_EQ(change.prepare(retained.identity(), "ktfull"), -EBUSY);
        if (count > 0) {
            KEXPECT_EQ(net::netdev_rename(retained.identity(), "rtpool0"), -EEXIST);
            auto const OLD = devices.front().name;
            claims.front().commit();
            KEXPECT_TRUE(devices.front().name == OLD);
            KEXPECT_FALSE(static_cast<bool>(net::netdev_find_by_name_ref("rtpool0")));
            KEXPECT_EQ(change.prepare(retained.identity(), "rtpool0"), 0);
            change.cancel();
        }
        retained.reset();
        KEXPECT_EQ(net::netdev_unregister(&extra), 0);
    }
    for (size_t i = 0; i < count; ++i) {
        claims.at(i).cancel();
        net::netdev_unregister_wait(tokens.at(i));
    }
}

KTEST(NetLinkPolicy, RegistryNamesAreUniqueAndRetiredIdentitiesCannotRename) {
    namespace net = ker::net;
    net::NetDevice first{}, occupied{}, duplicate{}, automatic{};
    int const REGISTERED = net::netdev_register(&first);
    KEXPECT_EQ(REGISTERED, 0);
    if (REGISTERED != 0) return;
    auto const ORIGINAL = first.name;
    uint32_t index = 0;
    auto const PARSED = std::from_chars(ORIGINAL.data() + 3, ORIGINAL.data() + std::strlen(ORIGINAL.data()), index);
    KEXPECT_TRUE(PARSED.ec == std::errc{});
    occupied.name = {'e', 't', 'h'};
    static_cast<void>(std::to_chars(occupied.name.data() + 3, occupied.name.data() + occupied.name.size() - 1, index + 1U));
    int const OCCUPIED = net::netdev_register(&occupied);
    KEXPECT_EQ(OCCUPIED, 0);
    if (OCCUPIED != 0) {
        static_cast<void>(net::netdev_unregister(&first));
        return;
    }
    duplicate.name = ORIGINAL;
    auto const BEFORE_READERS = duplicate.lifetime_readers.load();
    int const DUPLICATE = net::netdev_register(&duplicate);
    KEXPECT_EQ(DUPLICATE, -EEXIST);
    KEXPECT_EQ(duplicate.ifindex, 0U);
    KEXPECT_EQ(duplicate.lifetime_generation.load(), 0U);
    KEXPECT_EQ(duplicate.lifetime_readers.load(), BEFORE_READERS);
    KEXPECT_TRUE(duplicate.name == ORIGINAL);
    if (DUPLICATE == 0) static_cast<void>(net::netdev_unregister(&duplicate));
    std::array<char, net::NETDEV_NAME_LEN> claimed_name{'e', 't', 'h'};
    static_cast<void>(std::to_chars(claimed_name.data() + 3, claimed_name.data() + claimed_name.size() - 1, index + 2U));
    net::NetDeviceNameChange reservation;
    KEXPECT_EQ(reservation.prepare(net::netdev_registered_identity(&first), claimed_name.data()), 0);
    int const AUTOMATIC = net::netdev_register(&automatic);
    KEXPECT_EQ(AUTOMATIC, 0);
    KEXPECT_TRUE(automatic.name != ORIGINAL && automatic.name != occupied.name && automatic.name != claimed_name);
    reservation.cancel();
    auto retained = net::netdev_retain_registered(&first);
    auto const IDENTITY = retained.identity();
    KEXPECT_EQ(net::netdev_rename(IDENTITY, occupied.name.data()), -EEXIST);
    KEXPECT_EQ(net::netdev_rename(IDENTITY, {}), -EINVAL);
    KEXPECT_EQ(net::netdev_rename(IDENTITY, std::string_view("bad\0name", 8)), -EINVAL);
    KEXPECT_TRUE(first.name == ORIGINAL);
    KEXPECT_EQ(net::netdev_rename(IDENTITY, ORIGINAL.data()), 0);
    KEXPECT_EQ(net::netdev_rename(IDENTITY, "kt-renamed"), 0);
    {
        auto by_name = net::netdev_find_by_name_ref("kt-renamed");
        KEXPECT_TRUE(by_name.identity() == IDENTITY);
        KEXPECT_FALSE(static_cast<bool>(net::netdev_find_by_name_ref(ORIGINAL.data())));
    }
    net::NetDeviceRetireToken token{};
    int const RETIRED = net::netdev_unregister_begin(&first, token);
    KEXPECT_EQ(RETIRED, 0);
    KEXPECT_EQ(net::netdev_rename(IDENTITY, "kt-too-late"), -ENODEV);
    KEXPECT_TRUE(std::strcmp(first.name.data(), "kt-renamed") == 0);
    retained.reset();
    if (RETIRED == 0)
        net::netdev_unregister_wait(token);
    else
        static_cast<void>(net::netdev_unregister(&first));
    if (AUTOMATIC == 0) KEXPECT_EQ(net::netdev_unregister(&automatic), 0);
    KEXPECT_EQ(net::netdev_unregister(&occupied), 0);
}

KTEST(NetLinkPolicy, ExplicitOpenReceivesOriginalSnapshotAndPreservesDeniedFields) {
    KEXPECT_TRUE(ker::syscall::net::net_selftest_link_open_uses_admitted_snapshot());
}

KTEST(NetLinkPolicy, ExplicitClosePreservesSnapshotAndAllFieldsUntilSuccess) {
    KEXPECT_TRUE(ker::syscall::net::net_selftest_link_close_uses_admitted_snapshot());
}

KTEST(NetLinkPolicy, CombinedMappedAndNativeMacDenialsPreserveEveryField) {
    KEXPECT_TRUE(ker::syscall::net::net_selftest_link_combined_admission_precedes_all_effects());
}

KTEST(NetLinkPolicy, ControlWriterOverlapCannotPublishAndAuthorizationPrecedesBusy) {
    KEXPECT_TRUE(ker::syscall::net::net_selftest_link_writer_admission());
}

KTEST(NetLinkPolicy, ControlGuardIsPerDeviceAndFailedAcquisitionCannotReleaseOwner) {
    namespace net = ker::net;
    net::NetDevice first{}, second{};
    {
        net::NetDeviceControlGuard owner;
        KEXPECT_EQ(owner.try_acquire(&first), 0);
        KEXPECT_EQ(owner.try_acquire(&second), -EBUSY);
        {
            net::NetDeviceControlGuard contender, independent;
            KEXPECT_EQ(contender.try_acquire(&first), -EBUSY);
            KEXPECT_EQ(independent.try_acquire(&second), 0);
        }
        KEXPECT_TRUE(first.control_busy.load(std::memory_order_acquire));
        KEXPECT_FALSE(second.control_busy.load(std::memory_order_acquire));
        net::NetDeviceControlGuard retry;
        KEXPECT_EQ(retry.try_acquire(&first), -EBUSY);
    }
    net::NetDeviceControlGuard successor;
    KEXPECT_EQ(successor.try_acquire(nullptr), -ENODEV);
    KEXPECT_EQ(successor.try_acquire(&first), 0);
}

KTEST(NetLinkPolicy, ConfigurationSnapshotsHidePartialCommitAndRetirementWins) {
    namespace net = ker::net;
    net::NetDevice dev{};
    dev.name = {'k', 't', '-', 'c', 'o', 'n', 'f', 'i', 'g'};
    int const REGISTERED = net::netdev_register(&dev);
    KEXPECT_EQ(REGISTERED, 0);
    if (REGISTERED != 0) return;
    auto retained = net::netdev_retain_registered(&dev);
    auto const BEFORE = net::netdev_config_snapshot(&dev);
    {
        net::NetDeviceControlGuard control;
        KEXPECT_EQ(control.try_acquire(&dev), 0);
        dev.mtu = 1400;
        KEXPECT_TRUE(net::netdev_config_snapshot(&dev) == BEFORE);
        dev.mac.bytes = {2, 0, 0, 0, 0, 37};
        dev.tx_queue_len = 27;
        dev.state = 1;
        dev.link_flags |= 0x100;
        net::NetDeviceNameChange name;
        KEXPECT_EQ(name.prepare(retained.identity(), "kt-config-new"), 0);
        name.commit();
        KEXPECT_TRUE(net::netdev_config_snapshot(&dev) == BEFORE);
        std::array<net::NetDeviceSnapshot, net::MAX_NET_DEVICES> rows{};
        auto const COUNT = net::netdev_snapshot(rows.data(), rows.size());
        bool found = false;
        for (size_t i = 0; i < COUNT; ++i) {
            auto const& row = rows.at(i);
            if (row.ifindex != BEFORE.ifindex) continue;
            found = true;
            KEXPECT_TRUE(row.name == BEFORE.name && row.mac == BEFORE.mac && row.mtu == BEFORE.mtu &&
                         row.tx_queue_len == BEFORE.tx_queue_len && row.link_flags == BEFORE.link_flags && row.state == BEFORE.state);
        }
        KEXPECT_TRUE(found);
    }
    auto const AFTER = net::netdev_config_snapshot(&dev);
    KEXPECT_TRUE(AFTER.mtu == 1400 && AFTER.tx_queue_len == 27 && AFTER.mac[5] == 37 && AFTER.state == 1 &&
                 AFTER.link_flags == (BEFORE.link_flags | 0x100U) && std::strcmp(AFTER.name.data(), "kt-config-new") == 0);
    net::NetDeviceRetireToken token{};
    {
        net::NetDeviceControlGuard control;
        KEXPECT_EQ(control.try_acquire(&dev), 0);
        KEXPECT_EQ(net::netdev_unregister_begin(&dev, token), 0);
        auto const RETIRED = net::netdev_config_snapshot(&dev);
        KEXPECT_TRUE(RETIRED.state == 0 && RETIRED.mtu == AFTER.mtu && RETIRED.mac == AFTER.mac);
        dev.state = 1;  // Late native OPEN completion cannot restore visible UP.
        dev.mtu = 1300;
        KEXPECT_TRUE(net::netdev_config_snapshot(&dev) == RETIRED);
        net::NetDeviceControlGuard contender;
        KEXPECT_EQ(contender.try_acquire(&dev), -ENODEV);
    }
    KEXPECT_EQ(dev.state, 0);
    KEXPECT_EQ(net::netdev_config_snapshot(&dev).state, 0);
    auto const OLD_IDENTITY = retained.identity();
    retained.reset();
    net::netdev_unregister_wait(token);
    int const REUSED = net::netdev_register(&dev);
    KEXPECT_EQ(REUSED, 0);
    if (REUSED == 0) {
        {
            auto current = net::netdev_retain_registered(&dev);
            KEXPECT_TRUE(current.identity() != OLD_IDENTITY);
            net::NetDeviceControlGuard control;
            KEXPECT_EQ(control.try_acquire(&dev), 0);
        }
        KEXPECT_EQ(net::netdev_unregister(&dev), 0);
    }
    KEXPECT_TRUE(net::netdev_config_snapshot(nullptr) == net::NetDeviceConfigSnapshot{});
}

KTEST(NetLinkPolicy, ControlCompletionPreservesRequestedNotificationKinds) {
    namespace net = ker::net;
    net::NetDevice dev{};
    net::NetDeviceControlGuard control;
    KEXPECT_FALSE(net::netdev_defer_control_notify(&dev, net::NetDeviceControlNotify::OWNER_STATE));
    KEXPECT_EQ(control.try_acquire(&dev), 0);
    KEXPECT_TRUE(net::netdev_defer_control_notify(&dev, net::NetDeviceControlNotify::OWNER_STATE));
    KEXPECT_TRUE(net::netdev_defer_control_notify(&dev, net::NetDeviceControlNotify::OWNER_STATE));
    KEXPECT_EQ(dev.control_notifications, static_cast<uint8_t>(net::NetDeviceControlNotify::OWNER_STATE));
    control.finish();
    KEXPECT_EQ(dev.control_notifications, 0);
    KEXPECT_FALSE(dev.control_busy.load(std::memory_order_acquire));
    control.finish();
    KEXPECT_EQ(control.try_acquire(&dev), 0);
    KEXPECT_TRUE(net::netdev_defer_control_notify(&dev, net::NetDeviceControlNotify::RESOURCE_ADVERT));
    KEXPECT_EQ(dev.control_notifications, static_cast<uint8_t>(net::NetDeviceControlNotify::RESOURCE_ADVERT));
    control.finish();
    KEXPECT_EQ(dev.control_notifications, 0);
}
