#include <gtest/gtest.h>

#include <atomic>
#include <platform/mm/paging.hpp>
#include <platform/security/authorization.hpp>
#include <platform/security/credential_record.hpp>
#include <platform/security/credential_store.hpp>
#include <platform/security/debug_state.hpp>
#include <platform/security/exec_policy.hpp>
#include <platform/security/file_policy.hpp>
#include <thread>
#include <vector>
#include <vfs/timestamps.hpp>

namespace security = ker::mod::security;
using security::Authority;
using security::Change;
using security::Ids;
using security::Request;
using security::Snapshot;

TEST(Credentials, DefaultIsUnprivilegedAndUnknownAuthorityDenies) {
    Snapshot const untrusted;
    Snapshot const root = Snapshot::bootstrap_root();
    EXPECT_NE(untrusted.user().effective, 0);
    for (unsigned bit = 0; bit < 256; ++bit) {
        auto const authority = static_cast<Authority>(bit);
        EXPECT_FALSE(untrusted.allows(authority));
        EXPECT_EQ(root.allows(authority), bit < static_cast<unsigned>(Authority::COUNT));
    }
}

TEST(Credentials, HostnameRequiresItsOwnAuthorityAndCannotRegainAfterDrop) {
    Snapshot root = Snapshot::bootstrap_root();
    EXPECT_EQ(security::authorize_hostname_change(root), 0);
    EXPECT_EQ(security::authorize_hostname_change(Snapshot{}), -EPERM);
    auto service = root;
    ASSERT_EQ(service.prepare({.change = Change::NO_NEW_PRIVILEGES}, service), 0);
    ASSERT_EQ(service.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 2}, service), 0);
    ASSERT_EQ(service.prepare({.change = Change::SET_UID, .real = 102}, service), 0);
    EXPECT_EQ(security::authorize_network_administration(service), 0);
    EXPECT_EQ(security::authorize_hostname_change(service), -EPERM);
    uint64_t const HOSTNAME = uint64_t{1} << static_cast<unsigned>(Authority::HOSTNAME);
    auto hostname_only = root;
    ASSERT_EQ(hostname_only.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = HOSTNAME}, hostname_only), 0);
    EXPECT_EQ(security::authorize_hostname_change(hostname_only), 0);
    EXPECT_EQ(security::authorize_network_administration(hostname_only), -EPERM);
    ASSERT_EQ(root.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = security::ALL_AUTHORITY & ~HOSTNAME}, root), 0);
    EXPECT_EQ(security::authorize_hostname_change(root), -EPERM);
    EXPECT_EQ(root.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = security::ALL_AUTHORITY}, root), -EPERM);
    EXPECT_EQ(security::authorize_hostname_change(root), -EPERM);
}

TEST(Credentials, RootDropChangesAllUserIdsAndCannotRegain) {
    Snapshot const root = Snapshot::bootstrap_root();
    Snapshot user;
    ASSERT_EQ(root.prepare({.change = Change::SET_UID, .real = 1000}, user), 0);
    EXPECT_EQ(user.user(), (Ids{1000, 1000, 1000}));
    EXPECT_EQ(user.group(), root.group());
    EXPECT_FALSE(user.allows(Authority::SET_UID));
    Snapshot rejected;
    for (auto const change : {Change::SET_UID, Change::SET_EUID, Change::SET_RESUID, Change::SET_REUID}) {
        EXPECT_EQ(user.prepare({.change = change, .real = 0, .effective = 0, .saved = 0}, rejected), -EPERM);
        EXPECT_EQ(rejected, user);
    }
    EXPECT_EQ(root.user(), (Ids{0, 0, 0}));
}

TEST(Credentials, TemporaryDropRetainsSavedIdsAndPrivilegesFollowEffectiveUid) {
    Snapshot original = Snapshot::bootstrap_root();
    ASSERT_EQ(original.prepare({.change = Change::SET_RESUID, .real = 1000}, original), 0);
    Snapshot temporary;
    ASSERT_EQ(original.prepare({.change = Change::SET_EUID, .effective = 1000}, temporary), 0);
    EXPECT_EQ(temporary.user(), (Ids{1000, 1000, 0}));
    EXPECT_FALSE(temporary.allows(Authority::SET_GID));
    Snapshot restored;
    ASSERT_EQ(temporary.prepare({.change = Change::SET_EUID, .effective = 0}, restored), 0);
    EXPECT_EQ(restored.user(), original.user());
    EXPECT_EQ(restored.group(), original.group());
    EXPECT_FALSE(restored.dumpable());
}

TEST(Credentials, GroupAuthorityUsesCallerUserIdentityAndGroupsHaveStrictBounds) {
    Snapshot root = Snapshot::bootstrap_root();
    std::array<uint32_t, security::MAX_GROUPS + 1> groups{1, 2, 3, 4, 5, 6, 7, 8, 9};
    Snapshot next;
    EXPECT_EQ(root.prepare({.change = Change::SET_GROUPS, .groups = groups}, next), -EINVAL);
    EXPECT_EQ(next, root);
    ASSERT_EQ(root.prepare({.change = Change::SET_GROUPS, .groups = std::span(groups).first(8)}, next), 0);
    for (uint32_t group = 1; group <= 8; ++group) EXPECT_TRUE(next.has_group(group));
    EXPECT_FALSE(next.has_group(9));
    ASSERT_EQ(next.prepare({.change = Change::SET_GID, .real = 2000}, next), 0);
    EXPECT_EQ(next.group(), (Ids{2000, 2000, 2000}));
    EXPECT_TRUE(next.has_group(8));
    EXPECT_TRUE(next.allows(Authority::SET_GID));
    groups[3] = security::UNCHANGED_ID;
    Snapshot rejected;
    EXPECT_EQ(next.prepare({.change = Change::SET_GROUPS, .groups = std::span(groups).first(8)}, rejected), -EINVAL);
    EXPECT_EQ(rejected, next);
    ASSERT_EQ(next.prepare({.change = Change::SET_GROUPS}, next), 0);
    EXPECT_TRUE(next.groups().empty());
    EXPECT_FALSE(next.has_group(8));
    ASSERT_EQ(next.prepare({.change = Change::SET_UID, .real = 1000}, next), 0);
    EXPECT_EQ(next.prepare({.change = Change::SET_GROUPS}, rejected), -EPERM);
    EXPECT_EQ(rejected, next);
    EXPECT_EQ(next.prepare({.change = Change::SET_GID, .real = 0}, rejected), -EPERM);
    EXPECT_EQ(rejected, next);
}

TEST(Credentials, SentinelAndUnknownOperationsLeaveSnapshotUnchanged) {
    Snapshot const root = Snapshot::bootstrap_root();
    Snapshot next;
    for (Change const change : {Change::SET_UID, Change::SET_GID, Change::SET_EUID, Change::SET_EGID}) {
        EXPECT_EQ(root.prepare({.change = change}, next), -EINVAL);
        EXPECT_EQ(next, root);
    }
    EXPECT_EQ(root.prepare({.change = static_cast<Change>(255)}, next), -EINVAL);
    EXPECT_EQ(next, root);
    for (Change const change : {Change::SET_RESUID, Change::SET_RESGID, Change::SET_REUID, Change::SET_REGID}) {
        EXPECT_EQ(root.prepare({.change = change}, next), 0);
        EXPECT_EQ(next, root);
    }
}

TEST(Credentials, UnprivilegedResIdsValidateAgainstEntireOriginalTuple) {
    Snapshot original = Snapshot::bootstrap_root();
    ASSERT_EQ(original.prepare({.change = Change::SET_RESUID, .real = 1000, .effective = 1001, .saved = 1002}, original), 0);
    for (uint32_t real : {1000U, 1001U, 1002U, 1003U, security::UNCHANGED_ID}) {
        for (uint32_t effective : {1000U, 1001U, 1002U, 1003U, security::UNCHANGED_ID}) {
            for (uint32_t saved : {1000U, 1001U, 1002U, 1003U, security::UNCHANGED_ID}) {
                bool const valid = real != 1003 && effective != 1003 && saved != 1003;
                Snapshot next;
                EXPECT_EQ(original.prepare({.change = Change::SET_RESUID, .real = real, .effective = effective, .saved = saved}, next),
                          valid ? 0 : -EPERM);
                if (!valid) {
                    EXPECT_EQ(next, original);
                } else {
                    EXPECT_EQ(next.user(),
                              (Ids{real == security::UNCHANGED_ID ? 1000U : real, effective == security::UNCHANGED_ID ? 1001U : effective,
                                   saved == security::UNCHANGED_ID ? 1002U : saved}));
                }
            }
        }
    }
}

TEST(Credentials, ReIdsSavedRuleAndEffectiveNoop) {
    Snapshot original = Snapshot::bootstrap_root();
    ASSERT_EQ(original.prepare({.change = Change::SET_RESUID, .real = 1000, .effective = 1001, .saved = 1002}, original), 0);
    Snapshot next;
    ASSERT_EQ(original.prepare({.change = Change::SET_EUID, .effective = 1001}, next), 0);
    EXPECT_EQ(next, original);
    ASSERT_EQ(original.prepare({.change = Change::SET_REUID, .effective = 1000}, next), 0);
    EXPECT_EQ(next.user(), (Ids{1000, 1000, 1002}));
    ASSERT_EQ(original.prepare({.change = Change::SET_REUID, .real = 1001}, next), 0);
    EXPECT_EQ(next.user(), (Ids{1001, 1001, 1001}));
    EXPECT_EQ(original.prepare({.change = Change::SET_REUID, .real = 1002}, next), -EPERM);
    EXPECT_EQ(next, original);
}

TEST(Credentials, UnprivilegedGroupSavedIdsPermitRestoreButNotNewAuthority) {
    Snapshot original = Snapshot::bootstrap_root();
    ASSERT_EQ(original.prepare({.change = Change::SET_RESGID, .real = 2000, .effective = 2001, .saved = 2002}, original), 0);
    ASSERT_EQ(original.prepare({.change = Change::SET_UID, .real = 1000}, original), 0);
    Snapshot next;
    ASSERT_EQ(original.prepare({.change = Change::SET_GID, .real = 2002}, next), 0);
    EXPECT_EQ(next.group(), (Ids{2000, 2002, 2002}));
    ASSERT_EQ(original.prepare({.change = Change::SET_EGID, .effective = 2001}, next), 0);
    EXPECT_EQ(next, original);
    ASSERT_EQ(original.prepare({.change = Change::SET_RESGID, .real = 2002, .effective = 2000, .saved = 2001}, next), 0);
    EXPECT_EQ(next.group(), (Ids{2002, 2000, 2001}));
    ASSERT_EQ(original.prepare({.change = Change::SET_REGID, .real = 2001}, next), 0);
    EXPECT_EQ(next.group(), (Ids{2001, 2001, 2001}));
    EXPECT_EQ(original.prepare({.change = Change::SET_RESGID, .real = 2000, .effective = 2001, .saved = 0}, next), -EPERM);
    EXPECT_EQ(next, original);
    EXPECT_EQ(original.prepare({.change = Change::SET_REGID, .real = 2002}, next), -EPERM);
    EXPECT_EQ(next, original);
}

TEST(Credentials, InheritedValueAndNoNewPrivilegesSurviveFurtherChanges) {
    Snapshot parent = Snapshot::bootstrap_root();
    ASSERT_EQ(parent.prepare({.change = Change::NO_NEW_PRIVILEGES}, parent), 0);
    security::CredentialStore child(parent);
    ASSERT_EQ(child.update({.change = Change::SET_GID, .real = 2000}), 0);
    ASSERT_EQ(child.update({.change = Change::SET_UID, .real = 1000}), 0);
    EXPECT_EQ(parent.user().real, 0);
    EXPECT_EQ(parent.group().real, 0);
    EXPECT_TRUE(child.read().credentials.no_new_privileges());
    Snapshot next;
    ASSERT_EQ(child.read().credentials.prepare_exec({.uid = 0, .gid = 0, .set_uid = true, .set_gid = true}, next), 0);
    EXPECT_EQ(next.user(), (Ids{1000, 1000, 1000}));
    EXPECT_EQ(next.group(), (Ids{2000, 2000, 2000}));
    EXPECT_TRUE(next.no_new_privileges());
}

TEST(Credentials, OrdinaryExecRefreshesSavedIdsAndDoesNotMutateCaller) {
    Snapshot original = Snapshot::bootstrap_root();
    ASSERT_EQ(original.prepare({.change = Change::SET_RESUID, .real = 1000, .effective = 1001, .saved = 1002}, original), 0);
    Snapshot next;
    ASSERT_EQ(original.prepare_exec({}, next), 0);
    EXPECT_EQ(next.user(), (Ids{1000, 1001, 1001}));
    EXPECT_EQ(original.user().saved, 1002);
    EXPECT_EQ(next.authority_ceiling(), original.authority_ceiling());
}

TEST(Credentials, SetIdExecAndSuppressionAndInvalidOwners) {
    Snapshot original = Snapshot::bootstrap_root();
    ASSERT_EQ(original.prepare({.change = Change::SET_GID, .real = 1000}, original), 0);
    ASSERT_EQ(original.prepare({.change = Change::SET_UID, .real = 1000}, original), 0);
    for (unsigned policy = 0; policy < 8; ++policy) {
        Snapshot subject = original;
        if (policy & 1) ASSERT_EQ(subject.prepare({.change = Change::NO_NEW_PRIVILEGES}, subject), 0);
        Snapshot next;
        ASSERT_EQ(
            subject.prepare_exec(
                {.uid = 0, .gid = 0, .set_uid = true, .set_gid = true, .nosuid = (policy & 2) != 0, .traced = (policy & 4) != 0}, next),
            0);
        EXPECT_EQ(next.user(), (Ids{1000, policy == 0 ? 0U : 1000U, policy == 0 ? 0U : 1000U}));
        EXPECT_EQ(next.group(), next.user());
        EXPECT_EQ(next.no_new_privileges(), subject.no_new_privileges());
    }
    Snapshot next;
    EXPECT_EQ(original.prepare_exec({.uid = security::UNCHANGED_ID, .set_uid = true}, next), -EINVAL);
    EXPECT_EQ(next, original);
}

TEST(Credentials, DroppedAuthorityCannotBeRestoredByIdTransitionsOrExec) {
    Snapshot subject = Snapshot::bootstrap_root();
    uint64_t const ceiling = security::ALL_AUTHORITY & ~(uint64_t{1} << static_cast<unsigned>(Authority::POWER));
    ASSERT_EQ(subject.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = ceiling}, subject), 0);
    EXPECT_FALSE(subject.allows(Authority::POWER));
    EXPECT_TRUE(subject.allows(Authority::SET_UID));
    Snapshot next;
    EXPECT_EQ(subject.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = security::ALL_AUTHORITY}, next), -EPERM);
    EXPECT_EQ(next, subject);
    ASSERT_EQ(subject.prepare({.change = Change::SET_EUID, .effective = 1000}, subject), 0);
    ASSERT_EQ(subject.prepare({.change = Change::SET_EUID, .effective = 0}, subject), 0);
    ASSERT_EQ(subject.prepare_exec({.uid = 0, .set_uid = true}, next), 0);
    EXPECT_FALSE(next.allows(Authority::POWER));
}

TEST(CredentialStore, ReadersRetainOldValuesAndRejectionsDoNotPublish) {
    security::CredentialStore store(Snapshot::bootstrap_root());
    auto const before = store.read();
    ASSERT_EQ(store.update({.change = Change::SET_UID, .real = 1000}), 0);
    auto const dropped = store.read();
    EXPECT_EQ(dropped.generation, before.generation + 1);
    EXPECT_EQ(before.credentials.user().effective, 0);
    EXPECT_EQ(store.update({.change = Change::SET_UID, .real = 0}), -EPERM);
    EXPECT_EQ(store.read().generation, dropped.generation);
    EXPECT_EQ(store.read().credentials, dropped.credentials);
    EXPECT_EQ(store.update({.change = Change::SET_UID, .real = 1000}), 0);
    EXPECT_EQ(store.read().generation, dropped.generation);
}

TEST(CredentialStore, ConcurrentGroupPublicationsNeverExposeTornTuples) {
    security::CredentialStore store(Snapshot::bootstrap_root());
    std::atomic<bool> start{};
    std::atomic<unsigned> failures{};
    std::vector<std::thread> workers;
    for (unsigned worker = 0; worker < 6; ++worker) {
        workers.emplace_back([&, worker] {
            while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
            for (unsigned iteration = 0; iteration < 5000; ++iteration) {
                if (worker < 2) {
                    if (store.update({.change = Change::SET_GID, .real = 1000 + worker}) != 0) ++failures;
                } else {
                    auto const value = store.read();
                    auto const ids = value.credentials.group();
                    if (ids.real != ids.effective || ids.real != ids.saved || value.credentials.user().effective != 0) ++failures;
                }
            }
        });
    }
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(failures.load(), 0);
}

TEST(Credentials, SignalAuthorityUsesCallerRealEffectiveAndTargetRealSaved) {
    Snapshot const root = Snapshot::bootstrap_root();
    for (uint32_t cr = 1000; cr < 1003; ++cr) {
        for (uint32_t ce = 1000; ce < 1003; ++ce) {
            for (uint32_t cs = 1000; cs < 1003; ++cs) {
                Snapshot caller;
                ASSERT_EQ(root.prepare({.change = Change::SET_RESUID, .real = cr, .effective = ce, .saved = cs}, caller), 0);
                for (uint32_t tr = 1000; tr < 1003; ++tr) {
                    for (uint32_t te = 1000; te < 1003; ++te) {
                        for (uint32_t ts = 1000; ts < 1003; ++ts) {
                            Snapshot target;
                            ASSERT_EQ(root.prepare({.change = Change::SET_RESUID, .real = tr, .effective = te, .saved = ts}, target), 0);
                            bool const expected = cr == tr || cr == ts || ce == tr || ce == ts;
                            EXPECT_EQ(security::may_signal(caller, target, false, false), expected);
                            EXPECT_EQ(security::may_signal(caller, target, true, false), expected);
                            EXPECT_EQ(security::may_signal(caller, target, false, true), expected);
                            EXPECT_TRUE(security::may_signal(caller, target, true, true));
                        }
                    }
                }
            }
        }
    }
}

TEST(Credentials, SignalAuthorityCeilingCannotBeRestored) {
    Snapshot root = Snapshot::bootstrap_root();
    Snapshot user;
    ASSERT_EQ(root.prepare({.change = Change::SET_UID, .real = 1000}, user), 0);
    EXPECT_TRUE(security::may_signal(root, user, false, false));
    ASSERT_EQ(root.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = 0}, root), 0);
    EXPECT_FALSE(security::may_signal(root, user, false, false));
    EXPECT_EQ(root.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = security::ALL_AUTHORITY}, root), -EPERM);
    EXPECT_FALSE(security::may_signal(root, user, false, false));
}

TEST(Credentials, PriorityOwnershipRaiseDenialAndClamping) {
    Snapshot const root = Snapshot::bootstrap_root();
    Snapshot user;
    Snapshot target;
    ASSERT_EQ(root.prepare({.change = Change::SET_RESUID, .real = 1000, .effective = 1001, .saved = 1002}, user), 0);
    for (uint32_t real = 1000; real < 1004; ++real) {
        for (uint32_t effective = 1000; effective < 1004; ++effective) {
            ASSERT_EQ(root.prepare({.change = Change::SET_RESUID, .real = real, .effective = effective, .saved = 1001}, target), 0);
            bool const owns = real == 1001 || effective == 1001;
            for (int old = -20; old <= 19; ++old) {
                for (int requested = -22; requested <= 21; ++requested) {
                    int const clamped = requested < -20 ? -20 : (requested > 19 ? 19 : requested);
                    int const expected = !owns ? -EPERM : (clamped < old ? -EACCES : 0);
                    EXPECT_EQ(security::clamp_priority(requested), clamped);
                    EXPECT_EQ(security::authorize_priority(user, target, old, requested), expected);
                    EXPECT_EQ(security::authorize_priority(root, target, old, requested), 0);
                }
            }
        }
    }
}

TEST(Credentials, DumpabilityFollowsEffectiveIdentityAndRetainedExecutable) {
    Snapshot value = Snapshot::bootstrap_root();
    EXPECT_TRUE(value.dumpable());
    ASSERT_EQ(value.prepare({.change = Change::SET_GID, .real = 2000}, value), 0);
    EXPECT_FALSE(value.dumpable());
    ASSERT_EQ(value.prepare({.change = Change::SET_DUMPABLE, .dumpable = true}, value), 0);
    ASSERT_EQ(value.prepare({.change = Change::SET_UID, .real = 1000}, value), 0);
    EXPECT_FALSE(value.dumpable());
    Snapshot before = value;
    ASSERT_EQ(value.prepare({.change = Change::SET_EUID, .effective = 0}, value), -EPERM);
    EXPECT_EQ(value, before);
    ASSERT_EQ(value.prepare_exec({.uid = 1000, .gid = 2000, .mode = 0500}, value), 0);
    EXPECT_TRUE(value.dumpable());
    ASSERT_EQ(value.prepare_exec({.uid = 1000, .gid = 2000, .mode = 0111}, value), 0);
    EXPECT_FALSE(value.dumpable());
    ASSERT_EQ(value.prepare_exec({.uid = 2001, .gid = 2000, .mode = 0040}, value), 0);
    EXPECT_TRUE(value.dumpable());
    ASSERT_EQ(value.prepare_exec({.uid = 0, .gid = 0, .set_uid = true, .mode = 04755}, value), 0);
    EXPECT_FALSE(value.dumpable());
}

TEST(Credentials, PtraceRequiresAllIdsDumpabilityAndSupplementaryGroups) {
    Snapshot const root = Snapshot::bootstrap_root();
    Snapshot user = root;
    ASSERT_EQ(user.prepare({.change = Change::SET_GID, .real = 2000}, user), 0);
    ASSERT_EQ(user.prepare({.change = Change::SET_UID, .real = 1000}, user), 0);
    ASSERT_EQ(user.prepare({.change = Change::SET_DUMPABLE, .dumpable = true}, user), 0);
    EXPECT_TRUE(security::may_trace(user, user));
    for (unsigned field = 0; field < 6; ++field) {
        Snapshot target = root;
        uint32_t const ur = field == 0 ? 1001 : 1000;
        uint32_t const ue = field == 1 ? 1001 : 1000;
        uint32_t const us = field == 2 ? 1001 : 1000;
        uint32_t const gr = field == 3 ? 2001 : 2000;
        uint32_t const ge = field == 4 ? 2001 : 2000;
        uint32_t const gs = field == 5 ? 2001 : 2000;
        ASSERT_EQ(target.prepare({.change = Change::SET_RESGID, .real = gr, .effective = ge, .saved = gs}, target), 0);
        ASSERT_EQ(target.prepare({.change = Change::SET_RESUID, .real = ur, .effective = ue, .saved = us}, target), 0);
        ASSERT_EQ(target.prepare({.change = Change::SET_DUMPABLE, .dumpable = true}, target), 0);
        EXPECT_FALSE(security::may_trace(user, target));
        EXPECT_TRUE(security::may_trace(root, target));
    }
    Snapshot target = root;
    std::array<uint32_t, 1> const groups{3000};
    ASSERT_EQ(target.prepare({.change = Change::SET_GROUPS, .groups = groups}, target), 0);
    ASSERT_EQ(target.prepare({.change = Change::SET_GID, .real = 2000}, target), 0);
    ASSERT_EQ(target.prepare({.change = Change::SET_UID, .real = 1000}, target), 0);
    ASSERT_EQ(target.prepare({.change = Change::SET_DUMPABLE, .dumpable = true}, target), 0);
    EXPECT_FALSE(security::may_trace(user, target));
    EXPECT_TRUE(security::may_trace(target, user));
    ASSERT_EQ(user.prepare({.change = Change::SET_DUMPABLE, .dumpable = false}, user), 0);
    EXPECT_FALSE(security::may_trace(target, user));
    EXPECT_TRUE(security::may_trace(root, user));
}

TEST(Credentials, PtraceCannotBypassDiscardedAuthorityOrNoNewPrivileges) {
    Snapshot const root = Snapshot::bootstrap_root();
    Snapshot restricted = root;
    auto const ceiling = security::ALL_AUTHORITY & ~(uint64_t{1} << static_cast<unsigned>(Authority::POWER));
    ASSERT_EQ(restricted.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = ceiling}, restricted), 0);
    EXPECT_TRUE(restricted.allows(Authority::PTRACE));
    EXPECT_FALSE(security::may_trace(restricted, root));
    EXPECT_TRUE(security::may_trace(root, restricted));
    EXPECT_TRUE(security::may_trace(restricted, restricted));
    Snapshot bounded = root;
    ASSERT_EQ(bounded.prepare({.change = Change::NO_NEW_PRIVILEGES}, bounded), 0);
    EXPECT_FALSE(security::may_trace(bounded, root));
    EXPECT_TRUE(security::may_trace(root, bounded));
    EXPECT_TRUE(security::may_trace(bounded, bounded));
}

TEST(Credentials, DebugRegistersAcceptOnlyUserReturnState) {
    ker::abi::ptrace::X86_64GprState base{};
    base.rip = 0x401000;
    base.rsp = 0x7fff0000;
    base.cs = 0x23;
    base.ss = 0x1b;
    base.rflags = 0x202;
    auto valid = [](auto const& state) { return security::valid_debug_gprs(state, 0x23, 0x1b); };
    ASSERT_TRUE(valid(base));
    for (unsigned bit = 0; bit < 64; ++bit) {
        auto state = base;
        state.rflags ^= uint64_t{1} << bit;
        bool const allowed = bit == 0 || bit == 2 || bit == 4 || bit == 6 || bit == 7 || bit == 8 || bit == 10 || bit == 11 || bit == 16 ||
                             bit == 18 || bit == 21;
        EXPECT_EQ(valid(state), allowed) << bit;
    }
    for (uint64_t address :
         {uint64_t{0}, uint64_t{1}, security::DEBUG_USER_LIMIT - 1, security::DEBUG_USER_LIMIT, uint64_t{0xffff800000000000}, UINT64_MAX}) {
        for (auto field : {&ker::abi::ptrace::X86_64GprState::rip, &ker::abi::ptrace::X86_64GprState::rsp,
                           &ker::abi::ptrace::X86_64GprState::fs_base, &ker::abi::ptrace::X86_64GprState::gs_base}) {
            auto state = base;
            state.*field = address;
            bool const nullable =
                field == &ker::abi::ptrace::X86_64GprState::fs_base || field == &ker::abi::ptrace::X86_64GprState::gs_base;
            EXPECT_EQ(valid(state), address < security::DEBUG_USER_LIMIT && (address != 0 || nullable));
        }
    }
    for (uint64_t selector = 0; selector < 256; ++selector) {
        auto state = base;
        state.cs = selector;
        EXPECT_EQ(valid(state), selector == 0x23);
        state = base;
        state.ss = selector;
        EXPECT_EQ(valid(state), selector == 0x1b);
    }
}

TEST(Credentials, DebugXstateRejectsRestoreFaultsAndUnsupportedFeatures) {
    std::array<uint8_t, 2688> state{};
    for (uint64_t enabled : {uint64_t{0}, uint64_t{3}, uint64_t{7}}) {
        EXPECT_TRUE(security::valid_debug_xstate(state, enabled, 0xffbf));
        for (unsigned bit = 0; bit < 32; ++bit) {
            state[24 + bit / 8] = 1U << (bit % 8);
            EXPECT_EQ(security::valid_debug_xstate(state, enabled, 0xffbf), bit < 16 && bit != 6);
            state[24 + bit / 8] = 0;
        }
    }
    for (uint64_t enabled : {uint64_t{3}, uint64_t{7}}) {
        for (unsigned bit = 0; bit < 64; ++bit) {
            state[512 + bit / 8] = 1U << (bit % 8);
            EXPECT_EQ(security::valid_debug_xstate(state, enabled, 0xffff), bit < (enabled == 7 ? 3U : 2U));
            state[512 + bit / 8] = 0;
        }
        for (size_t byte = 520; byte < 576; ++byte) {
            state[byte] = 1;
            EXPECT_FALSE(security::valid_debug_xstate(state, enabled, 0xffff));
            state[byte] = 0;
        }
    }
    // An attacker-controlled MXCSR_MASK must not authorize the high bits.
    for (size_t byte = 28; byte < 32; ++byte) state[byte] = 0xff;
    state[27] = 0x80;
    EXPECT_FALSE(security::valid_debug_xstate(state, 7, 0xffff));
    state[27] = 0;
    EXPECT_FALSE(security::valid_debug_xstate(state, 0x207, 0xffff));
    EXPECT_FALSE(security::valid_debug_xstate(std::span(state).first(511), 0, 0xffff));
    EXPECT_FALSE(security::valid_debug_xstate(std::span(state).first(575), 3, 0xffff));
    EXPECT_FALSE(security::valid_debug_xstate(std::span(state).first(831), 7, 0xffff));
    EXPECT_TRUE(security::valid_debug_xstate(std::span(state).first(512), 0, 0xffff));
    EXPECT_TRUE(security::valid_debug_xstate(std::span(state).first(576), 3, 0xffff));
    EXPECT_TRUE(security::valid_debug_xstate(std::span(state).first(832), 7, 0xffff));
}

TEST(Credentials, DebugBreakpointsAreBoundedUserAccesses) {
    using Type = ker::abi::ptrace::hw_break_type;
    for (uint64_t type = 0; type < 8; ++type) {
        for (uint32_t length = 0; length <= 16; ++length) {
            for (uint32_t slot = 0; slot < 8; ++slot) {
                ker::abi::ptrace::HwBreak desc{
                    .address = 0x401000, .length = length, .type = static_cast<Type>(type), .slot = slot, .reserved = 0};
                bool const allowed = slot < 4 && ((type == 0 && length == 1) ||
                                                  ((type == 1 || type == 3) && (length == 1 || length == 2 || length == 4 || length == 8)));
                EXPECT_EQ(security::valid_debug_breakpoint(desc, true), allowed);
                EXPECT_EQ(security::valid_debug_breakpoint(desc, false), slot < 4);
                desc.reserved = 1;
                EXPECT_FALSE(security::valid_debug_breakpoint(desc, true));
                EXPECT_FALSE(security::valid_debug_breakpoint(desc, false));
            }
        }
    }
    for (uint64_t address : {uint64_t{0}, uint64_t{0x401001}, security::DEBUG_USER_LIMIT - 1, security::DEBUG_USER_LIMIT, UINT64_MAX}) {
        ker::abi::ptrace::HwBreak desc{.address = address, .length = 8, .type = Type::WRITE, .slot = 0, .reserved = 0};
        EXPECT_FALSE(security::valid_debug_breakpoint(desc, true));
    }
}

TEST(Credentials, IpcPermissionsUseOwnerCreatorAndGroupClasses) {
    for (uint32_t uid : {1000U, 1001U, 1002U}) {
        for (uint32_t gid : {2000U, 2001U, 2002U}) {
            Snapshot caller = Snapshot::bootstrap_root();
            ASSERT_EQ(caller.prepare({.change = Change::SET_GID, .real = gid}, caller), 0);
            ASSERT_EQ(caller.prepare({.change = Change::SET_UID, .real = uid}, caller), 0);
            for (uint32_t mode = 0; mode < 01000; ++mode) {
                security::IpcOwnership object{1000, 2000, 1001, 2001, mode};
                unsigned const shift = uid != 1002 ? 6 : (gid != 2002 ? 3 : 0);
                for (uint32_t required : {0U, 2U, 4U, 6U}) {
                    EXPECT_EQ(security::may_access_ipc(caller, object, required), ((mode >> shift) & required) == required);
                }
                EXPECT_EQ(security::may_control_ipc(caller, object), uid != 1002);
            }
        }
    }
}

TEST(Credentials, IpcSupplementaryGroupsAndDiscardedOverrides) {
    security::IpcOwnership object{1001, 2001, 1002, 2002, 0060};
    Snapshot caller = Snapshot::bootstrap_root();
    std::array<uint32_t, 1> groups{2002};
    ASSERT_EQ(caller.prepare({.change = Change::SET_GROUPS, .groups = groups}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_GID, .real = 2000}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_UID, .real = 1000}, caller), 0);
    EXPECT_TRUE(security::may_access_ipc(caller, object, 6));
    EXPECT_FALSE(security::may_control_ipc(caller, object));
    EXPECT_FALSE(security::may_access_ipc(caller, object, 7));
    auto root = Snapshot::bootstrap_root();
    object.mode = 0;
    EXPECT_TRUE(security::may_access_ipc(root, object, 6));
    EXPECT_TRUE(security::may_control_ipc(root, object));
    ASSERT_EQ(root.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = 0}, root), 0);
    EXPECT_FALSE(security::may_access_ipc(root, object, 6));
    EXPECT_FALSE(security::may_control_ipc(root, object));
    EXPECT_TRUE(security::may_access_ipc(root, object, 0));
}

TEST(Credentials, SharedWriteRestrictionIsDistinctFromReservedAddresses) {
    namespace paging = ker::mod::mm::paging;
    for (uint64_t flags = 0; flags < 4096; ++flags) {
        bool const expected = (flags & 1) != 0 && (flags & 0x400) != 0 && (flags & 0x800) != 0;
        EXPECT_EQ(paging::shared_write_restricted(flags), expected);
        EXPECT_EQ(paging::shared_write_restricted(flags | paging::PAGE_NX), expected);
    }
    EXPECT_FALSE(paging::shared_write_restricted(paging::PAGE_RESERVED));
    EXPECT_TRUE(paging::shared_write_restricted(paging::PAGE_PRESENT | paging::PAGE_SHARED | paging::PAGE_WRITE_RESTRICTED));
}

TEST(Credentials, FileMetadataUsesEffectiveOwnerAndGroupMembership) {
    Snapshot caller = Snapshot::bootstrap_root();
    std::array<uint32_t, 1> groups{3000};
    ASSERT_EQ(caller.prepare({.change = Change::SET_GROUPS, .groups = groups}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_GID, .real = 2000}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_RESUID, .real = 1001, .effective = 1000, .saved = 1002}, caller), 0);
    for (uint32_t uid : {0U, 1000U, 1001U, 1002U}) {
        for (uint32_t gid : {2000U, 3000U, 4000U}) {
            for (uint32_t type : {0100000U, 0040000U, 0120000U}) {
                security::FileIdentity const object{uid, gid, type | 0644};
                for (uint32_t mode = 0; mode < 010000; ++mode) {
                    security::FileIdentity next;
                    EXPECT_EQ(security::prepare_file_chmod(caller, object, mode, next), uid == 1000 ? 0 : -EPERM);
                    if (uid != 1000)
                        EXPECT_EQ(next, object);
                    else
                        EXPECT_EQ(next, (security::FileIdentity{uid, gid, type | (gid == 4000 ? mode & ~02000U : mode)}));
                }
                for (uint32_t next_uid : {UINT32_MAX, 0U, 1000U, 1001U}) {
                    for (uint32_t next_gid : {UINT32_MAX, 2000U, 3000U, 4000U}) {
                        security::FileIdentity next;
                        bool const allowed = uid == 1000 && (next_uid == UINT32_MAX || next_uid == uid) &&
                                             (next_gid == UINT32_MAX || next_gid == gid || next_gid == 2000 || next_gid == 3000);
                        EXPECT_EQ(security::prepare_file_chown(caller, object, next_uid, next_gid, next), allowed ? 0 : -EPERM);
                        EXPECT_EQ(next,
                                  allowed ? (security::FileIdentity{uid, next_gid == UINT32_MAX ? gid : next_gid, object.mode}) : object);
                    }
                }
            }
        }
    }
}

TEST(Credentials, FixedMetadataAuthorizesNoOpsAndRejectsUnrepresentableChanges) {
    auto root = Snapshot::bootstrap_root();
    auto user = root;
    ASSERT_EQ(user.prepare({.change = Change::SET_UID, .real = 1000}, user), 0);
    for (uint32_t mode : {0100644U, 0100444U, 0040755U}) {
        security::FileIdentity const object{0, 0, mode};
        for (uint32_t requested = 0; requested < 010000; ++requested) {
            EXPECT_EQ(security::authorize_fixed_file_chmod(root, object, requested), requested == (mode & 07777) ? 0 : -EOPNOTSUPP);
            EXPECT_EQ(security::authorize_fixed_file_chmod(user, object, requested), -EPERM);
        }
        for (uint32_t uid : {UINT32_MAX, 0U, 1000U}) {
            for (uint32_t gid : {UINT32_MAX, 0U, 2000U}) {
                EXPECT_EQ(security::authorize_fixed_file_chown(root, object, uid, gid), uid != 1000 && gid != 2000 ? 0 : -EOPNOTSUPP);
                EXPECT_EQ(security::authorize_fixed_file_chown(user, object, uid, gid), -EPERM);
            }
        }
    }
    ASSERT_EQ(root.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = 0}, root), 0);
    EXPECT_EQ(security::authorize_fixed_file_chmod(root, {0, 0, 0100644}, 0644), 0);
    EXPECT_EQ(security::authorize_fixed_file_chown(root, {1000, 2000, 0100644}, UINT32_MAX, UINT32_MAX), -EPERM);
}

TEST(Credentials, ChownClearsExecutableSetIdsAndDiscardedFileAuthorityCannotOverride) {
    auto caller = Snapshot::bootstrap_root();
    for (uint32_t type : {0100000U, 0040000U, 0120000U}) {
        for (uint32_t mode : {06000U, 06001U, 06010U, 06100U, 06755U}) {
            security::FileIdentity const object{1000, 2000, type | mode};
            security::FileIdentity next;
            ASSERT_EQ(security::prepare_file_chown(caller, object, UINT32_MAX, UINT32_MAX, next), 0);
            EXPECT_EQ(next.mode, type | (type == 0100000 && (mode & 0111) ? mode & ~06000U : mode));
            EXPECT_EQ(next.uid, object.uid);
            EXPECT_EQ(next.gid, object.gid);
        }
    }
    security::FileIdentity const foreign{1000, 2000, 0100644};
    security::FileIdentity next;
    ASSERT_EQ(security::prepare_file_chown(caller, foreign, 1234, 5678, next), 0);
    EXPECT_EQ(next, (security::FileIdentity{1234, 5678, 0100644}));
    ASSERT_EQ(caller.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = 0}, caller), 0);
    EXPECT_EQ(security::prepare_file_chmod(caller, foreign, 0777, next), -EPERM);
    EXPECT_EQ(next, foreign);
    EXPECT_EQ(security::prepare_file_chown(caller, foreign, 0, 0, next), -EPERM);
    EXPECT_EQ(next, foreign);
    EXPECT_EQ(security::prepare_file_chmod(caller, {0, 0, 0100644}, 0600, next), 0);
}

TEST(Credentials, TimestampRequestsPreserveNowOmitAndExplicitIntent) {
    using ker::vfs::TimestampUpdate;
    using Intent = security::FileTimestampIntent;
    ker::vfs::Timespec const clock{12345, 6789};
    for (int64_t first : {TimestampUpdate::NOW, TimestampUpdate::OMIT, int64_t{99}}) {
        for (int64_t second : {TimestampUpdate::NOW, TimestampUpdate::OMIT, int64_t{88}}) {
            std::array<ker::vfs::Timespec, 2> request{{{INT64_MIN, first}, {INT64_MAX, second}}};
            TimestampUpdate update;
            ASSERT_EQ(TimestampUpdate::prepare(request.data(), clock, update), 0);
            EXPECT_EQ(update.set_atime(), first != TimestampUpdate::OMIT);
            EXPECT_EQ(update.set_mtime(), second != TimestampUpdate::OMIT);
            EXPECT_EQ(update.intent(), first == TimestampUpdate::OMIT && second == TimestampUpdate::OMIT ? Intent::PRESERVE
                                       : first == TimestampUpdate::NOW && second == TimestampUpdate::NOW ? Intent::SET_TO_NOW
                                                                                                         : Intent::SET_EXPLICIT);
            if (first == TimestampUpdate::NOW) EXPECT_EQ(update.atime().tv_sec, clock.tv_sec);
            if (second == TimestampUpdate::NOW) EXPECT_EQ(update.mtime().tv_nsec, clock.tv_nsec);
            if (first == 99) EXPECT_EQ(update.atime().tv_sec, INT64_MIN);
            if (second == 88) EXPECT_EQ(update.mtime().tv_sec, INT64_MAX);
            request[0] = {};
            request[1] = {};
            EXPECT_EQ(update.ctime().tv_nsec, clock.tv_nsec);
        }
    }
    TimestampUpdate retained;
    ASSERT_EQ(TimestampUpdate::prepare(nullptr, clock, retained), 0);
    EXPECT_EQ(retained.intent(), Intent::SET_TO_NOW);
    for (int64_t invalid : {int64_t{-1}, int64_t{1000000000}, INT64_MAX}) {
        std::array<ker::vfs::Timespec, 2> request{{{0, TimestampUpdate::OMIT}, {0, invalid}}};
        EXPECT_EQ(TimestampUpdate::prepare(request.data(), {1, 2}, retained), -EINVAL);
        EXPECT_EQ(retained.intent(), Intent::SET_TO_NOW);
        EXPECT_EQ(retained.atime().tv_sec, clock.tv_sec);
        EXPECT_EQ(retained.mtime().tv_nsec, clock.tv_nsec);
        EXPECT_EQ(retained.ctime().tv_sec, clock.tv_sec);
    }
    EXPECT_EQ(TimestampUpdate::prepare(nullptr, {0, -1}, retained), -EINVAL);
}

TEST(Credentials, TimestampAuthorityUsesRequestIntentAndCurrentWritePermission) {
    using Intent = security::FileTimestampIntent;
    auto caller = Snapshot::bootstrap_root();
    std::array<uint32_t, 1> const groups{3000};
    ASSERT_EQ(caller.prepare({.change = Change::SET_GROUPS, .groups = groups}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_GID, .real = 2000}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_RESUID, .real = 1001, .effective = 1000, .saved = 1002}, caller), 0);
    for (uint32_t uid : {1000U, 1001U, 1002U}) {
        for (uint32_t gid : {2000U, 3000U, 4000U}) {
            for (uint32_t mode = 0; mode < 01000; ++mode) {
                security::FileIdentity const object{uid, gid, 0100000 | mode};
                bool const write = (mode & (gid == 4000 ? 0002 : 0020)) != 0;
                EXPECT_EQ(security::authorize_file_timestamps(caller, object, Intent::PRESERVE), 0);
                EXPECT_EQ(security::authorize_file_timestamps(caller, object, Intent::SET_EXPLICIT), uid == 1000 ? 0 : -EPERM);
                EXPECT_EQ(security::authorize_file_timestamps(caller, object, Intent::SET_TO_NOW), uid == 1000 || write ? 0 : -EACCES);
            }
        }
    }
    auto root = Snapshot::bootstrap_root();
    security::FileIdentity const foreign{1000, 2000, 0100000};
    EXPECT_EQ(security::authorize_file_timestamps(root, foreign, Intent::SET_EXPLICIT), 0);
    uint64_t const DAC_ONLY = uint64_t{1} << static_cast<unsigned>(Authority::DAC_OVERRIDE);
    ASSERT_EQ(root.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = DAC_ONLY}, root), 0);
    EXPECT_EQ(security::authorize_file_timestamps(root, foreign, Intent::SET_EXPLICIT), -EPERM);
    EXPECT_EQ(security::authorize_file_timestamps(root, foreign, Intent::SET_TO_NOW), 0);
    ASSERT_EQ(root.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = 0}, root), 0);
    EXPECT_EQ(security::authorize_file_timestamps(root, foreign, Intent::SET_TO_NOW), -EACCES);
    EXPECT_EQ(security::authorize_file_timestamps(root, foreign, static_cast<Intent>(255)), -EINVAL);
}

TEST(Credentials, FileAccessAndXattrUseOneSnapshotAcrossModeAndNamespace) {
    using Namespace = security::FileXattrNamespace;
    auto const root = security::Snapshot::bootstrap_root();
    auto caller = root;
    std::array<uint32_t, 1> const GROUPS{3000};
    ASSERT_EQ(caller.prepare({.change = security::Change::SET_GROUPS, .groups = GROUPS}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = security::Change::SET_GID, .real = 2000}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    for (uint32_t mode = 0; mode < 512; ++mode) {
        for (uint32_t uid : {1000U, 1001U}) {
            for (uint32_t gid : {2000U, 3000U, 4000U}) {
                security::FileIdentity const file{uid, gid, 0100000U | mode};
                unsigned const SHIFT = uid == 1000 ? 6 : (gid == 2000 || gid == 3000 ? 3 : 0);
                for (unsigned access = 0; access < 8; ++access) {
                    EXPECT_EQ(security::authorize_file_access(caller, file, access), ((mode >> SHIFT) & access) == access ? 0 : -EACCES);
                    EXPECT_EQ(security::authorize_file_access(root, file, access), (access & 1) != 0 && (mode & 0111) == 0 ? -EACCES : 0);
                }
                for (bool write : {false, true}) {
                    EXPECT_EQ(security::authorize_file_xattr(caller, file, Namespace::USER, write),
                              ((mode >> SHIFT) & (write ? 2 : 4)) != 0 ? 0 : -EACCES);
                    for (auto space : {Namespace::TRUSTED, Namespace::SECURITY}) {
                        EXPECT_EQ(security::authorize_file_xattr(caller, file, space, write), -EPERM);
                        EXPECT_EQ(security::authorize_file_xattr(root, file, space, write), 0);
                    }
                }
            }
        }
    }
    security::FileIdentity const symlink{1000, 2000, 0120777};
    EXPECT_EQ(security::authorize_file_xattr(caller, symlink, Namespace::USER, true), -EPERM);
    EXPECT_EQ(security::authorize_file_xattr(root, symlink, Namespace::USER, true), -EPERM);
    EXPECT_EQ(security::authorize_file_xattr(caller, symlink, Namespace::USER, false), 0);
    EXPECT_EQ(security::authorize_file_xattr(root, symlink, static_cast<Namespace>(255), false), -EINVAL);
}

TEST(Credentials, XattrNamespaceAuthorityIsIndependentOfDacOverride) {
    using Namespace = security::FileXattrNamespace;
    auto caller = security::Snapshot::bootstrap_root();
    security::FileIdentity const foreign{1000, 2000, 0100000};
    ASSERT_EQ(caller.prepare({.change = security::Change::DROP_AUTHORITY,
                              .authority_ceiling = uint64_t{1} << static_cast<unsigned>(security::Authority::FILE_OWNER)},
                             caller),
              0);
    EXPECT_EQ(security::authorize_file_xattr(caller, foreign, Namespace::USER, false), -EACCES);
    EXPECT_EQ(security::authorize_file_xattr(caller, foreign, Namespace::TRUSTED, true), 0);
    caller = security::Snapshot::bootstrap_root();
    ASSERT_EQ(caller.prepare({.change = security::Change::DROP_AUTHORITY,
                              .authority_ceiling = uint64_t{1} << static_cast<unsigned>(security::Authority::DAC_OVERRIDE)},
                             caller),
              0);
    EXPECT_EQ(security::authorize_file_xattr(caller, foreign, Namespace::USER, true), 0);
    EXPECT_EQ(security::authorize_file_xattr(caller, foreign, Namespace::SECURITY, true), -EPERM);
}

TEST(Credentials, FileOpenChecksAccessAndTruncationBeforeMutation) {
    auto caller = Snapshot::bootstrap_root();
    ASSERT_EQ(caller.prepare({.change = Change::SET_GID, .real = 2000}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_UID, .real = 1000}, caller), 0);
    for (uint32_t mode = 0; mode < 512; ++mode) {
        for (uint32_t uid : {1000U, 4000U}) {
            for (uint32_t gid : {2000U, 4000U}) {
                security::FileIdentity const file{uid, gid, 0100000U | mode};
                unsigned const SHIFT = uid == 1000 ? 6 : (gid == 2000 ? 3 : 0);
                for (unsigned access = 0; access < 3; ++access) {
                    for (bool truncate : {false, true}) {
                        unsigned const REQUIRED = (access != 1 ? 4U : 0U) | (access != 0 || truncate ? 2U : 0U);
                        EXPECT_EQ(security::authorize_file_open(caller, file, access, truncate),
                                  ((mode >> SHIFT) & REQUIRED) == REQUIRED ? 0 : -EACCES);
                    }
                }
            }
        }
    }
    security::FileIdentity const directory{1000, 2000, 0040777};
    EXPECT_EQ(security::authorize_file_open(caller, directory, 0, false), 0);
    EXPECT_EQ(security::authorize_file_open(caller, directory, 0, true), -EISDIR);
    EXPECT_EQ(security::authorize_file_open(Snapshot::bootstrap_root(), directory, 2, false), -EISDIR);
    EXPECT_EQ(security::authorize_file_open(caller, directory, 3, false), -EINVAL);
}

TEST(Credentials, FileCreationUsesEffectiveIdentityAndSetgidParent) {
    auto caller = Snapshot::bootstrap_root();
    std::array<uint32_t, 1> const GROUPS{3000};
    ASSERT_EQ(caller.prepare({.change = Change::SET_GROUPS, .groups = GROUPS}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_GID, .real = 2000}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_UID, .real = 1000}, caller), 0);
    for (uint32_t parent_mode = 0; parent_mode < 512; ++parent_mode) {
        security::FileIdentity const parent{4000, 4000, 0040000U | parent_mode};
        security::FileIdentity output{123, 456, 789};
        int const RET = security::prepare_file_create(caller, parent, 0000, output);
        EXPECT_EQ(RET, (parent_mode & 3U) == 3U ? 0 : -EACCES);
        EXPECT_EQ(output,
                  (parent_mode & 3U) == 3U ? (security::FileIdentity{1000, 2000, 0100000}) : (security::FileIdentity{123, 456, 789}));
    }
    for (uint32_t gid : {2000U, 3000U, 4000U}) {
        security::FileIdentity const parent{4000, gid, 0042777};
        for (uint32_t mode = 0; mode < 4096; ++mode) {
            security::FileIdentity output{};
            ASSERT_EQ(security::prepare_file_create(caller, parent, mode, output), 0);
            EXPECT_EQ(output.uid, 1000U);
            EXPECT_EQ(output.gid, gid);
            EXPECT_EQ(output.mode, 0100000U | (gid == 4000 ? mode & ~02000U : mode));
        }
    }
    security::FileIdentity output{123, 456, 789};
    EXPECT_EQ(security::prepare_file_create(caller, {1000, 2000, 0100777}, 0644, output), -ENOTDIR);
    EXPECT_EQ(output, (security::FileIdentity{123, 456, 789}));
}

TEST(Credentials, GlobalDiagnosticsRequireTheirSpecificAuthority) {
    using Access = security::DiagnosticAccess;
    auto const ROOT = Snapshot::bootstrap_root();
    Snapshot user;
    ASSERT_EQ(ROOT.prepare({.change = Change::SET_UID, .real = 1000}, user), 0);
    for (auto access :
         {Access::DENY, Access::PUBLIC_READ, Access::KERNEL_READ, Access::KERNEL_CONTROL, Access::WKI_CONTROL, static_cast<Access>(255)}) {
        for (bool write : {false, true}) {
            bool const PUBLIC = access == Access::PUBLIC_READ;
            bool const READ_ONLY = PUBLIC || access == Access::KERNEL_READ;
            bool const INVALID = access == Access::DENY || access == static_cast<Access>(255);
            EXPECT_EQ(security::authorize_diagnostic(ROOT, access, write), INVALID ? -EACCES : (write && READ_ONLY ? -EPERM : 0));
            EXPECT_EQ(security::authorize_diagnostic(user, access, write), write && READ_ONLY ? -EPERM : (PUBLIC && !write ? 0 : -EACCES));
        }
    }
    for (auto authority : {security::Authority::KERNEL_DEBUG, security::Authority::WKI_ADMIN, security::Authority::DAC_OVERRIDE}) {
        Snapshot limited;
        ASSERT_EQ(
            ROOT.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = uint64_t{1} << static_cast<unsigned>(authority)}, limited),
            0);
        for (bool write : {false, true}) {
            EXPECT_EQ(security::authorize_diagnostic(limited, Access::KERNEL_CONTROL, write),
                      authority == security::Authority::KERNEL_DEBUG ? 0 : -EACCES);
            EXPECT_EQ(security::authorize_diagnostic(limited, Access::WKI_CONTROL, write),
                      authority == security::Authority::WKI_ADMIN ? 0 : -EACCES);
        }
        EXPECT_EQ(security::authorize_diagnostic(limited, Access::PUBLIC_READ, false), 0);
    }
}

TEST(Credentials, PrivateProcessAccessPreservesDumpabilityAndAuthorityBounds) {
    auto const ROOT = Snapshot::bootstrap_root();
    Snapshot user;
    ASSERT_EQ(ROOT.prepare({.change = Change::SET_UID, .real = 1000}, user), 0);
    EXPECT_TRUE(security::may_access_process_private(user, user, true));
    EXPECT_FALSE(security::may_access_process_private(user, user, false));
    EXPECT_FALSE(security::may_access_process_private(user, ROOT, false));
    EXPECT_TRUE(security::may_access_process_private(ROOT, user, false));
    ASSERT_EQ(user.prepare({.change = Change::SET_DUMPABLE, .dumpable = true}, user), 0);
    EXPECT_TRUE(security::may_access_process_private(user, user, false));
    Snapshot limited;
    ASSERT_EQ(ROOT.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = 0}, limited), 0);
    EXPECT_FALSE(security::may_access_process_private(limited, ROOT, false));
    EXPECT_TRUE(security::may_access_process_private(limited, limited, true));
    ASSERT_EQ(ROOT.prepare({.change = Change::NO_NEW_PRIVILEGES}, limited), 0);
    EXPECT_FALSE(security::may_access_process_private(limited, ROOT, false));
}

TEST(CredentialStore, ObservationDetectsAuthorityChangeEvenWhenSnapshotReturnsToOriginal) {
    security::CredentialStore store(Snapshot::bootstrap_root());
    auto const BEFORE = store.read();
    ASSERT_EQ(store.update({.change = Change::SET_EUID, .effective = 1000}), 0);
    ASSERT_EQ(store.update({.change = Change::SET_EUID, .effective = 0}), 0);
    ASSERT_EQ(store.update({.change = Change::SET_DUMPABLE, .dumpable = true}), 0);
    auto const AFTER = store.read();
    EXPECT_EQ(AFTER.credentials, BEFORE.credentials);
    EXPECT_GT(AFTER.generation, BEFORE.generation);
}

TEST(Credentials, RemovalRequiresParentSearchWriteAndStickyOwnership) {
    auto const ROOT = Snapshot::bootstrap_root();
    auto user = ROOT;
    std::array<uint32_t, 1> const GROUPS{3000};
    ASSERT_EQ(user.prepare({.change = Change::SET_GROUPS, .groups = GROUPS}, user), 0);
    ASSERT_EQ(user.prepare({.change = Change::SET_GID, .real = 2000}, user), 0);
    ASSERT_EQ(user.prepare({.change = Change::SET_UID, .real = 1000}, user), 0);
    for (uint32_t uid : {1000U, 4000U}) {
        for (uint32_t gid : {2000U, 3000U, 4000U}) {
            unsigned const SHIFT = uid == 1000 ? 6 : (gid == 4000 ? 0 : 3);
            for (uint32_t mode = 0; mode < 512; ++mode) {
                for (uint32_t owner : {1000U, 4000U}) {
                    for (uint32_t sticky : {0U, 01000U}) {
                        int const EXPECTED = ((mode >> SHIFT) & 3U) != 3U                  ? -EACCES
                                             : sticky != 0 && uid != 1000 && owner != 1000 ? -EPERM
                                                                                           : 0;
                        for (uint32_t type : {0100000U, 0040000U, 0120000U}) {
                            EXPECT_EQ(security::authorize_file_removal(user, {uid, gid, 0040000U | mode | sticky}, {owner, 4000, type}),
                                      EXPECTED);
                        }
                    }
                }
            }
        }
    }
    for (auto authority : {security::Authority::FILE_OWNER, security::Authority::DAC_OVERRIDE}) {
        Snapshot limited;
        ASSERT_EQ(
            ROOT.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = uint64_t{1} << static_cast<unsigned>(authority)}, limited),
            0);
        EXPECT_EQ(security::authorize_file_removal(limited, {4000, 4000, 0041777}, {4000, 4000, 0100000}),
                  authority == security::Authority::FILE_OWNER ? 0 : -EPERM);
        EXPECT_EQ(security::authorize_file_removal(limited, {4000, 4000, 0040555}, {4000, 4000, 0100000}),
                  authority == security::Authority::DAC_OVERRIDE ? 0 : -EACCES);
    }
    EXPECT_EQ(security::authorize_file_removal(ROOT, {4000, 4000, 0041555}, {4000, 4000, 0100000}), 0);
    EXPECT_EQ(security::authorize_file_removal(ROOT, {0, 0, 0100777}, {0, 0, 0100777}), -ENOTDIR);
}

TEST(Credentials, DirectoryCreationPreservesModeAndInheritsSetgidAcrossGenerations) {
    auto caller = Snapshot::bootstrap_root();
    std::array<uint32_t, 1> const GROUPS{3000};
    ASSERT_EQ(caller.prepare({.change = Change::SET_GROUPS, .groups = GROUPS}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_GID, .real = 2000}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_UID, .real = 1000}, caller), 0);
    for (uint32_t uid : {1000U, 4000U}) {
        for (uint32_t gid : {2000U, 3000U, 4000U}) {
            unsigned const SHIFT = uid == 1000 ? 6 : (gid == 4000 ? 0 : 3);
            for (uint32_t mode = 0; mode < 512; ++mode) {
                security::FileIdentity out{123, 456, 789};
                int const RET = security::prepare_directory_create(caller, {uid, gid, 0040000U | mode}, 0000, out);
                bool const ALLOWED = ((mode >> SHIFT) & 3U) == 3U;
                EXPECT_EQ(RET, ALLOWED ? 0 : -EACCES);
                EXPECT_EQ(out, ALLOWED ? (security::FileIdentity{1000, 2000, 0040000}) : (security::FileIdentity{123, 456, 789}));
            }
            for (uint32_t mode = 0; mode < 4096; ++mode) {
                security::FileIdentity out{};
                ASSERT_EQ(security::prepare_directory_create(caller, {uid, gid, 0042777}, mode, out), 0);
                EXPECT_EQ(out, (security::FileIdentity{1000, gid, 0042000U | mode}));
                security::FileIdentity grandchild{};
                ASSERT_EQ(security::prepare_directory_create(caller, {out.uid, out.gid, out.mode | 0700U}, 0750, grandchild), 0);
                EXPECT_EQ(grandchild, (security::FileIdentity{1000, gid, 0042750}));
            }
        }
    }
    security::FileIdentity out{123, 456, 789};
    EXPECT_EQ(security::prepare_directory_create(caller, {1000, 2000, 0100777}, 0755, out), -ENOTDIR);
    EXPECT_EQ(out, (security::FileIdentity{123, 456, 789}));
}

TEST(Credentials, SymlinkCreationUsesParentPermissionsAndGroupWithFixedMode) {
    auto caller = Snapshot::bootstrap_root();
    std::array<uint32_t, 1> const GROUPS{3000};
    ASSERT_EQ(caller.prepare({.change = Change::SET_GROUPS, .groups = GROUPS}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_GID, .real = 2000}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_UID, .real = 1000}, caller), 0);
    for (uint32_t uid : {1000U, 4000U}) {
        for (uint32_t gid : {2000U, 3000U, 4000U}) {
            unsigned const SHIFT = uid == 1000 ? 6 : (gid == 4000 ? 0 : 3);
            for (uint32_t mode = 0; mode < 4096; ++mode) {
                security::FileIdentity out{123, 456, 789};
                int const RET = security::prepare_symlink_create(caller, {uid, gid, 0040000U | mode}, out);
                bool const ALLOWED = ((mode >> SHIFT) & 3U) == 3U;
                EXPECT_EQ(RET, ALLOWED ? 0 : -EACCES);
                EXPECT_EQ(out, ALLOWED ? (security::FileIdentity{1000, (mode & 02000U) != 0 ? gid : 2000, 0120777})
                                       : (security::FileIdentity{123, 456, 789}));
            }
        }
    }
    security::FileIdentity out{123, 456, 789};
    EXPECT_EQ(security::prepare_symlink_create(caller, {1000, 2000, 0100777}, out), -ENOTDIR);
    EXPECT_EQ(out, (security::FileIdentity{123, 456, 789}));
}

TEST(Credentials, HardlinkParentsAndSourceUseOneIdentityAndProtectForeignAuthority) {
    auto caller = Snapshot::bootstrap_root();
    std::array<uint32_t, 1> const GROUPS{3000};
    ASSERT_EQ(caller.prepare({.change = Change::SET_GROUPS, .groups = GROUPS}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_GID, .real = 2000}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_UID, .real = 1000}, caller), 0);
    security::FileIdentity const PUBLIC{0, 0, 0040777};
    security::FileIdentity const OWNED{1000, 4000, 0100000};
    for (uint32_t uid : {1000U, 4000U}) {
        for (uint32_t gid : {2000U, 3000U, 4000U}) {
            unsigned const SHIFT = uid == 1000 ? 6 : (gid == 4000 ? 0 : 3);
            for (uint32_t mode = 0; mode < 512; ++mode) {
                security::FileIdentity const PARENT{uid, gid, 0040000U | mode};
                EXPECT_EQ(security::authorize_file_link(caller, PARENT, PUBLIC, OWNED), ((mode >> SHIFT) & 1U) ? 0 : -EACCES);
                EXPECT_EQ(security::authorize_file_link(caller, PUBLIC, PARENT, OWNED), ((mode >> SHIFT) & 3U) == 3 ? 0 : -EACCES);
            }
        }
        for (uint32_t type : {0100000U, 0120000U, 0020000U, 0040000U}) {
            for (uint32_t mode = 0; mode < 4096; ++mode) {
                bool const SAFE_FOREIGN = type == 0100000 && (mode & 06000U) == 0 && (mode & 0060U) == 0060U;
                bool const ALLOWED = type != 0040000 && (uid == 1000 || SAFE_FOREIGN);
                EXPECT_EQ(security::authorize_file_link(caller, PUBLIC, PUBLIC, {uid, 3000, type | mode}), ALLOWED ? 0 : -EPERM);
            }
        }
    }
    EXPECT_EQ(security::authorize_file_link(Snapshot::bootstrap_root(), PUBLIC, PUBLIC, {4000, 4000, 0104755}), 0);
    EXPECT_EQ(security::authorize_file_link(caller, {0, 0, 0100777}, PUBLIC, OWNED), -ENOTDIR);
    EXPECT_EQ(security::authorize_file_link(caller, PUBLIC, {0, 0, 0100777}, OWNED), -ENOTDIR);
    EXPECT_EQ(security::authorize_file_link(caller, {0, 0, 0040666}, PUBLIC, {0, 0, 0104755}), -EACCES);
}

TEST(Credentials, RenameAuthorizesBothParentsAndEachStickyEntryBeforeDirectoryReparenting) {
    auto caller = Snapshot::bootstrap_root();
    std::array<uint32_t, 1> const GROUPS{3000};
    ASSERT_EQ(caller.prepare({.change = Change::SET_GROUPS, .groups = GROUPS}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_GID, .real = 2000}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_UID, .real = 1000}, caller), 0);
    security::FileIdentity const PUBLIC{0, 0, 0040777};
    security::FileIdentity const OWNED{1000, 4000, 0100000};
    for (uint32_t uid : {1000U, 4000U}) {
        for (uint32_t gid : {2000U, 3000U, 4000U}) {
            unsigned const SHIFT = uid == 1000 ? 6 : (gid == 4000 ? 0 : 3);
            for (uint32_t mode = 0; mode < 512; ++mode) {
                for (uint32_t sticky : {0U, 01000U}) {
                    security::FileIdentity const PARENT{uid, gid, 0040000U | mode | sticky};
                    for (uint32_t owner : {1000U, 4000U}) {
                        security::FileIdentity const ENTRY{owner, 4000, 0100000};
                        int const EXPECTED = ((mode >> SHIFT) & 3U) != 3U                  ? -EACCES
                                             : sticky != 0 && uid != 1000 && owner != 1000 ? -EPERM
                                                                                           : 0;
                        EXPECT_EQ(security::authorize_file_rename(caller, PARENT, ENTRY, PUBLIC, nullptr, true), EXPECTED);
                        EXPECT_EQ(security::authorize_file_rename(caller, PUBLIC, OWNED, PARENT, &ENTRY, true), EXPECTED);
                    }
                    EXPECT_EQ(security::authorize_file_rename(caller, PUBLIC, OWNED, PARENT, nullptr, true),
                              ((mode >> SHIFT) & 3U) == 3U ? 0 : -EACCES);
                }
                security::FileIdentity const DIRECTORY{uid, gid, 0040000U | mode};
                EXPECT_EQ(security::authorize_file_rename(caller, PUBLIC, DIRECTORY, PUBLIC, nullptr, true),
                          ((mode >> SHIFT) & 2U) != 0 ? 0 : -EACCES);
                EXPECT_EQ(security::authorize_file_rename(caller, PUBLIC, DIRECTORY, PUBLIC, nullptr, false), 0);
            }
        }
    }
    security::FileIdentity const FOREIGN{4000, 4000, 0100000};
    EXPECT_EQ(security::authorize_file_rename(caller, {0, 0, 0040555}, OWNED, {0, 0, 0041777}, &FOREIGN, true), -EACCES);
    EXPECT_EQ(security::authorize_file_rename(caller, {0, 0, 0041777}, FOREIGN, {0, 0, 0040555}, nullptr, true), -EPERM);
    EXPECT_EQ(
        security::authorize_file_rename(Snapshot::bootstrap_root(), {4000, 4000, 0041555}, FOREIGN, {4000, 4000, 0041555}, &FOREIGN, true),
        0);
    EXPECT_EQ(security::authorize_file_rename(caller, {0, 0, 0100777}, OWNED, PUBLIC, nullptr, true), -ENOTDIR);
    EXPECT_EQ(security::authorize_file_rename(caller, PUBLIC, OWNED, {0, 0, 0100777}, nullptr, true), -ENOTDIR);
}

TEST(Credentials, DirectorySearchOverrideDoesNotGrantRegularExecuteOrDiscardedAuthority) {
    auto const ROOT = Snapshot::bootstrap_root();
    auto limited = ROOT;
    ASSERT_EQ(ROOT.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = 0}, limited), 0);
    auto caller = ROOT;
    ASSERT_EQ(caller.prepare({.change = Change::SET_GID, .real = 2000}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_UID, .real = 1000}, caller), 0);
    for (uint32_t mode = 0; mode < 512; ++mode) {
        EXPECT_EQ(security::authorize_file_access(ROOT, {3000, 3000, 0040000U | mode}, 1), 0);
        EXPECT_EQ(security::authorize_file_access(ROOT, {3000, 3000, 0100000U | mode}, 1), (mode & 0111U) != 0 ? 0 : -EACCES);
        EXPECT_EQ(security::authorize_file_access(limited, {3000, 3000, 0040000U | mode}, 1), (mode & 1U) != 0 ? 0 : -EACCES);
        EXPECT_EQ(security::authorize_file_access(caller, {1000, 3000, 0040000U | mode}, 1), (mode & 0100U) != 0 ? 0 : -EACCES);
        EXPECT_EQ(security::authorize_file_access(caller, {3000, 2000, 0040000U | mode}, 1), (mode & 0010U) != 0 ? 0 : -EACCES);
        EXPECT_EQ(security::authorize_file_access(caller, {3000, 3000, 0040000U | mode}, 1), (mode & 0001U) != 0 ? 0 : -EACCES);
    }
}

TEST(Credentials, LegacyRemoteExecCannotEraseIrreversibleRestrictions) {
    auto const root = Snapshot::bootstrap_root();
    EXPECT_EQ(security::authorize_legacy_remote_exec(root), 0);
    EXPECT_EQ(security::authorize_legacy_remote_exec(Snapshot{}), -ENOTSUP);
    Snapshot changed;
    ASSERT_EQ(root.prepare({.change = Change::NO_NEW_PRIVILEGES}, changed), 0);
    EXPECT_EQ(security::authorize_legacy_remote_exec(changed), -ENOTSUP);
    ASSERT_EQ(root.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = security::ALL_AUTHORITY & ~uint64_t{1}}, changed), 0);
    EXPECT_EQ(security::authorize_legacy_remote_exec(changed), -ENOTSUP);
    ASSERT_EQ(root.prepare({.change = Change::SET_UID, .real = 1000}, changed), 0);
    EXPECT_EQ(security::authorize_legacy_remote_exec(changed), 0);
}

TEST(Credentials, LegacyRemoteExecCannotDiscardGroupsAndSelectMorePermissiveOtherBits) {
    auto caller = Snapshot::bootstrap_root();
    constexpr std::array<uint32_t, 1> GROUPS{3000};
    ASSERT_EQ(caller.prepare({.change = Change::SET_GROUPS, .groups = GROUPS}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_GID, .real = 2000}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_UID, .real = 1000}, caller), 0);
    auto receiver = Snapshot::bootstrap_root();
    ASSERT_EQ(receiver.prepare({.change = Change::SET_GID, .real = 2000}, receiver), 0);
    ASSERT_EQ(receiver.prepare({.change = Change::SET_UID, .real = 1000}, receiver), 0);
    // Both file and IPC DAC select the first matching class, not a union.
    EXPECT_EQ(security::authorize_file_access(caller, {4000, 3000, 0100004}, 4), -EACCES);
    EXPECT_EQ(security::authorize_file_access(receiver, {4000, 3000, 0100004}, 4), 0);
    security::IpcOwnership const OBJECT{4000, 3000, 4000, 3000, 0004};
    EXPECT_FALSE(security::may_access_ipc(caller, OBJECT, 4));
    EXPECT_TRUE(security::may_access_ipc(receiver, OBJECT, 4));
    EXPECT_EQ(security::authorize_legacy_remote_exec(caller), -ENOTSUP);
    EXPECT_EQ(security::authorize_legacy_remote_exec(receiver), 0);
}

TEST(Credentials, LegacyRemoteExecPreservesBothDumpabilityStates) {
    auto caller = Snapshot::bootstrap_root();
    ASSERT_EQ(caller.prepare({.change = Change::SET_DUMPABLE, .dumpable = false}, caller), 0);
    EXPECT_EQ(security::authorize_legacy_remote_exec(caller), -ENOTSUP);
    ASSERT_EQ(caller.prepare({.change = Change::SET_UID, .real = 1000}, caller), 0);
    EXPECT_EQ(security::authorize_legacy_remote_exec(caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_DUMPABLE, .dumpable = true}, caller), 0);
    EXPECT_EQ(security::authorize_legacy_remote_exec(caller), -ENOTSUP);
}

TEST(Credentials, LegacyRemoteExecRoundTripsEveryIdSlotAndRejectsEverySentinelWithoutMutation) {
    for (unsigned mask = 0; mask < 64; ++mask) {
        std::array<uint32_t, 6> ids{};
        for (unsigned slot = 0; slot < ids.size(); ++slot) ids[slot] = (mask & (1U << slot)) != 0 ? 1000 + slot : 0;
        auto caller = Snapshot::bootstrap_root();
        ASSERT_EQ(caller.prepare({.change = Change::SET_RESGID, .real = ids[3], .effective = ids[4], .saved = ids[5]}, caller), 0);
        ASSERT_EQ(caller.prepare({.change = Change::SET_RESUID, .real = ids[0], .effective = ids[1], .saved = ids[2]}, caller), 0);
        Snapshot restored;
        ASSERT_EQ(security::restore_legacy_remote_exec_identity(caller.user(), caller.group(), restored), 0);
        EXPECT_EQ(restored, caller);
        EXPECT_EQ(security::authorize_legacy_remote_exec(caller), 0);
        auto const BEFORE = caller;
        ASSERT_EQ(caller.prepare({.change = Change::SET_DUMPABLE, .dumpable = !caller.dumpable()}, caller), 0);
        EXPECT_EQ(security::authorize_legacy_remote_exec(caller), -ENOTSUP);
        for (unsigned slot = 0; slot < ids.size(); ++slot) {
            auto invalid = ids;
            invalid[slot] = security::UNCHANGED_ID;
            EXPECT_EQ(security::restore_legacy_remote_exec_identity({invalid[0], invalid[1], invalid[2]},
                                                                    {invalid[3], invalid[4], invalid[5]}, restored),
                      -EINVAL);
            EXPECT_EQ(restored, BEFORE);
        }
    }
}

TEST(Credentials, NetworkAdministrationRequiresEffectiveRootAndItsOwnAuthority) {
    auto const ROOT = security::Snapshot::bootstrap_root();
    EXPECT_EQ(security::authorize_network_administration(ROOT), 0);
    EXPECT_EQ(security::authorize_network_administration(security::Snapshot{}), -EPERM);
    auto caller = ROOT;
    ASSERT_EQ(caller.prepare({.change = security::Change::SET_EUID, .effective = 1000}, caller), 0);
    EXPECT_EQ(security::authorize_network_administration(caller), -EPERM);
    ASSERT_EQ(caller.prepare({.change = security::Change::SET_EUID, .effective = 0}, caller), 0);
    constexpr auto BIT = uint64_t{1} << static_cast<unsigned>(security::Authority::NETWORK_ADMIN);
    ASSERT_EQ(caller.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = security::ALL_AUTHORITY & ~BIT}, caller), 0);
    EXPECT_EQ(security::authorize_network_administration(caller), -EPERM);
    caller = ROOT;
    ASSERT_EQ(caller.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = BIT}, caller), 0);
    EXPECT_EQ(security::authorize_network_administration(caller), 0);
}

TEST(Credentials, ServiceResourceManagementRequiresInitRoleAndBothAuthorities) {
    auto const ROOT = Snapshot::bootstrap_root();
    EXPECT_EQ(security::authorize_service_resource_management(ROOT, true), 0);
    EXPECT_EQ(security::authorize_service_resource_management(ROOT, false), -EPERM);
    constexpr auto INIT = uint64_t{1} << static_cast<unsigned>(Authority::INIT_CONTROL);
    constexpr auto LIMIT = uint64_t{1} << static_cast<unsigned>(Authority::RESOURCE_LIMIT);
    for (auto ceiling : std::array<uint64_t, 5>{0, INIT, LIMIT, security::ALL_AUTHORITY & ~INIT, security::ALL_AUTHORITY & ~LIMIT}) {
        auto caller = ROOT;
        ASSERT_EQ(caller.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = ceiling}, caller), 0);
        EXPECT_EQ(security::authorize_service_resource_management(caller, true), -EPERM);
    }
    auto caller = ROOT;
    ASSERT_EQ(caller.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = INIT | LIMIT}, caller), 0);
    EXPECT_EQ(security::authorize_service_resource_management(caller, true), 0);
    caller = ROOT;
    ASSERT_EQ(caller.prepare({.change = Change::SET_EUID, .effective = 1000}, caller), 0);
    EXPECT_EQ(security::authorize_service_resource_management(caller, true), -EPERM);
    ASSERT_EQ(caller.prepare({.change = Change::SET_EUID, .effective = 0}, caller), 0);
    EXPECT_EQ(security::authorize_service_resource_management(caller, true), 0);
}

TEST(Credentials, ServicePrivilegeAssignmentIsBoundedAuthorizedAndSealed) {
    auto root = Snapshot::bootstrap_root();
    Snapshot next;
    EXPECT_EQ(root.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 1}, next), -EPERM);
    EXPECT_EQ(next, root);
    ASSERT_EQ(root.prepare({.change = Change::NO_NEW_PRIVILEGES}, root), 0);
    for (auto invalid : std::array<uint64_t, 3>{4, 256, UINT64_MAX}) {
        EXPECT_EQ(root.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = invalid}, next), -EINVAL);
        EXPECT_EQ(next, root);
    }
    for (auto authority : {Authority::INIT_CONTROL, Authority::KERNEL_DEBUG, Authority::NETWORK_ADMIN}) {
        auto restricted = root;
        ASSERT_EQ(restricted.prepare({.change = Change::DROP_AUTHORITY,
                                      .authority_ceiling = security::ALL_AUTHORITY & ~(uint64_t{1} << static_cast<unsigned>(authority))},
                                     restricted),
                  0);
        EXPECT_EQ(restricted.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, next), -EPERM);
        EXPECT_EQ(next, restricted);
    }
    auto nonroot = root;
    ASSERT_EQ(nonroot.prepare({.change = Change::SET_UID, .real = 1000}, nonroot), 0);
    EXPECT_EQ(nonroot.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 1}, next), -EPERM);
    EXPECT_EQ(next, nonroot);
    ASSERT_EQ(nonroot.prepare({.change = Change::SET_SERVICE_PRIVILEGES}, nonroot), 0);
    EXPECT_TRUE(nonroot.service_privileges_locked());
    ASSERT_EQ(root.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, root), 0);
    EXPECT_TRUE(root.service_privileges_locked());
    EXPECT_FALSE(root.dumpable());
    ASSERT_EQ(root.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, root), 0);
    ASSERT_EQ(root.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 1}, root), 0);
    EXPECT_EQ(root.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, next), -EPERM);
    EXPECT_EQ(next, root);
    ASSERT_EQ(root.prepare({.change = Change::SET_SERVICE_PRIVILEGES}, root), 0);
    EXPECT_EQ(root.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 1}, next), -EPERM);
    EXPECT_EQ(next, root);
}

TEST(Credentials, ServicePrivilegesSurviveIdentityAndExecButNeverBroadenOperations) {
    for (uint64_t mask : {1U, 2U, 3U}) {
        auto caller = Snapshot::bootstrap_root();
        ASSERT_EQ(caller.prepare({.change = Change::NO_NEW_PRIVILEGES}, caller), 0);
        ASSERT_EQ(caller.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = mask}, caller), 0);
        ASSERT_EQ(caller.prepare({.change = Change::SET_GID, .real = 1000}, caller), 0);
        ASSERT_EQ(caller.prepare({.change = Change::SET_UID, .real = 1000}, caller), 0);
        auto const CEILING = ker::abi::process::service_privilege_ceiling(mask);
        ASSERT_EQ(caller.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = CEILING}, caller), 0);
        EXPECT_EQ(security::authorize_journal_read(caller), (mask & 1) != 0 ? 0 : -EACCES);
        EXPECT_EQ(security::authorize_network_administration(caller), (mask & 2) != 0 ? 0 : -EPERM);
        EXPECT_EQ(security::authorize_diagnostic(caller, security::DiagnosticAccess::KERNEL_READ, false), -EACCES);
        EXPECT_EQ(security::authorize_mount_administration(caller), -EPERM);
        EXPECT_EQ(security::authorize_legacy_remote_exec(caller), -ENOTSUP);
        for (unsigned bit = 0; bit < 256; ++bit) {
            EXPECT_FALSE(caller.allows(static_cast<Authority>(bit)));
            EXPECT_EQ(caller.allows_service(static_cast<security::ServicePrivilege>(bit)), bit < 2 && (mask & (uint64_t{1} << bit)) != 0);
        }
        auto const CHILD = caller;
        Snapshot executed;
        ASSERT_EQ(CHILD.prepare_exec({.uid = 0, .gid = 0, .set_uid = true, .set_gid = true, .mode = 0755}, executed), 0);
        EXPECT_EQ(executed.user(), caller.user());
        EXPECT_EQ(executed.group(), caller.group());
        EXPECT_EQ(executed.service_privileges(), mask);
        EXPECT_TRUE(executed.service_privileges_locked());
        EXPECT_EQ(executed.effective_service_privileges(), mask);
        Snapshot denied;
        EXPECT_EQ(executed.prepare({.change = Change::SET_UID, .real = 0}, denied), -EPERM);
        EXPECT_EQ(denied, executed);
    }
}

TEST(Credentials, CeilingDropPermanentlyDisablesDelegatedPermission) {
    auto caller = Snapshot::bootstrap_root();
    ASSERT_EQ(caller.prepare({.change = Change::NO_NEW_PRIVILEGES}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_UID, .real = 1000}, caller), 0);
    ASSERT_EQ(
        caller.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = ker::abi::process::service_privilege_ceiling(1)}, caller),
        0);
    EXPECT_EQ(security::authorize_network_administration(caller), -EPERM);
    EXPECT_EQ(security::authorize_journal_read(caller), 0);
    ASSERT_EQ(caller.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, caller), 0);
    EXPECT_EQ(security::authorize_network_administration(caller), -EPERM);
    ASSERT_EQ(caller.prepare({.change = Change::DROP_AUTHORITY}, caller), 0);
    EXPECT_EQ(security::authorize_journal_read(caller), -EACCES);
    EXPECT_EQ(caller.effective_service_privileges(), 0U);
    Snapshot denied;
    EXPECT_EQ(caller.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = security::ALL_AUTHORITY}, denied), -EPERM);
    EXPECT_EQ(denied, caller);
}

TEST(Credentials, TraceAndPrivateDescriptorsCannotRecoverServicePrivilegesOrInstaller) {
    auto base = Snapshot::bootstrap_root();
    ASSERT_EQ(base.prepare({.change = Change::NO_NEW_PRIVILEGES}, base), 0);
    auto installer = base;
    auto target = base;
    ASSERT_EQ(target.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, target), 0);
    auto caller = target;
    EXPECT_FALSE(security::may_trace(caller, installer));
    EXPECT_FALSE(security::may_access_process_private(caller, installer, false));
    EXPECT_TRUE(security::may_trace(installer, caller));
    ASSERT_EQ(target.prepare({.change = Change::SET_UID, .real = 1000}, target), 0);
    ASSERT_EQ(target.prepare({.change = Change::SET_DUMPABLE, .dumpable = true}, target), 0);
    caller = target;
    EXPECT_TRUE(security::may_trace(caller, target));
    ASSERT_EQ(caller.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 1}, caller), 0);
    EXPECT_FALSE(security::may_trace(caller, target));
    EXPECT_FALSE(security::may_access_process_private(caller, target, false));
    EXPECT_TRUE(security::may_access_process_private(caller, target, true));
    ASSERT_EQ(target.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 1}, target), 0);
    ASSERT_EQ(target.prepare({.change = Change::SET_DUMPABLE, .dumpable = true}, target), 0);
    EXPECT_TRUE(security::may_trace(caller, target));
}

TEST(CredentialStore, ConcurrentFirstServiceGrantsPublishOnlyOneSealedSet) {
    for (unsigned round = 0; round < 64; ++round) {
        auto root = Snapshot::bootstrap_root();
        ASSERT_EQ(root.prepare({.change = Change::NO_NEW_PRIVILEGES}, root), 0);
        security::CredentialStore store(root);
        std::atomic<unsigned> ready{};
        std::array<int, 2> results{};
        auto grant = [&](size_t index) {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (ready.load(std::memory_order_acquire) != 2) std::this_thread::yield();
            results[index] = store.update({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = index + 1});
        };
        std::thread first(grant, 0), second(grant, 1);
        first.join();
        second.join();
        EXPECT_TRUE((results[0] == 0 && results[1] == -EPERM) || (results[0] == -EPERM && results[1] == 0));
        auto const OBSERVED = store.read();
        EXPECT_EQ(OBSERVED.generation, 2U);
        EXPECT_EQ(OBSERVED.credentials.service_privileges(), results[0] == 0 ? 1U : 2U);
        EXPECT_TRUE(OBSERVED.credentials.service_privileges_locked());
        EXPECT_FALSE(OBSERVED.credentials.dumpable());
        EXPECT_EQ(OBSERVED.credentials.user(), root.user());
        EXPECT_EQ(store.update({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}), -EPERM);
        EXPECT_EQ(store.read().generation, OBSERVED.generation);
    }
}

TEST(CredentialRecord, CanonicalBytesPreserveEveryField) {
    namespace record = security::credential_record;
    std::array<uint32_t, 2> groups{0x11223344, 0x55667788};
    Snapshot original;
    ASSERT_EQ(Snapshot::restore_fields({.user = {1, 2, 3},
                                        .group = {4, 5, 6},
                                        .groups = groups,
                                        .authority_ceiling = 0x1234,
                                        .service_privileges = 3,
                                        .no_new_privileges = true,
                                        .dumpable = true,
                                        .service_privileges_locked = true},
                                       original),
              0);
    record::Record const expected{
        1, 0, 80, 0, 7, 2,    0,    0,    1,    0,    0,    0,    2,    0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0,
        0, 6, 0,  0, 0, 0x44, 0x33, 0x22, 0x11, 0x88, 0x77, 0x66, 0x55, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0,  0, 0, 0,    0,    0,    0,    0,    0x34, 0x12, 0,    0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
    };
    EXPECT_EQ(record::encode(original), expected);
    Snapshot decoded = Snapshot::bootstrap_root();
    ASSERT_EQ(record::decode(expected, decoded), 0);
    EXPECT_EQ(decoded, original);
    EXPECT_EQ(security::authorize_legacy_remote_exec(decoded), -ENOTSUP);
    EXPECT_EQ(security::map_remote_identity(decoded, security::RemoteIdentityTrust::DENY, decoded), -EACCES);
    EXPECT_EQ(decoded, original);
}

TEST(CredentialRecord, RejectsMalformedInputWithoutChangingOutput) {
    namespace record = security::credential_record;
    auto const valid = record::encode(Snapshot::bootstrap_root());
    Snapshot const before;
    Snapshot output = before;
    for (size_t size = 0; size < record::SIZE; ++size) {
        EXPECT_EQ(record::decode({valid.data(), size}, output), -EINVAL);
        EXPECT_EQ(output, before);
    }
    std::array<uint8_t, record::SIZE + 1> oversized{};
    std::copy(valid.begin(), valid.end(), oversized.begin());
    EXPECT_EQ(record::decode(oversized, output), -EINVAL);
    EXPECT_EQ(output, before);
    auto reject = [&](record::Record bytes, int expected = -EINVAL) {
        EXPECT_EQ(record::decode(bytes, output), expected);
        EXPECT_EQ(output, before);
    };
    for (size_t offset : {0U, 1U}) {
        auto bytes = valid;
        bytes[offset] ^= 0x80;
        reject(bytes, -ENOTSUP);
    }
    for (size_t offset : {2U, 3U, 6U, 7U}) {
        auto bytes = valid;
        bytes[offset] ^= 1;
        reject(bytes);
    }
    for (unsigned bit = 3; bit < 8; ++bit) {
        auto bytes = valid;
        bytes[4] |= 1U << bit;
        reject(bytes);
    }
    for (unsigned count = 9; count < 256; ++count) {
        auto bytes = valid;
        bytes[5] = count;
        reject(bytes);
    }
    for (size_t offset = 8; offset < 32; offset += 4) {
        auto bytes = valid;
        for (size_t i = 0; i < 4; ++i) bytes[offset + i] = 0xff;
        reject(bytes);
    }
    for (size_t slot = 0; slot < security::MAX_GROUPS; ++slot) {
        auto bytes = valid;
        bytes[32 + slot * 4] = 1;  // Unused groups must be zero.
        reject(bytes);
        bytes[5] = 8;
        for (size_t i = 0; i < 4; ++i) bytes[32 + slot * 4 + i] = 0xff;
        reject(bytes);
    }
    for (unsigned bit = 16; bit < 64; ++bit) {
        auto bytes = valid;
        bytes[64 + bit / 8] |= 1U << (bit % 8);
        reject(bytes);
    }
    for (unsigned bit = 2; bit < 64; ++bit) {
        auto bytes = valid;
        bytes[4] = 7;
        bytes[72 + bit / 8] |= 1U << (bit % 8);
        reject(bytes);
    }
    auto bytes = valid;
    bytes[4] = record::SERVICE_LOCKED;  // Cannot seal grants without NNP.
    reject(bytes);
    bytes[4] = record::NO_NEW_PRIVILEGES;
    bytes[72] = 1;  // Grants must be sealed.
    reject(bytes);
}

TEST(CredentialRecord, RoundTripDoesNotRecoverDiscardedAuthority) {
    namespace record = security::credential_record;
    auto service = Snapshot::bootstrap_root();
    std::array<uint32_t, security::MAX_GROUPS> groups{10, 20, 30, 40, 50, 60, 70, UINT32_MAX - 1};
    ASSERT_EQ(service.prepare({.change = Change::SET_GROUPS, .groups = groups}, service), 0);
    ASSERT_EQ(service.prepare({.change = Change::NO_NEW_PRIVILEGES}, service), 0);
    ASSERT_EQ(service.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, service), 0);
    ASSERT_EQ(service.prepare({.change = Change::SET_RESGID, .real = 100, .effective = 101, .saved = 102}, service), 0);
    ASSERT_EQ(service.prepare({.change = Change::SET_RESUID, .real = 200, .effective = 201, .saved = 202}, service), 0);
    ASSERT_EQ(service.prepare({.change = Change::DROP_AUTHORITY}, service), 0);
    Snapshot decoded;
    ASSERT_EQ(record::decode(record::encode(service), decoded), 0);
    ASSERT_EQ(decoded, service);
    EXPECT_EQ(decoded.prepare({.change = Change::SET_EUID, .effective = 0}, decoded), -EPERM);
    EXPECT_EQ(decoded.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = security::ALL_AUTHORITY}, decoded), -EPERM);
    EXPECT_EQ(security::authorize_journal_read(decoded), -EACCES);
    EXPECT_EQ(security::authorize_network_administration(decoded), -EPERM);
    EXPECT_EQ(decoded, service);
    ASSERT_EQ(decoded.prepare({.change = Change::SET_SERVICE_PRIVILEGES}, decoded), 0);
    EXPECT_EQ(decoded.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, decoded), -EPERM);
}

TEST(CredentialRecord, RestoredFieldsAreCopiedAndErrorsDoNotPublish) {
    Snapshot const before = Snapshot::bootstrap_root();
    Snapshot output = before;
    std::array<uint32_t, security::MAX_GROUPS + 1> excessive{};
    EXPECT_EQ(Snapshot::restore_fields({.groups = excessive}, output), -EINVAL);
    EXPECT_EQ(output, before);
    std::array<uint32_t, 2> groups{10, 20};
    ASSERT_EQ(Snapshot::restore_fields({.groups = groups}, output), 0);
    groups[0] = 0;
    EXPECT_EQ(output.groups()[0], 10U);
    EXPECT_FALSE(output.has_group(0));
    for (auto original : {Snapshot{}, before, output}) {
        Snapshot decoded;
        ASSERT_EQ(security::credential_record::decode(security::credential_record::encode(original), decoded), 0);
        EXPECT_EQ(decoded, original);
    }
}

TEST(Credentials, ExecAuxvTracksFinalIdsAndSuppressionWithoutTreatingDumpabilityAsAuthority) {
    auto original = Snapshot::bootstrap_root();
    ASSERT_EQ(original.prepare({.change = Change::SET_GID, .real = 2002}, original), 0);
    ASSERT_EQ(original.prepare({.change = Change::SET_UID, .real = 1002}, original), 0);
    for (unsigned suppression = 0; suppression < 8; ++suppression) {
        auto caller = original;
        if (suppression & 1) ASSERT_EQ(caller.prepare({.change = Change::NO_NEW_PRIVILEGES}, caller), 0);
        for (unsigned bits = 0; bits < 4; ++bits) {
            Snapshot final;
            ASSERT_EQ(caller.prepare_exec({.uid = 1003,
                                           .gid = 2003,
                                           .set_uid = (bits & 1) != 0,
                                           .set_gid = (bits & 2) != 0,
                                           .nosuid = (suppression & 2) != 0,
                                           .traced = (suppression & 4) != 0,
                                           .mode = 0755},
                                          final),
                      0);
            uint64_t const uid = !suppression && (bits & 1) ? 1003 : 1002;
            uint64_t const gid = !suppression && (bits & 2) ? 2003 : 2002;
            std::array<uint64_t, 10> const expected{11, 1002, 12, uid, 13, 2002, 14, gid, 23, !suppression && bits != 0};
            EXPECT_EQ(security::exec_policy::identity_auxv(final), expected);
            ASSERT_EQ(final.prepare({.change = Change::SET_DUMPABLE, .dumpable = false}, final), 0);
            EXPECT_EQ(security::exec_policy::identity_auxv(final), expected);
        }
    }
    EXPECT_FALSE(security::exec_policy::requires_secure_loader(Snapshot::bootstrap_root()));
    auto differing = Snapshot::bootstrap_root();
    ASSERT_EQ(differing.prepare({.change = Change::SET_RESUID, .real = 1002, .effective = 1003, .saved = 1000}, differing), 0);
    ASSERT_EQ(differing.prepare({.change = Change::NO_NEW_PRIVILEGES}, differing), 0);
    Snapshot inherited;
    ASSERT_EQ(differing.prepare_exec({}, inherited), 0);
    EXPECT_TRUE(security::exec_policy::requires_secure_loader(inherited));
    EXPECT_EQ(inherited.user(), (Ids{1002, 1003, 1003}));
}

TEST(Credentials, ExecAuxvRequiresSecureLoaderForEffectiveDeclaredServiceGrants) {
    for (uint64_t mask : {1ULL, 2ULL, 3ULL}) {
        auto service = Snapshot::bootstrap_root();
        ASSERT_EQ(service.prepare({.change = Change::NO_NEW_PRIVILEGES}, service), 0);
        ASSERT_EQ(service.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = mask}, service), 0);
        ASSERT_EQ(service.prepare({.change = Change::SET_GID, .real = 2002}, service), 0);
        ASSERT_EQ(service.prepare({.change = Change::SET_UID, .real = 1002}, service), 0);
        Snapshot final;
        ASSERT_EQ(service.prepare_exec({}, final), 0);
        EXPECT_TRUE(security::exec_policy::requires_secure_loader(final));
        EXPECT_EQ(security::exec_policy::identity_auxv(final)[9], 1u);
        ASSERT_EQ(final.prepare({.change = Change::DROP_AUTHORITY, .authority_ceiling = 0}, final), 0);
        EXPECT_FALSE(security::exec_policy::requires_secure_loader(final));
        ASSERT_EQ(service.prepare({.change = Change::SET_SERVICE_PRIVILEGES, .service_privileges = 0}, service), 0);
        EXPECT_FALSE(security::exec_policy::requires_secure_loader(service));
    }
}

TEST(Credentials, RealIdAccessProjectionPreservesCallerAndRestrictions) {
    std::array<uint32_t, 2> const groups{2000, 3000};
    for (bool real_root : {false, true}) {
        for (bool restricted : {false, true}) {
            Snapshot caller;
            auto const user = real_root ? Ids{0, 1000, 1001} : Ids{1000, 0, 1001};
            ASSERT_EQ(Snapshot::restore_fields({.user = user,
                                                .group = {2000, 2001, 2002},
                                                .groups = groups,
                                                .authority_ceiling = restricted ? 0 : security::ALL_AUTHORITY,
                                                .service_privileges = 3,
                                                .no_new_privileges = true,
                                                .dumpable = false,
                                                .service_privileges_locked = true},
                                               caller),
                      0);
            auto const before = caller;
            auto const decision = caller.for_real_id_access();
            EXPECT_EQ(caller, before);
            EXPECT_EQ(decision.user(), (Ids{user.real, user.real, user.saved}));
            EXPECT_EQ(decision.group(), (Ids{2000, 2000, 2002}));
            EXPECT_EQ(decision.groups().size(), groups.size());
            for (size_t i = 0; i < groups.size(); ++i) EXPECT_EQ(decision.groups()[i], groups[i]);
            EXPECT_EQ(decision.authority_ceiling(), caller.authority_ceiling());
            EXPECT_EQ(decision.no_new_privileges(), caller.no_new_privileges());
            EXPECT_EQ(decision.dumpable(), caller.dumpable());
            EXPECT_EQ(decision.service_privileges(), caller.service_privileges());
            EXPECT_EQ(decision.service_privileges_locked(), caller.service_privileges_locked());
            EXPECT_EQ(decision.allows(Authority::DAC_OVERRIDE), real_root && !restricted);
            EXPECT_EQ(security::authorize_file_access(decision, {4000, 2000, 0040}, 4), 0);
            EXPECT_EQ(security::authorize_file_access(decision, {4000, 3000, 0040}, 4), 0);
            EXPECT_EQ(security::authorize_file_access(decision, {4000, 2001, 0040}, 4), real_root && !restricted ? 0 : -EACCES);
        }
    }
}
