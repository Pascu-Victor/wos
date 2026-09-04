#include <gtest/gtest.h>

#include <vfs/lookup_model.hpp>

namespace {

namespace model = ker::vfs::lookup_model;

struct FixtureModel {
    model::Model state{};
    model::NodeIdentity root{};
    model::NodeIdentity directory{};
    model::NodeIdentity file{};

    FixtureModel() {
        EXPECT_EQ(model::install_mount(state, 0), model::Result::OK);
        EXPECT_EQ(model::create_node(state, 0, 0, model::NodeKind::DIRECTORY, &root), model::Result::OK);
        EXPECT_EQ(model::create_node(state, 0, 1, model::NodeKind::DIRECTORY, &directory), model::Result::OK);
        EXPECT_EQ(model::create_node(state, 0, 2, model::NodeKind::FILE, &file), model::Result::OK);
        EXPECT_EQ(model::bind(state, root, 1, directory), model::Result::OK);
        EXPECT_EQ(model::bind(state, directory, 2, file), model::Result::OK);
    }
};

TEST(VfsLookupModel, RetainedObjectSurvivesUnlinkButBindingGenerationRejectsCommit) {
    FixtureModel fixture;
    model::LookupHandle root_handle{};
    model::LookupHandle directory_handle{};
    model::LookupHandle file_handle{};
    ASSERT_EQ(model::acquire_mount_root(fixture.state, 0, root_handle), model::Result::OK);
    ASSERT_EQ(model::lookup_child(fixture.state, root_handle, 1, directory_handle), model::Result::OK);
    ASSERT_EQ(model::lookup_child(fixture.state, directory_handle, 2, file_handle), model::Result::OK);

    EXPECT_EQ(model::unlink_binding(fixture.state, fixture.directory, 2), model::Result::OK);
    EXPECT_EQ(model::validate_handle(fixture.state, file_handle, model::ValidationUse::READ_OBJECT), model::Result::OK);
    EXPECT_EQ(model::revalidate_binding(fixture.state, file_handle), model::Result::STALE_BINDING);
    EXPECT_EQ(model::collect_unlinked_node(fixture.state, fixture.file), model::Result::BUSY);

    model::release(fixture.state, file_handle);
    EXPECT_EQ(model::collect_unlinked_node(fixture.state, fixture.file), model::Result::OK);
    model::NodeIdentity replacement{};
    ASSERT_EQ(model::create_node(fixture.state, 0, 2, model::NodeKind::FILE, &replacement), model::Result::OK);
    EXPECT_NE(replacement.generation, fixture.file.generation);
    EXPECT_EQ(model::validate_token(fixture.state,
                                    {.mount = model::mount_identity(fixture.state, 0), .object = fixture.file, .root = fixture.root},
                                    model::ValidationUse::READ_OBJECT),
              model::Result::STALE_HANDLE);

    model::release(fixture.state, directory_handle);
    model::release(fixture.state, root_handle);
}

TEST(VfsLookupModel, MountRetirementPinsExistingReadAndFencesNewWalkAndReplacement) {
    FixtureModel fixture;
    model::LookupHandle root_handle{};
    ASSERT_EQ(model::acquire_mount_root(fixture.state, 0, root_handle), model::Result::OK);
    model::LookupToken const OLD_ROOT = root_handle.token;
    model::MountIdentity const OLD_MOUNT = OLD_ROOT.mount;

    ASSERT_EQ(model::retire_mount(fixture.state, OLD_MOUNT), model::Result::OK);
    EXPECT_EQ(model::validate_handle(fixture.state, root_handle, model::ValidationUse::READ_OBJECT), model::Result::OK);
    EXPECT_EQ(model::validate_handle(fixture.state, root_handle, model::ValidationUse::CONTINUE_WALK), model::Result::OK);
    model::LookupHandle in_flight_child{};
    ASSERT_EQ(model::lookup_child(fixture.state, root_handle, 1, in_flight_child), model::Result::OK);
    EXPECT_EQ(model::validate_handle(fixture.state, in_flight_child, model::ValidationUse::READ_OBJECT), model::Result::OK);
    model::LookupHandle rejected{};
    EXPECT_EQ(model::acquire_mount_root(fixture.state, 0, rejected), model::Result::MOUNT_RETIRED);
    EXPECT_EQ(model::destroy_retired_mount(fixture.state, OLD_MOUNT), model::Result::BUSY);

    model::release(fixture.state, in_flight_child);
    model::release(fixture.state, root_handle);
    ASSERT_EQ(model::destroy_retired_mount(fixture.state, OLD_MOUNT), model::Result::OK);
    ASSERT_EQ(model::install_mount(fixture.state, 0), model::Result::OK);
    model::NodeIdentity new_root{};
    ASSERT_EQ(model::create_node(fixture.state, 0, 0, model::NodeKind::DIRECTORY, &new_root), model::Result::OK);
    EXPECT_NE(model::mount_identity(fixture.state, 0).generation, OLD_MOUNT.generation);
    EXPECT_NE(new_root.generation, OLD_ROOT.object.generation);
    EXPECT_EQ(model::validate_token(fixture.state, OLD_ROOT, model::ValidationUse::READ_OBJECT), model::Result::STALE_HANDLE);
}

TEST(VfsLookupModel, DotDotClampsAtRootAndFailsClosedAfterRenameOutsideRoot) {
    FixtureModel fixture;
    model::NodeIdentity outside{};
    ASSERT_EQ(model::create_node(fixture.state, 0, 3, model::NodeKind::DIRECTORY, &outside), model::Result::OK);
    ASSERT_EQ(model::bind(fixture.state, fixture.root, 3, outside), model::Result::OK);

    model::LookupHandle root_handle{};
    model::LookupHandle directory_handle{};
    model::LookupHandle dotdot{};
    ASSERT_EQ(model::acquire_mount_root(fixture.state, 0, root_handle), model::Result::OK);
    ASSERT_EQ(model::walk_dotdot(fixture.state, root_handle, dotdot), model::Result::OK);
    EXPECT_EQ(dotdot.token.object, fixture.root);
    model::release(fixture.state, dotdot);

    ASSERT_EQ(model::lookup_child(fixture.state, root_handle, 1, directory_handle), model::Result::OK);
    ASSERT_EQ(model::walk_dotdot(fixture.state, directory_handle, dotdot), model::Result::OK);
    EXPECT_EQ(dotdot.token.object, fixture.root);
    model::release(fixture.state, dotdot);

    // Model a concurrent move to a detached directory that is not under the
    // retained task root before the next '..'.
    ASSERT_EQ(model::unlink_binding(fixture.state, fixture.root, 1), model::Result::OK);
    ASSERT_EQ(model::unlink_binding(fixture.state, fixture.root, 3), model::Result::OK);
    ASSERT_EQ(model::bind(fixture.state, outside, 7, fixture.directory), model::Result::OK);
    EXPECT_EQ(model::walk_dotdot(fixture.state, directory_handle, dotdot), model::Result::ROOT_ESCAPE);

    model::release(fixture.state, directory_handle);
    model::release(fixture.state, root_handle);
}

TEST(VfsLookupModel, RetainedSymlinkCannotBeRedirectedByReplacement) {
    FixtureModel fixture;
    model::NodeIdentity old_target{};
    model::NodeIdentity new_target{};
    model::NodeIdentity old_link{};
    model::NodeIdentity new_link{};
    ASSERT_EQ(model::create_node(fixture.state, 0, 3, model::NodeKind::FILE, &old_target), model::Result::OK);
    ASSERT_EQ(model::create_node(fixture.state, 0, 4, model::NodeKind::FILE, &new_target), model::Result::OK);
    ASSERT_EQ(model::create_node(fixture.state, 0, 5, model::NodeKind::SYMLINK, &old_link), model::Result::OK);
    ASSERT_EQ(model::create_node(fixture.state, 0, 6, model::NodeKind::SYMLINK, &new_link), model::Result::OK);
    ASSERT_EQ(model::bind(fixture.state, fixture.directory, 3, old_target), model::Result::OK);
    ASSERT_EQ(model::bind(fixture.state, fixture.directory, 4, new_target), model::Result::OK);
    ASSERT_EQ(model::set_symlink_target(fixture.state, old_link, old_target), model::Result::OK);
    ASSERT_EQ(model::set_symlink_target(fixture.state, new_link, new_target), model::Result::OK);
    ASSERT_EQ(model::bind(fixture.state, fixture.directory, 5, old_link), model::Result::OK);

    model::LookupHandle root_handle{};
    model::LookupHandle directory_handle{};
    model::LookupHandle retained_old_link{};
    ASSERT_EQ(model::acquire_mount_root(fixture.state, 0, root_handle), model::Result::OK);
    ASSERT_EQ(model::lookup_child(fixture.state, root_handle, 1, directory_handle), model::Result::OK);
    ASSERT_EQ(model::lookup_child(fixture.state, directory_handle, 5, retained_old_link), model::Result::OK);
    ASSERT_EQ(model::replace_binding(fixture.state, fixture.directory, 5, new_link), model::Result::OK);

    uint8_t depth = 0;
    model::LookupHandle followed_old{};
    ASSERT_EQ(model::follow_symlink(fixture.state, retained_old_link, depth, followed_old), model::Result::OK);
    EXPECT_EQ(followed_old.token.object, old_target);
    EXPECT_EQ(model::revalidate_binding(fixture.state, retained_old_link), model::Result::STALE_BINDING);

    model::LookupHandle fresh_link{};
    model::LookupHandle followed_new{};
    ASSERT_EQ(model::lookup_child(fixture.state, directory_handle, 5, fresh_link), model::Result::OK);
    depth = 0;
    ASSERT_EQ(model::follow_symlink(fixture.state, fresh_link, depth, followed_new), model::Result::OK);
    EXPECT_EQ(followed_new.token.object, new_target);

    model::release(fixture.state, followed_new);
    model::release(fixture.state, fresh_link);
    model::release(fixture.state, followed_old);
    model::release(fixture.state, retained_old_link);
    model::release(fixture.state, directory_handle);
    model::release(fixture.state, root_handle);
}

TEST(VfsLookupModel, SymlinkDepthRejectsTheNinthFollow) {
    model::Model state{};
    model::NodeIdentity root{};
    ASSERT_EQ(model::install_mount(state, 0), model::Result::OK);
    ASSERT_EQ(model::create_node(state, 0, 0, model::NodeKind::DIRECTORY, &root), model::Result::OK);

    std::array<model::NodeIdentity, model::MAX_SYMLINK_DEPTH + 1> links{};
    for (size_t index = 0; index < links.size(); ++index) {
        ASSERT_EQ(model::create_node(state, 0, static_cast<model::NodeId>(index + 1), model::NodeKind::SYMLINK, &links[index]),
                  model::Result::OK);
        ASSERT_EQ(model::bind(state, root, static_cast<model::NameId>(index + 1), links[index]), model::Result::OK);
    }
    for (size_t index = 0; index + 1 < links.size(); ++index) {
        ASSERT_EQ(model::set_symlink_target(state, links[index], links[index + 1]), model::Result::OK);
    }
    ASSERT_EQ(model::set_symlink_target(state, links.back(), links.back()), model::Result::OK);

    model::LookupHandle root_handle{};
    model::LookupHandle cursor{};
    ASSERT_EQ(model::acquire_mount_root(state, 0, root_handle), model::Result::OK);
    ASSERT_EQ(model::lookup_child(state, root_handle, 1, cursor), model::Result::OK);
    uint8_t depth = 0;
    for (uint8_t followed = 0; followed < model::MAX_SYMLINK_DEPTH; ++followed) {
        model::LookupHandle next{};
        ASSERT_EQ(model::follow_symlink(state, cursor, depth, next), model::Result::OK);
        model::release(state, cursor);
        cursor = next;
        next.retained = false;
    }
    model::LookupHandle rejected{};
    EXPECT_EQ(model::follow_symlink(state, cursor, depth, rejected), model::Result::SYMLINK_DEPTH);
    EXPECT_EQ(depth, model::MAX_SYMLINK_DEPTH);
    model::release(state, cursor);
    model::release(state, root_handle);
}

TEST(VfsLookupModel, StaleBindingRestartsAreDeterministicallyBounded) {
    FixtureModel fixture;
    model::LookupHandle root_handle{};
    model::LookupHandle directory_handle{};
    ASSERT_EQ(model::acquire_mount_root(fixture.state, 0, root_handle), model::Result::OK);
    ASSERT_EQ(model::lookup_child(fixture.state, root_handle, 1, directory_handle), model::Result::OK);

    model::RestartBudget budget{};
    for (uint8_t attempt = 0; attempt <= model::DEFAULT_MAX_RESTARTS; ++attempt) {
        model::LookupHandle observed{};
        ASSERT_EQ(model::lookup_child(fixture.state, directory_handle, 2, observed), model::Result::OK);
        fixture.state.nodes[fixture.directory.id].directory_generation =
            model::next_generation(fixture.state.nodes[fixture.directory.id].directory_generation);
        model::Result const RESULT = model::revalidate_or_restart(fixture.state, observed, budget);
        EXPECT_EQ(RESULT, attempt < model::DEFAULT_MAX_RESTARTS ? model::Result::RESTART : model::Result::RETRY_LIMIT);
        model::release(fixture.state, observed);
    }
    EXPECT_EQ(budget.restarts, model::DEFAULT_MAX_RESTARTS);
    EXPECT_EQ(budget.attempts, model::DEFAULT_MAX_RESTARTS + 1);

    model::release(fixture.state, directory_handle);
    model::release(fixture.state, root_handle);
}

TEST(VfsLookupModel, DualParentOrderIsStableForReverseInputsAndCoalescesIdentity) {
    FixtureModel fixture;
    model::NodeIdentity second_directory{};
    ASSERT_EQ(model::create_node(fixture.state, 0, 3, model::NodeKind::DIRECTORY, &second_directory), model::Result::OK);
    ASSERT_EQ(model::bind(fixture.state, fixture.root, 3, second_directory), model::Result::OK);

    model::LookupHandle root_handle{};
    model::LookupHandle first{};
    model::LookupHandle second{};
    ASSERT_EQ(model::acquire_mount_root(fixture.state, 0, root_handle), model::Result::OK);
    ASSERT_EQ(model::lookup_child(fixture.state, root_handle, 1, first), model::Result::OK);
    ASSERT_EQ(model::lookup_child(fixture.state, root_handle, 3, second), model::Result::OK);

    auto const FORWARD = model::order_parents(first, second);
    auto const REVERSE = model::order_parents(second, first);
    EXPECT_EQ(FORWARD.first, REVERSE.first);
    EXPECT_EQ(FORWARD.second, REVERSE.second);
    EXPECT_EQ(FORWARD.first_mount, REVERSE.first_mount);
    EXPECT_EQ(FORWARD.second_mount, REVERSE.second_mount);
    EXPECT_FALSE(FORWARD.same_parent);
    EXPECT_LT(FORWARD.first.id, FORWARD.second.id);

    auto const SAME = model::order_parents(first, first);
    EXPECT_TRUE(SAME.same_parent);
    EXPECT_EQ(SAME.first, SAME.second);

    model::Model remote_state{};
    model::NodeIdentity remote_root{};
    ASSERT_EQ(model::install_mount(remote_state, 1), model::Result::OK);
    ASSERT_EQ(model::create_node(remote_state, 1, 4, model::NodeKind::DIRECTORY, &remote_root), model::Result::OK);
    model::LookupHandle remote{};
    ASSERT_EQ(model::acquire_mount_root(remote_state, 1, remote), model::Result::OK);
    auto const CROSS_FORWARD = model::order_parents(first, remote);
    auto const CROSS_REVERSE = model::order_parents(remote, first);
    EXPECT_EQ(CROSS_FORWARD.first_mount, CROSS_REVERSE.first_mount);
    EXPECT_EQ(CROSS_FORWARD.first, CROSS_REVERSE.first);
    EXPECT_EQ(CROSS_FORWARD.second_mount, CROSS_REVERSE.second_mount);
    EXPECT_EQ(CROSS_FORWARD.second, CROSS_REVERSE.second);
    EXPECT_LT(CROSS_FORWARD.first_mount.id, CROSS_FORWARD.second_mount.id);

    model::release(remote_state, remote);
    model::release(fixture.state, second);
    model::release(fixture.state, first);
    model::release(fixture.state, root_handle);
}

}  // namespace
