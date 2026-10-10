#!/usr/bin/env python3
"""Focused procfs ownership/publication contracts; native KTEST executes paths."""
from credential_policy_audit_test import (
    ROOT, body_after,
    test_procfs_global_policy_covers_types_and_precedes_content_or_controls,
    test_procfs_metadata_retains_owner_and_stages_before_publication,
    test_procfs_open_retains_identity_and_publishes_only_after_validation,
    test_procfs_final_readlink_retains_request_identity_and_stages_before_publication,
    test_procfs_root_consumers_use_canonical_value_snapshot,
    test_procfs_fd_access_authorizes_and_revalidates_before_publication,
    test_procfs_limits_keep_admitted_caller_and_retained_subject,
)


def main():
    for test in (
        test_procfs_global_policy_covers_types_and_precedes_content_or_controls,
        test_procfs_metadata_retains_owner_and_stages_before_publication,
        test_procfs_open_retains_identity_and_publishes_only_after_validation,
        test_procfs_final_readlink_retains_request_identity_and_stages_before_publication,
        test_procfs_root_consumers_use_canonical_value_snapshot,
        test_procfs_fd_access_authorizes_and_revalidates_before_publication,
        test_procfs_limits_keep_admitted_caller_and_retained_subject,
    ):
        test()
    proc = (ROOT / 'modules/kern/src/vfs/fs/procfs.cpp').read_text()
    parser = body_after(proc, 'auto procfs_lookup_path_as(')
    assert parser.index('view->closed()') < parser.index('new ProcFileData')
    assert 'find_task_by_pid_namespace_safe(*view, pid)' in parser
    assert 'credential_owner() == group->credential_owner()' in parser
    assert parser.index('!private_view_node(type)') < parser.index('new ProcFileData')
    assert 'view->retain(pfd->pid_view)' in parser
    assert 'pfd->node.pid = pfd->target->pid' in parser
    cloned = body_after(proc, 'auto procfs_open_lookup_as(')
    assert cloned.index('source.pid_view.retain(data->pid_view)') < cloned.index('source.target->try_acquire()')
    policy = body_after(proc, 'auto procfs_view_policy(')
    assert policy.index('data.pid_view.is_host()') < policy.index('data.pid_view.closed()')
    for prohibited in ('find_task_by_', 'wki_submitted_task_', 'MutexGuard', 'release()', 'new '):
        assert prohibited not in policy
    lookup_policy = body_after(proc, 'auto procfs_revalidate_lookup_result(')
    assert 'file->fs_type != FSType::PROCFS' in lookup_policy
    assert 'procfs_view_policy(' in lookup_policy
    for prohibited in ('credential_observation', 'TaskState::ACTIVE', 'procfs_lookup_path_as', 'MutexGuard', 'new '):
        assert prohibited not in lookup_policy
    for name in ('procfs_read_with_credentials', 'procfs_write_with_credentials', 'procfs_lseek_with_credentials',
                 'procfs_fill_stat', 'procfs_readdir', 'procfs_readlink_as_impl', 'procfs_revalidate_open_result',
                 'procfs_revalidate_metadata_result', 'procfs_revalidate_readlink_result'):
        assert 'procfs_view_policy(' in body_after(proc, 'auto ' + name + '('), name
    read = body_after(proc, 'auto procfs_read_private(')
    assert read.index('data.pid_view.is_host()') < read.index('credentials.allows(')
    link = body_after(proc, 'auto procfs_readlink_as_impl(')
    assert 'allow_remote_cwd = false' in link
    assert 'task_in_pid_view(*caller_task, pfd->pid_view)' in link
    assert 'DirectoryRecoveryMode::LOCAL_ONLY' in link
    assert '!pfd->pid_view.is_host() || allow_remote_cwd ? &CREDENTIALS : nullptr' in link
    assert link.index('if (pfd->pid_view.is_host())') < link.index('memcpy(link.data(), "/proc/", 6)')
    for name in ('generate_wki_launcher', 'generate_wki_runner', 'generate_wki_remote_pid'):
        assert 'local_view' in body_after(proc, 'auto ' + name + '(')
    child = (ROOT / 'modules/kern/src/syscalls_impl/process/child_events.cpp').read_text()
    diagnostics = body_after(child, 'auto diagnostics(')
    assert 'task.child_wait_view == View::PUBLIC' in diagnostics
    mounts = (ROOT / 'modules/kern/src/vfs/mount.cpp').read_text()
    prepare = body_after(mounts, 'auto prepare_mount_filesystem(')
    assert 'FSType::PROCFS' in prepare and 'mount->read_only = true' in prepare
    core = (ROOT / 'modules/kern/src/vfs/core.cpp').read_text()
    crossing = body_after(core, 'auto local_walk_cross_private_procfs_mount(')
    assert crossing.index('references->capture(') < crossing.index('validate_native_mount_attachment(')
    assert 'selected.target() != static_cast<tmpfs::TmpNode*>(covered->private_data)' in crossing
    assert 'ProcNodeType::ROOT_DIR' in crossing
    assert 'procfs_lookup_path_as(' not in crossing
    binding = body_after(core, 'auto local_walk_bind(')
    assert binding.index('local_walk_cross_private_procfs_mount(') < binding.index('int const RESULT = local_walk_read_stat(')
    assert 'PRIVATE && mount != nullptr && mount->fs_type == FSType::PROCFS' in binding
    assert 'epoch.physical_path && mount->fs_type == FSType::PROCFS' in binding
    validation = body_after(core, 'auto validate_lookup_directory_view_locked(')
    assert validation.index('origins->validate()') < validation.index('admit_directory_view(')
    assert 'procfs::procfs_revalidate_lookup_result(file)' in validation
    assert validation.index('procfs::procfs_revalidate_lookup_result(file)') < validation.index('references.capture(')
    assert validation.index('validate_native_mount_attachment(') < validation.index('directory_member_path_locked(covered, view.root()')
    pin = body_after(core, 'auto pin_directory_view_lookup(')
    assert 'file->is_directory ? -ENOTSUP : -ENOTDIR' in pin
    root = body_after(core, 'auto vfs_getroot_for_task(')
    cwd = body_after(core, 'auto vfs_getcwd_for_task(')
    assert root.index('recovery == DirectoryRecoveryMode::LOCAL_ONLY') < root.index('anchor = owner->root_anchor')
    assert cwd.index('recovery == DirectoryRecoveryMode::LOCAL_ONLY') < cwd.index('root_anchor = owner->root_anchor')
    walk = body_after(core, 'auto acquire_local_component_lookup(')
    assert '!PRIVATE || origin == nullptr || origin->pid_view.is_host()' in walk
    assert 'previous.policy().restricted_by(references.view.policy())' in walk
    assert 'admit_directory_view(wider, boundary, references.view.directory()' in walk
    assert walk.index('references.procfs_view_origins->validate()') < walk.rindex('LookupHandle candidate{}')
    assert walk.index('populate_lookup_path_fields(') < walk.index('set_procfs_view_origins(')
    assert 'reset_root = references.view.root()' in walk
    ownership = (ROOT / 'modules/kern/src/vfs/lookup.cpp').read_text()
    assert 'std::exchange(other.procfs_view_origins_, nullptr)' in ownership
    reset = body_after(ownership, 'void LookupHandle::reset(')
    assert 'release_procfs_view_origins_(procfs_view_origins_)' in reset
    assert 'procfs_view_origins_ = nullptr' in reset
    setter = body_after(ownership, 'void LookupHandleBuilder::set_procfs_view_origins(')
    assert 'release(' not in setter and 'delete ' not in setter and 'new ' not in setter
    print('Focused procfs credential, retained PID view, native mount and publication contracts passed')


if __name__ == '__main__':
    main()
