#include <syscalls_impl/process/init_control.hpp>
#include <test/ktest.hpp>

KTEST(InitControl, MailboxPreservesFifoAndFailedCopyout) { KEXPECT_TRUE(ker::syscall::process::init_control_selftest_mailbox()); }

KTEST(InitControl, RequestsAndPublicStatusAreValidatedAndSanitized) {
    KEXPECT_TRUE(ker::syscall::process::init_control_selftest_validation());
}

KTEST(InitControl, ReceiverAndPublisherUseProcessIdentity) { KEXPECT_TRUE(ker::syscall::process::init_control_selftest_process_role()); }
