#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <type_traits>

#define ker kernel_ker
#include "modules/kern/src/abi/callnums.hpp"
#include "modules/kern/src/abi/callnums/futex.h"
#include "modules/kern/src/abi/callnums/multiproc.h"
#include "modules/kern/src/abi/callnums/net.h"
#include "modules/kern/src/abi/callnums/power.h"
#include "modules/kern/src/abi/callnums/process.h"
#include "modules/kern/src/abi/callnums/shm.h"
#include "modules/kern/src/abi/callnums/sys_log.h"
#include "modules/kern/src/abi/callnums/time.h"
#include "modules/kern/src/abi/callnums/vfs.h"
#include "modules/kern/src/abi/callnums/vmem.h"
#undef ker

#define ker libc_ker
#include "toolchain/src/mlibc/sysdeps/wos/include/callnums/futex.h"
#include "toolchain/src/mlibc/sysdeps/wos/include/callnums/multiproc.h"
#include "toolchain/src/mlibc/sysdeps/wos/include/callnums/net.h"
#include "toolchain/src/mlibc/sysdeps/wos/include/callnums/power.h"
#include "toolchain/src/mlibc/sysdeps/wos/include/callnums/process.h"
#include "toolchain/src/mlibc/sysdeps/wos/include/callnums/shm.h"
#include "toolchain/src/mlibc/sysdeps/wos/include/callnums/sys_log.h"
#include "toolchain/src/mlibc/sysdeps/wos/include/callnums/time.h"
#include "toolchain/src/mlibc/sysdeps/wos/include/callnums/vfs.h"
#include "toolchain/src/mlibc/sysdeps/wos/include/callnums/vmem.h"
#include "toolchain/src/mlibc/sysdeps/wos/include/sys/callnums.h"
#undef ker

template <typename Enum>
constexpr auto enum_value(Enum value) -> std::underlying_type_t<Enum> {
    return static_cast<std::underlying_type_t<Enum>>(value);
}

#define ABI_ENUM_EQUAL(kernel_value, libc_value) static_assert(enum_value(kernel_value) == enum_value(libc_value))
#define ABI_VALUE_EQUAL(kernel_value, libc_value) static_assert((kernel_value) == (libc_value))
#define ABI_TYPE_EQUAL(kernel_type, libc_type)               \
    static_assert(sizeof(kernel_type) == sizeof(libc_type)); \
    static_assert(alignof(kernel_type) == alignof(libc_type))
#define ABI_OFFSET_EQUAL(kernel_type, libc_type, field) static_assert(offsetof(kernel_type, field) == offsetof(libc_type, field))

using KernelCall = kernel_ker::abi::callnums;
using LibcCall = libc_ker::abi::callnums;
ABI_TYPE_EQUAL(KernelCall, LibcCall);
ABI_ENUM_EQUAL(KernelCall::SYS_LOG, LibcCall::sys_log);
ABI_ENUM_EQUAL(KernelCall::FUTEX, LibcCall::futex);
ABI_ENUM_EQUAL(KernelCall::THREADING, LibcCall::threading);
ABI_ENUM_EQUAL(KernelCall::PROCESS, LibcCall::process);
ABI_ENUM_EQUAL(KernelCall::TIME, LibcCall::time);
ABI_ENUM_EQUAL(KernelCall::VFS, LibcCall::vfs);
ABI_ENUM_EQUAL(KernelCall::NET, LibcCall::net);
ABI_ENUM_EQUAL(KernelCall::VMEM, LibcCall::vmem);
ABI_ENUM_EQUAL(KernelCall::VMEM_MAP, LibcCall::vmem_map);
ABI_ENUM_EQUAL(KernelCall::DEBUG, LibcCall::debug);
ABI_ENUM_EQUAL(KernelCall::SHM, LibcCall::shm);
ABI_ENUM_EQUAL(KernelCall::PERSONALITY, LibcCall::personality);
ABI_ENUM_EQUAL(KernelCall::POWER, LibcCall::power);

using KernelFutex = kernel_ker::abi::futex::futex_ops;
using LibcFutex = libc_ker::abi::futex::futex_ops;
ABI_TYPE_EQUAL(KernelFutex, LibcFutex);
ABI_ENUM_EQUAL(KernelFutex::FUTEX_WAIT, LibcFutex::FUTEX_WAIT);
ABI_ENUM_EQUAL(KernelFutex::FUTEX_WAKE, LibcFutex::FUTEX_WAKE);

using KernelThreadInfo = kernel_ker::abi::multiproc::threadInfoOps;
using LibcThreadInfo = libc_ker::abi::multiproc::threadInfoOps;
ABI_TYPE_EQUAL(KernelThreadInfo, LibcThreadInfo);
ABI_ENUM_EQUAL(KernelThreadInfo::CURRENT_THREAD_ID, LibcThreadInfo::CURRENT_THREAD_ID);
ABI_ENUM_EQUAL(KernelThreadInfo::NATIVE_THREAD_COUNT, LibcThreadInfo::NATIVE_THREAD_COUNT);
ABI_ENUM_EQUAL(KernelThreadInfo::CURRENT_CPU, LibcThreadInfo::CURRENT_CPU);

using KernelThreadControl = kernel_ker::abi::multiproc::threadControlOps;
using LibcThreadControl = libc_ker::abi::multiproc::threadControlOps;
ABI_TYPE_EQUAL(KernelThreadControl, LibcThreadControl);
ABI_ENUM_EQUAL(KernelThreadControl::SET_TCB, LibcThreadControl::SET_TCB);
ABI_ENUM_EQUAL(KernelThreadControl::YIELD, LibcThreadControl::YIELD);
ABI_ENUM_EQUAL(KernelThreadControl::THREAD_CREATE, LibcThreadControl::THREAD_CREATE);
ABI_ENUM_EQUAL(KernelThreadControl::THREAD_EXIT, LibcThreadControl::THREAD_EXIT);
ABI_ENUM_EQUAL(KernelThreadControl::SET_AFFINITY, LibcThreadControl::SET_AFFINITY);
ABI_ENUM_EQUAL(KernelThreadControl::GET_AFFINITY, LibcThreadControl::GET_AFFINITY);
ABI_ENUM_EQUAL(KernelThreadControl::CREATE_DOMAIN, LibcThreadControl::CREATE_DOMAIN);
ABI_ENUM_EQUAL(KernelThreadControl::SET_DOMAIN, LibcThreadControl::SET_DOMAIN);
ABI_ENUM_EQUAL(KernelThreadControl::QUERY_DOMAIN, LibcThreadControl::QUERY_DOMAIN);

using KernelNet = kernel_ker::abi::net::ops;
using LibcNet = libc_ker::abi::net::ops;
ABI_TYPE_EQUAL(KernelNet, LibcNet);
ABI_ENUM_EQUAL(KernelNet::SOCKET, LibcNet::SOCKET);
ABI_ENUM_EQUAL(KernelNet::BIND, LibcNet::BIND);
ABI_ENUM_EQUAL(KernelNet::LISTEN, LibcNet::LISTEN);
ABI_ENUM_EQUAL(KernelNet::ACCEPT, LibcNet::ACCEPT);
ABI_ENUM_EQUAL(KernelNet::CONNECT, LibcNet::CONNECT);
ABI_ENUM_EQUAL(KernelNet::SEND, LibcNet::SEND);
ABI_ENUM_EQUAL(KernelNet::RECV, LibcNet::RECV);
ABI_ENUM_EQUAL(KernelNet::CLOSE, LibcNet::CLOSE);
ABI_ENUM_EQUAL(KernelNet::SENDTO, LibcNet::SENDTO);
ABI_ENUM_EQUAL(KernelNet::RECVFROM, LibcNet::RECVFROM);
ABI_ENUM_EQUAL(KernelNet::SETSOCKOPT, LibcNet::SETSOCKOPT);
ABI_ENUM_EQUAL(KernelNet::GETSOCKOPT, LibcNet::GETSOCKOPT);
ABI_ENUM_EQUAL(KernelNet::SHUTDOWN, LibcNet::SHUTDOWN);
ABI_ENUM_EQUAL(KernelNet::GETPEERNAME, LibcNet::GETPEERNAME);
ABI_ENUM_EQUAL(KernelNet::GETSOCKNAME, LibcNet::GETSOCKNAME);
ABI_ENUM_EQUAL(KernelNet::SELECT, LibcNet::SELECT);
ABI_ENUM_EQUAL(KernelNet::POLL, LibcNet::POLL);
ABI_ENUM_EQUAL(KernelNet::IOCTL_NET, LibcNet::IOCTL_NET);
ABI_ENUM_EQUAL(KernelNet::SET_DEV_CPU_AFFINITY, LibcNet::SET_DEV_CPU_AFFINITY);
ABI_ENUM_EQUAL(KernelNet::NETCTL_IF_LIST, LibcNet::NETCTL_IF_LIST);
ABI_ENUM_EQUAL(KernelNet::NETCTL_ADDR_LIST, LibcNet::NETCTL_ADDR_LIST);
ABI_ENUM_EQUAL(KernelNet::NETCTL_ADDR_SET, LibcNet::NETCTL_ADDR_SET);
ABI_ENUM_EQUAL(KernelNet::NETCTL_ADDR_DEL, LibcNet::NETCTL_ADDR_DEL);
ABI_ENUM_EQUAL(KernelNet::NETCTL_LINK_SET, LibcNet::NETCTL_LINK_SET);
ABI_ENUM_EQUAL(KernelNet::SENDTO_EX, LibcNet::SENDTO_EX);
ABI_ENUM_EQUAL(KernelNet::RECVFROM_EX, LibcNet::RECVFROM_EX);
ABI_ENUM_EQUAL(KernelNet::NETCTL_ADDR_SET_V2, LibcNet::NETCTL_ADDR_SET_V2);
ABI_ENUM_EQUAL(KernelNet::NETCTL_ROUTE_LIST, LibcNet::NETCTL_ROUTE_LIST);
ABI_ENUM_EQUAL(KernelNet::NETCTL_ROUTE_SET, LibcNet::NETCTL_ROUTE_SET);
ABI_ENUM_EQUAL(KernelNet::NETCTL_ROUTE_DEL, LibcNet::NETCTL_ROUTE_DEL);

using KernelSockaddrIo = kernel_ker::abi::net::SockaddrIoV1;
using LibcSockaddrIo = libc_ker::abi::net::SockaddrIoV1;
ABI_TYPE_EQUAL(KernelSockaddrIo, LibcSockaddrIo);
ABI_OFFSET_EQUAL(KernelSockaddrIo, LibcSockaddrIo, size);
ABI_OFFSET_EQUAL(KernelSockaddrIo, LibcSockaddrIo, version);
ABI_OFFSET_EQUAL(KernelSockaddrIo, LibcSockaddrIo, address);
ABI_OFFSET_EQUAL(KernelSockaddrIo, LibcSockaddrIo, address_length);
ABI_OFFSET_EQUAL(KernelSockaddrIo, LibcSockaddrIo, result_length);

using KernelPower = kernel_ker::abi::power::ops;
using LibcPower = libc_ker::abi::power::ops;
ABI_TYPE_EQUAL(KernelPower, LibcPower);
ABI_ENUM_EQUAL(KernelPower::REBOOT, LibcPower::REBOOT);
ABI_ENUM_EQUAL(KernelPower::GET_STATE, LibcPower::GET_STATE);
ABI_ENUM_EQUAL(KernelPower::PREPARE, LibcPower::PREPARE);
ABI_VALUE_EQUAL(kernel_ker::abi::power::RB_AUTOBOOT, libc_ker::abi::power::RB_AUTOBOOT);
ABI_VALUE_EQUAL(kernel_ker::abi::power::RB_HALT_SYSTEM, libc_ker::abi::power::RB_HALT_SYSTEM);
ABI_VALUE_EQUAL(kernel_ker::abi::power::RB_ENABLE_CAD, libc_ker::abi::power::RB_ENABLE_CAD);
ABI_VALUE_EQUAL(kernel_ker::abi::power::RB_DISABLE_CAD, libc_ker::abi::power::RB_DISABLE_CAD);
ABI_VALUE_EQUAL(kernel_ker::abi::power::RB_POWER_OFF, libc_ker::abi::power::RB_POWER_OFF);

using KernelProcessOp = kernel_ker::abi::process::procmgmt_ops;
using LibcProcessOp = libc_ker::abi::process::procmgmt_ops;
ABI_TYPE_EQUAL(KernelProcessOp, LibcProcessOp);
#define PROCESS_OP(name) ABI_ENUM_EQUAL(KernelProcessOp::name, LibcProcessOp::name)
PROCESS_OP(EXIT);
PROCESS_OP(EXEC);
PROCESS_OP(WAITPID);
PROCESS_OP(GETPID);
PROCESS_OP(GETPPID);
PROCESS_OP(FORK);
PROCESS_OP(SIGACTION);
PROCESS_OP(SIGPROCMASK);
PROCESS_OP(KILL);
PROCESS_OP(SIGRETURN);
PROCESS_OP(GETUID);
PROCESS_OP(GETEUID);
PROCESS_OP(GETGID);
PROCESS_OP(GETEGID);
PROCESS_OP(SETUID);
PROCESS_OP(SETGID);
PROCESS_OP(SETEUID);
PROCESS_OP(SETEGID);
PROCESS_OP(GETUMASK);
PROCESS_OP(SETUMASK);
PROCESS_OP(SETSID);
PROCESS_OP(GETSID);
PROCESS_OP(SETPGID);
PROCESS_OP(GETPGID);
PROCESS_OP(EXECVE);
PROCESS_OP(GETHOSTNAME);
PROCESS_OP(SETHOSTNAME);
PROCESS_OP(SETPRIORITY);
PROCESS_OP(SETWKITARGET);
PROCESS_OP(GETWKITARGET);
PROCESS_OP(PTRACE);
PROCESS_OP(GETGROUPS);
PROCESS_OP(SETGROUPS);
PROCESS_OP(SIGSUSPEND);
PROCESS_OP(UNAME);
PROCESS_OP(CLONE_VM_PROC);
PROCESS_OP(PRCTL);
PROCESS_OP(ARCH_PRCTL);
PROCESS_OP(SIGALTSTACK);
PROCESS_OP(GETRESUID);
PROCESS_OP(GETRESGID);
PROCESS_OP(SIGPENDING);
PROCESS_OP(GETPRIORITY);
PROCESS_OP(SPAWN);
PROCESS_OP(INIT_CONTROL_SUBMIT);
PROCESS_OP(INIT_CONTROL_RECEIVE);
PROCESS_OP(INIT_STATUS_PUBLISH);
PROCESS_OP(INIT_STATUS_READ);
#undef PROCESS_OP

using KernelSpawnActionType = kernel_ker::abi::process::SpawnFdActionType;
using LibcSpawnActionType = libc_ker::abi::process::SpawnFdActionType;
ABI_TYPE_EQUAL(KernelSpawnActionType, LibcSpawnActionType);
ABI_ENUM_EQUAL(KernelSpawnActionType::CLOSE, LibcSpawnActionType::CLOSE);
ABI_ENUM_EQUAL(KernelSpawnActionType::DUP2, LibcSpawnActionType::DUP2);
ABI_ENUM_EQUAL(KernelSpawnActionType::OPEN, LibcSpawnActionType::OPEN);
ABI_VALUE_EQUAL(kernel_ker::abi::process::SPAWN_FLAG_SETSIGMASK, libc_ker::abi::process::SPAWN_FLAG_SETSIGMASK);
ABI_VALUE_EQUAL(kernel_ker::abi::process::SPAWN_FLAG_SETPGROUP, libc_ker::abi::process::SPAWN_FLAG_SETPGROUP);
ABI_VALUE_EQUAL(kernel_ker::abi::process::SPAWN_FLAG_USEVFORK, libc_ker::abi::process::SPAWN_FLAG_USEVFORK);
ABI_VALUE_EQUAL(kernel_ker::abi::process::SPAWN_SUPPORTED_FLAGS, libc_ker::abi::process::SPAWN_SUPPORTED_FLAGS);
ABI_VALUE_EQUAL(kernel_ker::abi::process::SPAWN_OPTIONS_VERSION, libc_ker::abi::process::SPAWN_OPTIONS_VERSION);

using KernelSpawnAction = kernel_ker::abi::process::SpawnFdAction;
using LibcSpawnAction = libc_ker::abi::process::SpawnFdAction;
ABI_TYPE_EQUAL(KernelSpawnAction, LibcSpawnAction);
ABI_OFFSET_EQUAL(KernelSpawnAction, LibcSpawnAction, type);
ABI_OFFSET_EQUAL(KernelSpawnAction, LibcSpawnAction, fd);
ABI_OFFSET_EQUAL(KernelSpawnAction, LibcSpawnAction, srcfd);
ABI_OFFSET_EQUAL(KernelSpawnAction, LibcSpawnAction, oflag);
ABI_OFFSET_EQUAL(KernelSpawnAction, LibcSpawnAction, mode);
ABI_OFFSET_EQUAL(KernelSpawnAction, LibcSpawnAction, path);

using KernelSpawnOptions = kernel_ker::abi::process::SpawnOptions;
using LibcSpawnOptions = libc_ker::abi::process::SpawnOptions;
ABI_TYPE_EQUAL(KernelSpawnOptions, LibcSpawnOptions);
ABI_OFFSET_EQUAL(KernelSpawnOptions, LibcSpawnOptions, size);
ABI_OFFSET_EQUAL(KernelSpawnOptions, LibcSpawnOptions, version);
ABI_OFFSET_EQUAL(KernelSpawnOptions, LibcSpawnOptions, flags);
ABI_OFFSET_EQUAL(KernelSpawnOptions, LibcSpawnOptions, sig_mask);
ABI_OFFSET_EQUAL(KernelSpawnOptions, LibcSpawnOptions, pgroup);
ABI_OFFSET_EQUAL(KernelSpawnOptions, LibcSpawnOptions, actions);
ABI_OFFSET_EQUAL(KernelSpawnOptions, LibcSpawnOptions, action_count);
ABI_OFFSET_EQUAL(KernelSpawnOptions, LibcSpawnOptions, reserved0);
ABI_OFFSET_EQUAL(KernelSpawnOptions, LibcSpawnOptions, reserved1);

using KernelShmOp = kernel_ker::abi::shm::ops;
using LibcShmOp = libc_ker::abi::shm::ops;
ABI_TYPE_EQUAL(KernelShmOp, LibcShmOp);
ABI_ENUM_EQUAL(KernelShmOp::GET, LibcShmOp::GET);
ABI_ENUM_EQUAL(KernelShmOp::ATTACH, LibcShmOp::ATTACH);
ABI_ENUM_EQUAL(KernelShmOp::DETACH, LibcShmOp::DETACH);
ABI_ENUM_EQUAL(KernelShmOp::CTL, LibcShmOp::CTL);

using KernelIpcPerm = kernel_ker::abi::shm::IpcPerm;
using LibcIpcPerm = libc_ker::abi::shm::IpcPerm;
ABI_TYPE_EQUAL(KernelIpcPerm, LibcIpcPerm);
ABI_OFFSET_EQUAL(KernelIpcPerm, LibcIpcPerm, key);
ABI_OFFSET_EQUAL(KernelIpcPerm, LibcIpcPerm, uid);
ABI_OFFSET_EQUAL(KernelIpcPerm, LibcIpcPerm, gid);
ABI_OFFSET_EQUAL(KernelIpcPerm, LibcIpcPerm, cuid);
ABI_OFFSET_EQUAL(KernelIpcPerm, LibcIpcPerm, cgid);
ABI_OFFSET_EQUAL(KernelIpcPerm, LibcIpcPerm, mode);
ABI_OFFSET_EQUAL(KernelIpcPerm, LibcIpcPerm, seq);
ABI_OFFSET_EQUAL(KernelIpcPerm, LibcIpcPerm, unused);

using KernelShmidDs = kernel_ker::abi::shm::ShmidDs;
using LibcShmidDs = libc_ker::abi::shm::ShmidDs;
ABI_TYPE_EQUAL(KernelShmidDs, LibcShmidDs);
ABI_OFFSET_EQUAL(KernelShmidDs, LibcShmidDs, shm_perm);
ABI_OFFSET_EQUAL(KernelShmidDs, LibcShmidDs, shm_segsz);
ABI_OFFSET_EQUAL(KernelShmidDs, LibcShmidDs, shm_atime);
ABI_OFFSET_EQUAL(KernelShmidDs, LibcShmidDs, shm_dtime);
ABI_OFFSET_EQUAL(KernelShmidDs, LibcShmidDs, shm_ctime);
ABI_OFFSET_EQUAL(KernelShmidDs, LibcShmidDs, shm_cpid);
ABI_OFFSET_EQUAL(KernelShmidDs, LibcShmidDs, shm_lpid);
ABI_OFFSET_EQUAL(KernelShmidDs, LibcShmidDs, shm_nattch);
ABI_OFFSET_EQUAL(KernelShmidDs, LibcShmidDs, unused);
ABI_VALUE_EQUAL(kernel_ker::abi::shm::IPC_PRIVATE, libc_ker::abi::shm::ipc_private);
ABI_VALUE_EQUAL(kernel_ker::abi::shm::IPC_CREAT, libc_ker::abi::shm::ipc_create);
ABI_VALUE_EQUAL(kernel_ker::abi::shm::IPC_EXCL, libc_ker::abi::shm::ipc_exclusive);
ABI_VALUE_EQUAL(kernel_ker::abi::shm::IPC_RMID, libc_ker::abi::shm::ipc_remove);
ABI_VALUE_EQUAL(kernel_ker::abi::shm::IPC_STAT, libc_ker::abi::shm::ipc_stat);
ABI_VALUE_EQUAL(kernel_ker::abi::shm::SHM_RDONLY, libc_ker::abi::shm::shm_readonly);

using KernelLogOp = kernel_ker::abi::sys_log::sys_log_ops;
using LibcLogOp = libc_ker::abi::sys_log::sys_log_ops;
ABI_TYPE_EQUAL(KernelLogOp, LibcLogOp);
ABI_ENUM_EQUAL(KernelLogOp::LOG, LibcLogOp::LOG);
ABI_ENUM_EQUAL(KernelLogOp::LOG_LINE, LibcLogOp::LOG_LINE);
ABI_ENUM_EQUAL(KernelLogOp::LOG_EX, LibcLogOp::LOG_EX);
ABI_ENUM_EQUAL(KernelLogOp::LOG_BLOCK_BEGIN, LibcLogOp::LOG_BLOCK_BEGIN);
ABI_ENUM_EQUAL(KernelLogOp::LOG_BLOCK_END, LibcLogOp::LOG_BLOCK_END);

using KernelLogDevice = kernel_ker::abi::sys_log::sys_log_device;
using LibcLogDevice = libc_ker::abi::sys_log::sys_log_device;
ABI_TYPE_EQUAL(KernelLogDevice, LibcLogDevice);
ABI_ENUM_EQUAL(KernelLogDevice::SERIAL, LibcLogDevice::SERIAL);
ABI_ENUM_EQUAL(KernelLogDevice::VGA, LibcLogDevice::VGA);

using KernelLogLevel = kernel_ker::abi::sys_log::sys_log_level;
using LibcLogLevel = libc_ker::abi::sys_log::sys_log_level;
ABI_TYPE_EQUAL(KernelLogLevel, LibcLogLevel);
ABI_ENUM_EQUAL(KernelLogLevel::TRACE, LibcLogLevel::TRACE);
ABI_ENUM_EQUAL(KernelLogLevel::DEBUG, LibcLogLevel::DEBUG);
ABI_ENUM_EQUAL(KernelLogLevel::INFO, LibcLogLevel::INFO);
ABI_ENUM_EQUAL(KernelLogLevel::NOTICE, LibcLogLevel::NOTICE);
ABI_ENUM_EQUAL(KernelLogLevel::WARN, LibcLogLevel::WARN);
ABI_ENUM_EQUAL(KernelLogLevel::ERROR, LibcLogLevel::ERROR);
ABI_ENUM_EQUAL(KernelLogLevel::CRITICAL, LibcLogLevel::CRITICAL);
ABI_ENUM_EQUAL(KernelLogLevel::PANIC, LibcLogLevel::PANIC);

using KernelLogRecord = kernel_ker::abi::sys_log::JournalRecord;
using LibcLogRecord = libc_ker::abi::sys_log::JournalRecord;
ABI_TYPE_EQUAL(KernelLogRecord, LibcLogRecord);
ABI_OFFSET_EQUAL(KernelLogRecord, LibcLogRecord, magic);
ABI_OFFSET_EQUAL(KernelLogRecord, LibcLogRecord, version);
ABI_OFFSET_EQUAL(KernelLogRecord, LibcLogRecord, header_size);
ABI_OFFSET_EQUAL(KernelLogRecord, LibcLogRecord, sequence);
ABI_OFFSET_EQUAL(KernelLogRecord, LibcLogRecord, boot_id);
ABI_OFFSET_EQUAL(KernelLogRecord, LibcLogRecord, monotonic_us);
ABI_OFFSET_EQUAL(KernelLogRecord, LibcLogRecord, pid);
ABI_OFFSET_EQUAL(KernelLogRecord, LibcLogRecord, tid);
ABI_OFFSET_EQUAL(KernelLogRecord, LibcLogRecord, cpu);
ABI_OFFSET_EQUAL(KernelLogRecord, LibcLogRecord, level);
ABI_OFFSET_EQUAL(KernelLogRecord, LibcLogRecord, reserved0);
ABI_OFFSET_EQUAL(KernelLogRecord, LibcLogRecord, flags);
ABI_OFFSET_EQUAL(KernelLogRecord, LibcLogRecord, module);
ABI_OFFSET_EQUAL(KernelLogRecord, LibcLogRecord, message_len);
ABI_OFFSET_EQUAL(KernelLogRecord, LibcLogRecord, reserved1);
ABI_OFFSET_EQUAL(KernelLogRecord, LibcLogRecord, message);
ABI_VALUE_EQUAL(kernel_ker::abi::sys_log::JOURNAL_RECORD_MAGIC, libc_ker::abi::sys_log::JOURNAL_RECORD_MAGIC);
ABI_VALUE_EQUAL(kernel_ker::abi::sys_log::JOURNAL_RECORD_VERSION, libc_ker::abi::sys_log::JOURNAL_RECORD_VERSION);
ABI_VALUE_EQUAL(kernel_ker::abi::sys_log::JOURNAL_MODULE_MAX, libc_ker::abi::sys_log::JOURNAL_MODULE_MAX);
ABI_VALUE_EQUAL(kernel_ker::abi::sys_log::JOURNAL_MESSAGE_MAX, libc_ker::abi::sys_log::JOURNAL_MESSAGE_MAX);

using KernelTime = kernel_ker::abi::sys_time_ops;
using LibcTime = libc_ker::abi::sys_time_ops;
ABI_TYPE_EQUAL(KernelTime, LibcTime);
ABI_ENUM_EQUAL(KernelTime::GETTIMEOFDAY, LibcTime::GETTIMEOFDAY);
ABI_ENUM_EQUAL(KernelTime::CLOCK_GETTIME, LibcTime::CLOCK_GETTIME);
ABI_ENUM_EQUAL(KernelTime::NANOSLEEP, LibcTime::NANOSLEEP);
ABI_ENUM_EQUAL(KernelTime::TIMES, LibcTime::TIMES);
ABI_ENUM_EQUAL(KernelTime::SETITIMER, LibcTime::SETITIMER);
ABI_ENUM_EQUAL(KernelTime::GETITIMER, LibcTime::GETITIMER);

using KernelVfs = kernel_ker::abi::vfs::ops;
using LibcVfs = libc_ker::abi::vfs::ops;
ABI_TYPE_EQUAL(KernelVfs, LibcVfs);
#define VFS_OP(kernel_name, libc_name) ABI_ENUM_EQUAL(KernelVfs::kernel_name, LibcVfs::libc_name)
VFS_OP(OPEN, OPEN);
VFS_OP(READ, READ);
VFS_OP(WRITE, WRITE);
VFS_OP(CLOSE, CLOSE);
VFS_OP(LSEEK, LSEEK);
VFS_OP(ISATTY, ISATTY);
VFS_OP(READ_DIR_ENTRIES, READ_DIR_ENTRIES);
VFS_OP(MOUNT, MOUNT);
VFS_OP(MKDIR, MKDIR);
VFS_OP(READLINK, READLINK);
VFS_OP(SYMLINK, SYMLINK);
VFS_OP(SENDFILE, SENDFILE);
VFS_OP(STAT, STAT);
VFS_OP(FSTAT, FSTAT);
VFS_OP(UMOUNT, UMOUNT);
VFS_OP(DUP, DUP);
VFS_OP(DUP2, DUP2);
VFS_OP(GETCWD, GETCWD);
VFS_OP(CHDIR, CHDIR);
VFS_OP(ACCESS, ACCESS);
VFS_OP(UNLINK, UNLINK);
VFS_OP(RMDIR, RMDIR);
VFS_OP(RENAME, RENAME);
VFS_OP(CHMOD, CHMOD);
VFS_OP(TRUNCATE, TRUNCATE);
VFS_OP(PIPE, PIPE);
VFS_OP(PREAD, PREAD);
VFS_OP(PWRITE, PWRITE);
VFS_OP(FCNTL, FCNTL);
VFS_OP(FCHMOD, FCHMOD);
VFS_OP(CHOWN, CHOWN);
VFS_OP(FCHOWN, FCHOWN);
VFS_OP(FACCESSAT, FACCESSAT);
VFS_OP(UNLINKAT, UNLINKAT);
VFS_OP(RENAMEAT, RENAMEAT);
VFS_OP(EPOLL_CREATE, EPOLL_CREATE);
VFS_OP(EPOLL_CTL, EPOLL_CTL);
VFS_OP(EPOLL_PWAIT, EPOLL_PWAIT);
VFS_OP(IOCTL, IOCTL);
VFS_OP(FSYNC, FSYNC);
VFS_OP(LINK, LINK);
VFS_OP(WKI_RULE_ADD, WKI_RULE_ADD);
VFS_OP(WKI_RULE_GET, WKI_RULE_GET);
VFS_OP(WKI_RULE_CLEAR, WKI_RULE_CLEAR);
VFS_OP(PIVOT_ROOT, PIVOT_ROOT);
VFS_OP(WKI_RULE_GET_DEFAULT, WKI_RULE_GET_DEFAULT);
VFS_OP(STATVFS, STATVFS);
VFS_OP(FSTATVFS, FSTATVFS);
VFS_OP(LSTAT, LSTAT);
VFS_OP(SYNC, SYNC);
VFS_OP(REALPATH, REALPATH);
VFS_OP(OPENAT, OPENAT);
VFS_OP(STATAT, STATAT);
VFS_OP(UTIMENSAT, UTIMENSAT);
VFS_OP(MKDIRAT, MKDIRAT);
VFS_OP(READLINKAT, READLINKAT);
VFS_OP(LINKAT, LINKAT);
VFS_OP(SYMLINKAT, SYMLINKAT);
VFS_OP(FCHMODAT, FCHMODAT);
VFS_OP(FCHDIR, FCHDIR);
VFS_OP(FCHOWNAT, FCHOWNAT);
VFS_OP(FSTAT_CLOSE, FSTAT_CLOSE);
VFS_OP(METADATA_BATCH, METADATA_BATCH);
VFS_OP(SETXATTR, SETXATTR);
VFS_OP(LSETXATTR, LSETXATTR);
VFS_OP(FSETXATTR, FSETXATTR);
VFS_OP(GETXATTR, GETXATTR);
VFS_OP(LGETXATTR, LGETXATTR);
VFS_OP(FGETXATTR, FGETXATTR);
VFS_OP(LISTXATTR, LISTXATTR);
VFS_OP(LLISTXATTR, LLISTXATTR);
VFS_OP(FLISTXATTR, FLISTXATTR);
VFS_OP(REMOVEXATTR, REMOVEXATTR);
VFS_OP(LREMOVEXATTR, LREMOVEXATTR);
VFS_OP(FREMOVEXATTR, FREMOVEXATTR);
#undef VFS_OP
ABI_VALUE_EQUAL(kernel_ker::abi::vfs::XATTR_NAME_MAX, libc_ker::abi::vfs::XATTR_NAME_MAX);
ABI_VALUE_EQUAL(kernel_ker::abi::vfs::XATTR_SIZE_MAX, libc_ker::abi::vfs::XATTR_SIZE_MAX);
ABI_VALUE_EQUAL(kernel_ker::abi::vfs::XATTR_LIST_MAX, libc_ker::abi::vfs::XATTR_LIST_MAX);
ABI_VALUE_EQUAL(kernel_ker::abi::vfs::METADATA_BATCH_VERSION, libc_ker::abi::vfs::METADATA_BATCH_VERSION);
ABI_VALUE_EQUAL(kernel_ker::abi::vfs::METADATA_BATCH_MAX_ITEMS, libc_ker::abi::vfs::METADATA_BATCH_MAX_ITEMS);
ABI_VALUE_EQUAL(kernel_ker::abi::vfs::METADATA_BATCH_MAX_PATH_CHARS, libc_ker::abi::vfs::METADATA_BATCH_MAX_PATH_CHARS);

using KernelBatchOp = kernel_ker::abi::vfs::metadata_batch_operation;
using LibcBatchOp = libc_ker::abi::vfs::metadata_batch_operation;
ABI_TYPE_EQUAL(KernelBatchOp, LibcBatchOp);
ABI_ENUM_EQUAL(KernelBatchOp::INVALID, LibcBatchOp::INVALID);
ABI_ENUM_EQUAL(KernelBatchOp::CREATE_CLOSE, LibcBatchOp::CREATE_CLOSE);
ABI_ENUM_EQUAL(KernelBatchOp::STAT_FOLLOW, LibcBatchOp::STAT_FOLLOW);
ABI_ENUM_EQUAL(KernelBatchOp::UNLINK, LibcBatchOp::UNLINK);
ABI_ENUM_EQUAL(KernelBatchOp::RENAME, LibcBatchOp::RENAME);

using KernelBatchHeader = kernel_ker::abi::vfs::metadata_batch_header;
using LibcBatchHeader = libc_ker::abi::vfs::metadata_batch_header;
ABI_TYPE_EQUAL(KernelBatchHeader, LibcBatchHeader);
ABI_OFFSET_EQUAL(KernelBatchHeader, LibcBatchHeader, version);
ABI_OFFSET_EQUAL(KernelBatchHeader, LibcBatchHeader, operation);
ABI_OFFSET_EQUAL(KernelBatchHeader, LibcBatchHeader, count);
ABI_OFFSET_EQUAL(KernelBatchHeader, LibcBatchHeader, mode);

using KernelVmem = kernel_ker::abi::vmem::ops;
using LibcVmem = libc_ker::abi::vmem::ops;
ABI_TYPE_EQUAL(KernelVmem, LibcVmem);
ABI_ENUM_EQUAL(KernelVmem::ANON_ALLOCATE, LibcVmem::ANON_ALLOCATE);
ABI_ENUM_EQUAL(KernelVmem::ANON_FREE, LibcVmem::ANON_FREE);
ABI_ENUM_EQUAL(KernelVmem::PROTECT, LibcVmem::PROTECT);
ABI_ENUM_EQUAL(KernelVmem::MREMAP, LibcVmem::MREMAP);
ABI_ENUM_EQUAL(KernelVmem::MSYNC, LibcVmem::MSYNC);
ABI_ENUM_EQUAL(KernelVmem::SWAPON, LibcVmem::SWAPON);
ABI_ENUM_EQUAL(KernelVmem::SWAPOFF, LibcVmem::SWAPOFF);

TEST(WosAbiParity, CompileTimeContractsHold) { SUCCEED(); }
