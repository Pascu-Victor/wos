Problem: v1 all4 registered8/8 then safely fell back at100031–100204ns cumulative HPET discrepancy. It wrongly treated KVM raw-clock rate and QEMU adjusted CLOCK_MONOTONIC rate as identical forever. Read-only host21-bracket probe found adjusted-minus-raw+24.7177ppm/+123622ns in5.001s; primary Linux KVM and QEMU sources explain this distinction. This strongly supports a normal rate difference, rather than proving every guest rejected sample's cause.
Expected files to change: kvmclock_policy.hpp (actual shared controller), kvmclock{cpp,hpp} fallback transform exports, tsc.cpp KVM-only fallback time transform, kvmclock_test.cpp and clock docs. v1 source/build/raw archive immutable in clock-recon/rejected-v1 and closedpressure7-kvmclock package; original940+6a/53 fresh all4 health15879 EXIT0 completed BEFORE diagnosis.
Files already read: all originalcontract sourcepaths plus primary Linux arch/x86/kvm/x86.c get_kvmclock_base_ns/do_kvmclock_base and QEMU hpet_get_ticks/cpu_get_clock/get_clock. Existing perCPU migration/IRQ restrictions and HPET overflow semantics apply.
Invariants: original raw TSC policy/100us reference bound/guard unchanged. KVM requires every CPU stable host flag and consistent versioned conversions. Keep immutable common boot epoch and authoritative KVM rate on fast reads; DO NOT repeatedly rebase healthy clock or weaken100us sampling slack. Watchdog compares elapsed PV time to elapsed HPET bracket, allowing at most1000ppm rate difference plus100us sampling slack. 1000ppm is an explicit newclock rate qualification threshold, not proof of accuracy or an unbounded cumulative allowance. Boot AP anchor mismatch allows same rate bound across bootstrap elapsed interval. Reject backwards/overflow/version/unstable/rate errors before publishing rejected sample.
Locks: no new spinlock. PerCPU last-qualified checkpoint (PV value + before/after HPET bounds) uses atomic-field sequence snapshot with16bounded attempts; IRQs masked throughout read. Writer claims even revision using singleCAS; interrupted/NMI reader never waits for guest writer and conservatively falls back on exhaustion/conflict. All64bit atomics lock-free.
Allocation/blocking: no new allocations or guest waits. Keep registered physical pages untilshutdown. No new IRQ/wakeup, logger, or FPU change.
Fallback: maintain latest narrow-bracket qualified PV-minus-HPET offset only for a source transition. First failure freezes one signed offset using sentinelCAS before release-publishing failedmode. Later healthy/inflightcheckpoints cannot change the frozen offset. KVM-only HPET fallback applies that immutable offset and the existing shared monotonic floor, so normal raw/adjusted rate drift does not cause an accumulated fallback plateau. Bootstrap failed/unqualified haszero correction. Overflow saturates as existingtime arithmetic; original HPET/TSC branches unchanged. No claim of per-read independent100us reference bound for unsupported malicious/broken KVM promise.
ABI/proc/wire: no newABI/wire/trust changes. Status watchdogdeviation describes selected source's comparison: unchanged raw-TSC absolute bracket or KVM elapsed-reference interval error; quorum/count additive. Document it explicitly.
Tests/build: actual fake-provider controller ASanUBSan tests for long25/500ppm slew, boundaries/step/rate failure/frozen IRQ, versionNMI-style checkpoints, fixed fallback offset even afterlatequalifiedwrites, sourcefloor concurrency. BuildWOS normal andisolated wos targets/newsourcefreeze; fresh privatecandidatephase health/KVM8/8/continuingwatchdog/native/syscalls/migration/timer/idle/SSH/SFTP/fullmatrix, freshKTEST. No v1 performanceclaim; v1qualified16hosttests onlynotruntimeperformance.
Rollback: on candidate correctness/qualification failure retain/ownedstop/archive and original940+oldlibs/freshall4health BEFORE diagnosis. Preserve cache gate/unrelateddirtytree, all4defaultdisks andOPNsense. Broad rootgoal OPEN.

Implementation note: the rate baseline accumulates up to one second before
replacement. The ten-millisecond read deadline and existing timer IRQ checks
continue within that window. Resetting the rate baseline on every IRQ would
repeatedly forgive sampling uncertainty and hide a persistently bad rate.
Checkpoint fields are atomic, versioned, and never waited on without a retry
bound. Fast reads retain the immutable KVM boot epoch; only fallback applies
the frozen last-qualified narrow-bracket source offset.

Primary references:
- https://docs.kernel.org/virt/kvm/x86/msr.html (stable, versioned per-vCPU ABI)
- https://docs.kernel.org/virt/kvm/x86/cpuid.html (signature and feature flags)
- https://raw.githubusercontent.com/torvalds/linux/master/arch/x86/kvm/x86.c
  (get_kvmclock_base_ns and do_kvmclock_base use the raw clock frequency)
- https://raw.githubusercontent.com/qemu/qemu/master/hw/timer/hpet.c
- https://raw.githubusercontent.com/qemu/qemu/master/system/cpu-timers.c
- https://raw.githubusercontent.com/qemu/qemu/master/include/qemu/timer.h
  (HPET virtual clock derives from adjusted CLOCK_MONOTONIC on Linux)

These upstream references explain the expected clock-domain distinction; they
are not a source/build-ID audit of the installed host kernel or QEMU binaries.
The actual read-only host rate samples and failed v1 guest statuses are retained
under clock-recon, and each candidate build/source freeze remains separate.

Final defensive guard: reject a candidate older than `last_checked` before
subtracting that checkpoint. An NMI can reenter between checkpoint publication
and `last_seen` publication; the earlier `last_seen` check alone does not cover
that window for a backwards host sample. Valid monotonic samples retain the
same path. This was found during review, not a valid-clock runtime failure.
The final normal/diagnostic binaries are tracked separately from v2 runtime
evidence, with fresh ordinary and isolated qualification required.

Physical ownership: per-vCPU shared clock records use the dedicated `KVM_CLOCK`
owner (`kvm_clock` in memacc), with a permanent boot-to-shutdown lifetime and
at most one page per configured vCPU. These are hypervisor CPU metadata rather
than managed PCI DMA regions. The former `DMA_BUFFER` classification caused
exactly 32 extra pages outside the DMA registry in the 32-vCPU diagnostic run;
the existing DMA ownership assertions remain unchanged. Registered storage is
still retained on qualification failure because KVM may continue updating it.
This owner is appended so existing physical owner numeric values are stable.
