#!/usr/bin/env python3

"""Source invariants for block-device errno handling."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
BLOCK_DEVICE = ROOT / "modules/kern/src/dev/block_device.cpp"
BLOCK_DEVICE_HPP = ROOT / "modules/kern/src/dev/block_device.hpp"
BLOCK_DEVICE_KTEST = ROOT / "modules/kern/src/test/block_device_ktest.cpp"
BUFFER_CACHE = ROOT / "modules/kern/src/vfs/buffer_cache.cpp"
AHCI = ROOT / "modules/kern/src/dev/ahci.cpp"
BLOCK_REQUEST = ROOT / "modules/kern/src/dev/block_request.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void)\s+{re.escape(name)}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{",
        source,
    )
    if match is None:
        fail(f"missing function {name}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[match.end() : pos - 1]


def require(source: str, token: str, context: str) -> None:
    if token not in source:
        fail(f"{context}: missing {token!r}")


def require_absent(source: str, token: str, context: str) -> None:
    if token in source:
        fail(f"{context}: unexpected {token!r}")


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token!r}")
        cursor = found + len(token)


def require_task_only_block_wait_contract(source: str) -> None:
    syscall = function_body(source, "safe_syscall_task")
    for token in ("has_run_queues()", "can_query_current_task()", "is_panic_mode()", "TaskType::PROCESS",
                  "preempt_disable_depth != 0", "syscall_account_start_us != 0", "scheduler_transition_active.load(",
                  "(!task->deferred_task_switch || RETURN_YIELD)", "!task->wants_block", "!task->is_voluntary_blocked()",
                  "task->wki_proxy_task_id == 0", "WaitChannelKind::WKI_EXECVE_PROXY"):
        require(syscall, token, "shared task-only syscall qualification must retain every unsafe-context rejection")
    for token in ("task->deferred_task_switch && task->yield_switch", "task->wait_channel == nullptr",
                  "task->wait_channel_kind == sched::task::WaitChannelKind::NONE"):
        require(syscall, token, "only the exact channel-free syscall return yield may bypass deferred-wait rejection")
    pause = function_body(source, "pause_task")
    require_order(pause, ["safe_syscall_task()", "has_interrupting_signal_pending()", "kern_yield()", "preemptible_syscall_park(",
                          "TaskType::DAEMON", "sched::interrupts_enabled()", "kern_sleep_us("],
                  "qualified syscall parks must be selected before IF-gated daemon sleep")
    for body in (pause, syscall):
        require_absent(body, 'asm volatile("sti', "queue pause must delegate interrupt restoration to the existing safe park")
        require_absent(body, "task->deferred_task_switch =", "safe park must preserve the syscall return-boundary owner")
        require_absent(body, "task->yield_switch =", "safe park must preserve the pending return-boundary yield")
    context = function_body(source, "wait_poll_context")
    require_order(context, ["!queue.native || queue.limits.depth <= 1", "safe_syscall_task()",
                           "!task->has_interrupting_signal_pending()", "!sched::kernel_threads_shutdown_requested()"],
                  "legacy/depth1/unsafe/signal/shutdown contexts must avoid added polling clocks")
    allowed = function_body(source, "wait_poll_allowed")
    require_order(allowed, ["wait_poll_context(queue)", "queue.lock.lock_irqsave()", "lookup(queue.model, request.token)",
                           "queue.model.admission_open", "!queue.model.resetting", "!queue.closing", "!queue.caller_driven",
                           "BlockRequestState::ISSUED", "state->cancel_reason == 0", "queue.owners[request.token.slot] == &request",
                           "request.deadline_us", "queue.lock.unlock_irqrestore(FLAGS)"],
                  "each extra pass must requalify the exact issued owner/state outside provider work")
    service = function_body(source, "service_queue")
    require_order(service, ["queue.service_lock.try_lock()", "return false", "NoIoScope", "GENERATION = queue.model.generation",
                           "bool post_required = !queue.native", "block_model_detail::unfinished(state)",
                           "SERVICE_PROVIDER(extra_poll && queue.native && !post_required);", "finish_completions(queue)", "for (size_t attempt",
                           "if (request == nullptr) break", "post_required = true", "queue.ops.submit(queue.device, *request)",
                           "RESULT == -EAGAIN", "if (!post_required)", "queue.lock.lock_irqsave()",
                           "queue.model.generation != GENERATION", "block_model_detail::unfinished(state)",
                           "queue.lock.unlock_irqrestore(flags)", "if (post_required) SERVICE_PROVIDER();",
                           "finish_completions(queue)", "return true"],
                  "every attempt or reset/cancel/lifecycle obligation needs POST and the full retirement tail")
    require_order(service, ["finish_completions(queue)", "for (size_t attempt", "flags = queue.lock.lock_irqsave()",
                           "#ifdef WOS_ASYNC_BLOCK_IO", "attempt == 0 && !extra_poll && !post_required && queue.native",
                           "queue.model.admission_open && !queue.model.resetting", "!queue.closing && !queue.caller_driven",
                           "queue.model.generation == GENERATION", "queue.provider_completion_owner == nullptr",
                           "queue.model.stats.submitted != UINT64_MAX", "queue.model.stats.submitted == queue.model.stats.completed",
                           "queue.lock.unlock_irqrestore(flags)", "return true", "#endif", "BlockRequestToken token"],
                  "only a fresh finite empty certificate after full PRE and first gather may skip the second gather")
    require(source, "auto service_queue(BlockRequestQueue& queue, bool extra_poll = false)",
            "workers, boot, finalizers, capacity waits and mandatory wait service must default to full provider service")
    require(service, "if (!poll_active || queue.ops.poll_active == nullptr || !queue.ops.poll_active(queue.device)) queue.ops.service(queue.device);",
            "only a successful optional extra poll may omit PRE; absent hooks and false results must fall back to full service")
    lifecycle = "!queue.native || !queue.model.admission_open || queue.model.resetting || queue.closing || queue.caller_driven"
    if service.count(lifecycle) != 2 or service.count("block_model_detail::unfinished(state) && state.cancel_reason != 0") != 2:
        raise AssertionError("entry and final guards must independently preserve lifecycle and unfinished cancellation obligations")
    require(source, "constexpr size_t WAIT_POLL_EXTRA_PASSES = 32;", "a frozen clock still needs the hard pass bound")
    require(source, "constexpr uint64_t WAIT_POLL_BUDGET_US = 128;", "extra pass starts must obey the fixed sampled admission budget")
    require(source, "constexpr uint64_t WAIT_RETRY_INTERVAL_US = 16;", "pending retirement must request the qualified 16us retry")
    wait = function_body(source, "block_request_wait")
    require_order(wait, ["queue->lock.unlock_irqrestore(flags)", "POLLING = wait_poll_context(*queue)",
                        "POLL_STARTED = POLLING ? ker::mod::time::get_us() : 0", "SERVICED = service_queue(*queue)",
                        "POLLING && SERVICED", "pass < WAIT_POLL_EXTRA_PASSES", "wait_poll_allowed(*queue, request, last_poll_us)",
                        "uint64_t const NOW = ker::mod::time::get_us()", "NOW < last_poll_us", "NOW - POLL_STARTED >= WAIT_POLL_BUDGET_US",
                        "NOW >= request.deadline_us", "last_poll_us = NOW", "!service_queue(*queue, true)", "pause_task(WAIT_RETRY_INTERVAL_US)"],
                  "wait must start one immutable eligible window before initial service, sample after qualification and preserve final safe park")


def require_native_async_contract(ahci: str) -> None:
    poll = function_body(ahci, "ahci_async_poll_active")
    require_order(poll, ["native_memory(bdev)", "port_locks[PORT].try_lock()", "memory.slot_guard.lock_irqsave()",
                         "memory.pending_issue_mask", "memory.health_check_due", "memory.async.frozen", "memory.async.offline",
                         "memory.queue_reset_pending", "memory.interrupt_status", "memory.injected_fault",
                         "slot.phase != async_rules::Phase::active", "!slot.ncq", "slot.cancel_reason", "slot.generation",
                         "slot.epoch != memory.async.epoch", "request->token != slot.token", "submission.sequence == 0",
                         "owned_count < 2", "mmio_read32(port->ci)", "mmio_read32(port->sact)", "mmio_read32(port->is)",
                         "mod::time::get_us()", "(CI | SACT) != owned_mask", "STATUS != 0", "memory.interrupt_status",
                         "controller_failed", "admission.allows_command()", "NOW >= slot.deadline_us", "return true"],
                  "extra active polling must retain every owner and fall back on any observed work before completion")
    for forbidden in ("mmio_write", "exchange(", ".store(", "async_rules::claim", "async_rules::release",
                      "async_rules::freeze", "dma::complete", "native_copy", "block_request_complete", "kern_sleep", "new "):
        require_absent(poll, forbidden, "a successful active poll must make no claim, mutation, acknowledgement or retirement")

    require(ahci, "bool health_check_due = true", "a newly published native port must first qualify a full hardware snapshot")
    submit = function_body(ahci, "ahci_async_submit")
    require_order(submit, ["memory.slot_guard.lock_irqsave()", "memory.health_check_due = true", "async_rules::reserve("],
                  "every actual reserve attempt must retain POST health qualification even after rejected DMA cleanup")
    require_order(submit, ["port_locks[PORT].try_lock()", "async_rules::reserve(", "dma::prepare(",
                           "native_build(", "memory.requests[INDEX] = &request", "memory.async_submissions[INDEX] = submission",
                           "memory.pending_issue_mask |=", "increment(memory.stats.submitted)"],
                  "NCQ admission must retain independent prepared DMA and request ownership before staging a bounded hardware batch")
    for forbidden in ("new ", "wait_until(", "kern_sleep", "kern_yield", "async_rules::publish(",
                      "mmio_write32(port->sact", "mmio_write32(port->ci"):
        require_absent(submit, forbidden, "native admission must have bounded backpressure without allocation or polling")
    require(submit, "dma::Retirement::never_submitted", "rejected admission must retire prepared DMA without a command")
    service = function_body(ahci, "ahci_async_service")
    idle_start = service.index("if (async_rules::owned(memory.async) == 0")
    idle = service[idle_start : service.index("return;", idle_start) + len("return;")]
    for token in ("memory.pending_issue_mask == 0", "!memory.health_check_due", "!memory.async.frozen", "!memory.failed",
                  "!memory.queue_reset_pending.load(std::memory_order_acquire)",
                  "!controller_failed.load(std::memory_order_acquire)",
                  "memory.interrupt_status.load(std::memory_order_acquire) == 0"):
        require(idle, token, "an idle shortcut must exclude all owners, health/recovery obligations and latched IRQ errors")
    require_order(idle, ["memory.slot_guard.unlock_irqrestore(flags)", "port_locks[PORT].unlock()", "return;"],
                  "a qualified idle return must release both locks")
    for forbidden in ("mmio_", "get_us(", "interrupt_status.exchange(", "block_queue_", "dma::"):
        require_absent(idle, forbidden, "true idle service must not touch hardware/time or consume an IRQ latch")
    if ahci.count("memory.health_check_due = false") != 1:
        raise AssertionError("native health obligation must clear only in the full guarded snapshot")
    snapshot = service[service.index("std::array<NativeCompletion") : service.index("if (FROZEN)")]
    require_order(snapshot, ["memory.slot_guard.lock_irqsave()", "mmio_read32(port->ci)", "mmio_read32(port->sact)",
                             "memory.interrupt_status.exchange(", "mmio_read32(port->is)", "mmio_read32(port->tfd)",
                             "async_rules::freeze(", "async_rules::claim(", "memory.slot_guard.unlock_irqrestore(flags)"],
                  "one guarded active-before-error snapshot must qualify every completion before IRQ can acknowledge fatal status")
    require_order(snapshot, ["mmio_read32(port->ci)", "mmio_read32(port->sact)", "memory.interrupt_status.exchange(",
                             "mmio_read32(port->is)", "mmio_read32(port->tfd)", "mmio_write32(port->is, OBSERVED)",
                             "memory.health_check_due = false", "async_rules::freeze("],
                  "only a full guarded active-before-error capture and acknowledgment may discharge submit's health obligation")
    require_order(snapshot, ["async_rules::claim(", "uint32_t const PENDING = memory.pending_issue_mask",
                            "async_rules::publish(", 'asm volatile("mfence"', "mmio_write32(port->sact, ncq)",
                            "mmio_write32(port->ci, PENDING)", "PUBLISHED_SACT = mmio_read32(port->sact)",
                            "PUBLISHED_CI = mmio_read32(port->ci)", "memory.pending_issue_mask &= ~PENDING", "native_observe_hardware("],
                  "only the following service publishes staged phases and one combined SACT/CI doorbell after prior active claims")
    require_order(snapshot, ["size_t completion_index = async_rules::MAX_SLOTS", "(PENDING & (PENDING - 1)) == 0",
                            "async_rules::owned(memory.async) == 1", "bool const FLUSH = !SLOT.ncq && SLOT.bytes == 0",
                            "memory.requests[i]->description.op == BlockRequestOperation::FLUSH",
                            "memory.requests[i]->description.bytes == 0", "memory.async_submissions[i].sequence == 0",
                            "bool const DATA = memory.requests[i]->token == SLOT.token",
                            "memory.requests[i]->description.op == BlockRequestOperation::READ",
                            "memory.requests[i]->description.op == BlockRequestOperation::WRITE", "SLOT.bytes != 0",
                            "memory.requests[i]->description.bytes == SLOT.bytes", "memory.async_submissions[i].size == SLOT.bytes",
                            "memory.async_submissions[i].sequence != 0", "SOLE && (FLUSH || DATA)",
                            "completion_index = i", "mmio_write32(port->ci, PENDING)", "memory.pending_issue_mask &= ~PENDING"],
                  "fresh completion is limited to a sole newly published zero-DMA FLUSH or positive token/size/sequence-matched READ/WRITE")
    fresh = snapshot[snapshot.index("if (completion_index != async_rules::MAX_SLOTS") :]
    require(fresh, "if (completion_index != async_rules::MAX_SLOTS) {", "every eligible sole command must receive one fresh snapshot")
    require_absent(fresh, "PUBLISHED_CI == 0", "published readback must not gate fresh qualification or serve as claim authority")
    require_absent(fresh, "PUBLISHED_SACT == 0", "published readback must not gate fresh qualification or serve as claim authority")
    if fresh.count("FRESH_CI = mmio_read32(port->ci)") != 1 or fresh.count("FRESH_SACT = mmio_read32(port->sact)") != 1:
        raise AssertionError("sole completion must take exactly one fresh active snapshot")
    require_order(fresh, ["auto const SLOT = memory.async.slots[completion_index]", "FRESH_CI = mmio_read32(port->ci)",
                         "FRESH_SACT = mmio_read32(port->sact)", "memory.interrupt_status.exchange(", "mmio_read32(port->is)",
                         "FRESH_TFD = mmio_read32(port->tfd)", "mmio_write32(port->is, FRESH_STATUS)",
                         "native_observe_hardware(memory, FRESH_CI, FRESH_SACT)", "async_rules::freeze(",
                         "async_rules::timeout(memory.async, mod::time::get_us())", "fault = memory.injected_fault",
                         "memory.injected_fault = SelftestAsyncFault::none", "!admission.allows_command()",
                         "!memory.async.frozen && FRESH_CI == 0 && FRESH_SACT == 0",
                         "async_rules::claim(memory.async, completion_index, SLOT.generation, SLOT.epoch, FRESH_CI, FRESH_SACT)",
                         "qualified[completion_index] = SLOT", "memory.slot_guard.unlock_irqrestore(flags)"],
                  "sole FLUSH/READ/WRITE must qualify fresh activity/errors/controller/deadline/injection/lifecycle before generation claim under the same guard")
    for token in ("dma_rules::ERROR_INTERRUPTS | (1U << 6) | (1U << 22)", "(FRESH_TFD & 0x21U) != 0",
                  "controller_failed.load(std::memory_order_acquire)", "memory.failed", "memory.queue_reset_pending.load"):
        require(fresh, token, "fresh sole completion must preserve every health and lifecycle rejection")
    require(snapshot, "!admission.allows_command() || memory.failed || controller_failed.load",
            "staged batch publication must reject lifecycle closure before ringing hardware")
    for forbidden in ("dma::", "native_copy(", "block_request_complete", "block_queue_", "wait_until(", "new "):
        require_absent(snapshot, forbidden, "completion qualification guard must contain only bounded MMIO and model transitions")
    require_order(service, ["bool stopped = stop_cmd(port)", "dma::Retirement::device_quiesced",
                            "async_rules::release(", "native_port_reset(port)", "native_revalidate(port, PORT)",
                            "async_rules::finish_reset("],
                  "uncertain commands must retire after engine stop and qualify reset identity before reusing tags")
    require_order(service, ["bool const STAGED = (memory.pending_issue_mask", "STAGED ? dma::Retirement::never_submitted",
                           "memory.pending_issue_mask &= ~(uint32_t{1} << i)", "fail_stop(PORT)"],
                  "retained unissued DMA must retire with never_submitted proof before unproven issued backing is quarantined")
    revalidate = function_body(ahci, "native_revalidate")
    require_order(revalidate, ["execute_command_locked(", "ATA_CMD_IDENTIFY", "total == device->total_sectors",
                              "memory.stats.identify_queue_depth", "memory.stats.identify_sata_caps",
                              "memory.stats.identify_command_sets", "memory.identify_serial.begin()",
                              "mmio_read32(port->tfd) & 0x21U", "mmio_read32(port->ci) == 0", "mmio_read32(port->sact) == 0"],
                  "reset admission requires a real exclusive IDENTIFY with unchanged geometry, capabilities and serial identity")
    require_absent(function_body(ahci, "clear_port_errors"), "->tfd",
                   "clearing interrupt/error latches must never fabricate a successful task-file status")
    baseline = service[service.index("port_slots_in_use[PORT].store(0") : service.index("bool reset =")]
    require_order(baseline, ["memory.slot_guard.lock_irqsave()", "memory.interrupt_status.store(0", "clear_port_errors(port)",
                            "memory.slot_guard.unlock_irqrestore(flags)"],
                  "old captured reset errors must join retired old DMA before the exclusive reset qualification")
    final = service[service.index("uint32_t const FINAL_CI") : service.index("if (!reset) {", service.index("uint32_t const FINAL_CI"))]
    require_order(final, ["FINAL_CI", "FINAL_SACT", "memory.interrupt_status.exchange(", "mmio_read32(port->is)",
                         "FINAL_TFD", "reset = false", "async_rules::finish_reset(",
                         "memory.slot_guard.unlock_irqrestore(flags)"],
                  "new errors after IDENTIFY must prevent reset publication before releasing the same status guard")
    require_order(service, ["dma::Retirement::command_completed", "native_copy(",
                            "port_locks[PORT].unlock()", "block_request_complete_from_service("],
                  "read copy and DMA retirement must finish before completion outside port locks")
    require(service, "if (!SLOT.ncq && memory.clb_virt[i].prdbc", "NCQ success must not rely on undefined PRDBC")
    require(service, "fail_stop(PORT)", "an unproven engine stop must quarantine backing")
    irq = function_body(ahci, "native_irq")
    for forbidden in ("dma::", "native_copy(", "block_request_complete", "port_locks[", "new "):
        require_absent(irq, forbidden, "IRQ must defer DMA, CPU copying, callbacks and sleeping port locks")
    require_order(irq, ["memory.slot_guard.lock_irqsave()", "mmio_read32(hba_mem->ports[i].is)", "interrupt_status.fetch_or",
                       "mmio_write32(hba_mem->ports[i].is, STATUS)", "memory.slot_guard.unlock_irqrestore(FLAGS)", "block_queue_wake"],
                  "IRQ must serialize capture, publication and acknowledgment with service, then wake outside the slot guard")
    shutdown = function_body(ahci, "ahci_shutdown")
    require_order(shutdown, ["admission.close()", "block_queue_close(", "block_queue_drain(", "lifecycle::retire("],
                  "shutdown must join requests before retiring DMA rings")

    runtime = BLOCK_REQUEST.read_text()
    complete = function_body(runtime, "block_request_complete")
    require(complete, "complete_request(device, token, status, bytes, false)",
            "IRQ-safe completion must never enable provider-scope wake suppression")
    require_absent(complete, "provider_completion_owner", "generic completion must ignore matching task-service scope")
    task_complete = function_body(runtime, "block_request_complete_from_service")
    require(task_complete, "complete_request(device, token, status, bytes, true)",
            "only the distinct task-service entry may request inline completion wake suppression")
    claim = function_body(runtime, "complete_request")
    require_order(claim, ["queue->lock.lock_irqsave()", "block_model_claim_completion(", "CLAIMED && from_service",
                          "queue->provider_completion_owner != nullptr", "sched::can_query_current_task()",
                          "queue->provider_completion_owner == sched::get_current_task()", "queue->lock.unlock_irqrestore(FLAGS)",
                          "if (CLAIMED && !INLINE) wake_queue(*queue)"],
                  "only a valid explicit task claim in the matching retained provider scope may omit its worker wake")
    for forbidden in ("callback(", "ops.service", "ops.submit", "new ", "kern_yield"):
        require_absent(complete, forbidden, "IRQ-safe completion must only claim bounded queue state")
        require_absent(claim, forbidden, "shared completion claiming must remain allocation-free and bounded")
    callbacks = function_body(runtime, "finish_completions")
    require_order(callbacks, ["queue.lock.lock_irqsave()", "queue.provider_completion_owner = nullptr",
                             "state.state == BlockRequestState::COMPLETING", "queue.lock.unlock_irqrestore(flags)",
                             "request->callback("],
                  "the existing gather guard must end suppression before capturing descriptors or invoking callbacks")
    require_order(callbacks, ["queue.lock.unlock_irqrestore(flags)", "request->callback(",
                              "block_model_finish_completion(", "request->completed.store(true"],
                  "callback retirement precedes publication to the retained caller outside queue spin guards")
    service = function_body(runtime, "service_queue")
    require_order(service, ["sched::hold_kernel_shutdown()", "queue.service_lock.try_lock()", "NoIoScope const NO_IO",
                            "sched::get_current_task()", "owner->try_acquire()", "OwnerRef", "task->release()",
                            "auto const SERVICE_PROVIDER", "queue.lock.lock_irqsave()", "queue.provider_completion_owner = owner",
                            "queue.lock.unlock_irqrestore(FLAGS)", "queue.ops.service(queue.device)"],
                  "one retained per-pass owner must publish only around explicit provider calls after service/NoIo ownership")
    if service.count("owner->try_acquire()") != 1 or service.count("SERVICE_PROVIDER();") != 1 or service.count("SERVICE_PROVIDER(extra_poll && queue.native && !post_required);") != 1:
        raise AssertionError("service must retain at most one owner and wrap only PRE/POST provider calls")
    require_order(service, ["queue.ops.cancel(", "SERVICE_PROVIDER(extra_poll && queue.native && !post_required);", "finish_completions(queue)",
                            "int const RESULT", "if (post_required) SERVICE_PROVIDER();", "finish_completions(queue)"],
                  "cancel/submit/callbacks remain outside provider suppression and both mandatory gathers stay intact")
    wait = function_body(runtime, "block_request_wait")
    require(wait, "service_queue(*queue)", "bootstrap and finalizer waits must progress without a worker")
    reset = function_body(runtime, "block_queue_begin_reset")
    for forbidden in ("ops.cancel", "callback(", "block_queue_drain"):
        require_absent(reset, forbidden, "port-lock reset admission must not call into backend or completion readers")


def require_submit_for_wait_contract(source: str) -> None:
    submit = function_body(source, "submit_request")
    require(source, "bool caller_driven, bool wake_worker", "shutdown admission and worker wake policy must be independent")
    require(submit, "queue->caller_driven && !caller_driven", "callback-free admission must not bypass worker shutdown")
    require_order(submit, ["queue->owners[request.token.slot] = &request", "queue->lock.unlock_irqrestore(FLAGS)",
                           "if (RESULT == 0 && wake_worker) wake_queue(*queue)"],
                  "accepted owners must publish before optional admission wake outside the queue guard")
    ordinary = function_body(source, "block_request_submit")
    require(ordinary, "callback, callback_arg, false, true", "ordinary async admission must retain its worker wake")
    waiting = function_body(source, "block_request_submit_for_wait")
    require(waiting, "nullptr, nullptr, false, false", "caller-service admission must be callback-free without finalizer privilege")
    bridge = function_body(source, "block_request_sync")
    require_order(bridge, ["30'000'000, nullptr, nullptr, true, false", "block_request_wait(request)", "block_request_release(request)"],
                  "private finalizer bridge must immediately wait and release every accepted silent admission")
    for name in ("block_request_cancel", "block_queue_close", "block_queue_begin_reset",
                 "block_queue_finish_reset"):
        require(function_body(source, name), "wake_queue(*queue)", "all non-admission queue wake sites must remain intact")
    require(function_body(source, "block_request_complete"), "bytes, false)", "generic claims must retain their worker wake")
    require(function_body(source, "complete_request"), "if (CLAIMED && !INLINE) wake_queue(*queue)",
            "every successful claim outside the explicit matching provider scope must retain its worker wake")
    header = BLOCK_REQUEST.with_suffix(".hpp").read_text()
    for token in ("service/wait before other blocking, return or unwind", "retain/join ALL", "accepted batch entries",
                  "bounded capacity service on -EAGAIN"):
        require(header, token, "silent admission must document the caller's complete retirement responsibility")


def main() -> None:
    block_source = BLOCK_DEVICE.read_text()
    block_header = BLOCK_DEVICE_HPP.read_text()
    block_ktest = BLOCK_DEVICE_KTEST.read_text()
    ahci_source = AHCI.read_text()

    for token in [
        "enum class BlockWriterLeaseOwner",
        "class BlockWriterLease",
        "BlockWriterLease(BlockWriterLease&& other) noexcept",
        "auto try_acquire(const BlockDevice* device, BlockWriterLeaseOwner owner) -> bool",
    ]:
        require(block_header, token, "block writer lease API")

    lease_acquire = function_body(block_source, "BlockWriterLease::try_acquire")
    for token in [
        "block_writer_lease_lock.lock_irqsave()",
        "owner == BlockWriterLeaseOwner::REMOTE_BINDING",
        "lease->owner_ == BlockWriterLeaseOwner::REMOTE_BINDING",
        "block_devices_overlap(lease->device_, device)",
        "block_writer_leases = this",
    ]:
        require(lease_acquire, token, "block writer lease acquisition")
    for forbidden in ["new ", "delete", "kern_yield", "mount_lock", "s_server_lock"]:
        require_absent(lease_acquire, forbidden, "block writer lease acquisition must remain allocation-free and lock-local")
    require(block_source, "take_locked(other);", "block writer lease move must relink atomically")
    require(
        block_ktest,
        "KTEST(BlockWriterLease, RemoteIsExclusiveWhileLocalMountsMayCoexist)",
        "block writer lease policy KTEST",
    )

    normalize_body = function_body(block_source, "normalize_io_result")
    require(normalize_body, "return -EIO;", "raw block driver -1 must normalize to EIO")
    require(normalize_body, "return (rc < 0) ? rc : -rc;", "positive driver errors must normalize to negative errno")

    for name in ("block_read", "block_write", "block_flush"):
        body = function_body(block_source, name)
        require(body, "normalize_io_result(", f"{name} must normalize driver errors")
        require_absent(body, "return -1;", f"{name} must not leak raw -1")

    flush_body = function_body(block_source, "block_flush")
    require(
        flush_body,
        "return -EOPNOTSUPP;",
        "a missing flush callback must not claim that volatile writes are durable",
    )
    require(
        block_ktest,
        "KTEST(BlockDevice, MissingFlushFailsInsteadOfClaimingDurability)",
        "missing durability barriers require executable coverage",
    )
    require(
        block_ktest,
        "KEXPECT_EQ(ker::vfs::sync_blockdev(&dev), -EOPNOTSUPP);",
        "a clean buffer-cache sync must still test its device durability barrier",
    )
    buffer_cache_source = BUFFER_CACHE.read_text()
    sync_body = function_body(buffer_cache_source, "sync_blockdev")
    require(
        sync_body,
        "int const FLUSH_RC = flush_blockdev(bdev);",
        "sync_blockdev must issue a durability barrier even after background writeback emptied the dirty set",
    )
    require_absent(
        sync_body,
        "bdev->flush != nullptr",
        "sync_blockdev must not silently skip an unsupported durability barrier",
    )

    register_body = function_body(block_source, "block_device_register")
    unregister_body = function_body(block_source, "block_device_unregister")
    require(block_source, "struct BlockRegistryEntry", "block /dev wrappers must have stable heap storage")
    require_order(register_body, [
        "new (std::nothrow) BlockRegistryEntry", "entry->node.private_data = bdev",
        "if (dev_register(&entry->node) != 0)", "unlink_entry_locked(entry)", "delete entry",
        "entry->phase = RegistryPhase::PUBLISHED",
    ], "block device register must own stable /dev wrappers and unwind failures")
    require_order(unregister_body, [
        "entry->phase = RegistryPhase::RETIRING", "dev_unregister(&retired->node)",
        "unlink_entry_locked(retired)", "delete retired",
    ], "block device unregister must unpublish and remove its wrapper before freeing it")

    for name in ("partition_read", "partition_write", "partition_flush"):
        body = function_body(block_source, name)
        require_absent(body, "return -1;", f"{name} must not leak raw -1")
        require(body, "return -EINVAL;", f"{name} invalid arguments must use errno")

    for name in ("ahci_flush_blocks", "ahci_read_blocks", "ahci_write_blocks"):
        body = function_body(ahci_source, name)
        require_absent(body, "return -1;", f"{name} must not leak raw -1")
        require(body, "return -EINVAL;", f"{name} invalid arguments must use errno")
        require(body, "-EIO", f"{name} failed disk commands must report I/O error")
        require(body, "native_legacy_sync(", f"{name} must preserve legacy error compatibility through the native bridge")
    legacy_sync = function_body(ahci_source, "native_legacy_sync")
    require_order(legacy_sync, ["block_request_sync(", "RESULT == -ENODEV || RESULT == -ESHUTDOWN ? -EIO : RESULT"],
                  "static legacy providers must retain EIO after native provider teardown")

    require(ahci_source, '#include "platform/sys/mutex.hpp"', "AHCI port command locks must be sleeping locks")
    require(
        ahci_source,
        "std::array<ker::mod::sys::Mutex, MAX_PORTS> port_locks{};",
        "AHCI must serialize non-NCQ commands per port",
    )
    require(
        ahci_source,
        "non-NCQ commands, so the device must not see another command on this port",
        "AHCI completion wait must document non-NCQ serialization",
    )
    for raw in ("phys::page_alloc(", "virt_to_phys", "virt_to_phys_addr"):
        require_absent(ahci_source, raw, "AHCI DMA addresses must come from managed submissions")
    for managed in ("dma::allocate_coherent", "dma::allocate_streaming", "dma::prepare", "dma::complete"):
        require(ahci_source, managed, "AHCI must retain managed DMA ownership")
    prepare = function_body(ahci_source, "port_prepare")
    for ring in ("memory.clb, CLB_SIZE", "memory.fis, FIS_RECEIVE_SIZE", "memory.tables[i], command_table_size"):
        require(prepare, ring, "AHCI rings must use managed contiguous backing")
    install = function_body(ahci_source, "port_install")
    for address in ("memory.clb.submission.address", "memory.fis.submission.address"):
        require(install, address, "AHCI must program managed ring addresses")
    init = function_body(ahci_source, "ahci_init")
    require_order(init, ["port_prepare(i)", "port_install(", "pci::pci_set_bus_master(controller, true)"],
                  "AHCI must prepare and install DMA before enabling bus mastering")
    controller_init = function_body(ahci_source, "ahci_controller_init")
    require_absent(ahci_source, "__kernel_end", "AHCI MMIO placement must not depend on randomized kernel-image bounds")
    require_order(controller_init, ["MutexGuard const LIFECYCLE(lifecycle_lock)", "controller != nullptr || shutdown_attempted",
                                    "pci_find_ahci_controller()", "BAR5 & ~uint32_t{0xf}", "addr::get_hhdm_offset()",
                                    "AHCI_SIZE = 0x2000", "(BAR5 & 1U) == 0", "BAR5 != UINT32_MAX", "PHYSICAL != 0",
                                    "PHYSICAL & (mod::mm::paging::PAGE_SIZE - 1)", "HHDM >= 0xffff800000000000ULL",
                                    "HHDM & (mod::mm::paging::PAGE_SIZE - 1)", "PHYSICAL <= UINT64_MAX - (AHCI_SIZE - 1)",
                                    "HHDM <= UINT64_MAX - PHYSICAL", "AHCI_SIZE - 1 <= UINT64_MAX - HHDM - PHYSICAL",
                                    "AHCI_KERNEL_VADDR = HHDM + PHYSICAL", "pci_enable_memory_space(ahci_dev)",
                                    "map_to_kernel_page_table(AHCI_KERNEL_VADDR + offset, PHYSICAL + offset",
                                    "kernel_mmio_page_flags()", "ahci_set_base(ahci_base)", "ahci_init()"],
                  "controller MMIO must use a checked aligned high-HHDM alias before either fixed page or HBA publication")
    require(controller_init, "pci::pci_set_bus_master(ahci_dev, false)", "invalid BAR/HHDM must explicitly disable bus mastering")
    for forbidden in ("pci_map_bar(", "pci_get_bar_size(", "MMIO_GUARD_GAP"):
        require_absent(controller_init, forbidden, "the bounded fixed-aperture patch must preserve sizing and image independence")
    require(
        ahci_source,
        "constexpr uint32_t AHCI_MAX_SECTORS_PER_CMD = 1024;",
        "AHCI must split large ordinary ATA DMA transfers into conservative commands",
    )
    require_absent(
        ahci_source,
        "new (std::nothrow) uint8_t[CTB_SIZE]",
        "AHCI command tables must not use virtually contiguous heap arrays for DMA",
    )

    command = function_body(ahci_source, "execute_command")
    require(command, "MutexGuard const GUARD(port_lock)", "AHCI must retain the port mutex through completion")
    require_absent(command, ".unlock()", "AHCI must not release the mutex before command retirement")
    require_order(command, ["MutexGuard const GUARD(port_lock)", "admission.allows_command()", "execute_command_locked("],
                  "AHCI must check admission under its port lock")
    command_body = function_body(ahci_source, "execute_command_locked")
    for token in ("transfer.address + offset", "dma::bind_streaming", "COMMAND_TIMEOUT_NS", "dma::Retirement::command_completed"):
        require(command_body, token, "AHCI commands must use bounded managed DMA retirement")
    power = (ROOT / "modules/kern/src/platform/power/power.cpp").read_text()
    require_order(function_body(power, "run_finalizer"), ["vfs_shutdown_sync()", "vfs_shutdown_unmount_all(", "ahci_shutdown()", "halt_other_cores()"],
                  "AHCI shutdown must follow VFS durability and precede CPU halt")
    for name in ("read_write_disk", "identify_disk", "flush_disk"):
        require(function_body(ahci_source, name), "execute_command(", "all ATA commands must use the same serialization and retirement")
    require_native_async_contract(ahci_source)
    require_task_only_block_wait_contract(BLOCK_REQUEST.read_text())
    require_submit_for_wait_contract(BLOCK_REQUEST.read_text())


if __name__ == "__main__":
    main()
