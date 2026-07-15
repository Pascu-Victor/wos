#!/usr/bin/env python3

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
RENDERBENCH_MAIN_CPP = ROOT / "modules" / "renderbench" / "src" / "main.cpp"
VFS_CORE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void|int)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
        source,
        flags=re.DOTALL,
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


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, before: str, after: str, context: str) -> None:
    before_pos = source.find(before)
    after_pos = source.find(after)
    if before_pos < 0 or after_pos < 0 or before_pos >= after_pos:
        fail(f"{context}: expected {before!r} before {after!r}")


def test_renderbench_worker_reap_is_deadline_bounded() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    require_tokens(
        source,
        [
            "WORKER_EXIT_KILL_AFTER_SECONDS",
            "WORKER_EXIT_GIVE_UP_AFTER_SECONDS",
            "sleep_worker_wait_poll",
        ],
        "renderbench worker reap timeout surface",
    )

    wait_body = function_body(source, "wait_for_child")
    if "::waitpid(worker.pid, &status, 0)" in wait_body:
        fail("renderbench wait_for_child must not use a blocking waitpid")

    require_tokens(
        wait_body,
        [
            "::waitpid(worker.pid, &status, WNOHANG)",
            "WAITED == worker.pid",
            "note_worker_exit_status(worker, status, cancellation_expected)",
            "WORKER_EXIT_KILL_AFTER_SECONDS",
            "::kill(worker.pid, SIGKILL)",
            "WORKER_EXIT_GIVE_UP_AFTER_SECONDS",
            "sleep_worker_wait_poll()",
        ],
        "renderbench bounded wait_for_child",
    )

    children_body = function_body(source, "wait_for_children")
    require_tokens(
        children_body,
        ["wait_for_child(worker, cancellation_expected)"],
        "renderbench wait_for_children plumbing",
    )


def test_renderbench_worker_cancellation_is_cooperative() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    main_body = function_body(source, "main")
    require_order(
        main_body,
        "install_cancel_signal_handlers();",
        "if (worker.enabled)",
        "renderbench worker mode must install signal handlers before entering worker loop",
    )

    for name in [
        "read_exact",
        "write_all",
        "write_buffered_tile_packets",
        "render_worker_tile_packet",
        "batch_render_thread",
    ]:
        body = function_body(source, name)
        if "cancel_requested()" not in body:
            fail(f"{name} must observe cooperative cancellation")


def test_renderbench_worker_stream_corruption_is_fatal() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    body = function_body(source, "parse_worker_packets")
    if "resynchronized" in body:
        fail("renderbench must not resynchronize through corrupt worker byte streams")
    require_tokens(
        body,
        [
            "corrupt worker stream",
            "worker.done_failed = true;",
            "return false;",
        ],
        "renderbench corrupt worker stream handling",
    )


def test_renderbench_worker_pipe_read_uses_uninitialized_scratch() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    vfs_source = VFS_CORE_CPP.read_text()
    require_tokens(
        source,
        ["constexpr size_t WORKER_PIPE_READ_CHUNK = 4096;"],
        "renderbench worker pipe read chunk",
    )
    require_tokens(
        vfs_source,
        [
            "constexpr size_t PIPE_COPY_CHUNK = 4096;",
            "std::array<char, PIPE_COPY_CHUNK> bounce",
            "std::min({count, st->count, bounce.size()})",
            "constexpr size_t USER_IO_BOUNCE_STACK_CHUNK = size_t{16} * 1024;",
            "if (bounce_size > stack_bounce.size())",
        ],
        "local pipe read ceiling",
    )
    body = function_body(source, "drain_ready_worker_pipe")
    declaration = (
        "std::array<unsigned char, WORKER_PIPE_READ_CHUNK> chunk "
        "__attribute__((uninitialized));"
    )
    read_call = (
        "ssize_t const READ = "
        "::read(worker.read_fd, chunk.data(), chunk.size());"
    )
    insert_prefix = (
        "worker.buffer.insert(worker.buffer.end(), chunk.begin(), "
        "chunk.begin() + static_cast<ptrdiff_t>(READ));"
    )
    require_tokens(
        body,
        [declaration, read_call, "if (READ > 0) {", insert_prefix],
        "renderbench worker pipe read scratch",
    )
    require_order(
        body,
        declaration,
        read_call,
        "renderbench must allocate pipe scratch before read",
    )
    require_order(
        body,
        "if (READ > 0) {",
        insert_prefix,
        "renderbench must consume only a positive read prefix",
    )
    for forbidden in ["chunk = {}", "chunk.fill(", "memset(chunk"]:
        if forbidden in body:
            fail("renderbench worker pipe read scratch must not be cleared")


def test_renderbench_distributed_ipc_uses_chunk_safe_default_tiles() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    require_tokens(
        source,
        [
            "IPC_SAFE_DEFAULT_TILE_SIZE = 24",
            "apply_distributed_ipc_defaults",
            "options.tile_size_explicit",
            "exceeds chunk-safe max",
        ],
        "renderbench distributed IPC tile-size default",
    )

    main_body = function_body(source, "main")
    require_order(
        main_body,
        "apply_distributed_ipc_defaults(options);",
        "return run_distributed_ipc(options, peers, argv[0]);",
        "renderbench must apply distributed IPC defaults before launching workers",
    )


def test_renderbench_single_node_node_threads_use_ipc_coordinator() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    main_body = function_body(source, "main")
    if "peers.size() <= 1" in main_body:
        fail("single-node node-thread renders must use the IPC coordinator")
    require_order(
        main_body,
        "auto peers = read_wki_peers();",
        "apply_distributed_ipc_defaults(options);",
        "renderbench must discover the local peer before IPC setup",
    )
    require_order(
        main_body,
        "apply_distributed_ipc_defaults(options);",
        "return run_distributed_ipc(options, peers, argv[0]);",
        "renderbench must route single-node node-thread renders through IPC",
    )


def test_renderbench_worker_child_closes_inherited_fds() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    require_tokens(
        source,
        [
            "WORKER_CHILD_FD_CLOSE_LIMIT = 256",
            "close_worker_child_extra_fds",
        ],
        "renderbench worker child fd hygiene surface",
    )

    close_body = function_body(source, "close_worker_child_extra_fds")
    require_tokens(
        close_body,
        [
            "int fd = 3",
            "fd < WORKER_CHILD_FD_CLOSE_LIMIT",
            "(void)::close(fd);",
        ],
        "renderbench worker child fd close loop",
    )

    exec_body = function_body(source, "exec_worker_child")
    require_order(
        exec_body,
        "close_worker_child_extra_fds();",
        "install_worker_scene_vfs_policy(options, spec);",
        "renderbench worker child must close inherited coordinator fds before WKI targeting/exec",
    )


def test_renderbench_coordinator_stall_report_exposes_batch_state() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    require_tokens(
        source,
        [
            "COORDINATOR_STALL_REPORT_SECONDS",
            "print_worker_stall_report",
            "assigned_batches",
            "batch_done_packets",
            "phase_packets",
            "WorkerPhasePacket",
            "PHASE_PACKET_MAGIC",
            "note_worker_phase_packet",
            "last_batch_done_at",
            "last_assignment_at",
            "last_phase_at",
            "worker.local",
            "no completed tile progress",
            "phase={}",
        ],
        "renderbench coordinator stall report surface",
    )

    run_body = function_body(source, "run_distributed_ipc")
    require_tokens(
        run_body,
        [
            "last_stall_report_tiles_done",
            "next_stall_report",
            "tiles_done != last_stall_report_tiles_done",
            "print_worker_stall_report",
        ],
        "renderbench distributed IPC stall report loop",
    )
    require_order(
        run_body,
        "print_worker_stall_report(",
        "wait_for_worker_pipe_activity(",
        "renderbench must report stalled worker state before polling again",
    )


def test_renderbench_command_stream_uses_per_batch_thread_join() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    if "CommandStreamThreadPool" in source or "command_stream_worker_thread" in source:
        fail(
            "renderbench command-stream batches must not use a persistent condition-variable thread pool"
        )

    run_body = function_body(source, "run_ipc_worker")
    require_tokens(
        run_body,
        [
            "std::vector<tracebench::Tile> batch_tiles;",
            "std::vector<std::vector<unsigned char> > batch_packets",
            "std::vector<thrd_t> batch_threads",
            "batch_render_thread",
            "thrd_join(batch_threads.at",
            "write_buffered_tile_packets(WORKER_STDOUT_FD",
            "send_worker_phase_packet",
            "send_worker_batch_done_packet",
        ],
        "renderbench command-stream per-batch thread join path",
    )
    require_order(
        run_body,
        "thrd_join(batch_threads.at",
        "write_buffered_tile_packets(WORKER_STDOUT_FD",
        "renderbench must join batch threads before coalescing their packets",
    )
    require_order(
        run_body,
        "thrd_join(batch_threads.at",
        "send_worker_batch_done_packet",
        "renderbench must join batch threads before sending batch-done",
    )
    require_order(
        run_body,
        "write_buffered_tile_packets(WORKER_STDOUT_FD",
        "send_worker_batch_done_packet",
        "renderbench command-stream worker main thread must send tile packets before batch-done",
    )


def test_renderbench_non_live_tiles_use_bounded_write_batches() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    require_tokens(
        source,
        [
            "WORKER_TILE_WRITE_BATCH_BYTES = static_cast<size_t>(1U) * 1024U * 1024U",
            "write_buffered_tile_packets",
        ],
        "renderbench bounded tile write batching surface",
    )

    helper = function_body(source, "write_buffered_tile_packets")
    require_tokens(
        helper,
        [
            "output_batch.reserve(reserve_bytes)",
            "output_batch.insert(output_batch.end(), packet.begin(), packet.end())",
            "packet.size() > WORKER_TILE_WRITE_BATCH_BYTES",
            "packet.empty()",
            "(void)flush_output_batch()",
            "WORKER_TILE_WRITE_BATCH_BYTES - output_batch.size()",
            "write_all(fd",
            "send_seconds += tracebench::monotonic_seconds() - SEND_STARTED",
            "sent_tiles += buffered_packets",
            "++sent_tiles",
            "cancel_requested()",
            "return flush_output_batch();",
        ],
        "renderbench bounded tile write batching helper",
    )
    require_order(
        helper,
        "if (!SENT)",
        "sent_tiles += buffered_packets",
        "renderbench must count a buffered tile group only after its write completes",
    )

    run_body = function_body(source, "run_ipc_worker")
    if run_body.count("write_buffered_tile_packets(WORKER_STDOUT_FD") != 2:
        fail("renderbench must batch both command-stream and one-shot non-live tile output")
    non_live_blocks = re.findall(
        r"if \(!STREAM_PACKETS\) \{(?P<body>.*?)\n\s*\}",
        run_body,
        flags=re.DOTALL,
    )
    if len(non_live_blocks) != 2 or any(
        "write_buffered_tile_packets(WORKER_STDOUT_FD" not in block
        for block in non_live_blocks
    ):
        fail("renderbench tile coalescing must remain confined to both non-live output blocks")
    if "write_all(WORKER_STDOUT_FD, std::span<const unsigned char>(packet.data(), packet.size()))" in run_body:
        fail("renderbench non-live worker paths must not retain one write_all call per tile")
    if "for (const auto& packet : batch_packets)" in run_body:
        fail("renderbench non-live worker paths must not retain per-tile output loops")

    one_shot = run_body[run_body.find("std::vector<tracebench::Tile> assigned_tiles;") :]
    require_order(
        one_shot,
        "write_buffered_tile_packets(WORKER_STDOUT_FD",
        "send_worker_done_packet",
        "renderbench one-shot worker must flush tile batches before done",
    )


def test_renderbench_node_threads_avoid_persistent_command_stream() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    run_body = function_body(source, "run_distributed_ipc")
    require_tokens(
        run_body,
        [
            "USE_PERSISTENT_PROCESS_BATCHES =\n        options.placement == tracebench::Placement::ProcessPerCore && options.process_persistent_workers",
            "USE_DYNAMIC_BATCHES = !USE_PERSISTENT_PROCESS_BATCHES && (options.placement == tracebench::Placement::NodeThreads",
            "spec.command_stream = USE_PERSISTENT_PROCESS_BATCHES",
        ],
        "renderbench node-thread command-stream policy",
    )

    worker_body = function_body(source, "run_ipc_worker")
    require_tokens(
        worker_body,
        [
            "int const THREADS = std::max(1, worker.worker_threads);",
            "std::vector<thrd_t> threads(static_cast<size_t>(THREADS));",
            "thrd_create(&threads[static_cast<size_t>(i)], batch_render_thread",
        ],
        "renderbench node-thread worker thread fanout",
    )


def test_renderbench_live_mode_streams_worker_tiles() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    require_tokens(
        source,
        [
            "stream_packets",
            "LiveTileOutputQueue",
            "start_live_output_queue",
            "finish_live_output_queue",
            "push_live_output_packet",
            "live_output_writer_thread",
            "cnd_wait(&queue->not_empty",
            "cnd_signal(&queue.not_empty)",
            "options.live_preview",
            "LIVE_PROGRESS_PREVIEW",
            "STARTED + options.preview_update_interval_seconds",
        ],
        "renderbench live preview async streaming surface",
    )

    render_packet_body = function_body(source, "render_worker_tile_packet")
    if "write_all(" in render_packet_body:
        fail("renderbench live mode must not write worker pipes from render threads")
    require_order(
        render_packet_body,
        "state.tiles_rendered->fetch_add",
        "push_live_output_packet",
        "renderbench live mode must enqueue tile packets after render",
    )

    writer_body = function_body(source, "live_output_writer_thread")
    require_tokens(
        writer_body,
        [
            "write_all(queue->output_fd",
            "queue->send_seconds",
            "++queue->sent",
        ],
        "renderbench live output writer accounting",
    )


def test_renderbench_ipc_profile_separates_capacity_from_dynamic_runs() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    require_tokens(
        source,
        [
            "size_t configured_slots = 0;",
            "size_t configured_threads = 0;",
            '\\"configured_slots\\"',
            '\\"configured_threads\\"',
        ],
        "renderbench configured IPC capacity surface",
    )

    initialize_body = function_body(source, "initialize_process_ipc_profile")
    require_tokens(
        initialize_body,
        [
            "profile.worker_slots = specs.size();",
            "++host->configured_slots;",
            "host->configured_threads +=",
            "spec.worker_threads",
        ],
        "renderbench initial IPC capacity accounting",
    )

    note_body = function_body(source, "note_process_ipc_profile")
    require_tokens(
        note_body,
        ["++profile.completed_runs;", "++host->completed_runs;"],
        "renderbench dynamic IPC run accounting",
    )
    if "configured_slots" in note_body or "configured_threads" in note_body:
        fail("renderbench dynamic completions must not mutate configured IPC capacity")

    write_body = function_body(source, "write_process_ipc_profile_json")
    require_tokens(
        write_body,
        ['\\"configured_slots\\"', '\\"configured_threads\\"'],
        "renderbench configured IPC capacity JSON",
    )
    if "if (host.completed_runs == 0)" in write_body:
        fail(
            "renderbench IPC profile must retain configured hosts with no completed work"
        )

    run_body = function_body(source, "run_distributed_ipc")
    require_order(
        run_body,
        "initialize_process_ipc_profile(ipc_profile",
        "for (size_t i = 0; i < specs.size(); ++i)",
        "renderbench must snapshot configured capacity before launching workers",
    )


def test_renderbench_dynamic_launch_reuses_receive_capacity() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    launch_body = function_body(source, "launch_worker")
    require_order(
        launch_body,
        "auto reusable_buffer = std::move(out.buffer);",
        "reusable_buffer.clear();",
        "renderbench must retain capacity before discarding old receive bytes",
    )
    require_order(
        launch_body,
        "reusable_buffer.clear();",
        "out = {",
        "renderbench must clear old receive bytes before resetting worker state",
    )
    require_order(
        launch_body,
        ".buffer = {},",
        "out.buffer = std::move(reusable_buffer);",
        "renderbench must restore only cleared receive-buffer storage after reset",
    )
    require_order(
        launch_body,
        "out.buffer = std::move(reusable_buffer);",
        "out.buffer.reserve(WORKER_PIPE_BUFFER_RESERVE);",
        "renderbench must preserve the minimum receive-buffer reserve after reuse",
    )
    if "shrink_to_fit" in launch_body:
        fail("renderbench launch must not discard reusable receive-buffer capacity")

    run_body = function_body(source, "run_distributed_ipc")
    reaped_worker = run_body.find("bool const CHILD_OK = wait_for_child(worker")
    if reaped_worker < 0:
        fail("renderbench dynamic relaunch lacks its child-reap gate")
    relaunch_body = run_body[reaped_worker:]
    require_order(
        relaunch_body,
        "note_process_ipc_profile(ipc_profile, worker, STARTED);",
        "launch_next_worker_batch(",
        "renderbench must profile the completed child before reusing its worker slot",
    )


def main() -> None:
    test_renderbench_worker_reap_is_deadline_bounded()
    test_renderbench_worker_cancellation_is_cooperative()
    test_renderbench_worker_stream_corruption_is_fatal()
    test_renderbench_worker_pipe_read_uses_uninitialized_scratch()
    test_renderbench_distributed_ipc_uses_chunk_safe_default_tiles()
    test_renderbench_single_node_node_threads_use_ipc_coordinator()
    test_renderbench_worker_child_closes_inherited_fds()
    test_renderbench_coordinator_stall_report_exposes_batch_state()
    test_renderbench_command_stream_uses_per_batch_thread_join()
    test_renderbench_non_live_tiles_use_bounded_write_batches()
    test_renderbench_node_threads_avoid_persistent_command_stream()
    test_renderbench_live_mode_streams_worker_tiles()
    test_renderbench_ipc_profile_separates_capacity_from_dynamic_runs()
    test_renderbench_dynamic_launch_reuses_receive_capacity()
    print("renderbench worker source invariants hold")


if __name__ == "__main__":
    main()
