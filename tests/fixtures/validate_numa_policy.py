#!/usr/bin/env python3
"""Strict saved-only healthy NUMA policy output; guest/init/storage authority is external."""
from pathlib import Path
import argparse
import hashlib
import json
import stat

MAX_BYTES = 1024 * 1024
PAGE = 4096
PREFIX = "NUMA_POLICY "


def require(value, why):
    if not value:
        raise ValueError(why)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def decode(text, nonce, domains, pages):
    require(type(nonce) is int and 0 < nonce < 1 << 64 and domains in (1, 2) and type(pages) is int and 1 <= pages <= 256,
            "bounded expected identity")
    raw = text.encode("ascii", "strict")
    require(0 < len(raw) <= MAX_BYTES and text.endswith("\n") and "\r" not in text and "\x00" not in text,
            "complete bounded ASCII output")
    rows = []
    for line in text.splitlines():
        require(line.startswith(PREFIX), "unexpected output line")
        row = json.loads(line[len(PREFIX):], object_pairs_hook=unique_object,
                         parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))
        require(type(row) is dict and row.get("nonce") == nonce and type(row.get("nonce")) is int, "exact invocation nonce")
        require(all(type(value) is int and -(1 << 63) <= value < 1 << 64 or type(value) is str for value in row.values()),
                "flat integer/string record")
        rows.append(row)
    cursor = 0

    def take(kind, fields):
        nonlocal cursor
        require(cursor < len(rows), "missing " + kind)
        row = rows[cursor]
        cursor += 1
        require(set(row) == set(fields.split()) | {"type", "nonce"} and row["type"] == kind, "exact ordered " + kind + " schema")
        return row

    begin = take("begin", "version domains pages pid tid cpu local_node cpus features eligible excluded scope")
    require(begin["version"] == 1 and begin["domains"] == domains and begin["pages"] == pages and begin["features"] == 3,
            "version, public feature mask and fixture coverage")
    require(type(begin["pid"]) is int and begin["pid"] > 1 and begin["tid"] == begin["pid"] and
            1 <= begin["cpus"] <= 64 and 0 <= begin["cpu"] < begin["cpus"] and 0 <= begin["local_node"] < domains,
            "actual parent CPU/process identity")
    require((begin["eligible"], begin["excluded"], begin["scope"]) ==
            ("virgin_private_cpl3_write", "transport_stack_control", "healthy_emulated_placement"), "declared scope")
    masks = []
    for node in range(domains):
        row = take("node", "node managed cpu_mask")
        require(row["node"] == node and row["managed"] > 0 and row["managed"] % PAGE == 0 and
                0 < row["cpu_mask"] < 1 << begin["cpus"] and not (sum(masks) & row["cpu_mask"]), "eligible distinct node")
        masks.append(row["cpu_mask"])
    require(sum(masks) == (1 << begin["cpus"]) - 1 and masks[begin["local_node"]] & (1 << begin["cpu"]),
            "full CPU topology and observed local node")
    wait_children = set()
    actor_count = 0

    def wait(mode, actor):
        row = take("wait", "mode actor pid rc rc_valid status status_valid errno errno_valid consumed owned")
        require((row["mode"], row["actor"]) == (mode, actor) and row["pid"] > 1 and row["pid"] != begin["pid"] and
                row["pid"] not in wait_children, "exact new child wait identity")
        require((row["rc"], row["rc_valid"], row["status"], row["status_valid"], row["errno"], row["errno_valid"],
                 row["consumed"], row["owned"]) == (row["pid"], 1, 0, 1, 0, 0, 1, 0), "actual consuming zero-status wait")
        wait_children.add(row["pid"])
        return row["pid"]

    for mode in range(4):
        wanted = 0 if mode == 0 else (1 << domains) - 1 if mode == 3 else 1 << (1 - begin["local_node"] if domains == 2 else 0)
        row = take("mode", "mode mask set_rc query_rc observed_mode observed_mask")
        require((row["mode"], row["mask"], row["set_rc"], row["query_rc"], row["observed_mode"], row["observed_mask"]) ==
                (mode, wanted, 0, 0, mode, wanted), "actual public setter/query readback")
        for actor in range(3):
            child = wait(mode, actor) if actor == 2 else None
            row = take("actor", "mode actor pid tid cpu affinity address mask observed_mode observed_mask pages virgin resident stable unmapped local phase error map_rc map_errno query_rc change_rc change_query_rc changed_mode changed_mask restore_rc unmap_rc unmap_errno")
            require((row["mode"], row["actor"], row["mask"], row["observed_mode"], row["observed_mask"]) ==
                    (mode, actor, wanted, mode, wanted), "mode and inherited per-Task policy")
            require(row["cpu"] == begin["cpu"] and row["affinity"] == 1 << begin["cpu"], "CPU affinity independent of memory mask")
            require(row["pid"] == (child if actor == 2 else begin["pid"]) and row["tid"] > 0 and
                    (row["tid"] == row["pid"] if actor != 1 else row["tid"] != begin["tid"]), "actor PID/TID ownership")
            require(0 < row["address"] < (1 << 47) - pages * PAGE and row["address"] % PAGE == 0, "bounded aligned payload")
            require((row["pages"], row["virgin"], row["resident"], row["stable"], row["unmapped"], row["phase"]) ==
                    (pages, pages, pages, pages, 1, 5), "full virgin/resident/stable/cleanup coverage")
            require(all(row[key] == 0 for key in ("error", "map_rc", "map_errno", "query_rc", "change_rc", "change_query_rc", "restore_rc", "unmap_rc", "unmap_errno")),
                    "no hidden actor syscall or cleanup failure")
            require((row["changed_mode"], row["changed_mask"]) == (2, 1 << begin["local_node"]) if mode == 0 else
                    (row["changed_mode"], row["changed_mask"]) == (0, 0), "actual changed-actor policy readback")
            local = 0
            members = [node for node in range(domains) if wanted & (1 << node)]
            for index in range(pages):
                page = take("page", "mode actor index va node")
                address = row["address"] + index * PAGE
                require((page["mode"], page["actor"], page["index"], page["va"]) == (mode, actor, index, address) and
                        0 <= page["node"] < domains, "every ordered resident page")
                if mode:
                    expected = members[(address // PAGE) % len(members)] if mode == 3 else members[0]
                    require(page["node"] == expected, "exact requested node or absolute-VA interleave phase")
                local += page["node"] == begin["local_node"]
            require(row["local"] == local and (mode != 0 or local * 100 >= pages * 95), "all eligible LOCAL pages meet95percent")
            actor_count += 1
        wait(mode, 3)
        quota = take("quota", "mode before accepted final denied_before denied_after first_rc first_errno second_rc second_errno restored removed child_consumed inherited_soft inherited_hard saved_soft saved_hard final_soft final_hard error")
        require(quota["mode"] == mode and quota["before"] >= 0 and quota["accepted"] == quota["before"] + PAGE and
                quota["final"] == quota["before"] and quota["denied_after"] > quota["denied_before"] >= 0, "exact canonical AS charge/refusal/release")
        require((quota["first_rc"], quota["first_errno"], quota["second_rc"], quota["second_errno"], quota["restored"],
                 quota["removed"], quota["child_consumed"], quota["error"]) == (0, 0, -1, 12, 0, 1, 1, 0), "AS failures separate from physical exhaustion")
        require(quota["inherited_soft"] == quota["before"] + PAGE <= quota["saved_soft"] <= quota["saved_hard"] and
                quota["inherited_hard"] == quota["saved_hard"] == quota["final_hard"] and
                quota["final_soft"] == quota["saved_soft"], "inherited bound and exact original soft/hard restoration")
    end = take("end", "pass modes threads_joined children_consumed fds own_complete policy_restore affinity_restore error pressure huge")
    require(tuple(end[k] for k in ("pass", "modes", "threads_joined", "children_consumed", "fds", "own_complete", "policy_restore", "affinity_restore", "error", "pressure", "huge")) ==
            (1, 4, 4, 8, 0, 1, 0, 0, 0, 0, 0), "complete healthy mode and all owned cleanup")
    require(cursor == len(rows) and actor_count == 12 and len(wait_children) == 8, "no omitted/duplicate/extra records")
    return dict(qualification="PASS_SAVED_ONLY_HEALTHY_PUBLIC_POLICY_PROTOCOL", runtime_qualified=False,
                nonce=nonce, domains=domains, pages=pages, modes=4, actors=actor_count, children_consumed=8,
                pressure_qualified=False, physical_performance_qualified=False, sha256=hashlib.sha256(raw).hexdigest())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path)
    parser.add_argument("--nonce", type=int, required=True)
    parser.add_argument("--domains", type=int, choices=(1, 2), required=True)
    parser.add_argument("--pages", type=int, required=True)
    args = parser.parse_args()
    before = args.log.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and 0 < before.st_size <= MAX_BYTES, "bounded regular saved log")
    raw = args.log.read_bytes()
    after = args.log.lstat()
    require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
            (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns), "saved log changed")
    print(json.dumps(decode(raw.decode("ascii", "strict"), args.nonce, args.domains, args.pages), sort_keys=True))


if __name__ == "__main__":
    main()
