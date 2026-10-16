#!/usr/bin/env python3
"""Strict saved user-record validation. No WOS/VM/kernel qualification."""
import argparse
from array import array
from functools import lru_cache
import json
import math
from pathlib import Path
import statistics

PREFIX = "NUMA_CHASE_V1 "
DEFAULT = dict(pairs=10, bytes=67108864, stride=64, loads=16777216, seed=0x402109,
               chase_cpu=0, local_init_cpu=0, remote_init_cpu=4, timeout_ms=600000)
FIELDS = {
    'profile': 'version kind pairs bytes stride loads seed timeout_ms chase_cpu local_init_cpu remote_init_cpu chase_node local_node remote_node node_count cpu_count provenance clock_resolution_ns'.split(),
    'sample': 'version kind pair order condition seed bytes stride loads elapsed_ns checksum start_index warm_checksum warm_loads chase_cpu init_cpu chase_node expected_node cpu_before cpu_after mask_before mask_after init_cpu_before init_cpu_after init_mask_before init_mask_after node_count pages virgin_pages before_histogram after_histogram clock_resolution_ns status phase'.split(),
    'cleanup': 'version kind pair condition unmapped error created_threads joined_threads'.split(),
    'failure': 'version kind pair condition status phase'.split(),
    'end': 'version kind status samples completed_pairs created_threads joined_threads mappings_created mappings_unmapped peak_mappings'.split(),
}


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON key')
        result[key] = value
    return result


def integer(value, low=0, high=(1 << 64) - 1):
    return type(value) is int and low <= value <= high


@lru_cache(maxsize=4)
def expected_indices(byte_count, loads, seed):
    count = byte_count // 64
    indices = array('I', range(count))
    state = seed ^ 0x9e3779b97f4a7c15
    if state == 0:
        state = 1
    mask = (1 << 64) - 1
    for i in range(count - 1, 0, -1):
        state ^= (state << 13) & mask
        state ^= state >> 7
        state ^= (state << 17) & mask
        j = state % (i + 1)
        indices[i], indices[j] = indices[j], indices[i]
    return indices[0], indices[loads % count]


def summary(values):
    require(len(values) >= 1 and all(math.isfinite(x) and x > 0 for x in values), 'invalid statistics input')
    variance = statistics.variance(values) if len(values) > 1 else None
    mean = statistics.mean(values)
    deviation = math.sqrt(variance) if variance is not None else None
    return dict(count=len(values), raw=values, mean=mean, median=statistics.median(values),
                sample_variance=variance, sample_stddev=deviation, coefficient_of_variation=deviation / mean if deviation is not None else None,
                minimum=min(values), maximum=max(values))


def decode(text, require_default=True):
    require(isinstance(text, str) and text.endswith('\n'), 'input lacks complete LF tail')
    records = []
    for line in text.split('\n')[:-1]:
        if not line.startswith(PREFIX):
            continue
        require(all(32 <= ord(c) <= 126 for c in line), 'non-ASCII/control record boundary')
        require(len(line) <= 16384, 'record extent')
        row = json.loads(line[len(PREFIX):], object_pairs_hook=unique_object)
        require(type(row) is dict and row.get('kind') in FIELDS, 'record kind')
        require(set(row) == set(FIELDS[row['kind']]) and row['version'] == 1 and type(row['version']) is int, 'record schema')
        require(row['kind'] != 'failure', 'actual failure record')
        records.append(row)
        require(len(records) <= 64, 'too many records')
    require(records and records[0]['kind'] == 'profile' and records[-1]['kind'] == 'end', 'profile/end ordering')
    profile = records[0]
    require(all(integer(profile[k]) for k in profile if k != 'kind'), 'profile scalar types')
    require(1 <= profile['pairs'] <= 10 and (1 << 20) <= profile['bytes'] <= (64 << 20) and profile['bytes'] % 4096 == 0,
            'profile size bounds')
    require(profile['stride'] == 64 and 1 <= profile['loads'] <= (1 << 28) and 1000 <= profile['timeout_ms'] <= 600000,
            'profile work/deadline')
    require(2 <= profile['node_count'] <= 64 and 1 <= profile['cpu_count'] <= 256 and profile['provenance'] <= 3,
            'profile topology')
    require(profile['chase_node'] == profile['local_node'] != profile['remote_node'] and
            all(profile[k] < profile['node_count'] for k in ['chase_node', 'local_node', 'remote_node']), 'node tuple')
    require(all(profile[k] < min(profile['cpu_count'], 64) for k in ['chase_cpu', 'local_init_cpu', 'remote_init_cpu']), 'CPU tuple')
    require(profile['clock_resolution_ns'] > 0, 'clock resolution')
    if require_default:
        require(all(profile[k] == value for k, value in DEFAULT.items()), 'not frozen ten-pair campaign')
    pairs = profile['pairs']
    samples = records[1:1 + pairs * 2]
    cleanup = records[1 + pairs * 2:-1]
    require(len(samples) == pairs * 2 and len(cleanup) == pairs * 2 and all(r['kind'] == 'sample' for r in samples) and
            all(r['kind'] == 'cleanup' for r in cleanup), 'exact sample/cleanup count and ordering')
    expected_start, expected_final = expected_indices(profile['bytes'], profile['loads'], profile['seed'])
    work_keys = ['seed', 'bytes', 'stride', 'loads', 'chase_cpu', 'chase_node', 'node_count', 'clock_resolution_ns']
    for index, row in enumerate(samples):
        require(all(integer(v) for k, v in row.items() if k not in ['kind', 'order', 'condition', 'phase', 'before_histogram', 'after_histogram']), 'sample scalar types')
        pair = index // 2
        condition = ('local', 'remote')[index % 2] if pair % 2 == 0 else ('remote', 'local')[index % 2]
        require(row['pair'] == pair and row['order'] == ('LR' if pair % 2 == 0 else 'RL') and row['condition'] == condition, 'paired sample identity/order')
        require(all(row[k] == profile[k] for k in work_keys), 'common work/clock/CPU changed')
        require(row['status'] == 0 and row['phase'] == 'complete' and 0 < row['elapsed_ns'] <= profile['timeout_ms'] * 1000000,
                'sample failure/time')
        cpu = profile['remote_init_cpu'] if condition == 'remote' else profile['local_init_cpu']
        node = profile['remote_node'] if condition == 'remote' else profile['local_node']
        require(row['init_cpu'] == cpu and row['expected_node'] == node and row['cpu_before'] == row['cpu_after'] == profile['chase_cpu'] and
                row['mask_before'] == row['mask_after'] == 1 << profile['chase_cpu'] and
                row['init_cpu_before'] == row['init_cpu_after'] == cpu and row['init_mask_before'] == row['init_mask_after'] == 1 << cpu,
                'actual CPU/mask mismatch')
        pages = profile['bytes'] // 4096
        require(row['pages'] == row['virgin_pages'] == pages, 'virgin denominator')
        for name in ['before_histogram', 'after_histogram']:
            values = row[name]
            require(type(values) is list and len(values) == 64 and all(integer(v, 0, pages) for v in values), 'full histogram shape')
            require(sum(values) == pages and values[node] == pages, 'not exact all-page placement')
        require(row['start_index'] == row['warm_checksum'] == expected_start and row['checksum'] == expected_final and
                row['warm_loads'] == profile['bytes'] // 64, 'dependent cycle/work/checksum')
    for index, row in enumerate(cleanup):
        source = samples[index]
        require(row['pair'] == source['pair'] and type(row['pair']) is int and row['condition'] == source['condition'], 'cleanup identity/order')
        require(row['unmapped'] is True and type(row['error']) is int and row['error'] == 0 and
                type(row['created_threads']) is int and type(row['joined_threads']) is int and
                row['created_threads'] == row['joined_threads'] == 3, 'cleanup without three proven joins')
    end = records[-1]
    require(all(integer(v) for k, v in end.items() if k != 'kind'), 'end scalar types')
    require(end['status'] == 0 and end['samples'] == end['mappings_created'] == end['mappings_unmapped'] == end['peak_mappings'] == 2 * pairs and
            end['completed_pairs'] == pairs and end['created_threads'] == end['joined_threads'] == 3, 'terminal owner/work closure')
    local = [row['elapsed_ns'] / row['loads'] for row in samples if row['condition'] == 'local']
    remote = [row['elapsed_ns'] / row['loads'] for row in samples if row['condition'] == 'remote']
    # Source order inside each condition is pair order even when RL alternates.
    ratios = [r / l for l, r in zip(local, remote)]
    differences = [r - l for l, r in zip(local, remote)]
    result = dict(qualification='PASS_SAVED_EMULATED_NUMA_POINTER_CHASE_PROTOCOL_ONLY' if require_default else 'PASS_SMOKE_PROTOCOL_ONLY',
                  profile=profile, samples=samples, cleanup=cleanup, end=end, ns_per_load=dict(local=summary(local), remote=summary(remote)),
                  paired_remote_over_local=ratios, paired_remote_minus_local_ns_per_load=differences,
                  median_paired_ratio=statistics.median(ratios), ratio_of_medians=statistics.median(remote) / statistics.median(local),
                  physical_numa_threshold='unverified_physical_numa', kernel_qualified=False, vm_qualified=False,
                  actual_clocksource_requires_bound_boot_log=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('--allow-smoke', action='store_true')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    try:
        result = decode(args.input.read_bytes().decode('utf-8', errors='strict'), not args.allow_smoke)
        encoded = json.dumps(result, indent=2, allow_nan=False) + '\n'
        if args.output:
            with args.output.open('x') as f:
                f.write(encoded)
        print(encoded, end='')
        return 0
    except (ValueError, OSError, KeyError, TypeError) as error:
        print(json.dumps(dict(qualification='FAIL_SAVED_NUMA_CHASE_PROTOCOL', error=str(error), kernel_qualified=False, vm_qualified=False)))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
