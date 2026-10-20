#!/usr/bin/env python3
"""Observe the kernel's actual sanitizer CMake properties without compiling.

Each project deliberately refuses configuration after exporting its properties;
the expected fatal marker is not a successful configure or build result.
"""
from pathlib import Path
import argparse
import itertools
import os
import re
import subprocess
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[3]
BEGIN = '# Sanitizer flags (applied on top of the base kernel options)\n'
END = '\nif(WOS_SELFTEST)\n'
SHARED_CONDITION = 'if(WOS_KASAN OR WOS_KUBSAN OR WOS_KCOV)\n'
MARKER = 'WOS_SANITIZER_PROPERTIES_COMPLETE_BEFORE_GENERATION'
ORDINARY = 'src/test/numa_lifecycle_census.cpp'
BALANCE = 'src/platform/mm/numa_balance.cpp'
REQUIRED_CLOCK_SOURCES = {
    'tsc': 'src/platform/tsc/tsc.cpp',
    'kvmclock': 'src/platform/ktime/kvmclock.cpp',
}
SECONDS = 120
COMMAND_SECONDS = 10
OUTPUT_CAP = 1 << 20
CMAKE = None


def bounded_read(path):
    with path.open('rb') as stream:
        raw = stream.read(OUTPUT_CAP + 1)
    if len(raw) > OUTPUT_CAP:
        raise ValueError('sanitizer property input/output exceeds 1 MiB')
    return raw


def selected_block():
    text = bounded_read(ROOT / 'modules/kern/CMakeLists.txt').decode()
    if text.count(BEGIN) != 1 or text.count(END) != 1:
        raise ValueError('unique complete sanitizer block required')
    block = text[text.index(BEGIN):text.index(END)]
    if block.count(SHARED_CONDITION) != 1:
        raise ValueError('unique shared sanitizer-runtime exclusion condition required')
    return block


def excluded_sources(block):
    match = re.search(r'^    set\(KASAN_EXCLUDED_SRCS\n(.*?)^    \)\n', block, re.M | re.S)
    if match is None:
        raise ValueError('complete ordered source declaration required')
    names = [line.strip() for line in match[1].splitlines()
             if line.strip() and not line.lstrip().startswith('#')]
    if (not names or len(names) != len(set(names))
            or not all(re.fullmatch(r'src/[A-Za-z0-9_./]+', name) for name in names)
            or ORDINARY in names or BALANCE in names):
        raise ValueError('unique exclusions and separate ordinary/balance probes required')
    return names


PREFIX = '''cmake_minimum_required(VERSION 3.20)
project(wos_sanitizer_properties NONE)
# Real target commands, with no compiler language or generation phase.
set(KERNEL wos)
add_library(wos STATIC)
'''
SUFFIX = '''
function(port_emit label value)
    string(HEX "${value}" encoded)
    file(APPEND "${CMAKE_CURRENT_SOURCE_DIR}/properties.txt" "${label}=${encoded}\\n")
endfunction()
foreach(property COMPILE_OPTIONS COMPILE_DEFINITIONS LINK_OPTIONS)
    get_target_property(value wos ${property})
    if(value STREQUAL "value-NOTFOUND")
        set(value "")
    endif()
    port_emit("target:${property}" "${value}")
endforeach()
foreach(source IN LISTS PORT_SOURCES)
    get_source_file_property(value "${CMAKE_CURRENT_SOURCE_DIR}/${source}" COMPILE_OPTIONS)
    if(value STREQUAL "NOTFOUND")
        set(value "")
    endif()
    port_emit("source:${source}" "${value}")
endforeach()
port_emit("excluded_sources" "${KASAN_EXCLUDED_SRCS}")
port_emit("excluded_options" "${KASAN_EXCLUDED_COMPILE_OPTIONS}")
port_emit("complete" "yes")
# Stop before compiler identification, source generation or build rules.
message(FATAL_ERROR "WOS_SANITIZER_PROPERTIES_COMPLETE_BEFORE_GENERATION")
'''


class KernelSanitizerCMakeTest(unittest.TestCase):
    @classmethod
    def check_deadline(cls):
        if time.monotonic() >= cls.deadline:
            raise TimeoutError('120-second whole sanitizer property deadline')

    @classmethod
    def setUpClass(cls):
        cls.deadline = time.monotonic() + SECONDS
        block = selected_block()
        # This restores the actual former condition; every command/list remains
        # the current source body. No historical file or copied policy is used.
        old_condition = block.replace(SHARED_CONDITION, 'if(WOS_KASAN)\n', 1)
        cls.sources = excluded_sources(block)
        # Probe these required fault-runtime dependencies even if the actual
        # exclusion declaration accidentally loses one of them.
        cls.probes = list(dict.fromkeys(cls.sources + list(REQUIRED_CLOCK_SOURCES.values()) + [ORDINARY, BALANCE]))
        variants = [('current', block), ('old-condition', old_condition)]
        for name, source in REQUIRED_CLOCK_SOURCES.items():
            declaration = '        ' + source + '\n'
            if block.count(declaration) != 1:
                raise ValueError('unique required clock runtime declaration: ' + source)
            variants.append(('missing-' + name, block.replace(declaration, '', 1)))
        cls.rows = {}
        environment = dict(os.environ)
        for key in ('CC', 'CXX', 'CFLAGS', 'CXXFLAGS', 'LDFLAGS',
                    'CMAKE_TOOLCHAIN_FILE', 'CMAKE_PROJECT_INCLUDE',
                    'CMAKE_PROJECT_INCLUDE_BEFORE', 'CMAKE_PROJECT_TOP_LEVEL_INCLUDES'):
            environment.pop(key, None)
        with tempfile.TemporaryDirectory(prefix='wos-sanitizer-properties-') as temporary:
            root = Path(temporary)
            for flags in itertools.product((False, True), repeat=4):
                for label, actual in variants:
                    cls.check_deadline()
                    directory = root / (label + '-' + ''.join('1' if flag else '0' for flag in flags))
                    directory.mkdir()
                    variables = dict(zip(('WOS_KASAN', 'WOS_KUBSAN', 'WOS_KCOV', 'WOS_KCFI'), flags))
                    variables.update(WOS_KERNEL_UBSAN_TRAP=False, WOS_KCOV_NO_PRUNE=True,
                                     WOS_KCOV_SOURCE_FRIENDLY=False, WOS_KCOV_PANIC_TRACE=flags[2])
                    data = PREFIX + ''.join('set(' + key + ' ' + ('ON' if value else 'OFF') + ')\n'
                                           for key, value in variables.items())
                    data += 'set(PORT_SOURCES\n' + '\n'.join(cls.probes) + '\n)\n'
                    (directory / 'CMakeLists.txt').write_text(data + actual + SUFFIX)
                    log = directory / 'cmake.log'
                    with log.open('wb') as output:
                        result = subprocess.run(
                            [str(CMAKE), '-S', str(directory), '-B', str(directory / 'build'), '-G', 'Ninja'],
                            env=environment, stdout=output, stderr=subprocess.STDOUT,
                            timeout=min(COMMAND_SECONDS, cls.deadline - time.monotonic()), check=False)
                    cls.check_deadline()
                    output = bounded_read(log)
                    if result.returncode != 1 or output.count(MARKER.encode()) != 1:
                        raise AssertionError(('CMake did not reach the deliberate property terminal',
                                              result.returncode, output.decode(errors='replace')))
                    properties = {}
                    for line in bounded_read(directory / 'properties.txt').decode().splitlines():
                        key, encoded = line.split('=', 1)
                        if key in properties:
                            raise AssertionError('duplicate property')
                        properties[key] = bytes.fromhex(encoded).decode().replace(str(directory), '<PORT_SOURCE>')
                    required = {'target:COMPILE_OPTIONS', 'target:COMPILE_DEFINITIONS', 'target:LINK_OPTIONS',
                                'excluded_sources', 'excluded_options', 'complete'}
                    required.update('source:' + source for source in cls.probes)
                    if set(properties) != required or properties['complete'] != 'yes':
                        raise AssertionError('complete target and every declared source property required')
                    cls.rows[label, flags] = properties
        cls.check_deadline()

    def tearDown(self):
        self.check_deadline()

    def protection(self, row, kcfi):
        expected = '-fno-sanitize=all;-fno-sanitize-coverage=trace-pc' + (';-fsanitize=kcfi' if kcfi else '')
        self.assertEqual(row['excluded_options'], expected)
        self.assertEqual(row['excluded_sources'], ';'.join(self.sources))
        for source in self.sources:
            self.assertEqual(row['source:' + source], expected, source)

    def clock_protection(self, row, kcfi):
        expected = '-fno-sanitize=all;-fno-sanitize-coverage=trace-pc' + (';-fsanitize=kcfi' if kcfi else '')
        for source in REQUIRED_CLOCK_SOURCES.values():
            self.assertEqual(row['source:' + source], expected, source)

    def test_target_instrumentation_is_independent_of_exclusion_condition(self):
        for flags in itertools.product((False, True), repeat=4):
            with self.subTest(flags=flags):
                for key in ('target:COMPILE_OPTIONS', 'target:COMPILE_DEFINITIONS', 'target:LINK_OPTIONS'):
                    self.assertEqual(self.rows['current', flags][key], self.rows['old-condition', flags][key])

    def test_asan_on_properties_match_old_condition(self):
        for flags in itertools.product((False, True), repeat=4):
            if flags[0]:
                with self.subTest(flags=flags):
                    self.assertEqual(self.rows['current', flags], self.rows['old-condition', flags])
                    self.protection(self.rows['current', flags], flags[3])

    def test_all_off_and_kcfi_only_have_no_source_exclusions(self):
        for kcfi in (False, True):
            flags = (False, False, False, kcfi)
            self.assertEqual(self.rows['current', flags], self.rows['old-condition', flags])
            for source in self.probes:
                self.assertEqual(self.rows['current', flags]['source:' + source], '')

    def test_required_clock_runtime_properties(self):
        for flags in itertools.product((False, True), repeat=4):
            if any(flags[:3]):
                with self.subTest(flags=flags):
                    self.clock_protection(self.rows['current', flags], flags[3])

    def test_missing_clock_runtime_mutants_fail_the_same_protection(self):
        for flags in itertools.product((False, True), repeat=4):
            if any(flags[:3]):
                for name, source in REQUIRED_CLOCK_SOURCES.items():
                    row = self.rows['missing-' + name, flags]
                    with self.subTest(flags=flags, source=source):
                        self.assertEqual(row['source:' + source], '')
                        with self.assertRaises(AssertionError):
                            self.clock_protection(row, flags[3])

    def test_report_or_coverage_without_asan_retains_runtime_protection(self):
        for ubsan, kcov, kcfi in itertools.product((False, True), repeat=3):
            if ubsan or kcov:
                with self.subTest(ubsan=ubsan, kcov=kcov, kcfi=kcfi):
                    self.protection(self.rows['current', (False, ubsan, kcov, kcfi)], kcfi)

    def test_old_condition_mutant_fails_the_same_runtime_protection(self):
        for ubsan, kcov, kcfi in itertools.product((False, True), repeat=3):
            if ubsan or kcov:
                with self.subTest(ubsan=ubsan, kcov=kcov, kcfi=kcfi), self.assertRaises(AssertionError):
                    self.protection(self.rows['old-condition', (False, ubsan, kcov, kcfi)], kcfi)

    def test_ordinary_instrumentation_and_asan_only_balance_exception(self):
        for flags in itertools.product((False, True), repeat=4):
            asan, ubsan, kcov, kcfi = flags
            row = self.rows['current', flags]
            with self.subTest(flags=flags):
                self.assertEqual(row['source:' + ORDINARY], '')
                self.assertEqual(row['source:' + BALANCE], '-fno-sanitize=kernel-address' if asan else '')
                target = row['target:COMPILE_OPTIONS']
                for enabled, token in ((asan, '-fsanitize=kernel-address'), (ubsan, '-fsanitize=undefined'),
                                       (kcov, '-fsanitize-coverage=trace-pc'), (kcfi, '-fsanitize=kcfi')):
                    self.assertEqual(token in target, enabled)
                definitions = row['target:COMPILE_DEFINITIONS'].split(';')
                for enabled, token in ((asan, 'WOS_KASAN=1'), (ubsan, 'WOS_KUBSAN=1'),
                                       (kcov, 'WOS_KCOV=1'), (kcfi, 'WOS_KCFI=1')):
                    self.assertEqual(token in definitions, enabled)
                self.assertEqual('-fsanitize=kcfi' in row['target:LINK_OPTIONS'], kcfi)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cmake', type=Path, required=True)
    args, remaining = parser.parse_known_args()
    if not args.cmake.is_absolute():
        parser.error('--cmake must name the explicit CMake executable')
    CMAKE = args.cmake
    unittest.main(argv=[__file__, *remaining])
