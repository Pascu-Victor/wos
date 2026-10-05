#!/usr/bin/env python3
"""Exercise the actual perf parser against current and legacy event rows."""
from pathlib import Path
import os
import re
import resource
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]

def function(source, name):
    match = re.search(r'^auto ' + name + r'\([^)]*\) -> [^{\n]+ \{', source, re.M)
    assert match, name
    end = source.index('{', match.start()) + 1
    depth = 1
    while depth:
        depth += (source[end] == '{') - (source[end] == '}')
        end += 1
    return source[match.start():end]

CASES = r'''
int main() {
    EventInfo e;
    assert(parse_event_line("W 125763547271 6 135 0 749 96 wki.cpp:1177 kern_yield", e));
    assert(e.type == 'W' && e.ts_ns == 125763547271 && e.cpu == 6 && e.pid == 135);
    assert(e.data == 0 && e.aux == 749 && e.flags == 96);
    assert(e.callsite == "wki.cpp:1177" && e.wait_channel == "kern_yield");
    assert(parse_event_line("B 125735134674 0 125 0 62 8 sys_net.cpp:288 poll", e));
    assert(e.type == 'B' && e.data == 0 && e.aux == 62 && e.flags == 8);
    assert(e.callsite == "sys_net.cpp:288" && e.wait_channel == "poll");
    assert(parse_event_line("W 100 2 19 0 3 32 0xffffffff81234567 -", e));
    assert(e.callsite == "0xffffffff81234567" && e.wait_channel == "-");
    assert(parse_event_line("B 101 2 19 200 4 24 wait.cpp:17", e));
    assert(e.callsite == "wait.cpp:17" && e.wait_channel.empty() && e.flags == 24);
    assert(parse_event_line("W 102 2 19 0 5 32", e));
    assert(e.callsite.empty() && e.wait_channel.empty() && e.aux == 5 && e.flags == 32);
    assert(parse_event_line("W 103 2 19 500", e));
    assert(e.flags == FLAG_TIMED && e.aux == 0 && e.callsite.empty() && e.wait_channel.empty());
    assert(parse_event_line("B 104 2 19 0", e));
    assert(e.flags == FLAG_BLOCK && e.aux == 0 && e.callsite.empty() && e.wait_channel.empty());
    assert(parse_event_line("X 105 3 19 20 -42 8 7 switch.cpp:9", e));
    assert(e.other_pid == 20 && e.lag == -42 && e.flags == 8 && e.aux == 7);
    assert(e.callsite == "switch.cpp:9" && e.wait_channel.empty());
    assert(parse_event_line("C 106 3 20 vmem 1 12 32 vmem.cpp:10", e));
    assert(e.subsys_name == "vmem" && e.flags == 1 && e.lag == 12 && e.aux == 32);
    assert(e.callsite == "vmem.cpp:10" && e.wait_channel.empty());
    assert(parse_event_line("K 107 3 20 remote_compute reject point 4 3 123 -5 0 compute.cpp:11", e));
    assert(e.scope_name == "remote_compute" && e.op_name == "reject" && e.phase_name == "point");
    assert(e.peer == 4 && e.channel == 3 && e.correlation == 123 && e.status == -5);
    assert(e.callsite == "compute.cpp:11" && e.wait_channel.empty());
    assert(!parse_event_line("W 1 2 3", e));
    puts("perf event parser: current W/B columns, optional text, legacy rows and X/C/K compatibility pass");
}
'''

def main():
    directory = ROOT / 'modules/perf/src'
    header = (directory / 'perf.hpp').read_text()
    support = (directory / 'support.cpp').read_text()
    data = (directory / 'data.cpp').read_text()
    event = re.search(r'struct EventInfo \{.*?\n\};', header, re.S).group()
    names = ['PARSE_BASE_DECIMAL', 'MAX_EVENT_TOKENS', 'EVENT_MIN_TOKEN_COUNT', 'EVENT_EXTENDED_TOKEN_COUNT', 'FLAG_BLOCK', 'FLAG_TIMED']
    constants = '\n'.join(re.search(r'^constexpr [^\n]*\b' + name + r'\b[^\n]*;', header, re.M).group() for name in names)
    declarations = '\n'.join(re.search(r'^auto ' + name + r'\([^\n]*;', header, re.M).group() for name in ['parse_u64', 'parse_i64', 'parse_u32', 'parse_u8', 'next_token'])
    code = '#include <array>\n#include <cassert>\n#include <cstdint>\n#include <cstdio>\n#include <cstdlib>\n#include <string>\n#include <string_view>\n#include <utility>\n'
    code += constants + '\n' + event + '\n' + declarations + '\n'
    code += '\n'.join(function(support, name) for name in ['parse_u64', 'parse_i64', 'parse_u32', 'parse_u8', 'next_token'])
    code += '\n' + function(data, 'parse_event_line') + '\n' + CASES
    with tempfile.TemporaryDirectory(prefix='wos-perf-parser-') as temp:
        source, binary = Path(temp) / 'test.cpp', Path(temp) / 'test'
        source.write_text(code)
        subprocess.run([shutil.which('c++', path=os.defpath), '-std=c++23', '-O1', '-g',
            '-Wall', '-Wextra', '-Werror', '-fsanitize=address,undefined', str(source), '-o', str(binary)], check=True)
        subprocess.run([str(binary)], check=True, env={**os.environ, 'ASAN_OPTIONS': 'detect_leaks=0'})

if __name__ == '__main__':
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    main()
