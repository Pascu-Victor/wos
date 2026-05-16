#!/bin/sh
# Terminal / VFS validation suite for wos
# Run: sh /srv/tests/validate.sh

PASS=0
FAIL=0
WORKDIR=/tmp/vtest_$$
mkdir -p "$WORKDIR"
trap 'rm -rf "$WORKDIR"' EXIT

ok() {
    PASS=$((PASS + 1))
    printf '  PASS  %s\n' "$1"
}

fail() {
    FAIL=$((FAIL + 1))
    printf '  FAIL  %s\n' "$1"
}

check() {
    local label="$1"
    local got="$2"
    local want="$3"
    if [ "$got" = "$want" ]; then
        ok "$label"
    else
        fail "$label (got='$got' want='$want')"
    fi
}

section() {
    printf '\n=== %s ===\n' "$1"
}

# Basic file I/O
section "Basic file I/O"

printf 'hello\n' > "$WORKDIR/f1"
check "write then read"          "$(cat "$WORKDIR/f1")"       "hello"

printf 'line1\n' > "$WORKDIR/f2"
printf 'line2\n' >> "$WORKDIR/f2"
printf 'line3\n' >> "$WORKDIR/f2"
check "append (>>)"              "$(wc -l < "$WORKDIR/f2")"   "3"
check "append preserves content" "$(tail -n1 "$WORKDIR/f2")"  "line3"
check "append first line intact" "$(head -n1 "$WORKDIR/f2")"  "line1"

# multiple appends in a loop
> "$WORKDIR/f3"
i=1
while [ $i -le 5 ]; do
    printf '%d\n' $i >> "$WORKDIR/f3"
    i=$((i + 1))
done
check "loop append line count"   "$(wc -l < "$WORKDIR/f3")"   "5"
check "loop append last value"   "$(tail -n1 "$WORKDIR/f3")"  "5"

# truncate via >
printf 'old content\n' > "$WORKDIR/f4"
printf 'new\n' > "$WORKDIR/f4"
check "overwrite (>) truncates"  "$(cat "$WORKDIR/f4")"       "new"

# self-redirect must empty the file (expected POSIX behaviour)
printf 'some data\n' > "$WORKDIR/f5"
cat "$WORKDIR/f5" > "$WORKDIR/f5"
check "cat file > file empties"  "$(wc -c < "$WORKDIR/f5")"   "0"

# /dev/null
section "/dev/null"

printf 'discard me\n' > /dev/null
check "/dev/null write succeeds" "$?"                          "0"
check "/dev/null read is empty"  "$(cat /dev/null | wc -c)"   "0"

# Terminal control
section "Terminal control"

CLEAR_BYTES=$(clear | od -An -tx1 | tr -d ' \n')
check "clear emits scrollback erase" "$CLEAR_BYTES" "1b5b481b5b324a1b5b334a"

# Pipes
section "Pipes"

check "echo | cat"               "$(echo pipe_test | cat)"           "pipe_test"
check "echo | grep hit"          "$(echo needle | grep needle)"      "needle"
check "echo | grep miss (exit)"  "$(echo x | grep y; echo $?)"       "1"
check "multi-stage pipe"         "$(printf 'a\nb\nc\n' | grep b | cat)" "b"
check "pipe preserves newlines"  "$(printf 'x\ny\n' | wc -l)"        "2"

SEQ=$(printf '1\n2\n3\n4\n5\n' | tail -n3 | head -n1)
check "pipe tail|head"           "$SEQ"                              "3"

# Redirections
section "Redirections"

# stdin from file
printf 'from_file\n' > "$WORKDIR/stdin_src"
check "stdin redirect (<)"       "$(cat < "$WORKDIR/stdin_src")"     "from_file"

# stderr redirect
MSG=$(ls /nonexistent_path_xyz 2>&1 | head -n1)
if [ -n "$MSG" ]; then ok "stderr 2>&1 captures error"; else fail "stderr 2>&1 captures error"; fi

# stdout to file, then stderr discarded
echo "captured" > "$WORKDIR/cap"
check "stdout redirect to file"  "$(cat "$WORKDIR/cap")"             "captured"

# here-doc
check "here-doc"  "$(cat <<'EOF'
heredoc_value
EOF
)"  "heredoc_value"

# Exit codes
section "Exit codes"

true;  check "true exits 0"   "$?" "0"
false; check "false exits 1"  "$?" "1"

sh -c 'exit 42'; check "exit 42"  "$?" "42"

grep -q found <<'EOF'
found
EOF
check "grep found exits 0"    "$?" "0"

grep -q found <<'EOF'
nope
EOF
check "grep not found exits 1" "$?" "1"

# Arithmetic
section "Arithmetic"

check "addition"       "$(expr 3 + 4)"         "7"
check "subtraction"    "$(expr 10 - 3)"        "7"
check "multiplication" "$(expr 6 \* 7)"        "42"
check "division"       "$(expr 20 / 4)"        "5"
check "modulo"         "$(expr 17 % 5)"        "2"

A=10; B=3
check "shell arith \$(())" "$(( A * B + 1 ))"  "31"

# String operations
section "String operations"

S="hello world"
check "string length"       "${#S}"                   "11"
check "substring"           "${S#hello }"             "world"
check "var concat"          "${S} suffix"             "hello world suffix"

check "tr uppercase"        "$(echo abc | tr a-z A-Z)"  "ABC"
check "sed replace"         "$(echo foo | sed 's/foo/bar/')"  "bar"
check "cut field"           "$(echo a:b:c | cut -d: -f2)"     "b"
check "awk field"           "$(echo 'x y z' | awk '{print $2}')"  "y"

# Control flow
section "Control flow"

if true; then check "if true branch"  "taken" "taken"; else fail "if true branch"; fi
if false; then fail "if false skip"; else ok "if false -> else"; fi

RES=""
for x in a b c; do RES="${RES}${x}"; done
check "for loop"  "$RES"  "abc"

N=0; while [ $N -lt 3 ]; do N=$((N+1)); done
check "while loop" "$N" "3"

case "banana" in
    apple)  fail  "case banana=apple" ;;
    banana) ok    "case banana=banana" ;;
    *)      fail  "case banana=*" ;;
esac

# Environment variables
section "Environment variables"

export TESTVAR=hello_env
check "exported var visible"   "$TESTVAR"              "hello_env"
check "env var in subshell"    "$(sh -c 'echo $TESTVAR')"  "hello_env"

unset TESTVAR
check "unset var is empty"     "${TESTVAR:-gone}"      "gone"

# /tmp (tmpfs)
section "/tmp (tmpfs)"

printf 'tmpfs_data\n' > /tmp/wos_vtest_$$
check "write to /tmp"          "$(cat /tmp/wos_vtest_$$)"  "tmpfs_data"
printf 'more\n' >> /tmp/wos_vtest_$$
check "append to /tmp"         "$(wc -l < /tmp/wos_vtest_$$)"  "2"
rm /tmp/wos_vtest_$$
check "rm from /tmp (gone)"    "$(test -f /tmp/wos_vtest_$$ && echo y || echo n)"  "n"

# File system ops
section "Filesystem ops"

mkdir -p "$WORKDIR/d1/d2"
check "mkdir -p"   "$(test -d "$WORKDIR/d1/d2" && echo y || echo n)"  "y"

cp "$WORKDIR/f1" "$WORKDIR/f1_cp"
check "cp"         "$(cat "$WORKDIR/f1_cp")"   "$(cat "$WORKDIR/f1")"

mv "$WORKDIR/f1_cp" "$WORKDIR/f1_mv"
check "mv creates"  "$(test -f "$WORKDIR/f1_mv" && echo y || echo n)"  "y"
check "mv removes"  "$(test -f "$WORKDIR/f1_cp" && echo y || echo n)"  "n"

rm "$WORKDIR/f1_mv"
check "rm"          "$(test -f "$WORKDIR/f1_mv" && echo y || echo n)"  "n"

rmdir "$WORKDIR/d1/d2"
check "rmdir"       "$(test -d "$WORKDIR/d1/d2" && echo y || echo n)"  "n"

# find and wc
section "find / wc"

touch "$WORKDIR/find_a" "$WORKDIR/find_b"
FOUND=$(find "$WORKDIR" -name 'find_*' | wc -l)
check "find by name"  "$FOUND"  "2"

printf 'one two three\n' > "$WORKDIR/wc_in"
check "wc -w"  "$(wc -w < "$WORKDIR/wc_in")"  "3"
check "wc -c"  "$(wc -c < "$WORKDIR/wc_in")"  "14"

# /proc
section "/proc"

if [ -f /proc/version ]; then
    ok "/proc/version exists"
else
    fail "/proc/version exists"
fi

if [ -f /proc/uptime ]; then
    UP=$(cat /proc/uptime | awk '{print $1}')
    if [ -n "$UP" ]; then ok "/proc/uptime readable"; else fail "/proc/uptime readable"; fi
else
    fail "/proc/uptime exists"
fi

PID_SELF=$(cat /proc/self/stat 2>/dev/null | awk '{print $1}')
if [ -n "$PID_SELF" ]; then ok "/proc/self/stat has pid"; else fail "/proc/self/stat has pid"; fi

# Background jobs / wait
section "Background jobs"

printf '' > "$WORKDIR/bg_out"
(printf 'bg_done\n' >> "$WORKDIR/bg_out") &
wait $!
check "background job output"  "$(cat "$WORKDIR/bg_out")"  "bg_done"

wait
check "wait with no background jobs" "$?" "0"

> "$WORKDIR/wait_any_out"
(printf 'a\n' >> "$WORKDIR/wait_any_out") &
(printf 'b\n' >> "$WORKDIR/wait_any_out") &
wait
check "wait for all background jobs" "$?" "0"
LINES=$(wc -l < "$WORKDIR/wait_any_out")
check "wait for all background jobs output" "$LINES" "2"

# parallel appenders - stress O_APPEND with explicit PID waits
> "$WORKDIR/par_out"
(for i in 1 2 3 4 5; do printf '%d\n' $i >> "$WORKDIR/par_out"; done) &
PID1=$!
(for i in 6 7 8 9 10; do printf '%d\n' $i >> "$WORKDIR/par_out"; done) &
PID2=$!
wait "$PID1"
RC1=$?
wait "$PID2"
RC2=$?
check "parallel appenders exit" "$RC1:$RC2" "0:0"
LINES=$(wc -l < "$WORKDIR/par_out")
check "parallel append line count"  "$LINES"  "10"

# Traps
section "Traps"

TRAP_FILE="$WORKDIR/trap_check"
(
    trap "printf 'trapped\n' > '$TRAP_FILE'" EXIT
    exit 0
)
check "EXIT trap runs"  "$(cat "$TRAP_FILE" 2>/dev/null)"  "trapped"

# Summary
printf '\n'
printf '==============================\n'
printf '  Results: %d passed, %d failed\n' "$PASS" "$FAIL"
printf '==============================\n'

[ "$FAIL" -eq 0 ]
