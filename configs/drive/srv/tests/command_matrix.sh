#!/bin/sh
# Deterministic command-mix stress case for the WOS userland suite.
#
# The suite invokes this dispatcher once for each ID from 1 through 100. Five
# parameter variants of twenty workflows exercise shell/process behavior,
# XFS namespace and extent mutation, pipelines, archive tools, Git, and the
# native compiler/build tools without unbounded concurrency or data growth.

set -eu

case_id="${1:-}"
matrix_root="${2:-/root/wos-command-matrix}"

case "$case_id" in
    ""|*[!0-9]*)
        printf 'invalid command-matrix case id: %s\n' "$case_id" >&2
        exit 2
        ;;
esac

if [ "$case_id" -lt 1 ] || [ "$case_id" -gt 100 ]; then
    printf 'command-matrix case id out of range: %s\n' "$case_id" >&2
    exit 2
fi

group=$(( (case_id - 1) / 5 + 1 ))
variant=$(( (case_id - 1) % 5 + 1 ))
case_tag="$(printf '%03d' "$case_id")"
case_dir="$matrix_root/case-$case_tag"
case_ok=0

cleanup() {
    if [ "$case_ok" -eq 1 ] || [ "${WOS_COMMAND_MATRIX_KEEP_FAILURE:-0}" != "1" ]; then
        rm -rf "$case_dir"
    else
        printf 'retained failed command-matrix work at %s\n' "$case_dir" >&2
    fi
}

trap cleanup EXIT HUP INT TERM
rm -rf "$case_dir"
mkdir -p "$case_dir"

expect_eq() {
    label="$1"
    got="$2"
    want="$3"
    if [ "$got" != "$want" ]; then
        printf '%s: got <%s>, expected <%s>\n' "$label" "$got" "$want" >&2
        return 1
    fi
}

file_hash() {
    sha256sum "$1" | awk '{print $1}'
}

write_c_program() {
    output="$1"
    result="$2"
    cat > "$output" <<EOF
int main(void) {
    return $result;
}
EOF
}

printf 'command-matrix id=%s group=%s variant=%s root=%s\n' "$case_id" "$group" "$variant" "$case_dir"

case "$group" in
    1)
        # Repeated overwrite, append, descriptor close, truncate, and rename.
        file="$case_dir/rewrite.txt"
        printf 'old-%s\n' "$variant" > "$file"
        printf 'new-%s\n' "$variant" > "$file"
        exec 3>> "$file"
        printf 'tail-%s\n' "$variant" >&3
        exec 3>&-
        mv "$file" "$case_dir/renamed.txt"
        : > "$file"
        rm "$file"
        mv "$case_dir/renamed.txt" "$file"
        expect_eq rewrite_lines "$(wc -l < "$file" | awk '{print $1}')" 2
        expect_eq rewrite_tail "$(tail -n 1 "$file")" "tail-$variant"
        ;;
    2)
        # Multi-stage producer/filter/tee pipeline with independently checked output.
        limit=$((variant + 10))
        seq 1 "$limit" \
            | awk -v v="$variant" '{ print $1 * v }' \
            | tee "$case_dir/all.txt" \
            | tail -n "$variant" \
            | head -n 1 > "$case_dir/window.txt"
        expect_eq pipeline_count "$(wc -l < "$case_dir/all.txt" | awk '{print $1}')" "$limit"
        expect_eq pipeline_window "$(cat "$case_dir/window.txt")" "$(( (limit - variant + 1) * variant ))"
        ;;
    3)
        # Copy, mutate, move, and checksum files across directories.
        mkdir -p "$case_dir/a" "$case_dir/b" "$case_dir/c"
        seq 1 "$((variant * 7))" > "$case_dir/a/source"
        cp "$case_dir/a/source" "$case_dir/b/copy"
        expect_eq copied_hash "$(file_hash "$case_dir/b/copy")" "$(file_hash "$case_dir/a/source")"
        printf 'variant=%s\n' "$variant" >> "$case_dir/b/copy"
        mv "$case_dir/b/copy" "$case_dir/c/moved"
        test ! -e "$case_dir/b/copy"
        expect_eq moved_tail "$(tail -n 1 "$case_dir/c/moved")" "variant=$variant"
        ;;
    4)
        # Block writes plus an in-place byte patch exercise allocation and overwrite.
        blocks=$((variant + 1))
        file="$case_dir/blocks.bin"
        dd if=/dev/zero of="$file" bs=4096 count="$blocks" 2>/dev/null
        offset=$((variant * 257))
        printf X | dd of="$file" bs=1 seek="$offset" conv=notrunc 2>/dev/null
        expect_eq block_size "$(wc -c < "$file" | awk '{print $1}')" "$((blocks * 4096))"
        dd if="$file" bs=1 skip="$offset" count=1 2>/dev/null | grep -q '^X$'
        ;;
    5)
        # Deep relative paths, dot/dot-dot traversal, and absolute resolution.
        mkdir -p "$case_dir/a/b/c/d/e"
        printf 'path-%s\n' "$variant" > "$case_dir/a/b/c/value"
        resolved="$(cd "$case_dir/a/./b/c/d/e/../.." && pwd)"
        expect_eq normalized_pwd "$resolved" "$case_dir/a/b/c"
        expect_eq normalized_read "$(cat "$case_dir/a/b/../b/c/./value")" "path-$variant"
        realpath "$case_dir/a/b/c/../c/value" > "$case_dir/resolved"
        expect_eq realpath_value "$(cat "$case_dir/resolved")" "$case_dir/a/b/c/value"
        # A prior interrupted mkstemp leaves its first candidate behind. The
        # next O_CREAT|O_EXCL attempt must receive EEXIST and advance.
        mktemp_seed="$case_dir/mktemp.000000"
        : > "$mktemp_seed"
        mktemp_next="$(mktemp "$case_dir/mktemp.XXXXXX")"
        test "$mktemp_next" != "$mktemp_seed"
        test -f "$mktemp_next"
        ;;
    6)
        # Hard links, relative symlinks, rename, and unlink lifetime semantics.
        mkdir -p "$case_dir/links"
        printf 'link-%s\n' "$variant" > "$case_dir/links/original"
        ln "$case_dir/links/original" "$case_dir/links/hard"
        ln -s hard "$case_dir/links/soft"
        printf 'append-%s\n' "$variant" >> "$case_dir/links/hard"
        rm "$case_dir/links/original"
        expect_eq hard_link_lines "$(wc -l < "$case_dir/links/hard" | awk '{print $1}')" 2
        expect_eq soft_link_tail "$(tail -n 1 "$case_dir/links/soft")" "append-$variant"
        expect_eq readlink_target "$(readlink "$case_dir/links/soft")" hard
        mv "$case_dir/links/hard" "$case_dir/links/renamed"
        test ! -e "$case_dir/links/soft"
        ;;
    7)
        # Find, xargs, sort, and uniq over a small generated tree.
        mkdir -p "$case_dir/tree/left" "$case_dir/tree/right"
        i=1
        while [ "$i" -le "$((variant + 4))" ]; do
            printf '%s\n' "$((i % 3))" > "$case_dir/tree/left/item-$i.dat"
            printf '%s\n' "$((i % 3))" > "$case_dir/tree/right/item-$i.dat"
            i=$((i + 1))
        done
        find "$case_dir/tree" -type f -name '*.dat' | sort > "$case_dir/names"
        expect_eq find_count "$(wc -l < "$case_dir/names" | awk '{print $1}')" "$((2 * (variant + 4)))"
        xargs cat < "$case_dir/names" | sort | uniq > "$case_dir/values"
        expect_eq unique_values "$(wc -l < "$case_dir/values" | awk '{print $1}')" 3
        ;;
    8)
        # Text transformations cross-check grep, cut, sed, tr, and awk.
        cat > "$case_dir/input.tsv" <<EOF
alpha:${variant}:red
beta:$((variant + 1)):green
gamma:$((variant + 2)):blue
EOF
        cut -d: -f1,3 "$case_dir/input.tsv" | sed 's/:/=/g' | tr 'a-z' 'A-Z' > "$case_dir/output"
        expect_eq text_first "$(head -n 1 "$case_dir/output")" ALPHA=RED
        expect_eq text_last "$(tail -n 1 "$case_dir/output")" GAMMA=BLUE
        expect_eq awk_sum "$(awk -F: '{sum += $2} END {print sum}' "$case_dir/input.tsv")" "$((variant * 3 + 3))"
        grep -q "^beta:$((variant + 1)):green$" "$case_dir/input.tsv"
        ;;
    9)
        # Here-docs, command substitution, parameter expansion, loops, and case.
        # The explicit Bash here-doc exceeds the kernel pipe bounce size while
        # remaining below pipe capacity, guarding against spurious short writes.
        token="prefix-${variant}-suffix"
        middle="${token#prefix-}"
        middle="${middle%-suffix}"
        result="$(
            total=0
            for n in 1 2 3 4 5; do
                total=$((total + n * variant))
            done
            case "$middle" in
                "$variant") printf '%s\n' "$total" ;;
                *) exit 1 ;;
            esac
        )"
        cat > "$case_dir/here" <<EOF
$token
$result
EOF
        expect_eq expansion_result "$(tail -n 1 "$case_dir/here")" "$((15 * variant))"
        heredoc_bytes=$((8192 + variant * 257))
        awk -v bytes="$heredoc_bytes" 'BEGIN {
            for (i = 0; i < bytes; ++i) printf "%c", 65 + (i % 26)
            printf "\n"
        }' > "$case_dir/heredoc.expected"
        {
            printf '%s\n' '#!/bin/bash'
            printf '%s\n' 'set -eu'
            printf '%s\n' 'payload="$(cat "$1")"'
            printf '%s\n' 'cat > "$2" <<EOF'
            printf '%s\n' '$payload'
            printf '%s\n' 'EOF'
        } > "$case_dir/heredoc.bash"
        /bin/bash "$case_dir/heredoc.bash" "$case_dir/heredoc.expected" "$case_dir/heredoc.actual"
        cmp "$case_dir/heredoc.expected" "$case_dir/heredoc.actual"
        ;;
    10)
        # Explicit descriptor duplication, reads, appends, and stderr routing.
        file="$case_dir/fd.txt"
        exec 3> "$file"
        printf 'first-%s\n' "$variant" >&3
        printf 'second-%s\n' "$variant" >&3
        exec 3>&-
        exec 4< "$file"
        IFS= read -r first <&4
        IFS= read -r second <&4
        exec 4<&-
        exec 5>> "$file"
        printf 'third-%s\n' "$variant" >&5
        exec 5>&-
        expect_eq fd_first "$first" "first-$variant"
        expect_eq fd_second "$second" "second-$variant"
        expect_eq fd_lines "$(wc -l < "$file" | awk '{print $1}')" 3
        sh -c 'printf routed >&2' 2> "$case_dir/stderr"
        expect_eq stderr_route "$(cat "$case_dir/stderr")" routed
        ;;
    11)
        # Background pipelines with explicit PID ownership and ordered aggregation.
        pids=
        for worker in 1 2 3; do
            (
                seq 1 "$((variant + worker))" | awk -v w="$worker" '{sum += $1} END {print w ":" sum}' \
                    > "$case_dir/worker-$worker"
            ) &
            pids="$pids $!"
        done
        for pid in $pids; do
            wait "$pid"
        done
        cat "$case_dir"/worker-* | sort > "$case_dir/combined"
        expect_eq background_rows "$(wc -l < "$case_dir/combined" | awk '{print $1}')" 3
        grep -q '^1:' "$case_dir/combined"
        grep -q '^3:' "$case_dir/combined"
        ;;
    12)
        # Concurrent O_APPEND writers target the failure mode seen during checkout.
        file="$case_dir/append.log"
        : > "$file"
        per_worker=$((variant + 5))
        pids=
        for worker in 1 2 3 4; do
            (
                i=1
                while [ "$i" -le "$per_worker" ]; do
                    printf 'w%s-%s\n' "$worker" "$i" >> "$file" || exit 1
                    i=$((i + 1))
                done
            ) &
            pids="$pids $!"
        done
        for pid in $pids; do
            wait "$pid"
        done
        expected=$((4 * per_worker))
        expect_eq append_lines "$(wc -l < "$file" | awk '{print $1}')" "$expected"
        expect_eq append_unique "$(sort "$file" | uniq | wc -l | awk '{print $1}')" "$expected"
        ;;
    13)
        # Parallel rename cycles use disjoint files in one shared directory.
        mkdir -p "$case_dir/rename"
        loops=$((variant * 4 + 8))
        pids=
        for worker in 1 2 3 4; do
            (
                a="$case_dir/rename/a-$worker"
                b="$case_dir/rename/b-$worker"
                printf '%s\n' "$worker" > "$a"
                i=0
                while [ "$i" -lt "$loops" ]; do
                    mv "$a" "$b" || exit 1
                    mv "$b" "$a" || exit 1
                    i=$((i + 1))
                done
                test "$(cat "$a")" = "$worker"
            ) &
            pids="$pids $!"
        done
        for pid in $pids; do wait "$pid"; done
        expect_eq rename_survivors "$(find "$case_dir/rename" -type f | wc -l | awk '{print $1}')" 4
        ;;
    14)
        # Recreate, truncate, read, and unlink files concurrently.
        mkdir -p "$case_dir/recreate"
        loops=$((variant * 3 + 7))
        pids=
        for worker in 1 2 3 4; do
            (
                file="$case_dir/recreate/file-$worker"
                i=1
                while [ "$i" -le "$loops" ]; do
                    printf '%s:%s\n' "$worker" "$i" > "$file" || exit 1
                    test "$(cat "$file")" = "$worker:$i" || exit 1
                    rm "$file" || exit 1
                    i=$((i + 1))
                done
                printf done > "$case_dir/recreate/done-$worker"
            ) &
            pids="$pids $!"
        done
        for pid in $pids; do wait "$pid"; done
        expect_eq recreate_done "$(find "$case_dir/recreate" -name 'done-*' | wc -l | awk '{print $1}')" 4
        ;;
    15)
        # Concurrent nested directory create/remove churn stresses inode release.
        mkdir -p "$case_dir/dirs"
        loops=$((variant + 4))
        pids=
        for worker in 1 2 3; do
            (
                i=1
                while [ "$i" -le "$loops" ]; do
                    leaf="$case_dir/dirs/w$worker-$i/a/b/c"
                    mkdir -p "$leaf" || exit 1
                    printf '%s\n' "$i" > "$leaf/value" || exit 1
                    rm "$leaf/value" || exit 1
                    rmdir "$leaf" "${leaf%/c}" "${leaf%/b/c}" "${leaf%/a/b/c}" || exit 1
                    i=$((i + 1))
                done
                printf done > "$case_dir/dirs/done-$worker"
            ) &
            pids="$pids $!"
        done
        for pid in $pids; do wait "$pid"; done
        expect_eq directory_done "$(find "$case_dir/dirs" -name 'done-*' | wc -l | awk '{print $1}')" 3
        ;;
    16)
        # Tar and gzip round-trip a nested tree and compare content hashes.
        mkdir -p "$case_dir/src/a" "$case_dir/src/b" "$case_dir/dst"
        seq 1 "$((variant * 11))" > "$case_dir/src/a/numbers"
        printf 'archive-%s\n' "$variant" > "$case_dir/src/b/text"
        tar cf "$case_dir/archive.tar" -C "$case_dir/src" .
        gzip -c "$case_dir/archive.tar" > "$case_dir/archive.tar.gz"
        gzip -dc "$case_dir/archive.tar.gz" > "$case_dir/unpacked.tar"
        tar xf "$case_dir/unpacked.tar" -C "$case_dir/dst"
        expect_eq archive_numbers "$(file_hash "$case_dir/dst/a/numbers")" "$(file_hash "$case_dir/src/a/numbers")"
        expect_eq archive_text "$(cat "$case_dir/dst/b/text")" "archive-$variant"
        ;;
    17)
        # Procfs snapshots, device I/O, filters, and byte-count validation.
        cat /proc/version /proc/uptime /proc/self/stat > "$case_dir/proc.txt"
        test -s "$case_dir/proc.txt"
        bytes=$((variant * 257))
        head -c "$bytes" /dev/zero > "$case_dir/zero.bin"
        expect_eq zero_bytes "$(wc -c < "$case_dir/zero.bin" | awk '{print $1}')" "$bytes"
        head -c "$bytes" /dev/zero > "$case_dir/zero.expected"
        cmp "$case_dir/zero.bin" "$case_dir/zero.expected"
        printf 'discard-%s\n' "$variant" > /dev/null
        expect_eq null_read "$(cat /dev/null | wc -c | awk '{print $1}')" 0
        ;;
    18)
        # A bounded local Git lifecycle targets checkout/index/object-store paths.
        repo="$case_dir/repo"
        clone="$case_dir/clone"
        git init -q "$repo"
        (
            cd "$repo" || exit 1
            git config user.name 'WOS command matrix'
            git config user.email 'command-matrix@wos.invalid'
            i=1
            while [ "$i" -le "$variant" ]; do
                printf 'commit-%s\n' "$i" >> history.txt
                mkdir -p "tree/$i"
                printf 'payload-%s-%s\n' "$variant" "$i" > "tree/$i/value"
                git add history.txt "tree/$i/value"
                git commit -q -m "matrix $case_tag commit $i" || exit 1
                i=$((i + 1))
            done
            test -z "$(git status --porcelain)"
            git fsck --no-dangling
        )
        git clone -q "$repo" "$clone"
        expect_eq git_history_lines "$(wc -l < "$clone/history.txt" | awk '{print $1}')" "$variant"
        expect_eq git_head "$(git -C "$clone" show HEAD:"tree/$variant/value")" "payload-$variant-$variant"
        ;;
    19)
        # Compile, link, and execute a C program in distinct clang phases.
        src="$case_dir/main.c"
        obj="$case_dir/main.o"
        bin="$case_dir/main"
        cat > "$src" <<EOF
#include <stdio.h>
int main(void) {
    int value = $variant * 9;
    printf("matrix-%d\\n", value);
    return value == $((variant * 9)) ? 0 : 1;
}
EOF
        /usr/bin/clang -O$((variant % 3)) -Wall -Wextra -c "$src" -o "$obj"
        /usr/bin/clang "$obj" -o "$bin"
        "$bin" > "$case_dir/output"
        expect_eq clang_output "$(cat "$case_dir/output")" "matrix-$((variant * 9))"
        ;;
    20)
        # Five native build-tool paths: Make, Ninja, CMake, C++, and archives.
        src="$case_dir/main.c"
        write_c_program "$src" 0
        case "$variant" in
            1)
                printf 'all: app\n\napp: main.c\n\t/usr/bin/clang main.c -o app\n' > "$case_dir/Makefile"
                make -s -C "$case_dir"
                "$case_dir/app"
                ;;
            2)
                cat > "$case_dir/build.ninja" <<'EOF'
rule cc
  command = /usr/bin/clang $in -o $out
build app: cc main.c
default app
EOF
                ninja -C "$case_dir"
                "$case_dir/app"
                ;;
            3)
                cat > "$case_dir/CMakeLists.txt" <<'EOF'
cmake_minimum_required(VERSION 3.20)
project(wos_command_matrix C)
add_executable(app main.c)
EOF
                CC=/usr/bin/clang cmake -S "$case_dir" -B "$case_dir/build" -G Ninja -DCMAKE_BUILD_TYPE=Release
                cmake --build "$case_dir/build"
                "$case_dir/build/app"
                ;;
            4)
                cat > "$case_dir/main.cpp" <<'EOF'
constexpr auto answer() -> int { return 42; }
int main() { return answer() == 42 ? 0 : 1; }
EOF
                /usr/bin/clang++ -std=c++23 "$case_dir/main.cpp" -o "$case_dir/app"
                "$case_dir/app"
                ;;
            5)
                printf 'int matrix_value(void) { return 55; }\n' > "$case_dir/lib.c"
                printf 'int matrix_value(void); int main(void) { return matrix_value() == 55 ? 0 : 1; }\n' > "$src"
                /usr/bin/clang -c "$case_dir/lib.c" -o "$case_dir/lib.o"
                /usr/bin/clang -c "$src" -o "$case_dir/main.o"
                /usr/bin/llvm-ar rcs "$case_dir/libmatrix.a" "$case_dir/lib.o"
                /usr/bin/llvm-ranlib "$case_dir/libmatrix.a"
                /usr/bin/clang "$case_dir/main.o" "$case_dir/libmatrix.a" -o "$case_dir/app"
                "$case_dir/app"
                ;;
        esac
        ;;
esac

case_ok=1
printf 'command-matrix case %s passed\n' "$case_tag"
