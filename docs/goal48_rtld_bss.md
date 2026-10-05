# Runtime-linker anonymous BSS pages

The DSO segment loader previously cleared the entire memory suffix after mapping
its anonymous tail. Those pages already provide zero bytes on demand. The loader
now clears only declared BSS bytes in the final file-backed page. File prefixes,
bytes beyond declared BSS, mapping flags, protections, ELF validation, TLS/RELRO
metadata and initialization order remain unchanged. The copy-based path is unchanged.
Full virtual address-space admission and ordinary future anonymous-page fault/OOM
semantics remain intact; this removes redundant writes rather than reserving less space.

Actual loader and PHDR-validation bodies pass 588 host cases per mapping profile
and nine error cases with ASan/UBSan. The old loader passes byte semantics and fails
the anonymous-page residency check. Five incorrect variants are rejected. Normal,
diagnostic and separate private libc Build WOS builds passed, as did formatting and
22 focused checks. One sandbox LeakSanitizer failure passed its host recheck with
sanitizers enabled. Initial DNS-dependent build failures are retained separately.

Fresh isolated KTEST passed 66,703 assertions with zero failures and natural poweroff.
The final fresh four-node run passed settled artifact/auth/profile/KVM checks, SMP
clock checks, installed APIs, 96 real DSO BSS/TLS/constructor checks, 4,800 balanced
old/new loader-bundle samples, 800 ordinary launches, idle and stress checks, eight
lossless startup captures, counter/warm probes and final health. Independent checks
confirmed 32 complete aggregate snapshots. All owned VMs are closed; default disks
and shared normal libraries retain their original hashes.

Two failed diagnostic phases remain preserved. The first fixture expected one data
byte to equal two different values; changing its data array length to two passed on
Linux with sanitizers and all four original nodes. The second used a stale sysroot
BusyBox instead of the actual packaged image. The corrected control uses the verified
guest executable, changes only its interpreter bytes, verifies each loaded library
path and passed on all four original nodes before candidate qualification. The exact
internal cause of the stale executable's hang was not attributed to the loader patch.
Each failed phase was closed and archived, and the original baseline was restored
and health-checked before diagnosis.

Ordinary node medians were 1.277–1.383 ms wall for `true` and 1.379–1.465 ms for
`whoami`; child user time was 0.335–0.352 and 0.369–0.386 ms respectively. Balanced
bundle comparisons varied across nodes and blocks and do not establish a consistent
latency improvement. Modified interpreter/path controls and different artifact bundles
are not an isolated patch percentage. The roughly one-millisecond target and broader
Goals 48, 49 and 50 remain open, including topology/loss/trust/hardware gates.

Evidence is recorded under `build/goal48-rtld-bss-v22-evidence/`, with closed raw
phases and independent matrix proofs in the v22/v22b/v22c qualification directories.
