# Finite simultaneous compartment sharing and init retirement fixture

This NEW fixture uses actual scheduled WOS processes and existing public ABIs.
It does not establish all Goal39 criteria, two hours of operation, 1,000 full
lifecycles, complete reference/GC baselines or writable File MAP_SHARED coherence.
It changes no kernel, libc, init, launcher or old fixture behavior.

Install the actual compiled ELF at `/usr/libexec/compartment-shared-runtime`
(0755). Provide a fresh `/srv/compartment-shared-results` directory, UID100,
0700. The Host init service `compartment-shared-probe` runs it with no arguments
as UID/EUID/GID/EGID100, denied ambient privilege, no restart and exit-success
readiness. Host explicitly selects LOCAL placement. A separate UID100 service
`compartment-shared-exit-witness`, depending on that readiness, runs the SAME
ELF with exact optional argument `--init-exit-witness`.

The Host directly calls real COMPARTMENT_CREATE54 twice before waiting either
child. Each private root is `/app/probe --service A|B NONCE HOSTPID DEADLINE_US`,
PID1/PPID0/UID100, and forks one real local PID2/PPID1 child. Each root imports
the same actual ELF, `/lib/ld.so`, `/usr/lib/libc.so` and its own different
`/same/name` marker. The helper links libc only; before running, inspect actual
interpreter and DT_NEEDED and fail preparation if additional dependencies are
not explicitly imported. No ambient FDs inherit. Private root verifies all254
FDs other than9/10 are absent before opening its own marker/objects.

FD9 is that tree's deliberate O_RDWR regular Host control/report File; FD10 is
the SAME immutable O_RDONLY regular Host File granted to both roots. Local fork
inherits these exact capabilities. Private open of the Host backing pathname
fails ENOENT while FD10 permits reading; pwrite(FD10) fails -1/EBADF. Explicit
File rights/description lifetime are different from root path visibility. No
socket, pipe, directory, device or remote transport is granted. Use pread/pwrite
through FD9 because the inherited open description shares its offset.

No-argument workload has a checked60-second monotonic deadline; each phase has
at most5 seconds, and A descendant retirement has at most10 seconds from init
kill. Every actual timed futex WAIT uses5 seconds and ETIMEDOUT fails. Wake loops
have a total128-call budget PER process, record every signed result and sleep
1ms only after raw0. Failure cleanup has500 fixed10ms iterations for exact root
reaps, then failure-only exact captured-child SIGKILL fallback. Cleanup outcomes
cannot qualify successful retirement. The separate witness has30 seconds;
parent VM wall timeout is120 seconds. Userspace deadlines cannot interrupt an
individual stalled kernel syscall; the external exact-owned stop remains the
final bound. No unlimited EINTR retry occurs in a polling/IO loop.

## Actual phases and required outcomes

1. Host, A and B create4096-byte0600 SysV objects using the SAME key0x4039;
   distinct IDs and exact own lookups are required. A/B attach at EXACT same
   free VA0x0000200100000000. shmat rejects occupied space; no MAP_FIXED is used.
   Both real PID1 roots remain live together through all sharing phases. Host
   records returned physical roots, captures exactly one physical child per
   root through fresh Host proc PID/TGID/PPID/UID100/Name=probe rows, and checks
   actual local identity reports. Both keep different `/same/name` bytes.
2. A/B receive only Host/sibling integer shmid names, which confer no grant.
   Each raw foreign ATTACH, guarded STAT and RMID returns -EINVAL. All guard/
   output bytes remain unchanged; own lookup/stat/new attach remain valid.
   Private signal0 of guessed physical Host and sibling names>2 returns -ESRCH.
3. Mirror two negative futex phases. One root waits on its own SysV word0;
   Host sees its actual WAITING/futex_wait identity before the other root's raw
   WAKE at identical VA returns0 on distinct backing. The waiting root's
   inherited child then gets raw WAKE1, and the root returns WAIT0 and publishes
   fresh command-sequence ACK. One waiter per word/generation; no timeout counts
   as an ACK.
4. Each local root/child pair performs16 mutable shared SysV rounds. Host sees
   the actual child WAITING/futex_wait before root writes new payload+complement
   and raw WAKE1. Child WAIT0 reads the exact new bytes and release-publishes ACK;
   root acquire-observes that exact ACK. All32 rounds must finish.
5. Each root promptly materializes immutable FD10 using PROT_READ|MAP_SHARED,
   offset0, different nonfixed hints0x200200000000/0x200300000000. Different actual
   returned VAs are required. The mapped zero word is NEVER written. Execute16
   ordered rounds each direction: actual root WAITING/futex_wait, cross-root
   raw WAKE1, WAIT0 and fresh ACK. Neither Host nor children waits on that word.
6. Host archives completed A root/child sharing-prefix reports, then commands A
   child into a30-second nanosleep, observing actual WAITING/nanosleep and exact
   identity before killing ONLY A physical init. Kill occurs within2 seconds of
   the parked sample. Exact Host reap returns A physical PID/raw status9; the
   pre-captured child signal0 becomes -ESRCH within10 seconds of kill, before the
   natural sleep expiry. Successful execution never signals that child directly.
7. B root+child remain alive and progress after A disappears. Their own SHM
   payload/complement/ACK and marker remain intact; fresh B WAITING/nanosleep
   proc snapshot is stored. B cooperatively stops its child and locally reaps
   PID2/raw status0, then cleans its own object/mapping and exits0. Host exact
   reaps B physical PID/status0, reconfirms unchanged Host object and immutable
   backing, closes capabilities and saves complete receipts.

Host centrally coordinates generation-specific READY/DONE records and samples
fresh proc files. Proc fields are diagnostic samples, not one coherent internal
scheduler snapshot. Positive wake counts, retained identity, raw wait results
and fresh acknowledgements provide the stronger operation evidence.

## Concrete OS limitations and followups

Independent writable regular-File MAP_SHARED currently allocates separate
physical pages in lazy/eager map paths; shared-bit/writeback tracking does not
establish immediate cross-process coherence. This fixture uses inherited SysV
PAGE_SHARED for actual mutable sharing and DOES NOT qualify writable File maps.

Read-only File cache keys omit namespace and include actual content identity,
dev/ino/size/mtime/ctime/offset. Holding a live map does not prevent cache entry
eviction/reclaim; later maps may receive different pages. Repeated WAKE1 proves
common backing only for the actual tested mapping lifetime/rounds. It is not
proof of robust shared-object identity under eviction, write mutation, cache
disable or memory pressure. Default cache must remain enabled; mismatch fails
closed rather than substituting another backing.

Native Spec admits regular File capabilities, not arbitrary SysV-handle grants.
The inherited local fork shares SysV by the actual fork path, not by an invented
cross-compartment handle ABI. This bounded campaign does not complete Goal4.

The qualified Stage A replay requires private own-STAT creator/operator names1/1
under the current immutable PID-generation projection. Stage B below extends
that boundary with actual operator changes and retained creator history.
Host accounting/physical process identities remain separate. Disclosed integer
metadata grants no signal/IPC capability; foreign generation projection0 is
covered by native tests and remains outside this public fixture.

Host wait proves actual reap after descriptor cleanup. Child ESRCH/proc absence
proves absence of a live target; it does not prove every namespace/File/Task
reference, deferred pagemap GC or retirement row has drained. Exact census and
scoped scheduled allocation-failure qualification remains a separate campaign.

## Fixed binary reports

Exactly five workload reports are required: `host.bin`, `root-a.bin`,
`root-b.bin`, `child-a.bin`, `child-b.bin`. Each is2048 bytes,256 LE64 words,
MAGIC0x3430434f4d505348/version1. Signed values use two's-complement64 bits.
Only explicit arrays are serialized; no C++ struct padding is a report field.
Word255 is commit nonce and must equal word3. Nonzero nonce and identities must
match across all reports/control records/status and the actual init spawn.

| Word(s) | Meaning |
| --- | --- |
|0–4|magic,version,kind(Host0/root1/child2),nonce,role(Host0/A1/B2)|
|5–10|visible PID,PPID,UID,EUID,GID,EGID|
|11–15|stage,checks,failures,first failure code,failed-category bits|
|16–19|signed wait-log count,wake-log count,deadline errors,clock errors|
|20–28|RW wait0,RW wake1,RW ACKs,foreign wake0,rescue wake1,negative wait0,File wake1,File wait0,File ACKs|
|29–33|foreign ATTACH bits,STAT bits,RMID bits,unchanged guard bits,own-key lookup flag|
|34–38|File zero flag,readonly rights flag,marker flag,hidden Host path flag,signal-name refusal bits|
|39–46|own shmid,SysV VA,File VA,key,local fork return,physical root,physical child,Host supervisor PID|
|47–51|progress count,long-sleep announcement time,private intended return code,own-stat preserved flag,last RW payload|
|52–56|terminal local raw waitpid,status,WNOHANG-zero count,EINTR count,report IO errors|
|57–60|start_us,end_us,last command sequence,last command opcode|
|61–63|reserved0|
|64–95|private rows: first word16 signed raw FUTEX_WAIT results/remainder0; Host64–83: guarded event11 STAT below,84–95zero|
|96–223|first word17 signed raw FUTEX_WAKE results, INCLUDING every raw0 retry; remainder0|
|224–254|kind-specific fields below|
|255|nonce commit|

Stages are START0/READY1/PREFIX2/GRACEFUL3/COMPLETE4/FAILED5. A root+child rows
are completed PREFIX2 sharing receipts, with return word49 UINT64_MAX. They do
not claim normal exit or final sleep return. B root+child are GRACEFUL3/return0;
Host is COMPLETE4/return0. All required rows have positive checks and zero
failure/first/bits/clock/deadline/IO errors. Failure codes1..16 are CLOCK,
DEADLINE,IDENTITY,IO,CONTROL,SHM,FOREIGN,FORK,FUTEX,PAYLOAD,MAPPING,PROC,SIGNAL,
WAIT,RETIRE,PRESERVATION; word15 bit(code-1) summarizes category, not check index.

For each private root, required counters:20=0,21=16,22=16,23=1,24=0,25=1,
26=16,27=16,28=16,29..32=3,33..37=1,38=3,39=own positive ID,40=exact VA,
41=nonzero/different across roots,42=key,43=2,16=17. All17 WAIT results are0.
WAKE log has exactly32 ones and at least one zero (the negative attempt);
all remaining recorded outcomes must be0 retries, with total<=128.

For each child,20=16,21=0,22=16,23=0,24=1,25..28=0,16=16; all16 WAIT
results0. Its WAKE log has exactly one1 and only possible0 retries, count<=128.
Root/child IDs39/40/42/44/45/46 match their role/Host. Payload51 is16. B rows
progress47>0; B root local wait52=2/status53=0. A prefix has no local reap claim.
Root kind-specific fields249=254 absentFDs and250=0 first badFD+1.

| Root word | Actual result or observation |
| --- | --- |
|224,225|Host/sibling shmid names|
|226,227|raw foreign ATTACH results, both -EINVAL|
|228,229|raw foreign STAT results, both -EINVAL|
|230,231|raw foreign RMID results, both -EINVAL|
|232,233|raw signal0 Host/sibling results, both -ESRCH|
|234–238|backing inode,size4096,device,FD9 F_GETFL,FD10 F_GETFL|
|239,240|libc readonly pwrite result-1 and errno EBADF|
|241,242|private own STAT creator/operator visible PID names1/1; physical identities remain44/45|
|244,245,246|own key lookupID,fresh own extra-attach returned VA,raw own STAT return0|
|249,250|absentFD count254,first badFD+1(0 on success)|
|other224–254|reserved0 for root; child prefix uses none|

| Host word | Required observation |
| --- | --- |
|224–227|physical Aroot,Broot,Achild,Bchild; all positive>2/distinct|
|228,229|actual CREATE results equal corresponding root IDs|
|230,231|raw A wait result==224/raw status9|
|232,233|raw B wait result==225/raw status0|
|234|actual A init SIGKILL result0|
|235|last observed A child signal0 result0 (if absent immediately, default0)|
|236|A child final signal0 result-ESRCH|
|237,238|B root/child signal0 after A death, both0|
|239,240|B root progress before0/after>0|
|241,242|fresh common-live interval start/end, end>=start and before A kill|
|243,244|A init kill time/A child absence time, 0<=difference<10s|
|245|actual A child WAITING/nanosleep sample time, kill difference<2s|
|246,247|raw A/B WNOHANG-zero polling counts|
|249,250|Host same-key shmid/distinct three-ID flag1|
|251–253|completed mutable32,readonly32,negative2 rounds|
|254|A child's fresh LONG_SLEEP announcement time, positive<=245|
|other224–254|reserved0|

Host word47 is4 successfully archived private rows (Host report itself is the
fifth). Host raw FUTEX counts16/17 are0; words64–83 now hold event11 STAT. Its own Host same-key object is
checked for preserved nonce/complement/zero word/lookup/stat before removal;
these checks never overwrite common-live/retirement observations.

All files are O_EXCL-created in the fresh directory. Per-role control Files are
16384 bytes with root command at0,child command512,root progress1024,child
progress1536,root row4096,child row8192 and root-only metadata row10240. Control/progress Records are64 words:
magic/version/nonce/role at0..3,sequence4,opcode5,state6,arguments7..9,
failure count10,visible PID/PPID/UID11..13,reserved14..62,commit sequence63.
One writer owns each disjoint slot. Writer first writes full payload with
commit0, then publishes final8-byte commit; readers require two identical
complete reads and exact expected sequence/opcode/nonce/role. Rows similarly
publish final nonce last. Prefix row is read only after its exact DONE ACK;
Host terminal archive is fsynced/closed before death. Live control capabilities
remain retained through the final command and actual reap.

The five primary terminal rows total10240 bytes; two supplemental rows add4096.
Maximum fixture data including two16384-byte controls,4096-byte immutable
backing,two<=4096-byte proc snapshots,4160-byte witness,two<=512-byte status
texts and markers<64 bytes is64640 bytes,
below64KiB. Preserve small receipts, not full completed VM disks.

Final status is <=512 bytes, exactly one newline. Successful schema:

```
PASS compartment-shared-runtime nonce=N host=P root-a=A root-b=B child-a=C child-b=D mutable=32 readonly=32 negative=2 wait-a=A status-a=9 wait-b=B status-b=0 retired=1 survivor=1 reports=5 failures=0 first=0
```

Failure lines use actual completed round/report counts. A PASS line alone is
insufficient: offline validator checks every report/log/identity/round/error,
both fresh proc snapshots, stable source/artifact pins, actual root waits and
independent init exit witness. Record INIT's actual serial spawn
`init supervisor: spawned compartment-shared-probe pid=P pgid=P generation=1 priority=...`
and require P==Host row5/status host; terminal snapshot clears PID/PGID and
cannot establish this binding alone.

## Independent Host init reap witness

Exactly `init-exit-snapshot.bin`4160 bytes ABI1 and `init-exit-status`<=512 bytes.
Read uses guarded buffers and retries only EAGAIN. Header size4160/version1,
sequence/published_ns>0,count1..16; every service name bounded NUL/history<=4.
Target service appears once,generation1,restarts0,EXITED6,READY2,PID/PGID-1,
raw last_wait_status0,last_error0; history contains positive timestamp<=published
with EXIT reason5 and destination EXITED6. Successful text:

```
PASS compartment-shared-init-exit service=compartment-shared-probe generation=1 restarts=0 sequence=N published-ns=T state=6 readiness=2 wait=0 error=0 history-exit=1 snapshot-bytes=4160 abi=1 uid=100 read-error=0 failures=0
```

Raw snapshot offsets: header size/version at0/4,state8,flags12,sequence16,
published24,count32; services start64/stride256. Service name0[64],state64,
readiness68,PID80/PGID88,generation96,restarts104,wait112/error116,historycount136;
history160/stride24, timestamp0/from8/to12/reason16. Text sequence/time must equal
raw snapshot. Only witness PASS may trigger external stop; probe PASS cannot
stop the VM before actual init has reaped it. Launcher0 is not probe exit0.
Witness own exit and natural poweroff are separate limits.

Builder requires a NEW explicit `--output` and refuses existing path/symlink.
Compile, inspect dependencies, pin source/ABI/ELF/Normal artifacts and review
the runner before execution. Parent uses rootless `--no-setup`, private COW/
reflink images, bounded storage inventory and unconditional exact-owned cleanup/
offline extraction/pruning on PASS and FAIL. No unowned image/PID cleanup.


## Stage B: operator changes and retained creator history

This successor adds five opcodes17 META_ROOT,18 META_CHILD_ATTACH,
19 META_CHILD_DETACH,20 HISTORY_CREATE,21 HISTORY_ATTACH. Original opcodes1–16,
all32 mutable rounds,32 read-only rounds,2 negative rounds and their complete
signed FUTEX logs, physical identity meanings, credentials/FD rights, original
60s/5s/10s/30s deadlines, old consuming wait and init witness remain unchanged.
The Normal eight-CPU Stage B run passes all 17 durable reports (57,222 bytes),
the full guarded STAT observations and every preserved sharing/init check.
The runner stops only its owned VM at the existing 120-second wall deadline
and verifies saved outputs; natural poweroff is not claimed. All four disposable
images are pruned. The saved runtime manifest binds the README as it stood at
execution; this qualification note is a later documentation-only amendment.

After INFO both roots capture guarded original-object STAT1/1/nattch2, command
their local child to attach (1/2/3), detach (1/2/2), then root extra attach
(1/1/3) and detach (1/1/2). Root META_ROOT phase0..3 consumes command2..5;
child ATTACH/DETACH consume2/3. Root directly rereads the child's existing
committed progress twice, validating nonce, role, local2/PPID1/UID100, exact
opcode/sequence/DONE, zero failure/reserved fields and raw returned values.
It does not trust Host-forwarded values alone. All extra original maps are
detached before the preserved original futex protocol begins.

After B POST_DEATH, child command23 HISTORY_CREATE genuinely GETs exclusive
0600/4096-byte key0x403a, attaches it and release-writes two uint64 words:
marker=nonce XOR0x8000000000000000 (replace zero with1). It stores the complement
first, then release-stores the marker as the publication word.
Marker is nonzero and different from nonce. Root command60 HISTORY_ATTACH
binds the committed child20 ID/VA/marker, performs its own real lookup, captures
STAT2/2/nattch1, attaches and reads the marker, then captures STAT2/1/nattch2.
Child STOP_CHILD command24 detaches only the original segment and raw-exits0,
intentionally leaving the history map for real implicit kernel exit cleanup.
Root EXIT_ROOT command61 preserves the old consuming wait block byte-exact.
Only after that actual local wait2/status0, it checks the retained marker and
STAT2/1/nattch1, detaches its history mapping, captures STAT2/1/nattch0, RMIDs
the exact owned ID and requires guarded STAT-EINVAL with every output byte
unchanged. This does not qualify full Task/namespace/reference GC.

Successful terminal primary/control sequences become A root PREFIX58/child
PREFIX21 then LONG_SLEEP22, and B root EXIT_ROOT61/child STOP_CHILD24. Original
primary MAGIC/version/counts and status reports=5 remain; supplemental writes
do not increment primary W_PROGRESS. A supplement is archived before A init
SIGKILL and B supplement only after graceful B root reap. The Host existing
cleanup block captures one additional full guarded STAT before its original
detach: physical Host creator/operator and nattch1 are required.

Only roots write their new2048-byte metadata row at FD9[10240,12288). Original
16384-byte control File and FD capabilities are unchanged. Host independently
reads identical committed rows twice and persists `metadata-a.bin` and
`metadata-b.bin` with O_EXCL/fsync/close. Total required downloads17, aggregate
maximum64640 bytes. The original15 files remain required with their old limits.

Supplement MAGIC0x343253594d455441/version1/kind3;256 LE64 words. Header0..47:

|Words|Meaning|
|---|---|
|0–8|magic,version1,kind3,run nonce,roleA1/B2,localroot1,localchild2,UID100,GID100|
|9–13|physical root,physical child,physical Host PID,original shmid,original VA|
|14–15|B history shmid,key0x403a;A0|
|16–20|child extra VA,raw ATTACH returnbits=VA,ATTACHseq2,raw DETACH0,DETACHseq3|
|21–23|root extra VA,raw ATTACH=VA,raw DETACH0|
|24–27|root META_ROOT phase0..3 sequences2/3/4/5|
|28–36|B child GET=ID,ATTACH=VA,VA,childseq23,rootseq60,root ATTACH=VA,rootVA,marker,complement|
|37–41|B actual localwait2,status0,history DETACH0,RMID0,terminal EXIT_ROOTseq61|
|42–47|coverage mask,STAT count,metadata checks>0,failures0,first actual STAT_us,last actual STAT_us|

A history fields14/15/28..41 are zero, count5/mask0x1f. B final count10/mask0x3ff
is mandatory; earlier B partial publications cannot qualify. Slots1..10 begin
word48,20words each; words248..254 zero,word255 committed run nonce. A slots6..10
are zero. Header46/47 exactly equal first/final STAT timestamps. Each timestamp
is finite, nondecreasing and within the unchanged workload interval.

Each20-word slot serializes event1..10, signed raw STAT returnbits, actual
monotonic_us after the syscall, beforeguard, all15 LE64 words of actual120-byte
ShmidDs, afterguard. Guard=0xa596c387e1d24b6f XOR nonce XOR(uint64(role)<<32)
XOR event; afterguard=complement. The source guarded buffer has exact136-byte
size/stat offset8/after offset128. Success requires raw0 and full120-byte
comparison against an independently memset-zero expected status populated
with key/UID/GID/cuid/cgid100/mode0600/seqexactshmid/size4096, zero times,
expected creator/operator/nattch. Every padding/reserved byte must remain zero.
Event10 requires raw-EINVAL and all120 prefilled0xa5 bytes plus both guards
unchanged. Events1..5 originalkey0x4039 are respectively creator/operator/nattch
1/1/2,1/2/3,1/2/2,1/1/3,1/1/2. Events6..9 historykey0x403a are2/2/1,2/1/2,
2/1/1,2/1/0;event10 is removed guarded-EINVAL.

Host primary words64..83 use the same slot layout with event11/role0 and full
physical Host PID for both creator/operator/nattch1, before original detach.
Host words61..63 and84..223 remain zero; Host FUTEX counters remain zero.
The full120-byte expected object is independently reconstructible from raw
reports; no trusted comparison bool substitutes for it.

New mapping/ID leases contain only userspace values and transfer ownership on
move. Ordinary destructor exits explicitly detach/remove owned additions.
Raw private_exit does not unwind C++; private_fail explicitly cleans new
leases before publishing failure/raw exit. Successful child STOP_CHILD leaves
history live intentionally. Host exact positive-root kill and namespace
retirement are the bounded failure cleanup backstop, without successful-GC
or automatic-RAII-on-raw-exit claims. The builder remains byte unchanged and
must join the eventual tracked fixture CPP/README/builder commit only after
root admission, independent review, actual compile/format and bounded runtime.
