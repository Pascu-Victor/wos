# Goal19 9MO: non-reused mount identities

Mount publication now stops with `EOVERFLOW` after issuing `UINT32_MAX`, instead
of wrapping its 32-bit identity counter through zero and reusing earlier IDs.
Unmount and native-export retirement use these IDs to select an exact mount;
zero is invalid. The defect was established from source, without claiming a
runtime reproduction requiring billions of mount operations.

The existing mount lock protects both the counter and table. Publication checks
exhaustion after its existing validation and before inserting the mount or
changing the covered-directory reference, reservation or generation. Failed
insertion does not consume an identity. The public wrapper supplies the sole
production counter to a private implementation; no ABI, wire format, credential
decision, declared VFS routing or ordinary errno changes.

The actual kernel regression uses a private boundary counter through that same
implementation. It publishes the final ID, refuses two exhausted attempts with
unchanged candidate/backend/table/generation, publishes the same prepared
candidate through the ordinary wrapper, rejects the wrong unmount ID and removes
both exact mounts. It never rewinds the production counter. The fixture runs
before WKI initialization and refuses a production counter already at exhaustion.

Normal Build WOS, the isolated diagnostic build, formatting/tidy, policy/source
checks and the three credential/lookup/WKI host models passed. Full runtime and
shutdown receipts are recorded in
`build/goal19-mount-identity/validation.json`. Existing compiler/tidy warnings are
not represented as warning-free validation. An initially invalid build-only
`--no-setup` combination and the source test's old public-body location are
retained as failed initial receipts, followed by successful corrected commands.

This correction leaves all 21 domains present: 16 enforced and 5 pending. Mount
qualification remains a separate review. Goal19 stays active.
