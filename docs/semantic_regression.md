# Semantic regression and mutation tests

WOS keeps source-shape tests as migration safeguards, but critical correctness
claims must also name executable evidence.  The versioned policy in
`configs/testing/semantic-invariants.v1.json` discovers every
`*_source_test.py` assertion clause, classifies it, assigns a risk and target
tier, and links critical claims to registered semantic tests and mutants.

Run the fast gate and materialize the expanded machine-readable inventory:

```sh
scripts/test/run_tests.sh inventory
```

The output is
`test-results/semantic-regression/invariant-inventory.json`.  The inventory
uses normalized AST fingerprints instead of line numbers, rejects duplicate
JSON keys, unreachable standalone `test_*` functions, stale replacement test
names, stale mutation operators, orphan mutants, and critical source-only
claims.

## Mutation workflow

Run all representative mutants, one domain, or one mutant:

```sh
scripts/test/run_tests.sh mutation
scripts/test/run_tests.sh mutation scheduler
scripts/test/run_tests.sh mutation scheduler-handoff-preempt-depth
```

The runner copies the current `tests/`, `modules/`, and `shared/` trees into a
temporary directory.  That includes current uncommitted source changes without
modifying or restoring any checkout file.  For each manifest entry it:

1. builds and passes the focused baseline oracle;
2. requires exactly one mutation-site match;
3. applies the mutation only in the copy;
4. requires the mutant to compile and the semantic oracle to fail;
5. restores the copied file exactly, rebuilds, and passes the oracle again; and
6. records the exit evidence in
   `test-results/semantic-regression/mutation-report.json`.

A compile failure is invalid evidence rather than a killed mutant.  Commands
and tests have bounded timeouts.  If GoogleTest is already available from the
normal host-test build, it is reused through `WOS_GTEST_SOURCE`; otherwise the
ordinary test CMake FetchContent path is used.

## Test tiers and limitations

The policy gives each tier an owner, evidence shape, and time budget.  Fast
host models and shims are useful for deterministic state and failure paths, but
they do not claim IRQ, NAPI, SMP, or physical-hardware fidelity.  KTEST, TESTD,
multi-node, fuzz, and explicitly user-run hardware evidence remain separate
tiers.

Live WOS validation and debugging can run rootlessly with the existing VM
topology by appending `--no-setup`:

```sh
bin/wos-ktest --fast --no-setup
bin/wos-cluster --launch --no-setup
```

Use `scripts/test/runtime_test_audit.py` to reject missing, duplicate,
unexpected, failed, or silently skipped KTEST/TESTD/userland records.  Coverage
is linked to invariant replacement tests in the expanded inventory; line
percentage alone is not proof of an invariant.

## Adding or retiring a claim

- New source-shape checks are discovered automatically and begin as
  implementation-shape debt.
- Retained ABI/layout/schema/policy checks need an explicit rule and rationale.
- A critical implementation claim needs a registered non-source replacement
  and a fresh manifest mutant before the gate passes.
- Keep the old source safeguard until its buildable mutant is killed.  Remove
  or retire it only after the semantic replacement is green at the declared
  tier.
- Hardware-only properties stay marked as user-run; do not substitute a host
  model and call the property complete.
