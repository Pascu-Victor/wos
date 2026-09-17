# Debugger workspace

The primary GUI is organized by evidence and machine state, not backend verbs.
The tool catalog remains a complete advanced interface; it is not the main
navigation model. Agent discovery and structured results retain their separate,
concise presentation.

## Reference patterns

- [Cutter's Debug menu](https://cutter.re/docs/user-docs/menus/menu-bar/debug-menu.html)
  separates starting/attaching from machine inspection. WOSDBG uses a dedicated
  attach flow with controls for its actual capabilities, not decorative stepping buttons.
- [Ghidra's debugger UI tour](https://ghidra.re/ghidra_docs/GhidraClass/Debugger/A2-UITour.html)
  describes connected machine-state panes and explicit session controls. WOSDBG
  follows addresses across its stack, registers, code, and memory and keeps
  specialist object/tool exploration secondary.
- [VS Code debugging](https://code.visualstudio.com/docs/debugtest/debugging)
  separates debug controls, call stacks, variables, and launch configuration.
  WOSDBG similarly keeps navigation and session controls stationary and exposes
  only backend-supported operations.

These are interaction references, not claims of feature parity or imported code.

## Layout and state

The outer main window owns a fixed workspace tab bar. An inner main window
owns the legacy viewer's docks and toolbars. Switching workspaces cannot move
the outer navigation as a side effect of showing or hiding those docks.

The native debugger owns an evidence sidebar, a welcome page, crash inspection,
incident evidence/timeline, and live attach. Request IDs route replies to the
originating operation; evidence and address generations reject obsolete replies.
Addresses remain strings. Native tables build an entire response snapshot with
updates disabled before exposing it. Missing values are shown as unavailable,
not invented or interpreted as zero.

Live discovery is not attachment. Explicit enablement is process-local;
attachment requires a reason and pause confirmation. The GUI holds lease
credentials internally, shows expiration, and provides renew/detach controls.
Sensitive raw memory has a separate confirmation. No real target is contacted
by tests.

## Scrolling contract

The log client reads aligned 1,024-row chunks with at most four in-flight
requests. It prioritizes visible rows and reads ahead on both sides. Rows far
from the viewport are evicted; the formatted-row cache is separately bounded.
Scrolling never disables loading and never caches a partially populated row.

A cold jump retains the last completely rendered viewport with an explicit
loading banner until all visible rows are ready. Stale-frame pointer actions
are blocked. File/filter transitions fence old responses and prefetch initial
data before exposing a new row count. Model resets discard retained frames.

## Verification boundaries

Host tests cover geometry, transport, state transitions, and delayed scrolling;
screenshots exercise actual widgets. They are not substitutes for a user's
desktop review, high-DPI/multimonitor testing, or user-run VM debugging.
Large incident output remains bounded; the UI calls out incomplete previews
and offers Advanced Analysis for full returned diagnostics and pagination.
