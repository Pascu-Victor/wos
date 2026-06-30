## Debugging Approach

- When the user reports a bug, ask targeted clarifying questions BEFORE extensive code exploration, especially when the user has direct debugging access
- Stay focused on the specific subsystem the user identifies; do not investigate unrelated paths unless explicitly asked
- When asked for a specific deliverable (e.g., breakpoint locations, hypothesis, logging), produce that first before broader exploration
- After fixing a bug pattern, audit the codebase for other call sites with the same broken pattern
- Use a task agent to complete searches that will require a large scope, then come back with a summary so we can fix them in one pass.

## Code Editing Rules

- For mlibc-related changes, edit source in the mlibc source folder, NOT the generated sysroot headers
- When adding defensive guards, ensure bounds/cap checks come BEFORE any pointer dereference
- Verify const-correctness when calling methods from const iteration contexts

## Tooling Notes

- This kernel project uses KASAN/KUBSAN; sanitizer-related debugging should consider shadow memory mapping (preferred) over no_sanitize annotations
