# User address placement probes

Thread and ELF image placement now check an extent with the existing hierarchical
user VA walker, specialized to stop at its first occupied leaf. An absent table
proves its whole span empty, avoiding repeated hierarchy lookups for each 4 KiB
page of an empty stack region. Resource admission keeps the complete, exact
counting specialization; there is no cached or approximate address-space total.
The 4 KiB leaf loop adds exactly one page per occupied entry: public bounds and
recursive table clips are page aligned. Huge leaves retain clipped overlap
counting. The complete specialization still reads every leaf in the range.

Present and reserved pages remain occupied, including supervisor PROT_NONE and
present 1 GiB/2 MiB leaves. Invalid ranges and a failed root pin return false.
The nonwaiting pin prevents fork/teardown from reclaiming tables during the
bounded walk. Callers must still serialize mapping publication or own a private,
unpublished root. Entropy selection, placement windows, collision probes, stack
guards and sizes, resource limits, and accounting are unchanged.

The host test runs the actual placement and MM functions with controlled tables
and pin endpoints. It compares 3,000 randomized extents against independent
4 KiB lookups and checks exact counts, first-hit termination, reserved/PROT_NONE
pages, hierarchy boundaries, clipped huge leaves, and failed pins. Checks also
cover 32 mixed leaf tables and exact whole lower-half counts. Table-read counts
are algorithmic evidence; fresh KTEST and ordinary four-node measurements
are required before claiming runtime qualification or a launch-time improvement.
