# Decisions and open questions

The binding decisions of this repository's areas, then the owner questions still open. A decision is changed
in place when the owner changes it; a superseded or duplicate row is listed once under "Merged or dropped" with the
row that replaces it. A question leaves this file when it is answered: the answer becomes a decision row.

## Decisions

| key | date | who | decision | reason | source |
| --- | --- | --- | --- | --- | --- |
| DEC-7 | 23 Nov 2026 | owner | Releases of rho-tkg are always allowed, by `make release` after a reviewer's approval | - | - |
| DEC-31 | 19 Nov 2026 | owner | Read cost (RHO-20) is analysed and fixed in a general way; badger stays the store for the fixed-memory path (RHO-5); write arenas stay as they are (RHO-6); RHO-4, RHO-23, RHO-24 and PROC-57 are paused: necessary work first | - | - |

## Open owner questions

| key | question | options and recommendation | blocks | source |
| --- | --- | --- | --- | --- |
| Q-25 | Six rho items wait on the owner's word, each with rho's default: PROC-62 (skip), PROC-63 (detached after a release only), RHO-32 (a cold checkout loads the envelope), RHO-10 (none before the numbers), RHO-11 (none), RHO-45 (stays refused) | the defaults | PROC-62, PROC-63, RHO-32, RHO-10, RHO-11, RHO-45 | - |

## Merged or dropped

| key | replaced by or reason |
| --- | --- |
