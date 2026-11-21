# Status

## Correctness evidence (native Windows build of the same source)
- Python reference (`reference/reference.py`): 36/36 fixtures, dev set (seed 1, 1M events) MATCH vs dev-answer.
- Rust (`src/`): 36/36 fixtures, dev set MATCH vs dev-answer.
- Differential Rust vs reference: 10 generated datasets, all identical
  (seeds 2,3,4,5,6,7,9,10,13,42; 1k to 2M events; click-heavy and click-light mixes).

## Pending
- Docker image check with scorer flags.
- Timing on the 20M-event set, then optimization.
