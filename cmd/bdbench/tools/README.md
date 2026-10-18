# bdbench tools

Samplers that run beside a bdbench run and write one line every two
seconds, so that a minute's row can be taken apart afterwards.

- `timeline.sh <scratch> <run>` samples the run's `live.json` into
  `<run>.timeline`: the last ten seconds' seal p50/p90/max, the heap's
  cumulative fsync average and bytes per sync, snapshots, releases,
  moved bytes, hole and live bytes, perm merges and folds, and each
  store's seal p90 (`ps`).
- `sys-timeline.sh <scratch> <run>` samples the machine into
  `<run>.sys`: dirty and free memory, the NVMe device's write count,
  write ticks and in-flight requests from `/proc/diskstats`, and the
  load average.

Both wait for `<scratch>/<run>/live.json` to appear and stop when it
goes away (the run directory is removed).  Start them detached before
the run:

    setsid nohup tools/timeline.sh $S heap-2a >/dev/null 2>&1 &
    setsid nohup tools/sys-timeline.sh $S heap-2a >/dev/null 2>&1 &

## Reading them

Difference the cumulative fields between samples.  The device's write
ticks per 2-second window is the queue depth times the latency: 1-3 s
on this disk when healthy, 100-500 s when the drive is stalling.

A disk stall and a store tail look alike in the per-minute row (seal
p50 unchanged, p90 200-500 ms) and different here: a stall raises the
heap's fsync average for every store at once, the device's write ticks
with it, and the store's own counters (moved, snapshots, releases) show
no step.  A store tail lines up with a step in one of those counters
and the device stays quiet.  The disk this was learned on, and how it
was fixed, are in the operator's machine notes rather than here: on this
machine `~/infrastructure/disk-trim.md`.
