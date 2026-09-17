#!/bin/bash
# Samples a run's live.json every 2 s into <run>.timeline (one JSON line each)
S=$1; run=$2
until [ -f $S/$run/live.json ]; do sleep 2; done
while [ -f $S/$run/live.json ]; do
  jq -c '{t:.elapsedSec, s50:.last10s.sealP50ms, s90:.last10s.sealP90ms, smax:.last10s.sealMaxMs, fs:.heapFsyncMsAvg, kb:.heapSyncKBAvg, syncs:.heapSyncs, snaps:.heapSnapshots, snapms:.heapSnapshotMs, rel:.heapReleases, relms:.heapReleaseMs, mv:.heapMovedMB, hole:.heapHoleMB, live:.heapLiveMB, merges:.permMerges, folds:.permFolds, inflight:.maintenanceInFlight, ps:((.last10s.storeSealP90ms // [])|map(floor))}' $S/$run/live.json 2>/dev/null >> $S/$run.timeline
  sleep 2
done
