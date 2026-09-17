#!/bin/bash
# System counters every 2 s while a run's live.json exists: memory and the device
S=$1; run=$2
until [ -f $S/$run/live.json ]; do sleep 2; done
while [ -f $S/$run/live.json ]; do
  t=$(jq -r .elapsedSec $S/$run/live.json 2>/dev/null)
  m=$(awk '/^(Dirty|Writeback|MemFree|MemAvailable|Cached):/{printf "%s=%d ", $1, $2/1024}' /proc/meminfo)
  d=$(grep -w nvme0n1 /proc/diskstats | awk '{printf "wr=%d wr_ms=%d inflight=%d io_ms=%d", $8, $11, $12, $13}')
  l=$(cut -d' ' -f1 /proc/loadavg)
  echo "t=$t $m $d load=$l" >> $S/$run.sys
  sleep 2
done
