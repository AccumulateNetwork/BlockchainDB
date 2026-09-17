# The measurement disk must be trimmed

Written 2026-09-17, for the machine the load platform (`cmd/bdbench`,
spec 2.11) runs on.  It applies to any SSD the store is measured on.

## Why

An SSD does not learn that a file was deleted; the filesystem only
marks the blocks free in its own tables.  Until the filesystem sends a
discard ("trim") for them, the drive treats them as live data it must
preserve, and when it runs out of pre-erased flash under a sustained
write load it erases and rewrites around them.  On this machine that
showed as every fsync on the box going from 12-22 ms to 100-330 ms for
30-40 seconds at a time, with nothing in the store changing, and the
device's write ticks going from 1-3 s to 100-500 s per 2-second window
(`cmd/bdbench/tools/README.md`).  Half of a day's runs carried such
minutes.  They are not the store's, and they make the 30-minute
acceptance run (spec 2.11) meaningless until the disk is trimmed.

## How this disk is laid out

    nvme0n1p3  ->  cryptdata (LUKS2, dm-0)  ->  data-root (LVM linear, dm-1)  ->  ext4 /

Discards have to pass through every layer.  As found:

- The root filesystem is mounted without `discard`, so nothing is
  trimmed on delete.  Fine: the weekly `fstrim.timer` is the usual
  way.
- The timer's log showed it trimming only `/boot/efi`: `fstrim` skips
  a filesystem whose device reports no discard support.
- `lsblk -D` showed `cryptdata` and `data-root` both with `DISC-MAX`
  0: the LUKS mapping was opened without `allow-discards`, so the
  encrypted layer refused discards and the volume above it inherited
  that.

## What was done

1. `sudo cryptsetup refresh --allow-discards --persistent cryptdata`
   reloaded the encrypted mapping with discards allowed and wrote the
   flag into the LUKS2 header, so every future open has it without an
   `/etc/crypttab` change.  After it, `/sys/block/dm-0/queue/
   discard_max_bytes` read 2 TB.
2. `data-root` (dm-1) kept `discard_max_bytes` 0.  A volume computes
   its limits from the device beneath it when its table is loaded.
   `sudo lvchange --refresh data/root` and an explicit
   `dmsetup reload ... --table "0 7803994112 linear 252:0 2048" &&
   dmsetup resume` both ran without error and changed nothing: this
   kernel (6.17) did not recompute the limits of the mounted root
   volume on a live reload.
3. Remaining: a reboot, after which the volume activates over the
   discard-capable encrypted device, then `sudo fstrim -v /`, which
   should report over a terabyte discarded.  The weekly timer keeps it
   trimmed from then on.

## How to verify

    lsblk -D -o NAME,DISC-GRAN,DISC-MAX        # every layer non-zero
    cat /sys/block/dm-1/queue/discard_max_bytes  # non-zero
    sudo fstrim -v /                            # reports bytes trimmed
    journalctl -u fstrim.service                # the timer trims /

Then run the platform with the samplers and confirm the device's write
ticks stay at 1-3 s per window through a five-minute run.

## The trade-off

With discards on, someone holding the raw disk can tell which blocks
of the encrypted volume are unused.  For a development machine that is
acceptable; it is the reason distributions leave it off by default.
