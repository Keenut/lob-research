r"""compress_raw.py: gzip finished raw log hours to save disk space. Lossless and verified.

Usage (PowerShell, from the repo root):
    python compress_raw.py               # data\raw\BTCUSDT: every hour before the previous UTC hour
    python compress_raw.py --dry-run     # only list what would be compressed

The recorder writes the current UTC hour as plain text. The current and the previous hour are never
touched (the previous one as a margin, e.g. for a clock step back across the hour boundary).
For each older <hour>.log:
  1. compress it to <hour>.log.gz.tmp while computing the SHA-256 hash of the original bytes;
  2. decompress the .tmp and check that its SHA-256 is the same;
  3. rename the .tmp to <hour>.log.gz, then delete <hour>.log.
If it is interrupted, running it again finishes the job: a leftover .tmp is overwritten, and an hour
present both as .log and .log.gz is resolved by comparing the two contents before deleting the .log.
etl.py and check_continuity.py read .log and .log.gz alike.
"""
import argparse
import gzip
import hashlib
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from etl import hour_of

CHUNK = 1 << 20            # read and write 1 MiB at a time: memory stays small for any file size
EXPECTED_RATIO = 0.3       # pessimistic size of the .gz relative to the .log, for the free-space check


def sha256_of(f):
    """SHA-256 of everything left to read in a binary file object."""
    h = hashlib.sha256()
    for block in iter(lambda: f.read(CHUNK), b""):
        h.update(block)
    return h.hexdigest()


def compress_one(src, level=6):
    """Compress <hour>.log into <hour>.log.gz, verify, delete the .log. Returns (bytes_before, bytes_after)."""
    dst = src.with_name(src.name + ".gz")
    tmp = src.with_name(src.name + ".gz.tmp")
    h = hashlib.sha256()
    with open(src, "rb") as fin, gzip.open(tmp, "wb", compresslevel=level) as fout:
        for block in iter(lambda: fin.read(CHUNK), b""):
            h.update(block)
            fout.write(block)
    with gzip.open(tmp, "rb") as fcheck:
        if sha256_of(fcheck) != h.hexdigest():
            tmp.unlink()
            raise RuntimeError(f"{src.name}: the compressed copy does not decompress to the same bytes; kept the .log")
    before, after = src.stat().st_size, tmp.stat().st_size
    tmp.replace(dst)       # atomic rename: dst is either absent or complete
    src.unlink()
    return before, after


def resolve_duplicate(src, dst):
    """Both <hour>.log and <hour>.log.gz exist (an earlier run was interrupted between rename and delete).
    Delete the .log only if both hold exactly the same bytes. Returns the bytes freed."""
    with open(src, "rb") as a, gzip.open(dst, "rb") as b:
        if sha256_of(a) != sha256_of(b):
            raise RuntimeError(f"{src.name} and {dst.name} differ: kept both, check by hand")
    size = src.stat().st_size
    src.unlink()
    return size


def select(raw_dir, now_utc):
    """The plain .log hours that are older than the previous UTC hour, oldest first."""
    cutoff = (now_utc - timedelta(hours=1)).strftime("%Y-%m-%dT%H")
    return [p for p in sorted(raw_dir.glob("*.log")) if hour_of(p) < cutoff]


def main():
    ap = argparse.ArgumentParser(description="gzip finished raw log hours (lossless, verified)")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--raw", type=Path, default=Path("data/raw"))
    ap.add_argument("--dry-run", action="store_true", help="only list what would be compressed")
    ap.add_argument("--min-free-gb", type=float, default=0.2, help="keep at least this much disk space free")
    a = ap.parse_args()

    raw_dir = a.raw / a.symbol
    todo = select(raw_dir, datetime.now(timezone.utc))
    if not todo:
        print(f"nothing to compress in {raw_dir}")
        return
    total_before = total_after = 0
    for src in todo:
        size = src.stat().st_size
        dst = src.with_name(src.name + ".gz")
        if a.dry_run:
            print(f"would compress {src.name} ({size / 1e6:,.0f} MB)")
            continue
        if dst.exists():
            total_before += resolve_duplicate(src, dst)
            print(f"{src.name}: already compressed, deleted the duplicate .log")
            continue
        free = shutil.disk_usage(raw_dir).free
        need = size * EXPECTED_RATIO + a.min_free_gb * 1e9     # the .gz exists before the .log is deleted
        if free < need:
            print(f"stopping: {free / 1e9:.2f} GB free, {need / 1e9:.2f} GB needed to compress {src.name} safely")
            break
        t0 = time.time()
        before, after = compress_one(src)
        total_before += before
        total_after += after
        print(f"{src.name}: {before / 1e6:,.0f} MB -> {after / 1e6:,.0f} MB "
              f"(x{before / after:.1f}, {time.time() - t0:.1f} s)")
    if not a.dry_run:
        print(f"total: {total_before / 1e9:.2f} GB -> {total_after / 1e9:.2f} GB, "
              f"freed {(total_before - total_after) / 1e9:.2f} GB, "
              f"free now {shutil.disk_usage(raw_dir).free / 1e9:.2f} GB")


if __name__ == "__main__":
    main()