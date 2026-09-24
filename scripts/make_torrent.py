#!/usr/bin/env python3
"""Create a .torrent file (BEP3 format) from any file.

Usage:
    python scripts/make_torrent.py <file> [--out file.torrent] [--piece-length BYTES]
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bittorrent.metadata import TorrentMetadata, build_torrent

DEFAULT_PIECE_LENGTH = 256 * 1024


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a .torrent from a file.")
    parser.add_argument("file", help="the file to make a torrent for")
    parser.add_argument("--out", default=None, help="output path (default: <file>.torrent)")
    parser.add_argument(
        "--piece-length", type=int, default=DEFAULT_PIECE_LENGTH,
        help=f"bytes per piece (default: {DEFAULT_PIECE_LENGTH})",
    )
    args = parser.parse_args()

    with open(args.file, "rb") as fh:
        data = fh.read()

    name = os.path.basename(args.file)
    raw = build_torrent(data, name, args.piece_length)
    out = args.out or args.file + ".torrent"
    with open(out, "wb") as fh:
        fh.write(raw)

    meta = TorrentMetadata.from_bytes(raw)
    print(f"wrote {out}")
    print(f"  name:         {meta.name}")
    print(f"  info_hash:    {meta.info_hash.hex()}")
    print(f"  size:         {meta.total_length} bytes")
    print(f"  piece length: {meta.piece_length} bytes")
    print(f"  pieces:       {meta.piece_count}")


if __name__ == "__main__":
    main()
