#!/usr/bin/env python3
"""生成 Postman 上传测试用的合法 / 非法样例文件。

用法（仓库根目录）:
  python scripts/generate_testdata.py
  python scripts/generate_testdata.py --oversize   # 额外生成 ~50MB+1 的过大文件
"""
from __future__ import annotations

import argparse
import math
import struct
import wave
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TESTDATA = ROOT / "testdata"
VALID = TESTDATA / "valid"
INVALID = TESTDATA / "invalid"

# 与 api/config/settings.MAX_FILE_SIZE_MB 一致
MAX_FILE_SIZE_MB = 50
OVERSIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024 + 1


def _write_tone_wav(path: Path, *, freq_hz: float, duration_s: float = 0.15) -> None:
    """写一段短 PCM WAV；不同 freq 保证内容哈希不同。"""
    rate = 8000
    n = int(rate * duration_s)
    path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(path), "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(rate)
        frames = bytearray()
        for i in range(n):
            sample = int(16000 * math.sin(2 * math.pi * freq_hz * i / rate))
            frames.extend(struct.pack("<h", sample))
        wf.writeframes(frames)


def _write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def generate_valid() -> list[Path]:
    paths: list[Path] = []
    # valid_01..05.wav — 真实短 WAV
    for i in range(1, 6):
        p = VALID / f"valid_{i:02d}.wav"
        _write_tone_wav(p, freq_hz=220.0 * i, duration_s=0.12 + i * 0.02)
        paths.append(p)

    # valid_06..10.mp3 / 11..15.m4a / 16..20.aac — 扩展名合法 + 非空互异内容
    specs = (
        (range(6, 11), ".mp3", "mock-mp3"),
        (range(11, 16), ".m4a", "mock-m4a"),
        (range(16, 21), ".aac", "mock-aac"),
    )
    for indices, ext, prefix in specs:
        for i in indices:
            p = VALID / f"valid_{i:02d}{ext}"
            _write_bytes(p, f"{prefix}-{i:02d}-unique-payload\n".encode("utf-8"))
            paths.append(p)
    return paths


def generate_invalid(*, oversize: bool) -> list[Path]:
    paths: list[Path] = []

    # 空文件（扩展名合法 → 400 空内容）
    for name in ("empty.wav", "empty.mp3"):
        p = INVALID / name
        _write_bytes(p, b"")
        paths.append(p)

    # 错误扩展名 / 无扩展名（内容非空）
    bad_files = {
        "bad.txt": b"not-an-audio-file\n",
        "bad.flac": b"fake-flac\n",
        "bad.ogg": b"fake-ogg\n",
        "bad.mp4": b"fake-mp4\n",
        "bad.doc": b"fake-doc\n",
        "no_ext": b"no-extension-payload\n",
        "notes.md": b"# not audio\n",
        "archive.zip": b"PK\x03\x04fake\n",
    }
    for name, content in bad_files.items():
        p = INVALID / name
        _write_bytes(p, content)
        paths.append(p)

    # 凑满 10 个固定非法样例（不含 oversize）
    assert len(paths) == 10, f"expected 10 invalid fixtures, got {len(paths)}"

    if oversize:
        p = INVALID / "too_large.wav"
        # 小 WAV 头 + 填充，整文件 > 50MB；内容以可识别前缀开头
        header = b"RIFF....WAVEfmt "  # 非严格合法头，仅测大小拒绝
        remaining = OVERSIZE_BYTES - len(header)
        with p.open("wb") as f:
            f.write(header)
            chunk = b"\x00" * (1024 * 1024)
            written = 0
            while written + len(chunk) <= remaining:
                f.write(chunk)
                written += len(chunk)
            if written < remaining:
                f.write(b"\x00" * (remaining - written))
        paths.append(p)

    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Postman upload fixtures")
    parser.add_argument(
        "--oversize",
        action="store_true",
        help=f"also write invalid/too_large.wav (~{MAX_FILE_SIZE_MB}MB+1)",
    )
    args = parser.parse_args()

    VALID.mkdir(parents=True, exist_ok=True)
    INVALID.mkdir(parents=True, exist_ok=True)

    valid = generate_valid()
    invalid = generate_invalid(oversize=args.oversize)

    print(f"valid:   {len(valid)} files under {VALID.relative_to(ROOT)}")
    for p in valid:
        print(f"  {p.relative_to(ROOT)}  ({p.stat().st_size} bytes)")
    fixed_invalid = [p for p in invalid if p.name != "too_large.wav"]
    print(f"invalid: {len(fixed_invalid)} files under {INVALID.relative_to(ROOT)}")
    for p in fixed_invalid:
        print(f"  {p.relative_to(ROOT)}  ({p.stat().st_size} bytes)")
    if args.oversize:
        big = INVALID / "too_large.wav"
        print(f"oversize: {big.relative_to(ROOT)}  ({big.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
