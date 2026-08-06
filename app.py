from __future__ import annotations

import base64
import gzip
import hashlib
import subprocess
import tempfile
from pathlib import Path

_here = Path(__file__).resolve().parent
_base_parts = sorted((_here / "payload").glob("app_payload_*.b64"))
_patch_parts = sorted((_here / "patch").glob("v41_patch_*.b64"))
if not _base_parts:
    raise RuntimeError("Missing v4 base payload")
if not _patch_parts:
    raise RuntimeError("Missing v4.1 patch payload")

_base_source = gzip.decompress(
    base64.b64decode("".join(p.read_text().strip() for p in _base_parts))
)
if hashlib.sha256(_base_source).hexdigest() != "ecabff72b7b24710c8181e7c741ae6be6eccdcfba0ce27ee1ad5bc383d4467bb":
    raise RuntimeError("v4 base payload checksum mismatch")

_patch_bytes = base64.b64decode(
    "".join(p.read_text().strip() for p in _patch_parts)
)
if hashlib.sha256(_patch_bytes).hexdigest() != "8fe36cb53f36e7376ee85cfc3e0c5cfec3b1bb40b7bf0d0d632ed73a51420d26":
    raise RuntimeError("v4.1 patch checksum mismatch")

with tempfile.TemporaryDirectory(prefix="gate_v41_") as tmp:
    tmp_path = Path(tmp)
    base_path = tmp_path / "app_v4.py"
    patch_path = tmp_path / "app_v41.zstpatch"
    output_path = tmp_path / "app_v41.py"
    base_path.write_bytes(_base_source)
    patch_path.write_bytes(_patch_bytes)
    subprocess.run(
        [
            "zstd",
            "-q",
            "-d",
            f"--patch-from={base_path}",
            str(patch_path),
            "-o",
            str(output_path),
        ],
        check=True,
    )
    _source_bytes = output_path.read_bytes()

if hashlib.sha256(_source_bytes).hexdigest() != "c748623073d6cd53de43bb056f32326a423bf46a1d20f61cdb048bfb7b2d100e":
    raise RuntimeError("v4.1 source checksum mismatch")
_source = _source_bytes.decode("utf-8")
exec(compile(_source, str(_here / "app_source_v4_1.py"), "exec"), globals(), globals())
