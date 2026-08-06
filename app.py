from __future__ import annotations

import base64
import gzip
import hashlib
import subprocess
import tempfile
from pathlib import Path

_here = Path(__file__).resolve().parent
_base_parts = sorted((_here / "payload").glob("app_payload_*.b64"))
_v41_parts = sorted((_here / "patch").glob("v41_patch_*.b64"))
_v42_parts = sorted((_here / "patch").glob("v42_patch_*.b64"))
if not _base_parts:
    raise RuntimeError("Missing v4 base payload")
if not _v41_parts:
    raise RuntimeError("Missing v4.1 patch payload")
if not _v42_parts:
    raise RuntimeError("Missing v4.2 patch payload")

_base_source = gzip.decompress(
    base64.b64decode("".join(p.read_text().strip() for p in _base_parts))
)
if hashlib.sha256(_base_source).hexdigest() != "ecabff72b7b24710c8181e7c741ae6be6eccdcfba0ce27ee1ad5bc383d4467bb":
    raise RuntimeError("v4 base payload checksum mismatch")

_v41_patch = base64.b64decode(
    "".join(p.read_text().strip() for p in _v41_parts)
)
if hashlib.sha256(_v41_patch).hexdigest() != "8fe36cb53f36e7376ee85cfc3e0c5cfec3b1bb40b7bf0d0d632ed73a51420d26":
    raise RuntimeError("v4.1 patch checksum mismatch")

_v42_patch = base64.b64decode(
    "".join(p.read_text().strip() for p in _v42_parts)
)
if hashlib.sha256(_v42_patch).hexdigest() != "65b235bb42f1e21bf8d691fef526cc3cb160d550f994b464a3387c973faaf5bb":
    raise RuntimeError("v4.2 patch checksum mismatch")

with tempfile.TemporaryDirectory(prefix="gate_v42_") as tmp:
    tmp_path = Path(tmp)
    v4_path = tmp_path / "app_v4.py"
    v41_patch_path = tmp_path / "app_v41.zstpatch"
    v41_path = tmp_path / "app_v41.py"
    v42_patch_path = tmp_path / "app_v42.zstpatch"
    v42_path = tmp_path / "app_v42.py"
    v4_path.write_bytes(_base_source)
    v41_patch_path.write_bytes(_v41_patch)
    subprocess.run(
        ["zstd", "-q", "-d", f"--patch-from={v4_path}", str(v41_patch_path), "-o", str(v41_path)],
        check=True,
    )
    v41_source = v41_path.read_bytes()
    if hashlib.sha256(v41_source).hexdigest() != "c748623073d6cd53de43bb056f32326a423bf46a1d20f61cdb048bfb7b2d100e":
        raise RuntimeError("v4.1 source checksum mismatch")
    v42_patch_path.write_bytes(_v42_patch)
    subprocess.run(
        ["zstd", "-q", "-d", f"--patch-from={v41_path}", str(v42_patch_path), "-o", str(v42_path)],
        check=True,
    )
    _source_bytes = v42_path.read_bytes()

if hashlib.sha256(_source_bytes).hexdigest() != "90c80191ae5b0680b6b298a432eea8a3884aac1c579c332518b95bd23b825509":
    raise RuntimeError("v4.2 source checksum mismatch")
_source = _source_bytes.decode("utf-8")
exec(compile(_source, str(_here / "app_source_v4_2.py"), "exec"), globals(), globals())
