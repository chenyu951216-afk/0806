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
_v43_parts = sorted((_here / "patch").glob("v43_patch_*.b64"))
_v44_parts = sorted((_here / "patch").glob("v44_patch_*.b64"))
_v50_parts = sorted((_here / "patch").glob("v50_patch_*.b64"))
for name, parts in {
    "v4 base": _base_parts,
    "v4.1 patch": _v41_parts,
    "v4.2 patch": _v42_parts,
    "v4.3 patch": _v43_parts,
    "v4.4 patch": _v44_parts,
    "v5.0 patch": _v50_parts,
}.items():
    if not parts:
        raise RuntimeError(f"Missing {name}")

_base_source = gzip.decompress(base64.b64decode("".join(p.read_text().strip() for p in _base_parts)))
if hashlib.sha256(_base_source).hexdigest() != "ecabff72b7b24710c8181e7c741ae6be6eccdcfba0ce27ee1ad5bc383d4467bb":
    raise RuntimeError("v4 base payload checksum mismatch")

_specs = [
    ("v4.1", _v41_parts, "8fe36cb53f36e7376ee85cfc3e0c5cfec3b1bb40b7bf0d0d632ed73a51420d26", "c748623073d6cd53de43bb056f32326a423bf46a1d20f61cdb048bfb7b2d100e"),
    ("v4.2", _v42_parts, "65b235bb42f1e21bf8d691fef526cc3cb160d550f994b464a3387c973faaf5bb", "90c80191ae5b0680b6b298a432eea8a3884aac1c579c332518b95bd23b825509"),
    ("v4.3", _v43_parts, "fa2cc81ecdb6a0d41f5bfef3dfce11099216bed155072d862bb2f75eb4b9554c", "e1c0182b38a3aac8c7e55f7e82a7d1c020d5189fdc82b84754b266609921efea"),
    ("v4.4", _v44_parts, "1bdf23f94573e9b539929402e7c2a07054db1bb6df0260118e667f2e4df0e235", "b3bab91012f1893f40b97cc0671a0b2a0e0fe8dba58d03a68fa8e3d63c970e8d"),
    ("v5.0", _v50_parts, "434ccd18872c51c85a3a667d3eebd3e97ee61ff0e37b21bb273a151540333d4a", "7317c68ecc58279cf052cd6b16fdbe92d8b1b809d18bae52b048b6f1b0da838e"),
]

with tempfile.TemporaryDirectory(prefix="gate_v50_") as tmp:
    tmp_path = Path(tmp)
    current = tmp_path / "app_v4.py"
    current.write_bytes(_base_source)
    for idx, (version, parts, patch_sha, source_sha) in enumerate(_specs, start=1):
        patch_bytes = base64.b64decode("".join(p.read_text().strip() for p in parts))
        if hashlib.sha256(patch_bytes).hexdigest() != patch_sha:
            raise RuntimeError(f"{version} patch checksum mismatch")
        patch_path = tmp_path / f"patch_{idx}.zstpatch"
        output = tmp_path / f"app_{idx}.py"
        patch_path.write_bytes(patch_bytes)
        subprocess.run(["zstd", "-q", "-d", f"--patch-from={current}", str(patch_path), "-o", str(output)], check=True)
        rebuilt = output.read_bytes()
        if hashlib.sha256(rebuilt).hexdigest() != source_sha:
            raise RuntimeError(f"{version} source checksum mismatch")
        current = output
    _source_bytes = current.read_bytes()

_source = _source_bytes.decode("utf-8")
exec(compile(_source, str(_here / "app_source_v5_0.py"), "exec"), globals(), globals())
