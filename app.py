from __future__ import annotations

import base64
import lzma
from pathlib import Path

_here = Path(__file__).resolve().parent
_parts = sorted((_here / "payload").glob("app_payload_*.b64"))
if not _parts:
    raise RuntimeError("Missing compressed application payload")
_source = lzma.decompress(base64.b64decode("".join(p.read_text().strip() for p in _parts))).decode("utf-8")
exec(compile(_source, str(_here / "app_source.py"), "exec"), globals(), globals())
