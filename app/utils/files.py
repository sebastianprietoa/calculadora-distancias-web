from __future__ import annotations

from io import BytesIO
from pathlib import Path
import tempfile
import uuid


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def temporary_file_path(suffix: str = ".xlsx") -> Path:
    temp_dir = Path(tempfile.gettempdir()) / "calculadora_distancias_web"
    ensure_dir(temp_dir)
    return temp_dir / f"{uuid.uuid4().hex}{suffix}"


def to_bytes_io(binary: bytes) -> BytesIO:
    return BytesIO(binary)
