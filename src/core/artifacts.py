"""Internal artifact store for binary attachments and task outputs."""

from __future__ import annotations

import hashlib
import json
import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ARTIFACT_ROOT = PROJECT_ROOT / ".localagent" / "runtime" / "artifacts"
OBJECTS_ROOT = ARTIFACT_ROOT / "objects"
META_ROOT = ARTIFACT_ROOT / "meta"
ARTIFACT_PREFIX = "artifact://"


def is_artifact_ref(value: str) -> bool:
    return str(value or "").strip().startswith(ARTIFACT_PREFIX)


def artifact_id_from_ref(value: str) -> str:
    raw = str(value or "").strip()
    if not raw.startswith(ARTIFACT_PREFIX):
        raise ValueError(f"invalid artifact ref: {value}")
    artifact_id = raw[len(ARTIFACT_PREFIX):].strip()
    if not artifact_id:
        raise ValueError(f"empty artifact id: {value}")
    return artifact_id


def artifact_ref(artifact_id: str) -> str:
    normalized = str(artifact_id or "").strip()
    if not normalized:
        raise ValueError("artifact_id cannot be empty")
    return f"{ARTIFACT_PREFIX}{normalized}"


class ArtifactStore:
    def __init__(self) -> None:
        OBJECTS_ROOT.mkdir(parents=True, exist_ok=True)
        META_ROOT.mkdir(parents=True, exist_ok=True)

    def put_bytes(
        self,
        data: bytes,
        *,
        file_name: str = "",
        mime_type: str = "",
    ) -> str:
        digest = hashlib.sha256(data).hexdigest()
        object_path = OBJECTS_ROOT / digest
        if not object_path.exists():
            object_path.write_bytes(data)
        if not mime_type and file_name:
            mime_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
        meta = {
            "id": digest,
            "file_name": file_name.strip(),
            "mime_type": mime_type.strip() or "application/octet-stream",
            "size_bytes": len(data),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._meta_path(digest).write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
        return artifact_ref(digest)

    def put_text(
        self,
        text: str,
        *,
        file_name: str = "",
        mime_type: str = "text/plain; charset=utf-8",
    ) -> str:
        return self.put_bytes(
            text.encode("utf-8"),
            file_name=file_name,
            mime_type=mime_type,
        )

    def import_file(
        self,
        path: str | Path,
        *,
        file_name: str = "",
        mime_type: str = "",
    ) -> str:
        target = Path(path).resolve()
        return self.put_bytes(
            target.read_bytes(),
            file_name=file_name or target.name,
            mime_type=mime_type,
        )

    def read_bytes(self, ref: str) -> bytes:
        artifact_id = artifact_id_from_ref(ref)
        return self._object_path(artifact_id).read_bytes()

    def stat(self, ref: str) -> dict[str, Any]:
        artifact_id = artifact_id_from_ref(ref)
        meta_path = self._meta_path(artifact_id)
        if meta_path.is_file():
            return json.loads(meta_path.read_text(encoding="utf-8"))
        object_path = self._object_path(artifact_id)
        return {
            "id": artifact_id,
            "file_name": artifact_id,
            "mime_type": "application/octet-stream",
            "size_bytes": object_path.stat().st_size,
        }

    @staticmethod
    def _object_path(artifact_id: str) -> Path:
        return OBJECTS_ROOT / artifact_id

    @staticmethod
    def _meta_path(artifact_id: str) -> Path:
        return META_ROOT / f"{artifact_id}.json"
