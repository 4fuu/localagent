"""加密环境变量存储，按 person / conversation scope 隔离。"""

import base64
import hashlib
import json
import logging
import os
from pathlib import Path

from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)

SECRETS_FILE = Path(".localagent/secrets.json")
SECRET_KEY_ENV = "LOCALAGENT_SECRET_KEY"
PERSON_SCOPE_PREFIX = "person:"
CONVERSATION_SCOPE_PREFIX = "conversation:"


def _get_fernet() -> Fernet:
    key = os.environ.get(SECRET_KEY_ENV, "")
    if not key:
        raise RuntimeError(f"环境变量 {SECRET_KEY_ENV} 未设置")
    derived = hashlib.sha256(key.encode()).digest()
    return Fernet(base64.urlsafe_b64encode(derived))


def _normalize_scope(scope: str) -> str:
    return scope.strip()


def person_scope(person_id: str) -> str:
    normalized = str(person_id).strip()
    if not normalized:
        return ""
    return f"{PERSON_SCOPE_PREFIX}{normalized}"


def conversation_scope(conversation_id: str) -> str:
    normalized = str(conversation_id).strip()
    if not normalized:
        return ""
    return f"{CONVERSATION_SCOPE_PREFIX}{normalized}"


def _scope_candidates(scope: str) -> list[str]:
    normalized = _normalize_scope(scope)
    if not normalized:
        return []
    candidates = [normalized]
    if normalized.startswith(CONVERSATION_SCOPE_PREFIX):
        legacy = normalized[len(CONVERSATION_SCOPE_PREFIX):].strip()
        if legacy:
            candidates.append(legacy)
    return candidates


def _load() -> dict[str, dict[str, str]]:
    if not SECRETS_FILE.is_file():
        return {}
    raw = json.loads(SECRETS_FILE.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    if raw and all(isinstance(value, str) for value in raw.values()):
        return {"": {str(key): str(value) for key, value in raw.items()}}
    data: dict[str, dict[str, str]] = {}
    for scope, values in raw.items():
        if not isinstance(values, dict):
            continue
        data[str(scope)] = {
            str(key): str(value)
            for key, value in values.items()
            if isinstance(value, str)
        }
    return data


def _save(data: dict[str, dict[str, str]]) -> None:
    SECRETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    SECRETS_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def set_secret(scope: str, key: str, value: str) -> None:
    f = _get_fernet()
    normalized_scope = _normalize_scope(scope)
    data = _load()
    bucket = data.setdefault(normalized_scope, {})
    bucket[key] = f.encrypt(value.encode("utf-8")).decode("ascii")
    _save(data)


def delete_secret(scope: str, key: str) -> bool:
    data = _load()
    deleted = False
    for candidate in _scope_candidates(scope):
        bucket = data.get(candidate, {})
        if key not in bucket:
            continue
        del bucket[key]
        if not bucket:
            data.pop(candidate, None)
        deleted = True
    if deleted:
        _save(data)
    return deleted


def list_secrets(scope: str) -> list[str]:
    data = _load()
    keys: list[str] = []
    seen: set[str] = set()
    for candidate in _scope_candidates(scope):
        for key in (data.get(candidate) or {}).keys():
            if key in seen:
                continue
            seen.add(key)
            keys.append(key)
    return keys


def load_all_decrypted(scope: str | list[str] | None = None) -> dict[str, str]:
    """解密 secret；scope=None 时返回全量，用于 scrub。"""
    data = _load()
    if not data:
        return {}
    f = _get_fernet()
    result: dict[str, str] = {}
    buckets: list[tuple[str, dict[str, str]]]
    if scope is None:
        buckets = list(data.items())
    elif isinstance(scope, list):
        buckets = []
        for raw_scope in scope:
            for candidate in _scope_candidates(raw_scope):
                buckets.append((candidate, data.get(candidate, {})))
    else:
        buckets = [
            (candidate, data.get(candidate, {}))
            for candidate in _scope_candidates(scope)
        ]
    for scope_key, values in buckets:
        for key, value in values.items():
            try:
                result[key] = f.decrypt(value.encode("ascii")).decode("utf-8")
            except Exception:
                logger.warning("Failed to decrypt secret scope=%s key=%s", scope_key, key)
    return result


def scrub_text(text: str, secrets: dict[str, str]) -> str:
    """将文本中出现的 secret value 替换为 '***'。"""
    for v in secrets.values():
        if v and v in text:
            text = text.replace(v, "***")
    return text
