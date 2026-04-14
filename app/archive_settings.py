import json
import threading
from datetime import datetime, timezone
from pathlib import Path

import config

_SETTINGS_LOCK = threading.RLock()


def _settings_file_path() -> Path:
    raw_path = config.CREDENTIALS.get("ARCHIVE_ROOT_SETTINGS_FILE") or "archive_root_settings.json"
    return Path(raw_path)


def _normalize_folder_ids(values):
    seen = set()
    normalized = []
    for value in values or []:
        candidate = str(value or "").strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        normalized.append(candidate)
    return normalized


def parse_archive_root_ids(raw_text):
    if not raw_text:
        return []
    return _normalize_folder_ids(raw_text.replace(",", "\n").splitlines())


def _default_settings(default_root_id):
    return {
        "archive_root_folder_ids": [default_root_id],
        "active_archive_root_folder_id": default_root_id,
        "updated_at": None,
    }


def _load_settings_unlocked(default_root_id):
    settings = _default_settings(default_root_id)
    settings_path = _settings_file_path()
    if not settings_path.exists():
        return settings

    try:
        payload = json.loads(settings_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return settings

    folder_ids = _normalize_folder_ids(payload.get("archive_root_folder_ids"))
    if not folder_ids:
        folder_ids = [default_root_id]

    active_root_id = str(payload.get("active_archive_root_folder_id") or "").strip()
    if active_root_id and active_root_id not in folder_ids:
        folder_ids.append(active_root_id)
    if not active_root_id:
        active_root_id = folder_ids[0]

    return {
        "archive_root_folder_ids": folder_ids,
        "active_archive_root_folder_id": active_root_id,
        "updated_at": payload.get("updated_at"),
    }


def get_archive_root_settings(default_root_id):
    with _SETTINGS_LOCK:
        return _load_settings_unlocked(default_root_id)


def get_active_archive_root_id(default_root_id, override_root_id=None):
    override = str(override_root_id or "").strip()
    if override:
        return override
    settings = get_archive_root_settings(default_root_id)
    return settings.get("active_archive_root_folder_id") or default_root_id


def update_archive_root_settings(*, folder_ids, active_root_id, default_root_id):
    normalized_folder_ids = _normalize_folder_ids(folder_ids)
    if not normalized_folder_ids:
        raise ValueError("At least one archive root folder ID is required.")

    active = str(active_root_id or "").strip()
    if not active:
        active = normalized_folder_ids[0]
    elif active not in normalized_folder_ids:
        normalized_folder_ids.append(active)

    payload = {
        "archive_root_folder_ids": normalized_folder_ids,
        "active_archive_root_folder_id": active,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    with _SETTINGS_LOCK:
        settings_path = _settings_file_path()
        settings_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = settings_path.with_name(f"{settings_path.name}.tmp")
        temp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        temp_path.replace(settings_path)

    return payload
