import threading
import contextvars
import uuid
from datetime import datetime

_jobs_lock = threading.Lock()
_jobs = {}
_current_job_id = contextvars.ContextVar("current_job_id", default=None)


def _now_iso():
    return datetime.utcnow().isoformat() + "Z"


def create_job(initial_status=None):
    job_id = uuid.uuid4().hex
    status = {
        "running": True,
        "progress": "Starting migration",
        "details": "",
        "started_at": _now_iso(),
        "finished_at": None,
    }
    if initial_status:
        status.update(initial_status)
    with _jobs_lock:
        _jobs[job_id] = {
            "status": status,
            "cancel_requested": False,
        }
    return job_id


def set_current_job(job_id):
    return _current_job_id.set(job_id)


def reset_current_job(token):
    _current_job_id.reset(token)


def get_status(job_id):
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return None
        return dict(job["status"])


def update_status(job_id, *, running=None, progress=None, details=None, finished=False):
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return False
        status = job["status"]
        if running is not None:
            status["running"] = running
        if progress is not None:
            status["progress"] = progress
        if details is not None:
            status["details"] = details
        if finished:
            status["finished_at"] = _now_iso()
        return True


def request_cancel(job_id):
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return False
        job["cancel_requested"] = True
        return True


def is_cancel_requested(job_id=None):
    if job_id is None:
        job_id = _current_job_id.get()
    if not job_id:
        return False
    with _jobs_lock:
        job = _jobs.get(job_id)
        return bool(job and job["cancel_requested"])
