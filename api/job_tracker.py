import json
import os
import threading
from typing import Dict, Optional


class JobTracker:
    def __init__(self, persistence_file: str = "jobs.json"):
        self.persistence_file = persistence_file
        self._lock = threading.Lock()
        self._jobs: Dict[str, Dict] = {}
        self._load()

    def _load(self):
        if os.path.exists(self.persistence_file):
            try:
                with open(self.persistence_file, "r") as f:
                    self._jobs = json.load(f)
            except Exception:
                self._jobs = {}

    def _save(self):
        try:
            with open(self.persistence_file, "w") as f:
                json.dump(self._jobs, f, indent=2)
        except Exception:
            pass

    def create_job(self, job_id: str, dataset_id: str, filename: str, rows_total: int) -> Dict:
        with self._lock:
            # If 0 data rows (e.g. header only), immediately mark complete
            initial_status = "complete" if rows_total == 0 else "queued"
            job = {
                "job_id": job_id,
                "dataset_id": dataset_id,
                "filename": filename,
                "status": initial_status,
                "rows_total": rows_total,
                "rows_loaded": 0,
                "rows_failed": 0,
            }
            self._jobs[job_id] = job
            self._save()
            return job.copy()

    def get_job(self, job_id: str) -> Optional[Dict]:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None
            return {
                "job_id": job["job_id"],
                "status": job["status"],
                "rows_total": job["rows_total"],
                "rows_loaded": job["rows_loaded"],
                "rows_failed": job["rows_failed"],
            }

    def update_progress(
        self, job_id: str, loaded_delta: int = 0, failed_delta: int = 0, mark_failed: bool = False
    ) -> Optional[Dict]:
        with self._lock:
            if job_id not in self._jobs:
                return None

            job = self._jobs[job_id]
            job["rows_loaded"] += loaded_delta
            job["rows_failed"] += failed_delta

            if mark_failed:
                job["status"] = "failed"
            elif job["rows_loaded"] + job["rows_failed"] >= job["rows_total"]:
                job["status"] = "complete"
            elif job["rows_loaded"] > 0 or job["rows_failed"] > 0:
                job["status"] = "loading"

            self._save()
            return job.copy()


tracker = JobTracker()
