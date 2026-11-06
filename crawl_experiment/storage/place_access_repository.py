"""Durable per-place access state, separate from review payload/idempotency."""
import sqlite3
from pathlib import Path
from threading import RLock


class PlaceAccessRepository:
    def __init__(self, path):
        if str(path) != ':memory:':
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db=sqlite3.connect(str(path),check_same_thread=False)
        self.db.row_factory=sqlite3.Row
        self.lock=RLock()
        self.db.execute('''CREATE TABLE IF NOT EXISTS place_crawl_access_state (
            place_id TEXT PRIMARY KEY, last_access_state TEXT,
            last_attempt_at TEXT, last_successful_crawl_at TEXT,
            consecutive_limited_count INTEGER NOT NULL DEFAULT 0,
            consecutive_unknown_count INTEGER NOT NULL DEFAULT 0,
            consecutive_runtime_error_count INTEGER NOT NULL DEFAULT 0,
            next_eligible_at TEXT)''')
        self.db.commit()

    def get(self, place_id):
        with self.lock:
            row=self.db.execute('SELECT * FROM place_crawl_access_state WHERE place_id=?',(place_id,)).fetchone()
            return dict(row) if row else dict(place_id=place_id,last_access_state=None,last_attempt_at=None,
                last_successful_crawl_at=None,consecutive_limited_count=0,consecutive_unknown_count=0,
                consecutive_runtime_error_count=0,next_eligible_at=None)

    def update(self, place_id, change):
        with self.lock, self.db:
            self.db.execute('BEGIN IMMEDIATE')
            row=self.get(place_id)
            change(row)
            columns=list(row)
            self.db.execute('INSERT INTO place_crawl_access_state ('+','.join(columns)+') VALUES ('+
                ','.join('?' for _ in columns)+') ON CONFLICT(place_id) DO UPDATE SET '+
                ','.join(c+'=excluded.'+c for c in columns if c!='place_id'),list(row.values()))
            return row

    def close(self):
        self.db.close()
