# -*- coding:utf-8 -*-
import sqlite3
import os
from loguru import logger

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        with self._get_connection() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS report_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    url TEXT UNIQUE,
                    attach_url TEXT,
                    source TEXT,
                    broker TEXT,
                    sent_yn TEXT DEFAULT 'N',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()

    def insert_report(self, title, url, source, broker, attach_url=None, sent_yn='N'):
        try:
            with self._get_connection() as conn:
                conn.execute('''
                    INSERT INTO report_history (title, url, attach_url, source, broker, sent_yn) 
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (title, url, attach_url, source, broker, sent_yn))
                conn.commit()
                return True
        except sqlite3.IntegrityError:
            return False
        except Exception as e:
            logger.error(f"DB Insert Error: {e}")
            return False

    def get_unsent_reports(self):
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM report_history WHERE sent_yn = 'N' ORDER BY id ASC")
            return [dict(row) for row in cursor.fetchall()]

    def update_sent_status(self, report_ids):
        if not report_ids: return
        with self._get_connection() as conn:
            placeholders = ','.join(['?'] * len(report_ids))
            conn.execute(f"UPDATE report_history SET sent_yn = 'Y' WHERE id IN ({placeholders})", report_ids)
            conn.commit()
