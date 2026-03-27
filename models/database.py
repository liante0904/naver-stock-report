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
            # 컬럼명 대문자 통일 및 ATTACH_URL -> PDF_URL 변경
            conn.execute('''
                CREATE TABLE IF NOT EXISTS report_history (
                    ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    TITLE TEXT,
                    URL TEXT UNIQUE,
                    PDF_URL TEXT,
                    SOURCE TEXT,
                    BROKER TEXT,
                    SENT_YN TEXT DEFAULT 'N',
                    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 기존 테이블에 PDF_URL 컬럼이 없을 경우를 대비한 마이그레이션 (선택적)
            try:
                conn.execute("ALTER TABLE report_history ADD COLUMN PDF_URL TEXT")
            except sqlite3.OperationalError:
                pass # 이미 존재함
                
            conn.commit()

    def insert_report(self, title, url, source, broker, pdf_url=None, sent_yn='N'):
        try:
            with self._get_connection() as conn:
                conn.execute('''
                    INSERT INTO report_history (TITLE, URL, PDF_URL, SOURCE, BROKER, SENT_YN) 
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (title, url, pdf_url, source, broker, sent_yn))
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
            cursor = conn.execute("SELECT * FROM report_history WHERE SENT_YN = 'N' ORDER BY ID ASC")
            return [dict(row) for row in cursor.fetchall()]

    def update_sent_status(self, report_ids):
        if not report_ids: return
        with self._get_connection() as conn:
            placeholders = ','.join(['?'] * len(report_ids))
            conn.execute(f"UPDATE report_history SET SENT_YN = 'Y' WHERE ID IN ({placeholders})", report_ids)
            conn.commit()

    def update_report_pdf_url(self, report_id, pdf_url):
        with self._get_connection() as conn:
            conn.execute("UPDATE report_history SET PDF_URL = ? WHERE ID = ?", (pdf_url, report_id))
            conn.commit()
