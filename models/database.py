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
            # 1. 테이블 생성 (없을 경우)
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
            
            # 2. 마이그레이션: 기존 소문자 컬럼들을 대문자로 변경
            # SQLite 3.25.0+ 부터 RENAME COLUMN 지원
            cols_to_rename = {
                'id': 'ID',
                'title': 'TITLE',
                'url': 'URL',
                'source': 'SOURCE',
                'broker': 'BROKER',
                'sent_yn': 'SENT_YN',
                'created_at': 'CREATED_AT'
            }
            
            # 현재 테이블 정보 가져오기
            cursor = conn.execute("PRAGMA table_info(report_history)")
            current_cols = [row[1] for row in cursor.fetchall()]
            
            for old_col, new_col in cols_to_rename.items():
                if old_col in current_cols and old_col != new_col:
                    try:
                        conn.execute(f"ALTER TABLE report_history RENAME COLUMN {old_col} TO {new_col}")
                        logger.info(f"Column renamed: {old_col} -> {new_col}")
                    except Exception as e:
                        logger.warning(f"Failed to rename {old_col}: {e}")

            # 3. PDF_URL 컬럼 추가 (구버전에서 업그레이드 시)
            if 'PDF_URL' not in current_cols and 'pdf_url' not in current_cols:
                try:
                    conn.execute("ALTER TABLE report_history ADD COLUMN PDF_URL TEXT")
                    logger.info("Column added: PDF_URL")
                except Exception: pass
            
            # 4. SENT_YN 컬럼 추가 (구버전에서 업그레이드 시)
            if 'SENT_YN' not in current_cols and 'sent_yn' not in current_cols:
                try:
                    conn.execute("ALTER TABLE report_history ADD COLUMN SENT_YN TEXT DEFAULT 'N'")
                    logger.info("Column added: SENT_YN")
                except Exception: pass

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
