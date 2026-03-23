# -*- coding:utf-8 -*- 
import os
import asyncio
import requests
import html
from bs4 import BeautifulSoup
from loguru import logger
from dotenv import load_dotenv

from models.database import DatabaseManager
from utils.telegram_util import sendMarkDownText

load_dotenv()

# 환경 변수
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET')
CHANNEL_ID = os.getenv('TELEGRAM_CHANNEL_ID_REPORT_ALARM')

EMOJI_PICK = "👉"

class NaverReportScraper:
    def __init__(self, db: DatabaseManager, is_dev: bool = False):
        self.db = db
        self.target_url = 'https://finance.naver.com/research/company_list.naver'
        self.prefix = "<b>[DEV]</b> " if is_dev else ""
        logger.info(f"NaverReportScraper initialized with prefix: '{self.prefix}'")

    def escape_html(self, text):
        return html.escape(text) if text else ""

    async def _send_batch_message(self, header, body):
        if not body: return
        full_message = f"{self.prefix}{header}\n{body}"
        await sendMarkDownText(token=TELEGRAM_BOT_TOKEN, chat_id=CHANNEL_ID, sendMessageText=full_message, parse_mode="HTML")

    async def run(self):
        source = "NAVER"
        header = "● 네이버 리서치"
        try:
            logger.info(f"Fetching Naver Research: {self.target_url}")
            res = requests.get(self.target_url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
            })
            soup = BeautifulSoup(res.text, "html.parser")
            rows = soup.select('div#contentarea_left > table.type_1 > tr')
            send_buffer = ""
            for row in rows:
                if not row.select_one('td.file'): continue
                try:
                    title_raw = row.select_one('td:nth-child(1) > a').text.strip()
                    link = row.select_one('td:nth-child(1) > a').attrs['href']
                    if not link.startswith('http'):
                        link = 'https://finance.naver.com/research/' + link
                    broker = row.select_one('td:nth-child(2)').text.strip()
                    title = self.escape_html(title_raw)
                    if self.db.insert_report(title=title, url=link, source=source, broker=broker):
                        logger.info(f"New {source} Report: {title} ({broker})")
                        send_buffer += f"<b>{title}</b> ({broker})\n{EMOJI_PICK} <a href='{link}'>링크</a>\n\n"
                        if len(send_buffer) >= 3000:
                            await self._send_batch_message(header, send_buffer)
                            send_buffer = ""
                except Exception:
                    continue
            if send_buffer:
                await self._send_batch_message(header, send_buffer)
        except Exception as e:
            logger.error(f"Error fetching Naver: {e}")
