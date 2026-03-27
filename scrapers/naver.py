# -*- coding:utf-8 -*- 
import os
import asyncio
import aiohttp
import html
from bs4 import BeautifulSoup
from loguru import logger
from dotenv import load_dotenv

from models.database import DatabaseManager
from utils.telegram_util import sendMarkDownText

load_dotenv()

# 환경 변수
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET')
CHANNEL_ID = os.getenv('TELEGRAM_CHANNEL_ID_NAVER_REPORT_ALARM')

class NaverReportScraper:
    def __init__(self, db: DatabaseManager, is_dev: bool = False):
        self.db = db
        self.api_url = 'https://m.stock.naver.com/front-api/research/list'
        self.is_dev = is_dev
        self.prefix = "<b>[DEV]</b> " if is_dev else ""
        logger.info(f"NaverReportScraper 초기화됨 (is_dev: {is_dev})")

    async def _parse_pdf_url(self, session, end_url):
        """네이버 리서치 상세 페이지에서 '원문 보기' (PDF) 링크 추출"""
        if not end_url:
            return ""
        try:
            async with session.get(end_url, timeout=10) as response:
                if response.status != 200:
                    return ""
                text = await response.text()
                soup = BeautifulSoup(text, "html.parser")
                
                # '원문 보기' 텍스트를 포함하는 a 태그 찾기
                a_tag = soup.find("a", string=lambda t: t and "원문 보기" in t)
                if not a_tag:
                    # 텍스트가 정확히 일치하지 않을 경우 대비 (공백 등)
                    for tag in soup.find_all("a"):
                        if "원문 보기" in tag.get_text(strip=True):
                            a_tag = tag
                            break
                
                if a_tag and "href" in a_tag.attrs:
                    return f"https://m.stock.naver.com{a_tag['href']}"
        except Exception as e:
            logger.warning(f"PDF 링크 파싱 실패: {end_url} - {e}")
        return ""

    async def fetch_historical_data(self):
        """과거 데이터 전체 수집 (발송 방지를 위해 sent_yn='Y'로 저장)"""
        logger.info("과거 데이터 전체 수집 시작 (전체 이력 적재)...")
        
        async with aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0'}) as session:
            for category in ['company', 'industry']:
                page = 1
                logger.info(f"카테고리 [{category}] 수집 중...")
                
                while True:
                    url = f"{self.api_url}?category={category}&pageSize=100&page={page}"
                    try:
                        async with session.get(url) as response:
                            if response.status != 200:
                                break
                            
                            data = await response.json()
                            results = data.get('result', [])
                            if not results:
                                break
                            
                            new_count = 0
                            for item in results:
                                title = item['title']
                                if category == 'company':
                                    # 종목명: 제목 형식
                                    item_name = item.get('itemName', '미분류')
                                    if f"{item_name}:" not in title:
                                        title = f"{item_name}: {title}"
                                else:
                                    # 카테고리: 제목 형식
                                    cat_name = item.get('category', '미분류')
                                    if f"{cat_name}:" not in title:
                                        title = f"{cat_name}: {title}"
                                
                                # 과거 데이터는 발송 안 함 (sent_yn='Y')
                                if self.db.insert_report(
                                    title=title, 
                                    url=item['endUrl'], 
                                    source="NAVER", 
                                    broker=item['brokerName'],
                                    sent_yn='Y'
                                ):
                                    new_count += 1
                            
                            logger.info(f"[{category}] {page}페이지: {new_count}건 신규 저장됨")
                            if new_count == 0 and page > 1: # 이미 데이터가 충분히 쌓인 지점
                                break
                                
                            page += 1
                            await asyncio.sleep(0.05)
                    except Exception as e:
                        logger.error(f"과거 수집 오류: {e}")
                        break
        logger.info("과거 데이터 수집 완료.")

    async def run(self):
        """최신 데이터 수집 및 텔레그램 발송"""
        logger.info("실시간 데이터 체크 시작...")
        
        async with aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0'}) as session:
            # 1. 최신 글 20개씩 가져와서 DB에 저장 (신규는 sent_yn='N')
            for category in ['company', 'industry']:
                url = f"{self.api_url}?category={category}&pageSize=20&page=1"
                try:
                    async with session.get(url) as response:
                        if response.status == 200:
                            results = (await response.json()).get('result', [])
                            for item in results:
                                title = item['title']
                                if category == 'company':
                                    item_name = item.get('itemName', '미분류')
                                    if f"{item_name}:" not in title:
                                        title = f"{item_name}: {title}"
                                else:
                                    cat_name = item.get('category', '미분류')
                                    if f"{cat_name}:" not in title:
                                        title = f"{cat_name}: {title}"
                                
                                if self.db.insert_report(
                                    title=title, 
                                    url=item['endUrl'], 
                                    source="NAVER", 
                                    broker=item['brokerName'],
                                    sent_yn='N'
                                ):
                                    logger.info(f"신규 레포트 발견: {title}")
                except Exception as e:
                    logger.error(f"목록 수집 오류: {e}")

            # 2. 미발송 내역(sent_yn='N')을 긁어서 발송
            unsent = self.db.get_unsent_reports()
            if not unsent:
                logger.info("발송할 신규 레포트가 없습니다.")
                return

            logger.info(f"미발송 레포트 {len(unsent)}건 발송 시작...")
            
            # 발송 메시지 구성
            send_buffer = ""
            sent_ids = []
            last_broker = None
            
            for report in unsent:
                # 1. DB에 이미 PDF 링크가 있는지 확인
                display_url = report.get('attach_url')
                
                # 2. 없다면 실제 상세 페이지 방문하여 파싱 후 DB 업데이트
                if not display_url:
                    display_url = await self._parse_pdf_url(session, report['url'])
                    if display_url:
                        self.db.update_report_attach_url(report['id'], display_url)
                        logger.info(f"PDF URL 업데이트됨: {report['id']} -> {display_url}")
                    else:
                        display_url = report['url'] # PDF 추출 실패 시 원문 상세 페이지 유지
                
                # HTML 이스케이프 및 메시지 조립
                esc_title = html.escape(report['title'])
                
                content = ""
                if last_broker != report['broker']:
                    content += f"●<b>{html.escape(report['broker'])}</b>\n"
                    last_broker = report['broker']
                
                content += f"{esc_title}\n👉<a href='{display_url}'>링크</a>\n\n"
                
                if len(send_buffer) + len(content) > 3500:
                    # 메시지 길이 초과 시 분할 발송
                    await sendMarkDownText(
                        TELEGRAM_BOT_TOKEN, 
                        CHANNEL_ID, 
                        f"{self.prefix}●<b>네이버 증권 리서치</b>\n\n{send_buffer}", 
                        "HTML"
                    )
                    send_buffer = ""
                    last_broker = None  # 새 메시지 시작 시 증권사 명칭 다시 표시

                send_buffer += content
                sent_ids.append(report['id'])

            # 남은 메시지 발송
            if send_buffer:
                await sendMarkDownText(
                    TELEGRAM_BOT_TOKEN, 
                    CHANNEL_ID, 
                    f"{self.prefix}●<b>네이버 증권 리서치</b>\n\n{send_buffer}", 
                    "HTML"
                )
            
            # 발송 상태 업데이트
            self.db.update_sent_status(sent_ids)
            logger.info(f"발송 완료 및 상태 업데이트 완료 ({len(sent_ids)}건)")
