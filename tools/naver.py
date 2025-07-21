import os
import requests
import random
import io
import time
import warnings
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm
from tools.client import summarize_report
from pymongo import MongoClient
import pdfplumber
import re

class NaverReportScraper:
    def __init__(self):
        self.base_header = {
            "authority": "finance.naver.com",
            "method": "GET",
            "path": "/research/company_list.naver?&page=1",
            "scheme": "https",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "cache-control": "no-cache",
            "cookie": "",
            "pragma": "no-cache",
            "priority": "u=0, i",
            "referer": "https://finance.naver.com/research/company_list.naver?&page=2",
            "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "Windows",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": os.getenv("USER_AGENT")
        }
        self.report_list = []
        self.today = datetime.now().strftime("%y.%m.%d")

        # MongoDB 연결 초기화
        uri = os.getenv("MONGODB_URI")
        self.client = MongoClient(uri)
        self.db = self.client['quant']
        self.report_collection = self.db['naver_reports']

    @staticmethod
    def _is_text_page(text):
        """
        페이지 내 텍스트에서 숫자 비율이 50% 이상이면 False 반환 (즉, 차트/표 위주 페이지로 간주)
        """
        if not text:
            return False
        text = text.strip()
        if not text:
            return False
        total_chars = len(text)
        if total_chars == 0:
            return False
        num_chars = sum(c.isdigit() for c in text)
        return (num_chars / total_chars) < 0.5

    @staticmethod
    def _extract_pdf_text(pdf_file, skip_last_page=False):
        """
        pdfplumber PDF 객체에서 모든 페이지의 텍스트를 추출하되,
        각 페이지에서 숫자 비율이 50% 이상인 경우 해당 페이지는 스킵
        skip_last_page=True면 마지막 페이지는 무시
        """
        text_list = []
        with pdfplumber.open(pdf_file) as pdf:
            pages = pdf.pages[:-1] if skip_last_page and len(pdf.pages) > 0 else pdf.pages
            for page in pages:
                page_text = page.extract_text()
                if page_text and NaverReportScraper._is_text_page(page_text):
                    text_list.append(page_text)
        return "\n".join(text_list)

    def _parse_table_rows(self, rows, report_type):
        """
        테이블 row들을 파싱하여 report_list에 추가
        report_type: "company" 또는 "industry"
        """
        for row in tqdm(rows, desc=f"{'기업리포트' if report_type == 'company' else '산업리포트'}"):
            cols = row.find_all("td")
            if len(cols) < 5:
                continue
            securities = cols[2].get_text(strip=True)
            if securities == "신한투자증권":
                continue

            report_title = cols[1].get_text(strip=True)
            name = cols[0].get_text(strip=True)
            pdf_url = ""
            pdf_a_tag = row.find("a", href=lambda x: x and x.endswith(".pdf"))
            if pdf_a_tag and pdf_a_tag.get("href"):
                pdf_url = pdf_a_tag.get("href")
                if not pdf_url.startswith("http"):
                    pdf_url = "https://stock.pstatic.net" + pdf_url

            date = cols[4].get_text(strip=True) if len(cols) > 4 else ""
            if date != self.today:
                continue

            report_text = ""
            if pdf_url:
                try:
                    pdf_req = self._session.get(pdf_url)
                    time.sleep(random.uniform(1, 2))  # sleep 시간 단축
                    pdf_file = io.BytesIO(pdf_req.content)
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        if report_type == "company":
                            report_text = self._extract_pdf_text(pdf_file, skip_last_page=False)
                        else:
                            report_text = self._extract_pdf_text(pdf_file, skip_last_page=True)
                    if not report_text.strip():
                        report_text = "텍스트 없음 또는 PDF 추출 실패"
                except Exception as e:
                    report_text = f"PDF 추출 실패: {e}"

            if report_text and not report_text.startswith("PDF 추출 실패"):
                try:
                    summary = summarize_report(report_text, report_type)
                except Exception as e:
                    summary = f"요약 실패: {e}"
            else:
                summary = "텍스트 없음 또는 PDF 추출 실패"

            report_dict = {
                "종류": report_type,
                "증권사": securities,
                "리포트명": report_title,
                "pdf주소": pdf_url,
                "날짜": date,
                "텍스트": report_text,
                "요약": summary
            }
            if report_type == "company":
                report_dict["회사명"] = name
            else:
                report_dict["산업명"] = name
            self.report_list.append(report_dict)

    def scrape_company(self, start_page=1, end_page=2):
        """
        기업 리포트만 수집
        """
        self.report_list = []
        with requests.Session() as session:
            self._session = session  # 내부에서 재사용
            for page in range(start_page, end_page + 1):
                base_url = f"https://finance.naver.com/research/company_list.naver?&page={page}"
                try:
                    req = session.get(base_url, headers=self.base_header, timeout=10)
                except Exception as e:
                    print(f"페이지 {page} 요청 실패: {e}")
                    continue
                soup = BeautifulSoup(req.text, "html.parser")
                table = soup.find("table", class_="type_1")
                if not table:
                    continue
                rows = table.find_all("tr")
                self._parse_table_rows(rows, "company")
            del self._session

    def scrape_industry(self):
        """
        산업 리포트만 수집
        """
        self.report_list = []
        with requests.Session() as session:
            self._session = session
            industry_url = "https://finance.naver.com/research/industry_list.naver"
            try:
                req = session.get(industry_url, headers=self.base_header, timeout=10)
            except Exception as e:
                print(f"산업리포트 요청 실패: {e}")
                return
            soup = BeautifulSoup(req.text, "html.parser")
            table = soup.find("table", class_="type_1")
            if not table:
                return
            rows = table.find_all("tr")
            self._parse_table_rows(rows, "industry")
            del self._session

    def get_reports(self):
        return self.report_list

    def save_to_db(self):
        """
        self.report_list의 데이터를 MongoDB에 저장 (upsert 방식, pdf주소 기준)
        """
        if not self.report_list:
            print("저장할 리포트가 없습니다.")
            return

        bulk_ops = []
        for report in self.report_list:
            which = report.get("종류", "company")
            update_dict = {
                "securities": report.get("증권사"),
                "report_title": report.get("리포트명"),
                "pdf_url": report.get("pdf주소"),
                "date": report.get("날짜"),
                "text": report.get("텍스트"),
                "summary": report.get("요약"),
                "which": which,
            }
            if which == "company":
                update_dict["company_name"] = report.get("회사명")
            elif which == "industry":
                update_dict["industry_name"] = report.get("산업명")

            bulk_ops.append(
                {
                    "filter": {"pdf_url": report.get("pdf주소")},
                    "update": {"$set": update_dict},
                    "upsert": True
                }
            )
        # bulk_write로 최적화
        from pymongo import UpdateOne
        requests_bulk = [
            UpdateOne(op["filter"], op["update"], upsert=op["upsert"])
            for op in bulk_ops
        ]
        if requests_bulk:
            self.report_collection.bulk_write(requests_bulk)
            print(f"{len(self.report_list)}건의 리포트가 DB에 저장되었습니다.")
        else:
            print("저장할 리포트가 없습니다.")
