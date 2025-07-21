import os
import requests
import random
import io
import time
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm
from tools.client import summarize_report
from pymongo import MongoClient
import pdfplumber

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

    def scrape(self, start_page=1, end_page=2):
        with requests.Session() as session:
            for page in range(start_page, end_page + 1):
                base_url = f"https://finance.naver.com/research/company_list.naver?&page={page}"
                req = session.get(base_url, headers=self.base_header)
                soup = BeautifulSoup(req.text, "html.parser")    
                table = soup.find("table", class_="type_1")

                if not table:
                    continue
                rows = table.find_all("tr")

                for row in tqdm(rows):
                    cols = row.find_all("td")

                    if len(cols) < 5:
                        continue
                    securities = cols[2].get_text(strip=True)  # 증권사

                    # 신한투자증권이면 스킵
                    if securities == "신한투자증권":
                        continue

                    report_title = cols[1].get_text(strip=True)  # 리포트 이름
                    company_name = cols[0].get_text(strip=True)  # 회사 이름

                    # pdf 주소는 .pdf로 끝나는 a 태그에서 추출
                    pdf_url = ""
                    pdf_a_tag = row.find("a", href=lambda x: x and x.endswith(".pdf"))

                    if pdf_a_tag and pdf_a_tag.get("href"):
                        pdf_url = pdf_a_tag.get("href")

                        # pdf_url이 절대경로가 아니면, 앞에 https://stock.pstatic.net 붙이기
                        if not pdf_url.startswith("http"):
                            pdf_url = "https://stock.pstatic.net" + pdf_url

                    date = cols[4].get_text(strip=True) if len(cols) > 4 else ""  # 올라온 날짜

                    # 오늘 날짜가 아니면 스킵
                    if date != self.today:
                        continue

                    # pdf_url이 있으면 pdf에서 텍스트 추출
                    report_text = ""
                    if pdf_url:
                        try:
                            pdf_req = session.get(pdf_url)
                            time.sleep(random.uniform(1, 3))
                            pdf_file = io.BytesIO(pdf_req.content)

                            with pdfplumber.open(pdf_file) as pdf:
                                first_page = pdf.pages[0]
                                report_text = first_page.extract_text()
                                
                        except Exception as e:
                            report_text = f"PDF 추출 실패: {e}"

                    summary = ""
                    if report_text and not report_text.startswith("PDF 추출 실패"):
                        summary = summarize_report(report_text)
                    else:
                        summary = "텍스트 없음 또는 PDF 추출 실패"

                    report_dict = {
                        "증권사": securities,
                        "리포트명": report_title,
                        "회사명": company_name,
                        "pdf주소": pdf_url,
                        "날짜": date,
                        "텍스트": report_text,
                        "요약": summary
                    }
                    self.report_list.append(report_dict)

    def get_reports(self):
        return self.report_list

    def save_to_db(self):
        """
        self.report_list의 데이터를 MongoDB에 저장 (upsert 방식, pdf주소 기준)
        """
        if not self.report_list:
            print("저장할 리포트가 없습니다.")
            return

        for report in self.report_list:
            self.report_collection.update_one(
                {"pdf_url": report.get("pdf주소")},
                {"$set": {
                    "securities": report.get("증권사"),
                    "report_title": report.get("리포트명"),
                    "company_name": report.get("회사명"),
                    "pdf_url": report.get("pdf주소"),
                    "date": report.get("날짜"),
                    "text": report.get("텍스트"),
                    "summary": report.get("요약"),
                    "which": "company",
                }},
                upsert=True
            )
        print(f"{len(self.report_list)}건의 리포트가 DB에 저장되었습니다.")
