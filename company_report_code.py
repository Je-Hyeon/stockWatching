# 기업리포트 수집 및 출력 코드 (Jupyter 노트북용)

import re
from tools.naver import NaverReportScraper

# NaverReportScraper 인스턴스 생성
scraper = NaverReportScraper()

# 기업리포트만 수집 (1-2페이지)
scraper.scrape_company(start_page=1, end_page=2)
report_list = scraper.get_reports()
scraper.save_to_db()

# AI 요약 결과 출력
print("AI가 요약한 결과입니다. 정보의 정확성은 보장하지 않습니다.")
print()
print("================================================")

# 회사명 기준으로 report_list 정렬
sorted_report_list = sorted(report_list, key=lambda x: x["회사명"], reverse=True)

for report_dict in sorted_report_list:
    print("## ", report_dict["리포트명"], "|", report_dict["회사명"], "|", report_dict["증권사"], "|", report_dict["날짜"], " ##")
    print()
    # 요약을 문장 단위로 쪼개서 '. ' 뒤에 엔터를 넣어 출력
    summary = report_dict["요약"]
    # '. '로 끝나는 문장 뒤에만 줄바꿈 추가 (예: 7.9만원 등은 줄바꿈하지 않음)
    sentences = re.split(r'(?<=\.\s)', summary)
    for sentence in sentences:
        if sentence.strip():  # 빈 문장은 출력하지 않음
            print("◼︎ ", sentence.strip())
    print("================================================")
    print() 