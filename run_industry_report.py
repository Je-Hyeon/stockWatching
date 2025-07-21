from tools.naver import NaverReportScraper

def main():
    # NaverReportScraper 인스턴스 생성
    scraper = NaverReportScraper()
    
    print("산업리포트 수집을 시작합니다...")
    
    # 산업리포트만 수집
    scraper.scrape_industry()
    
    # 수집된 리포트 확인
    reports = scraper.get_reports()
    print(f"수집된 산업리포트 수: {len(reports)}")
    
    # 수집된 리포트 정보 출력
    for i, report in enumerate(reports, 1):
        print(f"\n=== 리포트 {i} ===")
        print(f"증권사: {report.get('증권사')}")
        print(f"리포트명: {report.get('리포트명')}")
        print(f"산업명: {report.get('산업명')}")
        print(f"날짜: {report.get('날짜')}")
        print(f"PDF 주소: {report.get('pdf주소')}")
        print(f"요약: {report.get('요약')[:100]}...")  # 요약의 처음 100자만 출력
    
    # MongoDB에 저장
    if reports:
        scraper.save_to_db()
        print("\nMongoDB에 저장 완료!")
    else:
        print("\n저장할 리포트가 없습니다.")

if __name__ == "__main__":
    main() 