from typing import List, Dict, Optional
import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()


class OpenFIGIClient:
    def __init__(self, api_key: Optional[str] = None):
        self.base_url = "https://api.openfigi.com"
        self.api_key = api_key
        self.headers = {
            "Content-Type": "application/json"
        }
        if api_key:
            self.headers["X-OPENFIGI-APIKEY"] = api_key
    
    def map_isin_to_ticker(self, isin_list: List[str], batch_size: int = 10) -> Dict[str, str]:
        """ISIN 리스트를 Ticker로 직접 매핑합니다."""
        isin_to_ticker = {}
        
        # API 키가 없으면 batch_size를 5로 제한
        if not self.api_key and batch_size > 5:
            batch_size = 5
            print("API 키가 없어서 batch_size를 5로 제한합니다.")
        
        total_batches = (len(isin_list) + batch_size - 1) // batch_size
        print(f"총 {len(isin_list)}개 ISIN을 {total_batches}개 배치로 처리합니다.")
        
        # ISIN을 배치로 나누어 처리
        for batch_idx in range(0, len(isin_list), batch_size):
            batch_isins = isin_list[batch_idx:batch_idx + batch_size]
            current_batch = (batch_idx // batch_size) + 1
                        
            # API 요청 데이터 준비
            request_data = [
                {"idType": "ID_ISIN", "idValue": isin} 
                for isin in batch_isins
            ]
            
            try:
                response = requests.post(
                    f"{self.base_url}/v3/mapping",
                    headers=self.headers,
                    json=request_data,
                    timeout=30
                )
                
                if response.status_code == 200:
                    results = response.json()
                    
                    batch_success = 0
                    for j, result in enumerate(results):
                        isin = batch_isins[j]
                        
                        if "data" in result and result["data"]:
                            # 첫 번째 결과에서 Ticker 추출
                            figi_data = result["data"][0]
                            ticker = figi_data.get("ticker", "")
                            
                            if ticker:
                                isin_to_ticker[isin] = ticker
                                batch_success += 1
                                # 너무 많은 출력을 방지하기 위해 가끔만 출력
                                if len(isin_to_ticker) % 100 == 0:
                                    print(f"  매핑 성공 누적: {len(isin_to_ticker)}개")
                        elif "warning" in result:
                            print(f"  매핑 실패: {isin} - {result['warning']}")
                        elif "error" in result:
                            print(f"  API 오류: {isin} - {result['error']}")
                                    
                elif response.status_code == 429:
                    print("Rate limit 도달. 잠시 대기 후 재시도...")
                    time.sleep(60)  # 1분 대기
                    # 현재 배치를 다시 시도
                    batch_idx -= batch_size
                    continue
                else:
                    print(f"API 요청 실패: {response.status_code} - {response.text}")
                
                # Rate limit 준수를 위한 대기
                if not self.api_key:
                    time.sleep(2.5)  # API 키 없으면 좀 더 여유롭게
                else:
                    time.sleep(0.3)
                
            except Exception as e:
                print(f"요청 중 오류 발생: {e}")
                time.sleep(5)
        
        return isin_to_ticker