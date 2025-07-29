import os
import requests
import pandas as pd 
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
import datetime
from tqdm import tqdm

class KofiaClient:
    """KOFIA 증시자금 데이터 수집 및 업데이트 클래스"""
    
    def __init__(self):
        """초기화: 환경변수 로드 및 MongoDB 연결"""
        load_dotenv()
        self.mongo_client = MongoClient(os.getenv("MONGODB_URI"))
        self.db = self.mongo_client['quant']
        self.collection = self.db["kofia_funds_daily"]
        
        self.url = "https://freesis.kofia.or.kr/meta/getMetaDataList.do"
        self.headers = {
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "connection": "keep-alive",
            "content-length": "129",
            "content-type": "application/json; charset=UTF-8",
            "cookie": "",
            "host": "freesis.kofia.or.kr",
            "origin": "https://freesis.kofia.or.kr",
            "referer": "https://freesis.kofia.or.kr/stat/FreeSIS.do?parentDivId=MSIS10000000000000&serviceId=STATSCU0100000140",
            "sec-ch-ua": "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Google Chrome\";v=\"138\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": os.getenv("USER_AGENT"),
            "x-requested-with": "XMLHttpRequest"
        }
        
        self.column_mapping = {
            "TMPV1": "DATE",
            "TMPV2": "투자자예탁금",
            "TMPV3": "장내파생상품 거래 예수금",
            "TMPV4": "RP 매도잔고",
            "TMPV5": "위탁매매 미수금",
            "TMPV6": "위탁매매 미수금 대비 실제 반대매매금액",
            "TMPV7": "미수금 대비 반대매매비중",
        }
    
    def get_latest_date(self):
        """DB에서 가장 최근 날짜를 조회"""
        latest_record = self.collection.find_one(
            {}, 
            sort=[("DATE", -1)]
        )
        
        if latest_record:
            latest_date = latest_record["DATE"]
            if isinstance(latest_date, str):
                latest_date = pd.to_datetime(latest_date).date()
            elif hasattr(latest_date, 'date'):
                latest_date = latest_date.date()
            return latest_date
        return None
    
    def fetch_data(self, start_date, end_date):
        """지정된 기간의 KOFIA 데이터를 수집"""
        # KOFIA API는 대용량 데이터 요청 시 제한이 있을 수 있으므로 1년 단위로 쪼개서 요청
        delta = datetime.timedelta(days=365)
        date_ranges = []
        cur_start = start_date
        
        while cur_start <= end_date:
            cur_end = min(cur_start + delta, end_date)
            date_ranges.append((cur_start, cur_end))
            cur_start = cur_end + datetime.timedelta(days=1)

        all_data = []

        for s, e in tqdm(date_ranges, desc="KOFIA 데이터 수집"):
            payload = {
                "dmSearch": {
                    "tmpV40": "1",
                    "tmpV41": "1",
                    "tmpV1": "D",
                    "tmpV45": s.strftime("%Y%m%d"),
                    "tmpV46": e.strftime("%Y%m%d"),
                    "OBJ_NM": "STATSCU0100000060BO"
                }
            }
            
            try:
                response = requests.post(self.url, headers=self.headers, json=payload)
                response.raise_for_status()  # HTTP 에러 체크
                
                result = response.json()
                if "ds1" in result and result["ds1"]:
                    df = pd.DataFrame(result["ds1"]).rename(columns=self.column_mapping)
                    all_data.append(df)
                    
            except Exception as e:
                print(f"데이터 수집 중 오류 발생 ({s} ~ {e}): {str(e)}")
                continue

        if all_data:
            data = pd.concat(all_data, ignore_index=True)
            data["DATE"] = pd.to_datetime(data["DATE"])
            data.sort_values("DATE", inplace=True)
            data.reset_index(drop=True, inplace=True)
            
            # 중복 제거 (같은 날짜가 여러 번 들어올 수 있음)
            data = data.drop_duplicates(subset=['DATE'], keep='last')
            
            return data
        
        return pd.DataFrame()
    
    def save_data(self, data):
        """데이터를 MongoDB에 저장 (bulk upsert 사용)"""
        if data.empty:
            print("저장할 데이터가 없습니다.")
            return 0, 0
        
        # bulk upsert를 위한 UpdateOne 리스트 생성
        operations = []
        for _, row in data.iterrows():
            doc = row.to_dict()
            operations.append(
                UpdateOne(
                    {"DATE": doc["DATE"]},
                    {"$set": doc},
                    upsert=True
                )
            )
        
        if operations:
            result = self.collection.bulk_write(operations, ordered=False)
            return result.upserted_count, result.modified_count
        
        return 0, 0
    
    def update_data(self):
        """최신 데이터 업데이트 (DB에 없는 날짜만 수집)"""
        print("=== KOFIA 증시자금 데이터 업데이트 시작 ===")
        
        # DB에서 가장 최근 날짜 조회
        latest_date = self.get_latest_date()
        
        if latest_date:
            # 다음날부터 데이터 수집 시작
            start_date = latest_date + datetime.timedelta(days=1)
            print(f"DB 최근 날짜: {latest_date}, 수집 시작 날짜: {start_date}")
        else:
            # DB가 비어있으면 2007년부터 시작 (KOFIA 데이터 시작일)
            start_date = datetime.date(2007, 1, 1)
            print("DB가 비어있습니다. 2007년부터 데이터 수집을 시작합니다.")

        end_date = datetime.date.today()

        # 수집할 데이터가 없으면 종료
        if start_date > end_date:
            print("이미 최신 데이터입니다. 업데이트할 데이터가 없습니다.")
            return
        
        print(f"데이터 수집 기간: {start_date} ~ {end_date}")
        
        # 데이터 수집
        data = self.fetch_data(start_date, end_date)
        
        if not data.empty:
            # 데이터 저장
            inserted_count, updated_count = self.save_data(data)
            print(f"데이터 저장 완료: 새로 추가 {inserted_count}건, 업데이트 {updated_count}건")
        else:
            print("API에서 데이터를 받아오지 못했습니다.")
        
        print("=== KOFIA 증시자금 데이터 업데이트 완료 ===")
    
    def get_data(self, start_date=None, end_date=None, limit=None):
        """DB에서 데이터 조회"""
        query = {}
        if start_date:
            query["DATE"] = {"$gte": pd.to_datetime(start_date)}
        if start_date and end_date:
            query["DATE"] = {"$gte": pd.to_datetime(start_date), "$lte": pd.to_datetime(end_date)}
        elif end_date:
            query["DATE"] = {"$lte": pd.to_datetime(end_date)}
        
        cursor = self.collection.find(query).sort("DATE", 1)
        if limit:
            cursor = cursor.limit(limit)
        
        data = list(cursor)
        if data:
            df = pd.DataFrame(data)
            # MongoDB의 _id 컬럼 제거
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
            return df
        return pd.DataFrame()
    
    def close(self):
        """MongoDB 연결 종료"""
        if self.mongo_client:
            self.mongo_client.close()
    
    def __enter__(self):
        """컨텍스트 매니저 진입"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """컨텍스트 매니저 종료"""
        self.close() 