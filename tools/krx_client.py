import os
import requests
import time
import pandas as pd 
from dotenv import load_dotenv
from pymongo import MongoClient
import datetime
from tqdm import tqdm
import numpy as np

class KrxClient:
    """KRX 코스피 인덱스 및 ETF 데이터 수집 및 업데이트 클래스"""
    
    def __init__(self):
        """초기화: 환경변수 로드 및 MongoDB 연결"""
        load_dotenv()
        self.mongo_client = MongoClient(os.getenv("MONGODB_URI"))
        self.db = self.mongo_client['quant']
        self.collection = self.db["krx_index_daily"]
        self.etf_collection = self.db["krx_etf"]  # ETF 데이터용 컬렉션 추가
        
        self.url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        self.headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-encoding": "gzip, deflate",
            "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "connection": "keep-alive",
            "content-length": "283",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "cookie": "",
            "host": "data.krx.co.kr",
            "origin": "http://data.krx.co.kr",
            "referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010105",
            "user-agent": os.getenv("USER_AGENT"),
            "x-requested-with": "XMLHttpRequest"
        }
        
        self.selected_columns = ['TRD_DD', 'CLSPRC_IDX', 'OPNPRC_IDX', 'HGPRC_IDX', 'LWPRC_IDX','ACC_TRDVAL', 'MKTCAP']
        self.column_mapping = {
            "TRD_DD": "date",
            "CLSPRC_IDX": "close",
            "OPNPRC_IDX": "open",
            "HGPRC_IDX": "high",
            "LWPRC_IDX": "low",
            "ACC_TRDVAL": "tvolWon",
            "MKTCAP": "mktcapWon"
        }
    
    def get_latest_date(self):
        """DB에서 가장 최근 날짜를 조회"""
        latest_record = self.collection.find_one(
            {}, 
            sort=[("date", -1)]
        )
        
        if latest_record:
            latest_date = latest_record["date"]
            if isinstance(latest_date, str):
                latest_date = pd.to_datetime(latest_date).date()
            elif hasattr(latest_date, 'date'):
                latest_date = latest_date.date()
            return latest_date
        return None
    
    def fetch_data(self, start_date, end_date):
        """지정된 기간의 KRX 데이터를 수집"""
        # 수집 기간이 1년 이상이면 1년 단위로 쪼개서 요청
        delta = datetime.timedelta(days=365)
        date_ranges = []
        cur_start = start_date
        
        while cur_start <= end_date:
            cur_end = min(cur_start + delta, end_date)
            date_ranges.append((cur_start, cur_end))
            cur_start = cur_end + datetime.timedelta(days=1)

        all_data = []

        for s, e in tqdm(date_ranges, desc="KRX 데이터 수집"):
            payload = {
                "bld": "dbms/MDC/STAT/standard/MDCSTAT00301",
                "locale": "ko_KR",
                "tboxindIdx_finder_equidx0_2": "코스피",
                "indIdx": "1",
                "indIdx2": "001",
                "codeNmindIdx_finder_equidx0_2": "코스피",
                "param1indIdx_finder_equidx0_2": "",
                "strtDd": s.strftime("%Y%m%d"),
                "endDd": e.strftime("%Y%m%d"),
                "share": "2",
                "money": "1",
                "csvxls_isNo": "false"
            }
            
            try:
                req = requests.post(self.url, headers=self.headers, data=payload)
                output = req.json().get("output", [])
                if output:
                    df = pd.DataFrame(output)[self.selected_columns].rename(
                        columns=self.column_mapping
                    )
                    all_data.append(df)
                time.sleep(0.2)
            except Exception as e:
                print(f"데이터 수집 중 오류 발생 ({s} ~ {e}): {str(e)}")
                continue

        if all_data:
            data = pd.concat(all_data, ignore_index=True)
            data["date"] = pd.to_datetime(data["date"])
            data.sort_values("date", inplace=True)
            
            # 나머지 컬럼(숫자형)에서 쉼표 제거 후 float로 변환
            float_columns = ['close', 'open', 'high', 'low', 'tvolWon', 'mktcapWon']
            for col in float_columns:
                data[col] = data[col].str.replace(',', '', regex=False).astype(float)
            data.reset_index(drop=True, inplace=True)
            return data
        
        return pd.DataFrame()
    
    def save_data(self, data):
        """데이터를 MongoDB에 저장 (중복 방지를 위해 upsert 사용)"""
        if data.empty:
            print("저장할 데이터가 없습니다.")
            return 0, 0
        
        inserted_count = 0
        updated_count = 0
        
        for _, row in data.iterrows():
            result = self.collection.replace_one(
                {"date": row["date"]},
                row.to_dict(),
                upsert=True
            )
            if result.upserted_id:
                inserted_count += 1
            elif result.modified_count > 0:
                updated_count += 1
        
        return inserted_count, updated_count
    
    def update_data(self):
        """최신 데이터 업데이트 (DB에 없는 날짜만 수집)"""
        print("=== KRX 코스피 데이터 업데이트 시작 ===")
        
        # DB에서 가장 최근 날짜 조회
        latest_date = self.get_latest_date()
        
        if latest_date:
            # 다음날부터 데이터 수집 시작
            start_date = latest_date + datetime.timedelta(days=1)
            print(f"DB 최근 날짜: {latest_date}, 수집 시작 날짜: {start_date}")
        else:
            # DB가 비어있으면 2020년부터 시작
            start_date = datetime.date(2020, 1, 1)
            print("DB가 비어있습니다. 2020년부터 데이터 수집을 시작합니다.")

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
        
        print("=== KRX 코스피 데이터 업데이트 완료 ===")
    
    def get_data(self, start_date=None, end_date=None, limit=None):
        """DB에서 데이터 조회"""
        query = {}
        if start_date:
            query["date"] = {"$gte": pd.to_datetime(start_date)}
        if start_date and end_date:
            query["date"] = {"$gte": pd.to_datetime(start_date), "$lte": pd.to_datetime(end_date)}
        elif end_date:
            query["date"] = {"$lte": pd.to_datetime(end_date)}
        
        cursor = self.collection.find(query).sort("date", 1)
        if limit:
            cursor = cursor.limit(limit)
        
        data = list(cursor)
        if data:
            return pd.DataFrame(data)
        return pd.DataFrame()
    
    # ===== ETF 관련 메소드들 =====
    
    def _get_etf_list(self, trade_date=None):
        """ETF 목록을 가져옵니다."""
        if trade_date is None:
            trade_date = datetime.datetime.today().strftime("%Y%m%d")
        
        data = {
            "bld": "dbms/MDC/STAT/standard/MDCSTAT04301",
            "locale": "ko_KR",  
            "trdDd": trade_date,
            "share": "1",
            "money": "1",
            "csvxls_isNo": "false"
        }
        
        req = requests.post(self.url, headers=self.headers, data=data)
        data = req.json()['output']
        etf_df = pd.DataFrame(data)[["ISU_ABBRV", "IDX_IND_NM", "ISU_SRT_CD", "ISU_CD", "MKTCAP"]]
        etf_df["MKTCAP"] = etf_df["MKTCAP"].str.replace(",", "").astype(float)
        
        # 시가총액 기준 상위 600개 선택
        etf_df = etf_df.loc[etf_df["MKTCAP"].nlargest(600).index]
        
        # 금리, 크레딧, MMF, 머니마켓, 채권, 회사채 관련 ETF 제외
        mask = (
            etf_df["ISU_ABBRV"].str.contains("금리") |
            etf_df["ISU_ABBRV"].str.contains("크레딧") |
            etf_df["ISU_ABBRV"].str.contains("MMF") |
            etf_df["ISU_ABBRV"].str.contains("머니마켓") |
            etf_df["ISU_ABBRV"].str.contains("채권") |
            etf_df["ISU_ABBRV"].str.contains("회사채")
        )
        etf_df = etf_df[~mask]
        
        return etf_df
    
    def get_etf_latest_date(self):
        """ETF DB에서 가장 최신 날짜를 가져옵니다."""
        try:
            latest_doc = self.etf_collection.find().sort("TRD_DD", -1).limit(1)
            latest_doc = list(latest_doc)
            if latest_doc:
                return latest_doc[0]["TRD_DD"]
            return None
        except Exception as e:
            print(f"ETF DB에서 최신 날짜 조회 중 오류: {e}")
            return None
    
    def _fetch_etf_ohlcv(self, etf_df, start_date, end_date):
        """ETF OHLCV 데이터를 가져와서 MongoDB에 저장합니다."""
        data_dict = {
            "bld": "dbms/MDC/STAT/standard/MDCSTAT04501",
            "locale": "ko_KR",
            "param1isuCd_finder_secuprodisu1_1": "",
            "strtDd": start_date,
            "endDd": end_date,
            "share": "1",
            "money": "1",
            "csvxls_isNo": "false"
        }
        
        for idx, row in tqdm(etf_df.iterrows(), total=len(etf_df), desc="ETF 데이터 수집 중"):
            data_dict["isuCd"] = row["ISU_CD"]
            
            try:
                req = requests.post(self.url, data=data_dict, headers=self.headers)
                df = pd.DataFrame(req.json()['output'])
                
                if df.empty:
                    continue
                
                # 날짜 컬럼 처리
                if 'TRD_DD' in df.columns:
                    df['TRD_DD'] = pd.to_datetime(df['TRD_DD'], errors='coerce')
                
                # 숫자 컬럼 처리
                for col in df.columns:
                    if df[col].dtype == object and col != 'TRD_DD':
                        try:
                            df[col] = df[col].str.replace(',', '', regex=False)
                            df[col] = pd.to_numeric(df[col], errors='ignore')
                        except Exception:
                            pass
                
                # 종목 정보 추가
                df['ISU_CD'] = row['ISU_CD']
                df['ISU_ABBRV'] = row['ISU_ABBRV']
                
                # MongoDB에 저장 (중복 체크)
                insert_records = []
                for record in df.to_dict(orient='records'):
                    trd_dd = record.get('TRD_DD')
                    if pd.isnull(trd_dd):
                        continue
                    
                    if isinstance(trd_dd, pd.Timestamp):
                        trd_dd_mongo = trd_dd.to_pydatetime()
                    else:
                        try:
                            trd_dd_mongo = pd.to_datetime(trd_dd).to_pydatetime()
                        except Exception:
                            continue
                    
                    # 중복 체크
                    exists = self.etf_collection.count_documents({
                        "TRD_DD": trd_dd_mongo,
                        "ISU_CD": record["ISU_CD"]
                    }, limit=1)
                    
                    if not exists:
                        record['TRD_DD'] = trd_dd_mongo
                        insert_records.append(record)
                
                if insert_records:
                    try:
                        self.etf_collection.insert_many(insert_records)
                        print(f"{row['ISU_ABBRV']} - {len(insert_records)}개 데이터 저장")
                    except Exception as e:
                        print(f"MongoDB 저장 오류 ({row['ISU_ABBRV']}): {e}")
                
            except Exception as e:
                print(f"API 호출 오류 ({row['ISU_ABBRV']}): {e}")
                continue
            
            # API 호출 간격 조절
            time.sleep(np.random.uniform(0.5, 2.0))
    
    def update_etf_data(self, days_back=10):
        """
        ETF 데이터를 업데이트합니다.
        
        Args:
            days_back: 최신 날짜에서 몇 일 전까지 데이터를 가져올지 (기본값: 10일)
        """
        print("=== KRX ETF 데이터 업데이트 시작 ===")
        
        try:
            # ETF 목록 가져오기
            etf_df = self._get_etf_list()
            print(f"ETF 목록 조회 완료: {len(etf_df)}개 종목")
            
            # DB에서 최신 날짜 확인
            latest_date = self.get_etf_latest_date()
            
            # 데이터 수집 기간 설정
            today = datetime.datetime.today()
            
            if latest_date:
                # DB에 데이터가 있으면 최신 날짜 다음날부터
                start_date = (latest_date + datetime.timedelta(days=1)).strftime("%Y%m%d")
                print(f"ETF DB 최신 데이터: {latest_date.strftime('%Y-%m-%d')}")
            else:
                # DB에 데이터가 없으면 지정된 일수만큼 이전부터
                start_date = (today - datetime.timedelta(days=days_back)).strftime("%Y%m%d")
                print("ETF DB에 데이터가 없어 새로 수집을 시작합니다.")
            
            end_date = today.strftime("%Y%m%d")
            
            print(f"ETF 데이터 수집 기간: {start_date} ~ {end_date}")
            
            # 업데이트할 데이터가 있는지 확인
            start_datetime = datetime.datetime.strptime(start_date, "%Y%m%d")
            if start_datetime > today:
                print("이미 최신 ETF 데이터입니다.")
                return
            
            # ETF OHLCV 데이터 수집 및 저장
            self._fetch_etf_ohlcv(etf_df, start_date, end_date)
            print("=== KRX ETF 데이터 업데이트 완료 ===")
            
        except Exception as e:
            print(f"ETF 데이터 업데이트 중 오류 발생: {e}")
    
    def get_etf_data(self, start_date, end_date, isu_codes=None):
        """
        지정된 기간의 ETF 데이터를 조회합니다.
        
        Args:
            start_date: 시작 날짜 (YYYY-MM-DD 형식)
            end_date: 종료 날짜 (YYYY-MM-DD 형식)
            isu_codes: 특정 종목코드 리스트 (선택사항)
        
        Returns:
            pd.DataFrame: 조회된 ETF 데이터
        """
        try:
            # 날짜 형식 변환
            start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
            
            # MongoDB 쿼리 구성
            query = {
                "TRD_DD": {
                    "$gte": start_dt,
                    "$lte": end_dt
                }
            }
            
            if isu_codes:
                query["ISU_CD"] = {"$in": isu_codes}
            
            # 데이터 조회
            cursor = self.etf_collection.find(query).sort("TRD_DD", 1)
            data = list(cursor)
            
            if not data:
                print("조회된 ETF 데이터가 없습니다.")
                return pd.DataFrame()
            
            # DataFrame으로 변환
            df = pd.DataFrame(data)
            
            # _id 컬럼 제거
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
            
            print(f"ETF 데이터 조회 완료: {len(df)}개 레코드")
            return df
            
        except Exception as e:
            print(f"ETF 데이터 조회 중 오류 발생: {e}")
            return pd.DataFrame()
    
    def get_etf_list(self):
        """현재 DB에 저장된 ETF 목록을 조회합니다."""
        try:
            pipeline = [
                {"$group": {
                    "_id": {
                        "ISU_CD": "$ISU_CD",
                        "ISU_ABBRV": "$ISU_ABBRV"
                    }
                }},
                {"$project": {
                    "_id": 0,
                    "ISU_CD": "$_id.ISU_CD",
                    "ISU_ABBRV": "$_id.ISU_ABBRV"
                }},
                {"$sort": {"ISU_ABBRV": 1}}
            ]
            
            result = list(self.etf_collection.aggregate(pipeline))
            df = pd.DataFrame(result)
            
            print(f"ETF 목록: {len(df)}개 종목")
            return df
            
        except Exception as e:
            print(f"ETF 목록 조회 중 오류 발생: {e}")
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