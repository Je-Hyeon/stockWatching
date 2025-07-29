import time
import os
import requests
import random
from datetime import datetime, timedelta, date
import numpy as np
import pandas as pd
import xml.etree.ElementTree as ET
from tqdm import tqdm
from pymongo import MongoClient
from dotenv import load_dotenv
from typing import List, Dict, Optional, Union
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SeibroClient:
    """
    SEIBRO API를 사용하여 미국 주식 국내 정산 데이터를 수집하는 클래스
    """
    
    def __init__(self, user_agent: Optional[str] = None, mongo_uri: Optional[str] = None):
        """
        SeibroClient 초기화
        
        Args:
            user_agent (str, optional): User-Agent 헤더. None이면 환경변수에서 가져옴
            mongo_uri (str, optional): MongoDB 연결 URI. None이면 환경변수에서 가져옴
        """
        load_dotenv()
        
        self.user_agent = user_agent or os.getenv("USER_AGENT")
        self.mongo_uri = mongo_uri or os.getenv("MONGODB_URI")
        
        if not self.user_agent:
            raise ValueError("USER_AGENT 환경변수가 설정되지 않았습니다.")
        
        self.base_url = "https://seibro.or.kr/websquare/engine/proworks/callServletService.jsp"

        self.session = requests.Session()
        
        # 기본 헤더 설정
        self.headers = {
            "accept": "application/xml",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "connection": "keep-alive",
            "content-type": "application/xml; charset=UTF-8",
            "cookie": "",
            "host": "seibro.or.kr",
            "origin": "https://seibro.or.kr",
            "referer": "https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/ovsSec/BIP_CNTS10013V.xml&menuNo=921",
            "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "submissionid": "submission_getImptFrcurStkSetlAmtList",
            "user-agent": self.user_agent
        }
        
        # MongoDB 클라이언트 초기화
        if self.mongo_uri:
            self.mongo_client = MongoClient(self.mongo_uri)
            self.db = self.mongo_client['quant']
            self.collection = self.db["us_stock_settlement_in_korea"]
        else:
            self.mongo_client = None
            self.db = None
            self.collection = None
            logger.warning("MongoDB URI가 설정되지 않아 데이터베이스 저장 기능이 비활성화됩니다.")
    
    def get_latest_date(self):
        """DB에서 가장 최근 날짜를 조회"""
        if self.collection is None:
            logger.warning("MongoDB가 설정되지 않았습니다.")
            return None
            
        try:
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
        except Exception as e:
            logger.error(f"최신 날짜 조회 중 오류: {e}")
            return None

    def _create_payload(self, date_str: str) -> str:
        """
        API 요청용 XML 페이로드 생성
        
        Args:
            date_str (str): 날짜 문자열 (YYYYMMDD 형식)
            
        Returns:
            str: XML 페이로드
        """
        return f"""
        <reqParam action="getImptFrcurStkSetlAmtList" task="ksd.safe.bip.cnts.OvsSec.process.OvsSecIsinPTask">
            <MENU_NO value="921"/>
            <CMM_BTN_ABBR_NM value="total_search,openall,print,hwp,word,pdf,seach,xls,"/>
            <W2XPATH value="/IPORTAL/user/ovsSec/BIP_CNTS10013V.xml"/>
            <PG_START value="1"/>
            <PG_END value="10"/>
            <START_DT value="{date_str}"/>
            <END_DT value="{date_str}"/>
            <S_TYPE value="2"/>
            <S_COUNTRY value="US"/>
            <D_TYPE value="4"/>
        </reqParam>
        """.strip()
    
    def _parse_xml_response(self, xml_str: str, current_date: datetime) -> List[Dict]:
        """
        XML 응답을 파싱하여 데이터 추출
        
        Args:
            xml_str (str): XML 응답 문자열
            current_date (datetime): 현재 처리 중인 날짜
            
        Returns:
            List[Dict]: 파싱된 데이터 리스트
        """
        rows = []
        
        try:
            root = ET.fromstring(xml_str)
        except ET.ParseError as e:
            logger.error(f"XML 파싱 오류: {e}")
            return rows
        
        # 주말 등 데이터가 없는 경우 <vector ... result="0">로 응답이 옴
        if root.tag == "vector" and root.attrib.get("result") == "0":
            logger.info(f"{current_date.strftime('%Y-%m-%d')}: 데이터가 없습니다.")
            return rows
        
        for data in root.findall('.//data'):
            result = data.find('result')
            if result is not None:
                row = {}
                for child in result:
                    row[child.tag] = child.attrib.get('value', '')
                # 날짜 컬럼 추가
                row['DATE'] = current_date
                rows.append(row)
        
        return rows
    
    def _process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        DataFrame 전처리 (컬럼 제거, 데이터 타입 변환)
        
        Args:
            df (pd.DataFrame): 원본 DataFrame
            
        Returns:
            pd.DataFrame: 전처리된 DataFrame
        """
        # 불필요한 컬럼 제거
        df = df.drop(columns=['RNUM', 'NATION_NM'], errors='ignore')
        
        # 숫자 컬럼 변환
        for col in df.columns:
            if col == 'DATE':
                continue
            try:
                df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='raise')
            except Exception:
                pass
        
        return df
    
    def _check_existing_dates(self, date_list: List[datetime]) -> List[datetime]:
        """
        DB에서 이미 존재하는 날짜들을 확인하고 없는 날짜만 반환
        
        Args:
            date_list (List[datetime]): 확인할 날짜 리스트
            
        Returns:
            List[datetime]: DB에 없는 날짜 리스트
        """
        # 변경: Collection 객체는 bool()로 평가할 수 없으므로 None 비교로 변경
        if self.collection is None:
            logger.warning("MongoDB가 설정되지 않아 모든 날짜를 수집합니다.")
            return date_list
        
        try:
            # DB에서 해당 날짜 범위의 데이터가 있는지 확인
            start_date = min(date_list)
            end_date = max(date_list)
            
            # 기존 데이터가 있는 날짜들 조회
            existing_dates = self.collection.distinct(
                "DATE", 
                {
                    "DATE": {
                        "$gte": start_date,
                        "$lte": end_date
                    }
                }
            )
            
            # 기존 데이터가 없는 날짜들만 필터링
            missing_dates = [date for date in date_list if date not in existing_dates]
            
            if existing_dates:
                logger.info(f"DB에 이미 존재하는 날짜: {len(existing_dates)}개")
                logger.info(f"수집이 필요한 날짜: {len(missing_dates)}개")
            else:
                logger.info("DB에 해당 기간의 데이터가 없습니다. 모든 날짜를 수집합니다.")
            
            return missing_dates
            
        except Exception as e:
            logger.error(f"DB 확인 중 오류 발생: {e}")
            logger.warning("DB 확인을 건너뛰고 모든 날짜를 수집합니다.")
            return date_list

    def update_data(self, days_back: int = 10):
        """
        최신 데이터 업데이트 (DB에 없는 날짜만 수집)
        
        Args:
            days_back: DB에 데이터가 없을 때 몇 일 전부터 수집할지 (기본값: 10일)
        """
        logger.info("=== SEIBRO 미국 주식 정산 데이터 업데이트 시작 ===")
        
        # DB에서 가장 최근 날짜 조회
        latest_date = self.get_latest_date()
        
        if latest_date:
            # 다음날부터 데이터 수집 시작
            start_date = latest_date + timedelta(days=1)
            logger.info(f"DB 최근 날짜: {latest_date}, 수집 시작 날짜: {start_date}")
        else:
            # DB가 비어있으면 지정된 일수만큼 이전부터 시작
            start_date = date.today() - timedelta(days=days_back)
            logger.info(f"DB가 비어있습니다. {days_back}일 전부터 데이터 수집을 시작합니다.")

        end_date = date.today()

        # 수집할 데이터가 없으면 종료
        if start_date > end_date:
            logger.info("이미 최신 데이터입니다. 업데이트할 데이터가 없습니다.")
            return
        
        logger.info(f"데이터 수집 기간: {start_date} ~ {end_date}")
        
        # 데이터 수집
        df = self.collect_settlement_data(
            start_date=start_date,
            end_date=end_date,
            save_to_db=True,
            skip_existing=True
        )
        
        if not df.empty:
            logger.info(f"데이터 업데이트 완료: {len(df)}건")
        else:
            logger.info("수집된 새로운 데이터가 없습니다.")
        
        logger.info("=== SEIBRO 미국 주식 정산 데이터 업데이트 완료 ===")

    def collect_settlement_data(self, start_date: Union[str, datetime], 
                               end_date: Union[str, datetime],
                               save_to_db: bool = True,
                               sleep_range: tuple = (2.5, 10),
                               skip_existing: bool = True) -> pd.DataFrame:
        """
        미국 주식 국내 정산 데이터 수집
        
        Args:
            start_date (Union[str, datetime]): 시작 날짜
            end_date (Union[str, datetime]): 종료 날짜
            save_to_db (bool): MongoDB에 저장할지 여부
            sleep_range (tuple): 요청 간 대기 시간 범위 (초)
            skip_existing (bool): DB에 이미 존재하는 날짜를 건너뛸지 여부
            
        Returns:
            pd.DataFrame: 수집된 데이터
        """
        # 날짜 변환
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        
        date_list = list(pd.date_range(start=start_date, end=end_date))
        logger.info(f"데이터 수집 시작: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
        
        # DB에 이미 존재하는 날짜 확인
        if skip_existing:
            date_list = self._check_existing_dates(date_list)
            if not date_list:
                logger.info("모든 날짜의 데이터가 이미 DB에 존재합니다.")
                return pd.DataFrame()
        
        all_rows = []
        
        try:
            for current_date in tqdm(date_list, desc="날짜별 데이터 수집 진행중"):
                date_str = current_date.strftime("%Y%m%d")
                payload = self._create_payload(date_str)
                
                try:
                    req = self.session.post(
                        self.base_url, 
                        headers=self.headers, 
                        data=payload.encode('utf-8'),
                        timeout=30
                    )
                    req.raise_for_status()
                    
                    xml_str = req.content.decode('utf-8')
                    rows = self._parse_xml_response(xml_str, current_date)
                    all_rows.extend(rows)
                    
                    logger.info(f"{date_str}: {len(rows)}개 데이터 수집 완료")
                    
                except requests.RequestException as e:
                    logger.error(f"{date_str} 요청 오류: {e}")
                    continue
                
                # 랜덤 슬립
                time.sleep(random.uniform(*sleep_range))
        
        except KeyboardInterrupt:
            logger.info("사용자에 의해 중단되었습니다.")
        
        # DataFrame 생성 및 전처리
        if not all_rows:
            logger.warning("수집된 데이터가 없습니다.")
            return pd.DataFrame()
        
        df = pd.DataFrame(all_rows)
        df = self._process_dataframe(df)
        
        logger.info(f"총 {len(df)}개 데이터 수집 완료")
        
        # MongoDB에 저장
        if save_to_db and self.collection is not None:
            try:
                records = df.to_dict(orient='records')
                self.collection.insert_many(records)
                logger.info(f"MongoDB에 {len(records)}개 데이터 저장 완료")
            except Exception as e:
                logger.error(f"MongoDB 저장 오류: {e}")
        elif save_to_db and self.collection is None:
            logger.warning("MongoDB가 설정되지 않아 저장을 건너뜁니다.")
        
        return df
    
    def get_data(self, start_date=None, end_date=None, limit=None):
        """
        DB에서 데이터 조회
        
        Args:
            start_date (str): 시작 날짜 (YYYY-MM-DD 형식)
            end_date (str): 종료 날짜 (YYYY-MM-DD 형식)
            limit (int): 조회할 최대 레코드 수
            
        Returns:
            pd.DataFrame: 조회된 데이터
        """
        if self.collection is None:
            logger.warning("MongoDB가 설정되지 않았습니다.")
            return pd.DataFrame()
            
        try:
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
                logger.info(f"데이터 조회 완료: {len(df)}개 레코드")
                return df
            else:
                logger.info("조회된 데이터가 없습니다.")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"데이터 조회 중 오류: {e}")
            return pd.DataFrame()
    
    def close(self):
        """리소스 정리"""
        if self.session:
            self.session.close()
        if self.mongo_client:
            self.mongo_client.close()
        logger.info("SeibroClient 리소스가 정리되었습니다.")
    
    def __enter__(self):
        """컨텍스트 매니저 진입"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """컨텍스트 매니저 종료"""
        self.close()
