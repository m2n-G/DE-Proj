''' market_config.py
- 의미와 역할
    - 한국 주식시장의 운영 규칙을 정의하는 파일
    - 파일 사용 시점 :
        - lambda_clean에서 장외시간 데이터를 필터링할 때
        - lambda_signal에서 장중에만 MA 계산을 실행할 때
        - Airflow DAG에서 장 시작·마감 스케줄을 설정할 때 이 파일을 참조해요. 
    - 특히 장외 체결 데이터(동시호가, 시간외 거래)가 파이프라인에 섞이지 않도록 막는 역할
- 담아야 할 내용 순서
    1. 장 시작 시간 (MARKET_OPEN = "09:00")
    2. 장 마감 시간 (MARKET_CLOSE = "15:30")
    3. 타임존 (TIMEZONE = "Asia/Seoul")
    4. 거래일 기준 (MARKET_DAYS = 월~금)
    5. 장중 여부 판단 함수
        └─ 입력: 현재 시각
        └─ 출력: True / False
    6. 거래일 여부 판단 함수
        └─ 입력: 날짜
        └─ 출력: True / False
        └─ 주의: 공휴일 처리 필요 (holidays 라이브러리 활용)
'''

from datetime import time, date
from zoneinfo import ZoneInfo
from holidayskr import is_holiday # 한국 공휴일 판단 라이브러리 : pip install holidayskr
import logging

logger = logging.getLogger(__name__)

MARKET_TIMEZONE = ZoneInfo("Asia/Seoul")     # 한국 주식시장 : 서울 시간 기준
MARKET_OPEN = time(9, 0)                     # 장 시작 시간 : 오전 9시 (09:00)
MARKET_CLOSE = time(15, 30)                  # 장 마감 시간 : 오후 3시 30분 (15:30)
MARKET_DAYS = {0, 1, 2, 3, 4}                # 거래일 : 월(0)~금(4)


# 장중 여부 판단 함수
def is_market_time(current_time: time) -> bool:
    # 현재 시각이 장 시작과 마감 사이에 있는지 확인
    return MARKET_OPEN <= current_time <= MARKET_CLOSE

# 거래일 여부 판단 함수(주의: 공휴일 처리 필요 (holidays 라이브러리 활용)
def is_market_day(current_date: date) -> bool:
    # 현재 날짜의 요일이 거래일에 포함되는지 확인
    if is_holiday(current_date):
        logger.info(f"{current_date}는 공휴일입니다.")
        return False
    else:
        return current_date.weekday() in MARKET_DAYS

