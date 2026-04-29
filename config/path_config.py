'''path_config.py
- 의미와 역할
    - S3 경로 규칙을 함수로 관리하는 파일
    - 모든 Lambda가 S3에 파일을 읽고 쓸 때 이 파일의 함수를 호출해서 경로를 가져가게 됨
    - 경로를 하드코딩하지 않으므로 경로 규칙이 바뀌어도 이 파일 하나만 수정하면 됨 
    - 특히 date={YYYYMMDD} 같은 파티션 형식이 파이프라인 전체에서 일관되게 유지되는 게 핵심
    - Medallion 계층 구조:
        Bronze  : raw/
        Silver  : {processed/} : 체결 경로 함수  &  {processed/daily/} : 일봉 경로 함수
        Gold    : signal/
        Config  : config/

- 담아야 할 내용 순서
    1. Bronze 경로 함수
        └─ 입력: stock_code, date, hour
        └─ 출력: raw/{stock_code}/date={YYYYMMDD}/hour={HH}/{stock_code}.log

    2. Silver 체결 경로 함수
            └─ 입력: stock_code, date, hour
        └─ 출력: processed/{stock_code}/date={YYYYMMDD}/hour={HH}/{stock_code}.parquet

    3. Silver 일봉 경로 함수
        └─ 입력: stock_code, date
        └─ 출력: processed/daily/{stock_code}/date={YYYYMMDD}/{stock_code}_daily.parquet

    4. Gold 시그널 경로 함수
        └─ 입력: stock_code, date
        └─ 출력: signal/{stock_code}/date={YYYYMMDD}/{stock_code}_signals.parquet

    5. Config 경로 함수
        └─ 입력: date
        └─ 출력: config/date={YYYYMMDD}/best_ma_config.json
'''

'''
RAW_PREFIX = "raw"
PROCESSED_PREFIX = "processed"
SIGNAL_PREFIX = "signal"
'''

# __all__ 리스트는 이 모듈에서 외부로 공개할 함수들을 명시하는 역할을 함.
# 모듈 호출 방법 : from config.path_config import bronze_key, silver_trade_key, silver_daily_key, gold_signal_key, config_key
__all__ = [
    "bronze_key",
    "silver_trade_key",
    "silver_daily_key",
    "gold_signal_key",
    "config_key",
]

# 경로 함수 템플릿
#   - 첫째, 타입 힌트를 붙이는 것이 중요
#     └─ def func_name(a: 변수타입, b: 변수타입) -> 반환타입:
#   - 둘째, 시간은 항상 2자리로 패딩 (hour:02d)

# Bronze 경로 함수
def bronze_key(stock_code: str, date: str, hour: int) -> str:
    return f"raw/{stock_code}/date={date}/hour={hour:02d}/{stock_code}.log"

# Silver 체결 경로 함수
def silver_trade_key(stock_code: str, date: str, hour: int) -> str:
    return f"processed/{stock_code}/date={date}/hour={hour:02d}/{stock_code}.parquet"

# Silver 일봉 경로 함수
def silver_daily_key(stock_code: str, date: str) -> str:
    return f"processed/daily/{stock_code}/date={date}/{stock_code}_daily.parquet"

# Gold 시그널 경로 함수
def gold_signal_key(stock_code: str, date:str) -> str:
    return f"signal/{stock_code}/date={date}/{stock_code}_signals.parquet"

# Config 경로 함수
def config_key(date: str) -> str:
    return f"config/date={date}/best_ma_config.json"