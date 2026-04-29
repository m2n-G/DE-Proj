''' schema_config.py
- 의미와 역할
    - 각 계층(Bronze·Silver·Gold)의 데이터 스키마를 명세하는 파일
    - lambda_clean에서 정제 후 컬럼이 맞는지 검증할 때, Athena에서 테이블을 생성할 때 이 파일의 스키마를 기준으로 사용
    - 스키마가 중앙에서 관리되므로 나중에 컬럼이 추가되거나 타입이 바뀌어도 한 곳만 수정하면 전체 파이프라인에 반영됨
- 스키마 명세 방식
    - 각 계층별로 컬럼 이름과 타입을 딕셔너리로 명시
    - 타입은 Athena에서 사용할 수 있는 기본 타입(string, int, float, date 등)으로 명시

    layer_name = {
        "column_name1" : "type1",
        "column_name2" : "type2",
        ...
    }

- 담아야 할 내용 순서
    1. Bronze 스키마
        └─ 원본 그대로 (스키마 강제 없음 · None 또는 주석으로 명시)

    2. Silver 체결 스키마
        └─ stock_code   : string
        └─ trade_time   : string   (HHMMSS 형식)
        └─ trade_price  : int
        └─ trade_volume : int
        └─ open_price   : int      (당일 시가 · 고정값)
        └─ prev_close   : int      (전일 종가)

    3. Silver 일봉 스키마
        └─ stock_code : string
        └─ date       : date
        └─ open       : int
        └─ high       : int        (장중 미확정 시 None)
        └─ low        : int        (장중 미확정 시 None)
        └─ close      : int        (장중 미확정 시 None)
        └─ volume     : int        (장중 미확정 시 None)

    4. Gold 스키마
        └─ date     : date
        └─ stock    : string
        └─ open     : int
        └─ MA_S     : float        (단기 이동평균)
        └─ MA_L     : float        (장기 이동평균)
        └─ signal   : string       (golden_cross / dead_cross / None)
        └─ ma_combo : string       (예: MA5/20)

    5. Config 스키마
        └─ updated_at : string     (ISO 8601 형식)
        └─ base       : string     (daily_close 고정)
        └─ best_ma    : dict       (종목코드 → short/long/sharpe)
'''

# Bronze 스키마
# 원본 그대로 보존 · 스키마 강제 없음
# Kafka Consumer가 적재한 원본 문자열 그대로 저장
BRONZE_SCHEMA = None

# Silver 체결 스키마
SILVER_TRADE_SCHEMA = {
    "stock_code"   : "string",   # string
    "trade_date"   : "string",   # string (YYYYMMDD 형식)
    "trade_time"   : "string",   # string (HHMMSS 형식)
    "trade_price"  : "int",      # int
    "volume"       : "int",      # int
    "open_price"   : "int",      # int (당일 시가 · 고정값)
    "prev_close"   : "int",      # int (전일 종가)
}

# Silver 일봉 스키마
SILVER_DAILY_SCHEMA = {
    "stock_code"   : "string",   # string
    "date"         : "date",     # date
    "open"         : "int",      # int                    
    "high"         : "int",      # int      (장중 미확정 시 None)
    "low"          : "int",      # int      (장중 미확정 시 None)
    "close"        : "int",      # int      (장중 미확정 시 None)
    "volume"       : "int"       # int       (장중 미확정 시 None)
}

# Gold 스키마
GOLD_SIGNAL_SCHEMA = {
    "date"         : "date",     # date
    "stock_code"   : "string",   # string
    "open"         : "int",      # int
    "ma_short"     : "float",    # float     (단기 이동평균)
    "ma_long"      : "float",    # float     (장기 이동평균)
    "signal"       : "string",   # string    (golden_cross / dead_cross / None)
    "ma_combo"     : "string",   # string    (예: MA5/20)
}

# Config 스키마
CONFIG_SCHEMA = {
    "updated_at"   : "string",   # string (ISO 8601 형식)
    "base"         : "string",   # string (daily_close 고정)
    "best_ma"      : "dict",     # dict   (종목코드 → short/long/sharpe)
}