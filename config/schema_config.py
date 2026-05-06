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

# 실시간 체결가 응답 스키마
KIS_TRADE_RAW_FIELDS  = [
    ('MKSC_SHRN_ISCD', str),                      # 유가증권 단축 종목코드
    ('STCK_CNTG_HOUR', str),                      # 주식 체결 시간
    ('STCK_PRPR', int),                           # 주식 현재가
    ('PRDY_VRSS_SIGN', str),                      # 전일 대비 부호
    ('PRDY_VRSS', int),                           # 전일 대비
    ('PRDY_CTRT', float),                         # 전일 대비율
    ('WGHN_AVRG_STCK_PRC', float),                # 가중 평균 주식 가격
    ('STCK_OPRC', int),                           # 주식 시가
    ('STCK_HGPR', int),                           # 주식 최고가
    ('STCK_LWPR', int),                           # 주식 최저가
    ('ASKP1', int),                               # 매도호가1
    ('BIDP1', int),                               # 매수호가1
    ('CNTG_VOL', int),                            # 체결 거래량
    ('ACML_VOL', int),                            # 누적 거래량
    ('ACML_TR_PBMN', int),                        # 누적 거래 대금
    ('SELN_CNTG_CSNU', int),                      # 매도 체결 건수
    ('SHNU_CNTG_CSNU', int),                      # 매수 체결 건수
    ('NTBY_CNTG_CSNU', int),                      # 순매수 체결 건수
    ('CTTR', float),                              # 체결강도
    ('SELN_CNTG_SMTN', int),                      # 총 매도 수량
    ('SHNU_CNTG_SMTN', int),                      # 총 매수 수량
    ('CCLD_DVSN', str),                           # 체결구분
    ('SHNU_RATE', float),                         # 매수비율
    ('PRDY_VOL_VRSS_ACML_VOL_RATE', float),       # 전일 거래량 대비 등락율
    ('OPRC_HOUR', str),                           # 시가 시간
    ('OPRC_VRSS_PRPR_SIGN', str),                 # 시가대비구분
    ('OPRC_VRSS_PRPR', int),                      # 시가대비
    ('HGPR_HOUR', str),                           # 최고가 시간
    ('HGPR_VRSS_PRPR_SIGN', str),                 # 고가대비구분
    ('HGPR_VRSS_PRPR', int),                      # 고가대비
    ('LWPR_HOUR', str),                           # 최저가 시간
    ('LWPR_VRSS_PRPR_SIGN', str),                 # 저가대비구분
    ('LWPR_VRSS_PRPR', int),                      # 저가대비
    ('BSOP_DATE', str),                           # 영업 일자
    ('NEW_MKOP_CLS_CODE', str),                   # 신 장운영 구분 코드
    ('TRHT_YN', str),                             # 거래정지 여부
    ('ASKP_RSQN1', int),                          # 매도호가 잔량1
    ('BIDP_RSQN1', int),                          # 매수호가 잔량1
    ('TOTAL_ASKP_RSQN', int),                     # 총 매도호가 잔량
    ('TOTAL_BIDP_RSQN', int),                     # 총 매수호가 잔량
    ('VOL_TNRT', float),                          # 거래량 회전율
    ('PRDY_SMNS_HOUR_ACML_VOL', int),             # 전일 동시간 누적 거래량
    ('PRDY_SMNS_HOUR_ACML_VOL_RATE', float),      # 전일 동시간 누적 거래량 비율
    ('HOUR_CLS_CODE', str),                       # 시간 구분 코드
    ('MRKT_TRTM_CLS_CODE', str),                  # 임의종료구분코드
    ('VI_STND_PRC', int),                         # 정적VI발동기준가
]

# Silver 체결 스키마
SILVER_TRADE_SCHEMA = {
    "stock_code"   : "string",   # 유가증권 단축 종목코드
    "trade_time"   : "string",   # 주식 체결 시간 (HHMMSS 형식)
    "trade_price"  : "int",      # 주식 현재가
    "volume"       : "int",      # 체결 거래량
    "open_price"   : "int",      # 당일 시가
    "prev_close"   : "int",      # 직전 종가
}

'''
SILVER_TRADE_SCHEMA = {
    "stock_code"                       : "string",   # 유가증권 단축 종목코드
    "trade_time"                       : "string",   # 주식 체결 시간 (HHMMSS 형식)
    "trade_price"                      : "int",      # 주식 현재가
    "price_change_sign"                : "string",   # 전일 대비 부호
    "price_change"                     : "int",      # 전일 대비
    "price_change_rate"                : "float",    # 전일 대비율
    "weighted_avg_price"               : "float",    # 가중 평균 주식 가격
    "open_price"                       : "int",      # 주식 시가
    "high_price"                       : "int",      # 주식 최고가
    "low_price"                        : "int",      # 주식 최저가
    "ask_price_1"                      : "int",      # 매도호가1
    "bid_price_1"                      : "int",      # 매수호가1
    "trade_volume"                     : "int",      # 체결 거래량
    "acc_trade_volume"                 : "int",      # 누적 거래량
    "acc_trade_amount"                 : "int",      # 누적 거래 대금
    "sell_trade_count"                 : "int",      # 매도 체결 건수
    "buy_trade_count"                  : "int",      # 매수 체결 건수
    "net_buy_trade_count"              : "int",      # 순매수 체결 건수
    "trade_strength"                   : "float",    # 체결강도
    "sell_trade_volume_sum"            : "int",      # 총 매도 수량
    "buy_trade_volume_sum"             : "int",      # 총 매수 수량
    "trade_type"                       : "string",   # 체결구분
    "buy_rate"                         : "float",    # 매수비율
    "prev_volume_change_rate"          : "float",    # 전일 거래량 대비 등락율
    "open_time"                        : "string",   # 시가 시간
    "open_price_change_sign"           : "string",   # 시가대비구분
    "open_price_change"                : "int",      # 시가대비
    "high_time"                        : "string",   # 최고가 시간
    "high_price_change_sign"           : "string",   # 고가대비구분
    "high_price_change"                : "int",      # 고가대비
    "low_time"                         : "string",   # 최저가 시간
    "low_price_change_sign"            : "string",   # 저가대비구분
    "low_price_change"                 : "int",      # 저가대비
    "trade_date"                       : "string",   # 영업 일자
    "market_operation_code"            : "string",   # 신 장운영 구분 코드
    "trading_halt_yn"                  : "string",   # 거래정지 여부
    "ask_remain_qty_1"                 : "int",      # 매도호가 잔량1
    "bid_remain_qty_1"                 : "int",      # 매수호가 잔량1
    "total_ask_remain_qty"             : "int",      # 총 매도호가 잔량
    "total_bid_remain_qty"             : "int",      # 총 매수호가 잔량
    "volume_turnover_rate"             : "float",    # 거래량 회전율
    "prev_same_time_acc_volume"        : "int",      # 전일 동시간 누적 거래량
    "prev_same_time_acc_volume_rate"   : "float",    # 전일 동시간 누적 거래량 비율
    "time_class_code"                  : "string",   # 시간 구분 코드
    "market_close_class_code"          : "string",   # 임의종료구분코드
    "vi_standard_price"                : "int",      # 정적VI발동기준가
}
'''

# Silver 일봉 스키마
SILVER_DAILY_SCHEMA = {
    "stock_code"   : "string",   # 유가증권 단축 종목코드
    "date"         : "date",     # 주식 체결 날짜
    "open"         : "int",      # 당일 시가                    
    "high"         : "int",      # 장 시작부터 현재까지의 누적 최고가      (장중 미확정 시 None)
    "low"          : "int",      # 장 시작부터 현재까지의 누적 최저가      (장중 미확정 시 None)
    "close"        : "int",      # 당일 종가                           (장중 미확정 시 None)
    "volume"       : "int"       # 누적 거래량 (장 시작 ~ 현재까지)       (장중 미확정 시 None)
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