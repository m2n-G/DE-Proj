# config

> Configuration files shared by the collector, consumer, Lambda functions, and tests. 
> Keep secrets out of this directory. Use environment variables or `.env` files for keys, ARNs, bucket names, and webhook values.

## signal_config.py
- 의미와 역할
    이 파이프라인의 퀀트 전략 파라미터 중앙 관리 파일이에요. lambda_signal, lambda_optimize, lambda_notify 세 Lambda가 전부 이 파일을 참조해서 MA 계산 기준과 종목 리스트를 가져가요. 나중에 "MA 파라미터를 바꾸고 싶다"거나 "종목을 추가하고 싶다"고 할 때 이 파일 하나만 수정하면 전체 파이프라인에 반영돼요.
- 담아야 할 내용 순서
  ```
    1. 모니터링 종목 리스트 (WATCHLIST)
    2. 백테스트 후보 MA 조합 목록 (MA_CANDIDATES)
    3. MA 계산 기준 명시 (MA_BASE = "daily")
    4. 백테스트 기간 (OPTIMIZE_DAYS)
    5. 거래비용 (TRADE_COST)
    6. 알림 필터 최소 샤프지수 (SHARPE_MIN)
    7. 종목별 현재 최적 MA 조합 딕셔너리 (BEST_MA)
    └─ lambda_optimize 가 매일 자동으로 이 값을 갱신함
  ```

## path_config.py
- 의미와 역할
    S3 경로 규칙을 함수로 관리하는 파일이에요. 모든 Lambda가 S3에 파일을 읽고 쓸 때 이 파일의 함수를 호출해서 경로를 가져가요. 경로를 하드코딩하지 않으므로 경로 규칙이 바뀌어도 이 파일 하나만 수정하면 돼요. 특히 date={YYYYMMDD} 같은 파티션 형식이 파이프라인 전체에서 일관되게 유지되는 게 핵심이에요.
    담아야 할 내용 순서
- 담아야 할 내용 순서
  ```
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
  ```

## market_config.py
- 의미와 역할
    한국 주식시장의 운영 규칙을 정의하는 파일이에요. lambda_clean에서 장외시간 데이터를 필터링할 때, lambda_signal에서 장중에만 MA 계산을 실행할 때, Airflow DAG에서 장 시작·마감 스케줄을 설정할 때 이 파일을 참조해요. 특히 장외 체결 데이터(동시호가, 시간외 거래)가 파이프라인에 섞이지 않도록 막는 역할이 중요해요.
- 담아야 할 내용 순서
  ```
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
  ```

## schema_config.py
- 의미와 역할
    각 계층(Bronze·Silver·Gold)의 데이터 스키마를 명세하는 파일이에요. lambda_clean에서 정제 후 컬럼이 맞는지 검증할 때, Athena에서 테이블을 생성할 때 이 파일의 스키마를 기준으로 써요. 스키마가 중앙에서 관리되므로 나중에 컬럼이 추가되거나 타입이 바뀌어도 한 곳만 수정하면 전체 파이프라인에 반영돼요.
- 담아야 할 내용 순서
  ```
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
  ```
