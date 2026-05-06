"""handler.py
- 의미와 역할
    - S3 Bronze에 적재된 원본 구분자 문자열을 읽어서 파싱
    - 문자열을 정제 후 S3 Silver 두 경로에 저장
    - Kinesis가 트리거하면 자동으로 실행
    - Bronze → Silver 변환의 핵심

- 담아야 할 내용 순서
1. 환경변수 로드
   └─ S3_BUCKET · AWS_REGION 읽기

2. S3 클라이언트 초기화
   └─ boto3.client("s3") 생성

3. 원본 문자열 파싱 함수
   └─ 구분자(|) 로 분리 → payload 추출
   └─ 구분자(^) 로 분리 → 필드 추출
   └─ 추출 필드:
       stock_code  · trade_time · trade_price
       trade_volume · open_price · prev_close
   └─ 문자열 → int / float 타입 변환
   └─ 유효성 검증 (결측값 · 이상값 제거)
   └─ 장외시간 데이터 필터링
       market_config.py 의 is_market_time() 활용

4. Silver 체결 데이터 저장 함수
   └─ 파싱 결과 → pandas DataFrame 변환
   └─ Parquet 포맷으로 변환
   └─ path_config.py 의 silver_trade_key() 로 경로 생성
   └─ S3 PutObject 로 Silver 체결 경로에 저장

5. Silver 일봉 데이터 저장 함수 (★ 핵심)
   └─ 당일 첫 체결가 → open_price 추출
   └─ 이미 오늘 시가가 저장됐는지 확인
       └─ 저장됐으면 skip (하루에 한 번만 저장)
       └─ 없으면 S3 daily/ 경로에 저장
   └─ path_config.py 의 silver_daily_key() 로 경로 생성
   └─ 과거 종가 데이터와 합쳐서 저장

6. Kinesis 이벤트 핸들러 (Lambda 진입점)
   └─ Kinesis 레코드에서 S3 이벤트 정보 추출
   └─ S3 Bronze 에서 원본 파일 읽기
   └─ 3번 파싱 함수 호출
   └─ 4번 Silver 체결 저장 함수 호출
   └─ 5번 Silver 일봉 저장 함수 호출

7. 로깅
   └─ 파싱 성공 · 실패 로그
   └─ Silver 적재 성공 · 실패 로그
"""


# 1. 필요한 라이브러리 import
import os
import io
import json
import uuid
import base64
import logging
import urllib.parse
import boto3
import pandas as pd

from dotenv import load_dotenv
from typing import Any
from botocore.exceptions import ClientError

from config.path_config import silver_daily_key, silver_trade_key
from config.schema_config import KIS_TRADE_RAW_FIELDS, SILVER_TRADE_SCHEMA
from src.common.time_utils import now_kst


# 1-2. 환경변수 로드
load_dotenv(".env")

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-3")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


# 7. logger 생성
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(levelname)s:%(name)s:%(message)s",
)

logger = logging.getLogger(__name__)

# boto3/botocore 내부 로그가 너무 많으면 아래처럼 줄인다.
# logging.getLogger("boto3").setLevel(logging.WARNING)
# logging.getLogger("botocore").setLevel(logging.WARNING)
# logging.getLogger("urllib3").setLevel(logging.WARNING)


# 2. S3 client 생성 함수
def create_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


# 3-1. Lambda event에서 S3 object 정보 추출 함수
def extract_s3_references_from_event(event: dict) -> list[dict]:
    """
    Lambda event에서 처리할 S3 bucket/key 목록을 추출한다.

    지원할 event 형태:
    - S3 Event 직접 트리거
    - Kinesis record 안에 S3 Event가 들어 있는 형태

    반환 예:
    [
        {"bucket": "my-bucket", "key": "raw/005930/date=20260505/hour=10/...log"}
    ]
    """
    # TODO:
    # 1. event["Records"]를 순회한다.
    # 2. record에 "s3"가 있으면 bucket/key를 꺼낸다.
    # 3. S3 key는 urllib.parse.unquote_plus()로 디코딩한다.
    # 4. record에 "kinesis"가 있으면 base64 decode 후 json.loads() 한다.
    # 5. Kinesis 안의 nested event를 다시 이 함수로 처리한다.
    # 6. bucket/key dict 목록을 반환한다.

    s3_object = []

    for record in event["Records"]:
        if "s3" in record:
            bucket = record["s3"]["bucket"]["name"]
            key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
            s3_object.append({"bucket": bucket, "key": key})

        elif "kinesis" in record:
            encoded_data = record["kinesis"]["data"].encode("ascii")
            decoded = base64.b64decode(encoded_data)
            decoded_data = decoded.decode("utf-8")
            nested_event = json.loads(decoded_data)

            nested_objects = extract_s3_references_from_event(nested_event)  # 찾아낸 결과는 그 “재귀 호출 내부의 로컬 리스트”에만 들어가게 됨
            s3_object.extend(nested_objects)                                 # extend(): 찾은 결과를 바깥 최종 결과에 반영한다

    return s3_object


# 3-2. Bronze object 읽기 함수
def read_bronze_object(s3_client, bucket: str, key: str) -> str:
    """
    S3 Bronze object를 읽어서 raw 문자열로 반환한다.

    입력:
    - s3_client
    - bucket
    - key

    반환:
    - Bronze에 저장된 raw trade message 문자열
    """
    # TODO:
    # 1. s3_client.get_object(Bucket=bucket, Key=key)를 호출한다.
    # 2. response["Body"].read()로 bytes를 읽는다.
    # 3. bytes를 utf-8 문자열로 decode 한다.
    # 4. 앞뒤 공백/개행을 strip 한다.
    # 5. raw_message를 반환한다.

    response = s3_client.get_object(Bucket=bucket, Key=key)
    bronze_obj = response["Body"].read()
    raw_message = bronze_obj.decode("utf-8").strip()
    return raw_message


# 3-3-1. 빈 문자열 대비 안전한 변환 함수
def safe_cast(val, dtype):
    if val == '':
        return None
    return dtype(val)


# 3-3. raw 체결 메시지 파싱 함수
def parse_raw_trade_message(raw_message: str) -> list[dict]:
    """
    KIS WebSocket raw 체결 문자열을 Silver용 record 목록으로 변환한다.

    입력 예:
    0|H0STCNT0|001|005930^093713^225000^2^4500^...

    반환 예:
    [
        {
            "stock_code": "005930",
            "trade_time": "093713",
            "trade_price": 225000,
            "trade_volume": 2,
            ...
        }
    ]

    주의:
    - |001|이면 체결 row가 1개다.
    - |005|이면 체결 row가 5개다.
    - 따라서 반환값은 dict 하나가 아니라 list[dict]가 좋다.
    """
    # TODO:
    # 1. raw_message를 "|" 기준으로 최대 4개로 나눈다.
    # 2. prefix, tr_id, count_text, payload를 꺼낸다.
    # 3. tr_id가 "H0STCNT0"인지 확인한다.
    # 4. count_text를 int로 변환한다.
    # 5. payload를 "^" 기준으로 나눈다.
    # 6. 체결 row 1개의 필드 개수를 정한다.
    # 7. count만큼 반복하면서 row 필드를 자른다.
    #   i=0 -> fields[0:46]
    #   i=1 -> fields[46:92]
    # 8. 필요한 필드를 int/float/string으로 변환한다.
    # 9. record dict를 records 리스트에 추가한다.
    # 10. records를 반환한다.
    parts = raw_message.split("|", maxsplit=3)

    if len(parts) != 4:
        raise ValueError("raw_message 형식이 올바르지 않습니다.")

    prefix, tr_id, count_text, payload = parts

    if tr_id != "H0STCNT0":
        raise ValueError("실시간 체결 데이터가 아닙니다.")

    row_count      = int(count_text)
    fields         = payload.split("^")
    fields_per_row = len(KIS_TRADE_RAW_FIELDS)
    records        = []

    for i in range(row_count):
        start = i * fields_per_row
        row = fields[start:start + fields_per_row]
        
        if len(row) < fields_per_row:
            raise ValueError(f"체결 row 필드 수가 부족합니다. expected={fields_per_row}, actual={len(row)}")

        # 한투 원본 필드 → 값 변환
        raw_record = {key: safe_cast(val, dtype) for (key, dtype), val in zip(KIS_TRADE_RAW_FIELDS, row)}
        records.append(raw_record)

    return records

# 4-1-1-1. prev_close 계산식
def calc_prev_close(trade_price, sign, diff):
    # KIS 전일대비부호: 1 상한, 2 상승, 3 보합, 4 하한, 5 하락
    if sign in ("1", "2"):
        return trade_price - diff
    if sign in ("4", "5"):
        return trade_price + diff
    return trade_price

# 4-1-1. silver 실시간 체결 데이터 변환
# records = parse_raw_trade_message(raw_message)
def create_silver_trade_records(records: list[dict]) -> list[dict]:
    silver_records = []
    for raw in records:
        silver_records.append({
            "stock_code":  raw["MKSC_SHRN_ISCD"],
            "trade_time":  raw["STCK_CNTG_HOUR"],
            "trade_price": raw["STCK_PRPR"],
            "volume":      raw["CNTG_VOL"],
            "open_price":  raw["STCK_OPRC"],
            "prev_close":  calc_prev_close(
                raw["STCK_PRPR"], raw["PRDY_VRSS_SIGN"], raw["PRDY_VRSS"],
            ),
        })
    return silver_records

# 4-1. Silver trade S3 key 생성 함수
def build_silver_trade_object_key(records: list[dict]) -> str:
    """
    Silver trade 저장 경로를 만든다.

    경로 예:
    processed/005930/date=20260505/hour=10/005930_105139_abcd1234.parquet
    """
    # TODO:
    # 1. records[0]에서 stock_code를 꺼낸다.
    # 2. now_kst()로 현재 KST 시간을 구한다.
    # 3. date = YYYYMMDD
    # 4. hour = HH
    # 5. timestamp = HHMMSS 또는 YYYYMMDDTHHMMSS
    # 6. msg_id = uuid.uuid4().hex[:8]
    # 7. silver_trade_key(stock_code, date, hour, timestamp, msg_id)를 호출한다.
    # 8. key를 반환한다.
    stock_code = records[0]['stock_code']
    now        = now_kst()
    date       = now.strftime("%Y%m%d")
    hour       = int(now.strftime("%H"))
    timestamp  = now.strftime("%H%M%S")
    msg_id     = uuid.uuid4().hex[:8]

    return silver_trade_key(stock_code, date, hour, timestamp, msg_id)


# 4-2. Silver trade 저장 함수
# records = silver_records
def write_silver_trade_records(s3_client, bucket: str, records: list[dict]) -> str:
    """
    정제된 체결 records를 S3 Silver trade 경로에 저장한다.

    MVP 권장:
    - 처음에는 Bronze object 1개당 Silver file 1개로 저장한다.
    - path_config.py의 silver_trade_key()를 사용한다.

    Parquet 사용 시:
    - pandas
    - pyarrow
    두 라이브러리가 Lambda layer 또는 배포 패키지에 포함되어야 한다.
    """
    # TODO:
    # 1. key = build_silver_trade_object_key(records)
    # 2. records를 pandas DataFrame으로 변환한다.
    # 3. DataFrame을 parquet bytes로 변환한다.
    # 4. s3_client.put_object(Bucket=bucket, Key=key, Body=..., ContentType=...) 호출
    # 5. 성공 로그를 남긴다.
    # 6. key를 반환한다.
    key = build_silver_trade_object_key(records)

    df = pd.DataFrame(records)

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    logger.info("S3 Silver write success: s3://%s/%s", bucket, key)
    return key

# ============================================================================
# 5. 일봉 데이터 처리 (Silver daily/)
# ============================================================================
# 흐름:
#   T1) 장 시작 첫 틱  → upsert_silver_daily_record() 가
#                        기존 행이 없으면 build_open_daily_record() 로 잠정 행 생성
#   T2) 장 마감 시점   → upsert_silver_daily_record() 가
#                        기존 행이 있으면 finalize_daily_record() 로 OHLCV 확정
#   그 외 장중 틱      → daily 경로에는 아무것도 쓰지 않음 (실시간 체결 경로만)
# ============================================================================

# 5-1. 장 마감 틱 판별 함수
def is_market_close_tick(record: dict) -> bool:
    """
    체결시간(STCK_CNTG_HOUR, HHMMSS)이 15:30 이후인지 판별.
    True 면 그 틱을 기준으로 일봉을 확정한다.
    """
    hhmmss = record.get("STCK_CNTG_HOUR") or ""
    if len(hhmmss) < 4:
        return False
    return hhmmss[:4] >= "1530"


# 5-2. 영업일자 결정 함수
def resolve_business_date(record: dict) -> str:
    """
    영업일자(YYYYMMDD) 결정.
      1순위: BSOP_DATE 필드
      2순위: 현재 KST 시각
    """
    bsop = record.get("BSOP_DATE")
    if bsop and len(bsop) == 8 and str(bsop).isdigit():
        return str(bsop)
    return now_kst().strftime("%Y%m%d")


# 5-3-1. 잠정 일봉 행 생성 (T1: 장 시작 첫 틱)
def build_open_daily_record(record: dict) -> dict:
    """
    장 시작 첫 틱에서 호출.
    open 만 채워진 잠정 일봉 행을 만든다.
    high/low/close/volume 은 장 마감 시점에 finalize_daily_record() 로 채운다.
    """
    return {
        "stock_code": record["MKSC_SHRN_ISCD"],
        "date":       resolve_business_date(record),
        "open":       record["STCK_OPRC"],
        "high":       None,
        "low":        None,
        "close":      None,
        "volume":     None,
    }


# 5-3-2. 일봉 행 확정 (T2: 장 마감 시점)
def finalize_daily_record(open_row: dict, last_record: dict) -> dict:
    """
    장 마감 틱 도달 시 호출.
    잠정 행(open 만 채워진 상태)에 KIS 누적값(high/low/volume) 과
    마지막 체결가(close) 를 채워 확정 OHLCV 행으로 만든다.
    """
    return {
        **open_row,
        "high":   last_record["STCK_HGPR"],   # KIS 누적 최고가
        "low":    last_record["STCK_LWPR"],   # KIS 누적 최저가
        "close":  last_record["STCK_PRPR"],   # 마지막 체결가 = 종가
        "volume": last_record["ACML_VOL"],    # KIS 누적 거래량
    }


# 5-4. Silver daily S3 key 생성 함수
def build_silver_daily_object_key(daily_row: dict) -> str:
    """
    processed/daily/{stock_code}/date={YYYYMMDD}/{stock_code}_daily.parquet
    """
    stock_code = daily_row["stock_code"]
    date       = daily_row["date"]
    return silver_daily_key(stock_code, date)


# 5-5. Silver daily 행 읽기 함수 (기존 잠정 행 조회용)
def read_silver_daily_record(s3_client, bucket: str, stock_code: str, date: str) -> dict | None:
    """
    오늘자 daily 행이 S3 에 이미 있으면 dict 로 반환, 없으면 None.
    같은 영업일에 잠정 행이 이미 적재됐는지 확인하는 용도.
    """
    key = silver_daily_key(stock_code, date)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except s3_client.exceptions.NoSuchKey:
        return None

    buffer = io.BytesIO(response["Body"].read())
    df = pd.read_parquet(buffer, engine="pyarrow")
    if df.empty:
        return None

    # daily Parquet 은 종목당 하루 1행이므로 첫 행만 반환
    return df.iloc[0].to_dict()


# 5-6. Silver daily 저장 함수
def write_silver_daily_record(s3_client, bucket: str, daily_row: dict) -> str:
    """
    잠정 행 또는 확정 행을 daily Parquet 로 덮어쓴다.
    하루에 종목당 최대 2회 호출됨 (T1 생성, T2 확정).
    """
    key = build_silver_daily_object_key(daily_row)

    df = pd.DataFrame([daily_row])

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    logger.info("S3 Silver daily write success: s3://%s/%s", bucket, key)
    return key


# 5-7. Silver daily upsert (라우팅) 함수
def upsert_silver_daily_record(s3_client, bucket: str, record: dict) -> str | None:
    """
    틱 한 건을 받아 daily 경로에 적절히 반영한다.

      - 오늘자 daily 행이 없음          → 잠정 행 생성 (T1)
      - 있음 + 마감 틱 + 아직 미확정    → 확정 행으로 갱신 (T2)
      - 그 외 (장중 일반 틱)            → 아무것도 하지 않음

    Returns:
        쓰기를 수행한 경우 S3 key, 아니면 None.
    """
    stock_code = record["MKSC_SHRN_ISCD"]
    date       = resolve_business_date(record)

    existing = read_silver_daily_record(s3_client, bucket, stock_code, date)

    # T1: 오늘 첫 틱 → 잠정 행 생성
    if existing is None:
        daily_row = build_open_daily_record(record)
        return write_silver_daily_record(s3_client, bucket, daily_row)

    # T2: 마감 틱 도달 & 아직 확정 전 → OHLCV 채워서 확정
    if is_market_close_tick(record) and existing.get("close") is None:
        daily_row = finalize_daily_record(existing, record)
        return write_silver_daily_record(s3_client, bucket, daily_row)

    # 그 외 장중 틱 → daily 경로에는 쓰지 않음
    return None

# 6. Lambda handler legacy draft
def _lambda_handler_legacy_draft(event, context):
    """
    Lambda 진입점이다.

    전체 흐름:
    - event에서 S3 bucket/key를 추출한다.
    - Bronze object를 읽는다.
    - raw 문자열을 Silver records로 파싱한다.
    - Silver trade 경로에 저장한다.
    - 처리 결과를 반환한다.
    """
    # TODO:
    # 1. logger.info("Clean Lambda invoked")
    # 2. s3_client = create_s3_client()
    # 3. references = extract_s3_references_from_event(event)
    # 4. references가 비어 있으면 처리 결과 0건을 반환한다.
    # 5. references를 반복한다.
    # 6. read_bronze_object() 호출
    # 7. parse_raw_trade_message() 호출
    # 8. write_silver_trade_records() 호출
    # 9. write_silver_daily_if_needed() 호출
    # 10. 처리 결과를 results에 담는다.
    # 11. {"statusCode": 200, "body": json.dumps(...)} 형태로 반환한다.

    logger.info("Clean Lambda invoked")

    s3_client = create_s3_client()
    references = extract_s3_references_from_event(event)

    if not references:
        raise ValueError("S3 object가 비어있습니다.")
    
    for reference in references:
        raw_message = read_bronze_object(s3_client, S3_BUCKET, reference)
        raw_records = parse_raw_trade_message(raw_message)

        silver_trade_records = create_silver_trade_records(raw_records)
        silver_trade_key = write_silver_trade_records(silver_trade_records)

        # 일봉 경로 적재 (신규) — 같은 메시지의 모든 틱에 대해 upsert 시도
        for record in raw_records:
            silver_daily_key = upsert_silver_daily_record(s3_client, S3_BUCKET, record)


def lambda_handler(event, context):
    """
    Lambda entrypoint for Bronze -> Silver cleaning.

    Logs each major processing step so CloudWatch can show:
    - which Bronze object was processed
    - how many trade rows were parsed/transformed
    - which Silver trade/daily objects were written
    """
    logger.info("Clean Lambda invoked: event_record_count=%s", len(event.get("Records", [])))

    s3_client = create_s3_client()
    references = extract_s3_references_from_event(event)
    logger.info("S3 references extracted: count=%s", len(references))

    if not references:
        logger.warning("No S3 references found in Lambda event")
        return {
            "statusCode": 200,
            "body": json.dumps({"processed": 0, "results": []}, ensure_ascii=False),
        }

    results = []

    for reference in references:
        bucket = reference.get("bucket") or S3_BUCKET
        key = reference["key"]

        logger.info("Processing Bronze object: s3://%s/%s", bucket, key)

        try:
            raw_message = read_bronze_object(s3_client, bucket, key)
            logger.info("Bronze object read complete: chars=%s", len(raw_message))

            raw_records = parse_raw_trade_message(raw_message)
            logger.info("Raw trade parse complete: raw_record_count=%s", len(raw_records))

            silver_trade_records = create_silver_trade_records(raw_records)
            logger.info(
                "Silver trade transform complete: silver_record_count=%s",
                len(silver_trade_records),
            )

            silver_trade_key = write_silver_trade_records(
                s3_client,
                bucket,
                silver_trade_records,
            )
            logger.info("Silver trade write complete: key=%s", silver_trade_key)

            silver_daily_keys = []
            for raw_record in raw_records:
                silver_daily_key = upsert_silver_daily_record(s3_client, bucket, raw_record)
                if silver_daily_key:
                    silver_daily_keys.append(silver_daily_key)
                    logger.info("Silver daily upsert complete: key=%s", silver_daily_key)

            if not silver_daily_keys:
                logger.info("Silver daily upsert skipped: no daily update needed")

            result = {
                "bronze_bucket": bucket,
                "bronze_key": key,
                "silver_trade_key": silver_trade_key,
                "silver_daily_keys": silver_daily_keys,
                "raw_record_count": len(raw_records),
                "silver_record_count": len(silver_trade_records),
            }
            results.append(result)

            logger.info(
                "Clean Lambda object complete: bronze_key=%s silver_trade_key=%s daily_update_count=%s",
                key,
                silver_trade_key,
                len(silver_daily_keys),
            )

        except Exception:
            logger.exception("Clean Lambda object failed: s3://%s/%s", bucket, key)
            raise

    logger.info("Clean Lambda completed: processed=%s", len(results))
    return {
        "statusCode": 200,
        "body": json.dumps({"processed": len(results), "results": results}, ensure_ascii=False),
    }
