# ===============================
# lambda_signal.handler
# Silver daily to Gold signal Lambda.
# MA 계산 및 크로스 감지
# ===============================

"""
1. 환경변수 로드
   └─ S3_BUCKET · SNS_ARN · LOG_LEVEL 읽기

2. S3 · SNS 클라이언트 초기화
   └─ boto3.client("s3") 생성
   └─ boto3.client("sns") 생성

3. 설정 로드 함수
   └─ signal_config.py 에서 BEST_MA · WATCHLIST 읽기
   └─ S3 config/ 경로에서 오늘 날짜 best_ma_config.json 읽기
   └─ 없으면 signal_config.py 기본값 사용

4. Silver daily 데이터 로드 함수
   └─ S3 daily/ 에서 이전 (MA_LONG - 1)일치 확정 종가 로드
   └─ 오늘 시가(open_price) 포함
   └─ pandas DataFrame 으로 반환

5. MA 계산 함수
   └─ [이전 N-1일 확정 종가] + [오늘 시가] = N개 값
   └─ pandas rolling(MA_SHORT).mean() → 단기 MA
   └─ pandas rolling(MA_LONG).mean()  → 장기 MA

   예시) MA5 계산
   ┌──────────────┬──────────────────────┐
   │ 2024-01-11   │ 종가 72,000 (확정)   │
   │ 2024-01-12   │ 종가 73,500 (확정)   │
   │ 2024-01-13   │ 종가 74,200 (확정)   │
   │ 2024-01-14   │ 종가 75,300 (확정)   │
   │ 2024-01-15   │ 시가 75,800 (고정) ★ │
   └──────────────┴──────────────────────┘
   → MA5 = 74,160

6. 크로스오버 감지 함수
   └─ 전일 단기MA < 장기MA & 오늘 단기MA > 장기MA → golden_cross
   └─ 전일 단기MA > 장기MA & 오늘 단기MA < 장기MA → dead_cross
   └─ 그 외 → None

7. 중복 방지 함수
   └─ S3 Gold signal/ 에서 오늘 발송 이력 조회
   └─ 이미 발송한 종목·신호 조합이면 True 반환 (skip)
   └─ 없으면 False 반환 (발송)

8. SNS 발행 함수
   └─ 골든크로스 · 데드크로스 메시지 포맷 생성
   └─ SNS Publish 호출
   └─ Slack 알림 메시지 형식:
      🟢 [골든크로스 감지] 삼성전자 (005930)
      오늘 시가  : 75,800원
      MA조합    : MA5/20
      단기MA    : 74,160 > 장기MA : 73,890
      감지 시각 : 09:05
      → 매수 신호 발생

9. Gold 시그널 저장 함수
   └─ 크로스오버 결과를 S3 Gold signal/ 에 Parquet 으로 저장
   └─ path_config.py 의 gold_signal_key() 활용
   └─ date · stock_code · open · MA_S · MA_L · signal · ma_combo 컬럼

10. lambda_handler (Lambda 진입점)
    └─ def lambda_handler(event, context)
    └─ WATCHLIST 종목 순회
    └─ 종목별로 3~9번 함수 순서대로 호출
    └─ 크로스 발생 & 미발송 → SNS 발행 + Gold 저장
    └─ 크로스 없음 or 중복  → Gold 저장만

11. 로깅
    └─ 종목별 MA 계산 결과 로그
    └─ 크로스오버 감지 · 미감지 로그
    └─ SNS 발행 성공 · 실패 로그
"""

# 0. 필요한 라이브러리 import
import io
import os
import json
import uuid
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from src.common.time_utils import now_kst
from config.path_config import config_key, silver_daily_key, gold_signal_key
from config.signal_config import WATCHLIST, MA_CONFIG, DEFAULT_MA_SHORT, DEFAULT_MA_LONG


# 1. 환경변수 로드
load_dotenv(".env")

S3_BUCKET = os.getenv("S3_BUCKET")
SNS_ARN = os.getenv("SNS_ARN")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-3")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# 11. logger 생성
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(levelname)s:%(name)s:%(message)s",
)

logger = logging.getLogger(__name__)

def load_runtime_config() -> dict:
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET is required")

    return {
        "bucket": S3_BUCKET,
        "sns_arn": SNS_ARN,
        "log_level": LOG_LEVEL,
    }

def _read_parquet_from_s3(s3_client, bucket: str, key: str) -> pd.DataFrame:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        buffer   = io.BytesIO(response["Body"].read())
        return pd.read_parquet(buffer, engine="pyarrow")
    except Exception:
        logger.exception("Failed to read parquet: s3://%s/%s", bucket, key)
        raise


def _list_s3_keys(s3_client, bucket: str, prefix: str) -> list[str]:
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys


def _extract_date_from_key(key: str) -> str | None:
    for part in key.split("/"):
        if part.startswith("date="):
            return part.replace("date=", "", 1)
    return None


def _is_missing_s3_key_error(exc: ClientError) -> bool:
    error_code = exc.response.get("Error", {}).get("Code")
    return error_code in ("NoSuchKey", "404", "NotFound")

# 2-1. S3 client 생성 함수
def create_s3_client():
    """
    boto3 S3 client를 생성한다.
    """
    # TODO:
    # 1. boto3.client("s3") 호출
    # 2. client 반환
    return boto3.client("s3", region_name=AWS_REGION)


# 2-2. SNS client 생성 함수
def create_sns_client():
    """
    boto3 SNS client를 생성한다.
    """
    # TODO:
    # 1. boto3.client("sns") 호출
    # 2. client 반환
    return boto3.client("sns", region_name=AWS_REGION)


# 3. Signal 설정 로드 함수
def load_signal_config(s3_client, bucket: str, date: str) -> dict:
    """
    MA 설정을 로드한다.

    우선순위:
    1. S3 config/date=YYYYMMDD/best_ma_config.json
    2. 없으면 config.signal_config.py의 기본값 사용

    S3 설정 파일 예:
    {
        "005930": {"short": 5, "long": 20},
        "000660": {"short": 20, "long": 60}
    }
    """
    # TODO:
    # 1. key = config_key(date)
    # 2. s3_client.get_object(Bucket=bucket, Key=key) 시도
    # 3. 있으면 JSON 파싱 후 반환
    # 4. NoSuchKey 또는 404면 signal_config.py 기본 설정 반환
    # 5. 그 외 S3 오류는 raise
    key = config_key(date)

    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        if not response:
            raise  ValueError("s3가 비워져있습니다.")
        else:
            silver_obj = response["Body"].read()
            silver_daily_text = silver_obj.decode("utf-8")
            config = json.loads(silver_daily_text)

            return config
    except Exception:
        logger.warning("config 로드 실패 · 기본값 사용: date=%s", date)
        return MA_CONFIG


# 10-1-1. 종목별 MA 조합 선택 함수
def get_ma_pair(stock_code: str, ma_config: dict) -> dict:
    """
    종목별 MA 조합을 반환한다.

    반환 예:
    {"short": 5, "long": 20}
    """
    # TODO:
    # 1. ma_config에서 stock_code 설정 조회
    # 2. 없으면 DEFAULT_MA_SHORT / DEFAULT_MA_LONG 사용
    # 3. {"short": ..., "long": ...} 반환
    default_pair = {
        "short": DEFAULT_MA_SHORT,
        "long": DEFAULT_MA_LONG,
    }

    pair = ma_config.get(stock_code, default_pair)

    return {
        "short": int(pair.get("short", DEFAULT_MA_SHORT)),
        "long": int(pair.get("long", DEFAULT_MA_LONG)),
    }


# 4. Silver daily 데이터 로드 함수
def load_silver_daily_data(
    s3_client,
    bucket: str,
    stock_code: str,
    ma_long: int,
    date: str,
):
    """
    MA 계산에 필요한 Silver daily 데이터를 로드한다.

    필요한 데이터:
    - 이전 (MA_LONG - 1)일치 확정 종가
    - 오늘 시가(open)

    반환 DataFrame 예:
    date        price_for_ma    source
    20260501    72000          close
    20260502    73500          close
    20260503    74200          close
    20260504    75300          close
    20260505    75800          open
    """
    # TODO:
    # 1. processed/daily/{stock_code}/ prefix 목록 조회
    # 2. 오늘 날짜보다 이전 daily parquet만 읽기
    # 3. close 값이 있는 확정 일봉만 필터링
    # 4. 최신 (ma_long - 1)개 row 선택
    # 5. silver_daily_key(stock_code, date)로 오늘 daily parquet 읽기
    # 6. 오늘 open 값이 없으면 skip 가능한 예외 또는 None 반환
    # 7. 이전 종가 + 오늘 시가를 price_for_ma 컬럼으로 구성
    # 8. DataFrame 반환
    previous_rows = []
    prefix = f"processed/daily/{stock_code}/"
    today_key = silver_daily_key(stock_code, date)

    for key in _list_s3_keys(s3_client, bucket, prefix):
        if not key.endswith(".parquet"):
            continue
        if key == today_key:
            continue

        object_date = _extract_date_from_key(key)
        if not object_date or object_date >= date:
            continue

        try:
            df = _read_parquet_from_s3(s3_client, bucket, key)
        except Exception:
            logger.exception("Failed to read previous daily parquet: s3://%s/%s", bucket, key)
            raise

        if df.empty or "close" not in df.columns:
            continue

        row = df.iloc[0].to_dict()
        close_price = row.get("close")
        if pd.isna(close_price):
            continue

        previous_rows.append(
            {
                "date": str(row.get("date") or object_date),
                "stock_code": stock_code,
                "price_for_ma": float(close_price),
                "source": "close",
            }
        )

    previous_rows = sorted(previous_rows, key=lambda row: row["date"])
    # 전일 MA와 오늘 MA를 모두 비교하려면 ma_long개 이전 종가 + 오늘 시가가 필요하다.
    previous_rows = previous_rows[-ma_long:]

    try:
        today_df = _read_parquet_from_s3(s3_client, bucket, today_key)
    except ClientError as exc:
        if _is_missing_s3_key_error(exc):
            logger.info("Today's daily parquet not found: s3://%s/%s", bucket, today_key)
            return pd.DataFrame(columns=["date", "stock_code", "price_for_ma", "source"])
        raise

    if today_df.empty or "open" not in today_df.columns:
        logger.info("Today's daily parquet has no open row: s3://%s/%s", bucket, today_key)
        return pd.DataFrame(columns=["date", "stock_code", "price_for_ma", "source"])

    today_row = today_df.iloc[0].to_dict()
    today_open = today_row.get("open")
    if pd.isna(today_open):
        logger.info("Today's open price is missing: stock_code=%s date=%s", stock_code, date)
        return pd.DataFrame(columns=["date", "stock_code", "price_for_ma", "source"])

    rows = previous_rows + [
        {
            "date": str(today_row.get("date") or date),
            "stock_code": stock_code,
            "price_for_ma": float(today_open),
            "source": "open",
        }
    ]

    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


# 5. MA 계산 함수
def calculate_moving_averages(daily_df, ma_short: int, ma_long: int) -> dict | None:
    """
    이동평균을 계산한다.

    계산 규칙:
    - [이전 N-1일 확정 종가] + [오늘 시가] = N개 값
    - rolling(MA_SHORT).mean()으로 단기 MA 계산
    - rolling(MA_LONG).mean()으로 장기 MA 계산

    반환 예:
    {
        "prev_short_ma": 73500.0,
        "prev_long_ma": 73100.0,
        "today_short_ma": 74160.0,
        "today_long_ma": 73890.0,
        "today_open": 75800,
    }
    """
    # TODO:
    # 1. daily_df row 수가 ma_long보다 작으면 None 반환
    # 2. price_for_ma 컬럼으로 rolling MA 계산
    # 3. 전일 short/long MA 추출
    # 4. 오늘 short/long MA 추출
    # 5. 오늘 open 값 추출
    # 6. dict 반환
    if daily_df is None or daily_df.empty:
        return None

    if "price_for_ma" not in daily_df.columns:
        raise ValueError("daily_df must contain price_for_ma column")

    df = daily_df.copy().sort_values("date").reset_index(drop=True)
    df["price_for_ma"] = pd.to_numeric(df["price_for_ma"], errors="coerce")
    df = df.dropna(subset=["price_for_ma"]).reset_index(drop=True)

    if len(df) < ma_long + 1:
        logger.info(
            "Not enough daily data for crossover MA: required=%s actual=%s",
            ma_long + 1,
            len(df),
        )
        return None

    prices = df["price_for_ma"]
    short_ma = prices.rolling(ma_short).mean()
    long_ma = prices.rolling(ma_long).mean()

    values = {
        "prev_short_ma": short_ma.iloc[-2],
        "prev_long_ma": long_ma.iloc[-2],
        "today_short_ma": short_ma.iloc[-1],
        "today_long_ma": long_ma.iloc[-1],
        "today_open": prices.iloc[-1],
    }

    if any(pd.isna(value) for value in values.values()):
        logger.info("MA calculation skipped because one or more MA values are NaN")
        return None

    return {
        "prev_short_ma": float(values["prev_short_ma"]),
        "prev_long_ma": float(values["prev_long_ma"]),
        "today_short_ma": float(values["today_short_ma"]),
        "today_long_ma": float(values["today_long_ma"]),
        "today_open": float(values["today_open"]),
    }


# 6. 크로스오버 감지 함수
def detect_crossover(ma_result: dict) -> str | None:
    """
    골든크로스/데드크로스를 감지한다.

    golden_cross:
    - 전일 단기MA < 장기MA
    - 오늘 단기MA > 장기MA

    dead_cross:
    - 전일 단기MA > 장기MA
    - 오늘 단기MA < 장기MA

    그 외:
    - None
    """
    # TODO:
    # 1. prev_short_ma, prev_long_ma 꺼내기
    # 2. today_short_ma, today_long_ma 꺼내기
    # 3. golden_cross 조건 확인
    # 4. dead_cross 조건 확인
    # 5. 해당 없으면 None 반환
    if not ma_result:
        return None

    prev_short = ma_result["prev_short_ma"]
    prev_long = ma_result["prev_long_ma"]
    today_short = ma_result["today_short_ma"]
    today_long = ma_result["today_long_ma"]

    if prev_short < prev_long and today_short > today_long:
        return "golden_cross"

    if prev_short > prev_long and today_short < today_long:
        return "dead_cross"

    return None


# 7. 중복 알림 방지 함수
def signal_already_sent(
    s3_client,
    bucket: str,
    stock_code: str,
    date: str,
    signal: str,
    ma_combo: str,
) -> bool:
    """
    오늘 이미 같은 종목/신호/MA조합으로 알림을 보냈는지 확인한다.

    확인 대상:
    - S3 Gold signal/ 경로
    - signal/{stock_code}/date={YYYYMMDD}/

    반환:
    - 이미 보냈으면 True
    - 아직 안 보냈으면 False
    """
    # TODO:
    # 1. signal/{stock_code}/date={date}/ prefix 조회
    # 2. 기존 parquet 파일들이 있으면 읽기
    # 3. stock_code + signal + ma_combo가 같은 row가 있는지 확인
    # 4. 있으면 True, 없으면 False 반환
    prefix = f"signal/{stock_code}/date={date}/"

    for key in _list_s3_keys(s3_client, bucket, prefix):
        if not key.endswith(".parquet"):
            continue

        try:
            df = _read_parquet_from_s3(s3_client, bucket, key)
        except Exception:
            logger.exception("Failed to read signal history parquet: s3://%s/%s", bucket, key)
            raise

        if df.empty:
            continue

        required_columns = {"stock_code", "signal", "ma_combo"}
        if not required_columns.issubset(df.columns):
            continue

        matched = df[
            (df["stock_code"].astype(str) == stock_code)
            & (df["signal"].astype(str) == signal)
            & (df["ma_combo"].astype(str) == ma_combo)
        ]

        if "notified" in matched.columns:
            matched = matched[matched["notified"] == True]

        if not matched.empty:
            return True

    return False


# 8-1. SNS 메시지 포맷 생성 함수
def format_signal_message(
    stock_code: str,
    stock_name: str,
    signal: str,
    ma_result: dict,
    ma_combo: str,
    detected_at,
) -> str:
    """
    Slack으로 전달될 알림 메시지를 만든다.

    메시지 예:
    [골든크로스 감지] 삼성전자 (005930)
    오늘 시가  : 75,800원
    MA조합    : MA5/20
    단기MA    : 74,160 > 장기MA : 73,890
    감지 시각 : 09:05
    -> 매수 신호 발생
    """
    # TODO:
    # 1. signal 값에 따라 제목 결정
    # 2. stock_name, stock_code 포함
    # 3. today_open, short_ma, long_ma 포함
    # 4. detected_at을 HH:MM 형식으로 포함
    # 5. 메시지 문자열 반환
    signal_meta = {
        "golden_cross": {
            "title": "[Golden Cross Detected]",
            "action": "Buy signal generated",
            "direction": ">",
        },
        "dead_cross": {
            "title": "[Dead Cross Detected]",
            "action": "Sell signal generated",
            "direction": "<",
        },
    }
    meta = signal_meta.get(
        signal,
        {
            "title": "[Signal Detected]",
            "action": "Signal generated",
            "direction": "",
        },
    )

    return (
        f"{meta['title']} {stock_name} ({stock_code})\n"
        f"Today open : {ma_result['today_open']:,.0f}\n"
        f"MA combo   : {ma_combo}\n"
        f"Short MA   : {ma_result['today_short_ma']:,.2f} {meta['direction']} "
        f"Long MA : {ma_result['today_long_ma']:,.2f}\n"
        f"Detected   : {detected_at.strftime('%H:%M')}\n"
        f"-> {meta['action']}"
    )


# 8. SNS 발행 함수
def publish_signal_notification(
    sns_client,
    sns_arn: str,
    stock_code: str,
    stock_name: str,
    signal: str,
    ma_result: dict,
    ma_combo: str,
    detected_at,
) -> dict:
    """
    SNS Topic으로 신호 알림을 발행한다.
    """
    # TODO:
    # 1. SNS_ARN 존재 확인
    # 2. format_signal_message() 호출
    # 3. sns_client.publish(...) 호출
    # 4. 성공 로그 남기기
    # 5. publish response 반환
    if not sns_arn:
        raise ValueError("SNS_ARN is required to publish signal notifications")

    message = format_signal_message(
        stock_code=stock_code,
        stock_name=stock_name,
        signal=signal,
        ma_result=ma_result,
        ma_combo=ma_combo,
        detected_at=detected_at,
    )

    response = sns_client.publish(
        TopicArn=sns_arn,
        Subject=f"{signal}: {stock_code}",
        Message=message,
    )
    logger.info("SNS publish success: stock_code=%s signal=%s", stock_code, signal)
    return response


# 9-1. Gold signal record 생성 함수
def build_signal_record(
    stock_code: str,
    stock_name: str,
    date: str,
    signal: str | None,
    ma_short: int,
    ma_long: int,
    ma_result: dict | None,
    notified: bool,
    detected_at,
) -> dict:
    """
    S3 Gold signal/에 저장할 record를 만든다.

    포함 컬럼:
    - date
    - stock_code
    - stock_name
    - open
    - ma_short
    - ma_long
    - short_ma
    - long_ma
    - signal
    - ma_combo
    - notified
    - detected_at
    """
    # TODO:
    # 1. ma_combo = f"MA{ma_short}/{ma_long}" 생성
    # 2. ma_result가 None이면 MA 관련 값은 None 처리
    # 3. dict 반환
    ma_combo = f"MA{ma_short}/{ma_long}"

    return {
        "date": date,
        "stock_code": stock_code,
        "stock_name": stock_name,
        "open": None if ma_result is None else ma_result["today_open"],
        "ma_short": ma_short,
        "ma_long": ma_long,
        "short_ma": None if ma_result is None else ma_result["today_short_ma"],
        "long_ma": None if ma_result is None else ma_result["today_long_ma"],
        "signal": signal,
        "ma_combo": ma_combo,
        "notified": notified,
        "detected_at": detected_at.isoformat(),
    }


# 9. Gold signal 저장 함수
def write_gold_signal_record(s3_client, bucket: str, record: dict) -> str:
    """
    크로스오버 결과를 S3 Gold signal/ 경로에 Parquet으로 저장한다.

    S3 key:
    - path_config.gold_signal_key(stock_code, date, timestamp, msg_id)
    """
    # TODO:
    # 1. timestamp = detected_at 기준 HHMMSS 생성
    # 2. msg_id = uuid.uuid4().hex[:8]
    # 3. key = gold_signal_key(...)
    # 4. pd.DataFrame([record]) 생성
    # 5. io.BytesIO()에 parquet 저장
    # 6. s3_client.put_object(...) 호출
    # 7. 성공 로그
    # 8. key 반환
    detected_at = datetime.fromisoformat(record["detected_at"])
    timestamp = detected_at.strftime("%H%M%S")
    msg_id = uuid.uuid4().hex[:8]
    key = gold_signal_key(record["stock_code"], record["date"], timestamp, msg_id)

    df = pd.DataFrame([record])
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    logger.info("S3 Gold signal write success: s3://%s/%s", bucket, key)
    return key


# 10-1. 종목 1개 처리 함수
def process_one_stock(
    s3_client,
    sns_client,
    bucket: str,
    sns_arn: str,
    stock_code: str,
    stock_name: str,
    ma_config: dict,
    date: str,
    detected_at,
) -> dict:
    """
    WATCHLIST의 종목 1개에 대해 신호 감지 흐름을 수행한다.

    처리 순서:
    1. MA 조합 조회
    2. Silver daily 데이터 로드
    3. MA 계산
    4. 크로스오버 감지
    5. 중복 알림 여부 확인
    6. 필요 시 SNS 발행
    7. Gold signal 저장
    8. 처리 결과 dict 반환
    """
    # TODO:
    # 1. ma_pair = get_ma_pair(...)
    # 2. daily_df = load_silver_daily_data(...)
    # 3. ma_result = calculate_moving_averages(...)
    # 4. 데이터 부족 또는 오늘 시가 없음이면 skip 결과 반환
    # 5. signal = detect_crossover(ma_result)
    # 6. signal이 있으면 signal_already_sent(...) 확인
    # 7. 미발송이면 publish_signal_notification(...) 호출
    # 8. build_signal_record(...) 호출
    # 9. write_gold_signal_record(...) 호출
    # 10. 결과 dict 반환
    ma_pair = get_ma_pair(stock_code, ma_config)
    ma_short = int(ma_pair["short"])
    ma_long = int(ma_pair["long"])
    ma_combo = f"MA{ma_short}/{ma_long}"

    daily_df = load_silver_daily_data(
        s3_client=s3_client,
        bucket=bucket,
        stock_code=stock_code,
        ma_long=ma_long,
        date=date,
    )

    ma_result = calculate_moving_averages(daily_df, ma_short, ma_long)
    if ma_result is None:
        logger.info("Signal skipped: stock_code=%s reason=not_enough_daily_data", stock_code)
        return {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "status": "skipped",
            "reason": "not_enough_daily_data_or_today_open_missing",
            "ma_combo": ma_combo,
        }

    signal = detect_crossover(ma_result)
    logger.info(
        "MA calculated: stock_code=%s ma_combo=%s short=%.2f long=%.2f signal=%s",
        stock_code,
        ma_combo,
        ma_result["today_short_ma"],
        ma_result["today_long_ma"],
        signal,
    )

    notified = False
    duplicate = False

    if signal:
        duplicate = signal_already_sent(
            s3_client=s3_client,
            bucket=bucket,
            stock_code=stock_code,
            date=date,
            signal=signal,
            ma_combo=ma_combo,
        )

        if duplicate:
            logger.info(
                "Signal notification skipped as duplicate: stock_code=%s signal=%s ma_combo=%s",
                stock_code,
                signal,
                ma_combo,
            )
        else:
            publish_signal_notification(
                sns_client=sns_client,
                sns_arn=sns_arn,
                stock_code=stock_code,
                stock_name=stock_name,
                signal=signal,
                ma_result=ma_result,
                ma_combo=ma_combo,
                detected_at=detected_at,
            )
            notified = True
    else:
        logger.info("No crossover detected: stock_code=%s ma_combo=%s", stock_code, ma_combo)

    record = build_signal_record(
        stock_code=stock_code,
        stock_name=stock_name,
        date=date,
        signal=signal,
        ma_short=ma_short,
        ma_long=ma_long,
        ma_result=ma_result,
        notified=notified,
        detected_at=detected_at,
    )
    gold_key = write_gold_signal_record(s3_client, bucket, record)

    return {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "status": "processed",
        "signal": signal,
        "notified": notified,
        "duplicate": duplicate,
        "ma_combo": ma_combo,
        "gold_key": gold_key,
    }


# 10. Lambda 진입점
if False:
    """
    Lambda entrypoint.

    전체 흐름:
    - 런타임 설정 로드
    - S3/SNS client 생성
    - 오늘 날짜 기준 signal config 로드
    - WATCHLIST 종목 순회
    - 종목별 신호 감지 처리
    - 결과 반환
    """
    # TODO:
    # 1. logger.info("Signal Lambda invoked")
    # 2. detected_at = now_kst()
    # 3. date = detected_at.strftime("%Y%m%d")
    # 4. runtime = load_runtime_config()
    # 5. s3_client, sns_client 생성
    # 6. ma_config = load_signal_config(...)
    # 7. results = []
    # 8. for stock_code, stock_name in WATCHLIST.items():
    # 9. process_one_stock(...) 호출
    # 10. 결과를 results에 append
    # 11. 종목별 실패는 logger.exception 후 정책에 따라 계속/중단 결정
    # 12. {"statusCode": 200, "body": json.dumps(...)} 반환
    pass


def lambda_handler(event, context):
    logger.info("Signal Lambda invoked")

    detected_at = now_kst()
    date = detected_at.strftime("%Y%m%d")

    runtime = load_runtime_config()
    bucket = runtime["bucket"]
    sns_arn = runtime["sns_arn"]

    s3_client = create_s3_client()
    sns_client = create_sns_client()

    ma_config = load_signal_config(s3_client, bucket, date)
    results = []

    for stock_code, stock_name in WATCHLIST.items():
        try:
            result = process_one_stock(
                s3_client=s3_client,
                sns_client=sns_client,
                bucket=bucket,
                sns_arn=sns_arn,
                stock_code=stock_code,
                stock_name=stock_name,
                ma_config=ma_config,
                date=date,
                detected_at=detected_at,
            )
            results.append(result)
        except Exception:
            logger.exception("Signal processing failed: stock_code=%s", stock_code)
            raise

    logger.info("Signal Lambda completed: processed=%s", len(results))
    return {
        "statusCode": 200,
        "body": json.dumps({"processed": len(results), "results": results}, ensure_ascii=False),
    }
