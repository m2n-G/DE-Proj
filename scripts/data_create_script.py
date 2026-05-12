import io
import os
import sys
import boto3
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from config.signal_config import WATCHLIST

load_dotenv(PROJECT_ROOT / ".env")
S3_BUCKET  = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-3")

if not S3_BUCKET:
    raise ValueError("S3_BUCKET 이 .env 에 없습니다.")

s3 = boto3.client("s3", region_name=AWS_REGION)

# 5/9 ~ 5/12 추가 더미 데이터
# 골든크로스 이후 데드크로스가 발생하도록 설계
# 5/9 ~ 5/12: 급락 → 데드크로스 발생

additional_dates_prices = {
    "20260509": {"close": 55000, "open": 56000},  # 급락 시작
    "20260510": {"close": 40000, "open": 41000},  # 계속 하락
    "20260511": {"close": 35000, "open": 36000},  # 계속 하락
    "20260512": {"close": 30000, "open": 31000},  # 데드크로스 발생
}

for stock_code in WATCHLIST.keys():
    print(f"\n📌 {stock_code} ({WATCHLIST[stock_code]}) 추가 데이터 업로드 중...")

    for date, price_info in additional_dates_prices.items():
        # 과거 종가 데이터 (확정)
        row = {
            "stock_code" : stock_code,
            "date"       : date,
            "open"       : price_info["open"],
            "high"       : price_info["open"] + 500,
            "low"        : price_info["close"] - 1000,
            "close"      : price_info["close"],
            "volume"     : 10000000,
        }
        df     = pd.DataFrame([row])
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)

        key = f"processed/daily/{stock_code}/date={date}/{stock_code}_daily.parquet"
        s3.put_object(
            Bucket      = S3_BUCKET,
            Key         = key,
            Body        = buffer.getvalue(),
            ContentType = "application/octet-stream",
        )
        print(f"  ✅ {date} 업로드 완료 (close={price_info['close']:,})")

# 오늘(5/12) 시가도 업로드 (데드크로스 유발)
today_str        = "20260512"
today_open_price = 28000  # 급락 → 단기MA < 장기MA → 데드크로스

print(f"\n📌 오늘 시가 데이터 업로드 중... ({today_str})")

for stock_code in WATCHLIST.keys():
    row = {
        "stock_code" : stock_code,
        "date"       : today_str,
        "open"       : today_open_price,
        "high"       : None,
        "low"        : None,
        "close"      : None,
        "volume"     : None,
    }
    df     = pd.DataFrame([row])
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    key = f"processed/daily/{stock_code}/date={today_str}/{stock_code}_daily.parquet"
    s3.put_object(
        Bucket      = S3_BUCKET,
        Key         = key,
        Body        = buffer.getvalue(),
        ContentType = "application/octet-stream",
    )
    print(f"  ✅ {today_str} 시가 업로드 완료: {stock_code}")

print("\n🎉 추가 테스트 데이터 업로드 완료!")