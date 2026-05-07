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

# 61개 종가 데이터 (MA60 까지 처리 가능 + 전일 비교용 +1)
# 앞부분 낮음 → 뒷부분 높음 → 골든크로스 발생 패턴
# 전일까지 낮게 유지하다가 오늘 시가에 급등
prices = [
    50000, 50000, 50000, 50000, 50000,
    50000, 50000, 50000, 50000, 50000,
    50000, 50000, 50000, 50000, 50000,
    50000, 50000, 50000, 50000, 50000,
    50000, 50000, 50000, 50000, 50000,
    50000, 50000, 50000, 50000, 50000,
    50000, 50000, 50000, 50000, 50000,
    50000, 50000, 50000, 50000, 50000,
    50000, 50000, 50000, 50000, 50000,
    50000, 50000, 50000, 50000, 50000,
    50000, 50000, 50000, 50000, 50000,
    50000, 40000, 40000, 40000, 40000, 40000,
]

today     = datetime.now()
today_str = today.strftime("%Y%m%d")

# 오늘 이전 영업일 61개 생성
dates = []
i = len(prices) + 30  # 여유분 포함해서 검색
while len(dates) < len(prices):
    d = today - timedelta(days=i)
    if d.weekday() < 5:
        dates.append(d.strftime("%Y%m%d"))
    i -= 1

dates  = sorted(dates)
dates  = [d for d in dates if d < today_str]
prices = prices[:len(dates)]

# 과거 종가 업로드
for stock_code in WATCHLIST.keys():
    print(f"\n📌 {stock_code} ({WATCHLIST[stock_code]}) 과거 데이터 업로드 중...")

    for date, close_price in zip(dates, prices):
        row = {
            "stock_code" : stock_code,
            "date"       : date,
            "open"       : close_price - 500,
            "high"       : close_price + 500,
            "low"        : close_price - 1000,
            "close"      : close_price,
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

    print(f"  ✅ {len(dates)}개 업로드 완료")

# 오늘 시가 업로드
today_open_price = 90000  # 상승 추세 유지

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

print("\n🎉 모든 테스트 데이터 업로드 완료!")