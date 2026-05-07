# scripts/debug_ma.py
import io
import os
import sys
import boto3
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")
S3_BUCKET  = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-3")

s3 = boto3.client("s3", region_name=AWS_REGION)

STOCK_CODE = "005930"
MA_SHORT   = 5
MA_LONG    = 20

# S3 에서 모든 daily 파일 읽기
prefix = f"processed/daily/{STOCK_CODE}/"
paginator = s3.get_paginator("list_objects_v2")
rows = []

for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if not key.endswith(".parquet"):
            continue
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        buffer   = io.BytesIO(response["Body"].read())
        df       = pd.read_parquet(buffer)
        if not df.empty:
            rows.append(df.iloc[0].to_dict())

# 날짜 정렬
df_all = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
print(f"\n총 {len(df_all)}개 데이터")
print(df_all[["date", "open", "close"]].to_string())

# price_for_ma 컬럼 생성
today_str = df_all["date"].max()
df_all["price_for_ma"] = df_all.apply(
    lambda row: row["open"] if str(row["date"]) == today_str else row["close"],
    axis=1
)

# MA 계산
df_all["ma_short"] = df_all["price_for_ma"].rolling(MA_SHORT).mean()
df_all["ma_long"]  = df_all["price_for_ma"].rolling(MA_LONG).mean()

print(f"\n--- MA 계산 결과 ---")
print(df_all[["date", "price_for_ma", "ma_short", "ma_long"]].tail(5).to_string())

# 크로스오버 확인
prev  = df_all.iloc[-2]
today = df_all.iloc[-1]

print(f"\n--- 크로스오버 확인 ---")
print(f"전일 MA{MA_SHORT}: {prev['ma_short']:.0f}  MA{MA_LONG}: {prev['ma_long']:.0f}")
print(f"오늘 MA{MA_SHORT}: {today['ma_short']:.0f}  MA{MA_LONG}: {today['ma_long']:.0f}")

if prev["ma_short"] < prev["ma_long"] and today["ma_short"] > today["ma_long"]:
    print("✅ 골든크로스 발생!")
elif prev["ma_short"] > prev["ma_long"] and today["ma_short"] < today["ma_long"]:
    print("✅ 데드크로스 발생!")
else:
    print("❌ 크로스오버 없음")