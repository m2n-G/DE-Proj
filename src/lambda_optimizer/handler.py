"""MA parameter optimizer Lambda.

This Lambda reads Silver daily Parquet files, evaluates candidate moving-average
pairs per stock, and writes the selected configuration to:

    config/date=YYYYMMDD/best_ma_config.json

The JSON is intentionally compatible with lambda_signal.load_signal_config():

    {
        "005930": {"short": 5, "long": 20, "sharpe": 1.23},
        "_metadata": {...}
    }
"""

import io
import json
import logging
import math
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from config import signal_config
from config.path_config import config_key
from src.common.time_utils import now_kst


S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-3")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

WATCHLIST = signal_config.WATCHLIST
MA_CONFIG = signal_config.MA_CONFIG
DEFAULT_MA_SHORT = signal_config.DEFAULT_MA_SHORT
DEFAULT_MA_LONG = signal_config.DEFAULT_MA_LONG
TRADE_COST = signal_config.TRADE_COST
SHARPE_MIN = signal_config.SHARPE_MIN

DEFAULT_OPTIMIZE_DAYS = getattr(signal_config, "OPTIMIZE_DAYS", 365)
DEFAULT_MA_CANDIDATES = getattr(
    signal_config,
    "MA_CANDIDATES",
    [
        (5, 20),
        (10, 30),
        (20, 60),
    ],
)


logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger(__name__)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def load_runtime_config() -> dict:
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET is required")

    return {
        "bucket": S3_BUCKET,
        "region": AWS_REGION,
        "log_level": LOG_LEVEL,
    }


def create_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def _is_missing_s3_key_error(exc: ClientError) -> bool:
    error_code = exc.response.get("Error", {}).get("Code")
    return error_code in ("NoSuchKey", "404", "NotFound")


def _extract_date_from_key(key: str) -> str | None:
    for part in key.split("/"):
        if part.startswith("date="):
            return part.replace("date=", "", 1)
    return None


def _list_s3_keys(s3_client, bucket: str, prefix: str) -> list[str]:
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys


def _read_parquet_from_s3(s3_client, bucket: str, key: str) -> pd.DataFrame:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if _is_missing_s3_key_error(exc):
            logger.info("Parquet not found: s3://%s/%s", bucket, key)
        raise

    buffer = io.BytesIO(response["Body"].read())
    return pd.read_parquet(buffer, engine="pyarrow")


def normalize_candidates(candidates) -> list[tuple[int, int]]:
    normalized = []

    for candidate in candidates:
        if isinstance(candidate, dict):
            short = candidate.get("short")
            long = candidate.get("long")
        else:
            short, long = candidate

        short = int(short)
        long = int(long)
        if short <= 0 or long <= 0:
            continue
        if short >= long:
            continue
        normalized.append((short, long))

    return sorted(set(normalized))


def resolve_stock_codes(event: dict | None) -> list[str]:
    event = event or {}
    requested = event.get("stock_codes")
    if not requested:
        return list(WATCHLIST.keys())

    if isinstance(requested, str):
        requested = [requested]

    return [str(stock_code) for stock_code in requested if str(stock_code) in WATCHLIST]


def load_daily_history(
    s3_client,
    bucket: str,
    stock_code: str,
    end_date: str,
    optimize_days: int,
) -> pd.DataFrame:
    """Load daily close prices from processed/daily/{stock_code}/."""
    prefix = f"processed/daily/{stock_code}/"
    rows = []

    for key in _list_s3_keys(s3_client, bucket, prefix):
        if not key.endswith(".parquet"):
            continue

        object_date = _extract_date_from_key(key)
        if not object_date or object_date > end_date:
            continue

        try:
            df = _read_parquet_from_s3(s3_client, bucket, key)
        except Exception:
            logger.warning("Failed to read parquet, skipping: s3://%s/%s", bucket, key)
            continue

        if df.empty:
            continue

        row = df.iloc[0].to_dict()
        close_price = row.get("close")
        if pd.isna(close_price):
            continue

        rows.append(
            {
                "date": str(row.get("date") or object_date),
                "stock_code": stock_code,
                "close": float(close_price),
            }
        )

    if not rows:
        return pd.DataFrame(columns=["date", "stock_code", "close"])

    history = pd.DataFrame(rows).sort_values("date").drop_duplicates("date")
    return history.tail(int(optimize_days)).reset_index(drop=True)


def calculate_strategy_metrics(
    history_df: pd.DataFrame,
    ma_short: int,
    ma_long: int,
    trade_cost: float,
) -> dict | None:
    """Simple long/cash MA crossover backtest on daily closes."""
    if history_df.empty or len(history_df) < ma_long + 2:
        return None

    df = history_df.copy().sort_values("date").reset_index(drop=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"]).reset_index(drop=True)
    if len(df) < ma_long + 2:
        return None

    df["ma_short"] = df["close"].rolling(ma_short).mean()
    df["ma_long"] = df["close"].rolling(ma_long).mean()
    df["position"] = (df["ma_short"] > df["ma_long"]).astype(int)
    df["position"] = df["position"].shift(1).fillna(0)
    df["daily_return"] = df["close"].pct_change().fillna(0)
    df["trade"] = df["position"].diff().abs().fillna(df["position"])
    df["strategy_return"] = (df["daily_return"] * df["position"]) - (df["trade"] * trade_cost)

    returns = df["strategy_return"].dropna()
    if returns.empty:
        return None

    total_return = float((1 + returns).prod() - 1)
    volatility = float(returns.std())
    sharpe = 0.0 if volatility == 0 or math.isnan(volatility) else float(returns.mean() / volatility * math.sqrt(252))

    equity_curve = (1 + returns).cumprod()
    drawdown = equity_curve / equity_curve.cummax() - 1
    max_drawdown = float(drawdown.min()) if not drawdown.empty else 0.0

    return {
        "short": ma_short,
        "long": ma_long,
        "total_return": total_return,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
        "trade_count": int(df["trade"].sum()),
        "sample_days": int(len(df)),
    }


def select_best_ma_pair(
    history_df: pd.DataFrame,
    candidates: list[tuple[int, int]],
    trade_cost: float,
) -> dict | None:
    evaluated = []

    for ma_short, ma_long in candidates:
        metrics = calculate_strategy_metrics(history_df, ma_short, ma_long, trade_cost)
        if metrics is not None:
            evaluated.append(metrics)

    if not evaluated:
        return None

    # 1순위: 샤프지수 높은 것
    # 2순위: 총수익률 높은 것
    # 3순위: MDD 작은 것 (절댓값 기준 오름차순)
    evaluated = sorted(
        evaluated,
        key=lambda row: (row["sharpe"], row["total_return"], -abs(row["max_drawdown"])),
        reverse=True,
    )
    best = evaluated[0]
    best["passed_sharpe_filter"] = best["sharpe"] >= SHARPE_MIN
    best["evaluated_count"] = len(evaluated)
    return best


def fallback_ma_pair(stock_code: str) -> dict:
    pair = MA_CONFIG.get(
        stock_code,
        {"short": DEFAULT_MA_SHORT, "long": DEFAULT_MA_LONG},
    )
    return {
        "short": int(pair.get("short", DEFAULT_MA_SHORT)),
        "long": int(pair.get("long", DEFAULT_MA_LONG)),
        "sharpe": None,
        "fallback": True,
    }


def process_one_stock(
    s3_client,
    bucket: str,
    stock_code: str,
    stock_name: str,
    date: str,
    candidates: list[tuple[int, int]],
    optimize_days: int,
) -> dict:
    history_df = load_daily_history(
        s3_client=s3_client,
        bucket=bucket,
        stock_code=stock_code,
        end_date=date,
        optimize_days=optimize_days,
    )

    best = select_best_ma_pair(
        history_df=history_df,
        candidates=candidates,
        trade_cost=TRADE_COST,
    )

    if best is None:
        logger.info(
            "Optimizer skipped: stock_code=%s reason=not_enough_daily_data",
            stock_code,
        )
        pair = fallback_ma_pair(stock_code)
        return {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "status": "fallback",
            "reason": "not_enough_daily_data",
            "sample_days": int(len(history_df)),
            "best": pair,
        }

    config_pair = {
        "short": int(best["short"]),
        "long": int(best["long"]),
        "sharpe": round(float(best["sharpe"]), 6),
        "total_return": round(float(best["total_return"]), 6),
        "max_drawdown": round(float(best["max_drawdown"]), 6),
        "trade_count": int(best["trade_count"]),
        "sample_days": int(best["sample_days"]),
        "passed_sharpe_filter": bool(best["passed_sharpe_filter"]),
    }

    logger.info(
        "Optimizer selected: stock_code=%s ma=%s/%s sharpe=%.4f",
        stock_code,
        config_pair["short"],
        config_pair["long"],
        config_pair["sharpe"],
    )

    return {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "status": "optimized",
        "best": config_pair,
    }


def build_best_ma_config(
    date: str,
    results: list[dict],
    candidates: list[tuple[int, int]],
    optimize_days: int,
    updated_at: datetime,
) -> dict:
    config = {}

    for result in results:
        stock_code = result["stock_code"]
        best = result["best"]
        config[stock_code] = best

    config["_metadata"] = {
        "updated_at": updated_at.isoformat(),
        "date": date,
        "base": "daily_close",
        "optimize_days": optimize_days,
        "trade_cost": TRADE_COST,
        "sharpe_min": SHARPE_MIN,
        "candidates": [{"short": short, "long": long} for short, long in candidates],
        "results": results,
    }
    return config


def write_best_ma_config(s3_client, bucket: str, date: str, config: dict) -> str:
    key = config_key(date)
    body = json.dumps(config, ensure_ascii=False, indent=2).encode("utf-8")

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )

    logger.info("Best MA config write success: s3://%s/%s processed=%s"
                , bucket, key, len([r for r in config.items() if not str(r[0]).startswith("_")])
            )
    return key


def lambda_handler(event, context):
    """Lambda entry point.

    Optional test event:
        {
          "date": "20260506",
          "stock_codes": ["000660"],
          "optimize_days": 365,
          "ma_candidates": [{"short": 5, "long": 20}, {"short": 20, "long": 60}]
        }
    """
    event = event or {}
    detected_at = now_kst()
    date = str(event.get("date") or detected_at.strftime("%Y%m%d"))
    optimize_days = int(event.get("optimize_days") or DEFAULT_OPTIMIZE_DAYS)
    candidates = normalize_candidates(event.get("ma_candidates") or DEFAULT_MA_CANDIDATES)
    stock_codes = resolve_stock_codes(event)

    runtime = load_runtime_config()
    bucket = runtime["bucket"]
    s3_client = create_s3_client()

    logger.info(
        "Optimizer Lambda invoked: date=%s stock_count=%s candidate_count=%s",
        date,
        len(stock_codes),
        len(candidates),
    )

    results = []
    for stock_code in stock_codes:
        try:
            stock_name = WATCHLIST[stock_code]
            result = process_one_stock(
            s3_client=s3_client,
            bucket=bucket,
            stock_code=stock_code,
            stock_name=stock_name,
            date=date,
            candidates=candidates,
            optimize_days=optimize_days,
        )
            results.append(result)
        except Exception:
            logger.exception("Optimizer failed: stock_code=%s", stock_code)
            results.append({
                "stock_code": stock_code,
                "status": "error",
                "reason": "unexpected_exception",
            })

    best_config = build_best_ma_config(
        date=date,
        results=results,
        candidates=candidates,
        optimize_days=optimize_days,
        updated_at=detected_at,
    )
    config_s3_key = write_best_ma_config(s3_client, bucket, date, best_config)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "date": date,
                "processed": len(results),
                "config_key": config_s3_key,
                "results": results,
            },
            ensure_ascii=False,
        ),
    }
