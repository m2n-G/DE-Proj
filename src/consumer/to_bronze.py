"""to_bronze.py
- 의미와 역할
    - Kafka 토픽(realtime-trade-topic)에서 메시지를 읽어서 S3 Bronze 경로에 원본 그대로 적재하는 파일
    - Producer가 Kafka에 발행한 데이터를 Consumer가 읽어서 S3에 쌓는 구조
    - path_config.py의 bronze_key()를 활용해서 경로를 생성해요.

- 담아야 할 내용 순서
```
    1. 환경변수 로드
    └─ KAFKA_BOOTSTRAP · S3_BUCKET 읽기

    2. S3 클라이언트 초기화
    └─ boto3.client("s3") 생성

    3. Kafka Consumer 초기화
    └─ kafka-python KafkaConsumer 사용
    └─ bootstrap_servers 설정
    └─ group_id 설정 (예: realtime-trade-consumer-group)
    └─ auto_offset_reset = "earliest"
    └─ value_deserializer 설정 (bytes → str)

    4. 메시지 처리 함수
    └─ 수신 메시지에서 stock_code · date · hour 추출
    └─ path_config.py 의 bronze_key() 로 S3 경로 생성
    └─ S3 PutObject 로 원본 문자열 적재

    5. Consumer 실행 함수
    └─ 토픽 구독
    └─ 메시지 루프 실행
    └─ 각 메시지마다 4번 함수 호출

    6. 로깅
    └─ S3 적재 성공 · 실패 로그

    Kafka 메시지 수신 → raw 문자열 S3 Bronze 저장 → 성공 시 offset commit → 실패 시 failed S3 경로 저장 → failed 저장 성공 시 offset commit
"""

# 1. 필요한 라이브러리 import
import os
import logging
import uuid
import boto3
import json
from dotenv import load_dotenv
from kafka import KafkaConsumer

from config.path_config import bronze_key
from src.common.time_utils import now_kst, yyyymmdd
from src.collector.kafka_producer import extract_stock_code


# 2. 환경변수 로드
# 환경변수 로드
load_dotenv(".env")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC")
S3_BUCKET       = os.getenv("S3_BUCKET")
AWS_REGION      = os.getenv("AWS_REGION", "ap-northeast-3")
AWS_ACCESS_KEY  = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY  = os.getenv("AWS_SECRET_KEY")
LOG_LEVEL       = "INFO"

if not KAFKA_BOOTSTRAP:
    raise ValueError("KAFKA_BOOTSTRAP 이 .env 에 없습니다.")
if not S3_BUCKET:
    raise ValueError("S3_BUCKET 이 .env 에 없습니다.")

# 3. logger 생성
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(levelname)s:%(name)s:%(message)s",
)

logger = logging.getLogger(__name__)


# 4. Kafka Consumer 설정 생성 함수
def build_consumer_config() -> dict:
    """
    KafkaConsumer에 넘길 설정 dict를 만든다.

    포함할 값:
    - bootstrap_servers
    - group_id
    - auto_offset_reset
    - enable_auto_commit
    - key_deserializer
    - value_deserializer
    """
    # TODO:
    # 1. KAFKA_BOOTSTRAP 확인
    # 2. KAFKA_CONSUMER_GROUP 기본값 지정
    # 3. 문자열 deserializer 설정
    # 4. config 반환

    kafkaconsuemr_config={
        "bootstrap_servers"    : KAFKA_BOOTSTRAP,
        "group_id"             : "realtime-trade-consumer-group",
        "auto_offset_reset"    : "earliest",
        "enable_auto_commit"   : False,
        "key_deserializer"     : lambda key: key.decode("utf-8") if key else None,
        "value_deserializer"   : lambda value: value.decode("utf-8"),
    }
    
    return kafkaconsuemr_config


# 5. Kafka Consumer 생성 함수
def create_consumer(topic: str | None = None) -> KafkaConsumer:

    """
    Kafka Consumer 객체를 생성하고 topic을 구독한다.

    기본 topic:
    - realtime-trade-topic
    """
    # TODO:
    # 1. topic 기본값 결정
    # 2. build_consumer_config() 호출
    # 3. KafkaConsumer(topic, **config) 생성
    # 4. consumer 반환

    topic = topic or KAFKA_TOPIC

    if not topic:
        raise ValueError("KAFKA_TOPIC이 .env 에 없습니다.")
    
    consumer_config = build_consumer_config()
    kafkaconsumer = KafkaConsumer(topic, **consumer_config)

    logger.info("Kafka Consumer 생성 완료")    

    return kafkaconsumer


# 6. S3 client 생성 함수
def create_s3_client():
    """
    boto3 S3 client를 생성한다.
    """
    # TODO:
    # 1. AWS_REGION 읽기
    # 2. boto3.client("s3", region_name=...)
    # 3. client 반환

    return boto3.client("s3", region_name=AWS_REGION)


# 7. Bronze S3 key 생성 함수
def build_bronze_object_key(raw_message: str) -> str:
    """
    raw 메시지에서 stock_code만 추출하고,
    현재 KST 기준 date/hour로 Bronze key를 만든다.

    MVP 권장:
    - 메시지 1건당 파일 1개 저장
    - S3 append 문제를 피하기 위해 파일명에 timestamp/uuid를 붙인다.

    예:
    raw/005930/date=20260504/hour=09/005930_20260504T090501_abc123.log
    """
    # TODO:
    # 1. stock_code = extract_stock_code(raw_message)
    # 2. now = now_kst()
    # 3. date_str = now.strftime("%Y%m%d")
    # 4. hour = int(now.strftime("%H"))
    # 5. timestamp = now.strftime("%Y%m%dT%H%M%S%f")
    # 6. unique_id = uuid.uuid4().hex[:8]
    # 7. path_config.bronze_key()를 그대로 쓸지, 1건 1파일용 key를 직접 만들지 결정
    # 8. key 반환

    stock_code = extract_stock_code(raw_message)
    now        = now_kst()
    date       = now.strftime("%Y%m%d")
    hour       = int(now.strftime("%H"))
    timestamp  = now.strftime("%H%M%S")
    msg_id     = uuid.uuid4().hex[:8]

    return bronze_key(stock_code, date, hour, timestamp, msg_id)


# 8. S3 Bronze 저장 함수
def put_raw_to_bronze(s3_client, bucket: str, raw_message: str) -> str:
    """
    raw 메시지를 S3 Bronze에 저장한다.

    입력:
    - s3_client
    - bucket
    - raw_message

    반환:
    - 저장된 S3 key
    """
    # TODO:
    # 1. key = build_bronze_object_key(raw_message)
    # 2. Body는 raw_message + "\n"
    # 3. ContentType은 "text/plain"
    # 4. s3_client.put_object(...)
    # 5. 성공 로그
    # 6. key 반환

    key = build_bronze_object_key(raw_message)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=raw_message + "\n",
        ContentType="text/plain",
    )
    logger.info("S3 Bronze write success: s3://%s/%s", bucket, key)
    return key


def safe_stock_code(raw_message: str) -> str:
    try:
        return extract_stock_code(raw_message)
    except Exception:
        return "unknown"


def build_failed_object_key(raw_message: str) -> str:
    stock_code = safe_stock_code(raw_message)
    now = now_kst()
    date = now.strftime("%Y%m%d")
    hour = now.strftime("%H")
    timestamp = now.strftime("%H%M%S%f")
    msg_id = uuid.uuid4().hex[:8]

    return (
        f"failed/raw/{stock_code}/date={date}/hour={hour}/"
        f"{stock_code}_{timestamp}_{msg_id}.json"
    )


def put_failed_message_to_s3(
    s3_client,
    bucket: str,
    raw_message: str,
    error: Exception,
    kafka_message,
) -> str:
    key = build_failed_object_key(raw_message)
    failed_at = now_kst().isoformat()

    body = {
        "raw_message": raw_message,
        "error": repr(error),
        "topic": kafka_message.topic,
        "partition": kafka_message.partition,
        "offset": kafka_message.offset,
        "failed_at": failed_at,
    }

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(body, ensure_ascii=False) + "\n",
        ContentType="application/json",
    )
    logger.info("S3 failed-message write success: s3://%s/%s", bucket, key)
    return key


# 9. Kafka -> S3 루프
def run_consumer() -> None:
    """
    Kafka 메시지를 계속 읽어서 S3 Bronze에 저장한다.
    """
    # TODO:
    # 1. consumer 생성
    # 2. s3_client 생성
    # 3. S3_BUCKET 확인
    # 4. for message in consumer:
    # 5. raw_message = message.value
    # 6. put_raw_to_bronze(...)
    # 7. 성공 시 commit
    # 8. 실패 시 logger.exception 후 계속할지 종료할지 정책 결정

    consumer  = create_consumer(KAFKA_TOPIC)
    s3_client = create_s3_client()

    for message in consumer:
        raw_message = message.value

        try:
            s3_key = put_raw_to_bronze(s3_client, S3_BUCKET, raw_message)
            consumer.commit()
            logger.info("Kafka -> Bronze committed: %s", s3_key)

        except Exception as bronze_error:
            logger.exception(
                "Kafka -> Bronze failed: partition=%s offset=%s",
                message.partition,
                message.offset,
            )

            try:
                failed_key = put_failed_message_to_s3(
                    s3_client=s3_client,
                    bucket=S3_BUCKET,
                    raw_message=raw_message,
                    error=bronze_error,
                    kafka_message=message,
                )
                consumer.commit()
                logger.info("Kafka offset committed after failed write: %s", failed_key)

            except Exception:
                logger.exception(
                    "Failed-message write failed; offset will not be committed: "
                    "partition=%s offset=%s",
                    message.partition,
                    message.offset,
                )


def main() -> None:
    logger.info("Bronze consumer with failed handling starting...")
    try:
        run_consumer()
    except KeyboardInterrupt:
        logger.info("Bronze consumer stopped by user")


if __name__ == "__main__":
    main()