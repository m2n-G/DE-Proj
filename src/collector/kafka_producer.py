''' kafka producer.py
- 의미와 역할
    - WebSocket으로 수신한 체결 데이터를 Kafka 토픽에 발행하는 파일
    - main.py의 handle_message 함수가 이 파일을 호출해서 수신 데이터를 Kafka로 넘기도록 구현
    - 요점 : 데이터는 파싱 없이 원본 문자열 그대로 발행해야 함

- 담아야 할 내용 순서
    1. 환경변수 로드
    └─ KAFKA_BOOTSTRAP 읽기

    2. Kafka Producer 초기화
    └─ confluent_kafka 또는 kafka-python 라이브러리 사용
    └─ bootstrap.servers 설정

    3. 메시지 발행 함수
    └─ 입력 : 원본 문자열 (message: str)
    └─ 토픽 : realtime-trade-topic
    └─ 파티션 키 : 종목코드 (같은 종목은 같은 파티션으로)
    └─ 원본 문자열 그대로 발행 (파싱 없음)

    4. 로깅
    └─ 발행 성공 · 실패 로그
'''

# 1. 필요한 라이브러리 import
import os
import logging                       # 4. 로깅 라이브러리
from dotenv import load_dotenv
from kafka import KafkaProducer     # Kafka producer 라이브러리

# 2. 환경변수 로드
load_dotenv(".env")
KAFKA_BOOTSTRAP          = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_TOPIC              = os.getenv("KAFKA_TOPIC")
KAFKA_CLIENT_ID          = os.getenv("KAFKA_CLIENT_ID")
KAFKA_SECURITY_PROTOCOL  = os.getenv("KAFKA_SECURITY_PROTOCOL")

# 3. 기본 설정값 정의
DEFAULT_TOPIC = "realtime-trade-topic"


# 4. logger 생성
logger = logging.getLogger(__name__)


# 5. stock_code 추출 함수
def extract_stock_code(raw_message: str) -> str:
    """
    raw WebSocket 문자열에서 Kafka partition key로 사용할 stock_code만 추출한다.

    입력 예:
    0|H0STCNT0|004|005930^102305^78500^5^3200^...

    반환 예:
    005930
    """
    # TODO:
    # 1. raw_message를 "|" 기준으로 나눈다.
    # 2. payload 부분을 꺼낸다.
    # 3. payload를 "^" 기준으로 나눈다.
    # 4. 첫 번째 필드를 stock_code로 반환한다.
    # 5. 실패하면 ValueError를 발생시킨다.

    try:
        payload    = raw_message.split("|")[3]
        stock_code = payload.split("^")[0]
    except IndexError as exc:
        raise ValueError(f"Invalid raw trade message: {raw_message}") from exc

    return stock_code



# 6. Producer 설정 생성 함수
def build_producer_config() -> dict:
    """
    Kafka Producer 설정 dict를 만든다.
    = Producer가 앞으로 어떻게 동작할지에 대한 설정값 모음집

    포함할 값:
    - bootstrap.servers
    - client.id
    - security.protocol
    - sasl.mechanisms, sasl.username, sasl.password 필요 시
    """
    # TODO:
    # 1. KAFKA_BOOTSTRAP 값 확인
    # 2. 없으면 ValueError
    # 3. producer 설정 dict 생성
    # 4. 보안 설정이 있으면 추가
    
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP")
    if not kafka_bootstrap:
        raise ValueError("KAFKA_BOOTSTRAP이 .env에 없습니다.")

    config = {
        "bootstrap_servers"  : KAFKA_BOOTSTRAP,
        "client_id"          : KAFKA_CLIENT_ID,
        "security_protocol"  : KAFKA_SECURITY_PROTOCOL,
        "key_serializer"     : lambda key: key.encode("utf-8"),
        "value_serializer"   : lambda value: value.encode("utf-8"),
    }

    sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM")
    sasl_username  = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password  = os.getenv("KAFKA_SASL_PASSWORD")

    if sasl_mechanism:
        config["sasl_mechanism"] = sasl_mechanism
    if sasl_username:
        config["sasl_plain_username"] = sasl_username
    if sasl_password:
        config["sasl_plain_password"] = sasl_password

    return config



# 7. Producer 생성 함수
def create_producer():
    """
    Kafka Producer 객체를 생성한다.

    반환:
    - confluent_kafka.Producer 또는 kafka.KafkaProducer 인스턴스
    """
    # TODO:
    # 1. build_producer_config() 호출
    # 2. 선택한 Kafka 라이브러리의 Producer 생성
    # 3. producer 반환
    

    producer_config = build_producer_config()
    producer = KafkaProducer(**producer_config)   # '**' : 'dict key' → 함수 인자 이름 | 'dict value' → 그 인자에 들어갈 값
    logger.info("Kafka Producer 생성 완료")
    '''
        KafkaProducer(**config)
        KafkaProducer(
            bootstrap_servers="localhost:9092",
            client_id="realtime-trade-producer",
            security_protocol="PLAINTEXT", ...
        )
    '''

    return producer


# 9 메시지 발행 함수
def publish_raw_trade(producer : KafkaProducer, raw_message: str, topic: str | None = None) -> None:
    """
    raw 체결 메시지를 Kafka로 발행한다.

    입력:
    - producer: Kafka producer 객체
    - raw_message: WebSocket에서 받은 원본 문자열
    - topic: 기본값은 realtime-trade-topic

    동작:
    - stock_code를 추출한다.
    - key는 stock_code로 설정한다.
    - value는 raw_message 원본 그대로 설정한다.
    - producer.produce 또는 producer.send 호출
    - flush/poll 처리
    """
    # TODO:
    # 1. topic 기본값 결정
    # 2. stock_code = extract_stock_code(raw_message)
    # 3. key = stock_code
    # 4. value = raw_message
    # 5. Kafka publish
    # 6. delivery callback 연결
    # 7. producer.poll(0) 또는 flush 정책 적용

    if not topic:
        topic = DEFAULT_TOPIC

    stock_code = extract_stock_code(raw_message)

    try:
        future          = producer.send(topic, key=stock_code, value=raw_message)
        record_metadata = future.get(timeout=10)
        logger.info(
            "Kafka publish success: topic=%s partition=%s offset=%s",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )
    except Exception as exc:
        logger.error("Kafka publish failed: %s", exc)
        raise


def close_producer(producer : KafkaProducer) -> None:
    """
    프로그램 종료 전에 producer에 남아 있는 메시지를 Kafka로 밀어낸다.

    주의:
    - 메시지 1건마다 호출하지 않는다.
    - collector 프로그램이 종료될 때 한 번만 호출한다.
    """
    try:
        producer.flush(timeout=10)
        logger.info("Kafka producer flushed successfully")
    except Exception as exc:
        logger.error("Kafka producer flush failed: %s", exc)
        raise