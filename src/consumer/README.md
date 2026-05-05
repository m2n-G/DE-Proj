# consumer

Kafka consumer that writes raw WebSocket messages to the S3 Bronze layer.
---

---
# to_bronze.py
## 의미와 역할
    - Kafka 토픽(realtime-trade-topic)에서 메시지를 읽어서 S3 Bronze 경로에 원본 그대로 적재하는 파일
    - Producer가 Kafka에 발행한 데이터를 Consumer가 읽어서 S3에 쌓는 구조
    - path_config.py의 bronze_key()를 활용해서 경로를 생성해요.

## 담아야 할 내용 순서
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
```
