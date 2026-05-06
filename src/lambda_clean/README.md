# lambda_clean

Lambda function that parses Bronze raw messages, validates records, and writes Silver Parquet outputs.

---
# AWS Kinesis Data Streams 생성

## 콘솔 접속
- 순선 : AWS 콘솔 → Kinesis → 데이터 스트림 생성
- 설정값
    - 스트림 이름   : de-ai-07-processing-stream 
    - 리전         : ap-northeast-3 (오사카)
    - 용량 모드     : 온디맨드 (On-demand) ← 자동 조정 · 비용 절감
    - 최대 레코드   : 1024 KB (= 1MB · Kinesis 기본 한도)
                    한투 메시지는 약 1KB 미만이라 한도 초과 없음

## 생성 후 작업
- .env 업데이트
```
KINESIS_STREAM=de-ai-07-processing-stream
KINESIS_ARN=arn:aws:kinesis:ap-northeast-3:827913617635:stream/de-ai-07-processing-stream
```

---
# S3 → EventBridge → Kinesis 연결
## 전체 흐름
```
S3 Bronze 파일 적재
        ↓
S3 이벤트 발생
        ↓
EventBridge 규칙 감지
        ↓
Kinesis processing-stream 으로 전달
```

## S3 이벤트 알림 활성화
### AWS 콘솔
→ S3 → de-ai-07-827913617635-ma-proj 버킷 클릭 → 속성(Properties) 탭 → Amazon EventBridge 섹션 → "이벤트 알림 전송" → 활성화(ON)

## EventBridge 규칙 생성
### AWS 콘솔
→ EventBridge → 규칙(Rules) → 규칙 생성

### 규칙 이름   : s3-bronze-to-kinesis
```
이벤트 버스 : default
이벤트 소스 선택
✅ AWS 이벤트 또는 EventBridge 파트너 이벤트
   └─ S3 같은 AWS 서비스 이벤트를 감지하기 위한 선택지

이벤트 패턴 생성 방법
✅ 사용자 지정 패턴 (JSON 편집기)
   └─ raw/ prefix 등 세밀한 조건 설정 가능

JSON 패턴 입력
json{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["de-ai-07-827913617635-ma-proj"]
    },
    "object": {
      "key": [{
        "prefix": "raw/"
      }]
    }
  }
}

대상(Target) 설정
서비스       : Kinesis Data Streams
스트림       : de-ai-07-processing-stream
파티션 키    : $.detail.object.key

실행 역할(IAM)
✅ 이 특정 리소스에 대해 새 역할 생성
   └─ EventBridge → Kinesis 권한을 AWS가 자동 설정
```

---
# handler.py
## 의미와 역할
- S3 Bronze에 적재된 원본 구분자 문자열을 읽어서 파싱
- 문자열을 정제 후 S3 Silver 두 경로에 저장
- Kinesis가 트리거하면 자동으로 실행
- Bronze → Silver 변환의 핵심

## 담아야 할 내용 순서
```
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

주의사항
⚠️ 5번 일봉 저장 함수
   오늘 시가는 하루에 한 번만 저장해야 해요.
   Lambda가 1분마다 실행되므로
   이미 저장됐는지 확인하는 로직이 반드시 필요해요.

⚠️ 한투 API 원본 필드 순서
   0|H0STCNT0|004|005930^102305^78500^5^3200^248720^78200^78600^75300^...
                          [0]    [1]   [2] [3]   [4]    [5]   [6]   [7]
   stock_code  = [0]  005930
   trade_time  = [1]  102305
   trade_price = [2]  78500
   trade_volume= [3]  3200 (또는 [5] acml_vol)
   bid_price   = [5]  78200
   ask_price   = [6]  78600
   prev_close  = [7]  75300
```