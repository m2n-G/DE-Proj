# lambda_signal.handler
## 의미와 역할
- Silver daily/ 경로에서 이전 N-1일 종가와 오늘 시가를 읽어서 MA를 계산하고 골든크로스·데드크로스를 감지
- 1분마다 EventBridge가 트리거하고, 크로스오버 감지 시 SNS로 Slack 알림을 발송
- 하루에 한 번만 알림이 가도록 중복 방지 로직 추가

## 담아야 할 내용 순서
```
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
```

## 주의사항
```
⚠️ 7번 중복 방지
   시가 기준이라 MA값이 하루 동안 고정
   → 1분마다 실행해도 같은 결과
   → 중복 알림 방지 로직 필수

⚠️ 데이터 부족 예외 처리
   MA_LONG = 20 이면 최소 20일치 데이터 필요
   → 데이터가 부족하면 MA 계산 skip

⚠️ 오늘 시가 없는 경우
   장 시작 전에 실행되면 오늘 시가가 없을 수 있음
   → open_price 없으면 skip
```
