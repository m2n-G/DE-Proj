# Apr30th 2026
## 1단계 — 1단계 설정/도메인 모델 고정 결정
가장 먼저 config 파일을 잡는 이유
  Lambda 4개가 전부 같은 S3 경로 · 스키마 · MA 파라미터를 참조
  → config 없이 시작하면 경로 불일치 · 타입 오류 발생
  → 전체 파이프라인이 같은 기준으로 움직이게 먼저 고정

## 2단계 — config 파일 5개 완성
- config파일: 공유되는 변수나 스키마들을 관리하는 파일
✅ signal_config.py
   WATCHLIST · MA_CANDIDATES · MA_BASE · OPTIMIZE_DAYS
   TRADE_COST=0.0023728 (한투 BanKIS 기준 왕복 거래비용)
   SHARPE_MIN=1.0 · BEST_MA 딕셔너리

✅ path_config.py
   bronze_key() · silver_trade_key() · silver_daily_key()
   gold_signal_key() · config_key()
   → 타입 힌트 · hour 2자리 패딩 · __all__ · docstring

✅ market_config.py
   MARKET_TIMEZONE · MARKET_OPEN · MARKET_CLOSE · MARKET_DAYS
   is_market_time() · is_market_day()
   → holidayskr 라이브러리로 공휴일 처리
   → logging 으로 print 대체

✅ schema_config.py
   BRONZE_SCHEMA=None
   SILVER_TRADE_SCHEMA · SILVER_DAILY_SCHEMA
   GOLD_SIGNAL_SCHEMA · CONFIG_SCHEMA
   → 딕셔너리`{}` 형태로 타입 정보 코드화

✅ .env
   APP_KEY · APP_SECRET
   S3_BUCKET=de-ai-07-827913617635-ma-proj
   SNS_ARN · KINESIS_STREAM · KAFKA_BOOTSTRAP
   ENV=dev · LOG_LEVEL=DEBUG
   리전: ap-northeast-3 (오사카)

## 4단계 — src/collector/ 구조 확정 + auth.py 작성
src/
  collector/
    auth.py              ✅ 완성
    websocket_client.py  ⬜ 다음 작업
    main.py              ⬜ 다음 작업
auth.py 핵심 내용
- 환경변수 로드 (load_dotenv)
- get_access_token()  : 한투 API 토큰 발급
- get_valid_token()   : 유효 토큰 반환 (23시간 만료 체크)
- global 변수로 토큰 · 발급시각 모듈 수준에서 관리
- logging 으로 성공/실패 로그 (토큰값 자체는 출력 안 함)

---

## 