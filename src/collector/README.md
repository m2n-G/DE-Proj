# collector

WebSocket collector that subscribes to Korea Investment real-time trade data and publishes raw messages to Kafka.
---

## auth.py
### 의미와 역할
    한국투자증권 API는 모든 요청에 액세스 토큰이 필요해요.
    auth.py는 앱키·시크릿키로 이 토큰을 발급받고 관리하는 파일이에요.
    토큰 유효시간이 24시간이라 하루에 한 번 갱신이 필요해요.
    websocket_client.py와 main.py가 이 파일을 호출해서 토큰을 가져가요.
### 담아야 할 내용 순서
```
1. 환경변수 로드
   └─ .env.dev 에서 APP_KEY · APP_SECRET 읽기

2. 토큰 발급 함수
   └─ 엔드포인트: POST https://openapi.koreainvestment.com:9443/oauth2/tokenP
   └─ 요청 body: grant_type · appkey · appsecret
   └─ 응답에서 access_token 추출 후 반환

3. 토큰 유효성 확인 함수
   └─ 토큰 발급 시각 저장
   └─ 현재 시각 - 발급 시각 >= 23시간 이면 만료로 판단
   └─ 만료됐으면 재발급 · 아니면 기존 토큰 반환

4. 로깅
   └─ 토큰 발급 성공 / 실패 로그
   └─ 토큰 값 자체는 로그에 출력하지 않음 (보안)
```

---

## websocket_client.py
### 의미와 역할
    한투 WebSocket 서버에 연결해서 실시간 체결 데이터를 수신하는 파일이에요. 
    WATCHLIST 종목을 구독 신청하고, 데이터가 들어올 때마다 콜백 함수를 호출해요. 
    main.py가 이 파일을 호출해서 수신된 데이터를 Kafka로 넘겨요.
### 담아야 할 내용 순서
```
1. 환경변수 로드
   └─ APP_KEY 읽기

2. WebSocket 연결 함수
   └─ 엔드포인트: wss://ops.koreainvestment.com:21000
   └─ websocket-client 라이브러리 사용
   └─ on_open · on_message · on_error · on_close 콜백 정의

3. 종목 구독 함수
   └─ on_open 시점에 WATCHLIST 종목 순회
   └─ 각 종목에 체결 데이터 구독 요청 전송
   └─ 구독 요청 포맷:
      {
        "header": {"appkey": APP_KEY, "tr_type": "1"},
        "body": {"tr_id": "H0STCNT0", "tr_key": stock_code}
      }

4. 데이터 수신 콜백 함수 (on_message)
   └─ 수신된 원본 문자열을 그대로 콜백으로 전달
   └─ 이 파일에서는 파싱 하지 않음 (main.py 에서 처리)

5. 로깅
   └─ 연결 성공 · 종목별 구독 성공 · 연결 종료 로그
```
---

## main.py
### 의미와 역할
    auth.py · websocket_client.py를 조립해서 전체 수집 흐름을 실행하는 파일이에요. 
    토큰을 발급받고 WebSocket을 연결한 뒤, 수신된 데이터를 Kafka 토픽에 발행하는 역할을 해요. 
    Day 1에서는 Kafka 발행 대신 터미널 출력으로 대체해서 수신 확인부터 해요.
### 담아야 할 내용 순서
```
1. 환경변수 로드
   └─ .env 전체 로드

2. 토큰 발급
   └─ auth.py 의 토큰 발급 함수 호출
   └─ 발급 실패 시 프로그램 종료

3. 수신 데이터 처리 함수 정의
   └─ websocket_client.py 의 on_message 콜백으로 전달할 함수
   └─ Day 1: 원본 문자열 터미널 출력 (수신 확인용)
   └─ Day 2: Kafka publish 로 교체 예정

4. WebSocket 실행
   └─ websocket_client.py 의 연결 함수 호출
   └─ 3번 함수를 콜백으로 전달

5. 장중 여부 확인 (옵션)
   └─ market_config.py 의 is_market_time · is_market_day 활용
   └─ 장 외 시간에는 연결 시도하지 않음

6. 로깅
   └─ 전체 흐름 시작 · 종료 로그
```

---

## 세 파일의 관계 요약
```
main.py
  ├─ auth.py 호출        → 토큰 발급
  └─ websocket_client.py 호출 → WebSocket 연결 · 수신
                                      ↓
                              on_message 콜백
                                      ↓
                            main.py 처리 함수
                                      ↓
                         Day 1: 터미널 출력
                         Day 2: Kafka publish
```