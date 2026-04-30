''' websocket_client.py
- 의미와 역할
    한투 WebSocket 서버에 연결해서 실시간 체결 데이터를 수신하는 파일이에요. 
    WATCHLIST 종목을 구독 신청하고, 데이터가 들어올 때마다 콜백 함수를 호출해요. 
    main.py가 이 파일을 호출해서 수신된 데이터를 Kafka로 넘겨요.
- 담아야 할 내용 순서
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
'''