''' websocket_client.py
- 의미와 역할
    한투 WebSocket 서버에 연결해서 실시간 체결 데이터를 수신하는 파일이에요. 
    WATCHLIST_LIST 종목을 구독 신청하고, 데이터가 들어올 때마다 콜백 함수를 호출해요. 
    main.py가 이 파일을 호출해서 수신된 데이터를 Kafka로 넘겨요.
- 담아야 할 내용 순서
    1. 환경변수 로드
    └─ APP_KEY 읽기

    2. WebSocket 연결 함수
    └─ 엔드포인트: https://openapi.koreainvestment.com:9443/oauth2/Approval
    └─ websocket-client 라이브러리 사용
    └─ on_open · on_message · on_error · on_close 콜백 정의

    Request Example = {
        "grant_type": "client_credentials",
        "appkey": "PSg5dctL9dKPo727J13Ur405OSXXXXXXXXXX",
        "secretkey": "yo2t8zS68zpdjGuWvFyM9VikjX.../OIXXXXXXXXXX"
    }

    3. 종목 구독 함수
    └─ on_open 시점에 WATCHLIST_LIST 종목 순회
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

import os
import json
import websocket
import logging
from dotenv import load_dotenv
import requests
from collections.abc import Callable
from config.signal_config import WATCHLIST

# 환경변수 로드
load_dotenv(".env")
APP_KEY    = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
LOG_LEVEL  = os.getenv("LOG_LEVEL", "DEBUG")

if not APP_KEY or not APP_SECRET:
    raise ValueError("APP_KEY 또는 APP_SECRET 이 .env 에 없습니다.")

# 로깅 설정
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# 한투API(WebSocket) 엔드포인트
APPROVAL_URL = "https://openapi.koreainvestment.com:9443/oauth2/Approval"   # 실시간 (웹소켓) 접속키 발급 URL
WS_URL       = "ws://ops.koreainvestment.com:21000"                         # 국내주식 실시간시세 URL
REALTIME_TRADE_TR_ID = "H0STCNT0"                                           # 국내주식 실시간 체결가 정보 요청 ID

# 쉼표로 구분된 종목 문자열을 실제 종목 코드 리스트로 변환
stock_codes = list(WATCHLIST.keys())

'''
 JSON 제어 메시지를 먼저 처리
    SUBSCRIBE SUCCESS는 구독 확인 로그만 남기고, 
    PINGPONG는 같은 메시지를 다시 보내 heartbeat를 유지한 뒤,
    에러 제어 메시지는 경고 로그로 분리
'''
def handle_control_message(
    ws_app: websocket.WebSocketApp,
    message: str,
) -> bool:
    """Handle JSON control frames and return True when consumed."""
    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        return False

    header = payload.get("header", {})
    body = payload.get("body", {})
    tr_id = header.get("tr_id")
    msg1 = body.get("msg1")
    rt_cd = body.get("rt_cd")

    if tr_id == "PINGPONG":
        # Echo heartbeat frames back to keep the session alive.
        ws_app.send(message)
        logger.debug("PINGPONG received and echoed back")
        return True

    if msg1 == "SUBSCRIBE SUCCESS":
        logger.info("%s subscription confirmed", header.get("tr_key"))
        return True

    if rt_cd and rt_cd != "0":
        logger.warning(
            "Control message error for %s: %s (%s)",
            header.get("tr_key"),
            msg1,
            body.get("msg_cd"),
        )
        return True

    return False

def get_approval_key():
    body = {
        "grant_type": "client_credentials",
        "appkey"    : APP_KEY,
        "secretkey" : APP_SECRET,
    }
    res = requests.post(APPROVAL_URL, json=body, timeout=10) # post요청으로 받은 response가 바로 res에 저장됨. 
    res.raise_for_status()                                   # HTTP 상태코드가 실패(4xx, 5xx) 일 때 예외를 발생시키는 메서드
    
    data = res.json()
    approval_key = data.get("approval_key")
    if not approval_key:
        raise RuntimeError(f"approval_key missing from response: {data}")

    logger.info("WebSocket approval key issued")
    return approval_key

# 3. 종목 구독 함수
def subscribe_stocks(ws_app: websocket.WebSocketApp, approval_key: str) -> None:
    """WATCHLIST_LIST 종목에 체결 데이터 구독 요청 전송"""
    
    # 각 종목마다 웹소켓으로 보낼 구독 메시지를 만듭니다.
    for stock_code in stock_codes:
        subscribe_message = {
            "header": {
                "approval_key" : approval_key,
                "custtype"     : "P",
                "tr_type"      : "1",
                "content-type" : "utf-8",

            },
            "body": {
                "input": {
                    "tr_id": REALTIME_TRADE_TR_ID,
                    "tr_key": stock_code,
                }
            },
        }
        ws_app.send(json.dumps(subscribe_message))
        logger.info("%s 구독 요청 전송", stock_code)


# 4. 데이터 수신 콜백 함수 (on_message)
def connect_websocket( on_message_callback: Callable[[str], None]) -> None:
    """Connect to KIS WebSocket and pass raw messages to the callback."""
    approval_key = get_approval_key()

    # 웹소켓 연결이 성공했을 때 자동으로 호출되는 콜백 함수 => “연결되자마자 관심 종목들을 구독하라”
    #   1. "WebSocket connected" 로그를 남기고
    #   2. subscribe_stocks(ws_app, approval_key)를 호출해서 종목 구독 요청을 보냅니다.
    def on_open(ws_app: websocket.WebSocketApp) -> None:
        logger.info("WebSocket connected")
        subscribe_stocks(ws_app, approval_key)       

    # 웹소켓 서버에서 메시지가 들어올 때마다 호출되는 콜백 함수
    # KIS에서 실시간 체결 데이터가 들어오면 message에 원본 문자열 입력
    def on_message(ws_app: websocket.WebSocketApp, message: str) -> None:
        if handle_control_message(ws_app, message):
            return
        on_message_callback(message)

    # 웹소켓 연결 중 에러가 발생했을 때 호출
    def on_error(ws_app: websocket.WebSocketApp, error: Exception) -> None:
        logger.error("WebSocket error: %s", error)

    # 웹소켓 연결이 종료될 때 호출
    def on_close(
        ws_app           : websocket.WebSocketApp,
        close_status_code: int | None,
        close_msg        : str | None,
    ) -> None:
        logger.info("WebSocket closed: %s - %s", close_status_code, close_msg)

    # 웹소켓 클라이언트 객체 생성
    ws_app = websocket.WebSocketApp(
        WS_URL,
        on_open   =on_open,
        on_message=on_message,
        on_error  =on_error,
        on_close  =on_close,
    )
    ws_app.run_forever()

