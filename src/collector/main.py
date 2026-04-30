''' main.py
- 의미와 역할
    auth.py · websocket_client.py를 조립해서 전체 수집 흐름을 실행하는 파일이에요. 
    토큰을 발급받고 WebSocket을 연결한 뒤, 수신된 데이터를 Kafka 토픽에 발행하는 역할을 해요. 
    Day 1에서는 Kafka 발행 대신 터미널 출력으로 대체해서 수신 확인부터 해요.

- 담아야 할 내용 순서
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

실행 방법
"""
    python -m src.collector.main
"""    

'''

import datetime
import logging
from datetime import date

from config.market_config import is_market_day, is_market_time
from src.collector.websocket_client import connect_websocket


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Collector template ready. Implement WebSocket subscription here.")

    today = date.today()
    current_time = datetime.datetime.now().time()

    def on_message_callback(message: str) -> None:
        print("체결 raw 데이터 :", message)

    connect_websocket(on_message_callback)

    '''
    if is_market_time(current_time):
        if is_market_day(today):
            connect_websocket(on_message_callback)
        else:
            logger.info("오늘은 거래일이 아닙니다.")
    else:
        logger.info("현재는 거래 시간이 아닙니다.")
    '''


if __name__ == "__main__":
    main()
