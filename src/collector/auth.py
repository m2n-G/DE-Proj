''' auth.py
- 의미와 역할
    한국투자증권 API는 모든 요청에 액세스 토큰이 필요해요.
    auth.py는 앱키·시크릿키로 이 토큰을 발급받고 관리하는 파일이에요.
    토큰 유효시간이 24시간이라 하루에 한 번 갱신이 필요해요.
    websocket_client.py와 main.py가 이 파일을 호출해서 토큰을 가져가요.
- 담아야 할 내용 순서
    1. 환경변수 로드
    └─ .env 에서 APP_KEY · APP_SECRET 읽기

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
'''

# 1. 환경변수 로드 라이브러리
import os
from dotenv import load_dotenv

import requests                      # 2. 토큰 발급 라이브러리
import time                          # 3. 토큰 유효성 확인 라이브러리
import logging                       # 4. 로깅 라이브러리

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

# 한투 API 토큰 발급 엔드포인트
TOKEN_URL = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"

# 토큰 상태 (모듈 수준 변수)
_access_token     = None
_token_issue_time = 0

# 2. 토큰 발급 함수
def get_access_token() -> str:
    """한투 API 액세스 토큰 발급"""
    body = {
        "grant_type": "client_credentials",
        "appkey"    : APP_KEY,
        "appsecret" : APP_SECRET,
    }
    try:
        response = requests.post(TOKEN_URL, json=body)
        data     = response.json()
        token    = data["access_token"]
        logger.info("토큰 발급 성공")
        return token
    except Exception as e:
        logger.error(f"토큰 발급 실패: {e}")
        raise


# 3. 토큰 유효성 확인 함수
    """
    매번 토큰을 새로 발급하면 API 서버에 불필요한 요청이 많아져요. 그래서 이미 유효한 토큰이 있으면 그걸 재사용하고, 
    만료됐을 때만 새로 발급하는 함수예요.
    """
def get_valid_token() -> str:
    global _access_token, _token_issue_time

    current_time = time.time()

    if _access_token is None or (current_time - _token_issue_time) >= 23 * 3600:
        _access_token     = get_access_token()
        _token_issue_time = current_time

    return _access_token