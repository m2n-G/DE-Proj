'''
## signal_config.py
- 의미와 역할
    이 파이프라인의 퀀트 전략 파라미터 중앙 관리 파일
    lambda_signal, lambda_optimize, lambda_notify 세 Lambda가 전부 이 파일을 참조해서 MA 계산 기준과 종목 리스트를 가져가요.  
    나중에 "MA 파라미터를 바꾸고 싶다"거나 "종목을 추가하고 싶다"고 할 때 이 파일 하나만 수정하면 전체 파이프라인에 반영돼요.
- 담아야 할 내용 순서
    1. 모니터링 종목 리스트 (WATCHLIST)
    2. 백테스트 후보 MA 조합 목록 (MA_CANDIDATES)
    3. MA 계산 기준 명시 (MA_BASE = "daily")
    4. 백테스트 기간 (OPTIMIZE_DAYS)
    5. 거래비용 (TRADE_COST)
    6. 알림 필터 최소 샤프지수 (SHARPE_MIN)
    7. 종목별 현재 최적 MA 조합 딕셔너리 (BEST_MA)
    └─ lambda_optimize 가 매일 자동으로 이 값을 갱신함
'''
# 모니터링 종목 리스트 (WATCHLIST)
WATCHLIST = {
    "005930": "Samsung Electronics",
    "000660": "SK hynix",
}

# 백테스트 후보 MA 조합 목록 (MA_CANDIDATES)
DEFAULT_MA_SHORT = 5 # 단기 MA 기본값 : 5days
DEFAULT_MA_LONG = 20 # 장기 MA 기본값 : 20days

# MA 계산 기준 명시 (MA_BASE = "daily")
MA_BASE = "daily"
# 종목별 MA 조합 딕셔너리 (MA_CONFIG) *초기 설정(추후에 자동화에 맞추어 갱신 예정)*
#  1. Samsung Electronics(005930)은 단기 5일, 장기 20일 MA
#  2. SK hynix(000660)은 단기 20일, 장기 60일 MA
MA_CONFIG = {
    "005930": {"short": 5, "long": 20},
    "000660": {"short": 20, "long": 60},
}




''' 자동화 단계에서 적용 예정

# 백테스트 기간 (OPTIMIZE_DAYS)
OPTIMIZE_DAYS = 365
def get_ma_config(stock_code: str) -> dict[str, int]:
    """Return the configured MA pair for a stock code."""
    return MA_CONFIG.get(
        stock_code,
        {"short": DEFAULT_MA_SHORT, "long": DEFAULT_MA_LONG},
    )

'''




# 거래비용 (TRADE_COST)
''' 한국투자증권 BanKIS 온라인 기준으로 매수+매도 왕복 거래비용 계산
매수 시)
증권사 수수료   0.015%
유관기관 비용   0.00364%
합계           0.01864%

매도 시)
증권사 수수료  0.015%
유관기관 비용    0.00364%
증권거래세      0.20%
합계           0.21864%

왕복(매수+매도) 합산)
0.01864% + 0.21864% = 0.23728%
'''
TRADE_COST = 0.0023728




# 알림 필터 최소 샤프지수 (SHARPE_MIN)
'''
[샤프지수란?]
샤프지수는 투자 전략의 위험 대비 수익률을 나타내는 지표
- 계산 공식
    샤프지수 = 평균 수익률 / 수익률 표준편차 × √252
- 샤프지수 해석 기준
    0.0 미만   →  손실 전략 (쓰면 안 됨)
    0.0 ~ 0.5  →  수익은 나지만 리스크 대비 매우 비효율
    0.5 ~ 1.0  →  그럭저럭 쓸 만하지만 좋지는 않음
    1.0 ~ 2.0  →  양호 (실무에서 수용 가능한 수준)
    2.0 이상   →  우수 (퀀트 펀드에서 목표로 하는 수준)
'''
SHARPE_MIN = 1.0 # 샤프지수가 1.0 이상인 경우에만 알림



''' 자동화 단계에서 적용 예정

# 종목별 현재 최적 MA 조합 딕셔너리 (BEST_MA)
BEST_MA = {
    "005930": {"short": 5, "long": 20},
    "000660": {"short": 20, "long": 60},
}

'''