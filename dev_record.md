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

# May 6th 2026
## urllib.parse.unquote_plus() 디코딩
- 값을 사람이 읽을 수 있는 원래 형태로 변환하는 라이브러리(디코딩)
- 문법 체계 예시
   ```python
   "%2F" → "/"
   "%20" → " "
   "%ED%95%9C%EA%B8%80" → "한글"
   "my+file.txt" → "my file.txt"  #'+' -> 공백
   ```

## base64 디코딩
- `Base64`는 이진 데이터(ex.이미지, 파일, 오디오...)를 64 개의 인쇄 가능한 문자로 표현하는 인코딩 방법
- base64로 표현된 정보를 바이너리 데이터로 변환하는 라이브러리
```python
import base64

code = '7JWI64WVPw=='
code_bytes = code.encode('ascii')

decoded = base64.b64decode(code_bytes)
str = decoded.decode('UTF-8')
print(str)

# 안녕?
```

## 재귀함수
- 함수 자기 자신을 다시 호출하는 함수
- 새로운 함수 실행 공간(stack frame)을 생성하며 진행
> - 각 재귀 호출은 자기만의 result 객체가 존재
> - 자기 호출 범위 안에서만 값 저장
> - 바깥 호출의 리스트에는 자동 반영되지 않음

## append() vs extend()
### append()
- 대상을 통째로 `하나의 원소`로 추가
- 예시
```python
result = [1, 2]

result.append([3, 4])

print(result)

>>> [1, 2, [3, 4]]
```
- 구조적 설명
```python
[
  1,
  2,
  [3,4]
]
```

### extend()
- 리스트 안의 원소들을 `하나씩 꺼내서` 추가
- 예시
```python
result = [1, 2]

result.extend([3, 4])

print(result)

>>> [1, 2, 3, 4]
```
- 구조적 설명
```python
[
  1,
  2,
  3,
  4
]
```

## raw_message를 체결 갯수 만큼 정확하게 나누는 방법
```python
   for i in range(row_count):                          # 1) i = 0                2) i=1                   3) i=2                   ...  n) i = n
   start = i * fields_per_row                      # 1) start = 0 * 46 = 0   2) start = 1 * 46 = 46   3) start = 2 * 46 = 92   ...  n) start = n * 46 = 46n
   row = fields[start:start + fields_per_row]      # 1) row = fields[0:46]   2) row = fields[46:92]   3) row = fields[92:138]  ...  n) row = fields[start:start+46]
```

## `zip`으로 바로 딕셔너리 만들기
- `zip()`  : 여러 iterable의 같은 위치 요소들을 묶어서 튜플로 만들어주는 역할
- `dict()` : 그 (key, value) 형태의 튜플들을 받아 딕셔너리를 생성
- ex
```python
keys = ["name", "age", "city"]
values = ["Tom", 20, "Seoul"]

result = dict(zip(keys, values))
```
```python
# 1. zip() 단계
zip(keys, values)

↓
("name", "Tom")
("age", 20)
("city", "Seoul")
```
```python
# 2. dict() 단계
{
  "name": "Tom",
  "age": 20,
  "city": "Seoul"
}
```

- 대입
```python
raw_record = {key: safe_cast(val, dtype) for (key, dtype), val in zip(KIS_TRADE_RAW_FIELDS, row)}
↓
raw_record = {}
for (key, dtype), val in zip(TRADE_FIELDS, row):
   raw_record[key] = safe_cast(val, dtype)
```
```python
TRADE_FIELDS = [
    ("price", int),
    ("quantity", int),
    ("symbol", str)
]

row = ["1000", "3", "AAPLE"]

# 1. zip()
zip(TRADE_FIELDS, row)

>>> [
    (("price", int), "1000"),
    (("quantity", int), "3"),
    (("symbol", str), "AAPL")
]

# 2. 반복문
# 생성된 튜플 수 만큼 각각 '(key, dtype), val'에 할당
(key, dtype) = ("price", int)
val = "1000"

# 실행값
raw_record[key] = safe_cast(val, dtype)
raw_record["price"] = safe_cast("1000", int)
raw_record["quantity"] = safe_cast("3", int)
raw_record["symbol"] = safe_cast("AAPL", str)

>>> raw_record = {
   "price" : 1000,
   "quantity" : 3,
   "symbol" : "AAPL",
}
```

## .get(key, default)
- `key`가 있으면 값을 반환하고, 없으면 `default` 값을 반환
- 매핑 테이블 생성시 사용 라이브러리
```
.get(k, k)
└─ FIELD_NAME_MAP[k] 가 있으면 → 매핑된 값 사용
└─ 없으면              → k 그대로 사용 (한투 원본 이름 유지)
```
```python
my_dict = {'a': 1, 'b': 2}

# 1. 키가 존재하는 경우
print(my_dict.get('a', 'a'))  # 출력: 1

# 2. 키가 존재하지 않는 경우 (기본값인 'c' 반환)
print(my_dict.get('c', 'c'))  # 출력: c
```
- 대입
```python
record = {SILVER_TRADE_SCHEMA.get(k, k): v for k, v in raw_record.items()}
↓
record = {}
for k, v in raw_record.items():
   new_key = SILVER_TRADE_SCHEMA.get(k, k)  # 매핑에 있으면 변환, 없으면 원본
   record[new_key] = v


for k, v in raw_record.items():
   # k=key, v=value
   


```
```python
# 1. raw_record.itmes()
raw_record = { "price" : 1000, "quantity" : 3, "symbol" : "AAPL",}

raw_record_items([('price', '1000'), ('quantity', '3'), ('symbol', 'AAPL')]) 

# 2. for k, v in raw_record.items():
k = 'price'
v = 1000

# 반복문 반환값
record = {
    "stock_code"     : "005930",   # MKSC_SHRN_ISCD → stock_code (매핑됨)
    "trade_time"     : "102305",   # STCK_CNTG_HOUR → trade_time (매핑됨)
    "trade_price"    : 78500,      # STCK_PRPR → trade_price (매핑됨)
    "PRDY_VRSS_SIGN" : "5",        # 매핑 없음 → 한투 원본 이름 유지
    "PRDY_VRSS"      : 3200,       # 매핑 없음 → 한투 원본 이름 유지
    ...
}
```

## parse_raw_trade_message 최종 데이터
```
raw_message: "0|H0STCNT0|001|005930^102305^78500^..."
        ↓ split
row: ["005930", "102305", "78500", ...]
        ↓ 1단계: zip + safe_cast (타입 변환)
raw_record: {"MKSC_SHRN_ISCD": "005930", "STCK_PRPR": 78500, ...}
        ↓ 2단계: FIELD_NAME_MAP (이름 변환)
record: {"stock_code": "005930", "trade_price": 78500, ...}
```


