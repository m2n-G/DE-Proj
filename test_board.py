#raw_message = "005380^093713^533000^2^2000^0.38^538708.09^545000^545000^533000^534000^533000^1^167194^90068614000^6470^5095^-1375^60.08^97077^58326^5^0.35^14.54^090010^5^-12000^090010^5^-12000^093653^3^0^20260504^20^N^1911^2597^21334^26985^0.08^258848^64.59^0^^545000"

#fields = raw_message.split("^")
#print(len(fields))


'''
# Python 으로 확인
import boto3
import os
from dotenv import load_dotenv
load_dotenv('.env')

client = boto3.client(
    'sts',
    region_name='ap-northeast-3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
)
print(client.get_caller_identity())
'''



import boto3
import os
from dotenv import load_dotenv
from pathlib import Path

# 프로젝트 루트 기준으로 .env 로드
load_dotenv(r"C:\Users\kkmj2\OneDrive\문서\DE-Proj\.env")

print("ACCESS KEY:", os.getenv("AWS_ACCESS_KEY_ID"))  # 확인용

client = boto3.client(
    "athena",
    region_name           = "ap-northeast-3",
    aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
)

# 1. 워크그룹 확인
try:
    wg = client.get_work_group(WorkGroup="primary")
    print("✅ Workgroup 확인:", wg["WorkGroup"]["Name"])
except Exception as e:
    print("❌ Workgroup 오류:", e)

# 2. 데이터베이스 확인
try:
    db = client.get_database(
        CatalogName  = "AwsDataCatalog",
        DatabaseName = "krx_pipeline",
    )
    print("✅ Database 확인:", db["Database"]["Name"])
except Exception as e:
    print("❌ Database 오류:", e)