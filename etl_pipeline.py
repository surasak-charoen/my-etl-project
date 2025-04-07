import requests
import json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger
import schedule
import time
from datetime import datetime

# === Step 1: Extract ===
def extract():
    print(f"[{datetime.now()}] Starting extraction...")
    all_data = []
    for currency in ['usd', 'thb']:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            'vs_currency': currency,
            'ids': 'bitcoin,ethereum,solana,ripple,susds',
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            for d in data:
                d['currency'] = currency  # เพิ่ม column ระบุสกุลเงิน
            all_data.extend(data)
        else:
            print(f"[{datetime.now()}] Failed to extract data for {currency}. Status code: {response.status_code}")
    print(f"[{datetime.now()}] Extracted total {len(all_data)} records.")
    return all_data

# === Step 2: Transform ===
def transform(data):
    print(f"[{datetime.now()}] Starting transformation...")

    # เลือก column ที่ต้องการ
    columns = [
        'currency','id','symbol','name','current_price','market_cap','market_cap_rank','total_volume',
         'high_24h','low_24h','price_change_24h','price_change_percentage_24h','circulating_supply',
         'total_supply','ath','ath_change_percentage'
        , 'last_updated', 'roi'
    ]
    
    df = pd.DataFrame(data)[columns]
    
    # แปลงประเภทข้อมูล
    
    df['current_price'] = pd.to_numeric(df['current_price'], errors='coerce')
    df = df.dropna(subset=['current_price'])

    df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce')
    df = df.dropna(subset=['market_cap'])

    df['market_cap_rank'] = pd.to_numeric(df['market_cap_rank'], errors='coerce')
    df = df.dropna(subset=['market_cap_rank'])

    df['total_volume'] = pd.to_numeric(df['total_volume'], errors='coerce')
    df = df.dropna(subset=['total_volume'])

    df['high_24h'] = pd.to_numeric(df['high_24h'], errors='coerce')
    df = df.dropna(subset=['high_24h'])

    df['low_24h'] = pd.to_numeric(df['low_24h'], errors='coerce')
    df = df.dropna(subset=['low_24h'])

    df['price_change_24h'] = pd.to_numeric(df['price_change_24h'], errors='coerce')
    df = df.dropna(subset=['price_change_24h'])

    df['price_change_percentage_24h'] = pd.to_numeric(df['price_change_percentage_24h'], errors='coerce')
    df = df.dropna(subset=['price_change_percentage_24h'])

    df['circulating_supply'] = pd.to_numeric(df['circulating_supply'], errors='coerce')
    df = df.dropna(subset=['circulating_supply'])

    df['total_supply'] = pd.to_numeric(df['total_supply'], errors='coerce')
    df = df.dropna(subset=['total_supply'])

    df['ath'] = pd.to_numeric(df['ath'], errors='coerce')
    df = df.dropna(subset=['ath'])

    df['ath_change_percentage'] = pd.to_numeric(df['ath_change_percentage'], errors='coerce')
    df = df.dropna(subset=['ath_change_percentage'])

    df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce')  # แปลง last_updated เป็น datetime
    df = df.dropna(subset=['last_updated'])  # ลบแถวที่ไม่มีค่าใน last_updated

    print(f"[{datetime.now()}] Transformed to {len(df)} clean records.")

    return df

# === Step 3: Load ===
    # โหลดข้อมูลลงในฐานข้อมูล

def load(df):
    print(f"[{datetime.now()}] Starting load to database...")
    engine = create_engine('sqlite:///crypto_data.db', echo=True, connect_args={'timeout': 30})

    # ถ้าคอลัมน์ 'roi' เป็น dict หรือ None เราก็ปล่อยให้เป็น None
    df['roi'] = df['roi'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
    
    # แปลง None เป็น NULL ในคอลัมน์ที่เกี่ยวข้อง
    df = df.where(pd.notnull(df), None)

    # จากนั้นทำการ load ข้อมูลไปยังฐานข้อมูล
    df.to_sql(
    'crypto_prices',
    con=engine,
    if_exists='replace',
    index=False,
    dtype={
        'circulating_supply': BigInteger(),
        'total_supply': BigInteger(),
        'market_cap': BigInteger()
    }
)

    # ดูคอลัมน์ที่มีใน DataFrame
    print(df.columns)

    # ดูตัวอย่างข้อมูล
    print(df.head())    

# === Step 4: Schedule Job ===
def run_etl():
    data = extract()
    
    if data:
       df = transform(data)
       if not df.empty:
           load(df)
    
# Set job to run at 08:48
# schedule.every().day.at("08:48").do(run_etl)

run_etl()

print("ETL Scheduler started... (Ctrl+C to stop)")
while True:
    schedule.run_pending()
    time.sleep(60)
