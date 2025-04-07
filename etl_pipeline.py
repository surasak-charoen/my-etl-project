import requests
import json
import pandas as pd
from sqlalchemy import create_engine
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
        'currency', 'id', 'symbol', 'name', 'current_price', 'market_cap' #,
        #'market_cap_rank', 'total_volume', 'high_24h', 'low_24h',
        #'price_change_percentage_24h', 'circulating_supply',
        #'total_supply', 'max_supply'
        , 'last_updated', 'roi'
    ]
    
    df = pd.DataFrame(data)[columns]
    
    # แปลงประเภทข้อมูล
    df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce')  # แปลง last_updated เป็น datetime
    df = df.dropna(subset=['last_updated'])  # ลบแถวที่ไม่มีค่าใน last_updated
    df['current_price'] = pd.to_numeric(df['current_price'], errors='coerce')
    df = df.dropna(subset=['current_price'])
    df['market_cap'] = df['market_cap'].astype('int64')  # แปลงเป็น integer
    df = df.dropna(subset=['market_cap'])

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
    df.to_sql('crypto_prices', con=engine, if_exists='replace', index=False)

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

# Set job to run at 00:29
# schedule.every().day.at("08:48").do(run_etl)

run_etl()

print("ETL Scheduler started... (Ctrl+C to stop)")
while True:
    schedule.run_pending()
    time.sleep(60)
