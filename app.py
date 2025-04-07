import streamlit as st
import sqlite3
import pandas as pd

# เชื่อมต่อกับฐานข้อมูล
conn = sqlite3.connect('crypto_data.db')
df = pd.read_sql('SELECT * FROM crypto_prices', conn)

# แสดงข้อมูลใน Streamlit
st.title('Crypto Prices Dashboard')
st.dataframe(df)
