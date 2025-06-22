from pykrx.stock import get_market_ohlcv_by_date
import pymysql
import pandas as pd
from datetime import datetime, timedelta
import time

# ✅ 한글 → 영문 컬럼명 매핑
column_mapping = {
    '시가': 'ope',
    '고가': 'high',
    '저가': 'low',
    '종가': 'close',
    '거래량': 'volume',
    '거래대금': 'amount'
}

# ✅ DB 연결
conn = pymysql.connect(
    host="127.0.0.1",
    user="root",
    password="1234",
    database="dartfss",
    charset="utf8mb4"
)
cursor = conn.cursor()

# ✅ 종목 목록 가져오기
cursor.execute("SELECT DISTINCT stock_code FROM corporation WHERE stock_code IS NOT NULL")
stock_codes = [row[0] for row in cursor.fetchall()]

count = 0
for code in stock_codes:
    # 1️⃣ 기존 저장된 마지막 날짜 가져오기
    cursor.execute("SELECT MAX(date) FROM stock_price WHERE stock_code = %s", (code,))
    last_date = cursor.fetchone()[0]

    # 2️⃣ 시작일 계산 (없으면 19600101부터)
    if last_date is None:
        start_date = "19600101"
    else:
        start_date_dt = last_date + timedelta(days=1)
        start_date = start_date_dt.strftime("%Y%m%d")

    end_date = datetime.today().strftime("%Y%m%d")

    # 이미 최신이면 패스
    if start_date > end_date:
        print(f"⏩ 최신 상태: {code}")
        continue

    print(f"[{count + 1}] 📊 {code} 수집 중 → {start_date} ~ {end_date}")

    try:
        df = get_market_ohlcv_by_date(start_date, end_date, code)
        df = df.rename(columns=column_mapping).reset_index()

        if df.empty:
            print(f"⚠️ 스킵: {code} - 신규 데이터 없음")
            continue

        if 'amount' not in df.columns:
            df['amount'] = 0

        for _, row in df.iterrows():
            trade_date = row['날짜']
            if isinstance(trade_date, pd.Timestamp):
                trade_date = trade_date.strftime('%Y-%m-%d')

            cursor.execute("""
                INSERT IGNORE INTO stock_price (
                    stock_code, date, open, high, low, close, volume, amount
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                code,
                trade_date,
                float(row['open']),
                float(row['high']),
                float(row['low']),
                float(row['close']),
                int(row['volume']),
                int(row['amount'])
            ))

        conn.commit()
        print(f"✅ 저장 완료: {code}")
        count += 1

    except Exception as e:
        print(f"❌ [수집 실패] {code}: {e}")

    time.sleep(1)

# ✅ 마무리
cursor.close()
conn.close()
print(f"🎯 {count}개 종목 신규 데이터 수집 완료")
# 수정