# -*- coding: utf-8 -*-
import requests
import time
import pandas as pd
import datetime
import uuid
from google.cloud import bigquery
import pandas_gbq
import os
import random # ★ 1. randomをインポート

# --- ★★★ 設定項目 (環境変数から読み込む) ★★★ ---
# (この部分は変更なし)
API_KEY = os.getenv("COINALYZE_API_KEY")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = "coinalyze_data"

# --- ★★★ ---
if not API_KEY:
    raise ValueError("環境変数 'COINALYZE_API_KEY' が設定されていません。GitHub Secretsを確認してください。")
if not PROJECT_ID:
    raise ValueError("環境変数 'GCP_PROJECT_ID' が設定されていません。GitHub Secretsを確認してください。")

TARGET_COINS = ["BTC", "ETH", "XRP", "SOL"]
DEBUG_MODE = False
DEBUG_RECORD_LIMIT = 5
OI_API_URL = "https://api.coinalyze.net/v1/open-interest-history"
PRICE_API_URL = "https://api.coinalyze.net/v1/ohlcv-history"
LSR_API_URL = "https://api.coinalyze.net/v1/long-short-ratio-history"
FR_API_URL = "https://api.coinalyze.net/v1/funding-rate-history"

# --- BigQuery テーブル操作関数 ---
# (この部分は変更なし)
def create_table_if_not_exists(client: bigquery.Client, table_id: str, schema_sql: str):
    try:
        client.get_table(table_id)
    except Exception:
        print(f"    テーブル {table_id} が存在しないため、新規作成します。")
        ddl = f"CREATE TABLE `{table_id}` ({schema_sql})"
        query_job = client.query(ddl)
        query_job.result()
        print(f"    ✅ テーブル {table_id} を作成しました。")

def setup_all_tables(client: bigquery.Client, coin_symbol: str):
    # ... (変更なし) ...

def save_data_to_bigquery(client: bigquery.Client, df: pd.DataFrame, table_name: str):
    # ... (変更なし) ...

# --- データ取得・処理関数 ---
def get_exchange_config(coin: str) -> dict:
    # ... (変更なし) ...

# ★ 2. fetch_api_data 関数をリトライ機能付きに置き換え
def fetch_api_data(url: str, params: dict, headers: dict, retries: int = 4, backoff_factor: float = 15.0) -> list:
    """
    APIにリクエストを送信する。レート制限(429)の場合、エクスポネンシャルバックオフでリトライする。
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # 200番台以外のステータスコードで例外を発生させる
            return response.json()
        except requests.exceptions.HTTPError as e:
            # 429エラーかつ、まだリトライ回数が残っている場合
            if e.response.status_code == 429 and attempt < retries - 1:
                # 待機時間を計算 (15s, 30s, 60s, ...) + ランダムな揺らぎ
                wait_time = backoff_factor * (2 ** attempt) + random.uniform(0, 1)
                print(f"    ⚠️ APIレート制限 (429) を検出。{wait_time:.1f}秒待機してリトライします... (試行 {attempt + 1}/{retries})")
                time.sleep(wait_time)
            else:
                # 429以外のエラー、または最後のリトライでも失敗した場合
                print(f"    ❌ APIリクエストに失敗しました (HTTP {e.response.status_code}): {e}")
                return []
        except requests.exceptions.RequestException as e:
            # タイムアウトや接続エラーなど
            print(f"    ❌ APIリクエストに失敗しました: {e}")
            # この場合もリトライを試みても良いが、今回は即時失敗とする
            return []

    # 全てのリトライが失敗した場合
    print(f"    ❌ {retries}回のリトライ後もAPIリクエストに失敗しました: {url}")
    return []


# その他の process_xxx_data_for_bq 関数は変更なし
def process_oi_data_for_bq(api_data: list) -> pd.DataFrame:
    # ... (変更なし) ...
def process_price_data_for_bq(price_history: list) -> pd.DataFrame:
    # ... (変更なし) ...
def process_lsr_data_for_bq(api_data: list) -> pd.DataFrame:
    # ... (変更なし) ...
def process_fr_data_for_bq(api_data: list) -> pd.DataFrame:
    # ... (変更なし) ...


# --- メイン実行部 ---
def main():
    jst = datetime.timezone(datetime.timedelta(hours=9))
    print(f"処理を開始します... ({datetime.datetime.now(jst).strftime('%Y-%m-%d %H:%M:%S')})")
    if DEBUG_MODE: print(f"🐞🐞🐞 DEBUG MODE IS ENABLED: DBには各データの先頭 {DEBUG_RECORD_LIMIT} 件のみ保存されます 🐞🐞🐞")
    
    try:
        client = bigquery.Client(project=PROJECT_ID)
        print(f"✅ Google Cloud への認証に成功しました。プロジェクト: {client.project}")

        for coin in TARGET_COINS:
            print(f"\n{'='*50}\n--- 通貨 [{coin}] の処理を開始 ---")
            setup_all_tables(client, coin)

            # --- 価格データ (変更なし) ---
            price_table_name = f"{coin.lower()}_price_history"; print(f"  -> 価格データを処理中...")
            price_params = {"symbols": f"{coin}USDT.6", "interval": "5min", "from": int(time.time()) - (86400 * 10), "to": int(time.time())}
            raw_price_data = fetch_api_data(PRICE_API_URL, params=price_params, headers={"api-key": API_KEY})
            price_history = raw_price_data[0].get("history", []) if raw_price_data and raw_price_data[0] else []
            price_df = process_price_data_for_bq(price_history)
            if DEBUG_MODE: price_df = price_df.head(DEBUG_RECORD_LIMIT)
            save_data_to_bigquery(client, price_df, price_table_name)
            
            exchange_config = get_exchange_config(coin)
            for ex_name, conf in exchange_config.items():
                print(f"\n  -> 取引所 [{ex_name}] のデータを処理中...")
                exchange_symbols = [f"{contract}{conf['code']}" for contract in conf['contracts']]
                common_params = {"symbols": ','.join(exchange_symbols), "interval": "5min", "from": int(time.time()) - (86400 * 10), "to": int(time.time())}
                oi_params = {**common_params, "convert_to_usd": "true"}; headers = {"api-key": API_KEY}

                # ★ 3. time.sleep() の見直し
                # 各APIコールの前に短い待機時間を設けて、リクエストが集中しすぎないようにする
                
                # OI
                time.sleep(2) # APIコール前に少し待つ
                oi_table_name = f"{coin.lower()}_{ex_name.lower()}_oi_history"
                raw_oi_data = fetch_api_data(OI_API_URL, params=oi_params, headers=headers)
                oi_df = process_oi_data_for_bq(raw_oi_data)
                if DEBUG_MODE: oi_df = oi_df.head(DEBUG_RECORD_LIMIT)
                save_data_to_bigquery(client, oi_df, oi_table_name)
                
                # LSR
                time.sleep(2) # APIコール前に少し待つ
                lsr_table_name = f"{coin.lower()}_{ex_name.lower()}_lsr_history"
                raw_lsr_data = fetch_api_data(LSR_API_URL, params=common_params, headers=headers)
                lsr_df = process_lsr_data_for_bq(raw_lsr_data)
                if DEBUG_MODE: lsr_df = lsr_df.head(DEBUG_RECORD_LIMIT)
                save_data_to_bigquery(client, lsr_df, lsr_table_name)
                
                # FR
                time.sleep(2) # APIコール前に少し待つ
                fr_table_name = f"{coin.lower()}_{ex_name.lower()}_funding_rate_history"
                raw_fr_data = fetch_api_data(FR_API_URL, params=common_params, headers=headers)
                fr_df = process_fr_data_for_bq(raw_fr_data)
                if DEBUG_MODE: fr_df = fr_df.head(DEBUG_RECORD_LIMIT)
                save_data_to_bigquery(client, fr_df, fr_table_name)
                
                # 取引所ごとの大きな待機時間は不要になるか、短くできる
                print("    ... 次の取引所処理まで5秒待機 ...")
                time.sleep(5) 

            if coin != TARGET_COINS[-1]: 
                print("    ... 次の通貨処理まで15秒待機 ...")
                time.sleep(15)

        print(f"\n{'='*50}\n🎉 全ての処理が正常に完了しました。")
    except Exception as e: 
        print(f"❌ 処理全体で致命的なエラーが発生しました: {e}")

# --- スクリプトの実行 ---
if __name__ == "__main__":
    main()
