# -*- coding: utf-8 -*-
import requests
import time
import pandas as pd
import datetime
import uuid
from google.cloud import bigquery
import pandas_gbq
import os # osモジュールをインポート

# --- ★★★ 設定項目 (環境変数から読み込む) ★★★ ---

# GitHub SecretsからAPIキーを読み込む
API_KEY = os.getenv("COINALYZE_API_KEY")

# GitHub SecretsからプロジェクトIDを読み込む
PROJECT_ID = os.getenv("GCP_PROJECT_ID")

# データセットIDは固定値として設定
DATASET_ID = "coinalyze_data"

# --- ★★★ ---

# --- スクリプトの前提条件チェック ---
if not API_KEY:
    raise ValueError("環境変数 'COINALYZE_API_KEY' が設定されていません。GitHub Secretsを確認してください。")
if not PROJECT_ID:
    raise ValueError("環境変数 'GCP_PROJECT_ID' が設定されていません。GitHub Secretsを確認してください。")


# 分析したい通貨のリスト
TARGET_COINS = ["BTC", "ETH", "XRP", "SOL"]

# デバッグ設定: Trueにすると各データの先頭5件のみ保存
DEBUG_MODE = False
DEBUG_RECORD_LIMIT = 5

# APIエンドポイント
OI_API_URL = "https://api.coinalyze.net/v1/open-interest-history"
PRICE_API_URL = "https://api.coinalyze.net/v1/ohlcv-history"
LSR_API_URL = "https://api.coinalyze.net/v1/long-short-ratio-history"
FR_API_URL = "https://api.coinalyze.net/v1/funding-rate-history"


# --- BigQuery テーブル操作関数 ---
# (関数の内容は変更なし)
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
    price_table_id = f"{PROJECT_ID}.{DATASET_ID}.{coin_symbol.lower()}_price_history"
    price_schema = "dt TIMESTAMP, date DATE, time TIME, open_price FLOAT64, high_price FLOAT64, low_price FLOAT64, close_price FLOAT64, volume FLOAT64"
    create_table_if_not_exists(client, price_table_id, price_schema)
    exchanges = ['Binance', 'Bybit', 'OKX', 'BitMEX']
    for ex_name in exchanges:
        oi_table_id = f"{PROJECT_ID}.{DATASET_ID}.{coin_symbol.lower()}_{ex_name.lower()}_oi_history"
        oi_schema = "dt TIMESTAMP, date DATE, time TIME, open_oi FLOAT64, high_oi FLOAT64, low_oi FLOAT64, close_oi FLOAT64"
        create_table_if_not_exists(client, oi_table_id, oi_schema)
        lsr_table_id = f"{PROJECT_ID}.{DATASET_ID}.{coin_symbol.lower()}_{ex_name.lower()}_lsr_history"
        lsr_schema = "dt TIMESTAMP, date DATE, time TIME, ratio FLOAT64, long_value FLOAT64, short_value FLOAT64"
        create_table_if_not_exists(client, lsr_table_id, lsr_schema)
        fr_table_id = f"{PROJECT_ID}.{DATASET_ID}.{coin_symbol.lower()}_{ex_name.lower()}_funding_rate_history"
        fr_schema = "dt TIMESTAMP, date DATE, time TIME, open_rate FLOAT64, high_rate FLOAT64, low_rate FLOAT64, close_rate FLOAT64"
        create_table_if_not_exists(client, fr_table_id, fr_schema)

def save_data_to_bigquery(client: bigquery.Client, df: pd.DataFrame, table_name: str):
    if df.empty:
        print(f"    [{table_name}] 保存するデータがありません。")
        return 0
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    temp_table_name = f"temp_{table_name}_{uuid.uuid4().hex}"
    temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.{temp_table_name}"
    try:
        # 認証情報は環境変数から自動で読み込まれる
        pandas_gbq.to_gbq(df, temp_table_id, project_id=PROJECT_ID, if_exists='replace')
        columns = [col for col in df.columns]
        merge_sql = f"""
            MERGE `{table_id}` T
            USING `{temp_table_id}` S
            ON T.dt = S.dt
            WHEN NOT MATCHED THEN
              INSERT ({', '.join(f'`{col}`' for col in columns)})
              VALUES ({', '.join(f'S.`{col}`' for col in columns)})
        """
        query_job = client.query(merge_sql)
        query_job.result()
        inserted_rows = query_job.num_dml_affected_rows if query_job.num_dml_affected_rows is not None else 0
        print(f"    ✅ [{table_name}] {len(df)}件を処理し、{inserted_rows}件の新規データを保存しました。")
        return inserted_rows
    except Exception as e:
        print(f"    ❌ [{table_name}] BigQueryへのマージ中にエラーが発生しました: {e}")
        return 0
    finally:
        client.delete_table(temp_table_id, not_found_ok=True)

# --- データ取得・処理関数 ---
# (関数の内容は変更なし)
def get_exchange_config(coin: str) -> dict: return {'Binance': {'code': 'A', 'contracts': [f'{coin}USD_PERP.', f'{coin}USDT_PERP.', f'{coin}USD.', f'{coin}USDT.']},'Bybit': {'code': '6', 'contracts': [f'{coin}USD.', f'{coin}USDT.']},'OKX': {'code': '3', 'contracts': [f'{coin}USD_PERP.', f'{coin}USDT_PERP.', f'{coin}USD.', f'{coin}USDT.']},'BitMEX': {'code': '0', 'contracts': [f'{coin}USD_PERP.', f'{coin}USDT_PERP.', f'{coin}USD.', f'{coin}USDT.']}}
def fetch_api_data(url: str, params: dict, headers: dict) -> list:
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e: print(f"    APIリクエストに失敗しました: {e}")
    except ValueError as e: print(f"    JSONの解析に失敗しました: {e}")
    return []
def process_oi_data_for_bq(api_data: list) -> pd.DataFrame:
    if not api_data: return pd.DataFrame()
    all_dfs = [pd.DataFrame(item['history']) for item in api_data if item.get('history')]
    if not all_dfs: return pd.DataFrame()
    df = pd.concat(all_dfs)
    df['dt'] = pd.to_datetime(df['t'], unit='s', utc=True)
    aggregated_df = df.groupby('dt').sum(numeric_only=True).rename(columns={'o': 'open_oi', 'h': 'high_oi', 'l': 'low_oi', 'c': 'close_oi'})
    dt_jst = aggregated_df.index.tz_convert('Asia/Tokyo')
    aggregated_df['date'] = dt_jst.date
    aggregated_df['time'] = dt_jst.time
    return aggregated_df.reset_index()[['dt', 'date', 'time', 'open_oi', 'high_oi', 'low_oi', 'close_oi']]
def process_price_data_for_bq(price_history: list) -> pd.DataFrame:
    if not price_history: return pd.DataFrame()
    df = pd.DataFrame(price_history)
    df['dt'] = pd.to_datetime(df['t'], unit='s', utc=True)
    df = df.rename(columns={'o': 'open_price', 'h': 'high_price', 'l': 'low_price', 'c': 'close_price', 'v': 'volume'})
    dt_jst = df['dt'].dt.tz_convert('Asia/Tokyo')
    df['date'] = dt_jst.dt.date
    df['time'] = dt_jst.dt.time
    return df[['dt', 'date', 'time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']]
def process_lsr_data_for_bq(api_data: list) -> pd.DataFrame:
    if not api_data: return pd.DataFrame()
    all_dfs = [pd.DataFrame(item['history']) for item in api_data if item.get('history')]
    if not all_dfs: return pd.DataFrame()
    df = pd.concat(all_dfs)
    df['dt'] = pd.to_datetime(df['t'], unit='s', utc=True)
    aggregated_df = df.groupby('dt').agg({'r': 'mean', 'l': 'sum', 's': 'sum'}).rename(columns={'r': 'ratio', 'l': 'long_value', 's': 'short_value'})
    dt_jst = aggregated_df.index.tz_convert('Asia/Tokyo')
    aggregated_df['date'] = dt_jst.date
    aggregated_df['time'] = dt_jst.time
    return aggregated_df.reset_index()[['dt', 'date', 'time', 'ratio', 'long_value', 'short_value']]
def process_fr_data_for_bq(api_data: list) -> pd.DataFrame:
    if not api_data: return pd.DataFrame()
    all_dfs = [pd.DataFrame(item['history']) for item in api_data if item.get('history')]
    if not all_dfs: return pd.DataFrame()
    df = pd.concat(all_dfs)
    df['dt'] = pd.to_datetime(df['t'], unit='s', utc=True)
    aggregated_df = df.groupby('dt').mean(numeric_only=True).rename(columns={'o': 'open_rate', 'h': 'high_rate', 'l': 'low_rate', 'c': 'close_rate'})
    dt_jst = aggregated_df.index.tz_convert('Asia/Tokyo')
    aggregated_df['date'] = dt_jst.date
    aggregated_df['time'] = dt_jst.time
    return aggregated_df.reset_index()[['dt', 'date', 'time', 'open_rate', 'high_rate', 'low_rate', 'close_rate']]

# --- メイン実行部 ---
def main():
    jst = datetime.timezone(datetime.timedelta(hours=9))
    print(f"処理を開始します... ({datetime.datetime.now(jst).strftime('%Y-%m-%d %H:%M:%S')})")
    if DEBUG_MODE: print(f"🐞🐞🐞 DEBUG MODE IS ENABLED: DBには各データの先頭 {DEBUG_RECORD_LIMIT} 件のみ保存されます 🐞🐞🐞")
    
    try:
        # GitHub Actionsの環境では、`google-github-actions/auth` で設定された
        # 認証情報が自動的に bigquery.Client に適用される
        client = bigquery.Client(project=PROJECT_ID)
        print(f"✅ Google Cloud への認証に成功しました。プロジェクト: {client.project}")

        for coin in TARGET_COINS:
            print(f"\n{'='*50}\n--- 通貨 [{coin}] の処理を開始 ---")
            setup_all_tables(client, coin)
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
                oi_table_name = f"{coin.lower()}_{ex_name.lower()}_oi_history"
                raw_oi_data = fetch_api_data(OI_API_URL, params=oi_params, headers=headers)
                oi_df = process_oi_data_for_bq(raw_oi_data)
                if DEBUG_MODE: oi_df = oi_df.head(DEBUG_RECORD_LIMIT)
                save_data_to_bigquery(client, oi_df, oi_table_name)
                lsr_table_name = f"{coin.lower()}_{ex_name.lower()}_lsr_history"
                time.sleep(10)
                raw_lsr_data = fetch_api_data(LSR_API_URL, params=common_params, headers=headers)
                lsr_df = process_lsr_data_for_bq(raw_lsr_data)
                if DEBUG_MODE: lsr_df = lsr_df.head(DEBUG_RECORD_LIMIT)
                save_data_to_bigquery(client, lsr_df, lsr_table_name)
                fr_table_name = f"{coin.lower()}_{ex_name.lower()}_funding_rate_history"
                time.sleep(10)
                raw_fr_data = fetch_api_data(FR_API_URL, params=common_params, headers=headers)
                fr_df = process_fr_data_for_bq(raw_fr_data)
                if DEBUG_MODE: fr_df = fr_df.head(DEBUG_RECORD_LIMIT)
                save_data_to_bigquery(client, fr_df, fr_table_name)
                print("    ... API負荷軽減のため20秒待機 ...")
                time.sleep(20)
            if coin != TARGET_COINS[-1]: time.sleep(30)
        print(f"\n{'='*50}\n🎉 全ての処理が正常に完了しました。")
    except Exception as e: print(f"❌ 処理全体で致命的なエラーが発生しました: {e}")

# --- スクリプトの実行 ---
if __name__ == "__main__":
    main()
