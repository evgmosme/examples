import os
import shutil
import json
import pandas as pd
import psycopg2
import pytz
import time
import logging
from tqdm import tqdm
from datetime import datetime, timedelta
from alpaca_trade_api.rest import REST, TimeFrame, TimeFrameUnit
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import glob
import multiprocess as mp
from multiprocess import Manager, Value, Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append('/home/tradingbot/proxima/airflow_docer/lib/')
from data_fetcher import fetch_minute_data, fetch_minute_data_deep, \
    fetch_minute_data_pg
from indicators import calculate_indicators

# Ignore warnings that are not critical for the execution
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=RuntimeWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
warnings.simplefilter(action='ignore', category=DeprecationWarning)

# Setup logger to capture logs with time and severity level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - \
%(message)s')
logger = logging.getLogger(__name__)

"""
Description:
This DAG is designed to manage the entire lifecycle of processing and 
analyzing market data from Alpaca broker. It performs the following key 
tasks:

1. Fetches historical 30-minute bar data from the Alpaca API for a list 
   of financial symbols.
2. Inserts the fetched bar data into a PostgreSQL database.
3. Computes resistance levels and other indicators based on the fetched 
   data.
4. Inserts the computed levels and indicators into a PostgreSQL 
   database.
5. Fetches and processes trade quotes for the same symbols.
6. Inserts the trade quotes into a PostgreSQL database.
7. Aggregates the quote data into metrics such as quote count, average 
   spread, and volume imbalance.

The DAG is structured to execute these tasks in sequence, utilizing 
Python operators for each task. It also makes use of parallel processing 
where appropriate to handle the large volume of data efficiently.
""" 

BASE_URL = "https://paper-api.alpaca.markets"
API_KEY_JOHNYSSAN = os.getenv('ALPACA_API_KEY_JOHNYSSAN')
SECRET_KEY_JOHNYSSAN = os.getenv('ALPACA_SECRET_KEY_JOHNYSSAN')

# Retry settings are configured to handle temporary failures
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='04.ALPACA_Daily_Data_Pipeline',
    default_args=default_args,
    description='A DAG to fetch and insert 30-minute bars from Alpaca \
Broker from 2016, compute Levels and insert then into PostgreSQL',
    schedule_interval='0 17 * * 1-5',
    start_date=datetime(2024, 5, 30),
    catchup=False,
    tags=['bars', 'levels', 'indicators', 'alpaca', 'postgresql']
)

# Fetches the trading calendar from Alpaca API
def get_calendar(API_KEY, SECRET_KEY, BASE_URL):
    api = REST(API_KEY, SECRET_KEY, BASE_URL)

    start_date = datetime.utcnow().date() - timedelta(days=14)
    end_date = datetime.utcnow().date()

    calendar = api.get_calendar(start=start_date, end=end_date)
    calendar_dict_list = [cal._raw for cal in calendar]
    calendar_df = pd.DataFrame(calendar_dict_list)
    # Convert the trading hours to UTC timezone
    calendar_df['open_utc'] = pd.to_datetime(calendar_df['date'] + \
        ' ' + calendar_df['open'] + ':00').dt.tz_localize('America/New_York')\
        .dt.tz_convert('UTC').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    calendar_df['close_utc'] = pd.to_datetime(calendar_df['date'] + \
        ' ' + calendar_df['close'] + ':00').dt.tz_localize('America/New_York')\
        .dt.tz_convert('UTC').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    calendar_df['date'] = pd.to_datetime(calendar_df['date'])

    return calendar_df

# Fetches bar data from Alpaca API with retry logic to ensure data is 
# fetched
def fetch_bars(symbol, save_path, start_str, end_str):
    api_key_johnyssan = os.getenv('ALPACA_API_KEY_JOHNYSSAN')
    secret_key_johnyssan = os.getenv('ALPACA_SECRET_KEY_JOHNYSSAN')
    api = REST(api_key_johnyssan, secret_key_johnyssan)

    max_retries = 10
    retry_count = 0
    while retry_count < max_retries:
        try:
            data = api.get_bars(
                symbol,
                TimeFrame(30, TimeFrameUnit.Minute),
                start=start_str,
                end=end_str,
                adjustment='all',
                limit=None,
                feed='sip'
            ).df

            if not data.empty:
                data.reset_index(inplace=True)

                data['timestamp'] = pd.to_datetime(data['timestamp'])
                ny_tz = pytz.timezone('America/New_York')
                # Convert timestamps to New York timezone for market 
                # hours filtering
                data['timestamp'] = data['timestamp'].dt.tz_convert(ny_tz)
                data.set_index('timestamp', inplace=True)
                data = data.between_time('09:30', '15:30')
                utc_tz = pytz.timezone('UTC')
                # Convert timestamps back to UTC
                data.index = data.index.tz_convert(utc_tz)
                data.reset_index(inplace=True)
                data['symbol'] = symbol

                columns_order = [
                    'symbol',
                    'timestamp',
                    'open',
                    'high',
                    'low',
                    'close',
                    'volume',
                    'trade_count',
                    'vwap',
                ]

                data = data[columns_order]
                data.sort_values(by=['symbol', 'timestamp'], inplace=True)
                data.drop_duplicates(inplace=True)

                file_start_date_str = pd.to_datetime(
                    data['timestamp']
                ).dt.date.min()
                file_end_date_str = pd.to_datetime(
                    data['timestamp']
                ).dt.date.max()
                csv_file_path = os.path.join(
                    save_path, 
                    f'{symbol}_bars_{file_start_date_str}-{file_end_date_str}.csv'
                )
                data.to_csv(csv_file_path, index=False)

                return True

            else:
                return True

        except Exception as e:
            retry_count += 1
            # Retry on failure to handle transient issues with the API
            logging.warning(
                f"Retry {retry_count}/{max_retries} for symbol {symbol}. \
                Error: {e}"
            )
            time.sleep(10)

    # If all retries fail, return an empty DataFrame to avoid crashing 
    # the pipeline
    logging.error(
        f"All retries failed for symbol {symbol}. Returning empty DataFrame."
    )
    return pd.DataFrame()

# Fetches and processes bar data in parallel to improve performance
def fetch_and_process_bars(max_workers=10):
    symbols_csv_path = '/home/tradingbot/proxima/airflow_docer/data/russel3000_symbols.csv'
    symbols_russel_df = pd.read_csv(symbols_csv_path, header=None)
    symbols_russel_list = symbols_russel_df[0].tolist()
    
    save_path = '/mnt/database_ssd/data_to_db/bars'

    if os.path.exists(save_path):
        shutil.rmtree(save_path)
    os.makedirs(save_path)

    now = datetime.utcnow()
    end_str = now.strftime('%Y-%m-%dT%H:%M:00Z')
    start_str = (now - timedelta(days=3600)).strftime('%Y-%m-%dT%H:%M:00Z')

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_bars, symbol, save_path, 
        start_str, end_str) for symbol in symbols_russel_list]
        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                result = future.result()
                if not result:
                    logging.warning("Failed to fetch data for a symbol.")
            except Exception as e:
                logging.error(f"An error occurred: {e}")

# Inserts bar data into the PostgreSQL database
def split_into_batches(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# Uses the psql command for efficient bulk insertion into PostgreSQL
def insert_bars_batch(csv_file, conn_string, table_name):
    copy_cmd = [
        'psql',
        conn_string,
        '-c',
        f"\COPY {table_name} (symbol, timestamp, open, high, low, \
        close, volume, trade_count, vwap) FROM '{csv_file}' DELIMITER ',' \
        CSV HEADER"
    ]
    result = subprocess.run(copy_cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error(result.stderr)
    # else:
    #     logger.info(f"Batch inserted successfully from {csv_file}")

# Creates and truncates the PostgreSQL table to ensure fresh data is 
# loaded
def create_and_truncate_table_bars(conn_string, table_name):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        symbol TEXT,
        timestamp TIMESTAMPTZ NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INT,
        trade_count INT,
        vwap REAL,
        PRIMARY KEY (symbol, timestamp)
    ) WITH (fillfactor=100);
    """
    cursor.execute(create_table_query)
    conn.commit()

    truncate_query = f"TRUNCATE TABLE {table_name};"
    cursor.execute(truncate_query)
    conn.commit()

    cursor.close()
    conn.close()
    logger.info(f"Table {table_name} created and truncated.")

# Orchestrates the insertion of all bar data into the database
def insert_bars_data():
    CONN_STRING = os.getenv('POSTGRESQL_LIVE_CONN_STRING')
    TABLE_NAME = "alpaca_bars_30min"
    BARS_PATH = "/mnt/database_ssd/data_to_db/bars"

    create_and_truncate_table_bars(CONN_STRING, TABLE_NAME)

    csv_files = glob.glob(os.path.join(BARS_PATH, '*.csv'))

    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as \
    executor:
        futures = {executor.submit(insert_bars_batch, csv_file, 
        CONN_STRING, TABLE_NAME): csv_file for csv_file in csv_files}
        for future in tqdm(concurrent.futures.as_completed(futures), 
        total=len(futures), desc="Inserting Bars"):
            future.result()

################################### LEVELS INDICATORS ###################################

# Computes resistance levels based on historical data
def compute_resistance_levels(df, lookback_period, min_volume):
    """
    Finds long-term resistance levels in a DataFrame based on a given 
    resistance period and minimum volume.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame containing the OHLC (Open-High-Low-Close) data.
    resistance_period : int
        The number of preceding days(rows) to consider for finding the 
        maximum high.
    min_volume : int
        The minimum trading volume threshold in USD per day.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the long-term resistance levels.

    """
    resistance_levels = []
    resistance_levels_df = pd.DataFrame()

    for index in range(len(df) - 1, -1, -1):
        row = df.iloc[index]

        if index - lookback_period < 0:
            window_start = 0
        else:
            window_start = index - lookback_period

        max_high = df["high"][window_start:index].max()

        window_start_for_volume = max(0, index - 10)
        df_mean_vol = df.iloc[window_start_for_volume:index].copy()
        
        # Ensures that levels are identified only when the price is at 
        # or near its maximum
        if row["high"] >= max_high and (df_mean_vol['volume'] >= 
        min_volume).all():
            row_dict = {
                "level_price": row["high"], 
                "formation_date": df.index[index],
                'volume': row['volume']
                }
            resistance_levels.append(row_dict)

    if len(resistance_levels) != 0:
        resistance_levels_df = pd.DataFrame(resistance_levels)\
        .sort_values(by='formation_date')
        resistance_levels_df.set_index('formation_date', inplace=True, 
        drop=False)
        resistance_levels_df.drop("formation_date", axis=1, inplace=True)
    else:
        resistance_levels_df = pd.DataFrame()

    return resistance_levels_df

# Retrieves unique symbols from a PostgreSQL table to process them 
# individually
def get_unique_symbols(table, conn_string):
    """
    Fetch unique symbols from a table.
    """
    query = f"SELECT DISTINCT symbol FROM {table};"
    symbols = []

    with psycopg2.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            symbols = [row[0] for row in cur.fetchall()]

    return symbols

# Divides a list into chunks to allow parallel processing
def divide_list_into_chunks(lst, m):
    """
    Divide a list into `m` chunks.
    """
    n = len(lst)
    chunk_size = n // m
    for i in range(m - 1):
        yield lst[i * chunk_size: (i + 1) * chunk_size]
    yield lst[(m - 1) * chunk_size:]

# Generates a comprehensive DataFrame for each asset, including 
# resistance levels
def generate_comprehensive_df(
    assets_list, conn_string, start_str, end_str, table, min_volume, 
    levels_save_path, indicators_save_path, progress_counter, lock, 
    lookback_periods):
    """
    Generate comprehensive DataFrame for a list of assets.
    """
    for symbol in assets_list:
        levels = []

        bars_data = fetch_minute_data_pg(
            conn_string,
            symbol,
            start_str,
            end_str,
            table,
            intraday_start='00:00:00',
            intraday_end='23:59:00',
            float32=False,
            rounding=False,
            indicators=False
        )

        bars_data['average'] = bars_data[['open', 'high', 'low', 
        'close']].mean(axis=1)

        for lookback_period in lookback_periods:
            resistance_levels = compute_resistance_levels(bars_data, 
            lookback_period, min_volume)
            resistance_levels['lookback_period'] = lookback_period
            resistance_levels['symbol'] = symbol
            resistance_levels['min_volume'] = min_volume
            resistance_levels['min_volume'] = \
            resistance_levels['min_volume'].astype(int)
            levels.append(resistance_levels)

        concated_df = pd.concat(levels, axis=0)
        if not concated_df.empty:
            concated_df = concated_df[['symbol', 'lookback_period', 
            'volume', 'min_volume', 'level_price']]
            concated_df.index.name = 'timestamp'
            concated_df['timestamp'] = concated_df.index
            concated_df['volume'] = concated_df['volume'].astype(int)
            levels_save_file = os.path.join(levels_save_path, 
            f'{symbol}_levels.csv')
            concated_df = concated_df[['symbol', 'timestamp', 
            'lookback_period', 'volume', 'min_volume', 'level_price']]
            concated_df.to_csv(levels_save_file, index=False)

        with lock:
            # Progress tracking in parallel processing
            progress_counter.value += 1

# Orchestrates the calculation of resistance levels and indicators for 
# all symbols
def compute_levels_indicators():
    CONN_STRING = os.getenv('POSTGRESQL_LIVE_CONN_STRING')
    start_str = '2015-01-01 00:00:00'
    end_str = datetime.utcnow().strftime('%Y-%m-%d 19:30:00')
    TABLE = 'alpaca_bars_30min'
    min_volume = 10000
    lookback_periods = [500]
    levels_save_path = os.path.expanduser('/mnt/database_ssd/data_to_db/\
    levels_temp')
    indicators_save_path = os.path.expanduser('/mnt/database_ssd/data_to_db/\
    indicators_temp')

    if not os.path.exists(levels_save_path):
        os.makedirs(levels_save_path)
    
    if not os.path.exists(indicators_save_path):
        os.makedirs(indicators_save_path)

    symbols_to_process = get_unique_symbols(TABLE, CONN_STRING)
    n_processes = 19
    chunks = list(divide_list_into_chunks(symbols_to_process, 
    n_processes))

    manager = Manager()
    progress_counter = manager.Value('i', 0)
    lock = manager.Lock()

    total_symbols = len(symbols_to_process)

    processes = [mp.Process(target=generate_comprehensive_df, args=(
        chunk, 
        CONN_STRING, 
        start_str, 
        end_str, 
        TABLE, 
        min_volume, 
        levels_save_path, 
        indicators_save_path,
        progress_counter, 
        lock,
        lookback_periods
    )) for chunk in chunks]

    progress_bar = tqdm(total=total_symbols, desc="Computing Levels & \
    Indicators")

    for process in processes:
        process.start()

    # Ensures that progress is continuously updated during parallel 
    # processing
    while any(process.is_alive() for process in processes):
        progress_bar.n = progress_counter.value
        progress_bar.refresh()
        time.sleep(1)

    for process in processes:
        process.join()

    progress_bar.n = total_symbols
    progress_bar.refresh()
    progress_bar.close()

################################### INSERTING LEVELS ###################################

# Maps pandas data types to PostgreSQL data types
def pandas_dtype_to_postgres(dtype):
    DTYPE_MAPPING = {
        'int64': 'bigint',
        'float64': 'real',
        'object': 'text',
        'datetime64[ns]': 'timestamp',
        'datetime64[ns, UTC]': 'timestamp'
    }
    return DTYPE_MAPPING.get(str(dtype), 'text')

# Creates the PostgreSQL table for storing levels, ensuring proper 
# schema and truncation
def create_levels_table(conn_string, table_name, temp_df):
    primary_keys = ['symbol', 'timestamp', 'lookback_period', 
    'volume', 'min_volume', 'level_price']
    column_names = list(temp_df.columns)
    with psycopg2.connect(conn_string) as conn:
        with conn.cursor() as cur:
            columns_sql = ""
            for col in column_names:
                postgres_dtype = pandas_dtype_to_postgres(temp_df[col].dtype)
                columns_sql += f"{col} {postgres_dtype}, "
            columns_sql = columns_sql.rstrip(', ')
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns_sql},
                PRIMARY KEY ({', '.join(primary_keys)})
            );
            """
            cur.execute(create_table_query)
            conn.commit()
            truncate_query = f"TRUNCATE TABLE {table_name};"
            cur.execute(truncate_query)
            conn.commit()
    logger.info(f"Table {table_name} created and truncated successfully.")

# Creates the PostgreSQL table for storing indicators
def create_indicators_table(conn_string, table_name, temp_df):
    primary_keys = ['symbol', 'timestamp']
    column_names = list(temp_df.columns)
    with psycopg2.connect(conn_string) as conn:
        with conn.cursor() as cur:
            columns_sql = ""
            for col in column_names:
                postgres_dtype = pandas_dtype_to_postgres(temp_df[col].dtype)
                columns_sql += f"{col} {postgres_dtype}, "
            columns_sql = columns_sql.rstrip(', ')
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns_sql},
                PRIMARY KEY ({', '.join(primary_keys)})
            );
            """
            cur.execute(create_table_query)
            conn.commit()
            truncate_query = f"TRUNCATE TABLE {table_name};"
            cur.execute(truncate_query)
            conn.commit()
            
    logger.info(f"Table {table_name} created and truncated successfully.")

# Inserts data from a CSV file into the PostgreSQL table
def insert_file(file_path, conn_string, table_name):
    temp_df = pd.read_csv(file_path)
    temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'])
    column_names = list(temp_df.columns)
    if 'symbol' not in temp_df.columns:
        logger.error(f"'symbol' column missing in {file_path}")
        return
    if temp_df['symbol'].isnull().any():
        logger.error(f"'symbol' column contains null values in {file_path}")
        return

    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as \
    tmp_file:
        temp_df.to_csv(tmp_file.name, index=False, header=False)

    columns_str = ', '.join(column_names)
    copy_cmd = [
        'psql', 
        conn_string, 
        '-c', 
        f"\COPY {table_name} ({columns_str}) FROM '{tmp_file.name}' \
        DELIMITER ',' CSV"
    ]
    result = subprocess.run(copy_cmd, capture_output=True, text=True)

    os.remove(tmp_file.name)

    if result.returncode != 0:
        logger.error(result.stderr)

# Tracks progress while inserting data files
def insert_file_with_progress(file_path, conn_string, table_name, 
progress_counter, lock):
    insert_file(file_path, conn_string, table_name)
    with lock:
        progress_counter.value += 1

# Updates the progress bar as files are inserted
def update_progress_bar(progress_bar, progress_counter, lock, 
total_files):
    while progress_bar.n < total_files:
        with lock:
            progress_bar.n = progress_counter.value
        progress_bar.refresh()
        time.sleep(1)

# Inserts data from multiple CSV files into the PostgreSQL table in 
# parallel
def insert_data_from_csv(conn_string, folder_path, table_name, 
create_table_func, num_processes=19):
    files = [os.path.join(folder_path, file_name) for file_name in \
    os.listdir(folder_path) if file_name.endswith('.csv')]
    if files:
        temp_df = pd.read_csv(files[0])
        temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'])

        create_table_func(conn_string, table_name, temp_df)

    manager = mp.Manager()
    progress_counter = manager.Value('i', 0)
    lock = manager.Lock()

    total_files = len(files)
    processes = []

    progress_bar = tqdm(total=total_files, desc=f"Inserting data into \
    {table_name}")

    progress_updater = mp.Process(target=update_progress_bar, args=\
    (progress_bar, progress_counter, lock, total_files))
    progress_updater.start()

    with mp.Pool(processes=num_processes) as pool:
        pool.starmap(insert_file_with_progress, [(file_path, 
        conn_string, table_name, progress_counter, lock) for file_path 
        in files])

    progress_updater.join()

    progress_bar.n = total_files
    progress_bar.refresh()
    progress_bar.close()

    logger.info(f"Data inserted into {table_name}.")

# Clears temporary folders to ensure they are ready for the next 
# execution
def clear_temp_folders(folder_paths):
    for folder_path in folder_paths:
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
            os.makedirs(folder_path)
    logger.info("Temporary folders cleared.")

# Main function to orchestrate the insertion of all data (levels and 
# indicators)
def insert_data():
    CONN_STRING = os.getenv('POSTGRESQL_LIVE_CONN_STRING')
    levels_save_path = os.path.expanduser('/mnt/database_ssd/data_to_db/\
    levels_temp')
    indicators_save_path = os.path.expanduser('/mnt/database_ssd/data_to_db/\
    indicators_temp')

    insert_data_from_csv(CONN_STRING, levels_save_path, 'levels_table', 
    create_levels_table, num_processes=19)

    clear_temp_folders([levels_save_path, indicators_save_path])

################################### FETCH QUOTES ###################################

# Fetches trade quotes from Alpaca API with retry logic
def fetch_quotes(symbol, save_path, quotes_start_str, quotes_end_str):
    """
    Fetch trade quotes for a single symbol with retry logic.

    This function attempts to fetch trade quotes for the given symbol.
    It retries the request up to a maximum number of attempts if an 
    exception occurs during the fetching process. If all retries fail, 
    it returns an empty DataFrame.

    Parameters:
    symbol (str): The symbol for which to fetch trade quotes.
    quotes_start_str (str): The start time for fetching quotes.
    quotes_end_str (str): The end time for fetching quotes.

    Returns:
    bool: True if the fetch and save operation is successful, False 
    otherwise.
    """
    
    # Fetch quotes
    api_key_johnyssan = os.getenv('ALPACA_API_KEY_JOHNYSSAN')
    secret_key_johnyssan = os.getenv('ALPACA_SECRET_KEY_JOHNYSSAN')
    api = REST(api_key_johnyssan, secret_key_johnyssan)

    max_retries = 10
    retry_count = 0
    while retry_count < max_retries:
        try:
            data = api.get_quotes(symbol, start=quotes_start_str, 
            end=quotes_end_str, limit=None).df
            if not data.empty:
                data.reset_index(inplace=True)
                data['symbol'] = symbol

                columns_order = [
                    'symbol',
                    'timestamp',
                    'ask_price',
                    'ask_size',
                    'ask_exchange',
                    'bid_price',
                    'bid_size',
                    'bid_exchange',
                    'conditions',
                    'tape'
                ]

                data = data[columns_order]
                
                file_start_date_str = pd.to_datetime(data['timestamp'])\
                .dt.date.min()
                file_end_date_str = pd.to_datetime(data['timestamp'])\
                .dt.date.max()
                csv_file_path = os.path.join(save_path, 
                f'{symbol}_quotes_{file_start_date_str}-{file_end_date_str}.csv')
                data.to_csv(csv_file_path, index=False)

                return True

            else:
                return True

        except Exception as e:
            retry_count += 1
            # Retry on failure to handle transient issues with the API
            logging.warning(f"Retry {retry_count}/{max_retries} for symbol \
            {symbol}. Error: {e}")
            time.sleep(10)

    logging.error(f"All retries failed for symbol {symbol}. Returning empty \
    DataFrame.")
    return False

def split_into_batches(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# Fetches and processes quotes in parallel to speed up the process
def fetch_and_process_quotes(API_KEY, SECRET_KEY, BASE_URL):
    """
    Fetch and process trade quotes in parallel for each symbol.

    This function reads the symbols from a CSV file, fetches the trade 
    quotes for each symbol in parallel, processes the data, and inserts 
    it into the PostgreSQL database.

    Parameters:
    max_workers (int): The maximum number of worker threads to use for 
    parallel processing.
    """
    symbols_csv_path = '/home/tradingbot/proxima/airflow_docer/data/\
    russel3000_symbols.csv'
    symbols_russel_df = pd.read_csv(symbols_csv_path, header=None)
    symbols_russel_list = symbols_russel_df[0].tolist()

    save_path = '/mnt/database_ssd/data_to_db/quote2'
    if os.path.exists(save_path):
        shutil.rmtree(save_path)
    os.makedirs(save_path)
    
    trading_calendar = get_calendar(API_KEY, SECRET_KEY, BASE_URL)
    start_str = str(trading_calendar.iloc[-6]['open_utc'])
    end_str = str(trading_calendar.iloc[-1]['close_utc'])
    print(start_str, end_str)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_quotes, symbol, save_path, 
        start_str, end_str) for symbol in symbols_russel_list]
        for future in tqdm(futures, total=len(futures)):
            try:
                result = future.result()
                if not result:
                    logging.warning("Failed to fetch data for a symbol.")
            except Exception as e:
                logging.error(f"An error occurred: {e}")

################################### INSERT QUOTES ###################################

# Inserts trade quotes into the PostgreSQL database
def insert_batch(csv_file, conn_string, table_name):
    copy_cmd = [
        'psql',
        conn_string,
        '-c',
        f"\COPY {table_name} (symbol, timestamp, ask_price, ask_size, \
        ask_exchange, bid_price, bid_size, bid_exchange, conditions, tape) \
        FROM '{csv_file}' DELIMITER ',' CSV HEADER"
    ]
    result = subprocess.run(copy_cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error(result.stderr)

# Creates and truncates the PostgreSQL table to ensure fresh data is 
# loaded
def create_and_truncate_table(conn_string, table_name):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        quote_id BIGSERIAL,
        symbol text,
        timestamp timestamp(6) with time zone,
        ask_price real,
        ask_size int,
        ask_exchange text,
        bid_price real,
        bid_size int,
        bid_exchange text,
        conditions text,
        tape text,
        PRIMARY KEY(symbol, timestamp, quote_id)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Truncate table if it exists
    truncate_query = f"TRUNCATE TABLE {table_name};"
    cursor.execute(truncate_query)
    conn.commit()

    cursor.close()
    conn.close()
    logger.info(f"Table {table_name} created and truncated.")

# Orchestrates the insertion of all quote data into the database
def insert_quote_data():
    CONN_STRING = os.getenv('POSTGRESQL_LIVE_CONN_STRING')
    TABLE_NAME = "alpaca_quotes"
    QUOTE_PATH = "/mnt/database_ssd/data_to_db/quote2"

    create_and_truncate_table(CONN_STRING, TABLE_NAME)

    csv_files = glob.glob(os.path.join(QUOTE_PATH, '*.csv'))

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as \
    executor:
        futures = {executor.submit(insert_batch, csv_file, CONN_STRING, 
        TABLE_NAME): csv_file for csv_file in csv_files}
        for future in tqdm(concurrent.futures.as_completed(futures), 
        total=len(futures), desc="Inserting Quotes"):
            future.result()

    logging.info('Cooling down for 2 minutes')
    time.sleep(120)

################################### AGGREGATE QUOTES ###################################

# Creates and truncates the table for storing quote metrics
def create_and_truncate_metrics_table(conn_string, metrics_table_name):
    # Create a connection to the PostgreSQL database
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    # Define the SQL query to create the new metrics table
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {metrics_table_name} (
        symbol text,
        timestamp timestamp with time zone DEFAULT (current_timestamp \
        AT TIME ZONE 'UTC'),
        quoteCount int,
        avgSpread real,
        maxSpread real,
        totalBidVolume int,
        totalOfferVolume int,
        volumeImbalance int,
        PRIMARY KEY(symbol, timestamp)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Truncate the table if it exists to ensure data consistency
    truncate_table_query = f"TRUNCATE TABLE {metrics_table_name};"
    cursor.execute(truncate_table_query)
    conn.commit()

    cursor.close()
    conn.close()

# Retrieves all unique symbols from the quotes table to aggregate their 
# metrics
def fetch_all_symbols(table_name, conn_string):
    # Create a connection to the PostgreSQL database
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    # Define the SQL query to fetch all unique symbols
    query = f"SELECT DISTINCT symbol FROM {table_name};"
    
    # Execute the query and fetch the data
    cursor.execute(query)
    symbols = [row[0] for row in cursor.fetchall()]
    
    # Close the cursor and the connection
    cursor.close()
    conn.close()
    
    return symbols

# Aggregates quote metrics and inserts them into the PostgreSQL table
def aggregate_and_insert_metrics(symbol, source_table, metrics_table, 
conn_string):
    # Create a connection to the PostgreSQL database
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    # Define the SQL query to fetch and aggregate the data for the given 
    # symbol
    query = f"""
        INSERT INTO {metrics_table} (symbol, timestamp, quoteCount, 
        avgSpread, maxSpread, totalBidVolume, totalOfferVolume, 
        volumeImbalance)
        SELECT 
            symbol,
            (timestamp 'epoch' + INTERVAL '30 minute' * 
            FLOOR(EXTRACT(EPOCH FROM timestamp) / (30 * 60))) 
            AT TIME ZONE 'UTC' AS timeslot,
            COUNT(*) AS quoteCount,
            AVG(ask_price - bid_price) AS avgSpread,
            MAX(ask_price - bid_price) AS maxSpread,
            SUM(bid_size) AS totalBidVolume,
            SUM(ask_size) AS totalOfferVolume,
            SUM(bid_size) - SUM(ask_size) AS volumeImbalance
        FROM {source_table}
        WHERE symbol = %s
        GROUP BY symbol, timeslot
        ORDER BY symbol, timeslot;
        """
    
    # Execute the query
    cursor.execute(query, (symbol,))
    conn.commit()

    # Close the cursor and the connection
    cursor.close()
    conn.close()

# Filters out metrics that fall outside of standard trading hours
def filter_metrics_by_time(metrics_table_name, conn_string):
    # Create a connection to the PostgreSQL database
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    # Define the SQL query to filter the records based on time in 
    # America/New_York
    filter_query = f"""
    DELETE FROM {metrics_table_name}
    WHERE NOT (
        (timestamp AT TIME ZONE 'America/New_York')::time >= '09:30:00'::\
        time 
        AND (timestamp AT TIME ZONE 'America/New_York')::time < '16:00:00'::\
        time
    );
    """
    
    # Execute the query
    cursor.execute(filter_query)
    conn.commit()

    cursor.close()

    conn.close()

# Orchestrates the aggregation and insertion of quote metrics
def aggregate_quotes():

    CONN_STRING = os.getenv('POSTGRESQL_LIVE_CONN_STRING')
    SOURCE_TABLE_NAME = "alpaca_quotes"
    METRICS_TABLE_NAME = "alpaca_quotes_metrics"

    # Fetch all unique symbols from the table
    symbols = fetch_all_symbols(SOURCE_TABLE_NAME, CONN_STRING)
    
    # Create and truncate the metrics table to ensure fresh data is 
    # loaded
    create_and_truncate_metrics_table(CONN_STRING, METRICS_TABLE_NAME)
    
    # Use ThreadPoolExecutor to process each symbol concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as \
    executor:
        futures = {executor.submit(aggregate_and_insert_metrics, 
        symbol, SOURCE_TABLE_NAME, METRICS_TABLE_NAME, CONN_STRING): 
        symbol for symbol in symbols}
        for future in tqdm(concurrent.futures.as_completed(futures), 
        total=len(futures), desc="Aggregating quotes"):
            future.result()
    
    # Filter the metrics table by time to keep only relevant trading 
    # hours
    filter_metrics_by_time(METRICS_TABLE_NAME, CONN_STRING)

# Define the task dependencies in the DAG
fetch_and_process_bars_task = PythonOperator(
    task_id='fetch_and_process_bars_task',
    python_callable=fetch_and_process_bars,
    dag=dag,
)

insert_bars_data_task = PythonOperator(
    task_id='insert_bars_data_task',
    python_callable=insert_bars_data,
    dag=dag,
)

compute_levels_indicators_task = PythonOperator(
    task_id='compute_levels_indicators_task',
    python_callable=compute_levels_indicators,
    dag=dag,
)

insert_levels_indicators_task = PythonOperator(
    task_id='insert_levels_indicators_task',
    python_callable=insert_data,
    dag=dag,
)

fetch_quotes_task = PythonOperator(
    task_id='fetch_quotes_task',
    python_callable=fetch_and_process_quotes,
    op_args=[API_KEY_JOHNYSSAN, SECRET_KEY_JOHNYSSAN, BASE_URL],
    dag=dag,
)

insert_quotes_task = PythonOperator(
    task_id='insert_quotes_task',
    python_callable=insert_quote_data,
    dag=dag,
)

aggregate_quotes_task = PythonOperator(
    task_id='aggregate_quotes_task',
    python_callable=aggregate_quotes,
    dag=dag,
)

# Execute tasks in a sequence to maintain correct workflow
fetch_and_process_bars_task >> \
insert_bars_data_task >> \
compute_levels_indicators_task >> \
insert_levels_indicators_task >> \
fetch_quotes_task >> \
insert_quotes_task >> \
aggregate_quotes_task
