import numpy as np
import pandas as pd
from event import MarketEvent
from datetime import datetime as dt, timedelta, timezone, time
import asyncio
import time as t
from alpaca_trade_api.rest import TimeFrame, TimeFrameUnit
from alpaca_trade_api.stream import Stream
from alpaca_trade_api.common import URL
import alpaca_trade_api as tradeapi
import threading
import nest_asyncio
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
nest_asyncio.apply()

class RealTimeDataHandler:
    """
    Handles real-time data streaming, processing, and retention for a list of financial symbols.
    
    This class is responsible for:
    - Initiating and managing the Alpaca Broker data stream in a separate thread.
    - Handling incoming trade data and updating internal data structures with the latest information.
    - Applying a retention policy to limit the amount of stored data, ensuring memory efficiency.
    - Fetching and updating the latest market data (both real-time and historical) for the given symbols.
    - Interacting with a portfolio object to update market positions based on the most recent data.
    - Providing utility methods to retrieve the latest market data for individual symbols.

    Attributes:
    - events (queue.Queue): Queue used to signal events to other parts of the system.
    - symbol_list (list): List of symbols (e.g., stock tickers) to be tracked and processed.
    - latest_symbol_data (dict): Dictionary containing the most recent data for each symbol.
    - streamed_data (dict): Dictionary holding streamed trade data for each symbol.
    - ols_window (int): The window size used for certain calculations (e.g., regression).
    - new_data_available (bool): Flag indicating whether new data has been received.
    - stream_initialized (bool): Flag indicating whether the data stream has been initialized.
    - api (alpaca_trade_api.REST): Instance of the Alpaca REST API client.
    - api_key (str): API key for accessing Alpaca services.
    - api_secret (str): API secret for accessing Alpaca services.
    - base_url (str): Base URL for the Alpaca API.
    - retention_period (timedelta): Time duration for which the data is retained.
    - portfolio (Portfolio): The portfolio object that is updated based on the latest market data.

    Methods:
    - __init__(self, events, symbol_list, strat_params_list, api=None, alpaca_credentials=None): 
      Initializes the data handler with the necessary parameters like the event queue, 
      symbol list, and Alpaca API credentials.
    - trade_callback(self, trade): Handles incoming trade data, updating the internal 
      DataFrame with the latest trade information asynchronously.
    - apply_retention_policy(self): Retains only the most recent data based on a 
      retention period to ensure memory efficiency.
    - stream_data(self): Initiates the data stream and subscribes to trade updates 
      for the symbols in the list asynchronously.
    - _run_coroutine_in_thread(self, coroutine): Runs an asynchronous coroutine in a 
      separate thread to prevent blocking the main thread.
    - initialize_stream(self): Ensures that the data stream is initialized only once, s
      tarting it in a separate thread to avoid blocking.
    - fetch_new_data(self): Fetches the latest market data and updates the handler 
      with new information, canceling any unfilled orders.
    - _handle_sleep_until_next_minute(self): Calculates the sleep duration to align 
      data fetching with the next minute mark.
    - extract_latest_streamed_data(self): Extracts the latest data from the streamed data, 
      pulling the most recent prices for analysis.
    - fetch_minute_bars(self, symbols, current_minute): Fetches minute bars for the given 
      symbols and current minute, ensuring data is filtered and converted to UTC.
    - fetch_minute_bars_crypto(self, symbols, current_minute): Fetches minute bars 
      for cryptocurrency symbols, adjusted for 24/7 market data.
    - trading_schedule(self): Retrieves the trading calendar of the US stock market 
      to determine when the market is open and closed.
    """
    def __init__(self, events, symbol_list, strat_params_list, api=None, alpaca_credentials=None):
        logging.info('Initiating Data Handler')
        self.events = events
        self.symbol_list = symbol_list
        self.latest_symbol_data = {symbol: pd.DataFrame() for symbol in symbol_list}
        self.streamed_data = {symbol: pd.DataFrame(columns=['timestamp', 'close']) for symbol in symbol_list}
        self.ols_window = strat_params_list[0]['ols_window']
        self.new_data_available = False
        self.stream_initialized = False
        self.api = tradeapi.REST(alpaca_credentials['api_key'], alpaca_credentials['api_secret'], base_url=URL(alpaca_credentials['base_url']))
        self.api_key = alpaca_credentials['api_key']
        self.api_secret = alpaca_credentials['api_secret']
        self.base_url = alpaca_credentials['base_url']
        self.retention_period = timedelta(seconds=30)
        self.portfolio = None

    async def trade_callback(self, trade):
        # Handles incoming trade data, updates the DataFrame with the latest trade information
        # The callback is asynchronous to allow the data to be processed in real-time without blocking other tasks
        symbol = trade.symbol
        timestamp = pd.to_datetime(trade.timestamp)
        price = trade.price
        df_row = pd.DataFrame({'timestamp': [timestamp], 'close': [price]}, index=[timestamp])
        if not df_row.empty:
            if not self.streamed_data[symbol].empty:
                # If there is existing data for the symbol, append the new trade to it
                self.streamed_data[symbol] = pd.concat([self.streamed_data[symbol], df_row])
            else:
                # If no data exists for the symbol, initialize the data frame with the first trade
                # This avoids issues with empty data structures
                self.streamed_data[symbol] = df_row

    def apply_retention_policy(self):
        # Retains only the most recent data based on the retention period, ensuring memory efficiency
        # This prevents the system from holding onto too much historical data, which could slow down processing or consume excessive memory
        for symbol in self.symbol_list:
            if not self.streamed_data[symbol].empty:
                current_time = pd.Timestamp.now(tz=timezone.utc)
                retention_threshold = current_time - self.retention_period
                before_count = len(self.streamed_data[symbol])
                self.streamed_data[symbol] = self.streamed_data[symbol][self.streamed_data[symbol].index >= retention_threshold]
                after_count = len(self.streamed_data[symbol])
                cleared_count = before_count - after_count
                if cleared_count > 0:
                    logging.info(f"Cleared {cleared_count} old records for symbol {symbol} based on the retention policy.")

    async def stream_data(self):
        # Initiates data stream and subscribes to trade updates for the symbols in the list
        # The streaming is done asynchronously to allow the system to handle real-time data without blocking other operations
        logging.info("Attempting to connect to stream...")
        try:
            stream = Stream(self.api_key, self.api_secret, base_url=URL(self.base_url), data_feed='sip')
            for symbol in self.symbol_list:
                # Replace '/' in symbols with empty string as the Alpaca API does not support symbols with '/' in them
                symbol = symbol.replace('/', '') if '/' in symbol else symbol
                logging.info(f"Subscribing to trades for {symbol}")
                stream.subscribe_trades(self.trade_callback, symbol)
            await stream._run_forever()
        except Exception as e:
            logging.error(f"Failed to start stream: {str(e)}")

    def _run_coroutine_in_thread(self, coroutine):
        # Runs an asynchronous coroutine in a separate thread to prevent blocking the main thread
        asyncio.run(coroutine)

    def initialize_stream(self):
        # Ensures that the data stream is initialized only once
        # This prevents multiple threads from trying to start the stream simultaneously, which could cause errors or inconsistencies
        # The stream is run in a separate thread to avoid blocking the main thread,
        # allowing the program to continue executing other tasks concurrently.
        if not self.stream_initialized:
            logging.info("Initializing stream...")
            threading.Thread(target=self._run_coroutine_in_thread, args=(self.stream_data(),)).start()
            self.stream_initialized = True
            logging.info("Stream initialized")

    def fetch_new_data(self):
        # Fetches the latest market data and updates the handler with new information
        # This method is central to ensuring that the latest data is always available for decision-making
        # Cancel all unfilled orders to start fresh with the latest market conditions
        self.api.cancel_all_orders()
        logging.info("Unfilled orders cancelled.")
        now = dt.utcnow().replace(tzinfo=timezone.utc)
        current_minute = now.replace(second=0, microsecond=0) - timedelta(hours=0)
        logging.info(f"Current minute: {current_minute}")

        # Update the portfolio with the latest market positions based on the new data
        # This ensures that trading decisions are made with the most accurate and current information
        self.portfolio.update_market_positions()
        logging.info("Updated market positions.")
        
        df_minute_bars = self.fetch_minute_bars(self.symbol_list, current_minute)
        df_minute_bars = df_minute_bars.groupby('symbol').tail(self.ols_window-0)
        # logging.info(df_minute_bars)
        
        self.latest_symbol_data = {symbol: pd.DataFrame() for symbol in self.symbol_list}

        if not df_minute_bars.empty:
            for symbol in self.symbol_list:
                symbol_data = df_minute_bars[df_minute_bars['symbol'] == symbol]
                if not symbol_data.empty:
                    # Update the latest_symbol_data with the newly fetched data
                    # This ensures that each symbol's data is up-to-date and ready for analysis
                    self.latest_symbol_data[symbol] = pd.concat([self.latest_symbol_data[symbol], symbol_data])
                    self.latest_symbol_data[symbol] = self.latest_symbol_data[symbol].sort_values(by='timestamp')
                    self.latest_symbol_data[symbol] = self.latest_symbol_data[symbol].drop_duplicates(subset='timestamp')
        
        self.apply_retention_policy()
        # Synchronize with the next minute
        self._handle_sleep_until_next_minute()
        latest_data = self.extract_latest_streamed_data()
        
        if latest_data:
            latest_df = pd.DataFrame(latest_data)
            for symbol in self.symbol_list:
                symbol_data = latest_df[latest_df['symbol'] == symbol]
                if not symbol_data.empty:
                    # Append the latest streamed data to the symbol's data frame, ensuring it's up-to-date
                    # This combination of real-time and historical data allows for comprehensive analysis
                    self.latest_symbol_data[symbol] = pd.concat([self.latest_symbol_data[symbol], symbol_data])
                    self.latest_symbol_data[symbol] = self.latest_symbol_data[symbol].sort_values(by='timestamp')
                    # logging.info(self.latest_symbol_data[symbol])
                    # latest_price = self.latest_symbol_data[symbol].iloc[-1]['close']
                    # logging.info(f"Latest price for {symbol}: {latest_price}")
            if all(len(self.latest_symbol_data[symbol]) >= self.ols_window for symbol in self.symbol_list):
                # If enough data points are available for each symbol, trigger a market event
                # This event will signal other parts of the system that new data is ready for processing
                self.events.put(MarketEvent())
            else:
                logging.info("Not enough data points to proceed after adding latest streamed data.")
        
    def _handle_sleep_until_next_minute(self):
        # Calculates the sleep duration to align data fetching with the next minute mark
        # This ensures that data fetching is synchronized with the market data updates, 
        # which occur at regular intervals (e.g., every minute)
        now = dt.utcnow().replace(tzinfo=timezone.utc)
        next_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        sleep_duration = (next_minute - dt.utcnow().replace(tzinfo=timezone.utc)).total_seconds()
        logging.info(f"Sleeping for {sleep_duration} seconds until the next minute.")
        t.sleep(sleep_duration)
        logging.info("Next minute reached")

    def extract_latest_streamed_data(self):
        # Extracts the latest data from the streamed data
        # This method is essential for pulling the most recent prices from 
        # the stream, which are then used for analysis and decision-making
        latest_data = []
        for symbol in self.symbol_list:
            if not self.streamed_data[symbol].empty:
                # Get the latest timestamp and price for each symbol from the streamed data
                # This is crucial for ensuring that the most current data is available for processing
                latest_timestamp = self.streamed_data[symbol].index[-1]
                latest_price = round(self.streamed_data[symbol]['close'].iloc[-1], 2)
                latest_data.append({'symbol': symbol, 'timestamp': latest_timestamp, 'close': latest_price})
        return latest_data   

    def fetch_minute_bars(self, symbols, current_minute):
        # Fetches minute bars for the given symbols and current minute, 
        # ensuring data is filtered and converted to UTC
        # The method uses the trading schedule to determine the appropriate time window for fetching data
        calendar_df = self.trading_schedule()
        start_date = calendar_df.iloc[-2]['open_utc']

        if current_minute.time() < time(13, 30):
            # If it's before the market close, fetch data up to one minute before the last close
            end_date = pd.to_datetime(calendar_df.iloc[-2]['close_utc']) - timedelta(minutes=1)
            end_date = end_date.isoformat()
        else:
            # If it's after the market close, fetch data up to the current minute or slightly beyond
            end_date = calendar_df.iloc[-1]['close_utc']
            end_date = pd.to_datetime(calendar_df.iloc[-1]['close_utc']) + timedelta(hours=0)
            end_date = end_date.isoformat()

        target_data = self.api.get_bars(
            symbols,
            TimeFrame(1, TimeFrameUnit.Minute),
            start=start_date,
            end=end_date,
            adjustment='all',
            limit=None,
            feed='sip'
        ).df

        target_data.reset_index(inplace=True)
        target_data = target_data[['symbol', 'timestamp', 'close']]
        # Convert the timestamp to New York time for consistency with market hours
        target_data['timestamp'] = target_data['timestamp'].dt.tz_convert('America/New_York')
        # Filter out data outside regular trading hours to focus on the most relevant market data
        target_data = target_data[(target_data['timestamp'].dt.time >= time(4, 00)) & 
                                (target_data['timestamp'].dt.time <= time(19, 59))]
        # Convert the timestamp back to UTC for storage and further processing
        target_data['timestamp'] = target_data['timestamp'].dt.tz_convert('UTC')
        target_data = target_data.sort_values(by=['symbol', 'timestamp'])
        # Ensure that only data up to the current minute is included, 
        # excluding any potential future timestamps
        target_data = target_data[target_data['timestamp'] <= current_minute]
        return target_data.round(2)

    def fetch_minute_bars_crypto(self, symbols, current_minute):
        # Fetches minute bars for cryptocurrency symbols, similar to the stock method but for 24/7 markets
        calendar_df = self.trading_schedule()
        start_date = calendar_df.iloc[-2]['open_utc']
        end_date = current_minute.strftime('%Y-%m-%dT%H:%M:%SZ')
        target_data = self.api.get_crypto_bars(
            symbols,
            TimeFrame(1, TimeFrameUnit.Minute),
            start=start_date,
            end=end_date,
            limit=None,
            loc='us'
        ).df

        target_data.reset_index(inplace=True)
        target_data = target_data[['symbol', 'timestamp', 'close']]
        # Sort the data by symbol and timestamp to ensure it's in the correct order for processing
        target_data = target_data.sort_values(by=['symbol', 'timestamp'])
        # Ensure that only data up to the current minute is included, which is crucial for real-time accuracy
        target_data = target_data[target_data['timestamp'] <= current_minute]
        return target_data

    def trading_schedule(self):
        # Retrieves the trading calendar of the US stock market
        # This method fetches the trading calendar to determine when the market is open and closed.
        # It helps to ensure that data processing and trading decisions are aligned with actual market hours.
        start_date = dt.now(timezone.utc).date() - timedelta(days=14)
        end_date = dt.now(timezone.utc).date()

        calendar = self.api.get_calendar(start=start_date, end=end_date)
        calendar_dict_list = [cal._raw for cal in calendar]
        calendar_df = pd.DataFrame(calendar_dict_list)
        calendar_df['session_open'] = pd.to_datetime(
            calendar_df['session_open'], format='%H%M'
        ).dt.strftime('%H:%M')
        calendar_df['session_close'] = pd.to_datetime(
            calendar_df['session_close'], format='%H%M'
        ).dt.strftime('%H:%M')
        calendar_df['session_open_utc'] = pd.to_datetime(
            calendar_df['date'] + ' ' + calendar_df['session_open'] + ':00'
        ).dt.tz_localize('America/New_York').dt.tz_convert('UTC').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        calendar_df['session_close_utc'] = pd.to_datetime(
            calendar_df['date'] + ' ' + calendar_df['session_close'] + ':00'
        ).dt.tz_localize('America/New_York').dt.tz_convert('UTC').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        calendar_df['open_utc'] = pd.to_datetime(
            calendar_df['date'] + ' ' + calendar_df['open'] + ':00'
        ).dt.tz_localize('America/New_York').dt.tz_convert('UTC').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        calendar_df['close_utc'] = pd.to_datetime(
            calendar_df['date'] + ' ' + calendar_df['close'] + ':00'
        ).dt.tz_localize('America/New_York').dt.tz_convert('UTC').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        calendar_df['date'] = pd.to_datetime(calendar_df['date']) 
        return calendar_df