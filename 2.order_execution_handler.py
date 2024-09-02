from datetime import datetime as dt
import queue
import alpaca_trade_api as tradeapi
from alpaca.data.historical.crypto import CryptoLatestTradeRequest
import logging
import concurrent.futures
import nest_asyncio
import asyncio
import websockets
import json
from event_alpaca_fees import FillEvent
import threading

class OrderExecutionHandler:
    """
    Handles the execution of trading orders and real-time trade updates for a 
    list of financial symbols using the Alpaca API.

    This class is responsible for:
    - Managing the execution of different types of orders (market, limit, stop-limit) based on incoming events.
    - Establishing and maintaining a WebSocket connection to receive real-time trade updates.
    - Processing trade updates to trigger appropriate actions such as logging, 
    updating the portfolio, and generating fill events.
    - Interacting with a portfolio object to execute trades in alignment with strategy signals.
    - Providing utility methods for placing various types of orders and fetching real-time prices.

    Attributes:
    - events (queue.Queue): Queue used to signal events, such as trade fills or 
    market data updates, to other parts of the system.
    - data_handler (RealTimeDataHandler): Instance responsible for managing and 
    processing real-time market data.
    - symbol_list (list): List of symbols (e.g., stock tickers) that the handler 
    is responsible for executing trades on.
    - portfolio (Portfolio): The portfolio object that tracks the current 
    positions and interacts with the execution handler.
    - api_key (str): API key for accessing Alpaca services.
    - api_secret (str): API secret for accessing Alpaca services.
    - base_url (str): Base URL for the Alpaca API
    - symbol_apis (dict): Dictionary mapping each symbol to its respective Alpaca 
    REST API client instance.
    - stream_initialized (bool): Flag indicating whether the trade update stream 
    has been initialized.
    - trade_logger (TradeLogger): Optional logger for recording trade details, 
    used for auditing and analysis.
    """
    def __init__(
        self, events, data_handler, alpaca_credentials, 
        portfolio, symbol_list, api=None, trade_logger=None
    ):
        self.events = events
        self.data_handler = data_handler
        self.symbol_list = symbol_list
        self.portfolio = portfolio
        self.api_key = alpaca_credentials['api_key']
        self.api_secret = alpaca_credentials['api_secret']
        self.base_url = alpaca_credentials['base_url']
        self.symbol_apis = {}
        self.stream_initialized = False 
        self.trade_logger = trade_logger 

        # Initialize an API client for each symbol in the symbol list
        for symbol in self.symbol_list:
            self.symbol_apis[symbol] = tradeapi.REST(
                alpaca_credentials['api_key'], 
                alpaca_credentials['api_secret'], 
                alpaca_credentials['base_url']
            )

    def initialize_trade_stream(self):
        # Initialize the trade update stream if it hasn't been initialized already
        if not self.stream_initialized:
            logging.info("Initializing trade update stream...")
            # Start the stream in a new thread to avoid blocking
            threading.Thread(target=self._start_trade_stream_thread).start()  
            self.stream_initialized = True
            logging.info("Trade update stream initialized")

    def _start_trade_stream_thread(self):
        # Start the async trade update stream in a separate thread to keep the main thread responsive
        asyncio.run(self.stream_trade_updates())

    async def stream_trade_updates(self):
        ws_url = "wss://paper-api.alpaca.markets/stream"
        
        while True:  # Continuously try to establish and maintain the WebSocket connection
            logging.info("Attempting to connect to trade update stream...")
            try:
                async with websockets.connect(ws_url) as websocket:
                    auth_data = {
                        "action": "auth",
                        "key": self.api_key,
                        "secret": self.api_secret
                    }
                    await websocket.send(json.dumps(auth_data))  # Authenticate with the WebSocket server
                    response = await websocket.recv()  # Receive authentication response
                    logging.info(f"Auth Response: {response}")

                    listen_message = {
                        "action": "listen",
                        "data": {
                            "streams": ["trade_updates"]
                        }
                    }
                    await websocket.send(json.dumps(listen_message))  # Subscribe to trade updates
                    response = await websocket.recv()  # Receive subscription confirmation
                    logging.info(f"Listen Response: {response}")

                    # Continuously listen for trade updates and process them using the trade callback
                    while True:
                        data = await websocket.recv()
                        data = json.loads(data)
                        stream = data.get('stream')
                        if stream == "trade_updates":
                            await self.trade_callback(data['data'])

            except Exception as e:
                logging.error(f"Failed to start trade update stream: {str(e)}")
                logging.info("Reconnecting to trade update stream in 5 seconds...")
                await asyncio.sleep(5)

    async def trade_callback(self, trade):
        # Callback function to process trade updates received via WebSocket
        try:
            order = trade['order']
            order_status = order['status']
            order_id = order['id']
            submit_timestamp = order['created_at']

            if order_status == 'filled' or order_status == 'partially_filled':
                symbol = order['symbol']
                fill_timestamp = trade['timestamp']
                order_type = order['type']
                limit_price = order['limit_price']
                truncated_timestamp = fill_timestamp[:26] + 'Z'
                filled_qty = float(order['filled_qty'])
                filled_avg_price = (float(order['filled_avg_price']) 
                    if order['filled_avg_price'] is not None else 0.0)
                direction = 'BUY' if order['side'] == 'buy' else 'SELL'

                # Create a FillEvent to record the execution details of the trade
                fill_event = FillEvent(
                    dt.utcnow(),
                    order_id,
                    symbol,
                    "ALPACA",
                    filled_qty,
                    direction,
                    filled_avg_price,
                    commission=float(filled_qty) * 0.005,
                )
                # Put the FillEvent in the event queue for further processing
                self.events.put(fill_event)  

                if self.trade_logger:
                    # Log the trade details if a trade logger is provided
                    self.trade_logger.log_trade_event(
                        submit_timestamp=submit_timestamp,
                        fill_timestamp=fill_timestamp,
                        order_id=order_id,
                        symbol=symbol,
                        direction=direction,
                        quantity=filled_qty,
                        limit_price=limit_price,
                        filled_qty=filled_qty,
                        filled_avg_price=filled_avg_price,
                        order_type=order_type,
                        status=order_status
                    )
                    self.trade_logger.save_to_file()

                logging.info(
                    f"Order {order_status}: symbol={symbol}, " 
                    f"filled_qty={filled_qty}, "
                    f"filled_avg_price={filled_avg_price}, "
                    f"timestamp={truncated_timestamp}"
                )

        except Exception as e:
            logging.error(
            f"Exception in trade_callback: {e}, trade data: {trade}"
        )

    def execute_order(self, event):
        # Execute an order when an ORDER event is received
        if event.type == 'ORDER':
            self._execute_order(event)

    def _execute_order(self, event):
        # Core method to handle order execution based on the event details
        try:
            side = 'buy' if event.direction == 'BUY' else 'sell'
            if event.order_type == 'MKT':
                order = self._place_order_Market(event)
            elif event.order_type in ['LMT', 'IOK']:
                last_close_price = round(self.data_handler.get_latest_bar_value(event.symbol, "close"), 2)
                if event.order_type == 'LMT':
                    order = self._place_order_Limit(event, last_close_price)
                else:
                    order = self._place_order_FOK_Limit(event, last_close_price)
            else:
                logging.error(f"Unknown order type {event.order_type}")
                return

            if order:
                logging.info(
                    f"Order submitted: Symbol={event.symbol}, "
                    f"Direction={event.direction}, Limit Price={order.limit_price}, Qty: {order.qty}"
                )

        except Exception as e:
            logging.error(f"Error executing order for {event.symbol}: {e}")

    def _place_order_Limit(self, order_event, limit_price):
        # Method to place a limit order via the Alpaca API
        try:
            symbol = order_event.symbol
            api = self.symbol_apis.get(symbol)

            logging.info(
                f"Submitting LMT order: Symbol={order_event.symbol}, "
                f"Quantity={order_event.quantity}, Direction={order_event.direction}"
            )
            side = 'buy' if order_event.direction == 'BUY' else 'sell'
            order = api.submit_order(
                symbol=symbol,
                qty=order_event.quantity,
                side=side,
                type='limit',
                time_in_force='gtc',
                limit_price=limit_price,
            )
            order.limit_price = limit_price
            return order
        except Exception as e:
            # Log any errors that occur during the placement of the limit order
            logging.error(f"Error placing limit order for {order_event.symbol}: {e}")
            return None

    def _place_order_FOK_Limit(self, order_event, limit_price):
        # Method to place a Fill-Or-Kill limit order via the Alpaca API
        try:
            side = 'buy' if order_event.direction == 'BUY' else 'sell'
            order = self.api.submit_order(
                symbol=order_event.symbol,
                qty=order_event.quantity,
                side=side,
                type='limit',
                time_in_force='fok',
                limit_price=limit_price
            )
            return order
        except Exception as e:
            logging.error(f"Error placing FOK limit order for {order_event.symbol}: {e}")
            return None

    def _place_order_Stop_Limit(self, order_event, stop_price, limit_price):
        # Method to place a Stop-Limit order via the Alpaca API
        try:
            side = 'buy' if order_event.direction == 'BUY' else 'sell'
            order = self.api.submit_order(
                symbol=order_event.symbol,
                qty=order_event.quantity,
                side=side,
                type='stop_limit',
                time_in_force='gtc',
                stop_price=stop_price,
                limit_price=limit_price
            )
            return order
        except Exception as e:
            logging.error(f"Error placing stop limit order for {order_event.symbol}: {e}")
            return None

    def _place_order_IOK_Stop_Limit(self, order_event, stop_price, limit_price):
        # Method to place an Immediate-Or-Cancel Stop-Limit order via the Alpaca API
        try:
            side = 'buy' if order_event.direction == 'BUY' else 'sell'
            order = self.api.submit_order(
                symbol=order_event.symbol,
                qty=order_event.quantity,
                side=side,
                type='stop_limit',
                time_in_force='iok',
                stop_price=stop_price,
                limit_price=limit_price
            )
            return order
        except Exception as e:
            logging.error(f"Error placing IOK stop limit order for {order_event.symbol}: {e}")
            return None

    def _place_order_Market(self, order_event):
        # Method to place a market order via the Alpaca API
        try:
            logging.info(
                f"Submitting market order: Symbol={order_event.symbol}, "
                f"Quantity={order_event.quantity}, Direction={order_event.direction}"
            )
            side = 'buy' if order_event.direction == 'BUY' else 'sell'
            order = self.api.submit_order(
                symbol=order_event.symbol,
                qty=order_event.quantity,
                side=side,
                type='market',
                time_in_force='gtc'
            )
            return order
        except Exception as e:
            logging.error(f"Error placing market order for {order_event.symbol}: {e}")
            return None

    def _get_real_time_price(self, symbol):
        # Fetch the real-time price of a given symbol using the Alpaca API
        try:
            latest_trade = self.api.get_latest_trade(symbol)
            price = latest_trade.price
            logging.info(f"Current market price for {symbol} is {price}")
            return price
        except Exception as e:
            logging.error(f"Error fetching real-time price for {symbol}: {e}")
            return None

    def _get_real_time_price_cypto(self, symbol):
        # Fetch the real-time price of a cryptocurrency symbol using Alpaca's crypto data API
        try:
            request_params = CryptoLatestTradeRequest(symbol_or_symbols=symbol)
            latest_trade = self.client.get_crypto_latest_trade(request_params)[symbol]
            price = latest_trade.price
            logging.info(f"Current market price for {symbol} is {price}")
            return price
        except Exception as e:
            logging.error(f"Error fetching real-time price for {symbol}: {e}")
            return None

    def _run_coroutine_in_thread(self, coroutine):
        # Helper method to run an asyncio coroutine in a separate thread to avoid blocking the main thread
        asyncio.run(coroutine)
