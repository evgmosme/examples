# Examples of my code

This repository contains select components of my currently deployed algorithmic trading system.

## Algorithmic Trading System

This trading system is designed to execute real-time algorithmic trading strategies using the Alpaca API (US Broker). The system is architected around an event-driven framework, where different components such as data handlers, strategy engines, and execution handlers communicate through a centralized event queue. The system supports both live and backtesting modes, allowing for robust strategy development and testing in a production-like environment. It processes live data and submits orders to the broker with an execution time of approximately 5 milliseconds.

## RealTimeDataHandler

The `RealTimeDataHandler` is responsible for managing the real-time data feed from Alpaca. It streams, processes, and retains data for a list of financial symbols, ensuring that the most current information is available for strategy evaluation. The handler also applies retention policies to maintain memory efficiency and interacts with the portfolio to update market positions based on the latest data.

## AlpacaExecutionHandler

The `AlpacaExecutionHandler` manages the execution of trades and processes real-time trade updates. It interfaces with Alpaca's API to place various types of orders (market, limit, stop-limit) and handles trade confirmations through WebSocket streams. The handler ensures that trade signals generated by the strategy are executed efficiently and logs all trading activity for auditing and analysis.
