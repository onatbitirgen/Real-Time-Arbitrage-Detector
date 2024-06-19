import logging
import websocket
import threading
import json
import time
from datetime import datetime
from queue import Queue, Empty
import requests

def get_top_450_coins_by_liquidity():
    url = "https://data-api.binance.vision/api/v3/ticker/24hr"
    response = requests.get(url)
    data = response.json()

    # Adding a simple liquidity measure: bid-ask spread
    # Lower spread generally indicates higher liquidity
    for coin in data:
        if float(coin['askPrice']) > 0 and float(coin['bidPrice']) > 0:
            coin['spread'] = float(coin['askPrice']) - float(coin['bidPrice'])
        else:
            coin['spread'] = float('inf')  # Assigning a high value to coins with no bids/asks

    # Sorting based on a combination of volume and spread
    # This is a simplistic approach and can be adjusted
    sorted_coins = sorted(data, key=lambda x: (float(x['quoteVolume']), x['spread']), reverse=True)

    # Get the symbols of the top 400 coins
    top_450_coin_symbols = [coin['symbol'] for coin in sorted_coins[:450]]
    return top_450_coin_symbols

top_450_coin_symbols = get_top_450_coins_by_liquidity()



from itertools import combinations

# Example list of trading pairs
trading_pairs = top_450_coin_symbols

# Known currencies
known_currencies = {'BTC', 'ETH', 'USDT', 'XRP', 'BNB', 'ADA', 'SOL', 'DOT', 'LTC', 'LINK', 'AVAX','EUR'}

# Function to extract base and quote from a trading pair
def split_pair(pair):
    for currency in known_currencies:
        if pair.startswith(currency):
            return currency, pair[len(currency):]
        elif pair.endswith(currency):
            return pair[:-len(currency)], currency
    return None, None  # If no known currency is found

# Step 1: Extract unique currencies
currencies = set()
for pair in trading_pairs:
    base, quote = split_pair(pair)
    if base and quote:
        currencies.update([base, quote])

# Step 2: Form all possible triangular combinations
triangular_combinations = list(combinations(currencies, 3))

# Step 3: Check for existence of all three pairs in each combination
valid_triangular_sets = []
for combo in triangular_combinations:
    pairs = [
        combo[0] + combo[1],
        combo[1] + combo[2],
        combo[0] + combo[2]
    ]
    if all(pair in trading_pairs or pair[::-1] in trading_pairs for pair in pairs):
        valid_triangular_sets.append(combo)

# Output valid triangular sets
print(valid_triangular_sets)



# Configure logging
logging.basicConfig(level=logging.INFO)

# Define the valid triangular sets for arbitrage checking
valid_triangular_sets = valid_triangular_sets

# Initialize an empty list to hold all unique pairs
all_pairs = []

# Generate all possible pairs from the triangular sets
for triangle in valid_triangular_sets:
    pair1, pair2, pair3 = f"{triangle[0]}{triangle[1]}", f"{triangle[1]}{triangle[2]}", f"{triangle[0]}{triangle[2]}"
    all_pairs.extend([pair1.lower(), pair2.lower(), pair3.lower()])

# Remove duplicates by converting the list to a set, then back to a list
unique_pairs = list(set(all_pairs))

# Initialize a queue for data batching
data_batch_queue = Queue()

# Real-time price data storage
price_data = {}
price_data_lock = threading.Lock()  # Add a lock for thread-safe access to price_data

shutdown_event = threading.Event()

# WebSocket message handling
def on_message(ws, message):
    data = json.loads(message)
    symbol = data['s'].lower()
    update = {
        'symbol': symbol,
        'bid': float(data['b']),
        'ask': float(data['a']),
        'timestamp': datetime.now()
    }
    data_batch_queue.put(update)

def on_error(ws, error):
    logging.error(f"Error on {ws.url}: {error}")

def on_close(ws, close_status_code, close_msg):
    logging.info(f"WebSocket closed for {ws.url}, attempting to reconnect...")
    if not shutdown_event.is_set():  # Only attempt to reconnect if shutdown wasn't requested
        time.sleep(10)  # Wait before reconnecting
        start_socket_thread(ws.url.split('/')[-1])  # Reconnect using the same pair

def on_open(ws):
    logging.info(f"Opened connection for {ws.url}")

# Function to run WebSocket for each pair
def run_socket(pair):
    def connect():
        ws_url = f"wss://data-stream.binance.vision/ws/{pair}@bookTicker"
        ws = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
        ws.run_forever()
    while not shutdown_event.is_set():
        connect()

def start_socket_thread(pair):
    thread = threading.Thread(target=run_socket, args=(pair,))
    thread.start()
    return thread

def check_arbitrage_opportunities():
    with price_data_lock:
        best_opportunity = {'set': None, 'profit_ratio': 0}

        for set in valid_triangular_sets:
            pair1 = f"{set[0]}{set[1]}".lower()
            pair2 = f"{set[1]}{set[2]}".lower()
            pair3 = f"{set[0]}{set[2]}".lower()

            try:
                # Retrieve the latest ask and bid prices for each pair
                Pab_ask = price_data[pair1]['ask']
                Pbc_ask = price_data[pair2]['ask']
                Pac_ask = price_data[pair3]['ask']

                Pab_bid = price_data[pair1]['bid']
                Pbc_bid = price_data[pair2]['bid']
                Pac_bid = price_data[pair3]['bid']

                # Calculate the potential profit ratio for the arbitrage cycle
                arbitrage_ratio = (Pab_ask * Pbc_ask) / Pac_ask
                inverse_arbitrage_ratio = (Pab_bid * Pbc_bid) / Pac_bid

                # Identify the most profitable opportunity
                if arbitrage_ratio < 1 and 1 - arbitrage_ratio > best_opportunity['profit_ratio']:
                    best_opportunity['profit_ratio'] = 1 - arbitrage_ratio
                    best_opportunity['set'] = set
                    best_opportunity['type'] = 'Direct'

                if inverse_arbitrage_ratio > 1 and inverse_arbitrage_ratio - 1 > best_opportunity['profit_ratio']:
                    best_opportunity['profit_ratio'] = inverse_arbitrage_ratio - 1
                    best_opportunity['set'] = set
                    best_opportunity['type'] = 'Inverse'

            except KeyError:
                # Missing price data for one of the pairs, skip this cycle
                continue

        # Report the most profitable arbitrage opportunity
        if best_opportunity['set']:
            logging.info(f"Most Profitable Arbitrage Opportunity: {best_opportunity['set']} - Type: {best_opportunity['type']} - Profit Ratio: {best_opportunity['profit_ratio']}")



# Modify process_data_batches to call check_arbitrage_opportunities
def process_data_batches():
    while not shutdown_event.is_set():
        start_time = time.time()
        batch = []

        # Collect all updates for 1 second
        while time.time() - start_time < 1:
            try:
                update = data_batch_queue.get_nowait()
                batch.append(update)
            except Empty:
                continue  # No more data in the queue

        if batch:
            # Update price_data with the latest info in the batch
            with price_data_lock:
                for update in batch:
                    symbol = update['symbol']
                    price_data[symbol] = update

        # Call the arbitrage check function
        check_arbitrage_opportunities()

        time.sleep(max(0, 1 - (time.time() - start_time)))  # Ensure at least 1 second per batch cycle


# Graceful shutdown
def graceful_shutdown():
    shutdown_event.set()
    logging.info("Shutting down...")

# Start WebSocket threads for each unique pair
socket_threads = [start_socket_thread(pair) for pair in unique_pairs]

# Start the batch processing thread
batch_processing_thread = threading.Thread(target=process_data_batches)
batch_processing_thread.start()


