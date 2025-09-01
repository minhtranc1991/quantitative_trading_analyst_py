import time
import datetime
import requests
import json
import hashlib
import hmac
import math
from datetime import datetime
from urllib.parse import urlencode
BASE_URL = "https://api.binance.com"

step_size = {
    'BTCUSDT': 0.00001
}

def round_quantity(quantity, step_size):
    """Rounds the quantity to the nearest step size multiple."""
    return math.floor(quantity / step_size) * step_size

def generate_signature(acc,params):
    query_string = "&".join([f"{key}={value}" for key, value in params.items()])
    return hmac.new(acc['API_SECRET'].encode(), query_string.encode(), hashlib.sha256).hexdigest()

def get_server_time():
    res = requests.get("https://api.binance.com/api/v3/time")
    return res.json()['serverTime']

def check_balance(acc):
    url = "https://api.binance.com/sapi/v3/asset/getUserAsset"
    timestamp = get_server_time()
    query_string = f"timestamp={timestamp}&needBtcValuation=true"

    signature = hmac.new(
        acc['API_SECRET'].encode('utf-8'),
        query_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    headers = {
        'X-MBX-APIKEY': acc['API_KEY']
    }

    full_url = f"{url}?{query_string}&signature={signature}"
    response = requests.post(full_url, headers=headers)

    if response.status_code == 200:
        try:
            # Giải mã bytes → JSON → Python list
            if isinstance(response.content, bytes):
                data = json.loads(response.content.decode('utf-8'))
            else:
                data = response.json()

            # Chuyển số thành float và gom dữ liệu gọn
            cleaned = []
            for item in data:
                cleaned.append({
                    "asset": item["asset"],
                    "free": float(item["free"]),
                    "locked": float(item.get("locked", 0)),
                    "btc_value": float(item.get("btcValuation", 0))
                })
            return cleaned

        except Exception as e:
            print(f"[PARSE ERROR] {e}")
            return None
    else:
        print(f"[ERROR] Status {response.status_code}")
        print(f"[ERROR] Body: {response.text}")
        return None
    
def get_binance_precision(symbol):
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url)
    data = response.json()
    
    for s in data["symbols"]:
        if s["symbol"] == symbol:
            for f in s["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    print(f"Step size for {symbol}: {f['stepSize']}")
                    return float(f["stepSize"])
    return None  # Symbol not found or API error

def new_trade_amount(acc,symbol,amount_usd):
    url = f"{BASE_URL}/api/v1/ticker/price?symbol={symbol}"
    response = requests.get(url).json()
    price = float(response['price'])
    quantity = amount_usd / price
    step_size = get_binance_precision(symbol)
    round_quantity = round(quantity/ step_size) * step_size
    return round_quantity

def check_open_positions(acc,symbol):
    """ Fetch open futures positions. """
    try:
        timestamp = int(time.time() * 1000)
        params = {"timestamp": timestamp}
        params["signature"] = generate_signature(acc,params)
        url = f"{BASE_URL}/api/v2/positionRisk"
        headers = {"X-MBX-APIKEY": acc['API_KEY']}
        response = requests.get(url, headers=headers, params=params).json()
        open_positions = [pos for pos in response if float(pos['positionAmt']) != 0 and pos['symbol'] == symbol]
        if open_positions:
            # print(f"Open Futures Positions: {open_positions}")
            return open_positions
    except Exception as e:
        print(f"Error checking open positions: {e}")
        return None
    
def check_price(symbol):
    url = f"{BASE_URL}/api/v1/ticker/price?symbol={symbol}"
    response = requests.get(url).json()
    price = float(response['price'])
    print(f'{symbol} {price}')
    return price

def place_future_order(acc,symbol, side, quantity):
    """ Place futures market order. """
    try:
        timestamp = int(time.time() * 1000)
        order_data = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": quantity,
            "timestamp": timestamp
        }
        order_data["signature"] = generate_signature(acc,order_data)
        url = f"{BASE_URL}/api/v1/order"
        headers = {"X-MBX-APIKEY": acc['API_KEY']}
        response = requests.post(url, headers=headers, params=order_data).json()

        if "orderId" in response:
            print(f"Order successful: {response}")
        else:
            print(f"Order failed: {response}")
    except Exception as e:
        print(f"Error placing order: {e}")

def check_open_positions(acc,symbol):
    """ Fetch open futures positions. """
    try:
        timestamp = int(time.time() * 1000)
        params = {"timestamp": timestamp}
        params["signature"] = generate_signature(acc,params)
        url = f"{BASE_URL}/api/v2/positionRisk"
        headers = {"X-MBX-APIKEY": acc['API_KEY']}
        response = requests.get(url, headers=headers, params=params).json()
        
        open_positions = [pos for pos in response if float(pos['positionAmt']) != 0 and pos['symbol'] == symbol]
        if open_positions:
            print(f"Open Futures Positions: {open_positions}")
        else: print (f'No position for {symbol}')
        return open_positions
    except Exception as e:
        print(f"Error checking open positions: {e}")
        return None

def close_positions(acc,symbol):
    open_positions = check_open_positions(acc,symbol)
    if open_positions:
        print(f"Closing existing positions for {symbol}.")
        for order in open_positions:
            if order['symbol'] == symbol:
                side = "BUY" if float(order['positionAmt']) < 0 else "SELL"
                place_future_order(acc,symbol, side, abs(float(order['positionAmt'])))

def set_leverage(acc,symbol: str, leverage: int):
    params = {
        "symbol": symbol.upper(),  # Ensure symbol is uppercase
        "leverage": leverage,
        "timestamp": int(time.time() * 1000)
    }

    # Generate signature
    params["signature"] = generate_signature(acc, params)

    # Send request
    headers ={"X-MBX-APIKEY": acc['API_KEY']}
    response = requests.post(f"{BASE_URL}/api/v1/leverage", params=params, headers=headers)
    print (response.json())

    return response.json() 

def check_leverage(acc: dict, symbol: str):
    
    params = {
        "symbol": symbol.upper(),  # Ensure symbol is uppercase
        "timestamp": int(time.time() * 1000)
    }

    # Generate signature
    params["signature"] = generate_signature(acc, params)

    # Send request
    headers = {"X-MBX-APIKEY": acc.get("API_KEY", "")}
    response = requests.get(f"{BASE_URL}/api/v2/positionRisk", params=params, headers=headers)

    data = response.json()

    # Handle API response
    if isinstance(data, list) and len(data) > 0:
        leverage = int(data[0].get("leverage", 0))  # Extract leverage safely
        print (f"symbol: {symbol}, leverage: {leverage}")
        return {"symbol": symbol, "leverage": leverage}
    else:
        return {"error": data}

def check_history(acc,symbol,start_time=None, end_time=None,limit=100):
    url = f"{BASE_URL}/api/v1/allOrders"
    timestamp = int(time.time() * 1000)
    params = {
        "symbol": symbol,
        "timestamp": timestamp,
        "limit": limit
    }
    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time
    query_string = urlencode(params)

    params['signature'] = generate_signature(acc,params)
    headers = {"X-MBX-APIKEY": acc['API_KEY']}
    response = requests.get(f"{url}?{urlencode(params)}", headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return {"error": response.json()}

# Function to send the updated equity to Telegram
def send_message(message,bot_token,chat_id):
    


    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    params = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    response = requests.post(url, params=params)
    print(f"📤 Sent to Telegram {response}")  # Check response for errors




# def place_order_quote(acc,symbol, side, quantity): #buy or sell in USDT
#     try:

#         timestamp = int(time.time() * 1000)
#         order_data = {
#             "symbol": symbol,
#             "side": side,
#             "type": "MARKET",
#             "quoteOrderQty": quantity,
#             "timestamp": timestamp
#         }
#         order_data["signature"] = generate_signature(acc,order_data)
#         url = f"{BASE_URL}/api/v3/order"
#         headers = {"X-MBX-APIKEY": acc['API_KEY']}
#         response = requests.post(url, headers=headers, params=order_data).json()
#         print(f"Order placed: {response}")
#     except Exception as e:
#         print(f"Error placing order: {e}")

# def place_order_base(acc,symbol, side, quantity): #buy or sell in btc
#     try:
#         quantity_round = round_quantity(quantity, step_size[symbol])
#         timestamp = int(time.time() * 1000)
#         order_data = {
#             "symbol": symbol,
#             "side": side,
#             "type": "MARKET",
#             "quantity": quantity_round,
#             "timestamp": timestamp
#         }
#         order_data["signature"] = generate_signature(acc,order_data)
#         url = f"{BASE_URL}/api/v3/order"
#         headers = {"X-MBX-APIKEY": acc['API_KEY']}
#         response = requests.post(url, headers=headers, params=order_data).json()
#         print(f"Order placed: {response}")
#     except Exception as e:
#         print(f"Error placing order: {e}")