import websocket
import json

def on_message(ws, message):
    data = json.loads(message)
    print("Order Book Update:")
    print("Bids:", data['bids'])
    print("Asks:", data['asks'])

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("Closed WebSocket connection.")

def on_open(ws):
    print("Connected to WebSocket.")

if __name__ == "__main__":
    ws_url = "wss://stream.binance.com:9443/ws/btcusdt@depth10"
    ws = websocket.WebSocketApp(ws_url, 
                                 on_message=on_message, 
                                 on_error=on_error, 
                                 on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()
