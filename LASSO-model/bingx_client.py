import time
import requests
import hmac
import json
from hashlib import sha256
import pandas as pd
from datetime import datetime
import os

class BingXClient:
    def __init__(self, api_key, secret_key):
        self.api_url = "https://open-api.bingx.com"
        self.api_key = api_key
        self.secret_key = secret_key
        self.max_retries = 3
        self.retry_delay = 5

    def _get_sign(self, payload):
        signature = hmac.new(self.secret_key.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()
        return signature

    def _praseParam(self, paramsMap):
        sortedKeys = sorted(paramsMap)
        paramsStr = "&".join([f"{x}={paramsMap[x]}" for x in sortedKeys])
        return f"{paramsStr}×tamp={int(time.time() * 1000)}"

    def send_request(self, method, path, params_map, payload=None, retry_count=0):
        urlpa = self._praseParam(params_map)
        url = f"{self.api_url}{path}?{urlpa}&signature={self._get_sign(urlpa)}"
        headers = {
            'X-BX-APIKEY': self.api_key,
        }

        try:
            response = requests.request(method, url, headers=headers, data=payload)
            response.raise_for_status()

            try:
                data = response.json()
                if data.get("code") != 0:
                    print(f"API Error: {data.get('msg')}")
                    return None
                return data
            except ValueError:
                print(f"Failed to parse JSON response: {response.text}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            if retry_count < self.max_retries:
                print(f"Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)
                return self.send_request(method, path, params_map, payload, retry_count + 1)
            else:
                print(f"Max retries exceeded.")
                return None
    
    def get_current_leverage(self, symbol):
        payload = {}
        path = '/openApi/swap/v2/trade/leverage'
        method = "GET"
        params_map = {
            "symbol": symbol
        }
        response = self.send_request(method, path, params_map, payload)
        if response:
            return response.get('data', {}).get('leverage', None)
        else:
            return None

    def set_leverage(self, symbol, side, leverage):
        payload = {}
        path = '/openApi/swap/v2/trade/leverage'
        method = "POST"
        params_map = {
            "symbol": symbol,
            "side": side,
            "leverage": leverage
        }
        response = self.send_request(method, path, params_map, payload)
        if response and response.get('code') == 0:
            print(f"Leverage for {symbol} set to {leverage} on {side} side successfully.")
            return True
        else:
            print(f"Failed to set leverage for {symbol} to {leverage} on {side} side.")
            return False

# Example usage:
if __name__ == '__main__':
    api_key = "KHvFdTmgXsrAdySUgEV5Z002Kq3lHmrRyyYSrXeqSvsY2NifqiPMLbtDbRqn6H7bd03V6yPBqgHdYdisEIruA"
    secret_key = "YhaSHWd7fqa4bZKOJv3a09BjUewWbtN5HVSTqQ7OyfX9d5s75tfdo7UFr2iy4oL4FJCFyxUiGDU3rg"
    client = BingXClient(api_key, secret_key)

    # Test get_current_leverage
    leverage = client.get_current_leverage("BTC-USDT")
    if leverage:
        print(f"Current leverage for BTC-USDT: {leverage}")

    # Test set_leverage
    success = client.set_leverage("BTC-USDT", "LONG", 10)
    if success:
        print("Leverage set successfully.")
    else:
        print("Failed to set leverage.")