import function 
import config
from datetime import datetime
import pytz
import time

def create_balance_message():
    try:
        balances = function.check_balance(config.acc_binance_1)
        if not balances:
            return "Error getting balance"
        
        # Create message header
        message = "*Spot Account Balance Report*\n"
        message += f"*Time:* {datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        # Add each asset balance
        for balance in balances:
            message += (f"*{balance['asset']}*\n"
                       f"Free: {balance['free']:.8f}\n"
                       f"Locked: {balance['locked']:.8f}\n"
                       f"Total: {balance['total']:.8f}\n\n")
        
        return message
    except Exception as e:
        print(f"Error creating message: {e}")
        return "Error getting balance"

def send_balance_update():
    message = create_balance_message()
    function.send_message(message, config.token, config.channel_strat_id)

if __name__ == "__main__":
    while True:
        try:
            utc_now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
            
            # Send update at 9:00 AM Vietnam time
            # if utc_now.hour == 9 and utc_now.minute == 0:
            send_balance_update()
            print("Balance update sent")
            time.sleep(1)  # Sleep for 1 minute to avoid multiple sends
            # else:
                # time.sleep(1)  # Check every 30 seconds
                
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(1)