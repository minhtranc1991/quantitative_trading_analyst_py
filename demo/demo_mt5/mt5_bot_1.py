import MetaTrader5 as mt5 
import pandas as pd 
from datetime import datetime, timedelta 
import time 
# import matplotlib.pyplot as plt 
import math 
import concurrent.futures 
import json 
import schedule 
 
 
login = '1604953453457274' 
password = 'Chu.fxtm@181' 
server = 'ForexTimeFXTM-Demo01' 
 
# mt initialize 
mt5.initialize() 
mt5.login(login, password, server) 
# Initialize connection to the MT5 terminal 
if not mt5.initialize(): 
    print("initialize() failed") 
    mt5.shutdown() 
 
symbol_list = ['EURUSD','USDCHF','USDCAD','AUDUSD','EURJPY','EURGBP','EURCHF', 'GBPCHF', 'GBPCAD', 'GBPAUD', 'GBPNZD', 'XAUUSD','Brent', 'Crude'] 
 
m15 = mt5.TIMEFRAME_M15 
H1 = mt5.TIMEFRAME_H1 
with open("jpara.json", "r") as fin: 
    para_list = json.load(fin) 
 
 
def update_para(symbol_name, para,value): 
    with open("jpara.json", "w") as fout: 
        symbol = symbol_name 
        para_name = para 
        para_list[symbol][para_name] = value 
        json.dump(para_list, fout) 
 
def market_order(symbol, volume,order_type): 
    tick = mt5.symbol_info_tick(symbol) 
    order_dict = {'buy':0, 'sell':1} 
    price_dict = {'buy': tick.ask,'sell':tick.bid} 
    price = price_dict[order_type] 
    # volume = 0.1 
    if order_type == 'buy': 
        sl = round(0.99*price,mt5.symbol_info(symbol).digits) 
        tp = round(1.01*price,mt5.symbol_info(symbol).digits) 
        
    elif order_type =='sell': 
        sl = round(1.01 * price,mt5.symbol_info(symbol).digits) 
        tp = round(0.99 * price,mt5.symbol_info(symbol).digits) 
     
    request = { 
        'action':mt5.TRADE_ACTION_DEAL, 
        'symbol': symbol, 
        'volume': volume, 
        'type': order_dict[order_type], 
        'price': price_dict[order_type]  , 
        'sl':sl, 
        'tp': tp, 
        'deviation': 10, 
        'magic': 100, 
        'comment': 'python market order', 
        'type_time': mt5.ORDER_TIME_GTC, 
        'type_filling': mt5.ORDER_FILLING_FOK,  
    } 
    order_result = mt5.order_send(request) 
    print (f'I  order {order_result}') 
    return price,order_result 
 
 
#close an order based on symbol 
def close_order(ticket): 
    position = None 
    positions = mt5.positions_get() 
     
    for pos in positions: 
        if pos.ticket == ticket: 
            position = pos 
            break 
 
    if position is None: 
        print(f"Position with ticket {ticket} not found.") 
        return False 
 
    # Determine the correct order type and price for closing the position 
    symbol = position.symbol 
    lot_size = position.volume 
    order_type = mt5.ORDER_TYPE_SELL if position.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY 
    price = mt5.symbol_info_tick(symbol).bid if position.type == mt5.ORDER_TYPE_BUY else mt5.symbol_info_tick(symbol).ask 
 
    # Prepare the close order request 
    request = { 
        "action": mt5.TRADE_ACTION_DEAL, 
        "symbol": symbol, 
        "volume": lot_size, 
        "type": order_type,  # Close with the opposite type 
        "position": ticket,  # Reference the specific position to close 
        "price": price, 
        "deviation": 20,  # Set deviation in points 
        "magic": 123456,  # Optional magic number to identify trades 
        "comment": f"Close order {ticket}", 
        "type_time": mt5.ORDER_TIME_GTC, 
        "type_filling": mt5.ORDER_FILLING_FOK, 
    } 
 
    # Send the close order 
    order_result = mt5.order_send(request) 
    print (f'I  close order {order_result}') 
    return order_result 
 
def get_live_data(symbol, timeframe, num_candles=1000): 
    # Request live data (OHLC data) 
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, num_candles) 
     
    if rates is None: 
        print(f"Failed to get data for {symbol}.") 
        return None 
     
    # Convert the rates into a DataFrame for better handling and readability 
    rates_frame = pd.DataFrame(rates) 
     
    # Convert time from seconds to datetime format 
    rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s') 
     
    return rates_frame 
 
def calculate_rsi_rma(close, window=3): 
    # Calculate the difference between consecutive close prices 
    
    delta =close.diff() 
     
    # Separate gains and losses 
    gain = delta.where(delta > 0, 0) 
    loss = -delta.where(delta < 0, 0) 
     
    # Calculate the RMA for gains and losses 
    rma_gain = gain.ewm(alpha=1/window, adjust=False).mean()  # Exponential weighted moving average (RMA) for gain 
    rma_loss = loss.ewm(alpha=1/window, adjust=False).mean()  # Exponential weighted moving average (RMA) for loss 
     
    # Calculate the relative strength (RS) 
    rs = rma_gain / rma_loss 
     
    # Calculate the RSI 
    rsi = 100 - (100 / (1 + rs)) 
     
    return rsi 
def RSI (symbol,timeframe): 
    data = get_live_data(symbol,timeframe) 
    data['RSI'] = calculate_rsi_rma(data['close']) 
    RSI = data['RSI'].iloc[-1] 
    return (RSI) 
def reset_para(symbol): 
    default_structure = { 
       'counter_buy':0, 
        'counter_sell':0, 
        'price_buy':0, 
        'price_sell':10000, 
        'last_sell_duong': 0, 
        'last_buy_duong': 0, 
        'last_sell_am': 0, 
        'last_buy_am': 0, 
        'ticket_dca_buy_duong': [], 
        'ticket_dca_buy_am': [], 
        'ticket_dca_sell_duong': [], 
        'ticket_dca_sell_am': [], 
        'ticket_sell':0, 
        'ticket_buy': 0, 
        'dca_sell_duong':0, 
        'dca_sell_am':0, 
        'dca_buy_duong':0, 
        'dca_buy_am':0, 
 
    } 
    with open("jpara.json", "r") as file: 
        data = json.load(file) 
        data[symbol] = default_structure 
    with open("jpara.json", 'w') as file: 
            json.dump(data, file) 
     
def trade (symbol): 
    positions = mt5.positions_get(symbol) 
    print (f'{symbol} {RSI(symbol,m15)}') 
    #open & dca # sell 
    dca_sell_duong = para_list[symbol]['dca_sell_duong'] 
    dca_sell_am = para_list[symbol]['dca_sell_am'] 
    if para_list[symbol]['counter_sell'] == 0: 
        if RSI(symbol,m15) >= 95: 
            price, order_result = market_order(symbol,0.01,'sell') 
            update_para(symbol,'counter_sell',1) 
            update_para(symbol,'price_sell',price) 
            update_para(symbol,'last_sell_am',price) 
            update_para(symbol,'last_sell_duong',price) 
            print (f'sell {symbol} first time') 
    elif para_list[symbol]['counter_sell'] > 0: 
         
        if para_list[symbol]['dca_sell_duong'] <=2: 
            if RSI(symbol,m15) >= 50: 
                if -mt5.symbol_info_tick(symbol).ask + para_list[symbol]['last_sell_duong'] >= 10*mt5.symbol_info(symbol).digits: 
                    price, order_result = market_order(symbol,0.01,'sell') 
                    dca_sell_duong +=1 
                    update_para(symbol,'dca_sell_duong',dca_sell_duong) 
                    update_para(symbol,'last_sell_duong',price) 
                    para_list[symbol]['ticket_dca_sell_duong'].append(order_result) 
        if para_list[symbol]['dca_sell_am'] <= 2: 
            if mt5.symbol_info_tick(symbol).ask - para_list[symbol]['last_sell_am'] >= 10*mt5.symbol_info(symbol).digits: 
                price, order_result = market_order(symbol,0.01,'sell') 
                dca_sell_am +=1 
                update_para(symbol,'dca_sell_am',dca_sell_am) 
                update_para(symbol,'last_sell',price) 
                para_list[symbol]['ticket_dca_sell_am'].append(order_result) 
     
    #open & dca # buy 
    dca_buy_duong = para_list[symbol]['dca_buy_duong'] 
    dca_buy_am = para_list[symbol]['dca_buy_am'] 
    if para_list[symbol]['counter_buy'] == 0: 
        if RSI(symbol,m15) <= 5: 
            price, order_result = market_order(symbol,0.01,'buy') 
            update_para(symbol,'counter_buy',1) 
            update_para(symbol,'price_buy',price) 
            update_para(symbol,'last_buy_duong',price) 
            update_para(symbol,'last_buy_am',price) 
    elif para_list[symbol]['counter_buy'] > 0: 
         
        if para_list[symbol]['dca_buy_duong'] <= 2: 
            if RSI(symbol,m15 ) <= 50: 
                if mt5.symbol_info_tick(symbol).bid - para_list[symbol]['last_buy_duong'] >= 10*mt5.symbol_info(symbol).digits: 
                    price, order_result = market_order(symbol,0.01,'buy') 
                    dca_buy_duong +=1 
                    update_para(symbol,'dca_buy_duong',dca_buy_duong) 
                    update_para(symbol,'last_buy_duong',price) 
                    para_list[symbol]['ticket_dca_buy_duong'].append(order_result) 
        if para_list[symbol]['dca_buy_am'] <= 2: 
            if -mt5.symbol_info_tick(symbol).bid + para_list[symbol]['last_buy_am'] >= 10*mt5.symbol_info(symbol).digits: 
                price, order_result = market_order(symbol,0.01,'buy') 
                dca_buy_am +=1 
                update_para(symbol,'dca_buy_am',dca_buy_am) 
                para_list[symbol]['ticket_dca_buy_am'].append(order_result) 
    #close lenh H1 
    if positions is not None: 
        if RSI(symbol, m15) <=15: 
            for ticket in para_list[symbol]['ticket_dca_sell_duong']: 
                close_order(ticket) 
        if RSI (symbol, m15) >=85: 
            for ticket in para_list[symbol]['ticket_dca_buy_duong']: 
                close_order(ticket) 
        #close lenh m15 
        if RSI (symbol, H1) <=15: 
            close_order(para_list[symbol]['ticket_sell']) 
            for ticket in para_list[symbol]['ticket_dca_sell_am']: 
                close_order(ticket) 
        if RSI (symbol, H1) >=85: 
            close_order(para_list[symbol]['ticket_buy']) 
            for ticket in para_list[symbol]['ticket_dca_buy_am']: 
                close_order(ticket) 
        # close lenh Sl 
        if para_list[symbol]['dca_buy_am'] ==2: 
            for pos in positions: 
                if pos.type == 0: 
                    if pos.price_open - mt5.symbol_info_tick(symbol).bid <= -30* mt5.symbol_info(symbol).digits: 
                        ticket = pos.ticket 
                        close_order(ticket) 
                        reset_para(symbol) 
        if para_list[symbol]['dca_sell_am'] ==2: 
            for pos in positions: 
                if pos.type == 1: 
                    if pos.price_open - mt5.symbol_info_tick(symbol).bid <= 30* mt5.symbol_info(symbol).digits: 
                        ticket = pos.ticket 
                        close_order(ticket) 
                        reset_para(symbol) 
 
 
def trade_symbol(): 
     
    for symbol in symbol_list: 
        trade(symbol) 
         
 
schedule.every().hour.at(":00").do(trade_symbol)  # At the 00 minute 
schedule.every().hour.at(":15").do(trade_symbol)  # At the 15 minute 
schedule.every().hour.at(":30").do(trade_symbol)  # At the 30 minute 
schedule.every().hour.at(":45").do(trade_symbol)  # At the 45 minute 
 
# Main loop to keep the scheduler running 
while True: 
    schedule.run_pending()  # Run pending tasks 
    time.sleep(1)  # Sleep briefly to avoid high CPU usage

'''
1. **Khởi tạo và kết nối MT5**: Đoạn code khởi tạo MT5 hai lần. Lần đầu là `mt5.initialize()` và `mt5.login()`, sau đó lại check `if not mt5.initialize():`. Điều này có thể gây ra lỗi hoặc dư thừa. Nên viết lại phần khởi tạo sao cho gọn và chắc chắn kết nối thành công.

2. **Xử lý lỗi và logging**: Hiện tại, code in ra các thông báo nhưng không có cơ chế xử lý lỗi đầy đủ. Ví dụ, khi gọi `mt5.order_send`, nên check xem kết quả có thành công không, và xử lý các trường hợp lỗi.

3. **Magic numbers và hard-coded values**: Các giá trị như số lot (0.01), các mức RSI (95, 5, 50, 15, 85), các khoảng cách SL/TP (10* digits) đều được hard-code. Nên đưa vào biến cấu hình hoặc tham số để dễ quản lý.

4. **Data handling với JSON**: File `jpara.json` được đọc và ghi nhiều lần, có thể gây ra vấn đề hiệu suất và race condition. Có thể tối ưu bằng cách đọc một lần và lưu trong bộ nhớ, chỉ ghi khi cần thiết.

5. **Code duplication**: Các phần xử lý cho buy và sell có cấu trúc tương tự nhau, có thể tách thành hàm chung để tránh lặp code. Ví dụ, hàm `market_order` xử lý cả buy và sell, nhưng trong phần trade, xử lý buy và sell riêng có nhiều đoạn trùng lặp.

6. **Sử dụng concurrent.futures nhưng không thấy dùng**: Trong import có `concurrent.futures` nhưng trong code không sử dụng. Có thể tác giả định dùng để xử lý đa luồng nhưng chưa triển khai. Nếu không cần thì nên bỏ để code sạch hơn.

7. **Hàm `trade` quá dài và phức tạp**: Hàm này xử lý cả mở lệnh, DCA, đóng lệnh cho cả buy và sell, dẫn đến code khó đọc và bảo trì. Nên tách thành các hàm nhỏ hơn như `handle_buy`, `handle_sell`, `check_close_conditions`, v.v.

8. **Sử dụng schedule và vòng lặp chính**: Hiện tại, lịch chạy mỗi 15 phút. Tuy nhiên, cách dùng `time.sleep(1)` có thể gây delay không cần thiết. Có thể cải thiện bằng cách dùng async hoặc tối ưu hơn.

9. **Quản lý trạng thái với file JSON**: Mỗi lần cập nhật tham số đều mở file, đọc toàn bộ, sửa và ghi lại. Điều này không hiệu quả và có thể gây lỗi nếu nhiều process cùng truy cập. Có thể dùng một class để quản lý state, load một lần và đồng bộ khi cần.

10. **Xử lý các điều kiện RSI phức tạp**: Các điều kiện RSI được kiểm tra nhiều lần với các ngưỡng khác nhau. Có thể tách thành các hàm helper như `is_overbought`, `is_oversold`, `should_dca`, v.v.

11. **Đặt tên biến và hàm**: Một số tên biến và hàm khó hiểu, ví dụ `dca_sell_duong`, `dca_sell_am` (có vẻ tiếng Việt). Nên dùng tên tiếng Anh rõ ràng để người khác dễ hiểu.

12. **Xử lý lỗi khi gọi MT5 functions**: Nhiều hàm MT5 như `symbol_info_tick`, `positions_get` có thể trả về None hoặc rỗng. Cần check kết quả trước khi sử dụng để tránh lỗi runtime.

13. **Magic numbers trong reset_para**: Giá trị mặc định như `price_sell: 10000` có thể không phù hợp với mọi symbol (ví dụ vàng XAUUSD có giá lớn hơn 1000). Cần xử lý linh hoạt hơn.

14. **Sử dụng digits một cách chính xác**: Các phép tính SL, TP sử dụng `digits` nhưng nhân với 10, 30, etc. Cần đảm bảo tính toán đúng pip value.

15. **Đóng các vị thế**: Trong hàm `close_order`, kiểm tra position có tồn tại không nhưng không xử lý trường hợp lỗi khi gọi `order_send`. Cần check `order_result` và xử lý lỗi.

Từ những điểm trên, mình có thể lên kế hoạch refactor từng bước:

**Bước 1: Tái cấu trúc phần khởi tạo MT5**

- Đảm bảo khởi tạo một lần, xử lý lỗi và thoát nếu không thành công.

- Tách phần login và khởi tạo thành hàm riêng, dễ quản lý.

**Bước 2: Đóng gói cấu hình**

- Đưa các tham số như lot size, RSI thresholds, SL/TP distances vào một dict config hoặc file cấu hình.

- Sử dụng class để quản lý cấu hình và state, tránh đọc/ghi file JSON liên tục.

**Bước 3: Tách các hàm lớn thành các hàm nhỏ**

- Tách phần xử lý buy, sell, DCA, close orders thành các hàm riêng.

- Sử dụng các helper functions cho các điều kiện RSI.

**Bước 4: Giảm code duplication**

- Tạo hàm chung cho xử lý mở lệnh, DCA cho cả buy và sell.

**Bước 5: Cải thiện error handling và logging**

- Thêm try-except cho các lệnh gọi MT5.

- Log lỗi chi tiết hơn.

**Bước 6: Tối ưu việc đọc/ghi file JSON**

- Load dữ liệu một lần khi khởi động, lưu trong bộ nhớ, chỉ ghi khi có thay đổi.

**Bước 7: Sử dụng tên biến rõ ràng**

- Đổi tên các biến như 'duong', 'am' thành 'positive', 'negative' hoặc tương tự.

**Bước 8: Kiểm tra và xử lý các giá trị digits đúng cách**

- Đảm bảo tính toán SL, TP dựa trên point thay vì digits, vì mỗi symbol có point khác nhau.

**Bước 9: Cải thiện phần schedule và main loop**

- Đảm bảo rằng các task chạy đúng lịch mà không bị block bởi sleep.'''