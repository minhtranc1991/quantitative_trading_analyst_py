import os
import glob
import polars as pl
from datetime import datetime

# Thiết lập hiển thị không giới hạn
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(-1)
pl.Config.set_fmt_str_lengths(50)

# --- Hàm tính PnL theo phương pháp Average Cost với log trạng thái vị thế ---
def compute_pnl_average_cost_with_log(df: pl.DataFrame) -> pl.DataFrame:
    """
    Tính PnL dựa trên phương pháp trung bình giá (average cost) cho mỗi ticker.
    Sau mỗi giao dịch, log lại trạng thái vị thế, trạng thái lệnh (Open/Close)
    và ghi nhận số lượng hiện tại (current_qty) của vị thế.
    """
    # Sắp xếp theo thời gian (chuỗi time có định dạng chuẩn lexicographically)
    df = df.sort(by="time")

    # positions[ticker] lưu trạng thái vị thế hiện tại:
    # - side: "long", "short" hoặc None
    # - quantity: khối lượng đang giữ
    # - avg_price: giá vốn trung bình
    positions = {}

    # Danh sách lưu kết quả PnL, log trạng thái và số lượng hiện tại sau mỗi giao dịch
    realized_pnl_list = []
    position_state_list = []
    current_qty_list = []  # lưu lại số lượng hiện tại của vị thế sau mỗi giao dịch

    for row in df.iter_rows(named=True):
        ticker = row["ticker"]
        is_buyer = row["isBuyer"]             # True/False
        reduce_only = row["reduceOnly_bool"]    # True/False
        price = row["averagePrice"]             # float
        qty = row["filledAmount"]               # float

        # Nếu ticker chưa có vị thế, khởi tạo
        if ticker not in positions:
            positions[ticker] = {"side": None, "quantity": 0.0, "avg_price": 0.0}
        pos = positions[ticker]
        row_pnl = 0.0  # PnL của giao dịch hiện tại

        # Xác định side của giao dịch hiện tại: "long" nếu mua, "short" nếu bán
        current_side = "long" if is_buyer else "short"

        # ===== TRƯỜNG HỢP MỞ HOẶC TĂNG VỊ THẾ (reduceOnly_bool = False) =====
        if not reduce_only:
            # Nếu chưa có vị thế, mở mới
            if pos["quantity"] == 0:
                pos["side"] = current_side
                pos["quantity"] = qty
                pos["avg_price"] = price
            else:
                # Nếu cùng side, tăng vị thế (tính trung bình giá)
                if pos["side"] == current_side:
                    total_cost = pos["avg_price"] * pos["quantity"] + price * qty
                    pos["quantity"] += qty
                    pos["avg_price"] = total_cost / pos["quantity"]
                else:
                    # Nếu khác side, đóng một phần hoặc toàn bộ vị thế hiện tại
                    current_qty = pos["quantity"]
                    if current_qty > qty:
                        row_pnl = (price - pos["avg_price"]) * qty if pos["side"] == "long" else (pos["avg_price"] - price) * qty
                        pos["quantity"] -= qty
                    elif current_qty == qty:
                        row_pnl = (price - pos["avg_price"]) * qty if pos["side"] == "long" else (pos["avg_price"] - price) * qty
                        pos["side"] = None
                        pos["quantity"] = 0.0
                        pos["avg_price"] = 0.0
                    else:  # current_qty < qty
                        closed_qty = current_qty
                        row_pnl = (price - pos["avg_price"]) * closed_qty if pos["side"] == "long" else (pos["avg_price"] - price) * closed_qty
                        # Đóng toàn bộ vị thế cũ
                        pos["side"] = None
                        pos["quantity"] = 0.0
                        pos["avg_price"] = 0.0
                        # Mở vị thế mới với phần dư
                        leftover_qty = qty - closed_qty
                        pos["side"] = current_side
                        pos["quantity"] = leftover_qty
                        pos["avg_price"] = price

        # ===== TRƯỜNG HỢP ĐÓNG HOẶC GIẢM VỊ THẾ (reduceOnly_bool = True) =====
        else:
            if pos["quantity"] == 0:
                pass  # Không có vị thế để đóng
            else:
                if pos["side"] == "long" and not is_buyer:
                    if pos["quantity"] > qty:
                        row_pnl = (price - pos["avg_price"]) * qty
                        pos["quantity"] -= qty
                    elif pos["quantity"] == qty:
                        row_pnl = (price - pos["avg_price"]) * qty
                        pos["side"] = None
                        pos["quantity"] = 0.0
                        pos["avg_price"] = 0.0
                    else:
                        closed_qty = pos["quantity"]
                        row_pnl = (price - pos["avg_price"]) * closed_qty
                        pos["side"] = None
                        pos["quantity"] = 0.0
                        pos["avg_price"] = 0.0
                elif pos["side"] == "short" and is_buyer:
                    if pos["quantity"] > qty:
                        row_pnl = (pos["avg_price"] - price) * qty
                        pos["quantity"] -= qty
                    elif pos["quantity"] == qty:
                        row_pnl = (pos["avg_price"] - price) * qty
                        pos["side"] = None
                        pos["quantity"] = 0.0
                        pos["avg_price"] = 0.0
                    else:
                        closed_qty = pos["quantity"]
                        row_pnl = (pos["avg_price"] - price) * closed_qty
                        pos["side"] = None
                        pos["quantity"] = 0.0
                        pos["avg_price"] = 0.0
                else:
                    # Lệnh đóng không khớp với side hiện tại, bỏ qua
                    pass

        realized_pnl_list.append(row_pnl)
        order_status = "Close" if reduce_only else "Open"
        position_log_str = f"{order_status}-{pos['side']}, qty={pos['quantity']:.4f}, avg={pos['avg_price']:.2f}"
        position_state_list.append(position_log_str)
        current_qty_list.append(pos["quantity"])

    # Thêm các cột computed vào DataFrame
    df = df.with_columns([
        pl.Series(name="realizedPnL", values=realized_pnl_list),
        pl.Series(name="position_state", values=position_state_list),
        pl.Series(name="current_qty", values=current_qty_list)
    ])
    df = df.with_columns(
        pl.col("realizedPnL").cum_sum().alias("cumulative_pnl")
    )
    return df

# --- Hàm tính các chỉ số cơ bản của một tài khoản (sử dụng thời gian giữ lệnh) ---
def compute_basic_metrics_from_df(df: pl.DataFrame, account: str) -> dict:
    """
    Tính các chỉ số cơ bản cho một tài khoản:
      - total_trades: Tổng số giao dịch
      - avg_profit_per_trade: Lợi nhuận trung bình trên mỗi giao dịch
      - start_time, end_time: Thời gian giao dịch đầu tiên và cuối cùng
      - holding_times: Tổng thời gian giữ lệnh (chỉ tính khoảng thời gian có lệnh mở, đơn vị giờ)
      - trades_per_hour: Số giao dịch trung bình trên 1 giờ (dựa trên thời gian giữ lệnh)
      - average_holding_time_minutes: Thời gian giữ lệnh trung bình cho mỗi khoảng giữ lệnh (đơn vị giờ)
    Yêu cầu: Cột "time" có định dạng chuỗi "%Y-%m-%d %H:%M:%S"
    """
    # Chuyển cột "time" sang kiểu Datetime nếu chưa có
    if "time_dt" not in df.columns:
        df = df.with_columns(
            pl.col("time").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S").alias("time_dt")
        )
    
    total_trades = df.height
    avg_profit = df["realizedPnL"].mean()
    start_time = df["time_dt"].min()
    end_time = df["time_dt"].max()
    
    # Tính tổng thời gian giữ lệnh và lưu lại từng khoảng thời gian giữ lệnh
    time_list = df["time_dt"].to_list()
    qty_list = df["current_qty"].to_list()
    holding_intervals = []
    for i in range(len(df) - 1):
        if qty_list[i] > 0:
            delta = (time_list[i+1] - time_list[i]).total_seconds()
            holding_intervals.append(delta)
    
    total_holding_seconds = sum(holding_intervals)
    holding_times = total_holding_seconds / 3600.0  # Tổng thời gian giữ lệnh tính bằng giờ
    
    # Tính thời gian giữ lệnh trung bình cho mỗi khoảng (nếu có khoảng giữ lệnh nào)
    average_holding_time_minutes = (sum(holding_intervals) / len(holding_intervals)) / 60.0 if holding_intervals else 0.0

    trades_per_hour = total_trades / holding_times if holding_times > 0 else None

    metrics = {
        "account": account,
        "total_trades": total_trades,
        "avg_profit_per_trade": avg_profit,
        "start_time": start_time,
        "end_time": end_time,
        "holding_times": holding_times,
        "average_holding_time_minutes": average_holding_time_minutes,
        "trades_per_hour": trades_per_hour        
    }
    return metrics

# --- Hàm xử lý một file CSV của một account ---
def process_csv_file(file_path: str) -> (pl.DataFrame, dict):
    """
    Đọc file CSV, xử lý dữ liệu (lọc, trích xuất reduceOnly, tính PnL & log vị thế),
    thêm cột account và tính các chỉ số cơ bản.
    Nếu file không chứa dữ liệu, file sẽ bị xóa tự động.
    Trả về:
      - df_with_pnl: DataFrame đã tính PnL
      - metrics: dict chứa các chỉ số cơ bản của tài khoản
    """
    try:
        # Đọc file CSV, chỉ định kiểu cho cột filledAmount
        df = pl.read_csv(
            file_path,
            schema_overrides={
                "filledAmount": pl.Float64
            }
        )
        # Nếu DataFrame rỗng thì tự xóa file
        if df.is_empty():
            os.remove(file_path)
            raise ValueError(f"File {file_path} trống và đã bị xóa tự động.")
    except Exception as e:
        # Nếu lỗi xuất hiện do file rỗng (empty CSV), xóa file và raise lại lỗi
        if "empty CSV" in str(e):
            if os.path.exists(file_path):
                os.remove(file_path)
            raise ValueError(f"File {file_path} trống và đã bị xóa tự động.") from e
        else:
            raise

    # Lọc các dòng có averagePrice khác None
    df = df.filter(pl.col("averagePrice").is_not_null())

    # Trích xuất giá trị reduceOnly từ cột expiration bằng regex
    df = df.with_columns([
        pl.col("expiration")
          .str.extract(r"'reduceOnly':\s*(\w+)", 1)
          .alias("reduceOnly_extracted")
    ])

    # Chuyển reduceOnly_extracted sang Boolean: "True" -> True, còn lại -> False
    df = df.with_columns([
        pl.when(pl.col("reduceOnly_extracted") == "True")
          .then(True)
          .otherwise(False)
          .alias("reduceOnly_bool")
    ])

    # Tính PnL và log trạng thái vị thế
    df_with_pnl = compute_pnl_average_cost_with_log(df)

    # Thêm cột account dựa trên tên file
    account_name = os.path.basename(file_path)
    df_with_pnl = df_with_pnl.with_columns([pl.lit(account_name).alias("account")])

    # Tính các chỉ số cơ bản
    metrics = compute_basic_metrics_from_df(df_with_pnl, account_name)
    return df_with_pnl, metrics

# --- Main: Xử lý tất cả các file CSV trong thư mục và tổng hợp kết quả ---
def main():
    current_dir = os.getcwd()
    folder_path = os.path.join(current_dir, "reverse-engineer-foundation")
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    
    if not csv_files:
        raise FileNotFoundError(f"Không tìm thấy file CSV nào trong thư mục: {folder_path}")
    
    account_pnl_list = []
    all_metrics = []
    
    for file_path in csv_files:
        print(f"Đang xử lý file: {file_path}")
        try:
            df_with_pnl, metrics = process_csv_file(file_path)
            
            # Lấy cumulative_pnl của giao dịch cuối cùng của account
            final_cum_pnl = df_with_pnl["cumulative_pnl"].to_list()[-1]
            account_pnl_list.append({"account": os.path.basename(file_path), "cumulative_pnl": final_cum_pnl})
            all_metrics.append(metrics)
            
            # print(df_with_pnl.select([
            #     "time",
            #     "ticker",
            #     "isBuyer",
            #     "reduceOnly_bool",
            #     "averagePrice",
            #     "filledAmount",
            #     "realizedPnL",
            #     "cumulative_pnl",
            #     "position_state"
            # ]))

        except Exception as e:
            print(f"❌ Lỗi khi xử lý file {file_path}: {e}")
    
    # Tạo DataFrame kết quả cho PnL và các chỉ số cơ bản
    result_df = pl.DataFrame(account_pnl_list)  # Chứa cột "account" và "cumulative_pnl"
    metrics_df = pl.DataFrame(all_metrics)      # Chứa cột "account" và các chỉ số khác

    # Gộp hai DataFrame dựa trên cột "account"
    combined_df = result_df.join(metrics_df, on="account", how="inner").sort("cumulative_pnl")
    print(combined_df)

    # Tính tổng PnL của tất cả các account
    total_pnl = result_df["cumulative_pnl"].sum()
    print("Tổng PnL của tất cả các account:", total_pnl)

if __name__ == "__main__":
    main()
