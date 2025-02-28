import os
import glob
import polars as pl

# Thiết lập hiển thị không giới hạn
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(-1)
pl.Config.set_fmt_str_lengths(100)

# --- Các hàm xử lý, tính PnL theo Average Cost với log trạng thái vị thế ---

def compute_pnl_average_cost_with_log(df: pl.DataFrame) -> pl.DataFrame:
    """
    Tính PnL dựa trên phương pháp trung bình giá (average cost) cho mỗi ticker.
    Sau mỗi giao dịch, log lại trạng thái vị thế (side, quantity, avg_price) vào một cột.
    """
    # Sắp xếp theo thời gian để xử lý giao dịch theo thứ tự
    df = df.sort(by="time")

    # positions[ticker] lưu trạng thái vị thế hiện tại cho từng ticker:
    # - side: "long" hoặc "short" hoặc None
    # - quantity: khối lượng đang giữ
    # - avg_price: giá vốn trung bình
    positions = {}

    # Danh sách PnL của từng giao dịch (theo thứ tự row)
    realized_pnl_list = []
    # Danh sách log trạng thái vị thế sau mỗi giao dịch
    position_state_list = []

    # Lặp qua từng giao dịch (mỗi row dưới dạng dict)
    for row in df.iter_rows(named=True):
        ticker = row["ticker"]
        is_buyer = row["isBuyer"]             # True/False
        reduce_only = row["reduceOnly_bool"]    # True/False
        price = row["averagePrice"]                    # float
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
            # Nếu chưa có vị thế (quantity = 0), mở mới
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
                        if pos["side"] == "long":
                            row_pnl = (price - pos["avg_price"]) * qty
                        else:  # side == "short"
                            row_pnl = (pos["avg_price"] - price) * qty
                        pos["quantity"] -= qty
                    elif current_qty == qty:
                        if pos["side"] == "long":
                            row_pnl = (price - pos["avg_price"]) * qty
                        else:
                            row_pnl = (pos["avg_price"] - price) * qty
                        pos["side"] = None
                        pos["quantity"] = 0.0
                        pos["avg_price"] = 0.0
                    else:  # current_qty < qty
                        closed_qty = current_qty
                        if pos["side"] == "long":
                            row_pnl = (price - pos["avg_price"]) * closed_qty
                        else:
                            row_pnl = (pos["avg_price"] - price) * closed_qty
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
            # Nếu không có vị thế, bỏ qua
            if pos["quantity"] == 0:
                pass
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
                    # Nếu lệnh đóng không khớp với side hiện tại, không tính PnL
                    pass

        # Lưu lại PnL của giao dịch hiện tại
        realized_pnl_list.append(row_pnl)
        # Log trạng thái vị thế sau giao dịch này
        position_log_str = f"side={pos['side']}, qty={pos['quantity']:.4f}, avg={pos['avg_price']:.2f}"
        position_state_list.append(position_log_str)

    # Tạo cột realizedPnL và position_state trong DataFrame
    df = df.with_columns([
        pl.Series(name="realizedPnL", values=realized_pnl_list),
        pl.Series(name="position_state", values=position_state_list)
    ])
    # Thêm cột cumulativePnL (lũy kế)
    df = df.with_columns(
        pl.col("realizedPnL").cum_sum().alias("cumulativePnL")
    )
    return df

# --- Xử lý tất cả các file trong thư mục ---

# Lấy thư mục hiện tại và thư mục chứa file CSV
current_dir = os.getcwd()
folder_path = os.path.join(current_dir, "reverse-engineer-foundation")

# Lấy danh sách tất cả các file CSV trong thư mục
csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

if not csv_files:
    raise FileNotFoundError(f"Không tìm thấy file CSV nào trong thư mục: {folder_path}")

# Danh sách DataFrame đã tính PnL cho từng file
df_list = []

for file_path in csv_files:
    print(f"Đang xử lý file: {file_path}")
    # Đọc file CSV
    df = pl.read_csv(file_path)
    
    # Lọc chỉ các dòng có averagePrice khác None
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
    
    # Nếu muốn thêm cột lưu tên file (để phân biệt)
    df_with_pnl = df_with_pnl.with_columns([
        pl.lit(os.path.basename(file_path)).alias("file")
    ])
    
    print(
        df_with_pnl.select([
            "time",
            "ticker",
            "isBuyer",
            "reduceOnly_bool",
            "averagePrice",
            "filledAmount",
            "cumulativePnL",
            "position_state"
        ])
    )