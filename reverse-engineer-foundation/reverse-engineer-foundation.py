import os
import polars as pl

# Thiết lập hiển thị không giới hạn
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(-1)
pl.Config.set_fmt_str_lengths(100)

current_dir = os.getcwd()
file_path = os.path.join(
    current_dir,
    "reverse-engineer-foundation",
    "0x6ac316ea98b4401fd5c9cbf2fdea5eb89db70a0a000000010000000000010000.csv"
)

# Đọc file CSV
df = pl.read_csv(file_path)

def compute_pnl_average_cost_with_log(df: pl.DataFrame) -> pl.DataFrame:
    """
    Tính PnL dựa trên phương pháp trung bình giá (average cost) cho mỗi ticker.
    Sau mỗi giao dịch, log lại trạng thái vị thế (side, quantity, avg_price) vào một cột.
    """

    # Bước 1: Sắp xếp theo thời gian để xử lý giao dịch theo thứ tự
    df = df.sort(by="time")

    # positions[ticker] sẽ lưu trữ trạng thái vị thế hiện tại cho từng ticker
    # side: "long" hoặc "short" hoặc None
    # quantity: khối lượng đang giữ
    # avg_price: giá vốn trung bình
    positions = {}

    # Danh sách PnL cho từng giao dịch (theo thứ tự row)
    realized_pnl_list = []

    # Danh sách để log lại trạng thái sau mỗi giao dịch
    position_state_list = []

    # Bước 2: Lặp qua từng giao dịch
    for row in df.iter_rows(named=True):
        ticker = row["ticker"]
        is_buyer = row["isBuyer"]            # True/False
        reduce_only = row["reduceOnly_bool"] # True/False
        price = row["price"]                # float
        qty = row["filledAmount"]           # float

        # Khởi tạo vị thế cho ticker nếu chưa có
        if ticker not in positions:
            positions[ticker] = {
                "side": None,       # "long" hoặc "short" hoặc None
                "quantity": 0.0,
                "avg_price": 0.0
            }

        pos = positions[ticker]
        row_pnl = 0.0  # PnL của giao dịch hiện tại

        # Xác định "long" hay "short" cho giao dịch này
        current_side = "long" if is_buyer else "short"

        # =========== TRƯỜNG HỢP MỞ HOẶC TĂNG VỊ THẾ (reduceOnly = False) ===========
        if not reduce_only:
            # Nếu chưa có vị thế nào (quantity=0), mở mới
            if pos["quantity"] == 0:
                pos["side"] = current_side
                pos["quantity"] = qty
                pos["avg_price"] = price

            # Nếu đang có vị thế
            else:
                # Nếu cùng side => tăng thêm vị thế (average cost)
                if pos["side"] == current_side:
                    total_cost = pos["avg_price"] * pos["quantity"] + price * qty
                    pos["quantity"] += qty
                    pos["avg_price"] = total_cost / pos["quantity"]

                # Nếu khác side => đóng một phần hoặc toàn bộ, rồi mở mới nếu còn dư
                else:
                    # Khối lượng vị thế hiện tại
                    current_qty = pos["quantity"]

                    if current_qty > qty:
                        # Đóng một phần
                        if pos["side"] == "long":
                            row_pnl = (price - pos["avg_price"]) * qty
                        else:  # pos["side"] == "short"
                            row_pnl = (pos["avg_price"] - price) * qty

                        # Cập nhật lại vị thế (vẫn còn quantity)
                        pos["quantity"] -= qty
                        # side, avg_price giữ nguyên

                    elif current_qty == qty:
                        # Đóng toàn bộ
                        if pos["side"] == "long":
                            row_pnl = (price - pos["avg_price"]) * qty
                        else:  # pos["side"] == "short"
                            row_pnl = (pos["avg_price"] - price) * qty

                        # Reset vị thế
                        pos["side"] = None
                        pos["quantity"] = 0.0
                        pos["avg_price"] = 0.0

                    else:  # current_qty < qty
                        # Đóng toàn bộ trước
                        closed_qty = current_qty
                        if pos["side"] == "long":
                            row_pnl = (price - pos["avg_price"]) * closed_qty
                        else:  # "short"
                            row_pnl = (pos["avg_price"] - price) * closed_qty

                        # Reset vị thế sau khi đóng
                        pos["side"] = None
                        pos["quantity"] = 0.0
                        pos["avg_price"] = 0.0

                        # Phần dư => mở vị thế ngược lại
                        leftover_qty = qty - closed_qty
                        pos["side"] = current_side
                        pos["quantity"] = leftover_qty
                        pos["avg_price"] = price

        # =========== TRƯỜNG HỢP ĐÓNG HOẶC GIẢM VỊ THẾ (reduceOnly = True) ===========
        else:
            # Nếu không có vị thế => không làm gì
            if pos["quantity"] == 0:
                pass
            else:
                # Nếu vị thế đang "long" và reduceOnly => phải "bán" (is_buyer=False) mới đóng được
                # Nếu vị thế đang "short" và reduceOnly => phải "mua" (is_buyer=True) mới đóng được
                if pos["side"] == "long" and not is_buyer:
                    # Đóng 1 phần hoặc toàn bộ
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
                        # Đóng toàn bộ
                        pos["side"] = None
                        pos["quantity"] = 0.0
                        pos["avg_price"] = 0.0
                        # leftover_qty = qty - closed_qty (thường không mở mới, vì reduceOnly)

                elif pos["side"] == "short" and is_buyer:
                    # Đóng 1 phần hoặc toàn bộ
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
                        # Đóng toàn bộ
                        pos["side"] = None
                        pos["quantity"] = 0.0
                        pos["avg_price"] = 0.0

                else:
                    # side mismatch => không đóng được gì
                    pass

        # Lưu lại PnL của giao dịch hiện tại
        realized_pnl_list.append(row_pnl)

        # Log lại trạng thái vị thế sau khi giao dịch này hoàn tất
        # Bạn có thể lưu ở dạng chuỗi, hoặc tạo thêm 3 cột (pos_side, pos_qty, pos_avg).
        # Ở đây ta gộp vào 1 chuỗi:
        position_log_str = (
            f"side={pos['side']}, "
            f"qty={pos['quantity']:.4f}, "
            f"avg={pos['avg_price']:.2f}"
        )
        position_state_list.append(position_log_str)

    # Bước 3: Tạo cột realizedPnL và position_state trong DataFrame
    df = df.with_columns([
        pl.Series(name="realizedPnL", values=realized_pnl_list),
        pl.Series(name="position_state", values=position_state_list)
    ])

    # Thêm cột cumulativePnL (PnL lũy kế)
    df = df.with_columns(
        (pl.col("realizedPnL").cum_sum()).alias("cumulativePnL")
    )

    return df

# Tạo cột isBuyer_num = 1 nếu isBuyer == "true", ngược lại = -1
df = df.with_columns([
    ((pl.col("isBuyer") == "true").cast(pl.Int8) * 2 - 1).alias("isBuyer_num")
])

# Trích xuất giá trị của reduceOnly từ cột expiration bằng regex
df = df.with_columns([
    pl.col("expiration")
      .str.extract(r"'reduceOnly':\s*(\w+)", 1)
      .alias("reduceOnly_extracted")
])

# Chuyển giá trị reduceOnly_extracted sang kiểu Boolean
# Nếu giá trị là "True" thì thành True, ngược lại là False
df = df.with_columns([
    pl.when(pl.col("reduceOnly_extracted") == "True")
      .then(True)
      .otherwise(False)
      .alias("reduceOnly_bool")
])

# Tính Position = isBuyer_num * filledAmount * price
df = df.with_columns([
    (pl.col("isBuyer_num") * pl.col("filledAmount") * pl.col("price")).alias("Position")
])

df_with_pnl = compute_pnl_average_cost_with_log(df)

# In ra các cột quan trọng
print(df_with_pnl.select(["time","ticker","isBuyer","reduceOnly_bool","price", "filledAmount", "realizedPnL", "cumulativePnL", "position_state"]).sort("time"))

