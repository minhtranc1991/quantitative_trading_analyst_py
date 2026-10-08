import random
from enum import Enum
from typing import List, Dict, Tuple

class NewsType(Enum):
    VERY_GOOD = (3, 5)
    GOOD = (1, 3)
    NEUTRAL = (-1, 1)
    BAD = (-3, -1)
    VERY_BAD = (-5, -3)

class PlayerRole(Enum):
    TRADER = "Trader"
    SPECULATOR = "Speculator"
    MARKET_MAKER = "Market Maker"
    INSTITUTIONAL_TRADER = "Institutional Trader"
    HEDGE_FUND = "Hedge Fund"

class Player:
    def __init__(self, role: PlayerRole, initial_balance: int = 0):
        self.role = role
        self.balance = initial_balance
        self.trades_count = 0
        self.max_trades = self._get_max_trades()
        self.max_loss = self._get_max_loss()
        self.current_loss = 0
        self.contracts_per_trade = self._get_max_contracts()
        self.is_eliminated = False
        self.loan_used = False
        self.trades = []

    def _get_max_trades(self) -> int:
        role_trades = {
            PlayerRole.TRADER: 3,
            PlayerRole.SPECULATOR: 7,
            PlayerRole.MARKET_MAKER: float('inf'),
            PlayerRole.INSTITUTIONAL_TRADER: 4,
            PlayerRole.HEDGE_FUND: 2
        }
        return role_trades[self.role]

    def _get_max_loss(self) -> int:
        role_max_loss = {
            PlayerRole.TRADER: 3500,
            PlayerRole.SPECULATOR: 2500,
            PlayerRole.MARKET_MAKER: 5000,
            PlayerRole.INSTITUTIONAL_TRADER: 10000,
            PlayerRole.HEDGE_FUND: 6000
        }
        return role_max_loss[self.role]

    def _get_max_contracts(self) -> int:
        role_contracts = {
            PlayerRole.TRADER: 2,
            PlayerRole.SPECULATOR: 3,
            PlayerRole.MARKET_MAKER: float('inf'),
            PlayerRole.INSTITUTIONAL_TRADER: 5,
            PlayerRole.HEDGE_FUND: 4
        }
        return role_contracts[self.role]

    def record_trade(self, trade_type: str, amount: int, price: int):
        if self.trades_count < self.max_trades:
            self.trades.append({
                'type': trade_type,
                'amount': amount,
                'price': price
            })
            self.trades_count += 1
            return True
        return False

class StockTradingGame:
    def __init__(self, initial_stock_price: int = 10000):
        self.stock_price = initial_stock_price
        self.players: List[Player] = []
        self.current_round = 0
        self.max_rounds = random.randint(5, 7)
        self.current_news = None
        self.is_news_authentic = None
        self.market_factor = None
        self.detailed_log = []

    def setup_players(self):
        # Create players based on role distribution
        roles = [
            PlayerRole.MARKET_MAKER,
            *[PlayerRole.TRADER] * 3,
            PlayerRole.SPECULATOR,
            PlayerRole.INSTITUTIONAL_TRADER,
            PlayerRole.HEDGE_FUND
        ]
        random.shuffle(roles)
        
        for role in roles:
            self.players.append(Player(role, initial_balance=50000))

    def generate_random_news(self):
        # Tạo tin tức ngẫu nhiên
        self.current_news = random.choice(list(NewsType))
        # 90% khả năng tin thật
        self.is_news_authentic = random.random() < 0.9
        
        # Ghi log chi tiết
        log_entry = {
            'news_type': self.current_news.name,
            'is_authentic': self.is_news_authentic
        }
        self.detailed_log.append(log_entry)
        
        print(f"\n🔔 Tin tức: {self.current_news.name} (Xác thực: {'Có' if self.is_news_authentic else 'Không'})")

    def calculate_price_change(self) -> int:
        # Hệ số thị trường từ 0.2 đến 5
        self.market_factor = random.uniform(0.2, 5)
        
        # Tính toán thay đổi giá
        base_change = random.uniform(self.current_news.value[0], self.current_news.value[1])
        
        # Điều chỉnh nếu tin giả
        if not self.is_news_authentic:
            base_change *= 0.5
        
        price_change = int(base_change * self.market_factor * 100)
        return price_change

    def automated_trading(self):
        print("\n🤝 Giai đoạn giao dịch:")
        for player in self.players:
            if not player.is_eliminated:
                self.automated_player_trading(player)

    def automated_player_trading(self, player: Player):
        # Quyết định giao dịch ngẫu nhiên
        if player.trades_count >= player.max_trades:
            print(f"{player.role.value} đã đạt giới hạn giao dịch.")
            return

        # Xác suất giao dịch phụ thuộc vào loại tin
        trade_probabilities = {
            NewsType.VERY_GOOD: 0.8,
            NewsType.GOOD: 0.7,
            NewsType.NEUTRAL: 0.5,
            NewsType.BAD: 0.3,
            NewsType.VERY_BAD: 0.2
        }

        # Quyết định có giao dịch không
        if random.random() < trade_probabilities[self.current_news]:
            # Chọn ngẫu nhiên loại giao dịch
            trade_type = random.choice(['mua', 'bán'])
            
            # Số hợp đồng ngẫu nhiên
            contracts = random.randint(1, min(player.contracts_per_trade, 3))
            trade_price = self.stock_price

            trade_amount = contracts * trade_price

            # Thực hiện giao dịch
            if trade_type == 'mua':
                player.balance -= trade_amount
                player.record_trade('mua', contracts, trade_price)
                print(f"🟢 {player.role.value} MUA {contracts} hợp đồng tại {trade_price} VND")
            else:
                player.balance += trade_amount
                player.record_trade('bán', contracts, trade_price)
                print(f"🔴 {player.role.value} BÁN {contracts} hợp đồng tại {trade_price} VND")

    def check_player_elimination(self):
        for player in self.players:
            if not player.is_eliminated:
                # Tính tổn thất từ các giao dịch
                total_loss = sum(abs(trade['amount'] * trade['price']) 
                                 if (trade['type'] == 'bán' and trade['price'] < self.stock_price) or 
                                    (trade['type'] == 'mua' and trade['price'] > self.stock_price)
                                 else 0 
                                 for trade in player.trades)
                
                player.current_loss += total_loss

                if player.current_loss > player.max_loss:
                    # Kiểm tra các lựa chọn vay
                    if player.role == PlayerRole.TRADER and not player.loan_used:
                        loan_amount = random.choice([1000, 2000])
                        player.balance += loan_amount
                        player.loan_used = True
                        print(f"💸 {player.role.value} đã vay {loan_amount} VND để tiếp tục chơi.")
                    elif player.role == PlayerRole.HEDGE_FUND:
                        player.current_loss = 0  # Miễn loại một lần
                        print(f"🛡️ {player.role.value} được miễn loại.")
                    else:
                        player.is_eliminated = True
                        print(f"❌ {player.role.value} đã bị loại!")

    def run_game(self):
        self.setup_players()
        
        for round in range(1, self.max_rounds + 1):
            self.current_round = round
            print(f"\n🎲 --- Vòng {round} ---")
            
            # Tạo tin tức ngẫu nhiên
            self.generate_random_news()
            
            # Tính toán thay đổi giá
            price_change = self.calculate_price_change()
            self.stock_price += price_change
            print(f"💹 Thay đổi giá: {price_change} VND (Giá mới: {self.stock_price} VND)")
            
            # Giai đoạn giao dịch tự động
            self.automated_trading()
            
            # Kiểm tra loại người chơi
            self.check_player_elimination()
        
        self.determine_winner()

    def determine_winner(self):
        active_players = [p for p in self.players if not p.is_eliminated]
        if active_players:
            winner = max(active_players, key=lambda p: p.balance)
            print("\n🏆 --- Kết quả trò chơi ---")
            for player in self.players:
                print(f"{player.role.value}: Số dư cuối {player.balance} VND")
            print(f"🥇 Người chiến thắng: {winner.role.value}")
            print(f"💰 Số dư cuối: {winner.balance} VND")
        else:
            print("❌ Không có người chiến thắng - tất cả người chơi đều bị loại!")

# Chạy trò chơi
if __name__ == "__main__":
    random.seed()  # Sử dụng seed thực để đảm bảo tính ngẫu nhiên
    game = StockTradingGame()
    game.run_game()