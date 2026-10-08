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

    def input_news(self):
        print("\nNhập thông tin tin tức:")
        print("Loại tin tức:")
        for i, news_type in enumerate(NewsType, 1):
            print(f"{i}. {news_type.name}")
        
        while True:
            try:
                choice = int(input("Chọn loại tin tức (1-5): "))
                if 1 <= choice <= 5:
                    self.current_news = list(NewsType)[choice-1]
                    break
                else:
                    print("Vui lòng chọn số từ 1-5.")
            except ValueError:
                print("Vui lòng nhập số hợp lệ.")
        
        while True:
            authentic_input = input("Tin này có thật không? (c/k): ").lower()
            if authentic_input == 'c':
                self.is_news_authentic = True
                break
            elif authentic_input == 'k':
                self.is_news_authentic = False
                break
            else:
                print("Vui lòng nhập 'c' hoặc 'k'.")

    def calculate_price_change(self, market_factor: float) -> int:
        # Calculate price change based on news and market factor
        base_change = random.uniform(self.current_news.value[0], self.current_news.value[1])
        
        # Adjust for authenticity
        if not self.is_news_authentic:
            base_change *= 0.5
        
        price_change = int(base_change * market_factor * 100)
        return price_change

    def trading_phase(self):
        print(f"\nGiai đoạn giao dịch - Giá hiện tại: {self.stock_price} VND")
        for player in self.players:
            if not player.is_eliminated:
                self.player_trading(player)

    def player_trading(self, player: Player):
        print(f"\n{player.role.value} - Giao dịch")
        if player.trades_count >= player.max_trades:
            print("Đã đạt giới hạn giao dịch.")
            return

        trade_choice = input("Bạn có muốn giao dịch không? (c/k): ").lower()
        if trade_choice != 'c':
            return

        while True:
            try:
                trade_type = input("Loại giao dịch (mua/bán): ").lower()
                if trade_type not in ['mua', 'bán']:
                    print("Vui lòng chọn 'mua' hoặc 'bán'.")
                    continue

                contracts = int(input(f"Số hợp đồng (tối đa {player.contracts_per_trade}): "))
                if 0 < contracts <= player.contracts_per_trade:
                    trade_price = self.stock_price
                    trade_amount = contracts * trade_price

                    # Simulate trade impact
                    if trade_type == 'mua':
                        player.balance -= trade_amount
                        player.record_trade('mua', contracts, trade_price)
                    else:
                        player.balance += trade_amount
                        player.record_trade('bán', contracts, trade_price)

                    print(f"Giao dịch {trade_type} thành công: {contracts} hợp đồng tại giá {trade_price} VND")
                    break
                else:
                    print(f"Số hợp đồng phải từ 1 đến {player.contracts_per_trade}.")
            except ValueError:
                print("Vui lòng nhập số hợp lệ.")

    def check_player_elimination(self):
        for player in self.players:
            if not player.is_eliminated:
                total_loss = sum(abs(trade['amount']) 
                                 if trade['type'] == 'bán' and trade['price'] < self.stock_price 
                                 else 0 
                                 for trade in player.trades)
                
                player.current_loss += total_loss

                if player.current_loss > player.max_loss:
                    # Check for loan options
                    if player.role == PlayerRole.TRADER and not player.loan_used:
                        loan_amount = random.choice([1000, 2000])
                        player.balance += loan_amount
                        player.loan_used = True
                        print(f"{player.role.value} đã vay {loan_amount} VND để tiếp tục chơi.")
                    elif player.role == PlayerRole.HEDGE_FUND:
                        player.current_loss = 0  # One-time elimination immunity
                        print(f"{player.role.value} được miễn loại.")
                    else:
                        player.is_eliminated = True
                        print(f"{player.role.value} đã bị loại!")

    def run_game(self):
        self.setup_players()
        
        for round in range(1, self.max_rounds + 1):
            self.current_round = round
            print(f"\n--- Vòng {round} ---")
            
            # Nhập tin tức từ người chơi
            self.input_news()
            
            # Hệ số thị trường (từ 0.2 đến 5)
            market_factor = random.uniform(0.2, 5)
            
            # Tính toán thay đổi giá
            price_change = self.calculate_price_change(market_factor)
            self.stock_price += price_change
            print(f"Thay đổi giá: {price_change} VND (Giá mới: {self.stock_price} VND)")
            
            # Giai đoạn giao dịch
            self.trading_phase()
            
            # Kiểm tra loại người chơi
            self.check_player_elimination()
        
        self.determine_winner()

    def determine_winner(self):
        active_players = [p for p in self.players if not p.is_eliminated]
        if active_players:
            winner = max(active_players, key=lambda p: p.balance)
            print("\n--- Kết quả trò chơi ---")
            for player in self.players:
                print(f"{player.role.value}: Số dư cuối {player.balance} VND")
            print(f"Người chiến thắng: {winner.role.value}")
            print(f"Số dư cuối: {winner.balance} VND")
        else:
            print("Không có người chiến thắng - tất cả người chơi đều bị loại!")

# Chạy trò chơi
if __name__ == "__main__":
    game = StockTradingGame()
    game.run_game()