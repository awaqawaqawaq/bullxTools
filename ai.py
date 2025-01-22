import pandas as pd
import json


class Position:
    def __init__(self, key, amount, entry_price, direction, entry_time):
        self.key = key
        self.initial_buy=amount
        self.amount = amount  # 当前持仓量
        self.entry_price = entry_price  # 开仓价格
        self.entry_value = amount * entry_price  # 开仓价值
        self.direction = direction  # "long" or "short"
        self.entry_time = entry_time  # 开仓时间
        self.hold_time = 0  # 持仓时间
        self.stoploss_levels = []  # [{"price": x, "amount": y}]
        self.takeprofit_levels = []  # [{"price": x, "amount": y}]
        self.final_profit = 0  # 已实现盈亏

    def update_hold_time(self, current_time):
        """更新持仓时间"""
        self.hold_time = current_time - self.entry_time

    def add_stoploss_level(self, price, amount):
        """添加止损级别"""
        self.stoploss_levels.append({"price": price, "amount": amount})

    def add_takeprofit_level(self, price, amount):
        """添加止盈级别"""
        self.takeprofit_levels.append({"price": price, "amount": amount})

    def adjust_position(self, amount):
        """调整仓位大小"""
        self.amount -= amount
        if self.amount <= 0:
            self.amount = 0

    def is_closed(self):
        """判断仓位是否已完全关闭"""
        return self.amount <= 0

    def current_value(self, current_price):
        """计算当前未平仓价值"""
        return self.amount * current_price

    def pnl_ratio(self, current_price):
        """计算当前盈亏比例"""
        if self.direction == "long":
            return (current_price - self.entry_price) / self.entry_price
        elif self.direction == "short":
            return (self.entry_price - current_price) / self.entry_price


class Backtest:
    def __init__(
        self,
        data: pd.DataFrame,
        strategy_callback,
        initial_balance: float =100000,
        history_window: int = 0,
        CA: str = "",
        slippage: float = 0.0,  # Slippage percentage
        transaction_cost: float = 0.0,  # Transaction cost per trade
    ):
        self.data = data
        self.strategy_callback = strategy_callback
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.positions = {}  # 当前所有仓位，key 为仓位编号
        self.history_window = history_window
        self.trades = []  # 记录交易历史
        self.summaries = []  # 记录关闭的仓位总结
        self.position_counter = 0  # 用于生成唯一仓位编号
        self.slippage = slippage  # Slippage percentage
        self.transaction_cost = transaction_cost  # Transaction cost per trade
        self.CA = CA


    def execute(self):
        for i in range(self.history_window, len(self.data)):
            current_row = self.data.iloc[i]
            history = self.data.iloc[i - self.history_window : i]
            actions = self.strategy_callback(current_row, history, self.positions)

            # 执行策略信号
            for action in actions:
                if action["type"] == "buy":
                    self.open_position(current_row, action, direction="long")
                elif action["type"] == "sell":
                    self.close_positions(current_row, direction="long")
                elif action["type"] == "sell_short":
                    self.open_position(current_row, action, direction="short")
                elif action["type"] == "cover":
                    self.close_positions(current_row, direction="short")

            # 检查止盈止损
            self.check_takeprofit_and_stoploss(current_row)

    def open_position(self, current_row, action, direction):
        """开仓操作"""
        amount = action["amount"]
        stoploss_levels = action["stoploss_levels"]  # [{"price", "amount"}]
        takeprofit_levels = action["takeprofit_levels"]  # [{"price", "amount"}]
        entry_price = current_row["close"]
        entry_value = amount * entry_price
        
        if self.balance >= entry_value:
            self.balance -= entry_value
        else:
            self.log(f"Not enough balance for position, need {entry_value} but have {self.balance}")
            return 
            

        position_key = self.position_counter
        self.position_counter += 1
        position = Position(
            key=position_key,
            amount=amount,
            entry_price=entry_price,
            direction=direction,
            entry_time=int(current_row["timestamp"]/1000),
        )

        # 设置止盈止损
        for sl in stoploss_levels:
            position.add_stoploss_level(sl["price"], sl["amount"])
        for tp in takeprofit_levels:
            position.add_takeprofit_level(tp["price"], tp["amount"])

        self.positions[position_key] = position



        # Correcting the action and variable name
        self.trades.append(
            {
                "key": position_key,  # Corrected pos_key to position_key
                "timestamp": int(current_row["timestamp"]/1000),
                "action": (
                    "open long" if direction == "long" else "open short"
                ),  # Correct action type for open positions
                "amount": position.amount,
                "price": entry_price,
                # "finalized_value": position.final_profit  # At this point, finalized_value will be 0 for open positions
            }
        )

        self.log(
            f"Opened position {position_key}, amount: {amount}, price: {entry_price}, direction: {direction}"
        )

    def close_positions(self, current_row, direction, key=None):
        """平仓操作"""
        to_close = [key] if key is not None else [
            pos_key
            for pos_key, pos in self.positions.items()
            if pos.direction == direction
        ]
        for pos_key in to_close:
            self._close_position(current_row, pos_key)

    def _close_position(self, current_row, pos_key):
        pos = self.positions[pos_key]


        # 计算时间
        pos.update_hold_time(int(current_row["timestamp"]/1000))
        close_price = current_row["close"]
        
        # 如果还有amount，更新余额，以当前close平仓
        if(pos.amount > 0):
            close_price = self._apply_slippage(close_price, pos.direction)
            close_value = pos.amount * close_price
            if pos.direction == "long":
                self.balance += close_value - self.transaction_cost
                pos.final_profit += pos.amount * (close_price - pos.entry_price)
            elif pos.direction == "short":
                self.balance -= close_value + self.transaction_cost
                pos.final_profit += pos.amount * (pos.entry_price - close_price)

            # 记录交易明细
            self.trades.append(
                {
                    "key": pos_key,
                    "timestamp": int(current_row["timestamp"]/1000),
                    "action": (
                        "close long" if pos.direction == "long" else "close short"
                    ),  # Correct action type for closing positions
                    "amount": pos.amount,
                    "price": close_price,
                    # "finalized_value": pos.final_profit
                }
            )


        # 记录仓位总结
        self.summaries.append(
            {
                "key": pos.key,
                "entry_time": pos.entry_time,
                "exit_time": int(current_row["timestamp"]/1000),
                "hold_time": pos.hold_time,
                "entry_price": pos.entry_price,
                "entry_value": pos.entry_value,
                "exit_price": close_price,
                "amount": pos.initial_buy,
                "final_profit": pos.final_profit,
                "PNL": str((pos.final_profit / pos.initial_buy)*100) + "%",
            }
        )

        # 删除仓位
        del self.positions[pos_key]

        self.log(f"Closed position {pos_key}, profit: {pos.final_profit}")


    def check_takeprofit_and_stoploss(self, current_row):
        """检查止盈和止损"""
        for pos_key, pos in list(self.positions.items()):
            if pos.direction == "long":
                # Optimized take profit check
                for tp in pos.takeprofit_levels:
                    if current_row["high"] >= tp["price"]:
                        self.execute_partial_close(
                            current_row, pos_key, tp, "takeprofit"
                        )
                        break # Stop looking for tp after first match


                # Optimized stop loss check
                for sl in pos.stoploss_levels:
                   if current_row["low"] <= sl["price"]:
                       self.execute_partial_close(current_row, pos_key, sl, "stoploss")
                       break
            elif pos.direction == "short":
                # Optimized take profit check
                for tp in pos.takeprofit_levels:
                    if current_row["low"] <= tp["price"]:
                        self.execute_partial_close(
                            current_row, pos_key, tp, "takeprofit"
                        )
                        break # Stop looking for tp after first match


                # Optimized stop loss check
                for sl in pos.stoploss_levels:
                   if current_row["high"] >= sl["price"]:
                       self.execute_partial_close(current_row, pos_key, sl, "stoploss")
                       break

    def execute_partial_close(self, current_row, pos_key, level, reason):
        """执行部分平仓"""
        pos = self.positions[pos_key]
        close_amount = min(level["amount"], pos.amount)
        close_price = level["price"]
        close_price = self._apply_slippage(close_price, pos.direction)
        close_value = close_amount * close_price

        # 更新仓位
        pos.adjust_position(close_amount)
        if reason == "takeprofit":
            pos.takeprofit_levels.remove(level)
        elif reason == "stoploss":
            pos.stoploss_levels.remove(level)

        # 更新余额
        if pos.direction == "long":
            self.balance += close_value - self.transaction_cost
            pos.final_profit += close_amount * (close_price - pos.entry_price)
        elif pos.direction == "short":
            self.balance -= close_value + self.transaction_cost
            pos.final_profit += close_amount * (pos.entry_price - close_price)

        # 记录交易明细
        self.trades.append(
            {
                "key": pos_key,
                "timestamp": int(current_row["timestamp"]/1000),
                "action": reason + ' ' + pos.direction,  # Correct action type for partial close
                "amount": close_amount,
                "price": close_price,
                # "finalized_value": pos.final_profit
            }
        )

        self.log(
            f"{reason.capitalize()} for position {pos_key}, amount: {close_amount}, price: {close_price}"
        )

        # 如果仓位完全平仓，记录总结
        if pos.is_closed():
            self._close_position(current_row, pos_key)


    def _apply_slippage(self, price, direction):
        """Applies slippage to a given price."""
        if self.slippage == 0:
            return price
        
        slippage_amount = price * self.slippage

        if direction == "long":
            return price + slippage_amount
        else:
            return price - slippage_amount


    def save_results(self):
        """保存交易记录和总结"""
        with open(f"{self.CA}_trades.json", "w") as trade_file:
            json.dump(self.trades, trade_file, indent=4)

        with open(f"{self.CA}_summaries.json", "w") as summary_file:
            json.dump(self.summaries, summary_file, indent=4)

    def log(self, message):
        """记录日志"""
        print(f"[LOG] {message}")