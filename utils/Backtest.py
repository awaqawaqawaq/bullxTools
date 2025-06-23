import pandas as pd
import json
import os
import datetime
from datetime import datetime

class Position:
    def __init__(self, key, amount, entry_price, direction, entry_time,change_takeprofit=None,change_stoploss=None):
        self.key = key
        self.initial_buy = amount
        self.amount = amount  # 当前持仓量
        self.entry_price = entry_price  # 开仓价格
        self.entry_value = amount * entry_price  # 开仓价值
        self.direction = direction  # "long" or "short"
        self.entry_time = entry_time  # 开仓时间
        self.change_takeprofit=change_takeprofit #修改止盈
        self.change_stoploss=change_stoploss    #修改止损
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
        initial_balance: float = 100000,
        history_window: int = 0,
        CA: str = "",
        Strategy_name='Strategy',
    ):

        self.interval = int(
            (data["timestamp"].iloc[1] - data["timestamp"].iloc[0]) / 1000
        ) 
        self.start_date = int(data["timestamp"].iloc[0] / 1000)
        self.end_date = int(data["timestamp"].iloc[-1] / 1000)
        self.data = data
        self.strategy_callback = strategy_callback
        self.initial_balance = initial_balance
        self.balance = initial_balance  # 当k线结束时，包括所有浮动盈亏的余额
        self.guarantee = initial_balance
        self.history_window = history_window
        self.Strategy_name=Strategy_name
        self.CA = CA
        self.positions = {}  # 当前所有仓位，key 为仓位编号
        self.trades = []  # 记录交易历史
        self.summaries = []  # 记录关闭的仓位总结
        self.position_counter = 0  # 用于生成唯一仓位编号
        self.win = 0
        self.lose = 0
        self.trade_count = 0
        self.closed_profit = 0  # 只计算完全平仓的盈亏
        self.total_bought = 0
        self.total_sold = 0
        self.realized_profit = 0  # 已实现盈亏
        self.unrealized_profit = 0
        self.debug_value = 0

    def run(self):
        self.execute()
        self.save_results()

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
                    self.close_positions(current_row, direction="long", key=None)
                elif action["type"] == "sell_short":
                    self.open_position(current_row, action, direction="short")
                elif action["type"] == "cover":
                    self.close_positions(current_row, direction="short", key=None)

            # 检查止盈止损
            self.check_takeprofit_and_stoploss(current_row)

        for pos in self.positions.values():
            close_price = current_row["close"]
            if pos.direction == "long":
                profit = pos.amount * (close_price - pos.entry_price)
            elif pos.direction == "short":
                profit = pos.amount * (pos.entry_price - close_price)
            self.unrealized_profit += profit

            # self.guarantee +=profit + pos.amount*pos.entry_price
        self.balance += self.unrealized_profit
        self.debug_value = (
            self.unrealized_profit + self.closed_profit + self.initial_balance
        )

    def open_position(self, current_row, action, direction):
        """开仓操作"""
        amount = action["amount"]
        stoploss_levels = action["stoploss_levels"]  # [{"price", "amount"}]
        takeprofit_levels = action["takeprofit_levels"]  # [{"price", "amount"}]
        change_takeprofit=action.get("change_takeprofit",None)
        change_stoploss=action.get('change_stoploss',None)
        entry_price = current_row["close"]
        entry_value = amount * entry_price
        self.total_bought += entry_value

        if self.guarantee >= entry_value:
            self.guarantee -= entry_value
        else:
            return

        position_key = self.position_counter
        self.position_counter += 1
        position = Position(
            key=position_key,
            amount=amount,
            entry_price=entry_price,
            direction=direction,
            entry_time=int(current_row["timestamp"] / 1000),
            change_takeprofit=change_takeprofit,
            change_stoploss=change_stoploss,
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
                "timestamp": int(current_row["timestamp"] / 1000),
                "action": (
                    "open long" if direction == "long" else "open short"
                ),  # Correct action type for open positions
                "amount": position.amount,
                "price": entry_price,
                "guarantee": self.guarantee,
                # "finalized_value": position.final_profit  # At this point, finalized_value will be 0 for open positions
            }
        )

        self.log(
            f"Opened position {position_key}, amount: {amount}, price: {entry_price}, direction: {direction}"
        )

    def close_positions(self, current_row, direction, key=None):
        """平仓操作"""
        to_close = (
            [key]
            if key is not None
            else [
                pos_key
                for pos_key, pos in self.positions.items()
                if pos.direction == direction
            ]
        )

        for pos_key in to_close:
            pos = self.positions[pos_key]

            # 计算时间
            pos.update_hold_time(int(current_row["timestamp"] / 1000))
            close_price = current_row["close"]

            # 如果还有amount，更新余额，以当前close平仓
            if pos.amount > 0:
                if pos.direction == "long":
                    profit = pos.amount * (close_price - pos.entry_price)
                elif pos.direction == "short":
                    profit = pos.amount * (pos.entry_price - close_price)

                pos.final_profit += profit
                self.balance += profit
                self.guarantee += profit + pos.amount * pos.entry_price
                self.realized_profit += profit
                # print(f"Before :{self.closed_profit+pos.final_profit} profit:{pos.final_profit} balance:{self.balance}")

                # 记录交易明细
                self.trades.append(
                    {
                        "key": pos_key,
                        "timestamp": int(current_row["timestamp"] / 1000),
                        "action": (
                            "close long" if pos.direction == "long" else "close short"
                        ),  # Correct action type for closing positions
                        "amount": pos.amount,
                        "price": close_price,
                        "guarantee": self.guarantee,
                        # "finalized_value": pos.final_profit
                    }
                )

            if pos.final_profit > 0:
                self.win += 1
            else:
                self.lose += 1

            self.trade_count += 1
            self.closed_profit += pos.final_profit
            # self.guarantee += pos.entry_value
            # 记录仓位总结
            self.summaries.append(
                {
                    "key": pos.key,
                    "entry_time": pos.entry_time,
                    "exit_time": int(current_row["timestamp"] / 1000),
                    "hold_time": pos.hold_time,
                    "entry_price": pos.entry_price,
                    "entry_value": pos.entry_value,
                    "exit_price": close_price,
                    "amount": pos.initial_buy,
                    "final_profit": pos.final_profit,
                    "PNL": (
                        f"{((pos.final_profit / pos.entry_value) * 100):.3f}%"
                        if pos.entry_value != 0
                        else "N/A%"
                    ),
                }
            )

            # 删除仓位
            del self.positions[pos_key]

            self.log(f"Closed position {pos_key}, profit: {pos.final_profit}")

    def check_takeprofit_and_stoploss(self, current_row):
        """检查止盈和止损"""
        for pos_key, pos in list(self.positions.items()):
            if pos.direction == "long":
                self._process_levels(
                    current_row, pos_key, pos, 
                    price_key="high", 
                    tp_sort_key=lambda x: x["price"], 
                    sl_sort_key=lambda x: -x["price"]
                )
            elif pos.direction == "short":
                self._process_levels(
                    current_row, pos_key, pos, 
                    price_key="low", 
                    tp_sort_key=lambda x: -x["price"], 
                    sl_sort_key=lambda x: x["price"]
                )

    def execute_partial_close(self,current_row, pos_key, level, reason):
        """执行部分平仓"""
        # self.log(pos_key)
        # self.log(self.positions[pos_key])
        pos = self.positions[pos_key]
        close_amount = min(level["amount"], pos.amount)
        close_price = level["price"]
        close_value = close_amount * close_price
        pos.adjust_position(close_amount)
        # 更新仓位
        if reason == "takeprofit":
            pos.takeprofit_levels.remove(level)
        elif reason == "stoploss":
            pos.stoploss_levels.remove(level)

        # 更新余额
        if pos.direction == "long":
            profit = close_amount * (close_price - pos.entry_price)
        elif pos.direction == "short":
            profit = close_amount * (pos.entry_price - close_price)

        pos.final_profit += profit
        self.balance += profit
        self.guarantee += profit + close_amount * pos.entry_price
        self.realized_profit += profit
        # self.log(self.guarantee)
        # 记录交易明细
        self.trades.append(
            {
                "key": pos_key,
                "timestamp": int(current_row["timestamp"] / 1000),
                "action": reason
                + " "
                + pos.direction,  # Correct action type for partial close
                "amount": close_amount,
                "price": close_price,
                "guarantee": self.guarantee,
                # "finalized_value": pos.final_profit
            }
        )

        self.log(
            f"{reason.capitalize()} for position {pos_key}, amount: {close_amount}, price: {close_price}"
        )

        # 如果仓位完全平仓，记录总结
        if pos.is_closed():
            self.close_positions(current_row, pos.direction, key=pos_key)

    def save_results(self):
        """保存交易记录和总结"""

        # 创建包含name和interval的对象
        metadata = {
            "name": self.CA,
            "interval": self.interval,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "opened_count": self.position_counter + 1,
            "closed_count": self.trade_count,
            "win": self.win,
            "lose": self.lose,
            "winrate": (
                f"{((self.win / self.trade_count) * 100):.2f}%"
                if self.trade_count != 0
                else "N/A"
            ),
            "initial_balance": self.initial_balance,
            "closed_profit": self.closed_profit,
            "realized_profit": self.realized_profit,
            "unrealized_profit": self.unrealized_profit,
            "guarantee": self.guarantee,
            "balance": self.balance,
            "debug_value": self.debug_value,
            "total_profit/total_bought": (
                f"{(((self.realized_profit+self.unrealized_profit)/ self.total_bought) * 100):.2f}%"
                if self.initial_balance != 0
                else "N/A"
            ),
            "PNL": (
                f"{((self.realized_profit+self.unrealized_profit)/ self.total_bought*(self.position_counter+1) * 100):.2f}%"
                if self.initial_balance != 0
                else "N/A"
            ),
            "BALANCE CHANGE %": (
                f"{(((self.balance-self.initial_balance )/ self.initial_balance) * 100):.2f}%"
                if self.initial_balance != 0
                else "N/A"
            ),
        }

        # 修改 self.trades 和 self.summaries，使其包含 metadata
        trades_with_metadata = {**metadata, "data": self.trades}
        summaries_with_metadata = {**metadata, "data": self.summaries}
        
        # 创建以Strategy_name命名的文件夹

        output_dir = os.path.join("./BACKTEST", self.Strategy_name)
        os.makedirs(output_dir, exist_ok=True)
        with open(os.path.join(output_dir, f"{self.CA}_trades.json"), "w") as trade_file:
            json.dump(trades_with_metadata, trade_file, indent=4)

        with open(os.path.join(output_dir, f"{self.CA}_summary.json"), "w") as summary_file:
            json.dump(summaries_with_metadata, summary_file, indent=4)

    def _process_levels(self, current_row, pos_key, pos, price_key, tp_sort_key, sl_sort_key):
        """处理止盈和止损"""
        # 处理止盈
        takeprofit_triggered = False
        for tp in sorted(pos.takeprofit_levels, key=tp_sort_key):
            if (price_key == "high" and current_row["high"] >= tp["price"]) or \
            (price_key == "low" and current_row["low"] <= tp["price"]):
                self.execute_partial_close(current_row, pos_key, tp, "takeprofit")
                takeprofit_triggered = True
            else:
                break

        # 如果触发了止盈，则更新止盈
        if not pos.is_closed() and takeprofit_triggered and pos.change_stoploss is not None:
            pos.stoploss_levels = pos.change_stoploss

        # 处理止损
        stoploss_triggered = False
        for sl in sorted(pos.stoploss_levels, key=sl_sort_key):
            if (price_key == "high" and current_row["low"] <= sl["price"]) or \
            (price_key == "low" and current_row["high"] >= sl["price"]):
                self.execute_partial_close(current_row, pos_key, sl, "stoploss")
                stoploss_triggered = True
            else:
                break

        # 如果触发了止损，则更新止损
        if not pos.is_closed() and stoploss_triggered and pos.change_takeprofit is not None:
            pos.takeprofit_levels = pos.change_takeprofit
            

    def log(self, message):
        """记录日志"""
        print(f"[LOG] {message}")


# 使用示例
def moving_average_strategy(current_row, history, positions):

    actions = []
    short_ma = history["close"].mean()  # 短期均线
    long_ma = history["close"].rolling(window=20).mean().iloc[-1]  # 长期均线

    # 开仓
    if short_ma > long_ma:
        actions.append(
            {
                "type": "sell_short",
                "amount": 100,
                "takeprofit_levels": [
                    {"price": current_row["close"] * 0.9, "amount": 50},
                    {"price": current_row["close"] * 0.8, "amount": 25},
                    {"price": current_row["close"] * 0.7, "amount": 25},
                ],
                "stoploss_levels": [{"price": current_row["close"] * 1.2, "amount": 100}],
                "change_stoploss": [{"price": current_row["close"], "amount": 100}],
                # "type": "buy",
                # "amount": 100,
                # "takeprofit_levels": [
                #     {"price": current_row["close"] * 1.1, "amount": 50},
                #     {"price": current_row["close"] * 1.2, "amount": 25},
                #     {"price": current_row["close"] * 1.3, "amount": 25},
                # ],
                # "stoploss_levels": [
                #     {"price": current_row["close"] * 0, "amount": 100}
                # ],
            }
        )

    # 平掉所有多仓
    if short_ma < long_ma and any(
        pos.direction == "long" for pos in positions.values()
    ):
        actions.append(
            {
                "type": "sell",
            }
        )

    return actions


if __name__ == "__main__":
    df = pd.read_csv("./output/kline.csv")  # 请替换成实际的数据路径
    ca = "test"  # 请替换成实际的CA名称
    # 创建回测对象
    backtest = Backtest(
        CA=ca,
        data=df,
        strategy_callback=moving_average_strategy,
        history_window=20,  # 使用20天的历史数据计算均线
        
    )
    backtest.run()