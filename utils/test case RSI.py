import pandas as pd
import numpy as np
from Backtest import Backtest  

peak = 0 

# 使用示例，只用于演示
# 计算 RSI 的函数
def calculate_rsi(data, window=14):
    delta = data.diff()  # 计算价格变化
    gain = (delta.where(delta > 0, 0)).fillna(0)  # 计算上涨部分
    loss = (-delta.where(delta < 0, 0)).fillna(0) # 计算下跌部分
    avg_gain = gain.rolling(window=window).mean() # 计算平均上涨
    avg_loss = loss.rolling(window=window).mean()  # 计算平均下跌
    rs = avg_gain / avg_loss  # 计算相对强度
    rsi = 100 - (100 / (1 + rs))  # 计算 RSI
    return rsi

# 合并的策略：同时处理超买回撤和超卖
def combined_rsi_strategy(current_row, history, positions):
    actions = []  # 初始化动作列表
    rsi = calculate_rsi(history["close"]).iloc[-1] # 计算当前的 RSI
    peak_price = peak
    # 超买回撤逻辑
    if rsi > 80:
        peak_price = max(peak_price, current_row["high"]) # 查找最近的高点

    # 检查是否从高点回撤 20%
    if current_row["close"] < peak_price * 0.8:
        actions.append(
            {
                "type": "buy",
                "amount": 100,
                "takeprofit_levels": [
                    {"price": current_row["close"] * 1.2, "amount": 50},
                    {"price": current_row["close"] * 1.3, "amount": 50},
                ],
                "stoploss_levels": [
                    {"price": current_row["close"] * 0.8, "amount": 100}
                ],
            }
        )

    # 超卖逻辑
    elif rsi < 30:
        actions.append(
            {
                "type": "buy",
                "amount": 100,
                "takeprofit_levels": [
                   {"price": current_row["close"] * 1.2, "amount": 100}
                ],
                "stoploss_levels": [
                    {"price": current_row["close"] * 0.9, "amount": 100}
                ],
            }
        )

    return actions # 返回动作列表

if __name__ == "__main__":
    df = pd.read_csv('./output/kline.csv')  # 请替换成实际的数据路径

    # 创建回测对象，使用合并后的策略
    backtest = Backtest(
        data=df,
        strategy_callback=combined_rsi_strategy, 
        history_window=20, # 使用 20 根k的历史数据
        CA="combined_rsi_strategy"
    )

    # 运行回测
    backtest.execute()
    # 保存回测结果
    backtest.save_results()
    print("Backtest completed with combined_rsi_strategy")