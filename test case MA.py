import pandas as pd
from Backtest import Backtest  # 确保 Backtest 类的路径正确

# 策略回调函数
def moving_average_strategy(current_row, history, positions):
    actions = []
    short_ma = history['close'].mean()  # 短期均线
    long_ma = history['close'].rolling(window=20).mean().iloc[-1]  # 长期均线

    # 开多仓
    if short_ma > long_ma and not any(pos.direction == "long" for pos in positions.values()):
        actions.append({
            "type": "buy",
            "amount": 100,
            "stoploss_levels": [{"price": current_row['close'] * 0, "amount": 100}],
            "takeprofit_levels": [{"price": current_row['close'] * 1.05, "amount": 100}]
        })

    # 平掉所有多仓
    if short_ma < long_ma and any(pos.direction == "long" for pos in positions.values()):
        actions.append({
            "type": "sell",
        })

    return actions

if __name__ == "__main__":
    df = pd.read_csv('./output/kline.csv')  # 请替换成实际的数据路径

    # 创建回测对象
    backtest = Backtest(
        data=df,
        strategy_callback=moving_average_strategy,
        initial_balance=1,
        history_window=20  # 使用20天的历史数据计算均线
    )

    # 运行回测
    backtest.execute()

    # 保存回测结果
    backtest.save_results()