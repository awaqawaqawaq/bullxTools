import os
from utils.Backtest import Backtest
from utils.KlineDataWithIndicators import Indicators
from utils.Strategy import Strategy
from utils.BullxAPIClient import BullxAPIClient
from utils.Statistic import Statistics

if __name__ == "__main__":
    
    API_KEY = "AIzaSyCdU8BxOul-NOOJ-e-eCf_-5QCz8ULqIPg"
    REFRESH_TOKEN = f"AMf-vBxlvPNuSBquOXW0C3gh5QoIPVhJuD6lZzB9Pvtkk2Mp4a7MjCa2TTi9SZxuAWyGY70vzudQPZnbFd4Od6AV7dq__rSh6YH5Gny8ojVbMjFK7mtLI_e8rHlUjMQ__WGF-Sn7L2Pb8kW-0hSfluyw2VGqXGN61Y-c33rOts-dXQjFXe7Vvt13n1O_N4wqdgQPiFdHrWsCRnIqaeiXitDnApGDX4KLj-SjpHgH2ROohiAjHXlMhKE"
    TOKEN_URL = f"https://securetoken.googleapis.com/v1/token?key={API_KEY}"

    client = BullxAPIClient(API_KEY, REFRESH_TOKEN, TOKEN_URL)

    interval=60
    start_time=1737529043
    end_time=1737644251
    
    # 首先爬取k线数据
    with open("asset/ca.txt", "r") as f:
        for base in f:
            base = base.strip()  # 移除行首尾的空白字符，包括换行符
            if base:  # 确保读取的地址不是空行
                Strategy.process_ca(client, base, "output", start_time, end_time, interval)


    # 在处理完所有合约地址后，遍历 kline_dir 目录
    kline_dir = "output"
    Strategy_name="moving_average_strategy"
    for filename in os.listdir(kline_dir):
        if filename.endswith(".csv"):  # 只处理 CSV 文件
            file_path = os.path.join(kline_dir, filename)  # 构造完整的文件路径
            ca = filename.replace(f"{interval}.csv", "")  # 从文件名中提取CA名称
            chart=Indicators.load_data(file_path)
            chart.add_ema(20)
            chart.add_rsi(14)
            chart.add_sma(50)
            df=chart.save_data(f"{ca}_with_indicators")
            # # 创建回测对象
            backtest = Backtest(
                CA=ca,
                data=df,
                strategy_callback=Strategy.moving_average_strategy,
                history_window=20,  # 使用20根历史数据
                Strategy_name=Strategy_name
            )
            # 运行回测
            backtest.run()
    
    
    # 统计
    trade_dir=f"./BACKTEST./{Strategy_name}"
    S=Statistics()
    for filename in os.listdir(trade_dir):  # 修改为使用 trade_dir
        if filename.endswith("summary.json"):  # 只处理 Summary.json 文件
            json_file_path = os.path.join(trade_dir, filename)  # 构造完整的文件路径
            S.append_metadata_to_csv("Summary", json_file_path) # 传入json_file_path