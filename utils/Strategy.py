from .BullxAPIClient import BullxAPIClient
import pandas as pd
import json
import csv
import os

BUY_1M = 1000000
BUY_10M = 10000000
BUY_100M = 100000000
BUY_1B = 1000000000


# from KlineDataWithIndicators import Indicators 
class Strategy:

    @staticmethod
    def moving_average_strategy(current_row, history, positions):
        """
        基于移动平均线的简单交易策略。
        只用于演示
        Args:
            current_row (pandas.Series): 当前时间点的K线数据，包含已计算的指标。
            history (pandas.DataFrame): 历史K线数据，也包含已计算的指标。
            positions (dict): 当前持仓信息，键为持仓ID，值为Position对象。

        Returns:
            list: 包含交易动作的列表。每个动作是一个字典，包括交易类型、数量等信息。
        """
        actions = []

        # 从当前行获取已经计算好的指标值，并进行空值检查
        SMA_50 = current_row.get('SMA_50')
        EMA_20 = current_row.get('EMA_20')
        if pd.isna(SMA_50) or pd.isna(EMA_20):
            return actions # 如果指标值为空，则直接返回空列表，不做任何操作

        # 检查是否有未平仓的多头仓位
        has_long_position = any(pos.direction == "long" for pos in positions.values())

        # 获取前一个时间点的值
        if not history.empty:
            previous_SMA_50 = history.iloc[-1].get('SMA_50')
            previous_EMA_20 = history.iloc[-1].get('EMA_20')
        else:
           return []
         # 确保 previous_SMA_50 和 previous_EMA_20 不是 None 并且是有效的数字
        if pd.isna(previous_SMA_50) or pd.isna(previous_EMA_20):
           return []
        
        # 开多仓条件：金叉
        if previous_SMA_50 != previous_EMA_20 and SMA_50 > EMA_20 and not has_long_position:
           actions.append({
                "type": "buy",  # 买入类型
                "amount": BUY_10M,  # 买入量
                "stoploss_levels": [{"price": current_row['close'] * 0.99, "amount": 100}],  # 止损位
                "takeprofit_levels": [{"price": current_row['close'] * 1.02, "amount": 100}]  # 止盈位
            })
        # 平多仓条件：死叉
        elif previous_SMA_50 >= previous_EMA_20 and SMA_50 < EMA_20 and has_long_position:
            actions.append({
                "type": "sell",  # 卖出类型
            })
        return actions
    
    
    @staticmethod
    def process_ca(client, base, output_dir, start_time, end_time, intervals):
            
        """
        Process a single token and write its chart data to a CSV file and its creation timestamp to a JSON file.
        爬取CA的k线数据以及基础信息
        Args:
            client (BullxAPIClient): The client to use for fetching data.
            base (str): The base token address.
            output_dir (str): The directory to write the CSV and JSON files to.
            start_time (int): The start time for fetching chart data.
            end_time (int): The end time for fetching chart data.
            intervals (int): The interval in seconds to fetch chart data for.

        Returns:
            None
        """

        json_file = os.path.join(output_dir, f"{base+str(intervals)}.json")
        csv_file = os.path.join(output_dir, f"{base+str(intervals)}.csv")
        
        if os.path.exists(json_file) and os.path.exists(csv_file):
            print(f"Skipping {base+str(intervals)}: JSON and CSV files already exist.")
            return
        
        print(f"Processing: {base}")
        try:
            dataa = client.resolve_tokens(token_addresses=[base])
            if dataa and dataa.get("data") and dataa["data"].get(base):
                creation_timestamp = dataa["data"][base].get("creationBlockTimestamp")
                print(f"Creation Block Timestamp: {creation_timestamp}")
            else:
                print(f"Warning: Unable to resolve token: {base}")
                return
        except Exception as e:
            print(f"Error resolving token {base}: {e}")
            return

        processed_timestamps = set()

        try:
            with open(csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "open", "high", "low", "close", "volume"])

                current_start_time = start_time
                while current_start_time < end_time:
                    current_end_time = min(current_start_time + intervals*1000, end_time)
                    print(f"Fetching data for {base} from {current_start_time} to {current_end_time}")
                    try:
                        klines = client.get_chart_data(
                            token_address=base,
                            interval_secs=intervals,
                            start_time=current_start_time,
                            end_time=current_end_time,
                        )
                        if klines and klines.get("t"):
                            new_data = {"t": [], "o": [], "h": [], "l": [], "c": [], "v": []}
                            for i in range(len(klines["t"])):
                                timestamp = klines["t"][i]
                                if timestamp not in processed_timestamps:
                                    for key in new_data.keys():
                                        if klines.get(key):
                                            new_data[key].append(klines[key][i])
                                    processed_timestamps.add(timestamp)

                            if new_data["t"]:
                                for i in range(len(new_data["t"])):
                                    writer.writerow(
                                        [
                                            new_data["t"][i],
                                            new_data["o"][i],
                                            new_data["h"][i],
                                            new_data["l"][i],
                                            new_data["c"][i],
                                            new_data["v"][i],
                                        ]
                                    )
                    except Exception as e:
                        print(f"Error fetching kline data for {base} between {current_start_time} and {current_end_time}: {e}")

                    current_start_time = current_end_time

        except Exception as e:
            print(f"Error opening or writing to CSV file for {base}: {e}")
            return


        try:
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(dataa, f, indent=4, ensure_ascii=False)
            print(f"Data written to {json_file}")
        except Exception as e:
            print(f"Error writing to JSON file {json_file}: {e}")

        if processed_timestamps:
            print(f"Kline data written to {csv_file}")
        else:
            print(f"Warning: No kline data to write for {base}.")

