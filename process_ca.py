from BullxAPIClient import BullxAPIClient
import json
import csv
import os
import time


def process_ca(client, base, output_dir, start_time, end_time, intervals):
        
    """
    Process a single token and write its chart data to a CSV file and its creation timestamp to a JSON file.

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
        print(f"Skipping {base+intervals}: JSON and CSV files already exist.")
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

      
      



  
#使用示例
"""
一次返回1000根k线，范围不要过大
单次超出范围，返回最近的1000根k线
"""
if __name__ == "__main__":
    API_KEY = "AIzaSyCdU8BxOul-NOOJ-e-eCf_-5QCz8ULqIPg"
    REFRESH_TOKEN = f"AMf-vBxlvPNuSBquOXW0C3gh5QoIPVhJuD6lZzB9Pvtkk2Mp4a7MjCa2TTi9SZxuAWyGY70vzudQPZnbFd4Od6AV7dq__rSh6YH5Gny8ojVbMjFK7mtLI_e8rHlUjMQ__WGF-Sn7L2Pb8kW-0hSfluyw2VGqXGN61Y-c33rOts-dXQjFXe7Vvt13n1O_N4wqdgQPiFdHrWsCRnIqaeiXitDnApGDX4KLj-SjpHgH2ROohiAjHXlMhKE"
    TOKEN_URL = f"https://securetoken.googleapis.com/v1/token?key={API_KEY}"
    client = BullxAPIClient(API_KEY, REFRESH_TOKEN, TOKEN_URL)
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    process_ca(client, "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN", output_dir, 1737356115, 1737471445, 60)