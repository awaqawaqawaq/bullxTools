import pandas as pd
import os
import json
from datetime import datetime, timezone
import pytz

class Statistics:
    def __init__(self, result_dir='./result'):
        self.result_dir = result_dir
        os.makedirs(self.result_dir, exist_ok=True)

    def load_metadata_from_json(self, json_file_path):
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            return metadata
        except FileNotFoundError:
            print(f"Error: File not found: {json_file_path}")
            return None
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON format: {json_file_path}")
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def append_metadata_to_csv(self, file_name, json_file_path):
        metadata = self.load_metadata_from_json(json_file_path)
        if metadata is None:
            return

        if 'data' in metadata:
            del metadata['data']

        file_path = os.path.join(self.result_dir, f"{file_name}.csv")

        if 'start_date' in metadata:
            metadata['start_date'] = self._convert_to_timezone(metadata['start_date'], 8)
        if 'end_date' in metadata:
            metadata['end_date'] = self._convert_to_timezone(metadata['end_date'], 8)
        if 'interval' in metadata:
            metadata['interval'] = metadata['interval'] / 60

        now = datetime.now()
        time_str = now.strftime("%Y%m%d_%H%M%S")
        metadata['timestamp'] = time_str

        df = pd.DataFrame([metadata])

        try:
            if os.path.exists(file_path):
                existing_df = pd.read_csv(file_path)
                df = pd.concat([existing_df, df], ignore_index=True)
            df.to_csv(file_path, index=False, encoding='utf-8')
            print(f"Metadata saved/appended to '{file_path}'")
        except Exception as e:
            print(f"An error occurred while writing to CSV: {e}")

    def _convert_to_timezone(self, timestamp, offset):
        try:
            dt_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            target_timezone = pytz.FixedOffset(offset * 60)
            dt_target_timezone = dt_utc.astimezone(target_timezone)
            return dt_target_timezone.strftime('%Y-%m-%dT%H:%M:%S')
        except Exception as e:
            print(f"Error converting timestamp {timestamp}: {e}")
            return None

    def _convert_to_utc(self, dt):
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.isoformat()
