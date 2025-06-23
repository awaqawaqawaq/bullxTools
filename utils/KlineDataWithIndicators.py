import pandas as pd
import pandas_ta as ta
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

OUTPUT_DIR = "./Kline_with_indicators"

class Indicators:
    """
    A class to calculate and manage technical indicators on financial data.
    """
    
    def __init__(self, df):
       
        self.df = df
        

    @classmethod
    def load_data(cls,file_path):
        """
        Loads data from a CSV file and sets the timestamp as the index.

        Args:
            file_path (str): The path to the CSV file.

        Returns:
           Indicators: An instance of the Indicators class or None if an error occurs.
        """
        try:
           df = pd.read_csv(file_path)
           # 保留 timestamp 列
           df['timestamp_col'] = df['timestamp']
           # 将时间戳列转换为时间类型
           df["timestamp"] = pd.to_datetime(df["timestamp"])
           # 将时间戳设置为索引
           df.set_index("timestamp", inplace=True)
           return cls(df)
        except FileNotFoundError:
           logging.error(f"Error: The file '{file_path}' was not found.")
           return None
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            return None

    def _reset_timestamp(self):
        """
        Resets the index and restores the 'timestamp_col' as a regular column named 'timestamp'.
        """
        if 'timestamp_col' in self.df.columns:
            self.df = self.df.reset_index()  
            self.df['timestamp'] = self.df['timestamp_col']
            self.df = self.df.drop(columns=['timestamp_col'])
    
    
    def add_sma(self, length, column="close"):
        """
        Adds a Simple Moving Average (SMA) indicator.

        Args:
          length (int): The SMA window size.
          column (str, optional): The column to calculate the SMA on. Defaults to 'close'.

        Returns:
          pandas.DataFrame: The modified DataFrame with the SMA indicator.
        """
        if self.df is not None:
            indicator_name = f"SMA_{length}"
            if indicator_name not in self.df.columns:
                try:
                   self.df[indicator_name] = ta.sma(self.df[column], length=length)
                except Exception as e:
                    logging.error(f"Error calculating SMA: {e}")
            else:
                logging.warning(f"Warning: Column '{indicator_name}' already exists. Skipping SMA calculation.")
        self._reset_timestamp()
        return self.df
    
    def add_ema(self, length, column="close"):
         """
        Adds an Exponential Moving Average (EMA) indicator.

        Args:
          length (int): The EMA window size.
          column (str, optional): The column to calculate the EMA on. Defaults to 'close'.
        Returns:
            pandas.DataFrame: The modified DataFrame with the EMA indicator.
        """
         if self.df is not None:
            indicator_name = f"EMA_{length}"
            if indicator_name not in self.df.columns:
                try:
                    self.df[indicator_name] = ta.ema(self.df[column], length=length)
                except Exception as e:
                    logging.error(f"Error calculating EMA: {e}")
            else:
                logging.warning(
                    f"Warning: Column '{indicator_name}' already exists. Skipping EMA calculation."
                )
                
         self._reset_timestamp()
         return self.df

    def add_macd(self, fast_period=12, slow_period=26, signal_period=9, column="close"):
        """
        Adds the Moving Average Convergence Divergence (MACD) indicator.

        Args:
            fast_period (int, optional): The fast EMA period for MACD. Defaults to 12.
            slow_period (int, optional): The slow EMA period for MACD. Defaults to 26.
            signal_period (int, optional): The signal EMA period for MACD. Defaults to 9.
            column (str, optional): The column to calculate the MACD on. Defaults to 'close'.
        
        Returns:
           pandas.DataFrame: The modified DataFrame with the MACD indicators.
        """
        if self.df is not None:
            macd_names = [
                f"MACD_{fast_period}_{slow_period}_{signal_period}",
                f"MACDh_{fast_period}_{slow_period}_{signal_period}",
                f"MACDs_{fast_period}_{slow_period}_{signal_period}",
            ]
            if not all(name in self.df.columns for name in macd_names):
                try:
                    macd = ta.macd(
                        self.df[column],
                        fast=fast_period,
                        slow=slow_period,
                        signal=signal_period,
                    )
                    self.df = pd.concat([self.df, macd], axis=1)
                except Exception as e:
                    logging.error(f"Error calculating MACD: {e}")
            else:
                logging.warning(
                    f"Warning: MACD columns already exist. Skipping MACD calculation."
                )
        self._reset_timestamp()
        return self.df

    def add_rsi(self, length=14, column="close"):
        """
        Adds the Relative Strength Index (RSI) indicator.
        Args:
           length (int, optional): The RSI window size. Defaults to 14.
           column (str, optional): The column to calculate the RSI on. Defaults to 'close'.

        Returns:
            pandas.DataFrame: The modified DataFrame with the RSI indicator.
        """
        if self.df is not None:
            indicator_name = f"RSI_{length}"
            if indicator_name not in self.df.columns:
                try:
                     self.df[indicator_name] = ta.rsi(self.df[column], length=length)
                except Exception as e:
                     logging.error(f"Error calculating RSI: {e}")
            else:
                logging.warning(
                    f"Warning: Column '{indicator_name}' already exists. Skipping RSI calculation."
                )
        self._reset_timestamp()
        return self.df

    def add_atr(self, length=14):
        """
        Adds the Average True Range (ATR) indicator.

        Args:
            length (int, optional): The ATR window size. Defaults to 14.

        Returns:
            pandas.DataFrame: The modified DataFrame with the ATR indicator.
        """
        if self.df is not None:
            indicator_name = f"ATR_{length}"
            if indicator_name not in self.df.columns:
                try:
                    self.df[indicator_name] = ta.atr(
                        self.df["high"], self.df["low"], self.df["close"], length=length
                    )
                except Exception as e:
                     logging.error(f"Error calculating ATR: {e}")
            else:
                logging.warning(
                    f"Warning: Column '{indicator_name}' already exists. Skipping ATR calculation."
                )
        self._reset_timestamp()
        return self.df
    
    def add_bbands(self, length=20, nbdevup=2, nbdevdn=2, column="close"):
        """
        Adds Bollinger Bands (BBANDS) indicator.

        Args:
            length (int, optional): The Bollinger Bands window size. Defaults to 20.
            nbdevup (int, optional): The number of standard deviations for the upper band. Defaults to 2.
            nbdevdn (int, optional): The number of standard deviations for the lower band. Defaults to 2.
            column (str, optional): The column to calculate the Bollinger Bands on. Defaults to 'close'.
        Returns:
            pandas.DataFrame: The modified DataFrame with the Bollinger Bands indicators.
        """
        if self.df is not None:
            bbands_names = [
                f"BBL_{length}_{nbdevup}",
                f"BBM_{length}_{nbdevup}",
                f"BBU_{length}_{nbdevup}",
            ]
            if not all(name in self.df.columns for name in bbands_names):
                 try:
                    bbands = ta.bbands(self.df[column], length=length, std=nbdevup)
                    self.df = pd.concat([self.df, bbands], axis=1)
                 except Exception as e:
                      logging.error(f"Error calculating BBANDS: {e}")
            else:
                logging.warning(
                    f"Warning: BBANDS columns already exist. Skipping BBANDS calculation."
                )
        self._reset_timestamp()
        return self.df

    def save_data(self, filename):
        """
        Saves the DataFrame with indicators to a CSV file, preventing overwrites.

        Args:
            filename (str): The base filename for the output CSV.
        """
        if self.df is not None:
            self._reset_timestamp() # before saving reset the index and move the timestamp to column
            output_file_path = os.path.join(OUTPUT_DIR, f"{filename}.csv")
            
            if os.path.exists(output_file_path):
                base, ext = os.path.splitext(output_file_path)
                i = 1
                while True:
                    output_file_path = f"{base}_{i}{ext}"
                    if not os.path.exists(output_file_path):
                       break
                    i += 1
            try:
                self.df.to_csv(output_file_path)
                logging.info(f"Processed data saved to '{output_file_path}'")
            except Exception as e:
                logging.error(f"Error saving data: {e}")
        else:
             logging.warning("No data to save.")
        return self.df
            
    def add_all(self, config=None):
            """
            Adds all indicators based on a configuration.
            
            Args:
                config (dict, optional): A dictionary specifying indicator parameters.
                   
                    e.g.
                    config = {
                        "sma": {"length": 20, "column": "close"},
                        "ema": {"length": 50, "column": "close"},
                        "macd": {"fast_period": 12, "slow_period": 26, "signal_period": 9, "column": "close"},
                        "rsi": {"length": 14, "column": "close"},
                        "atr": {"length": 14},
                        "bbands": {"length": 20, "nbdevup": 2, "nbdevdn": 2, "column": "close"},
                     }

                Defaults to standard parameters if config is None.
            
            Returns:
                pandas.DataFrame: The modified DataFrame with the indicators.

            """
            if config is None:
                # Default Configuration if none provided
                config = {
                    "sma": {"length": 20, "column": "close"},
                    "ema": {"length": 50, "column": "close"},
                    "macd": {"fast_period": 12, "slow_period": 26, "signal_period": 9, "column": "close"},
                    "rsi": {"length": 14, "column": "close"},
                    "atr": {"length": 14},
                    "bbands": {"length": 20, "nbdevup": 2, "nbdevdn": 2, "column": "close"},
                }
            
            if self.df is not None:
                if "sma" in config:
                    self.add_sma(**config["sma"])
                if "ema" in config:
                    self.add_ema(**config["ema"])
                if "macd" in config:
                    self.add_macd(**config["macd"])
                if "rsi" in config:
                    self.add_rsi(**config["rsi"])
                if "atr" in config:
                    self.add_atr(**config["atr"])
                if "bbands" in config:
                     self.add_bbands(**config["bbands"])
            return self.df