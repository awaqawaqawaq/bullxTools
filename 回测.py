import pandas as pd
from typing import Dict, List
import requests
import json
from datetime import datetime, timezone, timedelta
import time
import pytz
import os
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from threading import Lock, Event
import threading
# from ratelimit import limits, sleep_and_retry
import sys
import jwt
from queue import Queue, Empty
from BullxAPIClient import BullxAPIClient   
API_SLEEP_TIME = 1.2  # 每次请求后等待1.2秒，确保不超过每分钟50次
RETRY_SLEEP_TIME = 4  # 重试等待时间为4秒
JSON_NAME = f"./token_records_2025-01-19.json"

# 设置API请求限制
CALLS_PER_MINUTE = 55  # 每分钟最多50次请求
ONE_MINUTE = 60

# 创建全局请求计数器和锁
REQUEST_COUNT = multiprocessing.Value('i', 0)
REQUEST_LOCK = multiprocessing.Lock()
LAST_RESET = multiprocessing.Value('d', time.time())

# 添加文件处理进度锁
PROGRESS_LOCK = Lock()
FILE_LOCK = Lock()

def check_token_validity(token: str) -> bool:
    """检查token是否有效"""
    try:
        decoded = jwt.decode(token, options={"verify_signature": False})
        exp_time = datetime.fromtimestamp(decoded['exp'], timezone.utc)
        return exp_time > datetime.now(timezone.utc)
    except:
        return False

def get_token_expiry(token: str) -> datetime:
    """获取token的过期时间"""
    try:
        decoded = jwt.decode(token, options={"verify_signature": False})
        return datetime.fromtimestamp(decoded['exp'], timezone.utc)
    except:
        return None

class TokenPool:
    """Token池管理类"""
    def __init__(self):
        self.tokens = []  # 存储(token, exp_time)元组的列表
        self.lock = Lock()
        
    def add_token(self, token: str, exp_time: datetime):
        """添加token到池中"""
        with self.lock:
            # 检查是否已存在相同的token
            if not any(t[0] == token for t in self.tokens):
                self.tokens.append((token, exp_time))
            
    def get_valid_token(self) -> tuple:
        """获取一个有效的token"""
        with self.lock:
            now = datetime.now(timezone.utc)
            valid_tokens = [(t, e) for t, e in self.tokens if e > now]
            if valid_tokens:
                return valid_tokens[0]
            return None, None
            
    def remove_token(self, token: str):
        """从池中移除token"""
        with self.lock:
            self.tokens = [(t, e) for t, e in self.tokens if t != token]
            
    def get_token_count(self) -> int:
        """获取当前有效token数量"""
        with self.lock:
            now = datetime.now(timezone.utc)
            return len([(t, e) for t, e in self.tokens if e > now])

    def input_tokens(self) -> bool:
        """
        获取用户输入的tokens
        
        Returns:
            bool: True表示成功获取到有效token，False表示未获取到
        """
        # 清除现有的无效token
        with self.lock:
            now = datetime.now(timezone.utc)
            self.tokens = [(t, e) for t, e in self.tokens if e > now]
        
        while True:
            print("\n请粘贴所有token，每个token一行，完成后输入'y'确认:")
            new_tokens = set()  # 使用set避免重复token
            
            while True:
                try:
                    line = input().strip()
                    if not line:
                        continue
                        
                    if line.lower() == 'y':
                        break
                        
                    # 检查是否是有效的token格式
                    if line.startswith('eyJ'):  # JWT tokens通常以'eyJ'开头
                        if len(line) > 100:  # 简单的长度检查
                            new_tokens.add(line)
                        else:
                            print(f"已忽略无效token: {line[:20]}...")
                            
                except EOFError:
                    break
                    
            if not new_tokens:
                retry = input("\n未输入任何有效token，是否重试? (y/n): ")
                if retry.lower() != 'y':
                    return False
                continue
                
            # 验证新tokens
            valid_count = 0
            print(f"\n收集到 {len(new_tokens)} 个token，正在验证有效性...\n")
            
            for token in new_tokens:
                try:
                    exp_time = get_token_expiry(token)
                    if exp_time and exp_time > datetime.now(timezone.utc):
                        remaining_minutes = (exp_time - datetime.now(timezone.utc)).total_seconds() / 60
                        print(f"Token有效，剩余时间: {remaining_minutes:.1f}分钟")
                        self.add_token(token, exp_time)
                        valid_count += 1
                    else:
                        print(f"Token已过期: {token[:20]}...")
                except Exception as e:
                    print(f"Token验证失败: {str(e)}")
                    
            print(f"\n共验证 {len(new_tokens)} 个token，其中 {valid_count} 个有效\n")
            
            if valid_count > 0:
                return True
                
            retry = input("未获取到有效token，是否重试? (y/n): ")
            if retry.lower() != 'y':
                return False

    def get_all_valid_tokens(self) -> list:
        """获取所有有效的token"""
        with self.lock:
            now = datetime.now(timezone.utc)
            return [(t, e) for t, e in self.tokens if e > now]
            
    def has_valid_tokens(self) -> bool:
        """检查是否有有效的token"""
        return self.get_token_count() > 0

class ProcessingState:
    def __init__(self):
        self.active_tokens = set()  # 当前活跃的token
        self.lock = Lock()  # 用于同步访问
        self.current_file_index = 0  # 当前处理的文件索引
        self.file_lock = Lock()  # 用于同步文件处理
        self.processed_files = set()  # 记录已处理完成的文件
        self.consecutive_errors = 0  # 记录连续错误次数
        self.error_lock = Lock()  # 用于同步错误计数
        self.file_progress = {}  # 记录每个文件的处理进度

    def add_token(self, token):
        with self.lock:
            self.active_tokens.add(token)

    def remove_token(self, token):
        
        with self.lock:
            self.active_tokens.discard(token)

    def get_active_count(self):
        with self.lock:
            return len(self.active_tokens)

    def get_next_file_index(self):
        with self.file_lock:
            current = self.current_file_index
            self.current_file_index += 1
            return current

    def add_processed_file(self, file_path):
        with self.lock:
            self.processed_files.add(file_path)

    def get_next_unprocessed_file(self, file_paths):
        with self.file_lock:
            for i, file_path in enumerate(file_paths):
                if file_path not in self.processed_files:
                    # 检查进度文件
                    progress_file = f"{file_path}.progress"
                    try:
                        if os.path.exists(progress_file):
                            # 读取进度文件内容
                            with open(progress_file, 'r') as f:
                                progress = json.load(f)
                            # 读取原始文件，获取总条数
                            with open(file_path, 'r') as f:
                                total_records = len(json.load(f))
                            # 如果进度小于总条数，说明文件未处理完
                            if progress["last_processed"] < total_records:
                                return i
                            else:
                                self.processed_files.add(file_path)  # 标记为已完成
                        else:
                            return i
                    except Exception as e:
                        print(f"检查进度文件时出错: {str(e)}")
                        return i
            return len(file_paths)  # 没有新的未处理文件
            
    def increment_error_count(self):
        with self.error_lock:
            self.consecutive_errors += 1
            return self.consecutive_errors
            
    def reset_error_count(self):
        with self.error_lock:
            self.consecutive_errors = 0

class ConcurrentProcessor:
    def __init__(self, input_dir, token_pool):
        self.input_dir = input_dir
        self.token_pool = token_pool
        self.file_queue = Queue()
        self.lock = Lock()
        self.processing_files = {}  # 记录正在处理的文件
        self.results = {}  # 记录处理结果
        
    def process(self, process_func):
        """开始并发处理"""
        # 读取所有JSON文件
        input_files = [f for f in os.listdir(self.input_dir) if f.endswith('.json')]
        file_paths = [os.path.join(self.input_dir, f) for f in input_files]
        
        if not file_paths:
            print(f"在 {self.input_dir} 中没有找到JSON文件")
            return
        
        # 将文件放入队列
        for file_path in file_paths:
            self.file_queue.put(file_path)
        
        while True:
            # 确保有有效的token
            while not self.token_pool.has_valid_tokens():
                print("\n所有token都已失效，需要补充新token")
                if not self.token_pool.input_tokens():
                    retry = input("\n是否重试输入token? (y/n): ")
                    if retry.lower() != 'y':
                        print("\n程序终止")
                        return
                    continue
                print("\n成功添加新token")
            
            # 获取所有有效的token
            valid_tokens = self.token_pool.get_all_valid_tokens()
            
            # 创建线程池
            threads = []
            for token, exp_time in valid_tokens:
                t = threading.Thread(
                    target=self.worker,
                    args=(token, exp_time, process_func)
                )
                t.start()
                threads.append(t)
            
            # 等待所有线程完成
            for t in threads:
                t.join()
            
            # 检查是否所有文件都处理完成
            all_completed = True
            for file_path in file_paths:
                progress = load_progress(file_path)
                if get_next_pending_range(progress, float('inf')):
                    all_completed = False
                    break
            
            if all_completed and self.file_queue.empty() and not self.processing_files:
                print("\n所有文件处理完成！")
                break
            
            # 如果还有未完成的文件但token都失效了，继续循环获取新token
            if not self.token_pool.has_valid_tokens():
                continue
                
    def worker(self, token: str, token_exp_time: datetime, process_func):
        """工作线程函数"""
        while True:
            try:
                # 检查token是否已过期
                if datetime.now(timezone.utc) >= token_exp_time:
                    with self.lock:
                        self.token_pool.remove_token(token)
                        print(f"Token {token[:8]}... 已过期，线程退出")
                    break
                
                # 获取下一个要处理的文件
                try:
                    file_path = self.file_queue.get_nowait()
                except Empty:
                    print(f"Token {token[:8]}... 没有更多文件需要处理，线程退出")
                    break
                
                # 更新处理状态
                with self.lock:
                    self.processing_files[file_path] = token
                    print(f"\n线程 {token[:8]}... 开始处理文件: {os.path.basename(file_path)}")
                
                try:
                    # 处理文件
                    result = process_func(file_path, token)
                    
                    if not result:  # token失效
                        with self.lock:
                            self.token_pool.remove_token(token)
                            print(f"Token {token[:8]}... 失效，线程退出")
                            # 将文件放回队列以便其他token处理
                            self.file_queue.put(file_path)
                        break
                    
                    # 更新结果
                    with self.lock:
                        if file_path in self.processing_files:
                            del self.processing_files[file_path]
                        self.results[file_path] = "success"
                        
                except Exception as e:
                    print(f"处理文件时发生错误: {str(e)}")
                    with self.lock:
                        if file_path in self.processing_files:
                            del self.processing_files[file_path]
                        self.results[file_path] = f"error: {str(e)}"
                        # 将文件放回队列重试
                        self.file_queue.put(file_path)
                    
            except Exception as e:
                print(f"工作线程发生错误: {str(e)}")
                break

def process_with_token(token, exp_time, file_paths, state: ProcessingState, token_index, total_tokens):
    """
    使用单个token处理文件的函数
    
    依赖:
    - ProcessingState: 处理状态管理类
    - process_single_file: 单文件处理函数
    - datetime: 时间处理
    - timedelta: 时间间隔计算
    
    功能:
    1. 管理单个token的生命周期
    2. 自检测token过期
    3. 处理文件失败时的重试逻辑
    4. 维护token的错误计数
    
    Args:
        token (str): API token
        exp_time (datetime): token过期时间
        file_paths (list): 需要处理的文件路径列表
        state (ProcessingState): 全局处理状态对象
        token_index (int): 当前token的索引
        total_tokens (int): token总数
    
    Returns:
        bool: True表示正常完成或正常过期，False表示异常失效
    """
    state.add_token(token)
    
    try:
        while True:
            file_index = state.get_next_unprocessed_file(file_paths)
            if file_index >= len(file_paths):
                break
                
            file_path = file_paths[file_index]
            
            # 检查token是否即将过期
            now = datetime.now(timezone.utc)
            if exp_time - now < timedelta(minutes=5):
                print(f"Token即将过期，停止处理")
                return True  # 返回True因为这是正常的过期
                
            # 处理文件，传入token索引信息
            result = process_single_file(file_path, token, token_index, total_tokens, state)
            if not result:
                # 检查是否是因为连续错误导致的回False
                if hasattr(state, 'consecutive_errors') and state.consecutive_errors >= 5:
                    print(f"Token连续错误次数过多，可能已失效")
                    return False
                else:
                    print(f"处理失败，尝试下一个文件")
                    continue
                
            state.add_processed_file(file_path)
                
    finally:
        state.remove_token(token)
    
    return True  # 如果正常完成，返回True

def process_files(input_dir):
    """
    并发处理JSON文件
    """
    state = ProcessingState()
    pause_event = threading.Event()
    pause_event.set()  # 初始状态为运行
    
    def on_press(key):
        try:
            if key.char == 'p':  # 当按下p键时
                if pause_event.is_set():
                    print("\n暂停处理...")
                    pause_event.clear()  # 清除事件，暂停处理
                else:
                    print("\n继续处理...")
                    pause_event.set()  # 设置事件，继续处理
        except AttributeError:
            pass  # 特殊键不处理
    
    # 启动键盘监听
    try:
        import keyboard
        keyboard.on_press(on_press)
    except ImportError:
        print("未安装keyboard模块，暂停功能不可用")
        print("请运行: pip install keyboard")
        return
    
    while True:
        # 获取所有待处理的JSON文件
        input_files = [f for f in os.listdir(input_dir) 
                      if f.endswith('.json') 
                      and not f.endswith('.progress')]
        
        if not input_files:
            print(f"在 {input_dir} 中没有找到JSON文件")
            return
            
        # 检查是否还有未处理完的文件
        has_unfinished_files = False
        unfinished_files = []
        for file_name in input_files:
            file_path = os.path.join(input_dir, file_name)
            progress_file = f"{file_path}.progress"
            
            try:
                # 读取原始文件获取总记录数
                with open(file_path, 'r', encoding='utf-8') as f:
                    total_records = len(json.load(f))
                    
                current_position = 0
                # 检查进度文件是否存在且不为空
                if os.path.exists(progress_file) and os.path.getsize(progress_file) > 0:
                    try:
                        with open(progress_file, 'r', encoding='utf-8') as f:
                            progress = json.load(f)
                        current_position = progress.get("last_processed", 0)
                    except (json.JSONDecodeError, KeyError):
                        # 如果进度文件损坏，重新初始化
                        progress = {"last_processed": 0, "total_records": total_records}
                        with open(progress_file, 'w', encoding='utf-8') as f:
                            json.dump(progress, f, indent=2)
                else:
                    # 如果进度文件不存在或为空，创建新的进度文件
                    progress = {"last_processed": 0, "total_records": total_records}
                    with open(progress_file, 'w', encoding='utf-8') as f:
                        json.dump(progress, f, indent=2)
                
                if current_position < total_records:
                    has_unfinished_files = True
                    unfinished_files.append((file_name, current_position, total_records))
            except Exception as e:
                print(f"检查文件进度时出错: {str(e)}")
                continue
        
        if not has_unfinished_files:
            print("所有文件处理完成！")
            return
            
        print("\n未完成的文件:")
        for file_name, current_pos, total in unfinished_files:
            print(f"{file_name}: {current_pos}/{total} 条记录已处理")
            
        # 获取token
        valid_tokens = []
        while not valid_tokens:
            print("\n当前没有有效token，请输入新的token:")
            valid_tokens = input_tokens()
            if not valid_tokens:
                print("没有输入有效token，是否重试(y/n)")
                if input().lower() != 'y':
                    print("\n程序终止，以下文件未处理完成:")
                    for fname, pos, total in unfinished_files:
                        print(f"{fname}: {pos}/{total} 条记录已处理")
                    return
        
        # 创建线程池，每个token一个线程
        with ThreadPoolExecutor(max_workers=len(valid_tokens)) as executor:
            futures = []
            
            # 为每个未完成的文件创建处理任务
            for file_name, _, _ in unfinished_files:
                file_path = os.path.join(input_dir, file_name)
                
                # 读取文件数据
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    total_records = len(data)
                except Exception as e:
                    print(f"读取文件失败: {str(e)}")
                    continue
                    
                # 读取进度
                progress_file = f"{file_path}.progress"
                current_position = 0
                if os.path.exists(progress_file):
                    with open(progress_file, 'r') as f:
                        progress = json.load(f)
                    current_position = progress["last_processed"]
                
                # 如果文件已处理完，跳过
                if current_position >= total_records:
                    print(f"\n{file_name} 已处理完成，跳过")
                    continue
                
                print(f"\n开始处理 {file_name}...")
                print(f"总记录数: {total_records}, 当前进度: {current_position}")
                
                # 计算每个token应处理的数据范围
                records_per_token = max(1, (total_records - current_position) // len(valid_tokens))
                
                # 为每个token创建任务
                for i, (token, exp_time) in enumerate(valid_tokens):
                    start_idx = current_position + (i * records_per_token)
                    end_idx = start_idx + records_per_token if i < len(valid_tokens)-1 else total_records
                    
                    if start_idx >= end_idx or start_idx >= total_records:
                        continue
                    
                    future = executor.submit(
                        process_token_batch,
                        token,
                        exp_time,
                        file_path,
                        start_idx,
                        end_idx,
                        i,
                        len(valid_tokens),
                        state,
                        pause_event
                    )
                    futures_list.append((future, token, i, file_name))
            
            # 等待所有任务完成或处理失败
            all_tokens_invalid = False
            while futures_list:
                # 检查是否需要暂停
                if not pause_event.is_set():
                    print("处理已暂停，按p继续...")
                    pause_event.wait()  # 等待继续信号
                    print("继续处理...")
                
                done, not_done = wait([f[0] for f in futures_list], timeout=1, return_when=FIRST_COMPLETED)
                
                for future in done:
                    for f, token, idx, fname in futures_list[:]:
                        if f == future:
                            futures_list.remove((f, token, idx, fname))
                            try:
                                if not future.result():
                                    print(f"Token {idx+1} 处理失败")
                                    # 检查是否所有token都失效
                                    remaining_tokens = set(t for _, t, _, _ in futures_list)
                                    if not remaining_tokens:  # 如果没有剩余的有效token
                                        all_tokens_invalid = True
                            except Exception as e:
                                print(f"处理过程中发生错误: {str(e)}")
                                # 检查��否所有token都失效
                                remaining_tokens = set(t for _, t, _, _ in futures_list)
                                if not remaining_tokens:  # 如果没有剩余的有效token
                                    all_tokens_invalid = True
                
                # 如果所有token都失效，重新获取token
                if all_tokens_invalid:
                    print("\n所有token已失效，需要重新输入token")
                    valid_tokens = []
                    while not valid_tokens:
                        valid_tokens = input_tokens()
                        if not valid_tokens:
                            print("没有输入有效token，是否重试(y/n)")
                            if input().lower() != 'y':
                                print("\n程序终止，以下文件未处理完成:")
                                for fname, pos, total in unfinished_files:
                                    print(f"{fname}: {pos}/{total} 条记录已处理")
                                return
                        
                        # 使用新token创建新的任务
                        futures_list = []
                        for file_name, _, _ in unfinished_files:
                            file_path = os.path.join(input_dir, file_name)
                            
                            # 读取最新进度
                            with open(f"{file_path}.progress", 'r') as f:
                                progress = json.load(f)
                            current_position = progress["last_processed"]
                            
                            if current_position >= total_records:
                                continue
                            
                            # 为每个新token创建任务
                            records_per_token = max(1, (total_records - current_position) // len(valid_tokens))
                            for i, (token, exp_time) in enumerate(valid_tokens):
                                start_idx = current_position + (i * records_per_token)
                                end_idx = start_idx + records_per_token if i < len(valid_tokens)-1 else total_records
                                
                                if start_idx >= end_idx or start_idx >= total_records:
                                    continue
                                
                                future = executor.submit(
                                    process_token_batch,
                                    token,
                                    exp_time,
                                    file_path,
                                    start_idx,
                                    end_idx,
                                    i,
                                    len(valid_tokens),
                                    state,
                                    pause_event
                                )
                                futures_list.append((future, token, i, file_name))
                        
                        all_tokens_invalid = False

def process_token_batch(token, exp_time, file_path, start_idx, end_idx, token_idx, total_tokens, state, pause_event):
    """
    处理单个token的数据批次
    """
    print(f"Token {token_idx+1}/{total_tokens} 处理数据范围: {start_idx+1}-{end_idx}")
    
    # 初始化时间窗口数据
    time_windows = {
        "0min": [],
        "2min": [],
        "3min": [],
        "4min": [],
        "5min": []
    }
    
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        for i in range(start_idx, end_idx):
            pause_event.wait()
            
            # 检查token是否过期
            now = datetime.now(timezone.utc)
            if exp_time <= now:
                print(f"Token {token_idx+1} 已过期，停止处理")
                # 保存已处理的数据
                if any(len(window) > 0 for window in time_windows.values()):
                    base_name = os.path.splitext(os.path.basename(file_path))[0]
                    write_to_excel(time_windows, base_name)
                return False
                
            # 添加重试机制
            retry_count = 0
            max_retries = 5
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    # 添加当前索引和总数信息到记录中
                    data[i]["current_index"] = i
                    data[i]["total_records"] = len(data)
                    result = process_single_record(data[i], token, state)
                    if result:
                        # 获取记录数据
                        
                        token_address = data[i]["token_address"]
                        timestamp_call = data[i]["timestamp_call"]
                        ticker = BullxAPIClient.get_ticker(token_address)
                        
                        # 获取图表数据
                        chart_data = fetch_chart_data(token_address, token, timestamp_call, int(time.time()), state)
                        if chart_data and any(chart_data.values()):
                            # 处理不同时间窗口的数据
                            for minutes, data_list in zip([0, 2, 3, 4, 5], time_windows.values()):
                                timestamp_n_min = timestamp_call if minutes == 0 else get_nmin_timestamp(timestamp_call, minutes)
                                chart_data_filtered = chart_data_filter(chart_data, timestamp_n_min)
                                
                                if chart_data_filtered and len(chart_data_filtered.get('t', [])) > 0:
                                    # 计算策略结果
                                    initial_price = chart_data_filtered['o'][0]
                                    strategy_results = calculate_sell_strategy(chart_data_filtered, initial_price)
                                    
                                    # 添加到对应时间窗口
                                    data_entry = {
                                        "ticker": ticker,
                                        "token_address": token_address,
                                        "call_time": format_timestamp(timestamp_call),
                                        "initial_price": initial_price,
                                        "initial_time": format_timestamp(int(chart_data_filtered['t'][0]/1000)),
                                        "lowest_price": min(chart_data_filtered['l']),
                                        "lowest_time": format_timestamp(int(chart_data_filtered['t'][chart_data_filtered['l'].index(min(chart_data_filtered['l']))]/1000)),
                                        "highest_price": max(chart_data_filtered['h']),
                                        "highest_time": format_timestamp(int(chart_data_filtered['t'][chart_data_filtered['h'].index(max(chart_data_filtered['h']))]/1000)),
                                        "current_price": chart_data_filtered['c'][-1],
                                        "current_time": format_timestamp(int(chart_data_filtered['t'][-1]/1000)),
                                        "max_profit_rate": ((max(chart_data_filtered['h']) - initial_price) / initial_price) * 100,
                                        "max_loss_rate": ((min(chart_data_filtered['l']) - initial_price) / initial_price) * 100,
                                        "current_profit_rate": ((chart_data_filtered['c'][-1] - initial_price) / initial_price) * 100,
                                        # 策略1相关字段
                                        "first_sell_price": strategy_results["strategy1"]["first_sell"]["price"],
                                        "first_sell_time": strategy_results["strategy1"]["first_sell"]["time"],
                                        "first_sell_money": strategy_results["strategy1"]["first_sell"]["money"],
                                        "second_sell_price": strategy_results["strategy1"]["second_sell"]["price"],
                                        "second_sell_time": strategy_results["strategy1"]["second_sell"]["time"],
                                        "second_sell_money": strategy_results["strategy1"]["second_sell"]["money"],
                                        "stop_loss_price": strategy_results["strategy1"]["stop_loss"]["price"],
                                        "stop_loss_time": strategy_results["strategy1"]["stop_loss"]["time"],
                                        "stop_loss_money": strategy_results["strategy1"]["stop_loss"]["money"],
                                        "final_sell_price": strategy_results["strategy1"]["final_sell"]["price"],
                                        "final_sell_time": strategy_results["strategy1"]["final_sell"]["time"],
                                        "final_sell_money": strategy_results["strategy1"]["final_sell"]["money"],
                                        "strategy1_remaining_money": strategy_results["strategy1"]["remaining_money"],
                                        # 策略2相关字段
                                        "sell_100_price": strategy_results["strategy2"]["sell_100"]["price"],
                                        "sell_100_time": strategy_results["strategy2"]["sell_100"]["time"],
                                        "sell_100_money": strategy_results["strategy2"]["sell_100"]["money"],
                                        "sell_200_price": strategy_results["strategy2"]["sell_200"]["price"],
                                        "sell_200_time": strategy_results["strategy2"]["sell_200"]["time"],
                                        "sell_200_money": strategy_results["strategy2"]["sell_200"]["money"],
                                        "sell_400_price": strategy_results["strategy2"]["sell_400"]["price"],
                                        "sell_400_time": strategy_results["strategy2"]["sell_400"]["time"],
                                        "sell_400_money": strategy_results["strategy2"]["sell_400"]["money"],
                                        "sell_900_price": strategy_results["strategy2"]["sell_900"]["price"],
                                        "sell_900_time": strategy_results["strategy2"]["sell_900"]["time"],
                                        "sell_900_money": strategy_results["strategy2"]["sell_900"]["money"],
                                        "strategy2_final_sell_price": strategy_results["strategy2"]["final_sell"]["price"],
                                        "strategy2_final_sell_time": strategy_results["strategy2"]["final_sell"]["time"],
                                        "strategy2_final_sell_money": strategy_results["strategy2"]["final_sell"]["money"],
                                        "strategy2_remaining_money": strategy_results["strategy2"]["remaining_money"]
                                    }
                                    data_list.append(data_entry)
                            
                            success = True
                            state.reset_error_count()
                            
                            # 每处理10条数据保存一次
                            if (i + 1) % 10 == 0:
                                base_name = os.path.splitext(os.path.basename(file_path))[0]
                                write_to_excel(time_windows, base_name)
                                print(f"已处理并保存到第 {i + 1} 条数据")
                        
                    if not success:
                        retry_count += 1
                        print(f"处理第 {i + 1} 条数据失败，等待3秒后进行第 {retry_count} 次重试...")
                        time.sleep(3)
                        
                except Exception as e:
                    print(f"处理记录时出错 (重试 {retry_count+1}/{max_retries}): {str(e)}")
                    retry_count += 1
                    print(f"等待3秒后进行第 {retry_count} 次重试...")
                    time.sleep(3)
            
            if not success:
                error_count = state.increment_error_count()
                if error_count >= 5:
                    print(f"Token {token_idx+1} 连续 {error_count} 次错误，判定失效")
                    # 保存已处理的数据
                    if any(len(window) > 0 for window in time_windows.values()):
                        base_name = os.path.splitext(os.path.basename(file_path))[0]
                        write_to_excel(time_windows, base_name)
                    return False
                continue
            
            # 更新进度
            with FILE_LOCK:
                with open(f"{file_path}.progress", 'r+') as f:
                    progress = json.load(f)
                progress["last_processed"] = i + 1
                with open(f"{file_path}.progress", 'w') as f:
                    json.dump(progress, f, indent=2)
        
        # 处理完当前批次，保存数据
        if any(len(window) > 0 for window in time_windows.values()):
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            write_to_excel(time_windows, base_name)
            print(f"已完成并保存 {start_idx+1} 到 {end_idx} 的数据")
            
        return True
        
    except Exception as e:
        print(f"处理批次数据发生错误: {str(e)}")
        # 保存已处理的数据
        if any(len(window) > 0 for window in time_windows.values()):
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            write_to_excel(time_windows, base_name)
        return False

def write_to_excel(time_windows: dict, base_name: str):
    """
    将数据追加到Excel文件的函数
    """
    output_dir = os.path.dirname(JSON_NAME)
    filename = f'输出_{base_name}.xlsx'
    full_path = os.path.join(output_dir, filename)
    
    # JSON持久化文件路径
    json_cache_path = os.path.join(output_dir, f'processed_data_{base_name}.json')
    
    # 使用文件锁确保Excel和JSON操作的线程安全
    with FILE_LOCK:
        try:
            # 读取现有的持久化数据
            existing_data = {minutes: [] for minutes in ['0min', '2min', '3min', '4min', '5min']}
            if os.path.exists(json_cache_path):
                try:
                    # 修改这里：添加encoding='utf-8'
                    with open(json_cache_path, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                except Exception as e:
                    print(f"读取缓存文件时出错: {str(e)}")
            
            # 创建用于检查重复的集合
            existing_keys = {
                minutes: {
                    (item.get('token_address', ''), item.get('window_start_time', '')) 
                    for item in existing_data[minutes]
                }
                for minutes in existing_data
            }

            # 追加新数据（只追加不存在的数据）
            new_data_added = False
            for minutes in time_windows:
                for new_item in time_windows[minutes]:
                    # 确保window_start_time字段存在
                    if 'initial_time' in new_item and 'window_start_time' not in new_item:
                        new_item['window_start_time'] = new_item['initial_time']
                    
                    key = (new_item.get('token_address', ''), new_item.get('window_start_time', ''))
                    if key not in existing_keys[minutes]:
                        existing_data[minutes].append(new_item)
                        existing_keys[minutes].add(key)
                        new_data_added = True
            
            # 只有在有新数据时才写入文件
            if new_data_added:
                # 保存更新后的数据到JSON文件
                try:
                    with open(json_cache_path, 'w', encoding='utf-8') as f:
                        json.dump(existing_data, f, ensure_ascii=False, indent=2)
                except Exception as e:
                    print(f"保存缓存文件时出错: {str(e)}")

                # 创建ExcelWriter对象
                with pd.ExcelWriter(full_path, engine='openpyxl') as writer:
                    # 按照指定顺序处理时间窗口
                    for minutes in ['0min', '2min', '3min', '4min', '5min']:
                        if existing_data[minutes]:  # 只在有数据时写入
                            df = pd.DataFrame(existing_data[minutes])
                            # 按时间排序，确保数据有序
                            if 'window_start_time' in df.columns:
                                df = df.sort_values('window_start_time')
                            df.to_excel(writer, sheet_name=minutes, index=False)
                
                # 打印每个时间窗口的数据统计
                print(f"\n数据已保存到 {full_path}")
                for minutes in ['0min', '2min', '3min', '4min', '5min']:
                    new_count = len(time_windows.get(minutes, []))
                    total_count = len(existing_data[minutes])
                    print(f"{minutes}: 新增 {new_count} 条，总计 {total_count} 条数据")
        except Exception as e:
            print(f"写入Excel时发生错误: {str(e)}")
            # 保存错误数据以供调试
            error_file = os.path.join(output_dir, f'error_data_{base_name}.json')
            try:
                with open(error_file, 'w', encoding='utf-8') as f:
                    json.dump(time_windows, f, ensure_ascii=False, indent=2)
                print(f"错误数据已保存到 {error_file}")
            except Exception as save_error:
                print(f"保存错误数据时出错: {str(save_error)}")

def rate_limited_api_call(url, headers, data=None):
    """
    带有频率限制的API调用函数
    
    依赖:
    - requests: HTTP请求库
    - json: JSON数据处理
    - API_SLEEP_TIME: 请求间隔时间常量
    
    功能:
    1. 发送GET或POST请求
    2. 自动处理请求失败
    3. 检测token失效
    4. 强制请求间隔
    
    Args:
        url (str): API端点URL
        headers (dict): 请求头
        data (dict, optional): POST请求数据
    
    Returns:
        Response/None: 请求响应对象，失败返回None
    """
    try:
        if data:
            response = requests.post(url, headers=headers, data=json.dumps(data))
        else:
            response = requests.get(url, headers=headers)
        
        # 检查token是否失效
        if response.status_code in [401, 403]:
            return None
        
        response.raise_for_status()
        time.sleep(API_SLEEP_TIME)  # 强制等待1.2秒
        return response
    except Exception as e:
        print(f"API请求错误: {e}")
        return None

def get_nmin_timestamp(timestamp_call: int, minutes: int) -> int:
    """
    计算N分钟后时间戳的函数
    
    依赖:
    - None (纯数据处理)
    
    功能:
    1. 计算指定分钟数后的时间戳
    
    Args:
        timestamp_call (int): 起始时间戳(秒)
        minutes (int): 需要增加的分钟数
    
    Returns:
        int: N分钟后的时间戳(秒)
    """
    return timestamp_call + (minutes * 60)

def chart_data_filter(chart_data: dict, timestamp_call: int, timestamp_end: int = None) -> dict:
    """
    过滤K线数据的函数
    
    依赖:
    - None (纯数据处理)
    
    功能:
    1. 将秒级时间戳转换为毫秒级
    2. 按时间范围过滤数据
    3. 提取指定时间窗口的数据
    
    Args:
        chart_data (dict): 原K线数据
        timestamp_call (int): 起始时间戳(秒)
        timestamp_end (int, optional): 结束时间戳(秒)
    
    Returns:
        dict: 过滤后的K线数据，格式与输入同
    """
    # 将秒级时间转换毫秒级
    timestamp_call_ms = timestamp_call * 1000
    timestamp_end_ms = timestamp_end * 1000 if timestamp_end else None

    # 找到需要留的数据的起始索引
    start_index = -1
    end_index = len(chart_data['t']) if timestamp_end_ms is None else -1

    for i, t in enumerate(chart_data['t']):
        if start_index == -1 and t >= timestamp_call_ms:
            start_index = i
        if timestamp_end_ms and t > timestamp_end_ms:
            end_index = i
            break

    # 如果没有找到符合条件的数据，返回空
    if start_index == -1:
        return {}

    # 创建数据字典，截取所有数组从start_index到end_index的数据
    new_chart_data = {
        't': chart_data['t'][start_index:end_index],
        'o': chart_data['o'][start_index:end_index],
        'h': chart_data['h'][start_index:end_index],
        'l': chart_data['l'][start_index:end_index],
        'c': chart_data['c'][start_index:end_index],
        'v': chart_data['v'][start_index:end_index]
    }

    return new_chart_data


def format_timestamp(timestamp: int) -> str:
    """
    格式化时间戳的函数
    
    依赖:
    - datetime: 时间处理
    - pytz: 时区处理
    
    功能:
    1. 将Unix时间戳转换为欧洲/黎时区
    2. 格式化为中文友好格式
    
    Args:
        timestamp (int): Unix时间戳(秒)
    
    Returns:
        str: 格式化的时间字符串 (格式: MM月DD日 HH:MM:SS)
    """
    utc_time = datetime.fromtimestamp(timestamp, pytz.UTC)
    tz = pytz.timezone('Europe/Paris')
    local_time = utc_time.astimezone(tz)
    formatted_time = local_time.strftime('%m月%d日 %H:%M:%S')
    return formatted_time


def fetch_chart_data(base, api_token, time_from, time_to, state=None, current_index=None, total_records=None):
    """
    获取K线数据的函数
    """
    url = "https://api-edge.bullx.io/chart"
    TWELVE_HOURS = 12 * 60 * 60

    headers = {
        "accept": "application/json, text/plain, */*",
        "authorization": f"Bearer {api_token}",
        "content-type": "text/plain",
        "sec-ch-ua": "\"Chromium\";v=\"128\", \"Not;A=Brand\";v=\"24\", \"Google Chrome\";v=\"128\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "Referer": "https://bullx.io/",
        "Referrer-Policy": "strict-origin-when-cross-origin"
    }

    merged_data = {'t': [], 'o': [], 'h': [], 'l': [], 'c': [], 'v': []}
    
    current_time = time_from
    max_retries = 5
    total_error_count = 0
    
    while current_time < time_to:
        batch_end = min(current_time + TWELVE_HOURS, time_to)
        # 修改输出格式
        if current_index is not None and total_records is not None:
            print(f"正在获取数据（第{current_index+1}/{total_records}条代币）: {format_timestamp(current_time)} - {format_timestamp(batch_end)}")
        else:
            print(f"正在获取数据: {format_timestamp(current_time)} - {format_timestamp(batch_end)}")

        payload = {
            "name": "chart",
            "data": {
                "chainId": 1399811149,
                "base": base,
                "quote": "So11111111111111111111111111111111111111112",
                "from": current_time,
                "to": batch_end,
                "intervalSecs": 60,
            }
        }

        success = False
        retry_count = 0
        
        while not success and retry_count < max_retries:
            try:
                time.sleep(API_SLEEP_TIME)
                response = rate_limited_api_call(url, headers=headers, data=payload)
                
                if response is None:
                    retry_count += 1
                    total_error_count += 1
                    if retry_count < max_retries:
                        print(f"第 {retry_count} 次重试...")
                        time.sleep(RETRY_SLEEP_TIME * 2)
                        continue
                    if state and total_error_count >= 5:
                        error_count = state.increment_error_count()
                        if error_count >= 5:
                            print(f"连续 {error_count} 次错误，token已失效")
                            return None
                    break
                    
                batch_data = response.json()
                if batch_data and all(key in batch_data for key in ['t', 'o', 'h', 'l', 'c', 'v']):
                    for key in merged_data:
                        merged_data[key].extend(batch_data[key])
                    success = True
                    total_error_count = 0  # 重置错误计数
                    if state:
                        state.reset_error_count()
                        
                    # 每个时间段处理完就写入数据
                    if merged_data and any(merged_data.values()):
                        return merged_data
                else:
                    retry_count += 1
                    total_error_count += 1
                    if retry_count < max_retries:
                        print(f"数据格式错误，第 {retry_count} 次重试...")
                        time.sleep(RETRY_SLEEP_TIME * 2)
                        continue
                    if state and total_error_count >= 5:
                        error_count = state.increment_error_count()
                        if error_count >= 5:
                            print(f"连续 {error_count} 次错误，token已失效")
                            return None
                    break
                    
            except Exception as e:
                print(f"请求错误: {e}")
                retry_count += 1
                total_error_count += 1
                if retry_count < max_retries:
                    print(f"第 {retry_count} 次重试...")
                    time.sleep(RETRY_SLEEP_TIME * 2)
                    continue
                if state and total_error_count >= 5:
                    error_count = state.increment_error_count()
                    if error_count >= 5:
                        print(f"连续 {error_count} 次错误，token已失效")
                        return None
                break

        if not success:
            print(f"获取时间段数据失败: {format_timestamp(current_time)} - {format_timestamp(batch_end)}")
            if total_error_count >= 5:
                print("连续失败次数过多，停止获取")
                return None
            
        current_time = batch_end

    if not any(merged_data.values()):
        print("未获取到任何数据")
        return None

    return merged_data


def load_data_call(file_path: str):
    """
    加载JSON数据文件的函数
    
    依赖:
    - json: JSON数据处理
    
    功能:
    1. 读取JSON文件
    2. 错误处理和提示
    
    Args:
        file_path (str): JSON文件路径
    
    Returns:
        list: JSON数据列表
    
    Raises:
        FileNotFoundError: 文件不存在时出
        JSONDecodeError: JSON格式错误时抛出
    """
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            print(f"成功读取{len(data)}条数据")
            return data
    except FileNotFoundError:
        print(f"文件 {file_path} 未找到")
        raise
    except json.JSONDecodeError:
        print(f"文件 {file_path} 格式错误")
        raise

def calculate_sell_strategy(chart_data_filtered, initial_price, initial_investment=100):
    """
    计算两种卖出策略
    策略1: 100%卖50%, 400%卖30%, 剩余在结束时卖出
    策略2: 100%卖25%, 200%卖25%, 400%卖25%, 900%卖25%
    """
    # 初始化变量
    remaining_position = 1.0
    remaining_money = initial_investment
    stop_loss_price = initial_price * 0.55  # 45%止损
    
    # 策略1的卖出记录
    strategy1 = {
        "first_sell": {"price": None, "time": "", "money": None},
        "second_sell": {"price": None, "time": "", "money": None},
        "stop_loss": {"price": None, "time": "", "money": None},
        "final_sell": {"price": None, "time": "", "money": None},
        "remaining_money": 0
    }
    
    # 策略2的卖出记录
    strategy2 = {
        "sell_100": {"price": None, "time": "", "money": None},
        "sell_200": {"price": None, "time": "", "money": None},
        "sell_400": {"price": None, "time": "", "money": None},
        "sell_900": {"price": None, "time": "", "money": None},
        "stop_loss": {"price": None, "time": "", "money": None},
        "final_sell": {"price": None, "time": "", "money": None},
        "remaining_money": 0
    }

    # 计算目标价格 - 策略1
    first_target_price = initial_price * 2  # 100%收益
    second_target_price = initial_price * 4  # 400%收益

    # 计算目标价格 - 策略2
    target_100 = initial_price * 2    # 100%收益
    target_200 = initial_price * 3    # 200%收益
    target_400 = initial_price * 5    # 400%收益
    target_900 = initial_price * 10   # 900%收益

    # 策略1的状态
    s1_remaining = 1.0
    s1_money = initial_investment

    # 策略2的状态
    s2_remaining = 1.0
    s2_money = initial_investment

    # 遍历价格数据寻找卖出点
    for idx in range(len(chart_data_filtered['l'])):
        low_price = chart_data_filtered['l'][idx]
        high_price = chart_data_filtered['h'][idx]
        current_time = format_timestamp(int(chart_data_filtered['t'][idx] / 1000))

        # 检查是否触发止损 (两个策略共用)
        if low_price <= stop_loss_price:
            if strategy1["stop_loss"]["price"] is None and s1_remaining > 0:
                strategy1["stop_loss"]["price"] = stop_loss_price
                strategy1["stop_loss"]["time"] = current_time
                strategy1["stop_loss"]["money"] = initial_investment * s1_remaining * 0.55
                strategy1["remaining_money"] = (strategy1["first_sell"]["money"] or 0) + \
                                            (strategy1["second_sell"]["money"] or 0) + \
                                            strategy1["stop_loss"]["money"]
                s1_remaining = 0

            if strategy2["stop_loss"]["price"] is None and s2_remaining > 0:
                strategy2["stop_loss"]["price"] = stop_loss_price
                strategy2["stop_loss"]["time"] = current_time
                strategy2["stop_loss"]["money"] = initial_investment * s2_remaining * 0.55
                strategy2["remaining_money"] = (strategy2["sell_100"]["money"] or 0) + \
                                            (strategy2["sell_200"]["money"] or 0) + \
                                            (strategy2["sell_400"]["money"] or 0) + \
                                            (strategy2["sell_900"]["money"] or 0) + \
                                            strategy2["stop_loss"]["money"]
                s2_remaining = 0
            break  # 触发止损后结束

        # 策略1的卖出逻辑
        if s1_remaining > 0:
            if strategy1["first_sell"]["price"] is None and low_price <= first_target_price <= high_price:
                strategy1["first_sell"]["price"] = first_target_price
                strategy1["first_sell"]["time"] = current_time
                strategy1["first_sell"]["money"] = initial_investment * 0.5 * 2
                s1_remaining = 0.5
                s1_money = strategy1["first_sell"]["money"] + (initial_investment * 0.5)

            elif strategy1["first_sell"]["price"] is not None and \
                 strategy1["second_sell"]["price"] is None and \
                 low_price <= second_target_price <= high_price:
                strategy1["second_sell"]["price"] = second_target_price
                strategy1["second_sell"]["time"] = current_time
                strategy1["second_sell"]["money"] = initial_investment * 0.3 * 4
                s1_remaining = 0.2
                s1_money = strategy1["first_sell"]["money"] + \
                          strategy1["second_sell"]["money"] + \
                          (initial_investment * 0.2)

        # 策略2的卖出逻辑
        if s2_remaining > 0:
            if strategy2["sell_100"]["price"] is None and low_price <= target_100 <= high_price:
                strategy2["sell_100"]["price"] = target_100
                strategy2["sell_100"]["time"] = current_time
                strategy2["sell_100"]["money"] = initial_investment * 0.25 * 2
                s2_remaining = 0.75
                s2_money = strategy2["sell_100"]["money"] + (initial_investment * 0.75)

            elif strategy2["sell_100"]["price"] is not None and \
                 strategy2["sell_200"]["price"] is None and \
                 low_price <= target_200 <= high_price:
                strategy2["sell_200"]["price"] = target_200
                strategy2["sell_200"]["time"] = current_time
                strategy2["sell_200"]["money"] = initial_investment * 0.25 * 3
                s2_remaining = 0.5
                s2_money = strategy2["sell_100"]["money"] + \
                          strategy2["sell_200"]["money"] + \
                          (initial_investment * 0.5)

            elif strategy2["sell_200"]["price"] is not None and \
                 strategy2["sell_400"]["price"] is None and \
                 low_price <= target_400 <= high_price:
                strategy2["sell_400"]["price"] = target_400
                strategy2["sell_400"]["time"] = current_time
                strategy2["sell_400"]["money"] = initial_investment * 0.25 * 5
                s2_remaining = 0.25
                s2_money = strategy2["sell_100"]["money"] + \
                          strategy2["sell_200"]["money"] + \
                          strategy2["sell_400"]["money"] + \
                          (initial_investment * 0.25)

            elif strategy2["sell_400"]["price"] is not None and \
                 strategy2["sell_900"]["price"] is None and \
                 low_price <= target_900 <= high_price:
                strategy2["sell_900"]["price"] = target_900
                strategy2["sell_900"]["time"] = current_time
                strategy2["sell_900"]["money"] = initial_investment * 0.25 * 10
                s2_remaining = 0
                s2_money = strategy2["sell_100"]["money"] + \
                          strategy2["sell_200"]["money"] + \
                          strategy2["sell_400"]["money"] + \
                          strategy2["sell_900"]["money"]

    # 添加移动止盈的计算
    def calculate_trailing_stop(prices, times, highest_price=None, trailing_percentage=0.45):
        """计算移动止盈卖出点
        Args:
            prices: 价格列表
            times: 时间戳列表
            highest_price: 起始最高价（如果没有则从价格中计算）
            trailing_percentage: 回撤百分比（0.45 表示 45%）
        Returns:
            (sell_price, sell_time): 卖出价格和时间
        """
        if not prices or not times:
            return None, None
            
        current_highest = highest_price if highest_price is not None else prices[0]
        sell_price = None
        sell_time = None
        
        for i, price in enumerate(prices):
            # 更新最高价
            if price > current_highest:
                current_highest = price
            
            # 检查是否触发移动止盈
            stop_price = current_highest * (1 - trailing_percentage)
            if price <= stop_price:
                sell_price = price
                sell_time = format_timestamp(int(times[i]/1000))
                break
                
        return sell_price, sell_time

    # 策略1的移动止盈计算
    if s1_remaining > 0 and strategy1["stop_loss"]["price"] is None:
        # 获取剩余仓位对应的价格和时间序列
        remaining_prices = []
        remaining_times = []
        start_idx = 0
        
        # 找到最后一次卖出后的起始位置
        if strategy1["second_sell"]["price"]:
            for i, t in enumerate(chart_data_filtered['t']):
                if format_timestamp(int(t/1000)) == strategy1["second_sell"]["time"]:
                    start_idx = i + 1
                    break
        elif strategy1["first_sell"]["price"]:
            for i, t in enumerate(chart_data_filtered['t']):
                if format_timestamp(int(t/1000)) == strategy1["first_sell"]["time"]:
                    start_idx = i + 1
                    break
        
        remaining_prices = chart_data_filtered['c'][start_idx:]
        remaining_times = chart_data_filtered['t'][start_idx:]
        
        sell_price, sell_time = calculate_trailing_stop(remaining_prices, remaining_times, trailing_percentage=0.45)
        
        if sell_price:
            strategy1["final_sell"]["price"] = sell_price
            strategy1["final_sell"]["time"] = sell_time
            strategy1["final_sell"]["money"] = initial_investment * s1_remaining * (sell_price / initial_price)
        else:
            # 如果没有触发移动止盈，使用最后价格
            strategy1["final_sell"]["price"] = chart_data_filtered['c'][-1]
            strategy1["final_sell"]["time"] = format_timestamp(int(chart_data_filtered['t'][-1]/1000))
            strategy1["final_sell"]["money"] = initial_investment * s1_remaining * (chart_data_filtered['c'][-1] / initial_price)
        
        strategy1["remaining_money"] = (strategy1["first_sell"]["money"] or 0) + \
                                     (strategy1["second_sell"]["money"] or 0) + \
                                     strategy1["final_sell"]["money"]

    # 策略2的移动止盈计算
    if s2_remaining > 0 and strategy2["stop_loss"]["price"] is None:
        # 获取剩余仓位对应的价格和时间序列
        remaining_prices = []
        remaining_times = []
        start_idx = 0
        
        # 找到最后一次卖出后的起始位置
        if strategy2["sell_900"]["price"]:
            for i, t in enumerate(chart_data_filtered['t']):
                if format_timestamp(int(t/1000)) == strategy2["sell_900"]["time"]:
                    start_idx = i + 1
                    break
        elif strategy2["sell_400"]["price"]:
            for i, t in enumerate(chart_data_filtered['t']):
                if format_timestamp(int(t/1000)) == strategy2["sell_400"]["time"]:
                    start_idx = i + 1
                    break
        elif strategy2["sell_200"]["price"]:
            for i, t in enumerate(chart_data_filtered['t']):
                if format_timestamp(int(t/1000)) == strategy2["sell_200"]["time"]:
                    start_idx = i + 1
                    break
        elif strategy2["sell_100"]["price"]:
            for i, t in enumerate(chart_data_filtered['t']):
                if format_timestamp(int(t/1000)) == strategy2["sell_100"]["time"]:
                    start_idx = i + 1
                    break
                    
        remaining_prices = chart_data_filtered['c'][start_idx:]
        remaining_times = chart_data_filtered['t'][start_idx:]
        
        # 策略2使用不同的移动止盈百分比
        sell_price, sell_time = calculate_trailing_stop(remaining_prices, remaining_times, trailing_percentage=0.45)
        
        if sell_price:
            strategy2["final_sell"]["price"] = sell_price
            strategy2["final_sell"]["time"] = sell_time
            strategy2["final_sell"]["money"] = initial_investment * s2_remaining * (sell_price / initial_price)
        else:
            # 如果没有触发移动止盈，使用最后价格
            strategy2["final_sell"]["price"] = chart_data_filtered['c'][-1]
            strategy2["final_sell"]["time"] = format_timestamp(int(chart_data_filtered['t'][-1]/1000))
            strategy2["final_sell"]["money"] = initial_investment * s2_remaining * (chart_data_filtered['c'][-1] / initial_price)
        
        strategy2["remaining_money"] = sum(filter(None, [
            strategy2["sell_100"]["money"],
            strategy2["sell_200"]["money"],
            strategy2["sell_400"]["money"],
            strategy2["sell_900"]["money"],
            strategy2["final_sell"]["money"]
        ]))

    return {
        "strategy1": strategy1,
        "strategy2": strategy2
    }


def process_single_file(file_path, current_token, token_index=0, total_tokens=1, state=None):
    """
    处理单个文件的函数
    """
    print(f"\n开始处理 {os.path.basename(file_path)}...")
    failed_records = []  # 初始化失败记录列表
    
    # 设置回测终止时间
    timestamp_end = int(time.time())
    print(f"回测终止时间：{format_timestamp(timestamp_end)}")
    
    time_windows = {
        "0min": [],
        "2min": [],
        "3min": [],
        "4min": [],
        "5min": []
    }

    try:
        # 修改这里：添加encoding='utf-8'
        with open(file_path, 'r', encoding='utf-8') as f:
            data_call = sorted(json.load(f), key=lambda x: x['timestamp_call'])
        data_call_len = len(data_call)
        
        # 添加新的数据分割逻辑
        records_per_token = data_call_len // total_tokens
        remainder = data_call_len % total_tokens
        
        # 计算当前token的处理范围
        start_index = token_index * records_per_token
        if token_index == total_tokens - 1:
            end_index = data_call_len
        else:
            end_index = start_index + records_per_token
            if remainder > 0 and token_index < remainder:
                start_index += token_index
                end_index += token_index + 1
            else:
                start_index += remainder
                end_index += remainder
                
        # 使用文件锁确保线程安全
        with FILE_LOCK:
            # 读取进度文件
            progress_file = f"{file_path}.progress"
            start_index = 0
            
            # 检查进度文件是否存在且不为空
            if os.path.exists(progress_file) and os.path.getsize(progress_file) > 0:
                try:
                    with open(progress_file, 'r', encoding='utf-8') as f:
                        progress = json.load(f)
                    start_index = progress.get("last_processed", 0)
                    print(f"从断点 {start_index} 继续处理")
                except (json.JSONDecodeError, KeyError):
                    # 如果进度文件损坏，重新初始化
                    progress = {"last_processed": 0, "total_records": data_call_len}
            else:
                # 如果进度文件不存在或为空，创建新的进度文件
                progress = {"last_processed": 0, "total_records": data_call_len}
                
            # 更新进度文件
            with open(progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress, f, indent=2)
                
        print(f"Token {token_index + 1}/{total_tokens} 处理数据范围: {start_index + 1}-{end_index}/{data_call_len}")
        
        if start_index >= data_call_len:  # 修改判断条件
            print("所有数据已处理完成")
            return True
            
        if start_index >= end_index:  # 当前token的批次已处理完
            print("当前批次已处理完成")
            return True
        
        for i in range(start_index, end_index):
            call = data_call[i]
            token_address = call["token_address"]
            timestamp_call = call["timestamp_call"]
            
            ticker = BullxAPIClient.get_ticker(token_address)
            print(f"\n({i + 1}/{data_call_len}) ${ticker} {format_timestamp(timestamp_call)} | {token_address}")
            
            max_retries = 5
            retry_count = 0
            success = False
            
            while retry_count < max_retries:
                try:
                    # 修改这里：传递当前处理的代币序号和总数
                    chart_data = (token_address, current_token, timestamp_call, timestamp_end, state, i, data_call_len)
                    if chart_data is None:  # API错误
                        retry_count += 1
                        if retry_count >= max_retries:
                            # 回滚进度到当前位置
                            with FILE_LOCK:
                                with open(progress_file, 'r+') as f:
                                    progress = json.load(f)
                                progress["last_processed"] = i
                                with open(progress_file, 'w') as f:
                                    json.dump(progress, f, indent=2)
                            print(f"\n当前进度已保存到第 {i} 条数据")
                            print("Token失效，请输入新的token继续处理")
                            return False
                        print(f"第 {retry_count} 次重试...")
                        time.sleep(RETRY_SLEEP_TIME * 2)
                        continue
                        
                    # 如果成功获取数据
                    if chart_data and any(chart_data.values()):
                        success = True
                        break  # 添加break，因为已经成功了
                    else:
                        retry_count += 1
                        if retry_count >= max_retries:
                            failed_records.append((i, call))
                            break
                        print(f"第 {retry_count} 次重试...")
                        time.sleep(RETRY_SLEEP_TIME * 2)
                        continue
                    
                except Exception as e:
                    print(f"获取数据失败: {e}")
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(RETRY_SLEEP_TIME * 2)
                        continue
                    failed_records.append((i, call))
                    break

            if not success:
                print(f"无法获取数据，记录失败: {token_address}")
                continue

            # 处理数据
            for minutes, data_list in zip([0, 2, 3, 4, 5], time_windows.values()):
                timestamp_n_min = timestamp_call if minutes == 0 else get_nmin_timestamp(timestamp_call, minutes)
                chart_data_filtered = chart_data_filter(chart_data, timestamp_n_min, timestamp_end)
                
                if chart_data_filtered and len(chart_data_filtered.get('t', [])) > 0:
                    # 获取初始价格和时间
                    initial_price = chart_data_filtered['o'][0]
                    initial_time = format_timestamp(int(chart_data_filtered['t'][0] / 1000))

                    # 获取最低价和时间
                    lowest_price = min(chart_data_filtered['l'])
                    lowest_time_index = chart_data_filtered['l'].index(lowest_price)
                    lowest_time = format_timestamp(int(chart_data_filtered['t'][lowest_time_index] / 1000))

                    # 获取最高价和时间
                    highest_price = max(chart_data_filtered['h'])
                    highest_time_index = chart_data_filtered['h'].index(highest_price)
                    highest_time = format_timestamp(int(chart_data_filtered['t'][highest_time_index] / 1000))

                    # 获取最终价格和时间
                    current_price = chart_data_filtered['c'][-1]
                    current_time = format_timestamp(int(chart_data_filtered['t'][-1] / 1000))

                    # 计算收益
                    max_profit_rate = ((highest_price - initial_price) / initial_price) * 100
                    max_loss_rate = ((lowest_price - initial_price) / initial_price) * 100
                    current_profit_rate = ((current_price - initial_price) / initial_price) * 100

                    # 计算交易策略
                    strategy_results = calculate_sell_strategy(chart_data_filtered, initial_price)

                    data_entry = {
                        "ticker": ticker,
                        "token_address": token_address,
                        "call_time": format_timestamp(timestamp_call),
                        "initial_price": initial_price,
                        "initial_time": initial_time,
                        "lowest_price": lowest_price,
                        "lowest_time": lowest_time,
                        "highest_price": highest_price,
                        "highest_time": highest_time,
                        "current_price": current_price,
                        "current_time": current_time,
                        "max_profit_rate": max_profit_rate,
                        "max_loss_rate": max_loss_rate,
                        "current_profit_rate": current_profit_rate,
                        # 策略1相关字段
                        "first_sell_price": strategy_results["strategy1"]["first_sell"]["price"],
                        "first_sell_time": strategy_results["strategy1"]["first_sell"]["time"],
                        "first_sell_money": strategy_results["strategy1"]["first_sell"]["money"],
                        "second_sell_price": strategy_results["strategy1"]["second_sell"]["price"],
                        "second_sell_time": strategy_results["strategy1"]["second_sell"]["time"],
                        "second_sell_money": strategy_results["strategy1"]["second_sell"]["money"],
                        "stop_loss_price": strategy_results["strategy1"]["stop_loss"]["price"],
                        "stop_loss_time": strategy_results["strategy1"]["stop_loss"]["time"],
                        "stop_loss_money": strategy_results["strategy1"]["stop_loss"]["money"],
                        "final_sell_price": strategy_results["strategy1"]["final_sell"]["price"],
                        "final_sell_time": strategy_results["strategy1"]["final_sell"]["time"],
                        "final_sell_money": strategy_results["strategy1"]["final_sell"]["money"],
                        "strategy1_remaining_money": strategy_results["strategy1"]["remaining_money"],
                        # 策略2相关字段
                        "sell_100_price": strategy_results["strategy2"]["sell_100"]["price"],
                        "sell_100_time": strategy_results["strategy2"]["sell_100"]["time"],
                        "sell_100_money": strategy_results["strategy2"]["sell_100"]["money"],
                        "sell_200_price": strategy_results["strategy2"]["sell_200"]["price"],
                        "sell_200_time": strategy_results["strategy2"]["sell_200"]["time"],
                        "sell_200_money": strategy_results["strategy2"]["sell_200"]["money"],
                        "sell_400_price": strategy_results["strategy2"]["sell_400"]["price"],
                        "sell_400_time": strategy_results["strategy2"]["sell_400"]["time"],
                        "sell_400_money": strategy_results["strategy2"]["sell_400"]["money"],
                        "sell_900_price": strategy_results["strategy2"]["sell_900"]["price"],
                        "sell_900_time": strategy_results["strategy2"]["sell_900"]["time"],
                        "sell_900_money": strategy_results["strategy2"]["sell_900"]["money"],
                        "strategy2_final_sell_price": strategy_results["strategy2"]["final_sell"]["price"],
                        "strategy2_final_sell_time": strategy_results["strategy2"]["final_sell"]["time"],
                        "strategy2_final_sell_money": strategy_results["strategy2"]["final_sell"]["money"],
                        "strategy2_remaining_money": strategy_results["strategy2"]["remaining_money"]
                    }
                    data_list.append(data_entry)
                    
                    # 每处理完一条数据就写入Excel
                    base_name = os.path.splitext(os.path.basename(file_path))[0]
                    write_to_excel(time_windows, base_name)
                    
                    # 只显示0min和2min的结果
                    if minutes in [0, 2]:
                        window_name = "全部数据" if minutes == 0 else f"{minutes}分钟窗口"
                        print(f"\n{window_name}:")
                        print(f"喊单价格: {initial_price:.10f} ({initial_time})")
                        print(f"最低价格: {lowest_price:.10f} ({lowest_time})")
                        print(f"最高价格: {highest_price:.10f} ({highest_time})")
                        print(f"最终价格: {current_price:.10f} ({current_time})")
                        print(f"最高收益率: {max_profit_rate:.2f}%")
                        print(f"最大亏损: {max_loss_rate:.2f}%")
                        print(f"最终收益率: {current_profit_rate:.2f}%")

                        # 打印交易策略结果
                        print("\n策略1收益:")
                        if strategy_results["strategy1"]["first_sell"]["price"]:
                            print(f"首次卖出(50%): {strategy_results['strategy1']['first_sell']['price']:.10f} ({strategy_results['strategy1']['first_sell']['time']}) 卖出: ${strategy_results['strategy1']['first_sell']['money']:.2f}")
                        if strategy_results["strategy1"]["second_sell"]["price"]:
                            print(f"二次卖出(30%): {strategy_results['strategy1']['second_sell']['price']:.10f} ({strategy_results['strategy1']['second_sell']['time']}) 卖出: ${strategy_results['strategy1']['second_sell']['money']:.2f}")
                        if strategy_results["strategy1"]["stop_loss"]["price"]:
                            print(f"止损卖出: {strategy_results['strategy1']['stop_loss']['price']:.10f} ({strategy_results['strategy1']['stop_loss']['time']}) 卖出: ${strategy_results['strategy1']['stop_loss']['money']:.2f}")
                        elif strategy_results["strategy1"]["final_sell"]["price"]:
                            print(f"最终卖出: {strategy_results['strategy1']['final_sell']['price']:.10f} ({strategy_results['strategy1']['final_sell']['time']}) 卖出: ${strategy_results['strategy1']['final_sell']['money']:.2f}")
                        print(f"总收益: ${strategy_results['strategy1']['remaining_money']:.2f}")

                        print("\n策略2收益:")
                        if strategy_results["strategy2"]["sell_100"]["price"]:
                            print(f"100%卖出: {strategy_results['strategy2']['sell_100']['price']:.10f} ({strategy_results['strategy2']['sell_100']['time']}) 卖出: ${strategy_results['strategy2']['sell_100']['money']:.2f}")
                        if strategy_results["strategy2"]["sell_200"]["price"]:
                            print(f"200%卖出: {strategy_results['strategy2']['sell_200']['price']:.10f} ({strategy_results['strategy2']['sell_200']['time']}) 卖出: ${strategy_results['strategy2']['sell_200']['money']:.2f}")
                        if strategy_results["strategy2"]["sell_400"]["price"]:
                            print(f"400%卖出: {strategy_results['strategy2']['sell_400']['price']:.10f} ({strategy_results['strategy2']['sell_400']['time']}) 卖出: ${strategy_results['strategy2']['sell_400']['money']:.2f}")
                        if strategy_results["strategy2"]["sell_900"]["price"]:
                            print(f"900%卖出: {strategy_results['strategy2']['sell_900']['price']:.10f} ({strategy_results['strategy2']['sell_900']['time']}) 卖出: ${strategy_results['strategy2']['sell_900']['money']:.2f}")
                        if strategy_results["strategy2"]["stop_loss"]["price"]:
                            print(f"止损卖出: {strategy_results['strategy2']['stop_loss']['price']:.10f} ({strategy_results['strategy2']['stop_loss']['time']}) 卖出: ${strategy_results['strategy2']['stop_loss']['money']:.2f}")
                        elif strategy_results["strategy2"]["final_sell"]["price"]:
                            print(f"最终卖出: {strategy_results['strategy2']['final_sell']['price']:.10f} ({strategy_results['strategy2']['final_sell']['time']}) 卖出: ${strategy_results['strategy2']['final_sell']['money']:.2f}")
                        print(f"总收益: ${strategy_results['strategy2']['remaining_money']:.2f}")

            print("\n" + "=" * 50 + "\n")

        # 保存当前批次的数据
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        write_to_excel(time_windows, base_name)
        
        # 如果有失败的记录，保存到单独的文件
        if failed_records:
            failed_file = f"{file_path}.failed"
            with open(failed_file, 'w') as f:
                json.dump([rec[1] for rec in failed_records], f, indent=2)
            print(f"有 {len(failed_records)} 条记录处理失败，已保存到 {failed_file}")
            
        # 检查是否还有未处理的数据
        if end_index < data_call_len:
            print(f"\n当前批次处理完成，还有 {data_call_len - end_index} 条数据待处理")
            return True
        else:
            print(f"\n{os.path.basename(file_path)} 全部处理完成！")
            return True
            
    except Exception as e:
        print(f"处理 {os.path.basename(file_path)} 时发生错误: {str(e)}")
        return False

def init_worker():
    """初始化作进程"""
    pass

def process_single_record(record, token, state):
    """
    处理单条记录
    """
    retry_count = 0
    max_retries = 5
    
    while retry_count < max_retries:
        try:
            # 获取记录中的必要信息
            token_address = record.get("token_address")
            timestamp_call = record.get("timestamp_call")
            
            if not token_address or not timestamp_call:
                print("记录缺少必要信息")
                return False
                
            # 尝试获取图表数据
            chart_data = fetch_chart_data(token_address, token, timestamp_call, int(time.time()), state, record.get("current_index"), record.get("total_records"))
            
            if chart_data and any(chart_data.values()):
                state.reset_error_count()
                return True
                
            print(f"获取数据失败，等待3秒后进行第 {retry_count + 1} 次重试...")
            time.sleep(3)  # 固定等待3秒
            retry_count += 1
            
        except Exception as e:
            print(f"处理记录时出错 (重试 {retry_count+1}/{max_retries}): {str(e)}")
            print(f"等待3秒后进行第 {retry_count + 1} 次重试...")
            time.sleep(3)  # 固定等待3秒
            retry_count += 1
    
    # 如果达到最大重试次数，增加错误计数
    error_count = state.increment_error_count()
    if error_count >= 5:
        print(f"连续 {error_count} 次错误，token可能已失效")
        
    return False

def get_next_unprocessed_range(file_path):
    """获取下一个需要处理的数据范围"""
    with FILE_LOCK:
        with open(f"{file_path}.progress", 'r') as f:
            progress = json.load(f)
            
        # 优先处理未完成的区间
        if progress["unprocessed_ranges"]:
            range = progress["unprocessed_ranges"][0]
            return range["start"], range["end"]
            
        # 如果没有未完成区间，处理新数据
        last_processed = progress["last_processed"]
        if last_processed < progress["total_records"]:
            return last_processed + 1, progress["total_records"]
            
        return None, None  # 所有数据都处理完成

def update_progress(file_path, current_position, end_position, success):
    """更新处理进度"""
    with FILE_LOCK:
        try:
            with open(f"{file_path}.progress", 'r+') as f:
                progress = json.load(f)
        except:
            progress = {
                "total_records": total_records,
                "unprocessed_ranges": [],
                "last_processed": 0
            }
            
        if not success:
            # 添加未完成区间
            progress["unprocessed_ranges"].append({
                "start": current_position,
                "end": end_position
            })
        
        # 合并重叠区间
        progress["unprocessed_ranges"].sort(key=lambda x: x["start"])
        merged = []
        for range in progress["unprocessed_ranges"]:
            if not merged or merged[-1]["end"] < range["start"]:
                merged.append(range)
            else:
                merged[-1]["end"] = max(merged[-1]["end"], range["end"])
                
        progress["unprocessed_ranges"] = merged
        
        # 保存进度
        with open(f"{file_path}.progress", 'w') as f:
            json.dump(progress, f, indent=2)

def load_progress(file_path):
    """加载文件处理进度"""
    progress_file = f"{file_path}.progress"
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r') as f:
                return json.load(f)
        except:
            pass
    return {"last_processed": 0, "total_records": 0}

def get_next_pending_range(progress, total_length, batch_size=10):
    """获取下一个待处理的数据范围"""
    # 如果没有进度信息，从头开始
    if not progress or 'ranges' not in progress:
        progress['ranges'] = []
        return (0, min(batch_size, total_length))
    
    # 检查是否有未完成的范围
    for range_info in progress['ranges']:
        if range_info['status'] in ['pending', 'failed']:
            return (range_info['start'], range_info['end'])
    
    # 获取最后处理的位置
    last_end = 0
    if progress['ranges']:
        last_end = max(r['end'] for r in progress['ranges'])
    
    # 如果还有未处理的数据
    if last_end < total_length:
        return (last_end, min(last_end + batch_size, total_length))
    
    return None

def save_progress(file_path, start_idx, end_idx, status='success'):
    """保存处理进度"""
    with FILE_LOCK:
        progress = load_progress(file_path)
        if 'ranges' not in progress:
            progress['ranges'] = []
        
        # 更新或添加范围信息
        range_updated = False
        for range_info in progress['ranges']:
            if range_info['start'] == start_idx and range_info['end'] == end_idx:
                range_info['status'] = status
                range_updated = True
                break
        
        if not range_updated:
            progress['ranges'].append({
                'start': start_idx,
                'end': end_idx,
                'status': status
            })
        
        # 保存进度
        with open(f"{file_path}.progress", 'w') as f:
            json.dump(progress, f, indent=2)

if __name__ == "__main__":
    # 设置多进程启动方式
    multiprocessing.set_start_method('spawn', force=True)
    
    input_dir = os.path.join(os.path.dirname(JSON_NAME), "json待处理")
    
    # 确保输入目录存在
    if not os.path.exists(input_dir):
        print(f"输入目录不存在: {input_dir}")
        exit(1)
    
    timestamp_end = int(time.time())
    print(f"回测终止时间：{format_timestamp(timestamp_end)}")
    print("")
    
    # 创建token池
    token_pool = TokenPool()
    token_pool.input_tokens()  # 初始化时输入tokens
    
    # 创建并发处理器并开始处理
    processor = ConcurrentProcessor(input_dir, token_pool)
    processor.process(process_single_file)