import requests
import json
import time
from urllib.parse import urlencode


RETRY_SLEEP_TIME = 4

class BullxAPIClient:
    def __init__(self, api_key, refresh_token, token_url, base_url:str = 'https://api-edge.bullx.io'):
        
        self.api_key = api_key
        self.refresh_token = refresh_token
        self.token_url = token_url  #  获取 access_token 的 URL
        self.base_url = base_url  #  API
        self.access_token = None
        self.token_expiry_time = 0  # 初始化为 0，表示令牌未获取或已过期
        self.chainid = "1399811149"
        self.quoteToken='So11111111111111111111111111111111111111112'

    def _refresh_access_token(self):
        """
        刷新 access_token 并更新过期时间。
        """
        headers = {"content-type": "application/x-www-form-urlencoded"}
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
        }
        try:
            response = requests.post(
                self.token_url, data=urlencode(payload), headers=headers
            )
            response.raise_for_status()
            data = response.json()
            print(data)
            self.access_token = data.get("access_token")
            # 记录过期时间（提前 60 秒刷新，避免网络延迟导致的超时）
            expires_in = data.get("expires_in", 3600)
            if isinstance(expires_in, str):
                expires_in = int(expires_in)
            self.token_expiry_time = time.time() + expires_in - 60
            print("Access token refreshed successfully.")
        except requests.exceptions.RequestException as e:
            print(f"Failed to refresh access token: {e}")
            raise

    def _get_valid_access_token(self):
        """
        检查 access_token 是否有效，必要时刷新。
        """
        if self.access_token is None or time.time() >= self.token_expiry_time:
            print("Access token expired or not available. Refreshing...")
            self._refresh_access_token()
        return self.access_token

    def _make_request(self, method="GET", data=None, ednpoint="/api"):
        """
        通用的请求方法，会确保在发送请求前有有效的 access_token。
        """
        token = self._get_valid_access_token()
        headers = {
            "authorization": f"Bearer {token}",
            "content-type": "text/plain",
            "Referer": "https://bullx.io/",
            "Referrer-Policy": "strict-origin-when-cross-origin",
        }
        url = f"{self.base_url}" + ednpoint

        try:
            if method.upper() == "POST":
                response = requests.post(url, headers=headers, data=json.dumps(data))
            elif method.upper() == "GET":
                response = requests.get(url, headers=headers, params=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return None

    def get_chart_data(
        self,
        token_address: str,
        interval_secs: int,
        start_time: int = None,
        end_time: int = None,
        count_back: int = None,
    ) -> dict | None:
        """
        实际测试 endtime starttime countback 三个参数都是没用的🤣🤣🤣
        Args:
            token_address (str): 要查询的代币的地址。
            start_time (int): 查询的起始时间戳 (unix timestamp)。
            end_time (int): 查询的结束时间戳 (unix timestamp)。
            interval_secs (int):  图表数据的时间间隔，单位为秒。
            count_back (int):  返回的数据点数量，如果提供则忽略 start_time 和 end_time。
        Returns:
           dict | None: 如果请求成功，则返回包含图表数据的字典，否则返回 None.
           返回的字典结构如下:
           {
              "t": list[int], // 时间戳列表 (unix timestamp)
              "o": list[float], // 开盘价列表
              "h": list[float], // 最高价列表
              "l": list[float], // 最低价列表
              "c": list[float], // 收盘价列表
              "v": list[float], // 交易量列表
              "debugData": {  // 调试信息
                "poolAddress": str //  流动池地址
              }
            }
        """
        payload = {
            "name": "chart",
            "data": {
                "base": token_address,
                "chainId": self.chainid,
                "countBack": count_back,
                "from": start_time,
                "intervalSecs": interval_secs,
                "quote": "So11111111111111111111111111111111111111112",
                "to": end_time,
            },
        }
        return self._make_request(method="POST", data=payload, ednpoint="/chart")

    def get_token_technical_data(self, token_address: str) -> dict | None:
        """
                获取 dev信息 内部钱包地址 狙击手 等等
        {
            "statusCode": 200, // int, HTTP 状态码
            "message": null,   // str | null, 消息
            "data": {          // dict, 数据容器
                "tokenStats": [ // list, 代币统计信息列表
                    {
                        "id": "...",       // str, 代币统计ID
                        "chainId": 1399811149, // int, 链 ID
                        "address": "...",   // str, 代币合约地址
                        "syncedAt": "...",  // str, 数据同步时间 (ISO 8601)
                        "devDeployedTokens": [ // list, 开发者部署的代币列表],
                        "isDevRug": null,  // boolean | null, 是否开发者跑路项目
                        "tagsCount": {     // dict, 标签计数
                            "BullX": 1,    // int, BullX 标签计数
                            "Trojan": 11   // int, Trojan 标签计数
                        },
                        "sniperWallets": [  // list, 狙击手钱包地址列表
                            "...",
                           // ...
                        ],
                         "insiderWallets": [ // list, 内部人员钱包地址列表
                            "...",
                           // ...
                        ]
                    }
                ]
            }
        }
        """
        payload = {
            "name": "getTechnicals",
            "data": {
                "tokenAddress": "EXVUTukPd4W5EhLkWju67wo5kZ6UiVHdaUXXQHPwpump",
                "chainId": self.chainid,
            },
        }
        return self._make_request(method="POST", data=payload)

    def get_holders_summary(
        self, token_address: str, sort_by: str = "balance"
    ) -> dict | None:
        """
        获取代币的持有者数据汇总。

        Args:
            token_address (str): 要查询的代币的地址。
            sort_by (str, optional): 排序方式，balance or amount. Defaults to 'balance'.

        Returns:
            dict | None: {
                "chainId": int,
                "tokenAddress": str,
                "address": str,
                "currentlyHoldingAmount": str,
                "holderSince": int,
                "totalBoughtAmount": int,
                "totalBoughtETH": float,
                "totalBuyTransactions": int,
                "totalSellTransactions": int,
                "totalSoldAmount": int,
                "totalBoughtUSD": float,
                "totalSoldUSD": float,
                "totalSoldETH": float,
                "sent": str,
                "received": int,
                "tags": list[str]
            } | None
        """
        payload = {
            "name": "holdersSummary",
            "data": {
                "tokenAddress": token_address,
                "sortBy": sort_by,
                "chainId": 1399811149,
            },
        }
        return self._make_request(method="POST", data=payload)

    def get_native_balances(
        self, wallet_addresses: list[str], chain_ids: list[int]
    ) -> dict | None:
        """
        获取指定钱包地址的原生代币余额。

        Args:
            wallet_addresses (list[str]): 要查询的钱包地址列表。
            chain_ids (list[int]): 要查询的链 ID 列表。
        Returns:
           dict | None: 如果请求成功，则返回包含余额数据的字典，否则返回 None。
           返回的字典结构如下:
           {
                "statusCode": int,
                "message": str | None,
                  "data": {
                     "balances": list[dict]
                  }
            }
        """
        payload = {
            "name": "nativeBalances",
            "data": {
                "walletAddresses": wallet_addresses,
                "chainIds": chain_ids,
            },
        }
        return self._make_request(method="POST", data=payload)

    def get_token_security(self, token_addresses: list[str], chain_id: int = 1399811149) -> dict | None:
        """
        获取指定代币的安全信息。
        Args:
            token_addresses (list[str]): 要查询的代币地址列表。
            chain_id (int): 要查询的链 ID。

        Returns:
           dict | None: 如果请求成功，则返回包含安全数据的字典，否则返回 None。
           返回的字典结构如下:
           {
                "statusCode": int,  // HTTP 状态码，例如 200 表示成功
                "message": str | None,  // 消息，通常为 null 如果请求成功
                  "data": {
                     "security": list[dict] // 安全信息列表
                  }
            }
        """
        payload = {
            "name": "tokenSecurity",
            "data": {
                "addresses": token_addresses,
                "chainId": chain_id,
            },
        }
        return self._make_request(method="POST", data=payload)
   
    def resolve_tokens(self, token_addresses: list[str], chain_id: int = 1399811149) -> dict | None:
        """
        输出超级超级详细的池子信息
        """
        payload = {
            "name": "resolveTokens",
            "data": {
                "addresses": token_addresses,
                "chainId": chain_id,
            },
        }
        return self._make_request(method="POST", data=payload)

    def get_token_stats(self, token_address: str, chain_id: int = 1399811149) -> dict | None:
        """
        获取指定代币的统计数据 V3。

        Args:
            base_token_address (str): 基础代币的地址。
            quote_token_address (str): 报价代币的地址。
            chain_id (int): 要查询的链 ID。
        """
        payload = {
            "name": "getTokenStatsV3",
            "data": {
                "baseToken": token_address,
                "quoteToken": self.quoteToken,
                "chainId": chain_id,
            },
        }
        return self._make_request(method="POST", data=payload)

    def get_approval_status(self, token_address: str, wallet_addresses: list[str], chain_id: int = 1399811149, protocol: str = None) -> dict | None:
        """
        获取指定代币的授权状态。
        """
        payload = {
            "name": "getApprovalStatus",
            "data": {
                "chainId": chain_id,
                "tokenAddress": token_address,
                "walletAddresses": wallet_addresses,
                "protocol": protocol,
            },
        }
        return self._make_request(method="POST", data=payload)

    def get_trade_history(self, token_address: str, fetch_tokens_metadata: bool = False) -> dict | None:
        """
        获取指定交易对的交易历史。
        """
        payload = {
            "name": "tradeHistory",
            "data": {
                "baseTokenAddress": token_address,
                "quoteTokenAddress": self.quoteToken,
                "chainId": chain_id,
                "fetchTokensMetadata": fetch_tokens_metadata
            },
        }
        return self._make_request(method="POST", data=payload)
    
    def get_wallets_summary(self, wallet_addresses: list[str], fetch_native_balances: bool = False, fetch_tokens_metadata: bool = True, fetch_wallet_balances: bool = True) -> dict | None:
        """
        获取指定钱包地址的摘要信息 V2。
        Args:
            wallet_addresses (list[str]): 要查询的钱包地址列表。
            fetch_native_balances (bool, optional): 是否获取原生代币余额，默认为 False。
            fetch_tokens_metadata (bool, optional): 是否获取代币元数据，默认为 True。
            fetch_wallet_balances (bool, optional): 是否获取钱包余额，默认为 True。
        """
        payload = {
            "name": "walletsSummaryV2",
            "data": {
                "walletAddresses": wallet_addresses,
                "fetchNativeBalances": fetch_native_balances,
                "fetchTokensMetadata": fetch_tokens_metadata,
                "fetchWalletBalances": fetch_wallet_balances
            },
        }
        return self._make_request(method="POST", data=payload)
    @staticmethod
    def get_ticker_from_pump(token_address: str) -> str:
        """
        从pump.fun获取代币符号的函数
        
        依赖:
        - requests: HTTP请求库
        - API_SLEEP_TIME: 请求间隔时间常量
        
        功能:
        1. 调用pump.fun的API获取代币信息
        2. 自动重试3次
        3. 请求间隔控制
        
        Args:
            address (str): 代币合约地址
        
        Returns:
            str: 代币符号，获取失败返回空字符串
        """
        for _ in range(0, 3):
            try:
                url = f"https://frontend-api.pump.fun/coins/{token_address}"
                response = requests.get(url)
                response.raise_for_status()
                return response.json()["symbol"]
            except:
                time.sleep(RETRY_SLEEP_TIME)  # 使用重试等待时间
        return ""

    @staticmethod
    def get_ticker_from_dexscreener(token_address: str) -> str:
        """
        从dexscreener获取代币符号的函数
        
        依赖:
        - requests: HTTP请求库
        - API_SLEEP_TIME: 请求间隔时间常量
        
        功能:
        1. 调用dexscreener的API获取代币信息
        2. 自动重试3次
        3. 请求间隔控制
        
        Args:
            address (str): 代币合约地址
        
        Returns:
            str: 代币符号，获取失败返回空字符串
        """
        for _ in range(0, 3):
            try:
                url = f"https://api.dexscreener.com/latest/dex/search?q={token_address}"
                response = requests.get(url)
                response.raise_for_status()
                return response.json()["pairs"][0]["baseToken"]["symbol"]
            except:
                time.sleep(RETRY_SLEEP_TIME)  # 使用重试等待时间
        return ""

    @staticmethod
    def get_ticker(token_address: str) -> str:  
        """
        获取代币符号的函数
        
        依赖:
        - get_ticker_from_pump: pump.fun API获取函数
        - get_ticker_from_dexscreener: dexscreener API获取函数
        
        功能:
        1. 优先从pump.fun获取代币符号
        2. 失效后从dexscreener获取
        3. 多源容错
        
        Args:
            token_address (str): 代币合约地址
        
        Returns:
            str: 代币符号，获取失败返回空字符串
        """
        ticker = BullxAPIClient.get_ticker_from_pump(token_address)
        if ticker != "":
            return ticker

        ticker = BullxAPIClient.get_ticker_from_dexscreener(token_address)
        if ticker != "":
            return ticker

        return ""



if __name__ == "__main__":
    
    API_KEY = ""
    REFRESH_TOKEN = ""
    TOKEN_URL = f"https://securetoken.googleapis.com/v1/token?key={API_KEY}"

    client = BullxAPIClient(API_KEY, REFRESH_TOKEN, TOKEN_URL)

    chain_id = 1399811149
    base = "ARmMherUjirQ4LuTU9RQHwrtNiX1abVgTbsocHHMpump"
    dataa = client.get_token_technical_data(
        token_address='ARmMherUjirQ4LuTU9RQHwrtNiX1abVgTbsocHHMpump'
    )
    ans = json.dumps(dataa, indent=4)
    print(ans)
