import json
import time
import threading
from web3 import Web3
from eth_account import Account
from decimal import Decimal
from queue import Queue
import random

# ==================== 用户配置区域 ====================
CONFIG = {
    "rpc_url": "rpc",
    "chain_id": 1,
    "privkey_file": "txt",         # 私钥文本文件路径
    "to_address": "------",        # 接收地址
    "token_type": "native",        # native/ERC20
    "erc20_contract": "------",    # ERC20合约地址
    "erc20_decimals": 18,          # 代币精度
    "amount": 0,                   # 转账数量
    "max_retries": 3,              # 失败重试次数
    "eip1559": True,               # eip-1559 开关
    "gas_limit": 100000,           # 建议调高Gas Limit
    "max_fee_gwei": 65,            # max_fee
    "priority_fee_gwei": 2,        # priority_fee
    "thread_count": 5,             # 并发线程数
    "tx_timeout": 300,             # 交易等待超时(秒)
    "min_interval": 0.5,           # 最小间隔时间(秒)
    "max_interval": 0.5,           # 最大间隔时间(秒)
    "shuffle_accounts": True,      # 随机排序开关
    "custom_data_template": "------"  # DATA
}
# =====================================================

w3 = Web3(Web3.HTTPProvider(CONFIG['rpc_url']))
lock = threading.Lock()

class KeyLoader:
    @staticmethod
    def load_privkeys():
        """从文本文件加载私钥（保持单线程读取）"""
        try:
            with open(CONFIG['privkey_file'], 'r') as f:
                return [
                    line.strip() 
                    for line in f.readlines() 
                    if line.strip() != ''
                ]
        except FileNotFoundError:
            raise Exception(f"私钥文件 {CONFIG['privkey_file']} 不存在")

class ERC20Helper:
    @staticmethod
    def get_balance(address):
        """查询ERC20余额"""
        abi = [{
            "constant":True,
            "inputs":[{"name":"_owner","type":"address"}],
            "name":"balanceOf",
            "outputs":[{"name":"balance","type":"uint256"}],
            "type":"function"
        }]
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(CONFIG['erc20_contract']),
            abi=abi
        )
        balance = contract.functions.balanceOf(address).call()
        return balance / 10**CONFIG['erc20_decimals']

class TransactionTracker:
    def __init__(self, tx_hash):
        self.tx_hash = tx_hash
        self.status = "pending"
        
    def track(self):
        """追踪交易状态"""
        start_time = time.time()
        while time.time() - start_time < CONFIG['tx_timeout']:
            try:
                receipt = w3.eth.get_transaction_receipt(self.tx_hash)
                if receipt.status == 1:
                    self.status = f"confirmed (block #{receipt.blockNumber})"
                    return True
                else:
                    self.status = "failed (reverted)"
                    return False
            except:
                time.sleep(5)
        self.status = "timeout"
        return False

class BatchSender:
    def __init__(self):
        self.accounts = self._load_accounts()
    
    def _load_accounts(self):
        """多线程加载并初始化账户"""
        privkeys = KeyLoader.load_privkeys()
        account_queue = Queue()
        threads = []
        
        for _ in range(CONFIG['thread_count']):
            t = threading.Thread(
                target=self._process_privkeys,
                args=(privkeys, account_queue)
            )
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()
        
        account_list = []
        while not account_queue.empty():
            account_list.append(account_queue.get())
        if CONFIG.get('shuffle_accounts', True):
            random.shuffle(account_list)        
        return account_list
    
    def _process_privkeys(self, privkeys, queue):
        """多线程处理私钥的核心逻辑"""
        while True:
            try:
                with lock:
                    if not privkeys:
                        break
                    pk = privkeys.pop()
            except IndexError:
                break
            
            try:
                acc = Account.from_key(pk)
                balance = self._get_balance(acc.address)
                nonce = w3.eth.get_transaction_count(acc.address)
                
                with lock:
                    queue.put({
                        'address': acc.address,
                        'pk': pk,
                        'nonce': nonce,
                        'balance': balance
                    })
                    print(f"✅ 成功加载地址: {acc.address}")
            except Exception as e:
                with lock:
                    print(f"❌ 无效私钥: {str(e)[:50]}")
    
    def _get_balance(self, address):
        """获取余额（支持原生币/ERC20）"""
        if CONFIG['token_type'] == 'native':
            return w3.from_wei(w3.eth.get_balance(address), 'ether')
        else:
            return ERC20Helper.get_balance(address)
    
    def _build_custom_data(self, account):
        """直接替换40字符地址，禁止补零"""
        raw_address = Web3.to_checksum_address(account['address'])[2:].lower()
        return CONFIG['custom_data_template'].replace("{address}", raw_address)
    
    def _build_tx(self, account):
        """构建交易参数"""
        tx = {
            'chainId': CONFIG['chain_id'],
            'nonce': account['nonce'],
            'to': Web3.to_checksum_address(CONFIG['to_address'])
        }
        
        if CONFIG['token_type'] == 'native':
            tx['value'] = w3.to_wei(CONFIG['amount'], 'ether')
            tx['data'] = self._build_custom_data(account)
        else:
            tx.update({
                'to': CONFIG['erc20_contract'],
                'data': self._build_erc20_data(account['address'])
            })
        
        if CONFIG['eip1559']:
            tx.update({
                'maxFeePerGas': w3.to_wei(CONFIG['max_fee_gwei'], 'gwei'),
                'maxPriorityFeePerGas': w3.to_wei(CONFIG['priority_fee_gwei'], 'gwei')
            })
        else:
            tx['gasPrice'] = w3.to_wei(CONFIG['max_fee_gwei'], 'gwei')
        
        tx['gas'] = CONFIG['gas_limit'] or w3.eth.estimate_gas(tx)
        return tx
    
    def _build_erc20_data(self, from_address):
        """构建ERC20转账data"""
        func_hash = w3.keccak(text='transfer(address,uint256)')[:4].hex()
        to_address = Web3.to_checksum_address(CONFIG['to_address'])[2:].zfill(64)
        amount = int(CONFIG['amount'] * 10**CONFIG['erc20_decimals'])
        return func_hash + to_address + hex(amount)[2:].zfill(64)
    
    def _send_thread(self, account):
        """多线程发送核心逻辑"""
        for attempt in range(CONFIG['max_retries']):
            try:
                tx = self._build_tx(account)
                signed = Account.sign_transaction(tx, account['pk'])
                tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
                
                tracker = TransactionTracker(tx_hash)
                success = tracker.track()
                
                with lock:
                    account['nonce'] += 1
                    print(f"[{account['address'][:6]}]", 
                          f"Tx: {tx_hash.hex()[:10]}...",
                          f"Status: {tracker.status}")
                return success
                
            except Exception as e:
                print(f"Error: {str(e)[:50]}")
                time.sleep(5)
        return False
    
    def run(self):
        """启动多线程发送"""
        threads = []
        print(f"\n=== 账户余额概览 ===")
        for acc in self.accounts:
            print(f"{acc['address']} | 余额: {acc['balance']} {'ETH' if CONFIG['token_type'] == 'native' else 'TOKEN'}")
    
        input("\n按回车开始发送交易...")
    
        for acc in self.accounts:
            t = threading.Thread(target=self._send_thread, args=(acc,))
            threads.append(t)
            t.start()
            try:
                delay = random.uniform(CONFIG['min_interval'], CONFIG['max_interval'])
                time.sleep(delay)
            except KeyError:
                 time.sleep(0.5)
            
        for t in threads:
            if t.is_alive():
                t.join()

if __name__ == '__main__':
    assert w3.is_connected(), "RPC连接失败"
    sender = BatchSender()
    sender.run()
    print("\n所有交易处理完成")
