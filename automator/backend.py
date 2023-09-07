import datetime
from web3 import Web3, exceptions
from functools import wraps

from .logger import log
from .utils import aws_put_metric_heart_beat


# def on_transaction(method):
#     @wraps(method)
#     def _impl(self, *method_args, **method_kwargs):
#         pending_tx_receipt = self.pending_transaction_receipt(method_kwargs['task'])
#         if 'receipt' in pending_tx_receipt:
#             if not pending_tx_receipt['receipt']['confirmed'] or pending_tx_receipt['receipt']['reverted']:
#                 # Continue on pending status or reverted
#                 return pending_tx_receipt
#
#         method_output = method(self, *method_args, **method_kwargs)
#         return method_output
#     return _impl


class BaseTransactionManager:

    def __init__(self,
                 options,
                 connection_helper,
                 contracts_loaded
                 ):
        self.options = options
        self.connection_helper = connection_helper
        self.contracts_loaded = contracts_loaded

    # def pending_queue_is_full(self, account_index=0):
    #
    #     web3 = self.connection_helper.connection_manager.web3
    #
    #     # get first account
    #     account_address = self.connection_helper.connection_manager.accounts[account_index].address
    #
    #     nonce = web3.eth.get_transaction_count(account_address, 'pending')
    #     last_used_nonce = web3.eth.get_transaction_count(account_address)
    #
    #     # A limit of pending on blockchain
    #     if nonce >= last_used_nonce + 1:
    #         log.info('Cannot create more transactions for {} as the node queue will be full [{}, {}]'.format(
    #             account_address, nonce, last_used_nonce))
    #         return True
    #
    #     return False
    #
    # @staticmethod
    # def save_pending_tx_receipt(tx_hash, task_name):
    #     """ Save to pending tx receipt """
    #
    #     result = dict()
    #     result['receipt'] = dict()
    #
    #     if tx_hash is None:
    #         result['receipt']['id'] = None
    #         result['receipt']['timestamp'] = None
    #         return result
    #
    #     result['receipt']['id'] = Web3.to_hex(tx_hash)
    #     result['receipt']['timestamp'] = datetime.datetime.now()
    #
    #     log.info("Task :: {0} :: Sending tx: {1}".format(task_name, Web3.to_hex(tx_hash)))
    #
    #     return result
    #
    # def pending_transaction_receipt(self, task):
    #     """ Wait to pending receipt get confirmed"""
    #
    #     web3 = self.connection_helper.connection_manager.web3
    #     timeout_reverted = 3600
    #
    #     result = dict()
    #     if task.tx_receipt:
    #         result['receipt'] = dict()
    #         result['receipt']['confirmed'] = False
    #         result['receipt']['reverted'] = False
    #
    #         try:
    #             tx = web3.eth.get_transaction(task.tx_receipt)
    #         except exceptions.TransactionNotFound:
    #             # Transaction not exist anymore, blockchain reorder?
    #             # timeout and permit to send again transaction
    #             result['receipt']['id'] = None
    #             result['receipt']['timestamp'] = None
    #             result['receipt']['confirmed'] = True
    #
    #             log.error("Task :: {0} :: Transaction not found! {1}".format(task.task_name, task.tx_receipt))
    #
    #             return result
    #
    #         try:
    #             tx_rcp = web3.eth.get_transaction_receipt(task.tx_receipt)
    #         except exceptions.TransactionNotFound:
    #             # TX Pending state
    #             elapsed = datetime.datetime.now() - task.tx_receipt_timestamp
    #             timeout = datetime.timedelta(seconds=task.timeout)
    #
    #             if elapsed > timeout:
    #                 # timeout allow to send again transaction
    #                 result['receipt']['id'] = None
    #                 result['receipt']['timestamp'] = None
    #                 result['receipt']['confirmed'] = True
    #
    #                 log.error("Task :: {0} :: Timeout tx! [{1}]".format(task.task_name, task.tx_receipt))
    #             else:
    #                 log.info("Task :: {0} :: Pending tx state ... [{1}]".format(task.task_name, task.tx_receipt))
    #
    #             return result
    #
    #         # confirmed state
    #         if tx_rcp['status'] > 0:
    #
    #             result['receipt']['id'] = None
    #             result['receipt']['timestamp'] = None
    #             result['receipt']['confirmed'] = True
    #
    #             log.info("Task :: {0} :: Confirmed tx! [{1}]".format(task.task_name, task.tx_receipt))
    #
    #         # reverted
    #         else:
    #
    #             result['receipt']['confirmed'] = True
    #             result['receipt']['reverted'] = True
    #
    #             elapsed = datetime.datetime.now() - task.tx_receipt_timestamp
    #             timeout = datetime.timedelta(seconds=timeout_reverted)
    #
    #             log.error("Task :: {0} :: Reverted tx! [{1}] Elapsed: [{2}] Timeout: [{3}]".format(
    #                 task.task_name, task.tx_receipt, elapsed.seconds, timeout_reverted))
    #
    #             if elapsed > timeout:
    #                 # timeout allow to send again transaction on reverted
    #                 result['receipt']['id'] = None
    #                 result['receipt']['timestamp'] = None
    #                 result['receipt']['confirmed'] = True
    #
    #                 log.error("Task :: {0} :: Timeout Reverted tx! [{1}]".format(task.task_name, task.tx_receipt))
    #
    #     return result


class Automator(BaseTransactionManager):
    @on_pending_transactions
    def calculate_ema(self, task=None, global_manager=None):

        if self.contracts_loaded["MoCState"].sc.functions.shouldCalculateEma().call():

            if self.pending_queue_is_full():
                log.error("Task :: {0} :: Pending queue is full".format(task.task_name))
                return

            tx_hash = self.contracts_loaded["MoCState"].calculate_moving_average()

            return self.save_pending_tx_receipt(tx_hash, task.task_name)

        else:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return self.save_pending_tx_receipt(None, task.task_name)

    @on_pending_transactions
    def daily_inrate_payment(self, task=None, global_manager=None):

        if self.contracts_loaded["MoC"].sc.functions.isDailyEnabled().call():

            if self.pending_queue_is_full():
                log.error("Task :: {0} :: Pending queue is full".format(task.task_name))
                return

            tx_hash = self.contracts_loaded["MoC"].daily_inrate_payment()

            return self.save_pending_tx_receipt(tx_hash, task.task_name)

        else:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return self.save_pending_tx_receipt(None, task.task_name)

    @on_pending_transactions
    def run_settlement(self, task=None, global_manager=None):

        partial_execution_steps = self.options['tasks']['run_settlement']['partial_execution_steps']

        if self.contracts_loaded["MoC"].sc.functions.isSettlementEnabled().call():

            if self.pending_queue_is_full():
                log.error("Task :: {0} :: Pending queue is full".format(task.task_name))
                return

            tx_hash = self.contracts_loaded["MoC"].run_settlement(partial_execution_steps)

            return self.save_pending_tx_receipt(tx_hash, task.task_name)

        else:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return self.save_pending_tx_receipt(None, task.task_name)

    @on_pending_transactions
    def contract_liquidation(self, task=None, global_manager=None):

        partial_execution_steps = self.options['tasks']['liquidation']['partial_execution_steps']

        if self.contracts_loaded["MoCState"].sc.functions.isLiquidationReached().call():

            if self.pending_queue_is_full():
                log.error("Task :: {0} :: Pending queue is full".format(task.task_name))
                return

            tx_hash = self.contracts_loaded["MoC"].eval_liquidation(partial_execution_steps)

            return self.save_pending_tx_receipt(tx_hash, task.task_name)
        else:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return self.save_pending_tx_receipt(None, task.task_name)

    @on_pending_transactions
    def pay_bitpro_holders(self, task=None, global_manager=None):

        app_mode = self.options['app_mode']

        if app_mode == 'MoC':
            is_bitpro_interest_enabled = self.contracts_loaded["MoC"].sc.functions.isBitProInterestEnabled().call()
        else:
            is_bitpro_interest_enabled = self.contracts_loaded["MoC"].sc.functions.isRiskProInterestEnabled().call()

        if is_bitpro_interest_enabled:

            if self.pending_queue_is_full():
                log.error("Task :: {0} :: Pending queue is full".format(task.task_name))
                return

            tx_hash = self.contracts_loaded["MoC"].pay_bitpro_holders_interest_payment()

            return self.save_pending_tx_receipt(tx_hash, task.task_name)

        else:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return self.save_pending_tx_receipt(None, task.task_name)

    @on_pending_transactions
    def oracle_poke(self, task=None, global_manager=None):

        price_validity = self.contracts_loaded["PriceProvider"].sc.functions.peek().call()[1]
        if not self.contracts_loaded["PriceProvider"].sc.functions.compute().call()[1] and price_validity:

            if self.pending_queue_is_full():
                log.error("Task :: {0} :: Pending queue is full".format(task.task_name))
                return

            tx_hash = self.contracts_loaded["PriceProvider"].poke()
            log.error("Task :: {0} :: Not valid price! Disabling Price!".format(task.task_name))
            aws_put_metric_heart_beat(1)

            return self.save_pending_tx_receipt(tx_hash, task.task_name)

        # if no valid price in oracle please send alarm
        if not price_validity:
            log.error("Task :: {0} :: No valid price in oracle!".format(task.task_name))
            aws_put_metric_heart_beat(1)

        log.info("Task :: {0} :: No!".format(task.task_name))
        return self.save_pending_tx_receipt(None, task.task_name)

    @on_pending_transactions
    def splitter_split(self, task=None, global_manager=None):

        if 'pay_bitpro_holders_confirm_block' not in global_manager:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return

        pay_bitpro_holders_confirm_block = global_manager['pay_bitpro_holders_confirm_block']
        if pay_bitpro_holders_confirm_block <= 0:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return

        if 'commission_splitter_confirm_block' in global_manager:
            commission_splitter_confirm_block = global_manager['commission_splitter_confirm_block']
        else:
            commission_splitter_confirm_block = 0

        if pay_bitpro_holders_confirm_block > commission_splitter_confirm_block:
            pass
        else:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return

        if self.pending_queue_is_full():
            log.error("Task :: {0} :: Pending queue is full".format(task.task_name))
            return

        tx_hash = self.contracts_loaded["CommissionSplitter"].split()

        global_manager['commission_splitter_confirm_block'] = self.connection_helper.connection_manager.block_number

        log.info("Commission Splitter V2 - Execute successfully!")

        return self.save_pending_tx_receipt(tx_hash, task.task_name)

    @on_pending_transactions
    def splitter_split_v3(self, task=None, global_manager=None):

        if 'pay_bitpro_holders_confirm_block' not in global_manager:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return

        pay_bitpro_holders_confirm_block = global_manager['pay_bitpro_holders_confirm_block']
        if pay_bitpro_holders_confirm_block <= 0:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return

        if 'commission_splitter_confirm_block_v3' in global_manager:
            commission_splitter_confirm_block = global_manager['commission_splitter_confirm_block_v3']
        else:
            commission_splitter_confirm_block = 0

        if pay_bitpro_holders_confirm_block > commission_splitter_confirm_block:
            pass
        else:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return

        if self.pending_queue_is_full():
            log.error("Task :: {0} :: Pending queue is full".format(task.task_name))
            return

        tx_hash = self.contracts_loaded["CommissionSplitterV3"].split()

        global_manager['commission_splitter_confirm_block_v3'] = self.connection_helper.connection_manager.block_number

        log.info("Commission Splitter V3 - Execute successfully!")

        return self.save_pending_tx_receipt(tx_hash, task.task_name)
