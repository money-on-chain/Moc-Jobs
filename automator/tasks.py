import decimal
from web3 import Web3
import datetime

from .contracts import Multicall2, \
    MoC, \
    MoCConnector, \
    MoCState, \
    CommissionSplitter,\
    MoCMedianizer,\
    MoCRRC20, \
    MoCConnectorRRC20, \
    MoCStateRRC20,\
    CommissionSplitterRRC20,\
    MoCMedianizerRRC20,\
    MoCInrate,\
    MoCInrateRRC20

from .base.main import ConnectionHelperBase
from .tasks_manager import PendingTransactionsTasksManager, on_pending_transactions
from .logger import log
from .utils import aws_put_metric_heart_beat


__VERSION__ = '3.0.0'


log.info("Starting Stable Protocol Automator version {0}".format(__VERSION__))


class Automator(PendingTransactionsTasksManager):

    def __init__(self,
                 config,
                 connection_helper,
                 contracts_loaded
                 ):
        self.config = config
        self.connection_helper = connection_helper
        self.contracts_loaded = contracts_loaded

        # init PendingTransactionsTasksManager
        super().__init__(self.config,
                         self.connection_helper,
                         self.contracts_loaded)

    @on_pending_transactions
    def calculate_ema(self, task=None, global_manager=None, task_result=None):

        if self.contracts_loaded["MoCState"].sc.functions.shouldCalculateEma().call():

            # return if there are pending transactions
            if task_result.get('pending_transactions', None):
                return task_result

            web3 = self.connection_helper.connection_manager.web3

            nonce = web3.eth.get_transaction_count(
                self.connection_helper.connection_manager.accounts[0].address, "pending")

            # get gas price from node
            node_gas_price = decimal.Decimal(Web3.from_wei(web3.eth.gas_price, 'ether'))

            # Multiply factor of the using gas price
            calculated_gas_price = node_gas_price * decimal.Decimal(self.config['gas_price_multiply_factor'])

            try:
                tx_hash = self.contracts_loaded["MoCState"].calculate_moving_average(
                    gas_limit=self.config['tasks']['calculate_bma']['gas_limit'],
                    gas_price=int(calculated_gas_price * 10 ** 18),
                    nonce=nonce
                )
            except ValueError as err:
                log.error("Task :: {0} :: Error sending transaction! \n {1}".format(task.task_name, err))
                return task_result

            if tx_hash:
                new_tx = dict()
                new_tx['hash'] = tx_hash
                new_tx['timestamp'] = datetime.datetime.now()
                new_tx['gas_price'] = calculated_gas_price
                new_tx['nonce'] = nonce
                new_tx['timeout'] = self.config['tasks']['calculate_bma']['wait_timeout']
                task_result['pending_transactions'].append(new_tx)

                log.info("Task :: {0} :: Sending TX :: Hash: [{1}] Nonce: [{2}] Gas Price: [{3}]".format(
                    task.task_name, Web3.to_hex(new_tx['hash']), new_tx['nonce'], int(calculated_gas_price * 10 ** 18)))

        else:
            log.info("Task :: {0} :: No!".format(task.task_name))

        return task_result

    @on_pending_transactions
    def daily_inrate_payment(self, task=None, global_manager=None, task_result=None):

        if self.contracts_loaded["MoC"].sc.functions.isDailyEnabled().call():

            # return if there are pending transactions
            if task_result.get('pending_transactions', None):
                return task_result

            web3 = self.connection_helper.connection_manager.web3

            nonce = web3.eth.get_transaction_count(
                self.connection_helper.connection_manager.accounts[0].address, "pending")

            # get gas price from node
            node_gas_price = decimal.Decimal(Web3.from_wei(web3.eth.gas_price, 'ether'))

            # Multiply factor of the using gas price
            calculated_gas_price = node_gas_price * decimal.Decimal(self.config['gas_price_multiply_factor'])

            try:
                tx_hash = self.contracts_loaded["MoC"].daily_inrate_payment(
                    gas_limit=self.config['tasks']['daily_inrate_payment']['gas_limit'],
                    gas_price=int(calculated_gas_price * 10 ** 18),
                    nonce=nonce
                )
            except ValueError as err:
                log.error("Task :: {0} :: Error sending transaction! \n {1}".format(task.task_name, err))
                return task_result

            if tx_hash:
                new_tx = dict()
                new_tx['hash'] = tx_hash
                new_tx['timestamp'] = datetime.datetime.now()
                new_tx['gas_price'] = calculated_gas_price
                new_tx['nonce'] = nonce
                new_tx['timeout'] = self.config['tasks']['daily_inrate_payment']['wait_timeout']
                task_result['pending_transactions'].append(new_tx)

                log.info("Task :: {0} :: Sending TX :: Hash: [{1}] Nonce: [{2}] Gas Price: [{3}]".format(
                    task.task_name, Web3.to_hex(new_tx['hash']), new_tx['nonce'], int(calculated_gas_price * 10 ** 18)))

        else:
            log.info("Task :: {0} :: No!".format(task.task_name))

        return task_result

    @on_pending_transactions
    def run_settlement(self, task=None, global_manager=None, task_result=None):

        partial_execution_steps = self.config['tasks']['run_settlement']['partial_execution_steps']

        if self.contracts_loaded["MoC"].sc.functions.isSettlementEnabled().call():

            # return if there are pending transactions
            if task_result.get('pending_transactions', None):
                return task_result

            web3 = self.connection_helper.connection_manager.web3

            nonce = web3.eth.get_transaction_count(
                self.connection_helper.connection_manager.accounts[0].address, "pending")

            # get gas price from node
            node_gas_price = decimal.Decimal(Web3.from_wei(web3.eth.gas_price, 'ether'))

            # Multiply factor of the using gas price
            calculated_gas_price = node_gas_price * decimal.Decimal(self.config['gas_price_multiply_factor'])

            try:
                tx_hash = self.contracts_loaded["MoC"].run_settlement(
                    partial_execution_steps,
                    gas_limit=self.config['tasks']['run_settlement']['gas_limit'],
                    gas_price=int(calculated_gas_price * 10 ** 18),
                    nonce=nonce
                )
            except ValueError as err:
                log.error("Task :: {0} :: Error sending transaction! \n {1}".format(task.task_name, err))
                return task_result

            if tx_hash:
                new_tx = dict()
                new_tx['hash'] = tx_hash
                new_tx['timestamp'] = datetime.datetime.now()
                new_tx['gas_price'] = calculated_gas_price
                new_tx['nonce'] = nonce
                new_tx['timeout'] = self.config['tasks']['run_settlement']['wait_timeout']
                task_result['pending_transactions'].append(new_tx)

                log.info("Task :: {0} :: Sending TX :: Hash: [{1}] Nonce: [{2}] Gas Price: [{3}]".format(
                    task.task_name, Web3.to_hex(new_tx['hash']), new_tx['nonce'], int(calculated_gas_price * 10 ** 18)))

        else:
            log.info("Task :: {0} :: No!".format(task.task_name))

        return task_result

    @on_pending_transactions
    def contract_liquidation(self, task=None, global_manager=None, task_result=None):

        partial_execution_steps = self.config['tasks']['liquidation']['partial_execution_steps']

        if self.contracts_loaded["MoCState"].sc.functions.isLiquidationReached().call():

            # return if there are pending transactions
            if task_result.get('pending_transactions', None):
                return task_result

            web3 = self.connection_helper.connection_manager.web3

            nonce = web3.eth.get_transaction_count(
                self.connection_helper.connection_manager.accounts[0].address, "pending")

            # get gas price from node
            node_gas_price = decimal.Decimal(Web3.from_wei(web3.eth.gas_price, 'ether'))

            # Multiply factor of the using gas price
            calculated_gas_price = node_gas_price * decimal.Decimal(self.config['gas_price_multiply_factor'])

            try:
                tx_hash = self.contracts_loaded["MoC"].eval_liquidation(
                    partial_execution_steps,
                    gas_limit=self.config['tasks']['liquidation']['gas_limit'],
                    gas_price=int(calculated_gas_price * 10 ** 18),
                    nonce=nonce
                )
            except ValueError as err:
                log.error("Task :: {0} :: Error sending transaction! \n {1}".format(task.task_name, err))
                return task_result

            if tx_hash:
                new_tx = dict()
                new_tx['hash'] = tx_hash
                new_tx['timestamp'] = datetime.datetime.now()
                new_tx['gas_price'] = calculated_gas_price
                new_tx['nonce'] = nonce
                new_tx['timeout'] = self.config['tasks']['liquidation']['wait_timeout']
                task_result['pending_transactions'].append(new_tx)

                log.info("Task :: {0} :: Sending TX :: Hash: [{1}] Nonce: [{2}] Gas Price: [{3}]".format(
                    task.task_name, Web3.to_hex(new_tx['hash']), new_tx['nonce'], int(calculated_gas_price * 10 ** 18)))

        else:
            log.info("Task :: {0} :: No!".format(task.task_name))

        return task_result

    @on_pending_transactions
    def pay_bitpro_holders(self, task=None, global_manager=None, task_result=None):

        app_mode = self.config['app_mode']

        if app_mode == 'MoC':
            is_bitpro_interest_enabled = self.contracts_loaded["MoC"].sc.functions.isBitProInterestEnabled().call()
        else:
            is_bitpro_interest_enabled = self.contracts_loaded["MoC"].sc.functions.isRiskProInterestEnabled().call()

        # write global on confirmed transaction
        if task_result.get('confirmed_txs', None):
            global_manager['pay_bitpro_holders_confirm_block'] = task_result['confirmed_txs'][0][1]

        if is_bitpro_interest_enabled:

            # return if there are pending transactions
            if task_result.get('pending_transactions', None):
                return task_result

            web3 = self.connection_helper.connection_manager.web3

            nonce = web3.eth.get_transaction_count(
                self.connection_helper.connection_manager.accounts[0].address, "pending")

            # get gas price from node
            node_gas_price = decimal.Decimal(Web3.from_wei(web3.eth.gas_price, 'ether'))

            # Multiply factor of the using gas price
            calculated_gas_price = node_gas_price * decimal.Decimal(self.config['gas_price_multiply_factor'])

            try:
                tx_hash = self.contracts_loaded["MoC"].pay_bitpro_holders_interest_payment(
                    gas_limit=self.config['tasks']['pay_bitpro_holders']['gas_limit'],
                    gas_price=int(calculated_gas_price * 10 ** 18),
                    nonce=nonce
                )
            except ValueError as err:
                log.error("Task :: {0} :: Error sending transaction! \n {1}".format(task.task_name, err))
                return task_result

            if tx_hash:
                new_tx = dict()
                new_tx['hash'] = tx_hash
                new_tx['timestamp'] = datetime.datetime.now()
                new_tx['gas_price'] = calculated_gas_price
                new_tx['nonce'] = nonce
                new_tx['timeout'] = self.config['tasks']['pay_bitpro_holders']['wait_timeout']
                task_result['pending_transactions'].append(new_tx)

                log.info("Task :: {0} :: Sending TX :: Hash: [{1}] Nonce: [{2}] Gas Price: [{3}]".format(
                    task.task_name, Web3.to_hex(new_tx['hash']), new_tx['nonce'], int(calculated_gas_price * 10 ** 18)))

        else:
            log.info("Task :: {0} :: No!".format(task.task_name))

        return task_result

    @on_pending_transactions
    def oracle_poke(self, task=None, global_manager=None, task_result=None):

        price_validity = self.contracts_loaded["PriceProvider"].sc.functions.peek().call()[1]
        if not self.contracts_loaded["PriceProvider"].sc.functions.compute().call()[1] and price_validity:

            # return if there are pending transactions
            if task_result.get('pending_transactions', None):
                return task_result

            web3 = self.connection_helper.connection_manager.web3

            nonce = web3.eth.get_transaction_count(
                self.connection_helper.connection_manager.accounts[0].address, "pending")

            # get gas price from node
            node_gas_price = decimal.Decimal(Web3.from_wei(web3.eth.gas_price, 'ether'))

            # Multiply factor of the using gas price
            calculated_gas_price = node_gas_price * decimal.Decimal(self.config['gas_price_multiply_factor'])

            try:
                tx_hash = self.contracts_loaded["PriceProvider"].poke(
                    gas_limit=self.config['tasks']['oracle_poke']['gas_limit'],
                    gas_price=int(calculated_gas_price * 10 ** 18),
                    nonce=nonce
                )
            except ValueError as err:
                log.error("Task :: {0} :: Error sending transaction! \n {1}".format(task.task_name, err))
                return task_result

            if tx_hash:
                new_tx = dict()
                new_tx['hash'] = tx_hash
                new_tx['timestamp'] = datetime.datetime.now()
                new_tx['gas_price'] = calculated_gas_price
                new_tx['nonce'] = nonce
                new_tx['timeout'] = self.config['tasks']['oracle_poke']['wait_timeout']
                task_result['pending_transactions'].append(new_tx)

                log.info("Task :: {0} :: Sending TX :: Hash: [{1}] Nonce: [{2}] Gas Price: [{3}]".format(
                    task.task_name, Web3.to_hex(new_tx['hash']), new_tx['nonce'], int(calculated_gas_price * 10 ** 18)))

            log.error("Task :: {0} :: Not valid price! Disabling Price!".format(task.task_name))
            aws_put_metric_heart_beat(self.config['tasks']['oracle_poke']['cloudwatch'], 1)

        else:
            # if no valid price in oracle please send alarm
            if not price_validity:
                log.error("Task :: {0} :: No valid price in oracle!".format(task.task_name))
                aws_put_metric_heart_beat(self.config['tasks']['oracle_poke']['cloudwatch'], 1)

            log.info("Task :: {0} :: No!".format(task.task_name))

        return task_result

    @on_pending_transactions
    def splitter_split(self, task=None, global_manager=None, task_result=None):

        if 'pay_bitpro_holders_confirm_block' not in global_manager:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return task_result

        pay_bitpro_holders_confirm_block = global_manager['pay_bitpro_holders_confirm_block']
        if pay_bitpro_holders_confirm_block <= 0:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return task_result

        if 'commission_splitter_confirm_block' in global_manager:
            commission_splitter_confirm_block = global_manager['commission_splitter_confirm_block']
        else:
            commission_splitter_confirm_block = 0

        if pay_bitpro_holders_confirm_block > commission_splitter_confirm_block:
            pass
        else:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return task_result

        # return if there are pending transactions
        if task_result.get('pending_transactions', None):
            return task_result

        web3 = self.connection_helper.connection_manager.web3

        nonce = web3.eth.get_transaction_count(
            self.connection_helper.connection_manager.accounts[0].address, "pending")

        # get gas price from node
        node_gas_price = decimal.Decimal(Web3.from_wei(web3.eth.gas_price, 'ether'))

        # Multiply factor of the using gas price
        calculated_gas_price = node_gas_price * decimal.Decimal(self.config['gas_price_multiply_factor'])

        try:
            tx_hash = self.contracts_loaded["CommissionSplitter"].split(
                gas_limit=self.config['tasks']['splitter_split']['gas_limit'],
                gas_price=int(calculated_gas_price * 10 ** 18),
                nonce=nonce
            )
        except ValueError as err:
            log.error("Task :: {0} :: Error sending transaction! \n {1}".format(task.task_name, err))
            return task_result

        if tx_hash:
            new_tx = dict()
            new_tx['hash'] = tx_hash
            new_tx['timestamp'] = datetime.datetime.now()
            new_tx['gas_price'] = calculated_gas_price
            new_tx['nonce'] = nonce
            new_tx['timeout'] = self.config['tasks']['splitter_split']['wait_timeout']
            task_result['pending_transactions'].append(new_tx)

            log.info("Task :: {0} :: Sending TX :: Hash: [{1}] Nonce: [{2}] Gas Price: [{3}]".format(
                task.task_name, Web3.to_hex(new_tx['hash']), new_tx['nonce'], int(calculated_gas_price * 10 ** 18)))

        global_manager['commission_splitter_confirm_block'] = self.connection_helper.connection_manager.block_number

        return task_result

    @on_pending_transactions
    def splitter_split_v3(self, task=None, global_manager=None, task_result=None):

        if 'pay_bitpro_holders_confirm_block' not in global_manager:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return task_result

        pay_bitpro_holders_confirm_block = global_manager['pay_bitpro_holders_confirm_block']
        if pay_bitpro_holders_confirm_block <= 0:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return task_result

        if 'commission_splitter_confirm_block_v3' in global_manager:
            commission_splitter_confirm_block = global_manager['commission_splitter_confirm_block_v3']
        else:
            commission_splitter_confirm_block = 0

        if pay_bitpro_holders_confirm_block > commission_splitter_confirm_block:
            pass
        else:
            log.info("Task :: {0} :: No!".format(task.task_name))
            return task_result

        # return if there are pending transactions
        if task_result.get('pending_transactions', None):
            return task_result

        web3 = self.connection_helper.connection_manager.web3

        nonce = web3.eth.get_transaction_count(
            self.connection_helper.connection_manager.accounts[0].address, "pending")

        # get gas price from node
        node_gas_price = decimal.Decimal(Web3.from_wei(web3.eth.gas_price, 'ether'))

        # Multiply factor of the using gas price
        calculated_gas_price = node_gas_price * decimal.Decimal(self.config['gas_price_multiply_factor'])

        try:
            tx_hash = self.contracts_loaded["CommissionSplitterV3"].split(
                gas_limit=self.config['tasks']['splitter_split_v3']['gas_limit'],
                gas_price=int(calculated_gas_price * 10 ** 18),
                nonce=nonce
            )
        except ValueError as err:
            log.error("Task :: {0} :: Error sending transaction! \n {1}".format(task.task_name, err))
            return task_result

        if tx_hash:
            new_tx = dict()
            new_tx['hash'] = tx_hash
            new_tx['timestamp'] = datetime.datetime.now()
            new_tx['gas_price'] = calculated_gas_price
            new_tx['nonce'] = nonce
            new_tx['timeout'] = self.config['tasks']['splitter_split_v3']['wait_timeout']
            task_result['pending_transactions'].append(new_tx)

            log.info("Task :: {0} :: Sending TX :: Hash: [{1}] Nonce: [{2}] Gas Price: [{3}]".format(
                task.task_name, Web3.to_hex(new_tx['hash']), new_tx['nonce'], int(calculated_gas_price * 10 ** 18)))

        global_manager['commission_splitter_confirm_block_v3'] = self.connection_helper.connection_manager.block_number

        return task_result


class AutomatorTasks(Automator):

    def __init__(self, config):

        self.config = config
        self.app_mode = self.config['app_mode']
        self.connection_helper = ConnectionHelperBase(config)

        self.contracts_loaded = dict()
        self.contracts_addresses = dict()

        # contract addresses
        self.load_contracts()

        # init automator
        super().__init__(self.config,
                         self.connection_helper,
                         self.contracts_loaded)

        # Add tasks
        self.schedule_tasks()

    def load_contracts(self):
        """ Get contract address to use later """

        log.info("Getting addresses from Main Contract...")

        if self.config['app_mode'] == 'MoC':
            self.contracts_loaded["MoC"] = MoC(
                self.connection_helper.connection_manager,
                contract_address=self.config['addresses']['MoC'])
            self.contracts_addresses['MoC'] = self.contracts_loaded["MoC"].address().lower()
        else:
            self.contracts_loaded["MoC"] = MoCRRC20(
                self.connection_helper.connection_manager,
                contract_address=self.config['addresses']['MoC'])
            self.contracts_addresses['MoC'] = self.contracts_loaded["MoC"].address().lower()

        self.contracts_addresses['MoCConnector'] = self.contracts_loaded["MoC"].sc.functions.connector().call()

        if self.config['app_mode'] == 'MoC':
            self.contracts_loaded["MoCConnector"] = MoCConnector(
                self.connection_helper.connection_manager,
                contract_address=self.contracts_addresses['MoCConnector'])
            self.contracts_addresses['MoCConnector'] = self.contracts_loaded["MoCConnector"].address().lower()
        else:
            self.contracts_loaded["MoCConnector"] = MoCConnectorRRC20(
                self.connection_helper.connection_manager,
                contract_address=self.contracts_addresses['MoCConnector'])
            self.contracts_addresses['MoCConnector'] = self.contracts_loaded["MoCConnector"].address().lower()

        # get address fom moc connector
        self.contracts_addresses['MoCState'] = self.contracts_loaded["MoCConnector"].sc.functions.mocState().call()
        self.contracts_addresses['MoCSettlement'] = self.contracts_loaded[
            "MoCConnector"].sc.functions.mocSettlement().call()
        self.contracts_addresses['MoCExchange'] = self.contracts_loaded[
            "MoCConnector"].sc.functions.mocExchange().call()
        self.contracts_addresses['MoCInrate'] = self.contracts_loaded[
            "MoCConnector"].sc.functions.mocInrate().call()

        # Get oracle address from moc_state
        if self.config['app_mode'] == 'MoC':
            # MoC
            self.contracts_loaded["MoCState"] = MoCState(
                self.connection_helper.connection_manager,
                contract_address=self.contracts_addresses['MoCState'])
            self.contracts_loaded["MoCInrate"] = MoCInrate(
                self.connection_helper.connection_manager,
                contract_address=self.contracts_addresses['MoCInrate'])
            self.contracts_addresses['PriceProvider'] = self.contracts_loaded[
                "MoCState"].sc.functions.getBtcPriceProvider().call()
            self.contracts_loaded["PriceProvider"] = MoCMedianizer(
                self.connection_helper.connection_manager,
                contract_address=self.contracts_addresses['PriceProvider'])
            self.contracts_loaded["CommissionSplitter"] = CommissionSplitter(
                self.connection_helper.connection_manager,
                contract_address=self.config['addresses']['CommissionSplitter'])
            self.contracts_loaded["CommissionSplitterV3"] = CommissionSplitter(
                self.connection_helper.connection_manager,
                contract_address=self.config['addresses']['CommissionSplitterV3'])
        else:
            # RRC20
            self.contracts_loaded["MoCState"] = MoCStateRRC20(
                self.connection_helper.connection_manager,
                contract_address=self.contracts_addresses['MoCState'])
            self.contracts_loaded["MoCInrate"] = MoCInrateRRC20(
                self.connection_helper.connection_manager,
                contract_address=self.contracts_addresses['MoCInrate'])
            self.contracts_addresses['PriceProvider'] = self.contracts_loaded[
                "MoCState"].sc.functions.getPriceProvider().call()
            self.contracts_loaded["PriceProvider"] = MoCMedianizerRRC20(
                self.connection_helper.connection_manager,
                contract_address=self.contracts_addresses['PriceProvider'])
            self.contracts_loaded["CommissionSplitter"] = CommissionSplitterRRC20(
                self.connection_helper.connection_manager,
                contract_address=self.config['addresses']['CommissionSplitter'])
            self.contracts_loaded["CommissionSplitterV3"] = CommissionSplitterRRC20(
                self.connection_helper.connection_manager,
                contract_address=self.config['addresses']['CommissionSplitterV3'])

        # Multicall
        self.contracts_loaded["Multicall2"] = Multicall2(
            self.connection_helper.connection_manager,
            contract_address=self.config['addresses']['Multicall2'])

    def schedule_tasks(self):

        log.info("Starting adding tasks...")

        # set max workers
        self.max_workers = 1

        # run_settlement
        if 'run_settlement' in self.config['tasks']:
            log.info("Jobs add: 2. Run Settlement")
            interval = self.config['tasks']['run_settlement']['interval']
            self.add_task(self.run_settlement,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='2. Run Settlement')

        # liquidation
        if 'liquidation' in self.config['tasks']:
            log.info("Jobs add: 1. Liquidation")
            interval = self.config['tasks']['liquidation']['interval']
            self.add_task(self.contract_liquidation,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='1. Liquidation')

        # Daily inrate payment
        if 'daily_inrate_payment' in self.config['tasks']:
            log.info("Jobs add: 3. Daily Inrate Payment")
            interval = self.config['tasks']['daily_inrate_payment']['interval']
            self.add_task(self.daily_inrate_payment,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='3. Daily Inrate Payment')

        # pay bitpro holders
        if 'pay_bitpro_holders' in self.config['tasks']:
            log.info("Jobs add: 4. Pay Bitpro Holders")
            interval = self.config['tasks']['pay_bitpro_holders']['interval']
            self.add_task(self.pay_bitpro_holders,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='4. Pay Bitpro Holders')

        # calculate EMA
        if 'calculate_bma' in self.config['tasks']:
            log.info("Jobs add: 5. Calculate EMA")
            interval = self.config['tasks']['calculate_bma']['interval']
            self.add_task(self.calculate_ema,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='5. Calculate EMA')

        # Oracle Poke
        if 'oracle_poke' in self.config['tasks']:
            log.info("Jobs add: 6. Oracle Compute")
            interval = self.config['tasks']['oracle_poke']['interval']
            self.add_task(self.oracle_poke,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='6. Oracle Compute')

        # Splitter split
        if 'splitter_split' in self.config['tasks']:
            log.info("Jobs add: 7. Commission splitter")
            interval = self.config['tasks']['splitter_split']['interval']
            self.add_task(self.splitter_split,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='7. Commission splitter')

        # Splitter split V3
        if 'splitter_split_v3' in self.config['tasks']:
            log.info("Jobs add: 8. Commission splitter V3")
            interval = self.config['tasks']['splitter_split_v3']['interval']
            self.add_task(self.splitter_split_v3,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='8. Commission splitter V3')

        # Set max workers
        self.max_tasks = len(self.tasks)
