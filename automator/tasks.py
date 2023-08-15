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
    MoCMedianizerRRC20

from .base.main import ConnectionHelperBase
from .tasks_manager import TransactionsTasksManager
from .logger import log
from .backend import Automator


__VERSION__ = '3.0.0'


log.info("Starting Stable Protocol Automator version {0}".format(__VERSION__))


class AutomatorTasks(TransactionsTasksManager):

    def __init__(self, config):

        TransactionsTasksManager.__init__(self)

        self.config = config
        self.app_mode = self.config['app_mode']
        self.connection_helper = ConnectionHelperBase(config)

        self.contracts_loaded = dict()
        self.contracts_addresses = dict()

        # contract addresses
        self.load_contracts()

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

        automator = Automator(self.config, self.connection_helper, self.contracts_loaded)

        # run_settlement
        if 'run_settlement' in self.config['tasks']:
            log.info("Jobs add: 2. Run Settlement")
            interval = self.config['tasks']['run_settlement']['interval']
            self.add_task(automator.run_settlement,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='2. Run Settlement')

        # liquidation
        if 'liquidation' in self.config['tasks']:
            log.info("Jobs add: 1. Liquidation")
            interval = self.config['tasks']['liquidation']['interval']
            self.add_task(automator.contract_liquidation,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='1. Liquidation')

        # Daily inrate payment
        if 'daily_inrate_payment' in self.config['tasks']:
            log.info("Jobs add: 3. Daily Inrate Payment")
            interval = self.config['tasks']['daily_inrate_payment']['interval']
            self.add_task(automator.daily_inrate_payment,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='3. Daily Inrate Payment')

        # pay bitpro holders
        if 'pay_bitpro_holders' in self.config['tasks']:
            log.info("Jobs add: 4. Pay Bitpro Holders")
            interval = self.config['tasks']['pay_bitpro_holders']['interval']
            self.add_task(automator.pay_bitpro_holders,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='4. Pay Bitpro Holders')

        # calculate EMA
        if 'calculate_bma' in self.config['tasks']:
            log.info("Jobs add: 5. Calculate EMA")
            interval = self.config['tasks']['calculate_bma']['interval']
            self.add_task(automator.calculate_ema,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='5. Calculate EMA')

        # Oracle Poke
        if 'oracle_poke' in self.config['tasks']:
            log.info("Jobs add: 6. Oracle Compute")
            interval = self.config['tasks']['oracle_poke']['interval']
            self.add_task(automator.oracle_poke,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='6. Oracle Compute')

        # Splitter split
        if 'splitter_split' in self.config['tasks']:
            log.info("Jobs add: 7. Commission splitter")
            interval = self.config['tasks']['splitter_split']['interval']
            self.add_task(automator.splitter_split,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='7. Commission splitter')

        # Splitter split V3
        if 'splitter_split_v3' in self.config['tasks']:
            log.info("Jobs add: 8. Commission splitter V3")
            interval = self.config['tasks']['splitter_split_v3']['interval']
            self.add_task(automator.splitter_split_v3,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='8. Commission splitter V3')

        # Set max workers
        self.max_tasks = len(self.tasks)
