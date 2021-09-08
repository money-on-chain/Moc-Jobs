from web3 import Web3

from moneyonchain.networks import network_manager, web3
from moneyonchain.moc import MoC, CommissionSplitter
from moneyonchain.rdoc import RDOCMoC, RDOCCommissionSplitter
from moneyonchain.medianizer import MoCMedianizer, RDOCMoCMedianizer


from .tasks_manager import TasksManager
from .logger import log
from .utils import aws_put_metric_heart_beat


__VERSION__ = '2.2.1'


log.info("Starting MoC Jobs version {0}".format(__VERSION__))


def get_contract_moc(options):

    app_mode = options['networks'][network_manager.config_network]['app_mode']

    if app_mode == 'RRC20':
        contract_moc = RDOCMoC(
            network_manager,
            load_sub_contract=False).from_abi().contracts_discovery()
    elif app_mode == 'MoC':
        contract_moc = MoC(
            network_manager,
            load_sub_contract=False).from_abi().contracts_discovery()
    else:
        raise Exception("Not valid APP Mode")

    return contract_moc


def get_contract_medianizer(options):

    app_mode = options['networks'][network_manager.config_network]['app_mode']

    if app_mode == 'RRC20':
        contract_moc = RDOCMoC(
            network_manager,
            load_sub_contract=False).from_abi().contracts_discovery()
        contract_mocstate = contract_moc.sc_moc_state
        contract_medianizer = RDOCMoCMedianizer(
            network_manager,
            contract_address=contract_mocstate.price_provider()).from_abi()
    elif app_mode == 'MoC':
        contract_moc = MoC(network_manager, load_sub_contract=False).from_abi().contracts_discovery()
        contract_mocstate = contract_moc.sc_moc_state
        contract_medianizer = MoCMedianizer(
            network_manager,
            contract_address=contract_mocstate.price_provider()).from_abi()
    else:
        raise Exception("Not valid APP Mode")

    return contract_medianizer


def get_contract_commission_splitter(options):

    app_mode = options['networks'][network_manager.config_network]['app_mode']

    if app_mode == 'RRC20':
        contract_commission_splitter = RDOCCommissionSplitter(network_manager).from_abi()
    elif app_mode == 'MoC':
        contract_commission_splitter = CommissionSplitter(network_manager).from_abi()
    else:
        raise Exception("Not valid APP Mode")

    return contract_commission_splitter


def pending_queue_is_full(account_index=0):

    # get first account
    account_address = network_manager.accounts[account_index].address

    nonce = web3.eth.getTransactionCount(account_address, 'pending')
    last_used_nonce = web3.eth.getTransactionCount(account_address)

    # A limit of pending on blockchain
    if nonce >= last_used_nonce + 3:
        log.info('Cannot create more transactions for {} as the node queue will be full [{}, {}]'.format(
            account_address, nonce, last_used_nonce))
        return True

    return False


def task_contract_liquidation(options, task=None):

    partial_execution_steps = options['tasks']['liquidation']['partial_execution_steps']
    gas_limit = options['tasks']['liquidation']['gas_limit']

    log.info("Task :: liquidation :: Start")

    contract_moc = get_contract_moc(options)

    if contract_moc.sc_moc_state.is_liquidation():

        if pending_queue_is_full():
            log.error("Task :: liquidation :: Pending queue is full")
            aws_put_metric_heart_beat(1)
            log.info("Task :: liquidation :: End")
            return

        tx_receipt = contract_moc.execute_liquidation(
            partial_execution_steps,
            gas_limit=gas_limit)

    log.info("Task :: liquidation :: End")


def task_contract_bucket_liquidation(options, task=None):

    gas_limit = options['tasks']['bucket_liquidation']['gas_limit']

    log.info("Task :: bucket liquidation :: Start")

    contract_moc = get_contract_moc(options)

    if contract_moc.is_bucket_liquidation() and not contract_moc.is_settlement_enabled():

        if pending_queue_is_full():
            log.error("Task :: bucket liquidation :: Pending queue is full")
            aws_put_metric_heart_beat(1)
            log.info("Task :: bucket liquidation :: End")
            return

        tx_receipt = contract_moc.execute_bucket_liquidation(
            gas_limit=gas_limit)

    log.info("Task :: bucket liquidation :: End")


def task_contract_run_settlement(options, task=None):

    partial_execution_steps = options['tasks']['run_settlement']['partial_execution_steps']
    gas_limit = options['tasks']['run_settlement']['gas_limit']

    log.info("Task :: runSettlement :: Start")

    contract_moc = get_contract_moc(options)

    if contract_moc.is_settlement_enabled():

        if pending_queue_is_full():
            log.error("Task :: runSettlement :: Pending queue is full")
            aws_put_metric_heart_beat(1)
            log.info("Task :: runSettlement :: End")
            return

        tx_receipt = contract_moc.execute_run_settlement(
            partial_execution_steps,
            gas_limit=gas_limit)

    log.info("Task :: runSettlement :: End")


def task_contract_daily_inrate_payment(options, task=None):

    gas_limit = options['tasks']['daily_inrate_payment']['gas_limit']

    log.info("Task :: dailyInratePayment :: Start")

    contract_moc = get_contract_moc(options)

    if contract_moc.is_daily_enabled():

        if pending_queue_is_full():
            log.error("Task :: dailyInratePayment :: Pending queue is full")
            aws_put_metric_heart_beat(1)
            log.info("Task :: dailyInratePayment :: End")
            return

        tx_receipt = contract_moc.execute_daily_inrate_payment(
            gas_limit=gas_limit)

    log.info("Task :: dailyInratePayment :: End")


def task_contract_splitter_split(options, task=None):

    gas_limit = options['tasks']['splitter_split']['gas_limit']
    app_mode = options['networks'][network_manager.config_network]['app_mode']

    log.info("Calling Splitter ...")

    contract_splitter = get_contract_commission_splitter(options)

    info_dict = dict()
    info_dict['before'] = dict()
    info_dict['after'] = dict()
    info_dict['proportion'] = dict()

    info_dict['proportion']['moc'] = 0.5
    if app_mode == 'MoC':
        info_dict['proportion']['moc'] = Web3.fromWei(contract_splitter.moc_proportion(), 'ether')

    info_dict['proportion']['multisig'] = 1 - info_dict['proportion']['moc']

    resume = str()

    resume += "Splitter address: [{0}]\n".format(contract_splitter.address())
    resume += "Multisig address: [{0}]\n".format(contract_splitter.commission_address())
    resume += "MoC address: [{0}]\n".format(contract_splitter.moc_address())
    resume += "Proportion MOC: [{0}]\n".format(info_dict['proportion']['moc'])
    resume += "Proportion Multisig: [{0}]\n".format(info_dict['proportion']['multisig'])

    resume += "BEFORE SPLIT:\n"
    resume += "=============\n"

    info_dict['before']['splitter'] = contract_splitter.balance()
    resume += "Splitter balance: [{0}]\n".format(info_dict['before']['splitter'])

    # balances commision
    balance = Web3.fromWei(network_manager.network_balance(
        contract_splitter.commission_address()), 'ether')
    info_dict['before']['commission'] = balance
    resume += "Multisig balance (proportion: {0}): [{1}]\n".format(info_dict['proportion']['multisig'],
                                                             info_dict['before']['commission'])

    # balances moc
    balance = Web3.fromWei(network_manager.network_balance(contract_splitter.moc_address()), 'ether')
    info_dict['before']['moc'] = balance
    resume += "MoC balance (proportion: {0}): [{1}]\n".format(
        info_dict['proportion']['moc'],
        info_dict['before']['moc'])

    if pending_queue_is_full():
        log.error("Task :: Commission Splitter :: Pending queue is full")
        aws_put_metric_heart_beat(1)
        log.info("Task :: Commission Splitter :: End")
        return

    tx_receipt = contract_splitter.split(
        gas_limit=gas_limit)

    resume += "AFTER SPLIT:\n"
    resume += "=============\n"

    info_dict['after']['splitter'] = contract_splitter.balance()
    dif = info_dict['after']['splitter'] - info_dict['before']['splitter']
    resume += "Splitter balance: [{0}] Difference: [{1}]\n".format(info_dict['after']['splitter'], dif)

    # balances commision
    balance = Web3.fromWei(network_manager.network_balance(
        contract_splitter.commission_address()), 'ether')
    info_dict['after']['commission'] = balance
    dif = info_dict['after']['commission'] - info_dict['before']['commission']
    resume += "Multisig balance (proportion: {0}): [{1}] Difference: [{2}]\n".format(
        info_dict['proportion']['multisig'],
        info_dict['after']['commission'],
        dif)

    # balances moc
    balance = Web3.fromWei(network_manager.network_balance(contract_splitter.moc_address()), 'ether')
    info_dict['after']['moc'] = balance
    dif = info_dict['after']['moc'] - info_dict['before']['moc']
    resume += "MoC balance (proportion: {0}): [{1}] Difference: [{2}]\n".format(
        info_dict['proportion']['moc'],
        info_dict['after']['moc'],
        dif)

    if tx_receipt:
        log.info(resume)


def task_contract_pay_bitpro_holders(options, task=None):

    gas_limit = options['tasks']['pay_bitpro_holders']['gas_limit']

    log.info("Task :: payBitProHoldersInterestPayment :: Start")

    contract_moc = get_contract_moc(options)

    if contract_moc.is_bitpro_interest_enabled():

        if pending_queue_is_full():
            log.error("Task :: payBitProHoldersInterestPayment :: Pending queue is full")
            aws_put_metric_heart_beat(1)
            log.info("Task :: payBitProHoldersInterestPayment :: End")
            return

        tx_receipt = contract_moc.execute_pay_bitpro_holders(
            gas_limit=gas_limit)

        if tx_receipt:
            task_contract_splitter_split(options)

    log.info("Task :: payBitProHoldersInterestPayment :: End")


def task_contract_calculate_bma(options, task=None):

    gas_limit = options['tasks']['calculate_bma']['gas_limit']

    log.info("Task :: calculateBitcoinMovingAverage :: Start")

    contract_moc = get_contract_moc(options)

    if pending_queue_is_full():
        log.error("Task :: calculateBitcoinMovingAverage :: Pending queue is full")
        aws_put_metric_heart_beat(1)
        log.info("Task :: calculateBitcoinMovingAverage :: End")
        return

    tx_receipt = contract_moc.execute_calculate_ema(
        gas_limit=gas_limit)

    log.info("Task :: calculateBitcoinMovingAverage :: End")


def task_contract_oracle_poke(options, task=None):

    gas_limit = options['tasks']['oracle_poke']['gas_limit']

    log.info("Task :: oracle Poke :: Start")

    contract_medianizer = get_contract_medianizer(options)

    tx_receipt = None
    if not contract_medianizer.compute()[1] and contract_medianizer.peek()[1]:

        if pending_queue_is_full():
            log.error("Task :: oracle Poke :: Pending queue is full")
            aws_put_metric_heart_beat(1)
            log.info("Task :: oracle Poke :: End")
            return

        tx_receipt = contract_medianizer.poke(
            gas_limit=gas_limit)
        log.error("[POKE] Not valid price! Disabling MOC Price!")
        aws_put_metric_heart_beat(1)

    log.info("Task :: oracle Poke :: End")


def reconnect_on_lost_chain(task=None):

    log.info("Task :: Reconnect on lost chain :: Start")

    # get las block query last time from task result of the run last time task
    if task.result:
        last_block = task.result
    else:
        last_block = 0

    block = network_manager.block_number

    if not last_block:
        log.info("Task :: Reconnect on lost chain :: End :: [{0}/{1}]".format(
            last_block, block))
        last_block = block

        return last_block

    if block <= last_block:
        # this means no new blocks from the last call,
        # so this means a halt node, try to reconnect.

        log.error("Task :: Reconnect on lost chain :: "
                  "[ERROR BLOCKCHAIN CONNECT!] Same block from the last time! Terminate Task Manager! [{0}/{1}]".format(
                    last_block, block))

        # Put alarm in aws
        aws_put_metric_heart_beat(1)

        # terminate job
        return dict(shutdown=True)

    log.info("Task :: Reconnect on lost chain :: End :: [{0}/{1}]".format(
        last_block, block))

    # save the last block
    last_block = block

    return last_block


class MoCTasks(TasksManager):

    def __init__(self, app_config, config_net, connection_net):

        super().__init__()

        self.options = app_config
        self.config_network = config_net
        self.connection_network = connection_net

        self.app_mode = self.options['networks'][self.config_network]['app_mode']

        # install custom network if needit
        if self.connection_network.startswith("https") or self.connection_network.startswith("http"):

            a_connection = self.connection_network.split(',')
            host = a_connection[0]
            chain_id = a_connection[1]

            network_manager.add_network(
                network_name='rskCustomNetwork',
                network_host=host,
                network_chainid=chain_id,
                network_explorer='https://blockscout.com/rsk/mainnet/api',
                force=False
            )

            self.connection_network = 'rskCustomNetwork'

            log.info("Using custom network... id: {}".format(self.connection_network))

        # connect and init contracts
        self.connect()

        # Add tasks
        self.schedule_tasks()

    def connect(self):
        """ Init connection"""

        # Connect to network
        network_manager.connect(
            connection_network=self.connection_network,
            config_network=self.config_network)

    def schedule_tasks(self):

        log.info("Starting adding jobs...")

        # creating the alarm
        aws_put_metric_heart_beat(0)

        # Reconnect on lost chain
        log.info("Jobs add reconnect on lost chain")
        self.add_task(reconnect_on_lost_chain, args=[], wait=180, timeout=180)

        # run_settlement
        if 'run_settlement' in self.options['tasks']:
            log.info("Jobs add run_settlement")
            interval = self.options['tasks']['run_settlement']['interval']
            self.add_task(task_contract_run_settlement, args=[self.options], wait=interval, timeout=180)

        # liquidation
        if 'liquidation' in self.options['tasks']:
            log.info("Jobs add liquidation")
            interval = self.options['tasks']['liquidation']['interval']
            self.add_task(task_contract_liquidation, args=[self.options], wait=interval, timeout=180)

        # bucket_liquidation
        if 'bucket_liquidation' in self.options['tasks']:
            log.info("Jobs add bucket_liquidation")
            interval = self.options['tasks']['bucket_liquidation']['interval']
            self.add_task(task_contract_bucket_liquidation, args=[self.options], wait=interval, timeout=180)

        # daily_inrate_payment
        if 'daily_inrate_payment' in self.options['tasks']:
            log.info("Jobs add daily_inrate_payment")
            interval = self.options['tasks']['daily_inrate_payment']['interval']
            self.add_task(task_contract_daily_inrate_payment, args=[self.options], wait=interval, timeout=180)

        # pay_bitpro_holders
        if 'pay_bitpro_holders' in self.options['tasks']:
            log.info("Jobs add pay_bitpro_holders")
            interval = self.options['tasks']['pay_bitpro_holders']['interval']
            self.add_task(task_contract_pay_bitpro_holders, args=[self.options], wait=interval, timeout=180)

        # calculate_bma
        if 'calculate_bma' in self.options['tasks']:
            log.info("Jobs add calculate_bma")
            interval = self.options['tasks']['calculate_bma']['interval']
            self.add_task(task_contract_calculate_bma, args=[self.options], wait=interval, timeout=180)

        # Oracle Poke
        if 'oracle_poke' in self.options['tasks']:
            log.info("Jobs add oracle poke")
            interval = self.options['tasks']['oracle_poke']['interval']
            self.add_task(task_contract_oracle_poke, args=[self.options], wait=interval, timeout=180)

        # # Splitter split
        # if 'splitter_split' in self.options['tasks']:
        #     log.info("Jobs add Splitter split")
        #     interval = self.options['tasks']['splitter_split']['interval']
        #     self.add_task(task_contract_splitter_split, args=[self.options], wait=interval, timeout=180)

        # Set max workers
        self.max_tasks = len(self.tasks)
