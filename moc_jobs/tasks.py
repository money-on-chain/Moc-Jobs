import datetime

from web3 import Web3

from moneyonchain.networks import network_manager, web3, chain
from moneyonchain.moc import MoC, CommissionSplitter, MoCConnector, MoCState
from moneyonchain.rdoc import RDOCMoC, RDOCCommissionSplitter, RDOCMoCConnector, RDOCMoCState
from moneyonchain.medianizer import MoCMedianizer, RDOCMoCMedianizer
from moneyonchain.transaction import receipt_to_log


from .tasks_manager import TasksManager
from .logger import log
from .utils import aws_put_metric_heart_beat


__VERSION__ = '2.3.1'

BUCKET_X2 = '0x5832000000000000000000000000000000000000000000000000000000000000'
BUCKET_C0 = '0x4330000000000000000000000000000000000000000000000000000000000000'

last_pay_bitpro_holder_block = 0

log.info("Starting MoC Jobs version {0}".format(__VERSION__))


def get_contract_moc(options, moc_address=None):

    app_mode = options['networks'][network_manager.config_network]['app_mode']

    if app_mode == 'RRC20':
        contract_moc = RDOCMoC(
            network_manager,
            contract_address=moc_address,
            load_sub_contract=False).from_abi()
    elif app_mode == 'MoC':
        contract_moc = MoC(
            network_manager,
            contract_address=moc_address,
            load_sub_contract=False).from_abi()
    else:
        raise Exception("Not valid APP Mode")

    return contract_moc


def get_contract_moc_state(options, moc_state_address=None):

    app_mode = options['networks'][network_manager.config_network]['app_mode']

    if app_mode == 'RRC20':
        contract_moc_state = RDOCMoCState(
            network_manager,
            contract_address=moc_state_address).from_abi()
    elif app_mode == 'MoC':
        contract_moc_state = MoCState(
            network_manager,
            contract_address=moc_state_address).from_abi()
    else:
        raise Exception("Not valid APP Mode")

    return contract_moc_state


def get_contract_medianizer(options, medianizer_address=None):

    app_mode = options['networks'][network_manager.config_network]['app_mode']

    if app_mode == 'RRC20':
        contract_medianizer = RDOCMoCMedianizer(
            network_manager,
            contract_address=medianizer_address).from_abi()
    elif app_mode == 'MoC':
        contract_medianizer = MoCMedianizer(
            network_manager,
            contract_address=medianizer_address).from_abi()
    else:
        raise Exception("Not valid APP Mode")

    return contract_medianizer


def get_contract_commission_splitter(options, splitter_address=None):

    app_mode = options['networks'][network_manager.config_network]['app_mode']

    if app_mode == 'RRC20':
        contract_commission_splitter = RDOCCommissionSplitter(network_manager,
                                                              contract_address=splitter_address).from_abi()
    elif app_mode == 'MoC':
        contract_commission_splitter = CommissionSplitter(network_manager,
                                                          contract_address=splitter_address).from_abi()
    else:
        raise Exception("Not valid APP Mode")

    return contract_commission_splitter


def pending_queue_is_full(account_index=0):

    # get first account
    account_address = network_manager.accounts[account_index].address

    nonce = web3.eth.getTransactionCount(account_address, 'pending')
    last_used_nonce = web3.eth.getTransactionCount(account_address)

    # A limit of pending on blockchain
    if nonce >= last_used_nonce + 1:
        log.info('Cannot create more transactions for {} as the node queue will be full [{}, {}]'.format(
            account_address, nonce, last_used_nonce))
        return True

    return False


def save_pending_tx_receipt(tx_receipt, task_name):
    """ Tx receipt """

    result = dict()
    result['receipt'] = dict()

    if tx_receipt is None:
        result['receipt']['id'] = None
        result['receipt']['timestamp'] = None
        return result

    result['receipt']['id'] = tx_receipt.txid
    result['receipt']['timestamp'] = datetime.datetime.now()

    log.info("Task :: {0} :: Sending tx: {1}".format(task_name, tx_receipt.txid))

    return result


def pending_transaction_receipt(task):
    """ Wait to pending receipt get confirmed"""

    result = dict()
    if task.tx_receipt:
        result['receipt'] = dict()
        result['receipt']['confirmed'] = False
        tx_rcp = chain.get_transaction(task.tx_receipt)
        # pending state
        if tx_rcp.confirmations < 1 and tx_rcp.status < 0:
            elapsed = datetime.datetime.now() - task.tx_receipt_timestamp
            timeout = datetime.timedelta(seconds=task.timeout)

            if elapsed > timeout:
                # timeout allow to send again transaction
                result['receipt']['id'] = None
                result['receipt']['timestamp'] = None
                result['receipt']['confirmed'] = True
        else:
            # confirmed state
            result['receipt']['confirmed'] = True
            result['receipt']['id'] = None
            result['receipt']['timestamp'] = None

            log.info("Task :: {0} :: Confirmed tx!".format(task.task_name))
            tx_rcp.info()
            receipt_to_log(tx_rcp, log)

    return result


def task_contract_liquidation(options, contracts_addresses, task=None, global_manager=None):

    partial_execution_steps = options['tasks']['liquidation']['partial_execution_steps']
    gas_limit = options['tasks']['liquidation']['gas_limit']
    moc_address = contracts_addresses['MoC']
    moc_state_address = contracts_addresses['MoCState']

    # Not call until tx confirmated!
    pending_tx_receipt = pending_transaction_receipt(task)
    if 'receipt' in pending_tx_receipt:
        if not pending_tx_receipt['receipt']['confirmed']:
            log.info("Task :: {0} :: Pending tx state ...".format(task.task_name))
            return pending_tx_receipt

    contract_moc = get_contract_moc(options, moc_address=moc_address)
    contract_moc_state = get_contract_moc_state(options, moc_state_address=moc_state_address)

    if contract_moc_state.is_liquidation():

        if pending_queue_is_full():
            log.error("Task :: {0} :: Pending queue is full".format(task.task_name))
            aws_put_metric_heart_beat(1)
            return

        tx_args = contract_moc.tx_arguments(gas_limit=gas_limit, required_confs=0)

        # Only if is liquidation reach
        tx_receipt = contract_moc.sc.evalLiquidation(
            partial_execution_steps,
            tx_args)

        return save_pending_tx_receipt(tx_receipt, task.task_name)
    else:
        log.info("Task :: {0} :: No!".format(task.task_name))
        return save_pending_tx_receipt(None, task.task_name)


def task_contract_bucket_liquidation(options, contracts_addresses, task=None, global_manager=None):

    gas_limit = options['tasks']['bucket_liquidation']['gas_limit']
    moc_address = contracts_addresses['MoC']

    # Not call until tx confirmated!
    pending_tx_receipt = pending_transaction_receipt(task)
    if 'receipt' in pending_tx_receipt:
        if not pending_tx_receipt['receipt']['confirmed']:
            log.info("Task :: {0} :: Pending tx state ...".format(task.task_name))
            return pending_tx_receipt

    contract_moc = get_contract_moc(options, moc_address=moc_address)

    if contract_moc.is_bucket_liquidation() and not contract_moc.is_settlement_enabled():

        if pending_queue_is_full():
            log.error("Task :: {0} :: Pending queue is full".format(task.task_name))
            aws_put_metric_heart_beat(1)
            return

        tx_args = contract_moc.tx_arguments(gas_limit=gas_limit, required_confs=0)

        # Only if is liquidation reach
        tx_receipt = contract_moc.sc.evalBucketLiquidation(
            BUCKET_X2,
            tx_args)

        return save_pending_tx_receipt(tx_receipt, task.task_name)
    else:
        log.info("Task :: {0} :: No!".format(task.task_name))
        return save_pending_tx_receipt(None, task.task_name)


def task_contract_run_settlement(options, contracts_addresses, task=None, global_manager=None):

    partial_execution_steps = options['tasks']['run_settlement']['partial_execution_steps']
    gas_limit = options['tasks']['run_settlement']['gas_limit']
    moc_address = contracts_addresses['MoC']

    # Not call until tx confirmated!
    pending_tx_receipt = pending_transaction_receipt(task)
    if 'receipt' in pending_tx_receipt:
        if not pending_tx_receipt['receipt']['confirmed']:
            log.info("Task :: {0} :: Pending tx state ...".format(task.task_name))
            return pending_tx_receipt

    contract_moc = get_contract_moc(options, moc_address=moc_address)

    if contract_moc.is_settlement_enabled():

        if pending_queue_is_full():
            log.error("Task :: {0} :: Pending queue is full".format(task.task_name))
            aws_put_metric_heart_beat(1)
            return

        tx_args = contract_moc.tx_arguments(gas_limit=gas_limit, required_confs=0)

        tx_receipt = contract_moc.sc.runSettlement(
            partial_execution_steps,
            tx_args)

        return save_pending_tx_receipt(tx_receipt, task.task_name)

    else:
        log.info("Task :: {0} :: No!".format(task.task_name))
        return save_pending_tx_receipt(None, task.task_name)


def task_contract_daily_inrate_payment(options, contracts_addresses, task=None, global_manager=None):

    gas_limit = options['tasks']['daily_inrate_payment']['gas_limit']
    moc_address = contracts_addresses['MoC']

    # Not call until tx confirmated!
    pending_tx_receipt = pending_transaction_receipt(task)
    if 'receipt' in pending_tx_receipt:
        if not pending_tx_receipt['receipt']['confirmed']:
            log.info("Task :: {0} :: Pending tx state ...".format(task.task_name))
            return pending_tx_receipt

    contract_moc = get_contract_moc(options, moc_address=moc_address)

    if contract_moc.is_daily_enabled():

        if pending_queue_is_full():
            log.error("Task :: {0} :: Pending queue is full".format(task.task_name))
            aws_put_metric_heart_beat(1)
            return

        tx_args = contract_moc.tx_arguments(gas_limit=gas_limit, required_confs=0)

        tx_receipt = contract_moc.sc.dailyInratePayment(tx_args)

        return save_pending_tx_receipt(tx_receipt, task.task_name)

    else:
        log.info("Task :: {0} :: No!".format(task.task_name))
        return save_pending_tx_receipt(None, task.task_name)


def task_contract_splitter_split(options, contracts_addresses, task=None, global_manager=None):

    gas_limit = options['tasks']['splitter_split']['gas_limit']
    app_mode = options['networks'][network_manager.config_network]['app_mode']
    splitter_address = options['networks'][network_manager.config_network]['addresses']['CommissionSplitter']

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

    # Not call until tx confirmated!
    pending_tx_receipt = pending_transaction_receipt(task)
    if 'receipt' in pending_tx_receipt:
        if not pending_tx_receipt['receipt']['confirmed']:
            log.info("Task :: {0} :: Pending tx state ...".format(task.task_name))
        else:
            global_manager['commission_splitter_confirm_block'] = network_manager.block_number
        return pending_tx_receipt

    if pending_queue_is_full():
        log.error("Task :: {0} :: Pending queue is full".format(task.task_name))
        aws_put_metric_heart_beat(1)
        return

    contract_splitter = get_contract_commission_splitter(options, splitter_address=splitter_address)

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

    tx_args = contract_splitter.tx_arguments(gas_limit=gas_limit, required_confs=0)
    tx_receipt = contract_splitter.sc.split(tx_args)

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

    #global_manager['pay_bitpro_holders_confirm_block'] = 0

    return save_pending_tx_receipt(tx_receipt, task.task_name)


def task_contract_pay_bitpro_holders(options, contracts_addresses, task=None, global_manager=None):

    gas_limit = options['tasks']['pay_bitpro_holders']['gas_limit']
    app_mode = options['networks'][network_manager.config_network]['app_mode']
    moc_address = contracts_addresses['MoC']

    # Not call until tx confirmated!
    pending_tx_receipt = pending_transaction_receipt(task)
    if 'receipt' in pending_tx_receipt:
        if not pending_tx_receipt['receipt']['confirmed']:
            log.info("Task :: {0} :: Pending tx state ...".format(task.task_name))
            return pending_tx_receipt
        else:
            global_manager['pay_bitpro_holders_confirm_block'] = network_manager.block_number

    contract_moc = get_contract_moc(options, moc_address=moc_address)

    if contract_moc.is_bitpro_interest_enabled():

        if pending_queue_is_full():
            log.error("Task :: {0} :: Pending queue is full".format(task.task_name))
            aws_put_metric_heart_beat(1)
            return

        tx_args = contract_moc.tx_arguments(gas_limit=gas_limit, required_confs=0)

        if app_mode == 'MoC':
            tx_receipt = contract_moc.sc.payBitProHoldersInterestPayment(tx_args)
        else:
            tx_receipt = contract_moc.sc.payRiskProHoldersInterestPayment(tx_args)

        return save_pending_tx_receipt(tx_receipt, task.task_name)

    else:
        log.info("Task :: {0} :: No!".format(task.task_name))
        return save_pending_tx_receipt(None, task.task_name)


def task_contract_calculate_bma(options, contracts_addresses, task=None, global_manager=None):

    gas_limit = options['tasks']['calculate_bma']['gas_limit']
    moc_state_address = contracts_addresses['MoCState']
    app_mode = options['networks'][network_manager.config_network]['app_mode']

    # Not call until tx confirmated!
    pending_tx_receipt = pending_transaction_receipt(task)
    if 'receipt' in pending_tx_receipt:
        if not pending_tx_receipt['receipt']['confirmed']:
            log.info("Task :: {0} :: Pending tx state ...".format(task.task_name))
            return pending_tx_receipt

    contract_moc_state = get_contract_moc_state(options, moc_state_address=moc_state_address)

    if contract_moc_state.is_calculate_ema():

        if pending_queue_is_full():
            log.error("Task :: {0} :: Pending queue is full".format(task.task_name))
            aws_put_metric_heart_beat(1)
            return

        tx_args = contract_moc_state.tx_arguments(gas_limit=gas_limit, required_confs=0)

        if app_mode == 'MoC':
            tx_receipt = contract_moc_state.sc.calculateBitcoinMovingAverage(tx_args)
        else:
            tx_receipt = contract_moc_state.sc.calculateReserveTokenMovingAverage(tx_args)

        return save_pending_tx_receipt(tx_receipt, task.task_name)

    else:
        log.info("Task :: {0} :: No!".format(task.task_name))
        return save_pending_tx_receipt(None, task.task_name)


def task_contract_oracle_poke(options, contracts_addresses, task=None, global_manager=None):

    gas_limit = options['tasks']['oracle_poke']['gas_limit']
    medianizer_address = contracts_addresses['PriceProvider']

    # Not call until tx confirmated!
    pending_tx_receipt = pending_transaction_receipt(task)
    if 'receipt' in pending_tx_receipt:
        if not pending_tx_receipt['receipt']['confirmed']:
            log.info("Task :: {0} :: Pending tx state ...".format(task.task_name))
            return pending_tx_receipt

    contract_medianizer = get_contract_medianizer(options, medianizer_address=medianizer_address)

    if not contract_medianizer.compute()[1] and contract_medianizer.peek()[1]:

        if pending_queue_is_full():
            log.error("Task :: {0} :: Pending queue is full".format(task.task_name))
            aws_put_metric_heart_beat(1)
            return

        tx_args = contract_medianizer.tx_arguments(gas_limit=gas_limit, required_confs=0)

        tx_receipt = contract_medianizer.poke(tx_args)
        log.error("Task :: {0} :: Not valid price! Disabling MOC Price!".format(task.task_name))
        aws_put_metric_heart_beat(1)

        return save_pending_tx_receipt(tx_receipt, task.task_name)
    else:
        log.info("Task :: {0} :: No!".format(task.task_name))
        return save_pending_tx_receipt(None, task.task_name)


def reconnect_on_lost_chain(task=None, global_manager=None):

    # get las block query last time from task result of the run last time task
    if task.result:
        last_block = task.result
    else:
        last_block = 0

    block = network_manager.block_number

    if not last_block:
        log.info("Task :: Reconnect on lost chain :: Ok :: [{0}/{1}]".format(
            last_block, block))
        last_block = block

        return last_block

    if block <= last_block:
        # this means no new blocks from the last call,
        # so this means a halt node, try to reconnect.

        log.error("Task :: Reconnect on lost chain :: "
                  "[ERROR] :: Same block from the last time! Terminate Task Manager! [{0}/{1}]".format(
                    last_block, block))

        # Put alarm in aws
        aws_put_metric_heart_beat(1)

        # terminate job
        return dict(shutdown=True)

    log.info("Task :: Reconnect on lost chain :: Ok :: [{0}/{1}]".format(
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

        # contract addresses
        self.contract_addresses = self.connector_addresses()

        # Add tasks
        self.schedule_tasks()

    def connect(self):
        """ Init connection"""

        # Connect to network
        network_manager.connect(
            connection_network=self.connection_network,
            config_network=self.config_network)

    def connector_addresses(self):
        """ Get contract address to use later """

        log.info("Getting addresses from Main Contract...")

        app_mode = self.options['networks'][network_manager.config_network]['app_mode']
        moc_address = self.options['networks'][network_manager.config_network]['addresses']['MoC']

        contract_moc = get_contract_moc(self.options, moc_address=moc_address)

        contracts_addresses = dict()
        contracts_addresses['MoCConnector'] = contract_moc.connector()

        if app_mode == 'RRC20':
            conn = RDOCMoCConnector(network_manager, contract_address=contracts_addresses['MoCConnector']).from_abi()
        elif app_mode == 'MoC':
            conn = MoCConnector(network_manager, contract_address=contracts_addresses['MoCConnector']).from_abi()
        else:
            raise Exception("Not valid APP Mode")

        contracts_addresses = conn.contracts_addresses()

        # Get oracle address from moc_state
        contract_moc_state = get_contract_moc_state(self.options, moc_state_address=contracts_addresses['MoCState'])
        contracts_addresses['PriceProvider'] = contract_moc_state.price_provider()

        return contracts_addresses

    def schedule_tasks(self):

        log.info("Starting adding jobs...")

        # creating the alarm
        aws_put_metric_heart_beat(0)

        # Reconnect on lost chain
        log.info("Jobs add: 99. Reconnect on lost chain")
        self.add_task(reconnect_on_lost_chain, args=[], wait=180, timeout=180)

        # run_settlement
        if 'run_settlement' in self.options['tasks']:
            log.info("Jobs add: 3. Run Settlement")
            interval = self.options['tasks']['run_settlement']['interval']
            self.add_task(task_contract_run_settlement,
                          args=[self.options, self.contract_addresses],
                          wait=interval,
                          timeout=180,
                          task_name='3. Run Settlement')

        # liquidation
        if 'liquidation' in self.options['tasks']:
            log.info("Jobs add: 1. Liquidation")
            interval = self.options['tasks']['liquidation']['interval']
            self.add_task(task_contract_liquidation,
                          args=[self.options, self.contract_addresses],
                          wait=interval,
                          timeout=180,
                          task_name='1. Liquidation')

        # bucket_liquidation
        if 'bucket_liquidation' in self.options['tasks']:
            log.info("Jobs add: 2. Bucket Liquidation")
            interval = self.options['tasks']['bucket_liquidation']['interval']
            self.add_task(task_contract_bucket_liquidation,
                          args=[self.options, self.contract_addresses],
                          wait=interval,
                          timeout=180,
                          task_name='2. Bucket Liquidation')

        # daily_inrate_payment
        if 'daily_inrate_payment' in self.options['tasks']:
            log.info("Jobs add: 4. Daily Inrate Payment")
            interval = self.options['tasks']['daily_inrate_payment']['interval']
            self.add_task(task_contract_daily_inrate_payment,
                          args=[self.options, self.contract_addresses],
                          wait=interval,
                          timeout=180,
                          task_name='4. Daily Inrate Payment')

        # pay_bitpro_holders
        if 'pay_bitpro_holders' in self.options['tasks']:
            log.info("Jobs add: 5. Pay Bitpro Holders")
            interval = self.options['tasks']['pay_bitpro_holders']['interval']
            self.add_task(task_contract_pay_bitpro_holders,
                          args=[self.options, self.contract_addresses],
                          wait=interval,
                          timeout=180,
                          task_name='5. Pay Bitpro Holders')

        # calculate_bma
        if 'calculate_bma' in self.options['tasks']:
            log.info("Jobs add: 6. Calculate EMA")
            interval = self.options['tasks']['calculate_bma']['interval']
            self.add_task(task_contract_calculate_bma,
                          args=[self.options, self.contract_addresses],
                          wait=interval,
                          timeout=180,
                          task_name='6. Calculate EMA')

        # Oracle Poke
        if 'oracle_poke' in self.options['tasks']:
            log.info("Jobs add: 7. Oracle Compute")
            interval = self.options['tasks']['oracle_poke']['interval']
            self.add_task(task_contract_oracle_poke,
                          args=[self.options, self.contract_addresses],
                          wait=interval,
                          timeout=180,
                          task_name='7. Oracle Compute')

        # Splitter split
        if 'splitter_split' in self.options['tasks']:
            log.info("Jobs add: 8. Commission splitter")
            interval = self.options['tasks']['splitter_split']['interval']
            self.add_task(task_contract_splitter_split,
                          args=[self.options, self.contract_addresses],
                          wait=interval,
                          timeout=180,
                          task_name='8. Commission splitter')

        # Set max workers
        self.max_tasks = len(self.tasks)
