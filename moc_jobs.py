import os
from optparse import OptionParser
import datetime
import json
from importlib import reload

from timeloop import Timeloop
import boto3
import time
from web3 import Web3

from moneyonchain import networks
from moneyonchain.moc import MoC, CommissionSplitter
from moneyonchain.rdoc import RDOCMoC, RDOCCommissionSplitter
from moneyonchain.medianizer import MoCMedianizer, RDOCMoCMedianizer


import logging
import logging.config


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger('default')


__VERSION__ = '2.1.0'


log.info("Starting MoC Jobs version {0}".format(__VERSION__))


class JobsManager:

    def __init__(self, app_config, config_net, connection_net):

        self.options = app_config
        self.config_network = config_net
        self.connection_network = connection_net

        nm = networks.network_manager

        # install custom network if needit
        custom_installed = self.install_custom_network(connection_net)
        if custom_installed:
            self.connection_network = 'rskCustomNetwork'
            nt = reload(networks)
            nm = nt.network_manager

        # Connect to network
        nm.connect(
            connection_network=self.connection_network,
            config_network=self.config_network)

        self.app_mode = self.options['networks'][self.config_network]['app_mode']

        if self.app_mode == 'RRC20':
            self.contract_MoC = RDOCMoC(
                nm,
                load_sub_contract=False).from_abi().contracts_discovery()
            self.contract_MoCState = self.contract_MoC.sc_moc_state
            self.contract_MoCMedianizer = RDOCMoCMedianizer(
                nm,
                contract_address=self.contract_MoCState.price_provider()).from_abi()
            self.contract_splitter = RDOCCommissionSplitter(nm).from_abi()
        elif self.app_mode == 'MoC':
            self.contract_MoC = MoC(nm, load_sub_contract=False).from_abi().contracts_discovery()
            self.contract_MoCState = self.contract_MoC.sc_moc_state
            self.contract_MoCMedianizer = MoCMedianizer(
                nm,
                contract_address=self.contract_MoCState.price_provider()).from_abi()
            self.contract_splitter = CommissionSplitter(nm).from_abi()
        else:
            raise Exception("Not valid APP Mode")

        self.tl = Timeloop()

    @staticmethod
    def install_custom_network(connection_net):
        """ Install custom network """

        if connection_net.startswith("https") or connection_net.startswith("https"):
            a_connection = connection_net.split(',')
            host = a_connection[0]
            chain_id = a_connection[1]

            networks.network_manager.add_network(
                network_name='rskCustomNetwork',
                network_host=host,
                network_chainid=chain_id,
                network_explorer='https://blockscout.com/rsk/mainnet/api',
                force=False
            )

            time.sleep(10)

            return True

    @staticmethod
    def aws_put_metric_heart_beat(value):

        if 'AWS_ACCESS_KEY_ID' not in os.environ:
            return

        # Create CloudWatch client
        cloudwatch = boto3.client('cloudwatch')

        # Put custom metrics
        cloudwatch.put_metric_data(
            MetricData=[
                {
                    'MetricName': os.environ['MOC_JOBS_NAME'],
                    'Dimensions': [
                        {
                            'Name': 'JOBS',
                            'Value': 'Error'
                        },
                    ],
                    'Unit': 'None',
                    'Value': value
                },
            ],
            Namespace='MOC/EXCEPTIONS'
        )

    def contract_liquidation(self):

        partial_execution_steps = self.options['tasks']['liquidation']['partial_execution_steps']
        gas_limit = self.options['tasks']['liquidation']['gas_limit']

        tx_receipt = self.contract_MoC.execute_liquidation(
            partial_execution_steps,
            gas_limit=gas_limit)

        log.info("Task :: liquidation :: OK")

    def contract_bucket_liquidation(self):

        gas_limit = self.options['tasks']['bucket_liquidation']['gas_limit']

        tx_receipt = self.contract_MoC.execute_bucket_liquidation(
            gas_limit=gas_limit)

        log.info("Task :: bucket liquidation :: OK")

    def contract_run_settlement(self):

        partial_execution_steps = self.options['tasks']['run_settlement']['partial_execution_steps']
        gas_limit = self.options['tasks']['run_settlement']['gas_limit']

        tx_receipt = self.contract_MoC.execute_run_settlement(
            partial_execution_steps,
            gas_limit=gas_limit)

        log.info("Task :: runSettlement :: OK")

    def contract_daily_inrate_payment(self):

        gas_limit = self.options['tasks']['daily_inrate_payment']['gas_limit']

        tx_receipt = self.contract_MoC.execute_daily_inrate_payment(
            gas_limit=gas_limit)

        log.info("Task :: dailyInratePayment :: OK")

    def contract_splitter_split(self):

        gas_limit = self.options['tasks']['splitter_split']['gas_limit']

        log.info("Calling Splitter ...")

        info_dict = dict()
        info_dict['before'] = dict()
        info_dict['after'] = dict()
        info_dict['proportion'] = dict()

        info_dict['proportion']['moc'] = 0.5
        if self.app_mode == 'MoC':
            info_dict['proportion']['moc'] = Web3.fromWei(self.contract_splitter.moc_proportion(), 'ether')

        info_dict['proportion']['multisig'] = 1 - info_dict['proportion']['moc']

        resume = str()

        resume += "Splitter address: [{0}]\n".format(self.contract_splitter.address())
        resume += "Multisig address: [{0}]\n".format(self.contract_splitter.commission_address())
        resume += "MoC address: [{0}]\n".format(self.contract_splitter.moc_address())
        resume += "Proportion MOC: [{0}]\n".format(info_dict['proportion']['moc'])
        resume += "Proportion Multisig: [{0}]\n".format(info_dict['proportion']['multisig'])

        resume += "BEFORE SPLIT:\n"
        resume += "=============\n"

        info_dict['before']['splitter'] = self.contract_splitter.balance()
        resume += "Splitter balance: [{0}]\n".format(info_dict['before']['splitter'])

        # balances commision
        balance = Web3.fromWei(network_manager.network_balance(
            self.contract_splitter.commission_address()), 'ether')
        info_dict['before']['commission'] = balance
        resume += "Multisig balance (proportion: {0}): [{1}]\n".format(info_dict['proportion']['multisig'],
                                                                 info_dict['before']['commission'])

        # balances moc
        balance = Web3.fromWei(network_manager.network_balance(self.contract_splitter.moc_address()), 'ether')
        info_dict['before']['moc'] = balance
        resume += "MoC balance (proportion: {0}): [{1}]\n".format(
            info_dict['proportion']['moc'],
            info_dict['before']['moc'])

        tx_receipt = self.contract_splitter.split(
            gas_limit=gas_limit)

        resume += "AFTER SPLIT:\n"
        resume += "=============\n"

        info_dict['after']['splitter'] = self.contract_splitter.balance()
        dif = info_dict['after']['splitter'] - info_dict['before']['splitter']
        resume += "Splitter balance: [{0}] Difference: [{1}]\n".format(info_dict['after']['splitter'], dif)

        # balances commision
        balance = Web3.fromWei(network_manager.network_balance(
            self.contract_splitter.commission_address()), 'ether')
        info_dict['after']['commission'] = balance
        dif = info_dict['after']['commission'] - info_dict['before']['commission']
        resume += "Multisig balance (proportion: {0}): [{1}] Difference: [{2}]\n".format(
            info_dict['proportion']['multisig'],
            info_dict['after']['commission'],
            dif)

        # balances moc
        balance = Web3.fromWei(network_manager.network_balance(self.contract_splitter.moc_address()), 'ether')
        info_dict['after']['moc'] = balance
        dif = info_dict['after']['moc'] - info_dict['before']['moc']
        resume += "MoC balance (proportion: {0}): [{1}] Difference: [{2}]\n".format(
            info_dict['proportion']['moc'],
            info_dict['after']['moc'],
            dif)

        if tx_receipt:
            log.info(resume)

    def contract_pay_bitpro_holders(self):

        gas_limit = self.options['tasks']['pay_bitpro_holders']['gas_limit']

        tx_receipt = self.contract_MoC.execute_pay_bitpro_holders(
            gas_limit=gas_limit)

        if tx_receipt:
            self.contract_splitter_split()

        log.info("Task :: payBitProHoldersInterestPayment :: OK")

    def contract_calculate_bma(self):

        gas_limit = self.options['tasks']['calculate_bma']['gas_limit']

        tx_receipt = self.contract_MoC.execute_calculate_ema(
            gas_limit=gas_limit)

        log.info("Task :: calculateBitcoinMovingAverage :: OK")

    def contract_oracle_poke(self):

        gas_limit = self.options['tasks']['oracle_poke']['gas_limit']

        tx_receipt = None
        if not self.contract_MoCMedianizer.compute()[1] and self.contract_MoCMedianizer.peek()[1]:
            tx_receipt = self.contract_MoCMedianizer.poke(
                gas_limit=gas_limit)
            log.error("[POKE] Not valid price! Disabling MOC Price!")
            self.aws_put_metric_heart_beat(1)

        log.info("Task :: oracle Poke :: OK")

    def task_run_settlement(self):

        try:
            self.contract_run_settlement()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_liquidation(self):

        try:
            self.contract_liquidation()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_bucket_liquidation(self):

        try:
            self.contract_bucket_liquidation()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_daily_inrate_payment(self):

        try:
            self.contract_daily_inrate_payment()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_pay_bitpro_holders(self):

        try:
            self.contract_pay_bitpro_holders()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_calculate_bma(self):

        try:
            self.contract_calculate_bma()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_oracle_poke(self):

        try:
            self.contract_oracle_poke()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_splitter_split(self):

        try:
            self.contract_splitter_split()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def add_jobs(self):

        log.info("Starting adding jobs...")

        # creating the alarm
        self.aws_put_metric_heart_beat(0)

        # run_settlement
        if 'run_settlement' in self.options['tasks']:
            log.info("Jobs add run_settlement")
            interval = self.options['tasks']['run_settlement']['interval']
            self.tl._add_job(self.task_run_settlement, datetime.timedelta(seconds=interval))

        # liquidation
        if 'liquidation' in self.options['tasks']:
            log.info("Jobs add liquidation")
            interval = self.options['tasks']['liquidation']['interval']
            self.tl._add_job(self.task_liquidation, datetime.timedelta(seconds=interval))

        # bucket_liquidation
        if 'bucket_liquidation' in self.options['tasks']:
            log.info("Jobs add bucket_liquidation")
            interval = self.options['tasks']['bucket_liquidation']['interval']
            self.tl._add_job(self.task_bucket_liquidation, datetime.timedelta(seconds=interval))

        # daily_inrate_payment
        if 'daily_inrate_payment' in self.options['tasks']:
            log.info("Jobs add daily_inrate_payment")
            interval = self.options['tasks']['daily_inrate_payment']['interval']
            self.tl._add_job(self.task_daily_inrate_payment, datetime.timedelta(seconds=interval))

        # pay_bitpro_holders
        if 'pay_bitpro_holders' in self.options['tasks']:
            log.info("Jobs add pay_bitpro_holders")
            interval = self.options['tasks']['pay_bitpro_holders']['interval']
            self.tl._add_job(self.task_pay_bitpro_holders, datetime.timedelta(seconds=interval))

        # calculate_bma
        if 'calculate_bma' in self.options['tasks']:
            log.info("Jobs add calculate_bma")
            interval = self.options['tasks']['calculate_bma']['interval']
            self.tl._add_job(self.task_calculate_bma, datetime.timedelta(seconds=interval))

        # Oracle Poke
        if 'oracle_poke' in self.options['tasks']:
            log.info("Jobs add oracle poke")
            interval = self.options['tasks']['oracle_poke']['interval']
            self.tl._add_job(self.task_oracle_poke, datetime.timedelta(seconds=interval))

        # Splitter split
        # if 'splitter_split' in self.options['tasks']:
        #     log.info("Jobs add Splitter split")
        #     interval = self.options['tasks']['splitter_split']['interval']
        #     self.tl._add_job(self.task_splitter_split, datetime.timedelta(seconds=interval))

    def time_loop_start(self):

        self.add_jobs()
        #self.tl.start(block=True)
        self.tl.start()
        while True:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                self.tl.stop()
                log.info("Shutting DOWN! TASKS")
                network_manager.disconnect()
                break


def options_from_config(filename='config.json'):
    """ Options from file config.json """

    with open(filename) as f:
        config_options = json.load(f)

    return config_options


if __name__ == '__main__':

    usage = '%prog [options] '
    parser = OptionParser(usage=usage)

    parser.add_option('-n', '--connection_network', action='store', dest='connection_network', type="string",
                      help='network to connect')

    parser.add_option('-e', '--config_network', action='store', dest='config_network', type="string",
                      help='enviroment to connect')

    parser.add_option('-c', '--config', action='store', dest='config', type="string",
                      help='path to config')

    (options, args) = parser.parse_args()

    if 'APP_CONFIG' in os.environ:
        config = json.loads(os.environ['APP_CONFIG'])
    else:
        if not options.config:
            # if there are no config try to read config.json from current folder
            config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.json')
            if not os.path.isfile(config_path):
                raise Exception("Please select path to config or env APP_CONFIG. "
                                "Ex. /enviroments/moc-testnet/config.json "
                                "Full Ex.:"
                                "python moc_jobs.py "
                                "--connection_network=rskTestnetPublic "
                                "--config_network=mocTestnet "
                                "--config ./enviroments/moc-testnet/config.json"
                                )
        else:
            config_path = options.config

        config = options_from_config(config_path)

    if 'APP_CONNECTION_NETWORK' in os.environ:
        connection_network = os.environ['APP_CONNECTION_NETWORK']
    else:
        if not options.connection_network:
            raise Exception("Please select connection network or env APP_CONNECTION_NETWORK. "
                            "Ex.: rskTesnetPublic. "
                            "Full Ex.:"
                            "python moc_jobs.py "
                            "--connection_network=rskTestnetPublic "
                            "--config_network=mocTestnet "
                            "--config ./enviroments/moc-testnet/config.json")
        else:
            connection_network = options.connection_network

    if 'APP_CONFIG_NETWORK' in os.environ:
        config_network = os.environ['APP_CONFIG_NETWORK']
    else:
        if not options.config_network:
            raise Exception("Please select enviroment of your config or env APP_CONFIG_NETWORK. "
                            "Ex.: rdocTestnetAlpha"
                            "Full Ex.:"
                            "python moc_jobs.py "
                            "--connection_network=rskTestnetPublic "
                            "--config_network=mocTestnet "
                            "--config ./enviroments/moc-testnet/config.json"
                            )
        else:
            config_network = options.config_network

    jm = JobsManager(config, config_network, connection_network)
    jm.time_loop_start()
