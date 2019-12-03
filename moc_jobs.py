import os
from optparse import OptionParser
import datetime
import json

from timeloop import Timeloop
from web3 import Web3
import boto3
import time

# local imports
from node_manager import NodeManager

import logging
import logging.config


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='logs/moc_jobs.log',
                    filemode='w')
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)

log = logging.getLogger('default')


class ContractManager:
    MoCState = None
    MoC = None


class JobsManager:

    tl = Timeloop()

    def __init__(self, path_to_config, network_nm, build_dir_nm):

        config_options = self.options_from_config(path_to_config)
        config_options['build_dir'] = build_dir_nm
        self.options = config_options

        self.nm = NodeManager(options=config_options, network=network_nm)
        self.nm.set_log(log)

        self.cm = ContractManager()

    @staticmethod
    def options_from_config(filename='config.json'):
        """ Options from file config.json """

        with open(filename) as f:
            config_options = json.load(f)

        return config_options

    @staticmethod
    def aws_put_metric_heart_beat(value):
        # Create CloudWatch client
        cloudwatch = boto3.client('cloudwatch')

        # Put custom metrics
        cloudwatch.put_metric_data(
            MetricData=[
                {
                    'MetricName': os.environ['MOC_JOBS_NAME'],
                    'Dimensions': [
                        {
                            'Name': 'MoCJobs',
                            'Value': 'Status'
                        },
                    ],
                    'Unit': 'None',
                    'Value': value
                },
            ],
            Namespace='MOC/JOBS'
        )

    def connect(self):
        self.nm.connect_node()
        self.load_contracts()

    def load_contracts(self):

        path_build = self.options['build_dir']
        address_moc_state = self.options['networks'][network]['addresses']['MoCState']
        address_moc = self.options['networks'][network]['addresses']['MoC']

        self.cm.MoCState = self.nm.load_json_contract(os.path.join(path_build, "MoCState.json"),
                                                      deploy_address=address_moc_state)
        self.cm.MoC = self.nm.load_json_contract(os.path.join(path_build, "MoC.json"),
                                                 deploy_address=address_moc)

    def contract_liquidation(self):

        partial_execution_steps = self.options['partial_execution_steps']

        log.info("Calling isLiquidationReached ..")
        is_liquidation_reached = self.cm.MoCState.functions.isLiquidationReached().call()
        if is_liquidation_reached:
            log.info("Calling evalLiquidation steps [{0}] ...".format(partial_execution_steps))
            tx_hash = self.nm.fnx_transaction(self.cm.MoC, 'evalLiquidation', partial_execution_steps)
            tx_receipt = self.nm.wait_transaction_receipt(tx_hash)
            log.debug(tx_receipt)
            block_number = self.nm.block_number
            log.info("Successfully forced Liquidation in Block [{0}]".format(block_number))
        else:
            log.info("No liquidation reached!")

    def contract_bucket_liquidation(self):

        partial_execution_steps = self.options['partial_execution_steps']

        log.info("Calling isBucketLiquidationReached ..")
        is_bucket_liquidation_reached = self.cm.MoC.functions.isBucketLiquidationReached(str.encode('X2')).call()
        if is_bucket_liquidation_reached:
            log.info("Calling evalBucketLiquidation steps [{0}] ...".format(partial_execution_steps))
            tx_hash = self.nm.fnx_transaction(self.cm.MoC, 'evalBucketLiquidation', str.encode('X2'))
            tx_receipt = self.nm.wait_transaction_receipt(tx_hash)
            log.debug(tx_receipt)
            block_number = self.nm.block_number
            log.info("Successfully Bucket X2 Liquidation in Block [{0}]".format(block_number))
        else:
            log.info("No liquidation reached!")

    def contract_run_settlement(self):

        partial_execution_steps = self.options['partial_execution_steps']
        log.info("Calling isSettlementEnabled ..")
        is_settlement_enabled = self.cm.MoC.functions.isSettlementEnabled().call()
        if is_settlement_enabled:
            log.info("Calling runSettlement steps [{0}] ...".format(partial_execution_steps))
            tx_hash = self.nm.fnx_transaction(self.cm.MoC, 'runSettlement', partial_execution_steps)
            tx_receipt = self.nm.wait_transaction_receipt(tx_hash)
            log.debug(tx_receipt)
            block_number = self.nm.block_number
            log.info("Successfully runSettlement in Block [{0}]".format(block_number))
        else:
            log.info("No settlement reached!")

    def contract_daily_inrate_payment(self):

        log.info("Calling isDailyEnabled ...")
        is_daily_enabled = self.cm.MoC.functions.isDailyEnabled().call()
        if is_daily_enabled:
            log.info("Calling dailyInratePayment ...")
            tx_hash = self.nm.fnx_transaction(self.cm.MoC, 'dailyInratePayment')
            tx_receipt = self.nm.wait_transaction_receipt(tx_hash)
            log.debug(tx_receipt)
            block_number = self.nm.block_number
            log.info("Successfully dailyInratePayment in Block [{0}]".format(block_number))
        else:
            log.info("No isDailyEnabled reached!")

    def contract_pay_bitpro_holders(self):

        log.info("Calling isBitProInterestEnabled ...")
        is_bitpro_enabled = self.cm.MoC.functions.isBitProInterestEnabled().call()
        if is_bitpro_enabled:
            log.info("Calling payBitProHoldersInterestPayment ...")
            tx_hash = self.nm.fnx_transaction(self.cm.MoC, 'payBitProHoldersInterestPayment')
            tx_receipt = self.nm.wait_transaction_receipt(tx_hash)
            log.debug(tx_receipt)
            block_number = self.nm.block_number
            log.info("Successfully payBitProHoldersInterestPayment in Block [{0}]".format(block_number))
        else:
            log.info("No isBitProInterestEnabled reached!")

    def contract_calculate_bma(self):

        log.info("Calling shouldCalculateEma ...")
        is_ema_enabled = self.cm.MoCState.functions.shouldCalculateEma().call()
        if is_ema_enabled:
            log.info("Calling calculateBitcoinMovingAverage ...")
            tx_hash = self.nm.fnx_transaction(self.cm.MoCState, 'calculateBitcoinMovingAverage')
            tx_receipt = self.nm.wait_transaction_receipt(tx_hash)
            log.debug(tx_receipt)
            block_number = self.nm.block_number
            log.info("Successfully calculateBitcoinMovingAverage in Block [{0}]".format(block_number))
        else:
            log.info("No shouldCalculateEma reached!")

    def contracts_tasks(self):

        try:
            self.contract_liquidation()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

        try:
            self.contract_bucket_liquidation()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

        try:
            self.contract_run_settlement()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

        try:
            self.contract_daily_inrate_payment()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

        try:
            self.contract_pay_bitpro_holders()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

        try:
            self.contract_calculate_bma()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

    def schedule_jobs(self):

        try:
            self.connect()
            self.contracts_tasks()
            self.aws_put_metric_heart_beat(1)
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

    def add_jobs(self):

        self.tl._add_job(self.schedule_jobs, datetime.timedelta(seconds=self.options['interval']))

    def time_loop_start(self):

        self.add_jobs()
        #self.tl.start(block=True)
        self.tl.start()
        while True:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                self.tl.stop()
                break


if __name__ == '__main__':

    usage = '%prog [options] '
    parser = OptionParser(usage=usage)

    parser.add_option('-n', '--network', action='store', dest='network', type="string", help='network')

    parser.add_option('-c', '--config', action='store', dest='config', type="string", help='config')

    parser.add_option('-b', '--build', action='store', dest='build', type="string", help='build')

    (options, args) = parser.parse_args()

    if not options.config:
        config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'version', 'alpha-testnet', 'config.json')
    else:
        config_path = options.config

    if not options.network:
        network = 'local'
    else:
        network = options.network

    if not options.build:
        build_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'build')
    else:
        build_dir = options.build

    jm = JobsManager(config_path, network, build_dir)
    jm.time_loop_start()
