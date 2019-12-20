import os
from optparse import OptionParser
import datetime
import json

from timeloop import Timeloop
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


class ContractManager(NodeManager):

    def __init__(self, config_options, network_nm):
        self.options = config_options
        self.MoCState = None
        self.MoC = None
        super().__init__(options=config_options, network=network_nm)

    def connect_contract(self):
        self.connect_node()
        self.load_contracts()

    def load_contracts(self):

        path_build = self.options['build_dir']
        address_moc_state = self.options['networks'][network]['addresses']['MoCState']
        address_moc = self.options['networks'][network]['addresses']['MoC']

        self.MoCState = self.load_json_contract(os.path.join(path_build, "MoCState.json"),
                                                deploy_address=address_moc_state)
        self.MoC = self.load_json_contract(os.path.join(path_build, "MoC.json"),
                                           deploy_address=address_moc)

    def contract_liquidation(self):

        self.connect_contract()

        partial_execution_steps = self.options['partial_execution_steps']

        log.info("Calling isLiquidationReached ..")
        is_liquidation_reached = self.MoCState.functions.isLiquidationReached().call()
        if is_liquidation_reached:
            log.info("Calling evalLiquidation steps [{0}] ...".format(partial_execution_steps))
            tx_hash = self.fnx_transaction(self.MoC, 'evalLiquidation', partial_execution_steps)
            tx_receipt = self.wait_transaction_receipt(tx_hash)
            log.debug(tx_receipt)
            block_number = self.block_number
            log.info("Successfully forced Liquidation in Block [{0}]".format(block_number))
        else:
            log.info("No liquidation reached!")

    def contract_bucket_liquidation(self):

        partial_execution_steps = self.options['partial_execution_steps']

        log.info("Calling isBucketLiquidationReached ..")
        is_bucket_liquidation_reached = self.MoC.functions.isBucketLiquidationReached(str.encode('X2')).call()
        if is_bucket_liquidation_reached:
            log.info("Calling evalBucketLiquidation steps [{0}] ...".format(partial_execution_steps))
            tx_hash = self.fnx_transaction(self.MoC, 'evalBucketLiquidation', str.encode('X2'))
            tx_receipt = self.wait_transaction_receipt(tx_hash)
            log.debug(tx_receipt)
            block_number = self.block_number
            log.info("Successfully Bucket X2 Liquidation in Block [{0}]".format(block_number))
        else:
            log.info("No liquidation reached!")

    def contract_run_settlement(self):

        partial_execution_steps = self.options['partial_execution_steps']
        log.info("Calling isSettlementEnabled ..")
        is_settlement_enabled = self.MoC.functions.isSettlementEnabled().call()
        if is_settlement_enabled:
            log.info("Calling runSettlement steps [{0}] ...".format(partial_execution_steps))
            tx_hash = self.fnx_transaction(self.MoC, 'runSettlement', partial_execution_steps)
            tx_receipt = self.wait_transaction_receipt(tx_hash)
            log.debug(tx_receipt)
            block_number = self.block_number
            log.info("Successfully runSettlement in Block [{0}]".format(block_number))
        else:
            log.info("No settlement reached!")

    def contract_daily_inrate_payment(self):

        log.info("Calling isDailyEnabled ...")
        is_daily_enabled = self.MoC.functions.isDailyEnabled().call()
        if is_daily_enabled:
            log.info("Calling dailyInratePayment ...")
            tx_hash = self.fnx_transaction(self.MoC, 'dailyInratePayment')
            tx_receipt = self.wait_transaction_receipt(tx_hash)
            log.debug(tx_receipt)
            block_number = self.block_number
            log.info("Successfully dailyInratePayment in Block [{0}]".format(block_number))
        else:
            log.info("No isDailyEnabled reached!")

    def contract_pay_bitpro_holders(self):

        log.info("Calling isBitProInterestEnabled ...")
        is_bitpro_enabled = self.MoC.functions.isBitProInterestEnabled().call()
        if is_bitpro_enabled:
            log.info("Calling payBitProHoldersInterestPayment ...")
            tx_hash = self.fnx_transaction(self.MoC, 'payBitProHoldersInterestPayment')
            tx_receipt = self.wait_transaction_receipt(tx_hash)
            log.debug(tx_receipt)
            block_number = self.block_number
            log.info("Successfully payBitProHoldersInterestPayment in Block [{0}]".format(block_number))
        else:
            log.info("No isBitProInterestEnabled reached!")

    def contract_calculate_bma(self):

        log.info("Calling shouldCalculateEma ...")
        is_ema_enabled = self.MoCState.functions.shouldCalculateEma().call()
        if is_ema_enabled:
            log.info("Calling calculateBitcoinMovingAverage ...")
            tx_hash = self.fnx_transaction(self.MoCState, 'calculateBitcoinMovingAverage')
            tx_receipt = self.wait_transaction_receipt(tx_hash)
            log.debug(tx_receipt)
            block_number = self.block_number
            log.info("Successfully calculateBitcoinMovingAverage in Block [{0}]".format(block_number))
        else:
            log.info("No shouldCalculateEma reached!")


class JobsManager:

    def __init__(self, moc_jobs_config, network_nm, build_dir_nm):

        self.tl = Timeloop()

        self.options = moc_jobs_config
        self.options['build_dir'] = build_dir_nm

        self.cm = ContractManager(self.options, network_nm)
        self.cm.set_log(log)

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

    def contracts_tasks(self):

        try:
            self.cm.contract_liquidation()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

        try:
            self.cm.contract_bucket_liquidation()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

        try:
            self.cm.contract_run_settlement()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

        try:
            self.cm.contract_daily_inrate_payment()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

        try:
            self.cm.contract_pay_bitpro_holders()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

        try:
            self.cm.contract_calculate_bma()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

    def schedule_jobs(self):

        try:
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


def options_from_config(filename='config.json'):
    """ Options from file config.json """

    with open(filename) as f:
        config_options = json.load(f)

    return config_options


if __name__ == '__main__':

    usage = '%prog [options] '
    parser = OptionParser(usage=usage)

    parser.add_option('-n', '--network', action='store', dest='network', type="string", help='network')

    parser.add_option('-c', '--config', action='store', dest='config', type="string", help='config')

    parser.add_option('-b', '--build', action='store', dest='build', type="string", help='build')

    (options, args) = parser.parse_args()

    if 'MOC_JOBS_CONFIG' in os.environ:
        config = json.loads(os.environ['MOC_JOBS_CONFIG'])
    else:
        if not options.config:
            config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.json')
        else:
            config_path = options.config

        config = options_from_config(config_path)

    if 'MOC_JOBS_NETWORK' in os.environ:
        network = os.environ['MOC_JOBS_NETWORK']
    else:
        if not options.network:
            network = 'mocTestnetAlpha'
        else:
            network = options.network

    if not options.build:
        build_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'build')
    else:
        build_dir = options.build

    jm = JobsManager(config, network, build_dir)
    jm.time_loop_start()
