import os
from optparse import OptionParser
import datetime
import json

from timeloop import Timeloop
import boto3
import time

# local imports
from contracts_manager import NodeManager

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
        self.contract_MoCState = None
        self.contract_MoC = None

        super().__init__(options=config_options, network=network_nm)
        self.connect_node()
        self.load_contracts()

    def load_contracts(self):

        path_build = self.options['build_dir']
        address_moc_state = self.options['networks'][network]['addresses']['MoCState']
        address_moc = self.options['networks'][network]['addresses']['MoC']

        self.contract_MoCState = self.load_json_contract(os.path.join(path_build, "MoCState.json"),
                                                         deploy_address=address_moc_state)
        self.contract_MoC = self.load_json_contract(os.path.join(path_build, "MoC.json"),
                                                    deploy_address=address_moc)

    def contract_liquidation(self):

        partial_execution_steps = self.options['tasks']['liquidation']['partial_execution_steps']
        wait_timeout = self.options['tasks']['liquidation']['wait_timeout']
        gas_limit = self.options['tasks']['liquidation']['gas_limit']

        is_liquidation_reached = self.contract_MoCState.functions.isLiquidationReached().call()
        if is_liquidation_reached:
            log.info("Calling evalLiquidation steps [{0}] ...".format(partial_execution_steps))
            tx_hash = self.fnx_transaction(self.contract_MoC, 'evalLiquidation',
                                           partial_execution_steps,
                                           gas_limit=gas_limit)
            tx_receipt = self.wait_transaction_receipt(tx_hash, timeout=wait_timeout)
            log.debug(tx_receipt)
            block_number = self.block_number
            log.info("Successfully forced Liquidation in Block [{0}]".format(block_number))
        else:
            log.info("No liquidation reached!")

    def contract_bucket_liquidation(self):

        partial_execution_steps = self.options['tasks']['bucket_liquidation']['partial_execution_steps']
        wait_timeout = self.options['tasks']['bucket_liquidation']['wait_timeout']
        gas_limit = self.options['tasks']['bucket_liquidation']['gas_limit']

        is_bucket_liquidation_reached = self.contract_MoC.functions.isBucketLiquidationReached(str.encode('X2')).call()
        if is_bucket_liquidation_reached:
            log.info("Calling evalBucketLiquidation steps [{0}] ...".format(partial_execution_steps))
            tx_hash = self.fnx_transaction(self.contract_MoC, 'evalBucketLiquidation',
                                           str.encode('X2'),
                                           gas_limit=gas_limit)
            tx_receipt = self.wait_transaction_receipt(tx_hash, timeout=wait_timeout)
            log.debug(tx_receipt)
            block_number = self.block_number
            log.info("Successfully Bucket X2 Liquidation in Block [{0}]".format(block_number))
        else:
            log.info("No bucket liquidation reached!")

    def contract_run_settlement(self):

        partial_execution_steps = self.options['tasks']['run_settlement']['partial_execution_steps']
        wait_timeout = self.options['tasks']['run_settlement']['wait_timeout']
        gas_limit = self.options['tasks']['run_settlement']['gas_limit']

        is_settlement_enabled = self.contract_MoC.functions.isSettlementEnabled().call()
        if is_settlement_enabled:
            log.info("Calling runSettlement steps [{0}] ...".format(partial_execution_steps))
            tx_hash = self.fnx_transaction(self.contract_MoC, 'runSettlement',
                                           partial_execution_steps,
                                           gas_limit=gas_limit)
            tx_receipt = self.wait_transaction_receipt(tx_hash, timeout=wait_timeout)
            log.debug(tx_receipt)
            block_number = self.block_number
            log.info("Successfully runSettlement in Block [{0}]".format(block_number))
        else:
            log.info("No settlement reached!")

    def contract_daily_inrate_payment(self):

        wait_timeout = self.options['tasks']['daily_inrate_payment']['wait_timeout']
        gas_limit = self.options['tasks']['daily_inrate_payment']['gas_limit']

        is_daily_enabled = self.contract_MoC.functions.isDailyEnabled().call()
        if is_daily_enabled:
            log.info("Calling dailyInratePayment ...")
            tx_hash = self.fnx_transaction(self.contract_MoC, 'dailyInratePayment', gas_limit=gas_limit)
            tx_receipt = self.wait_transaction_receipt(tx_hash, timeout=wait_timeout)
            log.debug(tx_receipt)
            block_number = self.block_number
            log.info("Successfully dailyInratePayment in Block [{0}]".format(block_number))
        else:
            log.info("No isDailyEnabled reached!")

    def contract_pay_bitpro_holders(self):

        wait_timeout = self.options['tasks']['pay_bitpro_holders']['wait_timeout']
        gas_limit = self.options['tasks']['pay_bitpro_holders']['gas_limit']

        if self.options['app_mode'] == 'RRC20':
            is_bitpro_enabled = self.contract_MoC.functions.isRiskProInterestEnabled().call()
            contract_function = 'payRiskProHoldersInterestPayment'
        else:
            is_bitpro_enabled = self.contract_MoC.functions.isBitProInterestEnabled().call()
            contract_function = 'payBitProHoldersInterestPayment'

        if is_bitpro_enabled:
            log.info("Calling payBitProHoldersInterestPayment ...")
            tx_hash = self.fnx_transaction(self.contract_MoC, contract_function, gas_limit=gas_limit)
            tx_receipt = self.wait_transaction_receipt(tx_hash, timeout=wait_timeout)
            log.debug(tx_receipt)
            block_number = self.block_number
            log.info("Successfully payBitProHoldersInterestPayment in Block [{0}]".format(block_number))
        else:
            log.info("No isBitProInterestEnabled reached!")

    def contract_calculate_bma(self):

        wait_timeout = self.options['tasks']['calculate_bma']['wait_timeout']
        gas_limit = self.options['tasks']['calculate_bma']['gas_limit']

        if self.options['app_mode'] == 'RRC20':
            contract_function = 'setExponentalMovingAverage'
        else:
            contract_function = 'calculateBitcoinMovingAverage'

        is_ema_enabled = self.contract_MoCState.functions.shouldCalculateEma().call()
        if is_ema_enabled:
            log.info("Calling calculateBitcoinMovingAverage ...")
            tx_hash = self.fnx_transaction(self.contract_MoCState, contract_function,
                                           gas_limit=gas_limit)

            tx_receipt = self.wait_transaction_receipt(tx_hash, timeout=wait_timeout)
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

    def task_run_settlement(self):

        try:
            self.cm.contract_run_settlement()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

    def task_liquidation(self):

        try:
            self.cm.contract_liquidation()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

    def task_bucket_liquidation(self):

        try:
            self.cm.contract_bucket_liquidation()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

    def task_daily_inrate_payment(self):

        try:
            self.cm.contract_daily_inrate_payment()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

    def task_pay_bitpro_holders(self):

        try:
            self.cm.contract_pay_bitpro_holders()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

    def task_calculate_bma(self):

        try:
            self.cm.contract_calculate_bma()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(0)

    def add_jobs(self):

        log.info("Starting adding jobs...")

        # run_settlement
        log.info("Jobs add run_settlement")
        interval = self.options['tasks']['run_settlement']['interval']
        self.tl._add_job(self.task_run_settlement, datetime.timedelta(seconds=interval))

        # liquidation
        log.info("Jobs add liquidation")
        interval = self.options['tasks']['liquidation']['interval']
        self.tl._add_job(self.task_liquidation, datetime.timedelta(seconds=interval))

        # bucket_liquidation
        log.info("Jobs add bucket_liquidation")
        interval = self.options['tasks']['bucket_liquidation']['interval']
        self.tl._add_job(self.task_bucket_liquidation, datetime.timedelta(seconds=interval))

        # daily_inrate_payment
        log.info("Jobs add daily_inrate_payment")
        interval = self.options['tasks']['daily_inrate_payment']['interval']
        self.tl._add_job(self.task_daily_inrate_payment, datetime.timedelta(seconds=interval))

        # pay_bitpro_holders
        log.info("Jobs add pay_bitpro_holders")
        interval = self.options['tasks']['pay_bitpro_holders']['interval']
        self.tl._add_job(self.task_pay_bitpro_holders, datetime.timedelta(seconds=interval))

        # calculate_bma
        log.info("Jobs add calculate_bma")
        interval = self.options['tasks']['calculate_bma']['interval']
        self.tl._add_job(self.task_calculate_bma, datetime.timedelta(seconds=interval))

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
