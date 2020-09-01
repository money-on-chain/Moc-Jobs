import os
from optparse import OptionParser
import datetime
import json

from timeloop import Timeloop
import boto3
import time

from moneyonchain.manager import ConnectionManager
from moneyonchain.rdoc import RDOCMoC, RDOCMoCMedianizer
from moneyonchain.moc import MoC, MoCMedianizer

import logging
import logging.config


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger('default')


class JobsManager:

    def __init__(self, moc_jobs_config, network_nm):

        self.options = moc_jobs_config
        self.network = network_nm
        self.connection_manager = ConnectionManager(options=moc_jobs_config, network=network_nm)
        self.app_mode = self.options['networks'][network]['app_mode']

        if self.app_mode == 'RRC20':
            self.contract_MoC = RDOCMoC(self.connection_manager, contracts_discovery=True)
            self.contract_MoCState = self.contract_MoC.sc_moc_state
            self.contract_MoCMedianizer = RDOCMoCMedianizer(self.connection_manager,
                                                            contract_address=self.contract_MoCState.price_provider())
        elif self.app_mode == 'MoC':
            self.contract_MoC = MoC(self.connection_manager, contracts_discovery=True)
            self.contract_MoCState = self.contract_MoC.sc_moc_state
            self.contract_MoCMedianizer = MoCMedianizer(self.connection_manager,
                                                        contract_address=self.contract_MoCState.price_provider())
        else:
            raise Exception("Not valid APP Mode")

        self.tl = Timeloop()

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
        wait_timeout = self.options['tasks']['liquidation']['wait_timeout']
        gas_limit = self.options['tasks']['liquidation']['gas_limit']

        tx_hash, tx_receipt = self.contract_MoC.execute_liquidation(partial_execution_steps,
                                                                    gas_limit=gas_limit,
                                                                    wait_timeout=wait_timeout)

        if not tx_hash:
            log.info("NO: liquidation!")

    def contract_bucket_liquidation(self):

        wait_timeout = self.options['tasks']['bucket_liquidation']['wait_timeout']
        gas_limit = self.options['tasks']['bucket_liquidation']['gas_limit']

        tx_hash, tx_receipt = self.contract_MoC.execute_bucket_liquidation(gas_limit=gas_limit,
                                                                           wait_timeout=wait_timeout)

        if not tx_hash:
            log.info("NO: bucket liquidation!")

    def contract_run_settlement(self):

        partial_execution_steps = self.options['tasks']['run_settlement']['partial_execution_steps']
        wait_timeout = self.options['tasks']['run_settlement']['wait_timeout']
        gas_limit = self.options['tasks']['run_settlement']['gas_limit']

        tx_hash, tx_receipt = self.contract_MoC.execute_run_settlement(partial_execution_steps,
                                                                       gas_limit=gas_limit,
                                                                       wait_timeout=wait_timeout)

        if not tx_hash:
            log.info("NO: runSettlement!")

    def contract_daily_inrate_payment(self):

        wait_timeout = self.options['tasks']['daily_inrate_payment']['wait_timeout']
        gas_limit = self.options['tasks']['daily_inrate_payment']['gas_limit']

        tx_hash, tx_receipt = self.contract_MoC.execute_daily_inrate_payment(gas_limit=gas_limit,
                                                                             wait_timeout=wait_timeout)

        if not tx_hash:
            log.info("NO: dailyInratePayment!")

    def contract_pay_bitpro_holders(self):

        wait_timeout = self.options['tasks']['pay_bitpro_holders']['wait_timeout']
        gas_limit = self.options['tasks']['pay_bitpro_holders']['gas_limit']

        tx_hash, tx_receipt = self.contract_MoC.execute_pay_bitpro_holders(gas_limit=gas_limit,
                                                                           wait_timeout=wait_timeout)

        if not tx_hash:
            log.info("NO: payBitProHoldersInterestPayment!")

    def contract_calculate_bma(self):

        wait_timeout = self.options['tasks']['calculate_bma']['wait_timeout']
        gas_limit = self.options['tasks']['calculate_bma']['gas_limit']

        tx_hash, tx_receipt = self.contract_MoC.execute_calculate_ema(gas_limit=gas_limit,
                                                                      wait_timeout=wait_timeout)

        if not tx_hash:
            log.info("NO: calculateBitcoinMovingAverage!")

    def contract_oracle_poke(self):

        wait_timeout = self.options['tasks']['oracle_poke']['wait_timeout']
        gas_limit = self.options['tasks']['oracle_poke']['gas_limit']

        tx_hash = None
        tx_receipt = None
        if not self.contract_MoCMedianizer.compute()[1] and self.contract_MoCMedianizer.peek()[1]:
            tx_hash, tx_receipt = self.contract_MoCMedianizer.poke(gas_limit=gas_limit,
                                                                   wait_timeout=wait_timeout)
            log.error("[POKE] Not valid price! Disabling MOC Price!")
            self.aws_put_metric_heart_beat(1)
        else:
            log.info("NO: oracle Poke!")

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

    jm = JobsManager(config, network)
    jm.time_loop_start()
