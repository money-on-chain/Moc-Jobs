import os
from optparse import OptionParser
import datetime
import json

from timeloop import Timeloop
import boto3
import time
from pymongo import MongoClient
from collections import OrderedDict
import pprint

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

"""
Search and update pending transactions status.

const findEvents = web3 => (tx, eventName, eventArgs) => {
  const txLogs = decodeLogs(tx);
  const logs = txLogs.filter(log => log && log.name === eventName);
  const events = logs.map(log => transformEvent(web3, log, tx));

  // Filter
  if (eventArgs) {
    return events.filter(ev => Object.entries(eventArgs).every(([k, v]) => ev[k] === v));
  }

  return events;
};

module.findEvents = async (transactionHash, eventName, eventArgs) => {
    const txReceipt = await web3.eth.getTransactionReceipt(transactionHash);
    return decoder.findEvents(txReceipt, eventName, eventArgs);
  };

const processTransaction = async (nodeManager, tx) => {
  const { transactionHash } = tx;
  const txReceipt = await nodeManager.getTransactionReceipt(transactionHash);
  const { status } = txReceipt;
  logInfo({ message: `Transaction receipt for hash: ${transactionHash}.`, data: txReceipt });
  if (status === true) {
    const { event } = tx;
    const [eventObject] = await nodeManager.findEvents(transactionHash, event);
    if (eventObject) {
      return updateTransactions(nodeManager, eventObject, false);
    }
    logWarn({
      message: `Transaction receipt with hash: ${transactionHash} does not contain logs.`,
      data: txReceipt
    });
  }
  return updateFailedTransactions(transactionHash);
};


const updatePendingTransactions = async nodeManager => {
  // TODO: change query when exist isMined state
  const txs = Transaction.find({
    status: commonTxStates.pending,
    confirmationTime: { $not: { $exists: true } }
  }).fetch();

  const promises = txs.map(tx => processTransaction(nodeManager, tx));
  return Promise.all(promises);
};

"""


class ContractManager(NodeManager):

    def __init__(self, config_options, network_nm):
        self.options = config_options
        self.MoCState = None
        self.MoCInrate = None
        self.MoC = None
        super().__init__(options=config_options, network=network_nm)

    def connect_contract(self):
        self.connect_node()
        self.load_contracts()

    def connect_mongo(self):

        mongo_uri = self.options['mongo_uri']
        client = MongoClient(mongo_uri)

        return client

    def load_contracts(self):

        path_build = self.options['build_dir']
        address_moc_state = self.options['networks'][network]['addresses']['MoCState']
        address_moc_inrate = self.options['networks'][network]['addresses']['MoCInrate']
        address_moc = self.options['networks'][network]['addresses']['MoC']

        self.MoCState = self.load_json_contract(os.path.join(path_build, "MoCState.json"),
                                                deploy_address=address_moc_state)
        self.MoCInrate = self.load_json_contract(os.path.join(path_build, "MoCInrate.json"),
                                                 deploy_address=address_moc_inrate)
        self.MoC = self.load_json_contract(os.path.join(path_build, "MoC.json"),
                                           deploy_address=address_moc)

    def mongo_mocstate(self, client):

        mongo_db = self.options['mongo_db']
        db = client[mongo_db]
        collection = db['MocState']
        return collection

    def mongo_price(self, client):

        mongo_db = self.options['mongo_db']
        db = client[mongo_db]
        collection = db['Price']
        return collection

    def node_mocstate(self, lastUpdateHeight, dailyPriceRef):

        bucketX2 = str.encode('X2')
        bucketCero = str.encode('C0')

        d_mocstate = OrderedDict()
        d_mocstate["bitcoinPrice"] = str(self.MoCState.functions.getBitcoinPrice().call())
        d_mocstate["bproAvailableToMint"] = str(self.MoCState.functions.maxMintBProAvalaible().call())
        d_mocstate["bproAvailableToRedeem"] = str(self.MoCState.functions.absoluteMaxBPro().call())
        d_mocstate["bprox2AvailableToMint"] = str(self.MoCState.functions.maxBProx(bucketX2).call())
        d_mocstate["docAvailableToMint"] = str(self.MoCState.functions.absoluteMaxDoc().call())
        d_mocstate["docAvailableToRedeem"] = str(self.MoCState.functions.freeDoc().call())
        d_mocstate["b0Leverage"] = str(self.MoCState.functions.leverage(bucketCero).call())
        d_mocstate["b0TargetCoverage"] = str(self.MoCState.functions.cobj().call())
        d_mocstate["x2Leverage"] = str(self.MoCState.functions.leverage(bucketX2).call())
        d_mocstate["totalBTCAmount"] = str(self.MoCState.functions.rbtcInSystem().call())
        d_mocstate["bitcoinMovingAverage"] = str(self.MoCState.functions.getBitcoinMovingAverage().call())
        d_mocstate["b0BTCInrateBag"] = str(self.MoCState.functions.getInrateBag(bucketCero).call())
        d_mocstate["b0BTCAmount"] = str(self.MoCState.functions.getBucketNBTC(bucketCero).call())
        d_mocstate["b0DocAmount"] = str(self.MoCState.functions.getBucketNDoc(bucketCero).call())
        d_mocstate["b0BproAmount"] = str(self.MoCState.functions.getBucketNBPro(bucketCero).call())
        d_mocstate["x2BTCAmount"] = str(self.MoCState.functions.getBucketNBTC(bucketX2).call())
        d_mocstate["x2DocAmount"] = str(self.MoCState.functions.getBucketNDoc(bucketX2).call())
        d_mocstate["x2BproAmount"] = str(self.MoCState.functions.getBucketNBPro(bucketX2).call())
        d_mocstate["globalCoverage"] = str(self.MoCState.functions.globalCoverage().call())
        d_mocstate["reservePrecision"] = self.MoC.functions.getReservePrecision().call()
        d_mocstate["mocPrecision"] = self.MoC.functions.getMocPrecision().call()
        d_mocstate["x2Coverage"] = str(self.MoCState.functions.coverage(bucketX2).call())
        d_mocstate["bproPriceInRbtc"] = str(self.MoCState.functions.bproTecPrice().call())
        d_mocstate["bproPriceInUsd"] = str(self.MoCState.functions.bproUsdPrice().call())
        d_mocstate["bproDiscountRate"] = str(self.MoCState.functions.bproSpotDiscountRate().call())
        d_mocstate["maxBproWithDiscount"] = str(self.MoCState.functions.maxBProWithDiscount().call())
        d_mocstate["bproDiscountPrice"] = str(self.MoCState.functions.bproDiscountPrice().call())
        d_mocstate["bprox2PriceInRbtc"] = str(self.MoCState.functions.bucketBProTecPrice(bucketX2).call())
        d_mocstate["bprox2PriceInBpro"] = str(self.MoCState.functions.bproxBProPrice(bucketX2).call())
        d_mocstate["spotInrate"] = str(self.MoCInrate.functions.spotInrate().call())
        d_mocstate["commissionRate"] = str(self.MoCInrate.functions.getCommissionRate().call())
        d_mocstate["bprox2PriceInUsd"] = str(int(d_mocstate["bprox2PriceInRbtc"]) * int(d_mocstate["bitcoinPrice"]) / int(d_mocstate["reservePrecision"]))
        d_mocstate["lastUpdateHeight"] = lastUpdateHeight
        d_mocstate["createdAt"] = datetime.datetime.now()
        d_mocstate["dayBlockSpan"] = self.MoCState.functions.dayBlockSpan().call()
        d_mocstate["blocksToSettlement"] = self.MoCState.functions.blocksToSettlement().call()
        d_mocstate["state"] = self.MoCState.functions.state().call()
        d_mocstate["lastPriceUpdateHeight"] = 0
        d_mocstate["priceVariation"] = dailyPriceRef
        d_mocstate["paused"] = self.MoC.functions.paused().call()

        return d_mocstate

    def update_mongo(self):

        self.connect_contract()

        lastUpdateHeight = self.block_number
        blockDailySpan = self.MoCState.functions.getDayBlockSpan().call()
        blockHeight = lastUpdateHeight - blockDailySpan

        mongo_client = self.connect_mongo()

        dailyPriceRef = self.get_price_first_older(mongo_client, blockHeight)

        self.update_mocstate(mongo_client, lastUpdateHeight, dailyPriceRef)

    def get_price_first_older(self, mongo_client, lastUpdateHeight):

        m_price = self.mongo_price(mongo_client)
        result = m_price.find_one(filter={"blockHeight": {"$lt": lastUpdateHeight}}, sort=[("blockHeight", -1)])
        return result

    def update_mocstate(self, mongo_client, lastUpdateHeight, dailyPriceRef):

        m_mocstate = self.mongo_mocstate(mongo_client)
        n_mocstate = self.node_mocstate(lastUpdateHeight, dailyPriceRef)

        result = m_mocstate.find_one_and_update(
            {},
            {"$set": n_mocstate},
            upsert=True)

        return result

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

    def update_mongo(self):
        """Update mocstate"""

        self.cm.update_mongo()


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
            network = 'mocGanacheDesktop'
        else:
            network = options.network

    if not options.build:
        build_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'build')
    else:
        build_dir = options.build

    jm = JobsManager(config, network, build_dir)
    #jm.time_loop_start()
    jm.update_mongo()
