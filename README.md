# Moc Jobs

This is a backend executor jobs. Periodic tasks that runs differents jobs, 
that call the contracts and asks if the are ready to execute it. This jobs 
run async of the app, and call directly to the contract througth node. 

### Currents jobs

 1. Contract liquidation
 2. Contract bucket liquidation
 3. Contract run settlement
 4. Contract daily inrate payment
 5. Contract pay bitpro holders
 6. Contract calculate EMA
 
 
### Usage

**Requirement and installation**
 
*  We need Python 3.6+

Install libraries

`pip install -r requirements.txt`

**Usage Job**

Make sure to change **config.json** to point to your network and
pricefeed.

`python moc_jobs.py`

Alternatives:

`python moc_jobs.py --config=path_to_config.json --network=local`

**--config:** Path to config.json 

**--network=local:** Network name in the json


**Usage Docker**

Build

```
docker build -t moc_jobs -f Dockerfile .
```

Run

```
docker run -d \
--name moc_jobs_1 \
--env ACCOUNT_ADDRESS=0x0e424e9a8598a55918e12de47172f3180c4b4e13 \
--env ACCOUNT_PK_SECRET=0x9e790b185e5b7f11f2924c7b809936866c38ed3ab3f33e0fbd3cfe791c2cdbd6 \
--env MOC_JOBS_NETWORK=local \
moc_jobs
```
  
 
### Jobs explain

Some of this tasks you can run directly from the app in https://alpha.moneyonchain.com/advanced
 
#### 1. Contract liquidation

check if liquidation is reached, if yes then run liquidation

**time every:** 10 min

**Call code:**

```
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

```

**Contract code:**

```
/**
  * @dev Evaluates if liquidation state has been reached and runs liq if that's the case
  */
  function evalLiquidation(uint256 steps) public {
    mocState.nextState();

    if (mocState.state() == MoCState.States.Liquidated) {
      liquidate();
      mocBurnout.executeBurnout(steps);
    }
  }
```


#### 2. Contract bucket liquidation

check if bucket liquidation is reached, if yes then run bucket liquidation.

**time every:** 10 min

**Call code:**

```
def contract_bucket_liquidation(self):

    partial_execution_steps = self.options['tasks']['bucket_liquidation']['partial_execution_steps']
    wait_timeout = self.options['tasks']['bucket_liquidation']['wait_timeout']
    gas_limit = self.options['tasks']['bucket_liquidation']['gas_limit']

    is_bucket_liquidation_reached = self.contract_MoC.functions.isBucketLiquidationReached(str.encode('X2')).call()
    is_settlement_enabled = self.contract_MoC.functions.isSettlementEnabled().call()
    if is_bucket_liquidation_reached and not is_settlement_enabled:
        log.info("Calling evalBucketLiquidation steps [{0}] ...".format(partial_execution_steps))
        tx_hash = self.fnx_transaction(self.contract_MoC, 'evalBucketLiquidation',
                                       str.encode('X2'),
                                       gas_limit=gas_limit)
        tx_receipt = self.wait_transaction_receipt(tx_hash, timeout=wait_timeout)
        log.info(tx_receipt)
        block_number = self.block_number
        log.info("Successfully Bucket X2 Liquidation in Block [{0}]".format(block_number))
    else:
        log.info("No bucket liquidation reached!")
```

**Contract code:**

```
function evalBucketLiquidation(bytes32 bucket) public availableBucket(bucket) notBaseBucket(bucket) {
    if (mocState.coverage(bucket) <= mocState.liq()) {
      bproxManager.liquidateBucket(bucket, BUCKET_C0);

      emit BucketLiquidation(bucket);
    }
  }
```


#### 3. Contract run settlement

check if settlement time is reached, if yes then run settlement.

**time every:** 10 min

**Call code:**

```
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

```

**Contract code:**

```
function evalBucketLiquidation(bytes32 bucket) public availableBucket(bucket) notBaseBucket(bucket) {
    if (mocState.coverage(bucket) <= mocState.liq()) {
      bproxManager.liquidateBucket(bucket, BUCKET_C0);

      emit BucketLiquidation(bucket);
    }
  }
```


#### 4. Contract daily inrate payment

check if daily inrate payment time is reached, if yes then run daily inrate payment.

**time every:** 10 min

**Call code:**

```
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
```

**Contract code:**

```
  /**
    @dev Moves the daily amount of interest rate to C0 bucket
  */
  function dailyInratePayment() public
  onlyWhitelisted(msg.sender) onlyOnceADay() returns(uint256) {
    uint256 toPay = dailyInrate();
    lastDailyPayBlock = block.number;

    if (toPay != 0) {
      bproxManager.deliverInrate(BUCKET_C0, toPay);
    }

    emit InrateDailyPay(toPay, mocState.daysToSettlement(), mocState.getBucketNBTC(BUCKET_C0));
  }
```


#### 5. Contract pay bitpro holders

check if  pay bitpro holders time is reached, if yes then run  bitpro holders payment

**time every:** 10 min

**Call code:**

```
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
```

**Contract code:**

```
  /**
  * @dev Pays the BitPro interest and transfers it to the address mocInrate.bitProInterestAddress
  * BitPro interests = Nb (bucket 0) * bitProRate.
  */
  function payBitProHoldersInterestPayment() public whenNotPaused() {
    uint256 toPay = mocInrate.payBitProHoldersInterestPayment();
    if (doSend(mocInrate.getBitProInterestAddress(), toPay)) {
      bproxManager.substractValuesFromBucket(BUCKET_C0, toPay, 0, 0);
    }
  }
```


#### 6. Contract calculate EMA

check if calculate ema time is reached, if yes then run  calculate bitcoin moving average



**Call code:**

```
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
```

**Contract code:**

```
  /** @dev Calculates a EMA of the price.
    * More information of EMA calculation https://en.wikipedia.org/wiki/Exponential_smoothing
    * @param btcPrice Current price.
    */
  function setBitcoinMovingAverage(uint256 btcPrice) internal {
    if (shouldCalculateEma()) {
      uint256 weightedPrice = btcPrice.mul(smoothingFactor);
      uint256 currentEma = bitcoinMovingAverage.mul(coefficientComp()).add(weightedPrice)
        .div(FACTOR_PRECISION);

      lastEmaCalculation = block.number;
      bitcoinMovingAverage = currentEma;

      emit MovingAverageCalculation(btcPrice, currentEma);
    }
  }
```
