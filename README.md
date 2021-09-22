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
 7. Oracle Compute: Check expiration of price in Oracle.
 8. Execute Commission splitter
 
 
### Usage

**Requirement and installation**
 
*  We need Python 3.9+

Install libraries

`pip install -r requirements.txt`

**Brownie and node connection**

`pip install eth-brownie==1.16.2`

and to install connection nodes required to connect:

```
console> brownie networks add RskNetwork rskTesnetPublic host=https://public-node.testnet.rsk.co chainid=31 explorer=https://blockscout.com/rsk/mainnet/api
console> brownie networks add RskNetwork rskMainnetPublic host=https://public-node.rsk.co chainid=30 explorer=https://blockscout.com/rsk/mainnet/api
```

**Connection table**

| Network Name      | Network node          | Host                               | Chain    |
|-------------------|-----------------------|------------------------------------|----------|
| rskTestnetPublic   | RSK Testnet Public    | https://public-node.testnet.rsk.co | 31       |    
| rskTestnetLocal    | RSK Testnet Local     | http://localhost:4444              | 31       |
| rskMainnetPublic  | RSK Mainnet Public    | https://public-node.rsk.co         | 30       |
| rskMainnetLocal   | RSK Mainnet Local     | http://localhost:4444              | 30       |


**Usage Job**

Make sure to change **config.json** to point to your network.

`export ACCOUNT_PK_SECRET=(Your PK)`

`python app_run_moc_jobs.py --connection_network=rskTestnetPublic --config_network=mocTestnetAlpha --config ./enviroments/moc-alphatestnet2/config.json`

**--config:** Path to config.json 

**--config_network=mocTestnetAlpha:** This is enviroment we want to use

**--connection_network=rskTesnetPublic:** Connection Network name this is the label of brownie predefined connection or 
custom connection:`--connection_network=https://public-node.testnet.rsk.co,31` 


**Usage Docker**

Build, change path to correct enviroment

```
docker build -t moc_jobs -f Dockerfile --build-arg CONFIG=./enviroments/moc-alphatestnet2/config.json .
```

Run, replace ACCOUNT_PK_SECRET  with your private key owner of the account

```
docker run -d \
--name moc_jobs_1 \
--env ACCOUNT_PK_SECRET=asdfasdfasdf \
--env APP_CONNECTION_NETWORK=rskTestnetPublic \
--env APP_CONFIG_NETWORK=mocTestnetAlpha \
moc_jobs_ec2_alphatestnet
```

### Custom node

**APP_CONNECTION_NETWORK:** https://public-node.testnet.rsk.co,31
  
 
### Jobs explain

Some of this tasks you can run directly from the app in https://alpha.moneyonchain.com/advanced

