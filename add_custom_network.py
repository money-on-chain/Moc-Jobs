import os
import subprocess
from moneyonchain.networks import network_manager

import logging
import logging.config


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger('default')


def add_custom_network(connection_net,
                       network_group='live',
                       network_group_name="RskNetwork",
                       network_name='rskCustomNetwork',
                       network_host='https://public-node.testnet.rsk.co',
                       network_chainid='31',
                       network_explorer='https://blockscout.com/rsk/mainnet/api'
                       ):
    """ add custom network"""

    if connection_net.startswith("https") or connection_net.startswith("http"):
        log.info("Innn>>>")
        a_connection = connection_net.split(',')
        host = a_connection[0]
        chain_id = a_connection[1]

        # network_manager.add_network(
        #     network_name='rskCustomNetwork',
        #     network_host=host,
        #     network_chainid=chain_id,
        #     network_explorer='https://blockscout.com/rsk/mainnet/api',
        #     force=False
        # )

        subprocess.run(["brownie", "networks", "add",
                        network_group_name,
                        network_name,
                        "host={}".format(host),
                        "chainid={}".format(chain_id),
                        "explorer={}".format(network_explorer)])


if __name__ == '__main__':

    log.info("[ADD CUSTOM NETWORK] Init ...")
    if 'APP_CONNECTION_NETWORK' in os.environ:
        connection_network = os.environ['APP_CONNECTION_NETWORK']
        add_custom_network(connection_network)
        log.info("[ADD CUSTOM NETWORK] Added ...")
    log.info("[ADD CUSTOM NETWORK] End ...")
