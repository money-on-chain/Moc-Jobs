import os
from moneyonchain.networks import network_manager

import logging
import logging.config


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger('default')


def add_custom_network(connection_net):
    """ add custom network"""

    if connection_net.startswith("https") or connection_net.startswith("https"):
        a_connection = connection_net.split(',')
        host = a_connection[0]
        chain_id = a_connection[1]

        network_manager.add_network(
            network_name='rskCustomNetwork',
            network_host=host,
            network_chainid=chain_id,
            network_explorer='https://blockscout.com/rsk/mainnet/api',
            force=False
        )


if __name__ == '__main__':

    log.info("[ADD CUSTOM NETWORK] Init ...")
    if 'APP_CONNECTION_NETWORK' in os.environ:
        connection_network = os.environ['APP_CONNECTION_NETWORK']
        add_custom_network(connection_network)
        log.info("[ADD CUSTOM NETWORK] Added ...")
    log.info("[ADD CUSTOM NETWORK] End ...")
