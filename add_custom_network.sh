if  [[ $APP_CONNECTION_NETWORK == http* ]] || [[ $APP_CONNECTION_NETWORK == https* ]] ;
then
    arrConn=(${APP_CONNECTION_NETWORK//,/ })
    connHost=arrConn[0]
    connChainid=arrConn[1]
    echo connHost
    echo connChainid
    brownie networks add RskNetwork rskCustomNetwork host=$connHost chainid=$connChainid explorer=https://blockscout.com/rsk/mainnet/api
fi