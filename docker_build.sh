#!/bin/bash

# exit as soon as an error happen
set -e

usage() { echo "Usage: $0 -e <environment> -c <config file> -i <aws id>" 1>&2; exit 1; }

while getopts ":e:c:i:" o; do
    case "${o}" in
        e)
            e=${OPTARG}
             ((e == "ec2_alphatestnet" || e=="ec2_testnet" || e=="ec2_mainnet" || e=="ec2_rdoc_alphatestnet" || e=="ec2_rdoc_testnet" || e=="ec2_rdoc_mainnet")) || usage
            case $e in
                ec2_alphatestnet)
                    ENV=$e
                    ;;
                ec2_testnet)
                    ENV=$e
                    ;;
                ec2_mainnet)
                    ENV=$e
                    ;;
                ec2_rdoc_alphatestnet)
                    ENV=$e
                    ;;
                ec2_rdoc_testnet)
                    ENV=$e
                    ;;
                ec2_rdoc_mainnet)
                    ENV=$e
                    ;;
                *)
                    usage
                    ;;
            esac
            ;;
        c)
            c=${OPTARG}
            CONFIG_FILE=$c
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${e}" ] || [ -z "${c}" ]; then
    usage
fi

docker image build -t moc_jobs_$ENV -f Dockerfile --build-arg CONFIG=$CONFIG_FILE .

echo "Build done!"