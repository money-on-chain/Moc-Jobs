#!/bin/bash

# exit as soon as an error happen
set -e

usage() { echo "Usage: $0 -e <environment> -c <config file> -i <aws id>" 1>&2; exit 1; }

while getopts ":e:c:i:" o; do
    case "${o}" in
        e)
            e=${OPTARG}
             ((e == "ec2_tyd" || e == "ec2_alphatestnet" || e=="ec2_testnet" || e=="ec2_mainnet" || e=="ec2_rdoc_alphatestnet" || e=="ec2_rdoc_testnet" || e=="ec2_rdoc_mainnet")) || usage
            case $e in
                ec2_tyd)
                    ENV=$e
                    ;;
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
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${e}" ] || [ -z "${c}" ] || [ -z "${i}" ]; then
    usage
fi

docker image build -t moc_jobs_$ENV -f Dockerfile --build-arg CONFIG=$CONFIG_FILE .

echo "Build done!"

REGION="us-west-1"

# login into aws ecr
$(aws ecr get-login --no-include-email --region $REGION)

echo "Logging to AWS done!"

docker tag moc_jobs_$ENV:latest $AWS_ID.dkr.ecr.$REGION.amazonaws.com/moc_jobs_$ENV:latest

docker push $AWS_ID.dkr.ecr.$REGION.amazonaws.com/moc_jobs_$ENV:latest

echo "Done!"
