#!/bin/bash

# exit as soon as an error happen
set -e

usage() { echo "Usage: $0 -e <environment> -c <config file> -i <aws id> -r <aws region>" 1>&2; exit 1; }

while getopts ":e:c:i:r:" o; do
    case "${o}" in
        e)
            e=${OPTARG}
             ((e == "ec2_alphatestnet" || e=="ec2_testnet" || e=="ec2_mainnet" )) || usage
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
                *)
                    usage
                    ;;
            esac
            ;;
        c)
            c=${OPTARG}
            CONFIG_FILE=$c
            ;;
        i)
            i=${OPTARG}
            AWS_ID=$i
            ;;
        r)
            r=${OPTARG}
            AWS_REGION=$r
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${e}" ] || [ -z "${c}" ] || [ -z "${i}" ] || [ -z "${r}" ]; then
    usage
fi

docker image build -t moc_jobs_$ENV -f Dockerfile --build-arg CONFIG=$CONFIG_FILE .

echo "Build done!"

# login into aws ecr
$(aws ecr get-login --no-include-email --region $AWS_REGION)

echo "Logging to AWS done!"

docker tag moc_jobs_$ENV:latest $AWS_ID.dkr.ecr.$AWS_REGION.amazonaws.com/moc_jobs_$ENV:latest

docker push $AWS_ID.dkr.ecr.$AWS_REGION.amazonaws.com/moc_jobs_$ENV:latest

echo "Done!"
