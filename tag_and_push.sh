# exit as soon as an error happen
set -e 

usage() { echo "Usage: $0 -e <environment>" 1>&2; exit 1; }

while getopts ":e:" o; do
    case "${o}" in
        e)
            e=${OPTARG}
            ((e == "moc-alphatestnet" || e == "moc-testnet" || e == "moc-mainnet" || e == "rdoc-mainnet" || e == "rdoc-testnet" || e == "rdoc-alpha-testnet" || e == "ec2_alphatestnet" || e=="ec2_testnet" || e=="ec2_mainnet" || e=="ec2_rdoc_alphatestnet" || e=="ec2_rdoc_testnet" || e=="ec2_rdoc_mainnet")) || usage
            case $e in
                moc-alphatestnet)
                    ENV=$e
                    ;;
                moc-testnet)      
                    ENV=$e
                    ;;
                moc-mainnet)      
                    ENV=$e
                    ;;
                rdoc-alpha-testnet)
                    ENV=$e
                    ;;
                rdoc-testnet)
                    ENV=$e
                    ;; 
                rdoc-mainnet)
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

if [ -z "${e}" ]; then
    usage
fi

# login into aws ecr
$(aws ecr get-login --no-include-email --region us-west-1)

docker tag moc_jobs_$ENV:latest 551471957915.dkr.ecr.us-west-1.amazonaws.com/moc_jobs_$ENV:latest
docker push 551471957915.dkr.ecr.us-west-1.amazonaws.com/moc_jobs_$ENV:latest