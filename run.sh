#!/bin/bash
docker run  -d \
    --env-file=price_feeder.env \
    --env PK_SECRET=987fd790c4998141d3c28a07dd80c168247ddfe9c75835d45f7b4d79020026db \
    moc_jobs_alpha-testnet