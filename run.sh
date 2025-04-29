#!/bin/bash

#docker image rm harmony-filtering:latest


# Build the Docker image
#docker build --no-cache -t harmony-filtering:latest .
docker build --no-cache -t harmony/filtering:latest -f docker/service.Dockerfile .
#docker build -t harmony-filtering:latest .
#docker build -t harmony/filtering:latest -f docker/service.Dockerfile .


# Run the Docker container
docker run --rm \
  -e "ENV=dev" \
  -e "OAUTH_REDIRECT_URI=http://localhost:3000/oauth2/redirect" \
  -e "STAGING_BUCKET=example-bucket" \
  -e "STAGING_PATH=public/some-org/some-service/some-uuid/" \
  -e "SHARED_SECRET_KEY=a1599ab6b94269c1d61ca400376b2b94" \
  -e "OAUTH_CLIENT_ID=EPbQoi8pVrMpExOgS8BQ_g" \
  -e "OAUTH_UID=localharmonyservice" \
  -e "OAUTH_PASSWORD=LocalHarmonyls -lt" \
  -v "/Users/srizvi/filtering_data:/data" \
  -v "/Users/srizvi/filtering_output:/tmp" \
  harmony/filtering:latest \
    --harmony-action invoke \
    --harmony-input-file    /data/message.json \
    --harmony-sources       /data/catalog.json \
    --harmony-metadata-dir  /tmp \
    --harmony-data-location /data





