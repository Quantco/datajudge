#!/bin/bash

set -e


if [[ "$(uname -m)" == "arm64" || "$(uname -m)" == "aarch64" ]]; then
  # run the azure-sql-edge image
  docker run \
    -e "ACCEPT_EULA=Y" \
    -e "MSSQL_SA_PASSWORD=QuantCo@MSSQL" \
    -e "MSSQL_USER=sa" \
    -p 1433:1433 \
    --name=mssql \
    --rm -it mcr.microsoft.com/azure-sql-edge
else
  # run the default image
  docker run \
    -e "ACCEPT_EULA=Y" \
    -e "SA_PASSWORD=QuantCo@MSSQL" \
    -p 1433:1433 \
    --name=mssql \
    --rm -it mcr.microsoft.com/mssql/server:2019-latest
fi
