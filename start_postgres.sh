#!/bin/bash

set -e

docker run --name postgres_datajudge --rm -e POSTGRES_DB=datajudge -e POSTGRES_USER=datajudge -e POSTGRES_PASSWORD=datajudge -p 5432:5432 postgres:11
