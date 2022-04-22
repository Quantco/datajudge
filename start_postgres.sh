#!/bin/bash

set -e

docker run -e POSTGRES_DB=datajudge -e POSTGRES_USER=datajudge -e POSTGRES_PASSWORD=datajudge -p 5432:5432 postgres:11
