#!/bin/bash

set -e

docker run -e POSTGRES_DB=dbcheck -e POSTGRES_USER=dbcheck -e POSTGRES_PASSWORD=dbcheck -p 5432:5432 postgres:11
