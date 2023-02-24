#!/bin/bash

set -e

docker run -itd --name mydb2 --privileged=true -p 50000:50000 -e LICENSE=accept -e DB2INST1_PASSWORD=password -e DBNAME=testdb -v ~/database ibmcom/db2
