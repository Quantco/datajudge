#!/bin/bash

docker stop $(docker ps -q --filter name=postgres_datajudge)

./start_postgres.sh &
bash -c "while true; do printf '\nPress enter once postgres is ready: '; sleep 1; done" &

read -p "Press enter to once postgres is ready: "
kill %%

echo "STARTING PYTEST"
pixi run -e postgres-py38 test $@
docker stop $(docker ps -q --filter name=postgres_datajudge)
