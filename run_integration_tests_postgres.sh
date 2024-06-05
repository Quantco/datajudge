#!/bin/bash

docker stop $(docker ps -q --filter name=postgres_datajudge)

./start_postgres.sh &
bash -c "while true; do printf '\nPress enter to once postgres is ready: '; sleep 1; done" & 

read -p "Press enter to once postgres is ready: "
kill %%

echo "STARTING PYTEST"
pytest tests/integration -vv  --backend=postgres "$@"

docker stop $(docker ps -q --filter name=postgres_datajudge)

