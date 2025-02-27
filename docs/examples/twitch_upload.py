import os

import pandas as pd
import sqlalchemy as sa

df_v1 = pd.read_csv("twitch_version1.csv")
df_v2 = pd.read_csv("twitch_version2.csv")

# Upload tables to local Postgres instance.
# Run ``$ ./start_postgres.sh`` to make sure it is up and running.
address = os.environ.get("DB_ADDR", "localhost")
connection_string = f"postgresql://datajudge:datajudge@{address}:5432/datajudge"
engine = sa.create_engine(connection_string)
df_v2.to_sql("twitch_v2", engine, schema="public", if_exists="replace")
df_v1.to_sql("twitch_v1", engine, schema="public", if_exists="replace")
