import os

import numpy as np
import pandas as pd
import sqlalchemy as sa

SEED = 1337

# Data obtaines from
# https://www.kaggle.com/datasets/aayushmishra1512/twitchdata
df = pd.read_csv("twitchdata.csv")
df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace(" ", "_")
df.columns = df.columns.str.replace("(minutes)", "", regex=False)

increasing_columns = [
    "watch_time",
    "stream_time",
    "views_gained",
]
fluctuating_columns = [
    "followers_gained",
    "followers",
    "average_viewers",
    "peak_viewers",
]

# Make old version not have data about all channels from current version.
df_v0 = df.copy().sample(frac=0.85, random_state=SEED)

# Make numeric columns of new version change from old version.
df_v1 = df.copy()
rng = np.random.default_rng(SEED)
for increasing_column in increasing_columns:
    growth = np.abs(rng.normal(0, 0.15, size=df_v0.shape[0]))
    df_v1[increasing_column] = ((1 + growth) * df_v0[increasing_column]).astype(int)

for fluctuating_column in fluctuating_columns:
    change = rng.normal(0, 0.15, size=df_v0.shape[0])
    df_v1[fluctuating_column] = ((1 + change) * df_v0[fluctuating_column]).astype(int)

# Introduce a data error.
index = (~df_v1["channel"].isin(df_v0["channel"])).idxmax()
df_v1.loc[index, "language"] = "Sw3d1zh"

# Upload tables to local Postgres instance.
# Run ``$ ./start_postgres.sh`` to make sure it is up and running.
address = os.environ.get("DB_ADDR", "localhost")
connection_string = f"postgresql://datajudge:datajudge@{address}:5432/datajudge"
engine = sa.create_engine(connection_string)
df_v1.to_sql("twitch_v1", engine, schema="public", if_exists="replace")
df_v0.to_sql("twitch_v0", engine, schema="public", if_exists="replace")
