import numpy as np
import pandas as pd

SEED = 1337

# Data obtained from
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

df_v1 = df.copy()
df_v2 = df.copy()

# Make numeric columns of new version change from old version.
rng = np.random.default_rng(SEED)
for increasing_column in increasing_columns:
    growth = np.abs(rng.normal(0, 0.15, size=df_v1.shape[0]))
    new_values = (1 + growth) * df_v1[increasing_column]
    df_v2[increasing_column] = new_values.astype(int)

for fluctuating_column in fluctuating_columns:
    change = rng.normal(0, 0.15, size=df_v1.shape[0])
    df_v2[fluctuating_column] = ((1 + change) * df_v1[fluctuating_column]).astype(int)

# Make old version not have data about all channels from current version.
df_v1 = df_v1.sample(frac=0.85, random_state=SEED)

# Introduce a data error.
index = (~df_v2["channel"].isin(df_v1["channel"])).idxmax()
df_v2.loc[index, "language"] = "Sw3d1zh"

df_v1.to_csv("twitch_version1.csv")
df_v2.to_csv("twitch_version2.csv")
