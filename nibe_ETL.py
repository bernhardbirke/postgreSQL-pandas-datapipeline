import pandas as pd
import numpy as np
from sqlalchemy import create_engine, URL
from postgresql_config import config

params = config()

url_object = URL.create(
    "postgresql",
    username=params["user"],
    password=params["password"],  # plain (unescaped) text
    host=params["host"],
    database=params["database"],
)

# Create an engine instance
alchemyEngine = create_engine(url_object)

# Connect to PostgreSQL server
dbConnection = alchemyEngine.connect()

# Read data from PostgreSQL database table and load into a DataFrame instance
df_diff = pd.read_sql("nibe_diff", dbConnection)
# find the last data_id in the table
if df_diff.empty == True:
    max_data_id = 0
else:
    max_data_id = df_diff["data_id"].max()


# print(max_data_id)

# Read data from PostgreSQL database table and load into a DataFrame instance
df = pd.read_sql("nibe", dbConnection)
df_filtered = df[df["data_id"] >= max_data_id].copy()
# test with a certain number of rows
# df_filtered = df[(df["data_id"] >= 150) & (df["data_id"] <= 250)].copy()
df_filtered = df_filtered.sort_values(by=["data_id"])
# calculate differences in pac and total_energy

df_filtered["verdichterstarts_diff"] = df_filtered["verdichterstarts"].diff()
df_filtered["gesamtbetriebszeit_verdichter_diff"] = df_filtered[
    "gesamtbetriebszeit_verdichter"
].diff()
df_filtered["brauchwasser_nur_verdichter_diff"] = df_filtered[
    "brauchwasser_nur_verdichter"
].diff()
df_filtered["heizung_nur_verdichter_diff"] = df_filtered[
    "heizung_nur_verdichter"
].diff()
df_filtered["intervall"] = df_filtered["time"].diff() / np.timedelta64(1, "s")
df_filtered["stromverbrauch"] = (
    df_filtered["intervall"] * df_filtered["momentan_verwendete_leistung"]
)
df_filtered["stromverbrauch_brauchwasser"] = (
    df_filtered["intervall"]
    * df_filtered["momentan_verwendete_leistung"]
    * df_filtered["umschaltventil_brauchwasser"]
)
df_filtered = df_filtered[
    [
        "data_id",
        "time",
        "verdichterstarts_diff",
        "gesamtbetriebszeit_verdichter_diff",
        "momentan_verwendete_leistung",
        "brauchwasser_nur_verdichter_diff",
        "heizung_nur_verdichter_diff",
        "intervall",
        "stromverbrauch",
        "stromverbrauch_brauchwasser",
    ]
]

# drop rows with NaN values (this is always the first row)
df_filtered = df_filtered.dropna(
    subset=[
        "verdichterstarts_diff",
        "gesamtbetriebszeit_verdichter_diff",
        "brauchwasser_nur_verdichter_diff",
        "heizung_nur_verdichter_diff",
    ]
)

# print(df_filtered)

# add data to database nibe_diff
try:
    df_filtered.to_sql(
        "nibe_diff",
        alchemyEngine,
        schema="public",
        if_exists="append",
        index=False,
        chunksize=10000,
    )
except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("PostgreSQL Data has been added successfully.")

finally:
    # Close the database connection
    dbConnection.close()
