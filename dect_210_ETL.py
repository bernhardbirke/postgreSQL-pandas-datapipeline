import pandas as pd
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
df_diff = pd.read_sql("dect_210_diff", dbConnection)
# find the last data_id in the table
if df_diff.empty == True:
    max_data_id = 0
else:
    max_data_id = df_diff["data_id"].max()

# Read data from PostgreSQL database table and load into a DataFrame instance
df = pd.read_sql("dect_210", dbConnection)
df_filtered = df[df["data_id"] >= max_data_id].copy()

df_filtered = df_filtered.sort_values(by=["data_id"])
#remove null values
df_filtered = df_filtered[df_filtered["energy"]!=0].copy()
# calculate differences
df_filtered["power_diff"] = df_filtered["power"].diff()
df_filtered["energy_diff"] = df_filtered["energy"].diff()
df_filtered["temperature_diff"] = df_filtered["temperature"].diff()

df_filtered = df_filtered.drop(
    [
        "power",
        "energy",
        "temperature",
    ],
    axis=1,
)

# drop rows with NaN values (this is always the first row)
df_filtered = df_filtered.dropna(subset=["power_diff", "energy_diff", "temperature_diff"])


# add data to database _diff
try:
    df_filtered.to_sql(
        "dect_210_diff",
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