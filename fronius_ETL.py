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
df_diff = pd.read_sql("fronius_gen24_diff", dbConnection)
# find the last data_id in the table
if df_diff.empty == True:
    max_data_id = 0
else:
    max_data_id = df_diff["data_id"].max()


# print(max_data_id)

# Read data from PostgreSQL database table and load into a DataFrame instance
df = pd.read_sql("fronius_gen24", dbConnection)
df_filtered = df[df["data_id"] >= max_data_id].copy()
# test with a certain number of rows
# df_filtered = df[(df["data_id"] >= 150) & (df["data_id"] <= 250)].copy()
df_filtered = df_filtered.sort_values(by=["data_id"])
# calculate differences in pac and total_energy
df_filtered["pac_diff"] = df_filtered["pac"].diff()
df_filtered["total_energy_diff"] = df_filtered["total_energy"].diff()

df_filtered = df_filtered.drop(
    [
        "pac",
        "total_energy",
    ],
    axis=1,
)

# drop rows with NaN values (this is always the first row)
df_filtered = df_filtered.dropna(subset=["pac_diff", "total_energy_diff"])

# print(df_filtered)

# add data to database smartmeter_diff
try:
    df_filtered.to_sql(
        "fronius_gen24_diff",
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
