import psycopg2
import pandas as pd
from sqlalchemy import create_engine, URL
from postgresql_config import config

params = config()

url_object = URL.create(
    "postgresql",
    username=params['user'],
    password=params['password'],  # plain (unescaped) text
    host=params['host'],
    database=params['database'],
)

# Create an engine instance
alchemyEngine = create_engine(url_object)

# Connect to PostgreSQL server
dbConnection = alchemyEngine.connect()

# Read data from PostgreSQL database table and load into a DataFrame instance
df_diff = pd.read_sql("smartmeter_diff", dbConnection)
# find the last data_id in the table
if df_diff.empty == True:
    max_data_id = 0
else:
    max_data_id = df_diff['data_id'].max()


print(max_data_id)

# Read data from PostgreSQL database table and load into a DataFrame instance
df = pd.read_sql("smartmeter", dbConnection)
df_filtered = df[df["data_id"] >= max_data_id]
# test with a certain number of rows
# df_filtered = df[(df["data_id"] >= -10) & (df["data_id"] <= 50)]

# calculate differences in wirkenergie_p and wirkenergie_n
df_filtered['wirkenergie_p_diff'] = df_filtered['wirkenergie_p'].diff()
df_filtered['wirkenergie_n_diff'] = df_filtered['wirkenergie_n'].diff()

# rename the columns wirkenergie_p and wirkenergie_n
# df_filtered.rename(columns={'wirkenergie_p': 'wirkenergie_p_diff',
#                   'wirkenergie_n': 'wirkenergie_n_diff'}, inplace=True)

# drop unused columns
df_filtered = df_filtered.drop(['momentanleistung_p', 'momentanleistung_n', 'wirkenergie_p', 'wirkenergie_n', 'spannung_l1', 'spannung_l2',
                                'spannung_l3', 'strom_l1', 'strom_l2', 'strom_l3',  'leistungsfaktor'], axis=1)

# drop rows with NaN values (this is always the first row)
df_filtered = df_filtered.dropna(
    subset=['wirkenergie_p_diff', 'wirkenergie_n_diff'])

print(df_filtered)

# add data to database smartmeter_diff
try:
    df_filtered.to_sql("smartmeter_diff", alchemyEngine, schema='public',
                       if_exists='append', index=False, chunksize=10000)
except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("PostgreSQL Data has been added successfully.")

finally:
    # Close the database connection
    dbConnection.close()
