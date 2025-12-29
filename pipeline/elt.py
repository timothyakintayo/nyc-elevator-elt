import duckdb
import os
import re
from dotenv import load_dotenv
import requests
import pandas as pd
import io
from sodapy import Socrata

# Load environment variables from .env
load_dotenv()

motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
socrata_token = os.getenv("SOCRATA_APP_TOKEN")
use_motherduck = os.getenv("USE_MOTHERDUCK", "false").lower() == "true"

# Use the API Token for connection
if not socrata_token:
    raise RuntimeError("Socrata token not found.")

# Connect to motherduck using the token
if use_motherduck:
    if not motherduck_token:
        raise RuntimeError("MotherDuck token required but not found.")
    print("Connecting to MotherDuck Database")
    con = duckdb.connect(
        f"md:elt_pipeline_motherduck?motherduck_token={motherduck_token}"
    )
else:
    local_db_path = "nyc_elevator_elt.duckdb"
    print(f"Using local DuckDB database: {local_db_path}")
    con = duckdb.connect(local_db_path)


# Fetch data from NYC Open Data via API for the elevator dataset
url = "https://data.cityofnewyork.us/api/v3/views/erm2-nwe9/query.csv?query=SELECT%0A%20%20%60unique_key%60%2C%0A%20%20%60created_date%60%2C%0A%20%20%60closed_date%60%2C%0A%20%20%60agency%60%2C%0A%20%20%60agency_name%60%2C%0A%20%20%60complaint_type%60%2C%0A%20%20%60descriptor%60%2C%0A%20%20%60location_type%60%2C%0A%20%20%60incident_zip%60%2C%0A%20%20%60incident_address%60%2C%0A%20%20%60street_name%60%2C%0A%20%20%60cross_street_1%60%2C%0A%20%20%60cross_street_2%60%2C%0A%20%20%60intersection_street_1%60%2C%0A%20%20%60intersection_street_2%60%2C%0A%20%20%60address_type%60%2C%0A%20%20%60city%60%2C%0A%20%20%60landmark%60%2C%0A%20%20%60facility_type%60%2C%0A%20%20%60status%60%2C%0A%20%20%60due_date%60%2C%0A%20%20%60resolution_description%60%2C%0A%20%20%60resolution_action_updated_date%60%2C%0A%20%20%60community_board%60%2C%0A%20%20%60bbl%60%2C%0A%20%20%60borough%60%2C%0A%20%20%60x_coordinate_state_plane%60%2C%0A%20%20%60y_coordinate_state_plane%60%2C%0A%20%20%60open_data_channel_type%60%2C%0A%20%20%60park_facility_name%60%2C%0A%20%20%60park_borough%60%2C%0A%20%20%60vehicle_type%60%2C%0A%20%20%60taxi_company_borough%60%2C%0A%20%20%60taxi_pick_up_location%60%2C%0A%20%20%60bridge_highway_name%60%2C%0A%20%20%60bridge_highway_direction%60%2C%0A%20%20%60road_ramp%60%2C%0A%20%20%60bridge_highway_segment%60%2C%0A%20%20%60latitude%60%2C%0A%20%20%60longitude%60%2C%0A%20%20%60location%60%0AWHERE%0A%20%20%60created_date%60%0A%20%20%20%20BETWEEN%20%222024-01-01T09%3A42%3A31%22%20%3A%3A%20floating_timestamp%0A%20%20%20%20AND%20%222024-12-31T09%3A42%3A31%22%20%3A%3A%20floating_timestamp%0A%20%20AND%20caseless_one_of(%60complaint_type%60%2C%20%22Elevator%22)%0AORDER%20BY%20%60created_date%60%20DESC%20NULL%20FIRST"

# Getting the data using APP Token from Socrata
headers = {
    "X-App-Token": socrata_token, 
    "User-Agent": "PythonApp"
}

# Step 1: Download the CSV data to local disk from NYC Open Database using the API
# Use request to pull the url data
response = requests.get(url, headers=headers)
if response.status_code != 200:
    raise Exception(f"Error downloading CSV: {response.status_code}")

with open("nyc_elevator_request_raw.csv", mode = "wb") as elevator_file:
    elevator_file.write(response.content)

print("nyc_elevator_request_raw.csv pulled from NYC Open Data")


# Create a table in duckdb from the csv using SELECT statement
con.execute("""
    CREATE OR REPLACE TABLE raw_nyc_311 AS
    SELECT * 
    FROM read_csv_auto('nyc_elevator_request_raw.csv', header=True);
""")

print("Loaded into DuckDB: Table raw_nyc_311")

# Step 4: Create table where complaint type rows are lowered and we only select complaint type elevator and 2024 data
con.execute("""
    CREATE OR REPLACE TABLE clean_elevator_2024 AS
    SELECT *
    REPLACE (LOWER(complaint_type) AS complaint_type)
    FROM raw_nyc_311
    WHERE complaint_type ILIKE '%Elevator%'
      AND created_date >= '2024-01-01'
      AND created_date < '2025-01-01';
""")

print("Filtered table saved: elevator_2024")


# Step 5: Fix column names in clean_elevator_2024
cols = con.execute("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'clean_elevator_2024'
    ORDER BY ordinal_position;
""").fetchall()

# Here i replace spaces and bad characters
def normalize_colname(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r'^[^a-z]+', '', s)
    s = re.sub(r'_+', '_', s)
    if s == '':
        s = 'col'
    return s

for (col,) in cols:
    new = normalize_colname(col)
    if new != col:  # only rename if changed
        sql = f'ALTER TABLE clean_elevator_2024 RENAME COLUMN "{col}" TO {new};'
        print("Executing the modification made and printing renamed columns:", sql)
        con.execute(sql)

# Step 6: Verify the column name and data type of the data
df = con.execute("PRAGMA table_info('clean_elevator_2024');").fetchdf()
print(df)

# Step 7: Add a new column closed in days to the table
con.execute("""
  ALTER TABLE clean_elevator_2024
  ADD COLUMN closed_in_days INTEGER
""")

# Step 8: Compute the values of the added column
con.execute("""
  UPDATE clean_elevator_2024
  SET closed_in_days = DATEDIFF('day', created_date, closed_date)
""")

# Step 9:Verify the computed column in the table
print(con.execute("""
    SELECT created_date, closed_date, closed_in_days
    FROM clean_elevator_2024
    WHERE closed_date IS NOT NULL
    LIMIT 10;
""").fetchdf())

# Export filtered CSV as csv to local disk

df = con.execute("SELECT * FROM clean_elevator_2024").fetchdf()
df.to_csv("clean_elevator_2024.csv", index=False)

# Print the number of rows in the dataset after the filter
print(f"Filtered CSV: elevator_2024.csv (rows = {len(df)})")

# Close the db connection
con.close()
