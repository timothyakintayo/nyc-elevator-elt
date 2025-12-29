# import necessary libraries
import duckdb
from pathlib import Path

# connect to duckdb database
con = duckdb.connect("nyc_elevator_elt.duckdb")

# create a path object for the csv file
csv_path = Path("clean_elevator_requests.csv")
con.execute(f"""
    COPY clean_elevator_2024 TO '{csv_path}' (HEADER, DELIMITER ',');
""")
print(f"Exported clean_elevator_2024 to {csv_path.resolve()}")


# create a path object for the paquet
parquet_path = Path("clean_elevator_requests.parquet")
con.execute(f"""
    COPY clean_elevator_2024 TO '{parquet_path}' (FORMAT PARQUET);
""")
print(f"Exported clean_elevator_2024 to {parquet_path.resolve()}")


df = con.execute("""
    SELECT complaint_type, COUNT(*) AS issues
    FROM read_parquet('clean_elevator_requests.parquet')
    GROUP BY complaint_type
    ORDER BY issues DESC
    LIMIT 10;
""").fetchdf()


print("\nTop 10 complaint types from Parquet:")
print(df)