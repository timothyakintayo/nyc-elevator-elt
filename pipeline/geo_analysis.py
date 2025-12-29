# STEP 1: Import Libraries
import os
import math
import numpy as np
import duckdb
import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv

# STEP 2: Set Constants
DB_PATH = "nyc_elevator_elt.duckdb"
RADIUS_MILES = 1.0
SORT_YEAR = "2022"

# STEP 3: Connect to Database and fetch data
con = duckdb.connect(DB_PATH, read_only=True)

# STEP 4: Get All Manhattan Elevator Locations : Give me latitude and longitude for all Manhattan elevator complaints that have coordinates"
points_sql = """
SELECT
  CAST(latitude AS DOUBLE)  AS lat,
  CAST(longitude AS DOUBLE) AS lon
FROM clean_elevator_2024
WHERE UPPER(borough) = 'MANHATTAN'
  AND complaint_type = 'elevator'
  AND latitude IS NOT NULL AND longitude IS NOT NULL;
"""

# STEP 5: Run the Query pts and store as list tuples
pts = con.execute(points_sql).fetchall()

# STEP 6: Check If We Got Data
if not pts:
    raise RuntimeError("No elevator points found in Manhattan.")

# STEP 7: Extract Latitudes
lats = np.array([p[0] for p in pts])
# STEP 8: Extract Longitudes
lons = np.array([p[1] for p in pts])

# STEP 9: Find the Hotspot (HQ Location) (Location with highest complaint count)
hq_sql = """
SELECT
  ROUND(lat, 3) AS lat_bin,
  ROUND(lon, 3) AS lon_bin,
  COUNT(*) AS complaints_in_bin,
  AVG(lat) AS avg_lat,
  AVG(lon) AS avg_lon
FROM (
  SELECT
    CAST(latitude AS DOUBLE) AS lat,
    CAST(longitude AS DOUBLE) AS lon
  FROM clean_elevator_2024
  WHERE UPPER(borough) = 'MANHATTAN'
    AND complaint_type = 'elevator'
    AND latitude IS NOT NULL AND longitude IS NOT NULL
)
GROUP BY 1,2
ORDER BY complaints_in_bin DESC
LIMIT 1;
"""
# STEP 10: Extract HQ Coordinates
_, _, complaints_in_bin, hq_lat, hq_lon = con.execute(hq_sql).fetchone()

# STEP 11: Print HQ Info
print(f"HQ candidate (hotspot): lat={hq_lat:.6f}, lon={hq_lon:.6f}, complaints_in_bin={complaints_in_bin}")
# STEP 12: Print Google Maps Link
print("Google Maps:", f"https://www.google.com/maps?q={hq_lat:.6f},{hq_lon:.6f}")

# STEP 13: Round All Latitudes to 3 Decimals
lat_bins = np.round(lats, 3)

# STEP 14: Round All Longitudes
lon_bins = np.round(lons, 3)

# STEP 15: Create DataFrame with Binned Coordinates
df_bins = pd.DataFrame({"lat_bin": lat_bins, "lon_bin": lon_bins})


# STEP 16: Count Complaints Per Grid Square
counts = df_bins.value_counts().reset_index(name="count")

# STEP 17: Pivot to 2D Grid
pivot = counts.pivot(index="lat_bin", columns="lon_bin", values="count").fillna(0)

# Prepare mesh for plotting
# STEP 18: Get Longitude Coordinates for Plotting
lon_coords = pivot.columns.values

# STEP 19: Get Latitude Coordinates for Plotting
lat_coords = pivot.index.values

# STEP 20: Create Coordinate Meshgrid: Create 2D grids of coordinates for plotting
X, Y = np.meshgrid(lon_coords, lat_coords)

# STEP 21: Create Figure (blank canvas) to draw on
fig, ax = plt.subplots(figsize=(7, 9))

# STEP 22: Draw the Heat Map
mesh = ax.pcolormesh(X, Y, pivot.values, cmap="hot", shading="auto")

# STEP 23: Mark HQ Location (Put a big red dot at the HQ location)
ax.plot([hq_lon], [hq_lat], marker="o", markersize=6, color="red")

# STEP 24: Calculate Latitude Distance Conversion
lat_per_mile = 1.0 / 69.0

# STEP 25: Calculate Longitude Distance Conversion
lon_per_mile = 1.0 / (69.0 * math.cos(math.radians(hq_lat)))

# STEP 26: Create Circle Angles (circle)
theta = np.linspace(0, 2*np.pi, 360)

# STEP 27: Draw 1-Mile Radius Circle
ax.plot(
    hq_lon + RADIUS_MILES * lon_per_mile * np.cos(theta),
    hq_lat + RADIUS_MILES * lat_per_mile * np.sin(theta),
    linewidth=1.5,
    color="blue"
)

# STEP 28: Add Colorbar: Adds a color legend below the map
cbar = fig.colorbar(mesh, ax=ax, orientation="horizontal", fraction=0.05, pad=0.1)
cbar.set_label("Complaints per 0.001° bin")

# STEP 29: Labels and Save
ax.set_title("Manhattan Elevator Requests — Heat Map (SQL Bin HQ)")
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
plt.tight_layout()
fig.savefig("manhattan_elevator_heatmap.png", dpi=200)
print("Saved heat map to manhattan_elevator_heatmap.png")


# CLOUD ANALYSIS (MotherDuck)
load_dotenv()

# STEP 30: Load MotherDuck Token
token = os.getenv("MOTHERDUCK_TOKEN")
if not token:
    raise RuntimeError("MotherDuck token not found in .env (MOTHERDUCK_TOKEN).")

# STEP 31: Connect to MotherDuck
md_con = duckdb.connect(f"md:?motherduck_token={token}")

# STEP 32: Set Earth's Radius
EARTH_MI = 3958.7613

# STEP 33: Get all complaints from MotherDuck's sample data and calculate distances
# Calculate Distance from HQ using Haversine formula for calculating distance between two points on a sphere (Earth).
cloud_sql_yearly = f"""
WITH src AS (
  SELECT
    complaint_type,
    created_date,
    CAST(latitude  AS DOUBLE) AS lat,
    CAST(longitude AS DOUBLE) AS lon
  FROM sample_data.nyc.service_requests
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL
),
dist AS (
  SELECT
    complaint_type,
    strftime('%Y', created_date) AS year,
    2 * {EARTH_MI} * ASIN(
      SQRT(
        POWER(SIN(RADIANS((lat - {hq_lat})/2)), 2)
        + COS(RADIANS({hq_lat})) * COS(RADIANS(lat)) *
          POWER(SIN(RADIANS((lon - {hq_lon})/2)), 2)
      )
    ) AS distance_miles
  FROM src
)
SELECT
  year,
  complaint_type,
  COUNT(*) AS requests_in_radius
FROM dist
WHERE distance_miles <= {RADIUS_MILES}
GROUP BY year, complaint_type
ORDER BY year, complaint_type;
"""

df = md_con.execute(cloud_sql_yearly).fetchdf()
pivoted = (
    df.pivot(index="complaint_type", columns="year", values="requests_in_radius")
      .fillna(0)
      .astype(int)
)

if SORT_YEAR in pivoted.columns:
    pivoted = pivoted.sort_values(by=SORT_YEAR, ascending=False)

print("\nPivoted DataFrame (complaint_type and year, sorted by", SORT_YEAR, "):")
print(pivoted.head(25))

# STEP 34: Export ALL rows to CSV
pivoted.to_csv("complaint_analysis_by_year.csv", index=True)
print(f"Exported {len(pivoted)} complaint types to complaint_analysis_by_year.csv")

# STEP 35: Load pivoted complaints CSV for plotting
df_trends = pd.read_csv("complaint_analysis_by_year.csv", index_col=0)

# STEP 36: Identify the top 10 complaint types by the latest year
top_complaints = df_trends.sort_values(by=str(SORT_YEAR), ascending=False).head(10)

fig2, ax2 = plt.subplots(figsize=(10, 6))

# STEP 37: Plot each complaint type as a line
for complaint in top_complaints.index:
    ax2.plot(top_complaints.columns, top_complaints.loc[complaint], marker='o', label=complaint)

ax2.set_title("Top 10 Complaint Types Trend (by Year)")
ax2.set_xlabel("Year")
ax2.set_ylabel("Number of Complaints")
ax2.legend(loc="upper left", bbox_to_anchor=(1,1))
plt.xticks(rotation=45)
plt.tight_layout()

# STEP 38: Save line chart
fig2.savefig("complaint_trends_top10.png", dpi=200)
print("Saved the line chart of complaint trends")

plt.show()
