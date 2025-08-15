# This script extracts ISS pass data, transforms it, calculates when close to LAT/LON and loads it into a CSV file.

# Import necessary libraries
import requests     # API calls to get ISS pass data
import json         # Parse JSON data from API
import os           # Access environment variables
import logging      # Logging events
import pandas as pd # Data manipulation
import sys          # Exit if missing LATITUDE or LONGITUDE
from pathlib import Path # File paths
import time         # Sleep to avoid hitting API rate limits
from datetime import datetime, timezone # Timestamp the files and return timezone for positions
from dotenv import load_dotenv # Load environment variables from .env file
import folium

# Load environment variables
load_dotenv()
LAT = float(os.getenv("LAT", 0))  # optional map marker
LON = float(os.getenv("LON", 0))

# Setup logging
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)

logging.info("Starting ETL pipeline...")

# Function to fetch ISS positions from open-notify
def fetch_iss_positions(hours=24):
    base_url = "http://api.open-notify.org/iss-now.json"
    positions = []
    for _ in range(hours):
        try:
            response = requests.get(base_url, timeout=10)
            response.raise_for_status()
            data = response.json().get("iss_position", {})
            if data:
                positions.append({
                    "timestamp": datetime.now(timezone.utc),
                    "lat": float(data.get("latitude")),
                    "lon": float(data.get("longitude"))
                })
        except requests.exceptions.Timeout:
            logging.warning("Timeout fetching ISS position. Skipping this point.")
        except Exception as e:
            logging.error(f"Error fetching ISS position: {e}")
        time.sleep(1)
    return pd.DataFrame(positions)

# Fetch ISS positions
iss_positions = fetch_iss_positions(hours=24)  # adjust hours as needed

if iss_positions.empty:
    logging.warning("No ISS positions fetched. Exiting pipeline.")
else:
    # Save raw data
    Path("data/raw").mkdir(exist_ok=True)
    raw_file = f"data/raw/iss_positions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    iss_positions.to_csv(raw_file, index=False)
    logging.info(f"Saved raw data to {raw_file}")

    # Generate map visualization
    map_ = folium.Map(location=[0, 0], zoom_start=2)
    if LAT != 0 and LON != 0:
        folium.Marker([LAT, LON], popup="Your Location", icon=folium.Icon(color="red")).add_to(map_)
    for _, row in iss_positions.iterrows():
        folium.CircleMarker(location=[row['lat'], row['lon']], radius=3, color='blue', fill=True).add_to(map_)
    map_.save("iss_map.html")
    logging.info("Saved ISS map to iss_map.html")

logging.info("ETL pipeline finished successfully!")