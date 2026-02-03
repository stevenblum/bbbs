#!/bin/bash

if [ ! -d "osm" ]; then
  mkdir osm
fi

# Define the download URL and output filename
DOWNLOAD_URL="https://download.geofabrik.de/north-america/us/rhode-island-latest.osm.pbf"

OUTPUT_FILE="osm/ri.osm.pbf"

# Download the file using wget
echo "Downloading OpenStreetMap data for Rhode Island..."
wget -O "$OUTPUT_FILE" "$DOWNLOAD_URL"

if [ $? -eq 0 ]; then
    echo "Download complete. File saved as $OUTPUT_FILE"
else
    echo "Download failed."
fi

# Define the download URL and output filename
DOWNLOAD_URL="https://download.geofabrik.de/north-america/us/massachusetts-latest.osm.pbf"

OUTPUT_FILE="osm/ma.osm.pbf"

# Download the file using wget
echo "Downloading OpenStreetMap data for Massachusetts..."
wget -O "$OUTPUT_FILE" "$DOWNLOAD_URL"

if [ $? -eq 0 ]; then
    echo "Download complete. File saved as $OUTPUT_FILE"
else
    echo "Download failed."
fi