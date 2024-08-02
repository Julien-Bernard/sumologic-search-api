#!/bin/bash

# Directory containing the .yaml configuration files
CONFIG_DIR="config"

# Check if the config directory exists
if [ ! -d "$CONFIG_DIR" ]; then
  echo "Directory $CONFIG_DIR does not exist."
  exit 1
fi

# Loop through each .yaml file in the config directory
for CONFIG_FILE in "$CONFIG_DIR"/*.yaml; do
  # Check if any .yaml files were found
  if [ "$CONFIG_FILE" == "$CONFIG_DIR/*.yaml" ]; then
    echo "No .yaml files found in $CONFIG_DIR."
    exit 1
  fi
  
  # Call the Python script with the current .yaml file
  echo "Processing $CONFIG_FILE"
  python3 ./sumologic-search-api.py -c "$CONFIG_FILE"
done