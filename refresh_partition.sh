#!/bin/bash

# Get the current date in the format YYYYMMDD
TODAY=$(date +%Y%m%d)

# Set the database name
DATABASE_NAME="pos"  # Update with your actual database name

# List of specific tables to run MSCK REPAIR TABLE on
TABLES=("t_customer" "t_employee" "t_location" "t_order" "t_product")

# Run MSCK REPAIR TABLE for each specified table
for TABLE in "${TABLES[@]}"; do
    echo "Running MSCK REPAIR TABLE for $TABLE"
    hive -e "USE $DATABASE_NAME; MSCK REPAIR TABLE $TABLE;"
    echo "MSCK REPAIR TABLE executed for $TABLE"
done
