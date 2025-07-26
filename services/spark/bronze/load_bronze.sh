#!/bin/bash

#===============================================================================
# This script is used to copy data from shared server locally to hadoop container,
# adds ingestion date as a column to text-based files, and uploads them to HDFS
#===============================================================================

# Source & Destination directories
SOURCE_HOST="orca@shared-layer"
SOURCE_DIR="/shared/bucket"
DEST_DIR="/tmp/shared_data"
LOG_FILE="/home/itversity/batch_files_copied.log"

# Create destination directory if it doesn't exist
if [ ! -d "$DEST_DIR" ]; then
  mkdir -p "$DEST_DIR"
fi
# Create log file if it doesn't exist
if [ ! -f "$LOG_FILE" ]; then
  touch "$LOG_FILE"
fi
# List files in the source directory
MAPFILE=()
while IFS= read -r -d '' file; do
    MAPFILE+=("$file")
done < <(ssh "$SOURCE_HOST" "find $SOURCE_DIR -type f -print0")

# Loop through files
for file in "${MAPFILE[@]}"; do
    # Extract relative path and filename
    rel_path=${file#"$SOURCE_DIR/"}
    dest_path="$DEST_DIR/$rel_path"
    dest_dir=$(dirname "$dest_path")
    filename=$(basename "$file")

    # Ensure destination directory exists
    if [ ! -d "$dest_dir" ]; then
        mkdir -p "$dest_dir"
    fi
    # Check if the file is already copied (from logs)
    if ! grep -q "$filename" "$LOG_FILE"; then
        # Copy file from shared server to local directory
        scp -r "$SOURCE_HOST:$file" "$dest_path"

        # Check if the file is text-based (using file command to avoid binary files)
        if file "$dest_path" | grep -q "text"; then
            # Create a temporary file for modified content
            temp_file="$dest_path.tmp"
            # Get current timestamp in format YYYY-MM-DD HH:MM:SS
            ingestion_date=$(date -u '+%Y-%m-%d %H:%M:%S')

            # Attempt to detect delimiter (comma, tab, or pipe) from the first line
            first_line=$(head -n 1 "$dest_path")
            if [[ "$first_line" == *","* ]]; then
                delimiter=","
            elif [[ "$first_line" == *$'\t'* ]]; then
                delimiter=$'\t'
            elif [[ "$first_line" == *"|"* ]]; then
                delimiter="|"
            else
                delimiter="," # Default to comma if no delimiter is detected
            fi

            # Process the entire file with awk to add ingestion_date as a column on the same line
            awk -v delimiter="$delimiter" -v date="$ingestion_date" '
            BEGIN { FS=delimiter; OFS=delimiter }
            {
                # Remove trailing carriage return (if present)
                gsub(/\r$/, "", $0)
                
                # Concatenate existing fields with ingestion_date on the same line
                if (NR==1) {
                    print $0 delimiter "ingestion_date"
                } else {
                    print $0 delimiter date
                }
            }
            ' "$dest_path" > "$temp_file"

            # Replace original file with modified file
            mv "$temp_file" "$dest_path"
        fi

        # Ensure HDFS destination directory exists
        hdfs_dir="/home/itversity/bronze/$(dirname "$rel_path")"
        hdfs dfs -mkdir -p "$hdfs_dir"

        # Upload the file to HDFS
        hdfs dfs -put -f "$dest_path" "$hdfs_dir"

        # Check if the copy and upload was successful
        if [ $? -eq 0 ]; then
            echo "Copied and uploaded to HDFS: $filename"
            # Log the copied file
            echo "At $(date '+%Y-%m-%d %H:%M:%S') $filename is copied and uploaded to HDFS" >> "$LOG_FILE"
        else
            echo "Failed to copy and upload to HDFS: $filename"
        fi
    else
        echo "File already copied and uploaded to HDFS: $filename"
    fi
done