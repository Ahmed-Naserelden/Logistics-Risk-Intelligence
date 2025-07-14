#! /bin/bash

#===============================================================================
# This script is used to copy data from shared server locally to hadoop container
#================================================================================

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
        scp -r"$SOURCE_HOST:$file" "$dest_path"

        # Check if the copy was successful
        if [ $? -eq 0 ]; then
            echo "Copied: $filename"
            # Log the copied file
            echo "At $(date '+%Y-%m-%d %H:%M:%S') $filename is copied" >> "$LOG_FILE"
        else
            echo "Failed to copy: $filename"
        fi

    else
        echo "File already copied: $filename"
    fi
done