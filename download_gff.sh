#!/bin/bash

# download_gff.sh - Script to download a single GFF file from RNAcentral
# Usage: ./download_gff.sh <url> <output_path> <max_retries>

URL="$1"
OUTPUT_PATH="$2"
MAX_RETRIES="${3:-3}"
TIMEOUT="${4:-300}"

# Create output directory if it doesn't exist
OUTPUT_DIR=$(dirname "$OUTPUT_PATH")
mkdir -p "$OUTPUT_DIR"

# Function to attempt download
download_file() {
    local attempt=$1
    echo "[Attempt $attempt/$MAX_RETRIES] Downloading: $URL"
    
    # Use wget with timeout and retry logic
    wget --timeout="$TIMEOUT" \
         --tries=1 \
         --quiet \
         --show-progress \
         --no-check-certificate \
         -O "$OUTPUT_PATH.tmp" \
         "$URL"
    
    local status=$?
    
    if [ $status -eq 0 ]; then
        # Download successful, move temp file to final location
        mv "$OUTPUT_PATH.tmp" "$OUTPUT_PATH"
        echo "[SUCCESS] Downloaded: $OUTPUT_PATH"
        return 0
    elif [ $status -eq 8 ]; then
        # 404 error - file doesn't exist
        echo "[NOT FOUND] File not available: $URL"
        return 8
    else
        # Other error
        echo "[ERROR] Download failed with status $status"
        # Clean up partial download
        rm -f "$OUTPUT_PATH.tmp"
        return $status
    fi
}

# Main download loop with retries
for attempt in $(seq 1 $MAX_RETRIES); do
    download_file $attempt
    status=$?
    
    if [ $status -eq 0 ]; then
        # Success
        exit 0
    elif [ $status -eq 8 ]; then
        # 404 - no point retrying
        exit 8
    else
        # Other error - retry after delay
        if [ $attempt -lt $MAX_RETRIES ]; then
            echo "[RETRY] Waiting 5 seconds before retry..."
            sleep 5
        fi
    fi
done

echo "[FAILED] Maximum retries exceeded for: $URL"
exit 1
