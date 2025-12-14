#!/bin/bash

# Create test files for SFTP source server
# This script creates date-partitioned test files

set -e

echo "Creating test files for SFTP source server..."

# Configuration
SOURCE_DIR="./sftp-source-data/source"
NUM_DATES=3  # Create files for 3 dates
FILES_PER_DATE=10  # Create 10 files per date
FILE_SIZE_KB=100  # Each file is ~100KB

# Get dates (today, yesterday, day before)
DATES=(
    $(date +%Y-%m-%d)
    $(date -v-1d +%Y-%m-%d 2>/dev/null || date -d "1 day ago" +%Y-%m-%d)
    $(date -v-2d +%Y-%m-%d 2>/dev/null || date -d "2 days ago" +%Y-%m-%d)
)

# Create source directory structure
for date in "${DATES[@]}"; do
    DATE_DIR="${SOURCE_DIR}/${date}"
    mkdir -p "${DATE_DIR}"

    echo "Creating files for date: ${date}"

    # Create files for this date
    for i in $(seq -f "%03g" 1 ${FILES_PER_DATE}); do
        FILE_PATH="${DATE_DIR}/file_${i}.txt"

        # Generate random content
        {
            echo "Test File: file_${i}.txt"
            echo "Date: ${date}"
            echo "Generated at: $(date -Iseconds)"
            echo "----------------------------------------"
            # Add random data to reach desired file size
            head -c $((FILE_SIZE_KB * 1024)) /dev/urandom | base64
        } > "${FILE_PATH}"

        echo "  Created: ${FILE_PATH} ($(du -h ${FILE_PATH} | cut -f1))"
    done
done

echo ""
echo "Test file creation complete!"
echo "----------------------------------------"
echo "Summary:"
for date in "${DATES[@]}"; do
    FILE_COUNT=$(find "${SOURCE_DIR}/${date}" -name "*.txt" 2>/dev/null | wc -l)
    TOTAL_SIZE=$(du -sh "${SOURCE_DIR}/${date}" 2>/dev/null | cut -f1)
    echo "  ${date}: ${FILE_COUNT} files (${TOTAL_SIZE})"
done

echo ""
echo "Total files created: $((NUM_DATES * FILES_PER_DATE))"
echo ""
echo "To start SFTP servers, run:"
echo "  cd tests && docker-compose -f docker-compose.sftp.yml up -d"
echo ""
echo "To connect to source SFTP:"
echo "  sftp -P 2222 sftpuser@localhost"
echo "  password: password"
