# SFTP Test Environment

This folder contains the SFTP server setup and test files for local development.

## Setup

### 1. Generate Test Files

```bash
cd tests
./create_test_files.sh
```

This creates:
- 3 dates of test data (today, yesterday, day before)
- 10 files per date (~133KB each)
- Files in `sftp-source-data/source/{date}/file_001.txt` format

### 2. Start SFTP Servers

```bash
docker-compose -f docker-compose.sftp.yml up -d
```

This starts:
- **Source SFTP**: `localhost:2222`
- **Target SFTP**: `localhost:2223`
- **Credentials**: `sftpuser` / `password`

### 3. Verify SFTP Servers

```bash
# Check containers are running
docker ps --filter "name=sftp"

# Check files inside source SFTP
docker exec sftp-source ls -lh /home/sftpuser/data/source/2025-12-13/
```