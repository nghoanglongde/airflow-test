# SFTP Test Environment

This folder contains the SFTP server setup and test files for local development.

## Setup
### 1. Start SFTP Servers

```bash
docker-compose -f docker-compose.sftp.yml up -d
```

This starts:
- **Source SFTP**: `localhost:2222`
- **Target SFTP**: `localhost:2223`
- **Credentials**: `sftpuser` / `password`