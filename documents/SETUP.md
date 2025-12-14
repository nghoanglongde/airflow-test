# Setup Guide

If your machine dont have Docker installed, run the installation script in the main folder:
```bash
./docker_install.sh
```

Then for start Airflow, run line by line:

```bash
docker-compose build

docker-compose up airflow-init

docker-compose up -d
```


**Airflow UI**: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

**Flower (Celery Monitoring)**: http://localhost:5555

Stop airflow:

```bash
docker-compose down
```
