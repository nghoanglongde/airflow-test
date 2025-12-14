FROM apache/airflow:2.10.5-python3.11

USER root

# Copy requirements and set permissions
COPY requirements.txt /requirements.txt
RUN chown airflow:root /requirements.txt

USER airflow

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
