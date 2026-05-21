FROM apache/airflow:2.8.1-python3.11

# Swtch to root to install system dependencies if needed
USER root
RUN apt-get update && apt-get install -y git && apt-get clean

# Switch back to airflow user
USER airflow

# Install project requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
