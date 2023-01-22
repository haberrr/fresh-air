FROM prefecthq/prefect:sha-26c145b-python3.10
COPY requirements.txt ./
RUN pip install -r requirements.txt
