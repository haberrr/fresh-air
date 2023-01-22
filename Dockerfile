FROM prefecthq/prefect:sha-26c145b-python3.10
COPY ./dist/ ./dist
RUN pip install --find-links ./dist fresh_air
