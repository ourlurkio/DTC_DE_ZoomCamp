#Dockerfile modified to either run ingest_data.py for trip data, or ingest_zones.py to ingest the zones csv

FROM python

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_zones.py ingest_zones.py

ENTRYPOINT ["python", "ingest_zones.py"]
